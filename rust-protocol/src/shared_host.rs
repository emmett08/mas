//! Shared host intent, checkpoints and atomic hierarchical effect admission.
//! The protocol reservation, every scope charge and executing marker share one
//! PostgreSQL transaction. A replayed command receipt is never a dispatch grant.
use crate::{
    postgres_repository::{db_error, layout, reconcile, run, PostgresRepository},
    repository::ProtocolRepository,
    store::config,
};
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;
const DDL:&str="
CREATE TABLE host_scopes(id text PRIMARY KEY,max_runs bigint NOT NULL CHECK(max_runs>0),max_calls bigint NOT NULL CHECK(max_calls>0),max_microusd bigint NOT NULL CHECK(max_microusd>=0),runs bigint NOT NULL DEFAULT 0 CHECK(runs>=0 AND runs<=max_runs),calls bigint NOT NULL DEFAULT 0 CHECK(calls>=0 AND calls<=max_calls),microusd bigint NOT NULL DEFAULT 0 CHECK(microusd>=0 AND microusd<=max_microusd));
CREATE TABLE host_runs(run text PRIMARY KEY REFERENCES runs(id),principal text NOT NULL,scopes text NOT NULL,metadata text NOT NULL,owner text,owner_epoch bigint);
CREATE TABLE host_steps(seq bigint GENERATED ALWAYS AS IDENTITY,run text NOT NULL REFERENCES host_runs(run),key text NOT NULL,kind text NOT NULL,input text NOT NULL,data text NOT NULL,revision bigint NOT NULL DEFAULT 1,PRIMARY KEY(run,key));
CREATE TABLE host_invocations(seq bigint GENERATED ALWAYS AS IDENTITY,run text NOT NULL REFERENCES host_runs(run),call_id text NOT NULL,step_key text NOT NULL,actor text NOT NULL,state text NOT NULL CHECK(state IN ('prepared','executing','unknown','completed','not_executed','failed_known','resolved','retry_allowed')),output text,evidence text,reservation_microusd bigint NOT NULL CHECK(reservation_microusd>=0),PRIMARY KEY(run,call_id),FOREIGN KEY(run,step_key) REFERENCES host_steps(run,key));
CREATE TABLE host_commands(id text PRIMARY KEY,digest text NOT NULL,receipt text NOT NULL);
CREATE TABLE host_resolutions(seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,run text NOT NULL,call_id text NOT NULL,principal text NOT NULL,decision text NOT NULL,evidence text NOT NULL,created double precision NOT NULL,FOREIGN KEY(run,call_id) REFERENCES host_invocations(run,call_id));
";
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or_else(|| format!("invalid {k}"))
}
fn id<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    let s = text(v, k)?;
    if !crate::identity(s) {
        return Err(format!("invalid {k}"));
    }
    Ok(s)
}
fn integer(v: &Value, k: &str, min: i64) -> Result<i64> {
    v[k].as_i64()
        .filter(|n| *n >= min)
        .ok_or_else(|| format!("invalid {k}"))
}
fn object(v: &Value, k: &str) -> Result<String> {
    if !v[k].is_object() {
        return Err(format!("invalid {k}"));
    }
    Ok(v[k].to_string())
}
fn decoded(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| "invalid shared host record".into())
}
fn fields(v: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let o = v.as_object().ok_or("host parameters must be an object")?;
    if required.iter().any(|k| !o.contains_key(*k))
        || o.keys()
            .any(|k| !required.contains(&k.as_str()) && !optional.contains(&k.as_str()))
    {
        return Err("unsupported shared host fields".into());
    }
    Ok(())
}
fn owner(tx: &mut Transaction<'_>, p: &Value) -> Result<String> {
    let run_id = id(p, "run")?;
    let claim = id(p, "owner")?;
    let row=tx.query_opt("SELECT h.owner,h.owner_epoch,a.epoch FROM host_runs h CROSS JOIN authority a WHERE h.run=$1 AND a.singleton",&[&run_id]).map_err(db_error)?.ok_or("unknown host run")?;
    if row.get::<_, Option<String>>(0).as_deref() != Some(claim)
        || row.get::<_, Option<i64>>(1) != Some(row.get(2))
    {
        return Err("stale run ownership".into());
    }
    Ok(run_id.into())
}
fn invocation(tx: &mut Transaction<'_>, r: &str, c: &str) -> Result<Value> {
    let row=tx.query_opt("SELECT call_id,step_key,actor,state,output,evidence,reservation_microusd FROM host_invocations WHERE run=$1 AND call_id=$2",&[&r,&c]).map_err(db_error)?;
    row.map(|row|Ok(json!({"call_id":row.get::<_,String>(0),"step_key":row.get::<_,String>(1),"actor":row.get::<_,String>(2),"state":row.get::<_,String>(3),"output":row.get::<_,Option<String>>(4).as_deref().map(decoded).transpose()?,"evidence":row.get::<_,Option<String>>(5).as_deref().map(decoded).transpose()?,"reservation_microusd":row.get::<_,i64>(6)}))).unwrap_or(Ok(Value::Null))
}
fn step(tx: &mut Transaction<'_>, r: &str, k: &str) -> Result<Value> {
    tx.query_opt("SELECT kind,input,data,revision FROM host_steps WHERE run=$1 AND key=$2",&[&r,&k]).map_err(db_error)?.map(|row|Ok(json!({"kind":row.get::<_,String>(0),"input":decoded(row.get::<_,&str>(1))?,"data":decoded(row.get::<_,&str>(2))?,"revision":row.get::<_,i64>(3)}))).unwrap_or(Ok(Value::Null))
}
impl PostgresRepository {
    /// Explicit additive migration. Older version-one writers reject version 2.
    pub fn install_host_repository(&mut self) -> Result<()> {
        self.transaction(|tx| {
            let version: i64 = tx
                .query_one("SELECT version FROM authority WHERE singleton", &[])
                .map_err(db_error)?
                .get(0);
            if version >= 2 {
                return Ok(());
            }
            if version != 1 {
                return Err("unsupported shared host schema".into());
            }
            tx.batch_execute(DDL).map_err(db_error)?;
            let digest = layout(tx)?;
            tx.execute(
                "UPDATE authority SET version=2,layout_digest=$1 WHERE singleton",
                &[&digest],
            )
            .map_err(db_error)?;
            Ok(())
        })
    }
    pub fn install_service_repository(&mut self) -> Result<()> {
        self.transaction(|tx| {
            let version: i64 = tx
                .query_one("SELECT version FROM authority WHERE singleton", &[])
                .map_err(db_error)?
                .get(0);
            if version == 4 {
                return Ok(());
            }
            if version != 3 {
                return Err(
                    "controller repository must be installed before service repositories".into(),
                );
            }
            for ddl in [
                crate::shared_timing::DDL,
                crate::shared_tasks::DDL,
                crate::shared_composition::DDL,
                crate::shared_remote::DDL,
                crate::shared_artifacts::DDL,
                crate::shared_control::DDL,
            ] {
                tx.batch_execute(ddl).map_err(db_error)?;
            }
            let digest = layout(tx)?;
            tx.execute(
                "UPDATE authority SET version=4,layout_digest=$1 WHERE singleton",
                &[&digest],
            )
            .map_err(db_error)?;
            Ok(())
        })
    }
    pub fn host_command(
        &mut self,
        command_id: &str,
        operation: &str,
        parameters: &Value,
    ) -> Result<Value> {
        if !crate::identity(command_id) {
            return Err("invalid command identity".into());
        }
        let mut bytes = crate::MAX_BYTES;
        crate::validate_value(parameters, 0, &mut bytes).map_err(|e| e.message)?;
        crate::parse_json(&parameters.to_string()).map_err(|e| e.message)?;
        if operation == "admit_invocation" {
            self.reconcile(id(parameters, "run")?)?;
        }
        let digest: String = Sha256::digest(json!([operation, parameters]).to_string().as_bytes())
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        self.transaction(|tx| {
            if tx
                .query_one("SELECT version FROM authority WHERE singleton", &[])
                .map_err(db_error)?
                .get::<_, i64>(0)
                < 2
            {
                return Err("shared host repository not installed".into());
            }
            if let Some(row) = tx
                .query_opt(
                    "SELECT digest,receipt FROM host_commands WHERE id=$1",
                    &[&command_id],
                )
                .map_err(db_error)?
            {
                if row.get::<_, String>(0) != digest {
                    return Err("shared command identity conflict".into());
                }
                return Ok(json!({"replayed":true,"receipt":decoded(row.get::<_,&str>(1))?}));
            }
            if matches!(
                operation,
                "create_run"
                    | "prepare_invocation"
                    | "admit_invocation"
                    | "task.submit"
                    | "task.update"
                    | "task.claim"
                    | "task.recover"
                    | "composition.submit"
                    | "composition.claim"
                    | "composition.node"
                    | "composition.scope"
                    | "composition.attempt"
                    | "composition.start_attempt"
                    | "composition.continue"
                    | "composition.recover"
                    | "remote.submit_begin"
                    | "remote.operation_begin"
                    | "circuit.admit"
                    | "peer.register"
                    | "artifact.configure"
                    | "artifact.reserve"
                    | "artifact.link"
                    | "worker.submit"
                    | "worker.claim"
                    | "worker.dispatch"
            ) {
                crate::shared_leadership::check_admission(tx)?;
            }
            let receipt = apply(tx, operation, parameters)?;
            tx.execute(
                "INSERT INTO host_commands(id,digest,receipt) VALUES($1,$2,$3)",
                &[&command_id, &digest, &receipt.to_string()],
            )
            .map_err(db_error)?;
            Ok(json!({"replayed":false,"receipt":receipt}))
        })
    }
    pub fn host_query(&self, operation: &str, p: &Value) -> Result<Value> {
        self.transaction(|tx|match operation {
            "scope"=>{fields(p,&["scope"],&[])?;let row=tx.query_opt("SELECT max_runs,max_calls,max_microusd,runs,calls,microusd FROM host_scopes WHERE id=$1",&[&id(p,"scope")?]).map_err(db_error)?.ok_or("unknown allocation scope")?;Ok(json!({"max_runs":row.get::<_,i64>(0),"max_calls":row.get::<_,i64>(1),"max_microusd":row.get::<_,i64>(2),"runs":row.get::<_,i64>(3),"calls":row.get::<_,i64>(4),"microusd":row.get::<_,i64>(5)}))},
            "metadata"=>{fields(p,&["run"],&[])?;let row=tx.query_opt("SELECT metadata FROM host_runs WHERE run=$1",&[&id(p,"run")?]).map_err(db_error)?.ok_or("unknown host run")?;decoded(row.get::<_,&str>(0))},
            "invocation"=>{fields(p,&["run","call_id"],&[])?;invocation(tx,id(p,"run")?,id(p,"call_id")?)},
            "step"=>{fields(p,&["run","key"],&[])?;step(tx,id(p,"run")?,text(p,"key")?)},
            "steps"=>{fields(p,&["run"],&[])?;let mut out=Vec::new();for row in tx.query("SELECT key,kind,input,data,revision FROM host_steps WHERE run=$1 ORDER BY seq",&[&id(p,"run")?]).map_err(db_error)? {out.push(json!({"key":row.get::<_,String>(0),"kind":row.get::<_,String>(1),"input":decoded(row.get::<_,&str>(2))?,"data":decoded(row.get::<_,&str>(3))?,"revision":row.get::<_,i64>(4)}));}Ok(json!(out))},
            "invocations"=>{fields(p,&["run"],&[])?;let r=id(p,"run")?;let mut out=Vec::new();for row in tx.query("SELECT call_id FROM host_invocations WHERE run=$1 ORDER BY seq",&[&r]).map_err(db_error)? {out.push(invocation(tx,r,row.get::<_,&str>(0))?);}Ok(json!(out))},
            "recovery_snapshot"=>recovery_snapshot(tx,p),
            op if op.starts_with("authority.")=>crate::shared_leadership::query(tx,op,p),
            "resolutions"=>{fields(p,&["run"],&[])?;let r=id(p,"run")?;let mut out=Vec::new();for row in tx.query("SELECT seq,call_id,principal,decision,evidence,(created*1000)::bigint FROM host_resolutions WHERE run=$1 ORDER BY seq",&[&r]).map_err(db_error)? {out.push(json!({"seq":row.get::<_,i64>(0),"call_id":row.get::<_,String>(1),"principal":row.get::<_,String>(2),"decision":row.get::<_,String>(3),"evidence":decoded(row.get::<_,&str>(4))?,"created_ms":row.get::<_,i64>(5)}));}Ok(json!(out))},
            op if op.starts_with("task.") || op.starts_with("definition.") || op.starts_with("epoch.")=>crate::shared_tasks::query(tx,op,p),
            op if op.starts_with("composition.")=>crate::shared_composition::query(tx,op,p),
            op if op.starts_with("remote.") || op.starts_with("peer.") || op.starts_with("circuit.")=>crate::shared_remote::query(tx,op,p),
            op if op.starts_with("artifact.")=>crate::shared_artifacts::query(tx,op,p),
            op if op.starts_with("worker.") || op.starts_with("event.") || op.starts_with("operator.")=>crate::shared_control::query(tx,op,p),
            _=>Err("unknown shared host query".into())
        })
    }
}
fn apply(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    match operation {
        "configure_scope" => {
            fields(p, &["scope", "max_runs", "max_calls", "max_microusd"], &[])?;
            let s = id(p, "scope")?;
            let runs = integer(p, "max_runs", 1)?;
            let calls = integer(p, "max_calls", 1)?;
            let money = integer(p, "max_microusd", 0)?;
            if let Some(row) = tx
                .query_opt(
                    "SELECT max_runs,max_calls,max_microusd FROM host_scopes WHERE id=$1",
                    &[&s],
                )
                .map_err(db_error)?
            {
                if (
                    row.get::<_, i64>(0),
                    row.get::<_, i64>(1),
                    row.get::<_, i64>(2),
                ) != (runs, calls, money)
                {
                    return Err("frozen allocation scope differs".into());
                }
            } else {
                tx.execute("INSERT INTO host_scopes(id,max_runs,max_calls,max_microusd) VALUES($1,$2,$3,$4)",&[&s,&runs,&calls,&money]).map_err(db_error)?;
            }
            Ok(json!({"scope":s}))
        }
        "create_run" => {
            fields(
                p,
                &["run", "principal", "ancestors", "configuration", "metadata"],
                &[],
            )?;
            let r = id(p, "run")?;
            let principal = id(p, "principal")?;
            let mut scopes = vec!["deployment".to_owned(), format!("principal/{principal}")];
            let ancestors = p["ancestors"]
                .as_array()
                .filter(|a| a.len() <= 64)
                .ok_or("invalid ancestor scopes")?;
            for a in ancestors {
                let a = a
                    .as_str()
                    .filter(|a| crate::identity(a) && a.starts_with("ancestor/"))
                    .ok_or("invalid ancestor scope")?;
                if scopes.iter().any(|s| s == a) {
                    return Err("duplicate ancestor scope".into());
                }
                scopes.push(a.into());
            }
            let c = config(&p["configuration"])?;
            fields(
                &p["metadata"],
                &[
                    "run_id",
                    "workflow_version",
                    "topology",
                    "max_depth",
                    "secret_reference",
                    "principal",
                ],
                &[],
            )?;
            if p["metadata"]["run_id"] != r
                || p["metadata"]["principal"] != principal
                || p["metadata"]["topology"] != c
                || !matches!(p["metadata"]["max_depth"].as_i64(), Some(1..=64))
            {
                return Err("host metadata differs from frozen run".into());
            }
            text(&p["metadata"], "workflow_version")?;
            id(&p["metadata"], "secret_reference")?;
            let metadata = object(p, "metadata")?;
            let scope_text = json!(scopes).to_string();
            if let Some(row)=tx.query_opt("SELECT h.principal,h.scopes,h.metadata,r.config FROM host_runs h JOIN runs r ON r.id=h.run WHERE h.run=$1",&[&r]).map_err(db_error)? {
                if row.get::<_,String>(0)!=principal||row.get::<_,String>(1)!=scope_text||row.get::<_,String>(2)!=metadata||row.get::<_,String>(3)!=c.to_string(){return Err("frozen shared run differs".into());}
                return Ok(json!({"run_id":r}));
            }
            for scope in &scopes {
                let changed = tx
                    .execute(
                        "UPDATE host_scopes SET runs=runs+1 WHERE id=$1 AND runs<max_runs",
                        &[&scope],
                    )
                    .map_err(db_error)?;
                if changed != 1 {
                    return Err("run allocation scope unavailable or exhausted".into());
                }
            }
            let wall = c["wall_seconds"].as_i64().unwrap() as f64;
            tx.execute("INSERT INTO runs(id,config,deadline) VALUES($1,$2,extract(epoch from clock_timestamp())::double precision+$3)",&[&r,&c.to_string(),&wall]).map_err(db_error)?;
            tx.execute(
                "INSERT INTO host_runs(run,principal,scopes,metadata) VALUES($1,$2,$3,$4)",
                &[&r, &principal, &scope_text, &metadata],
            )
            .map_err(db_error)?;
            Ok(json!({"run_id":r}))
        }
        "claim_run" => {
            fields(p, &["run", "owner"], &[])?;
            let r = id(p, "run")?;
            let o = id(p, "owner")?;
            let epoch: i64 = tx
                .query_one("SELECT epoch FROM authority WHERE singleton", &[])
                .map_err(db_error)?
                .get(0);
            let changed=tx.execute("UPDATE host_runs SET owner=$1,owner_epoch=$2 WHERE run=$3 AND (owner IS NULL OR owner_epoch<>$2 OR owner=$1)",&[&o,&epoch,&r]).map_err(db_error)?;
            if changed != 1 {
                return Err("shared run is already owned".into());
            }
            Ok(json!({"owner":o,"epoch":epoch}))
        }
        "release_run" => {
            fields(p, &["run", "owner"], &[])?;
            let r = owner(tx, p)?;
            tx.execute(
                "UPDATE host_runs SET owner=NULL,owner_epoch=NULL WHERE run=$1",
                &[&r],
            )
            .map_err(db_error)?;
            Ok(json!({"released":true}))
        }
        "step" => {
            fields(p, &["run", "owner", "key", "kind", "input", "initial"], &[])?;
            let r = owner(tx, p)?;
            let k = text(p, "key")?;
            let kind = text(p, "kind")?;
            let input = object(p, "input")?;
            let data = object(p, "initial")?;
            let old = step(tx, &r, k)?;
            if !old.is_null() {
                if old["kind"] != kind || old["input"].to_string() != input {
                    return Err("workflow replay differs from shared checkpoint".into());
                }
                return Ok(old);
            }
            tx.execute(
                "INSERT INTO host_steps(run,key,kind,input,data) VALUES($1,$2,$3,$4,$5)",
                &[&r, &k, &kind, &input, &data],
            )
            .map_err(db_error)?;
            step(tx, &r, k)
        }
        "set_step" => {
            fields(
                p,
                &["run", "owner", "key", "expected_revision", "data"],
                &["kind"],
            )?;
            let r = owner(tx, p)?;
            let k = text(p, "key")?;
            let revision = integer(p, "expected_revision", 1)?;
            let data = object(p, "data")?;
            let kind = p.get("kind").map(|_| text(p, "kind")).transpose()?;
            let changed=tx.execute("UPDATE host_steps SET data=$1,kind=coalesce($2,kind),revision=revision+1 WHERE run=$3 AND key=$4 AND revision=$5",&[&data,&kind,&r,&k,&revision]).map_err(db_error)?;
            if changed != 1 {
                return Err("checkpoint revision conflict".into());
            }
            step(tx, &r, k)
        }
        "prepare_invocation" => {
            fields(
                p,
                &[
                    "run",
                    "owner",
                    "step_key",
                    "actor",
                    "call_id",
                    "reservation_microusd",
                ],
                &[],
            )?;
            let r = owner(tx, p)?;
            let k = text(p, "step_key")?;
            let a = id(p, "actor")?;
            let c = id(p, "call_id")?;
            let amount = integer(p, "reservation_microusd", 0)?;
            run(tx, &r, Some(a))?;
            let saved = step(tx, &r, k)?;
            if saved.is_null() || saved["kind"] != "invoke" {
                return Err("invocation step is missing".into());
            }
            if let Some(old_id) = saved["data"]["call_id"].as_str() {
                let old = invocation(tx, &r, old_id)?;
                if old["state"] != "retry_allowed" {
                    return Ok(old);
                }
            }
            tx.execute("INSERT INTO host_invocations(run,call_id,step_key,actor,state,reservation_microusd) VALUES($1,$2,$3,$4,'prepared',$5)",&[&r,&c,&k,&a,&amount]).map_err(db_error)?;
            tx.execute(
                "UPDATE host_steps SET data=$1,revision=revision+1 WHERE run=$2 AND key=$3",
                &[&json!({"call_id":c}).to_string(), &r, &k],
            )
            .map_err(db_error)?;
            invocation(tx, &r, c)
        }
        "admit_invocation" => {
            fields(p, &["run", "owner", "call_id"], &[])?;
            let r = owner(tx, p)?;
            let c = id(p, "call_id")?;
            let invocation = invocation(tx, &r, c)?;
            if invocation["state"] != "prepared" {
                return Err(
                    "invocation already admitted or unavailable; reconcile original identity"
                        .into(),
                );
            }
            let actor = invocation["actor"]
                .as_str()
                .ok_or("invalid invocation actor")?;
            let amount = invocation["reservation_microusd"]
                .as_i64()
                .ok_or("invalid invocation allocation")?;
            let protocol = run(tx, &r, Some(actor))?;
            if reconcile(tx, &r)?.0 {
                return Err("wall-clock budget exhausted".into());
            }
            if protocol.calls >= protocol.config["max_calls"].as_i64().unwrap() {
                return Err("call budget exhausted".into());
            }
            let scopes: Value = decoded(
                tx.query_one("SELECT scopes FROM host_runs WHERE run=$1", &[&r])
                    .map_err(db_error)?
                    .get::<_, &str>(0),
            )?;
            for scope in scopes.as_array().ok_or("invalid retained scopes")? {
                let scope = scope.as_str().ok_or("invalid retained scope")?;
                let changed=tx.execute("UPDATE host_scopes SET calls=calls+1,microusd=microusd+$1 WHERE id=$2 AND calls<max_calls AND $1<=max_microusd-microusd",&[&amount,&scope]).map_err(db_error)?;
                if changed != 1 {
                    return Err("call allocation scope unavailable or exhausted".into());
                }
            }
            tx.execute(
                "INSERT INTO calls(run,id,actor) VALUES($1,$2,$3)",
                &[&r, &c, &actor],
            )
            .map_err(db_error)?;
            tx.execute("UPDATE runs SET call_count=call_count+1 WHERE id=$1", &[&r])
                .map_err(db_error)?;
            tx.execute(
                "UPDATE host_invocations SET state='executing' WHERE run=$1 AND call_id=$2",
                &[&r, &c],
            )
            .map_err(db_error)?;
            Ok(json!({"call_id":c,"admitted":true}))
        }
        "observe_invocation" => {
            fields(
                p,
                &["run", "owner", "call_id", "state", "output", "evidence"],
                &[],
            )?;
            let r = owner(tx, p)?;
            let c = id(p, "call_id")?;
            let state = text(p, "state")?;
            let prior = invocation(tx, &r, c)?;
            if !matches!(
                state,
                "completed" | "unknown" | "not_executed" | "failed_known"
            ) {
                return Err("unsupported invocation observation".into());
            }
            if !matches!(prior["state"].as_str(), Some("executing" | "prepared")) {
                if prior["state"] == state
                    && prior["output"] == p["output"]
                    && prior["evidence"] == p["evidence"]
                {
                    return Ok(prior);
                }
                return Err("invocation observation conflicts with retained state".into());
            }
            if prior["state"] == "prepared" && state != "not_executed"
                || prior["state"] == "executing" && state == "not_executed"
            {
                return Err(
                    "observation cannot establish whether an admitted call was sent".into(),
                );
            }
            let output = if p["output"].is_null() {
                None
            } else {
                Some(object(p, "output")?)
            };
            let evidence = if p["evidence"].is_null() {
                None
            } else {
                Some(object(p, "evidence")?)
            };
            if state == "completed" && (output.is_none() || evidence.is_none()) {
                return Err("completed invocation requires output and evidence".into());
            }
            if matches!(state, "completed" | "failed_known") {
                if evidence.is_none() {
                    return Err("known outcome requires evidence".into());
                }
                let changed=tx.execute("UPDATE calls SET state='recorded',result=$1 WHERE run=$2 AND id=$3 AND state='reserved'",&[&evidence,&r,&c]).map_err(db_error)?;
                if changed != 1 {
                    return Err("matching native call reservation unavailable".into());
                }
            }
            tx.execute("UPDATE host_invocations SET state=$1,output=$2,evidence=$3 WHERE run=$4 AND call_id=$5",&[&state,&output,&evidence,&r,&c]).map_err(db_error)?;
            invocation(tx, &r, c)
        }
        "resolve_invocation" => {
            fields(
                p,
                &[
                    "run", "owner", "call_id", "operator", "decision", "evidence", "output",
                ],
                &[],
            )?;
            let r = owner(tx, p)?;
            let c = id(p, "call_id")?;
            let operator = id(p, "operator")?;
            let decision = text(p, "decision")?;
            let evidence = object(p, "evidence")?;
            if p["evidence"].as_object().unwrap().is_empty() {
                return Err("resolution evidence required".into());
            }
            let prior = invocation(tx, &r, c)?;
            if !matches!(prior["state"].as_str(), Some("executing" | "unknown")) {
                return Err("only an uncertain invocation can be resolved".into());
            }
            let (state, output) = match decision {
                "completed" => ("resolved", Some(object(p, "output")?)),
                "confirmed_no_effect" if p["output"].is_null() => ("retry_allowed", None),
                _ => return Err("unsupported resolution decision".into()),
            };
            // Operator knowledge never removes the original native reservation
            // or refunds any allocation. Resolution is a separate audited fact.
            tx.execute("INSERT INTO host_resolutions(run,call_id,principal,decision,evidence,created) VALUES($1,$2,$3,$4,$5,extract(epoch FROM clock_timestamp())::double precision)",&[&r,&c,&operator,&decision,&evidence]).map_err(db_error)?;
            tx.execute("UPDATE host_invocations SET state=$1,output=$2,evidence=$3 WHERE run=$4 AND call_id=$5",&[&state,&output,&evidence,&r,&c]).map_err(db_error)?;
            invocation(tx, &r, c)
        }
        op if op.starts_with("authority.") => crate::shared_leadership::apply(tx, op, p),
        op if op.starts_with("task.")
            || op.starts_with("definition.")
            || op.starts_with("epoch.") =>
        {
            crate::shared_tasks::apply(tx, op, p)
        }
        op if op.starts_with("composition.") => crate::shared_composition::apply(tx, op, p),
        op if op.starts_with("remote.")
            || op.starts_with("peer.")
            || op.starts_with("circuit.") =>
        {
            crate::shared_remote::apply(tx, op, p)
        }
        op if op.starts_with("artifact.") => crate::shared_artifacts::apply(tx, op, p),
        op if op.starts_with("worker.")
            || op.starts_with("event.")
            || op.starts_with("operator.") =>
        {
            crate::shared_control::apply(tx, op, p)
        }
        _ => Err("unknown shared host command".into()),
    }
}

fn recovery_snapshot(tx: &mut Transaction<'_>, p: &Value) -> Result<Value> {
    fields(p, &["run", "principal"], &[])?;
    let r = id(p, "run")?;
    let principal = id(p, "principal")?;
    let Some(binding)=tx.query_opt("SELECT h.principal,h.metadata,h.owner,h.owner_epoch,a.epoch FROM host_runs h CROSS JOIN authority a WHERE h.run=$1 AND a.singleton",&[&r]).map_err(db_error)? else {return Ok(Value::Null)};
    if binding.get::<_, String>(0) != principal {
        return Err("task_not_found".into());
    }
    let mut calls = Vec::new();
    for row in tx
        .query(
            "SELECT call_id FROM host_invocations WHERE run=$1 ORDER BY seq LIMIT 10001",
            &[&r],
        )
        .map_err(db_error)?
    {
        calls.push(invocation(tx, r, row.get::<_, &str>(0))?);
    }
    let mut remote = Vec::new();
    for row in tx.query("SELECT e.logical_key,e.remote_id,e.state,e.cancel_requested,e.updated,(e.pending_action IS NOT NULL OR e.state='unknown' OR EXISTS(SELECT 1 FROM remote_operations o WHERE o.key=e.key AND o.state='unknown')) FROM remote_effects e WHERE e.run=$1 ORDER BY e.key LIMIT 10001",&[&r]).map_err(db_error)?{
        remote.push(json!({"kind":"remote_task","key":row.get::<_,String>(0),"remote_id":row.get::<_,Option<String>>(1),"state":row.get::<_,String>(2),"cancel_requested":row.get::<_,bool>(3),"observed_at_ms":row.get::<_,i64>(4)*1000,"uncertain":row.get::<_,bool>(5)}));
    }
    if calls.len() > 10000 || remote.len() > 10000 {
        return Err("recovery snapshot capacity exceeded".into());
    }
    Ok(
        json!({"run_id":r,"metadata":decoded(binding.get::<_,&str>(1))?,"owned":binding.get::<_,Option<String>>(2).is_some()&&binding.get::<_,Option<i64>>(3)==Some(binding.get::<_,i64>(4)),"invocations":calls,"remote_observations":remote}),
    )
}
