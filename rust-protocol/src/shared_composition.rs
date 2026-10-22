//! Transactional composition graph and ready frontier in the shared authority.
//! No local journal or translated SQLite database participates in ownership.
use crate::{postgres_repository::db_error, shared_timing};
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;

pub(crate) const DDL: &str = "
CREATE TABLE composition_policy(singleton boolean PRIMARY KEY CHECK(singleton),max_instances bigint NOT NULL CHECK(max_instances>0));
CREATE TABLE composition_instances(id text PRIMARY KEY,principal text NOT NULL,request_namespace text NOT NULL,request_epoch text NOT NULL,request_key text NOT NULL,plan text NOT NULL,identity text NOT NULL,input text NOT NULL,metadata text NOT NULL,binding text NOT NULL,limits text NOT NULL,state text NOT NULL,output text,error text,created bigint NOT NULL,updated bigint NOT NULL,deadline bigint NOT NULL,cancel_requested boolean NOT NULL DEFAULT false,owner text,owner_epoch bigint,UNIQUE(principal,request_namespace,request_epoch,request_key));
CREATE TABLE composition_statistics(instance text PRIMARY KEY REFERENCES composition_instances(id),nodes bigint NOT NULL DEFAULT 0,completed bigint NOT NULL DEFAULT 0,revision bigint NOT NULL DEFAULT 0);
CREATE TABLE composition_nodes(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,kind text NOT NULL,input text NOT NULL,state text NOT NULL,output text,error text,PRIMARY KEY(instance,path));
CREATE TABLE composition_attempts(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,revision bigint NOT NULL,workflow text NOT NULL,identity text NOT NULL,run_id text UNIQUE NOT NULL,input text NOT NULL,wall_seconds bigint NOT NULL,ancestors text NOT NULL,state text NOT NULL,output text,error text,latest boolean NOT NULL DEFAULT true,queued_ms bigint NOT NULL DEFAULT (floor(extract(epoch FROM clock_timestamp())*1000)::bigint),started_ms bigint,PRIMARY KEY(instance,path,revision));
CREATE INDEX composition_ready ON composition_attempts(instance,latest,state,path);
CREATE UNIQUE INDEX composition_latest ON composition_attempts(instance,path) WHERE latest;
CREATE TABLE composition_scopes(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,id text UNIQUE NOT NULL,limits text NOT NULL,deadline bigint NOT NULL,PRIMARY KEY(instance,path));
CREATE TABLE composition_progress(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,kind text NOT NULL,next_index bigint NOT NULL,output text NOT NULL,consumed bigint NOT NULL,PRIMARY KEY(instance,path));
CREATE TABLE composition_joins(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,winners text NOT NULL,obsolete text NOT NULL,PRIMARY KEY(instance,path));
CREATE TABLE composition_continuations(instance text NOT NULL REFERENCES composition_instances(id),request_key text NOT NULL,digest text NOT NULL,receipt text NOT NULL,PRIMARY KEY(instance,request_key));
CREATE TABLE composition_protocol_events(run_id text NOT NULL REFERENCES composition_attempts(run_id),message_id text NOT NULL,data text NOT NULL,PRIMARY KEY(run_id,message_id));
CREATE TABLE composition_events(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,instance text NOT NULL REFERENCES composition_instances(id),path text,event text NOT NULL,state text NOT NULL,data text,created bigint NOT NULL);
";
pub(crate) const RETENTION_DDL: &str = "
ALTER TABLE composition_instances ADD COLUMN archived_at bigint;
ALTER TABLE composition_instances ADD COLUMN input_digest text;
ALTER TABLE composition_instances ADD COLUMN output_digest text;
ALTER TABLE composition_instances ADD COLUMN event_floor bigint NOT NULL DEFAULT 0;
ALTER TABLE composition_attempts ADD COLUMN input_digest text;
ALTER TABLE composition_attempts ADD COLUMN output_digest text;
ALTER TABLE composition_attempts ADD COLUMN error_digest text;
";
fn text<'a>(p: &'a Value, key: &str) -> Result<&'a str> {
    p[key]
        .as_str()
        .filter(|x| !x.is_empty() && x.len() <= 4096)
        .ok_or_else(|| format!("invalid {key}"))
}
fn integer(p: &Value, key: &str, min: i64) -> Result<i64> {
    p[key]
        .as_i64()
        .filter(|v| *v >= min)
        .ok_or_else(|| format!("invalid {key}"))
}
fn decoded(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| "invalid composition record".into())
}
fn optional(s: Option<&str>) -> Result<Value> {
    s.map(decoded).transpose().map(|v| v.unwrap_or(Value::Null))
}
fn now(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()))::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0))
}
fn terminal(s: &str) -> bool {
    matches!(s, "completed" | "failed" | "cancelled" | "expired")
}
fn valid_state(s: &str) -> bool {
    terminal(s)
        || matches!(
            s,
            "queued"
                | "running"
                | "waiting"
                | "unknown"
                | "input_required"
                | "auth_required"
                | "blocked_definition"
        )
}
fn event(
    tx: &mut Transaction<'_>,
    instance: &str,
    path: Option<&str>,
    name: &str,
    state: &str,
    data: Option<&Value>,
) -> Result<()> {
    let timestamp = now(tx)?;
    let data = data.map(Value::to_string);
    tx.execute("UPDATE composition_instances SET updated=floor(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE id=$1",&[&instance]).map_err(db_error)?;
    tx.execute("INSERT INTO composition_events(instance,path,event,state,data,created) VALUES($1,$2,$3,$4,$5,$6)", &[&instance,&path,&name,&state,&data,&timestamp]).map_err(db_error)?;
    Ok(())
}
fn instance(tx: &mut Transaction<'_>, id: &str) -> Result<Value> {
    let r=tx.query_opt("SELECT id,principal,request_namespace,request_epoch,request_key,plan,identity,input,metadata,binding,limits,state,output,error,created,deadline,cancel_requested,updated,archived_at,input_digest,output_digest,event_floor FROM composition_instances WHERE id=$1",&[&id]).map_err(db_error)?.ok_or("composition not found")?;
    Ok(
        json!({"id":r.get::<_,&str>(0),"principal":r.get::<_,&str>(1),"request_namespace":r.get::<_,&str>(2),"request_epoch":r.get::<_,&str>(3),"request_key":r.get::<_,&str>(4),"plan":r.get::<_,&str>(5),"identity":r.get::<_,&str>(6),"input":decoded(r.get(7))?,"metadata":decoded(r.get(8))?,"binding":decoded(r.get(9))?,"limits":decoded(r.get(10))?,"state":r.get::<_,&str>(11),"output":optional(r.get(12))?,"error":optional(r.get(13))?,"created":r.get::<_,i64>(14),"deadline":r.get::<_,i64>(15),"cancel_requested":r.get::<_,bool>(16),"updated":r.get::<_,i64>(17),"archived":r.get::<_,Option<i64>>(18).is_some(),"archived_at":r.get::<_,Option<i64>>(18),"input_digest":r.get::<_,Option<&str>>(19),"output_digest":r.get::<_,Option<&str>>(20),"event_floor":r.get::<_,i64>(21),"clock":shared_timing::read(tx,id,"")?,"timing":shared_timing::status(tx,id,"")?}),
    )
}
fn owned(tx: &mut Transaction<'_>, p: &Value) -> Result<String> {
    let id = text(p, "instance")?;
    let owner = text(p, "owner")?;
    let r=tx.query_opt("SELECT i.owner,i.owner_epoch,a.epoch FROM composition_instances i CROSS JOIN authority a WHERE i.id=$1 AND a.singleton",&[&id]).map_err(db_error)?.ok_or("composition not found")?;
    if r.get::<_, Option<&str>>(0) != Some(owner) || r.get::<_, Option<i64>>(1) != Some(r.get(2)) {
        return Err("stale composition owner".into());
    }
    Ok(id.into())
}
fn node(tx: &mut Transaction<'_>, id: &str, path: &str) -> Result<Value> {
    tx.query_opt("SELECT kind,input,state,output,error FROM composition_nodes WHERE instance=$1 AND path=$2",&[&id,&path]).map_err(db_error)?.map(|r|Ok(json!({"path":path,"kind":r.get::<_,&str>(0),"input":decoded(r.get(1))?,"state":r.get::<_,&str>(2),"output":optional(r.get(3))?,"error":optional(r.get(4))?}))).unwrap_or(Ok(Value::Null))
}
fn attempt(tx: &mut Transaction<'_>, id: &str, path: &str) -> Result<Value> {
    attempt_at(tx, id, path, None)
}
fn attempt_at(
    tx: &mut Transaction<'_>,
    id: &str,
    path: &str,
    revision: Option<i64>,
) -> Result<Value> {
    tx.query_opt("SELECT revision,workflow,identity,run_id,input,wall_seconds,ancestors,state,output,error,queued_ms,started_ms,input_digest,output_digest,error_digest FROM composition_attempts WHERE instance=$1 AND path=$2 AND ($3::bigint IS NULL OR revision=$3) ORDER BY revision DESC LIMIT 1",&[&id,&path,&revision]).map_err(db_error)?.map(|r|Ok(json!({"instance":id,"path":path,"revision":r.get::<_,i64>(0),"workflow":r.get::<_,&str>(1),"identity":r.get::<_,&str>(2),"run_id":r.get::<_,&str>(3),"run_directory":r.get::<_,&str>(3),"input":decoded(r.get(4))?,"wall_seconds":r.get::<_,i64>(5),"ancestors":decoded(r.get(6))?,"state":r.get::<_,&str>(7),"output":optional(r.get(8))?,"error":optional(r.get(9))?,"queued_at_ms":r.get::<_,i64>(10),"queue_elapsed_ms":r.get::<_,Option<i64>>(11).map(|started|(started-r.get::<_,i64>(10)).max(0)),"input_digest":r.get::<_,Option<&str>>(12),"output_digest":r.get::<_,Option<&str>>(13),"error_digest":r.get::<_,Option<&str>>(14)}))).unwrap_or(Ok(Value::Null))
}
fn scope(
    tx: &mut Transaction<'_>,
    id: &str,
    path: &str,
    scope_id: &str,
    limits: &Value,
    deadline: i64,
) -> Result<()> {
    let runs = integer(limits, "max_steps", 1)?;
    let calls = integer(limits, "max_calls", 1)?;
    let money = integer(limits, "max_reserved_microusd", 0)?;
    tx.execute(
        "INSERT INTO host_scopes(id,max_runs,max_calls,max_microusd) VALUES($1,$2,$3,$4)",
        &[&scope_id, &runs, &calls, &money],
    )
    .map_err(db_error)?;
    tx.execute(
        "INSERT INTO composition_scopes(instance,path,id,limits,deadline) VALUES($1,$2,$3,$4,$5)",
        &[&id, &path, &scope_id, &limits.to_string(), &deadline],
    )
    .map_err(db_error)?;
    Ok(())
}
fn ancestors(tx: &mut Transaction<'_>, id: &str, path: &str) -> Result<Value> {
    let mut scopes = Vec::new();
    for r in tx
        .query(
            "SELECT path,id FROM composition_scopes WHERE instance=$1 ORDER BY length(path),path",
            &[&id],
        )
        .map_err(db_error)?
    {
        let prefix = r.get::<_, &str>(0);
        if path == prefix || path.starts_with(&format!("{prefix}/")) {
            scopes.push(r.get::<_, String>(1));
        }
    }
    Ok(json!(scopes))
}
fn progress(tx: &mut Transaction<'_>, id: &str, path: &str) -> Result<Value> {
    tx.query_opt("SELECT kind,next_index,output,consumed FROM composition_progress WHERE instance=$1 AND path=$2",&[&id,&path]).map_err(db_error)?.map(|r|Ok(json!({"kind":r.get::<_,&str>(0),"next_index":r.get::<_,i64>(1),"output":decoded(r.get(2))?,"consumed":r.get::<_,i64>(3)}))).unwrap_or(Ok(Value::Null))
}
fn join(tx: &mut Transaction<'_>, id: &str, path: &str) -> Result<Value> {
    tx.query_opt(
        "SELECT winners,obsolete FROM composition_joins WHERE instance=$1 AND path=$2",
        &[&id, &path],
    )
    .map_err(db_error)?
    .map(|r| Ok(json!({"winners":decoded(r.get(0))?,"obsolete":decoded(r.get(1))?})))
    .unwrap_or(Ok(Value::Null))
}
fn obsolete(tx: &mut Transaction<'_>, id: &str, path: &str) -> Result<bool> {
    for r in tx
        .query(
            "SELECT obsolete FROM composition_joins WHERE instance=$1",
            &[&id],
        )
        .map_err(db_error)?
    {
        let values = decoded(r.get(0))?;
        for value in values.as_array().ok_or("invalid join")? {
            let prefix = value.as_str().ok_or("invalid join path")?;
            if prefix == path || path.starts_with(&format!("{prefix}/")) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
fn require_path_open(tx: &mut Transaction<'_>, id: &str, path: &str) -> Result<()> {
    shared_timing::require_open(tx, id, "")?;
    for r in tx
        .query(
            "SELECT path FROM service_clocks WHERE owner=$1 AND path<>''",
            &[&id],
        )
        .map_err(db_error)?
    {
        let prefix = r.get::<_, &str>(0);
        if path == prefix || path.starts_with(&format!("{prefix}/")) {
            shared_timing::require_open(tx, id, prefix)?;
        }
    }
    Ok(())
}
fn activity(tx: &mut Transaction<'_>, id: &str) -> Result<()> {
    let paths = tx
        .query("SELECT path FROM service_clocks WHERE owner=$1", &[&id])
        .map_err(db_error)?;
    for r in paths {
        let path = r.get::<_, &str>(0);
        let clock = shared_timing::read(tx, id, path)?;
        if clock["phase"] == "terminal" {
            continue;
        }
        let count:i64=tx.query_one("SELECT count(*) FROM composition_attempts WHERE instance=$1 AND state='running' AND ($2='' OR path=$2 OR starts_with(path,$2||'/'))",&[&id,&path]).map_err(db_error)?.get(0);
        let state = shared_timing::status(tx, id, path)?;
        let phase = if count > 0 {
            "running"
        } else if state["expired"].as_array().is_some_and(|v| !v.is_empty())
            || clock["phase"] == "paused"
        {
            "paused"
        } else {
            "queued"
        };
        // Saving the last worker's result must remain possible after expiry.
        // Existing running time is charged; admission still requires open clocks.
        if count > 0 && state["expired"].as_array().is_some_and(|v| !v.is_empty()) {
            continue;
        }
        shared_timing::transition(tx, id, path, phase, count)?;
    }
    Ok(())
}
fn expired(tx: &mut Transaction<'_>, id: &str) -> Result<bool> {
    let item = instance(tx, id)?;
    if integer(&item, "deadline", 1)? <= now(tx)?
        || item["timing"]["expired"]
            .as_array()
            .is_some_and(|v| !v.is_empty())
    {
        return Ok(true);
    }
    for r in tx.query("SELECT s.path,s.deadline,n.state FROM composition_scopes s LEFT JOIN composition_nodes n ON n.instance=s.instance AND n.path=s.path WHERE s.instance=$1",&[&id]).map_err(db_error)?{
        if r.get::<_,Option<&str>>(2).is_some_and(terminal){continue}
        let status=shared_timing::status(tx,id,r.get(0))?;
        if r.get::<_,i64>(1)<=now(tx)?||status["expired"].as_array().is_some_and(|v|!v.is_empty()){return Ok(true)}
    }
    Ok(false)
}
pub(crate) fn apply(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    if operation == "composition.expire" {
        let candidates = p["instances"].as_array().filter(|v| v.len()<=1000).ok_or("invalid expiry candidates")?;
        let candidates = candidates.iter().map(|v| v.as_str().map(str::to_owned).ok_or("invalid expiry candidate")).collect::<std::result::Result<Vec<_>,_>>()?;
        let rows=tx.query("SELECT id FROM composition_instances WHERE id=ANY($1) AND (owner IS NULL OR owner_epoch<>(SELECT epoch FROM authority WHERE singleton)) AND state IN ('queued','waiting','blocked_definition','input_required','auth_required')",&[&candidates]).map_err(db_error)?;
        let mut count = 0;
        for row in rows {
            let id = row.get::<_, &str>(0);
            if !expired(tx, id)? {
                continue;
            }
            let uncertain:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM composition_attempts WHERE instance=$1 AND state IN ('running','unknown'))",&[&id]).map_err(db_error)?.get(0);
            let state = if uncertain { "unknown" } else { "expired" };
            tx.execute("UPDATE composition_instances SET state=$2,error=$3,owner=NULL,owner_epoch=NULL WHERE id=$1",&[&id,&state,&json!({"code":if uncertain{"reconciliation_required"}else{"deadline_exceeded"}}).to_string()]).map_err(db_error)?;
            shared_timing::transition(
                tx,
                id,
                "",
                if uncertain { "paused" } else { "terminal" },
                0,
            )?;
            event(tx, id, None, "expired", state, None)?;
            count += 1;
        }
        return Ok(json!({"expired":count}));
    }
    if operation == "composition.configure" {
        let maximum = integer(p, "max_instances", 1)?;
        if let Some(row) = tx
            .query_opt(
                "SELECT max_instances FROM composition_policy WHERE singleton",
                &[],
            )
            .map_err(db_error)?
        {
            if row.get::<_, i64>(0) != maximum {
                return Err("composition policy changed".into());
            }
        } else {
            tx.execute(
                "INSERT INTO composition_policy VALUES(true,$1)",
                &[&maximum],
            )
            .map_err(db_error)?;
        }
        return Ok(json!({"max_instances":maximum}));
    }
    if operation == "composition.submit" {
        let id = text(p, "instance")?;
        let principal = text(p, "principal")?;
        let namespace = text(p, "request_namespace")?;
        let epoch = p["request_epoch"].as_str().ok_or("invalid request epoch")?;
        let key = text(p, "request_key")?;
        let digest: String = Sha256::digest(
            json!([p["plan"], p["input"], p["metadata"]])
                .to_string()
                .as_bytes(),
        )
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
        if let Some(target) = crate::shared_tasks::request(
            tx,
            principal,
            namespace,
            epoch,
            key,
            &digest,
            "composition",
        )? {
            return instance(tx, &target);
        }
        if let Some(r)=tx.query_opt("SELECT id FROM composition_instances WHERE principal=$1 AND request_namespace=$2 AND request_epoch=$3 AND request_key=$4",&[&principal,&namespace,&epoch,&key]).map_err(db_error)? {let saved=instance(tx,r.get(0))?;if ["plan","identity","input","metadata"].iter().any(|k|saved[k]!=p[k]){return Err("composition request conflict".into())}return Ok(saved)}
        let plan = text(p, "plan")?;
        let identity = text(p, "identity")?;
        let current=tx.query_opt("SELECT a.identity,d.retired FROM service_aliases a JOIN service_definitions d ON d.kind=a.kind AND d.name=a.name AND d.identity=a.identity WHERE a.kind='plan' AND a.name=$1",&[&plan]).map_err(db_error)?;
        if current.is_none_or(|r|r.get::<_,&str>(0)!=identity||r.get::<_,bool>(1)){return Err("workflow_changed".into())}
        if !p["input"].is_object() || !p["metadata"].is_object() || !p["binding"].is_object() {
            return Err("invalid composition submission".into());
        }
        crate::shared_scheduler::queue_admission::composition(tx)?;
        crate::shared_tasks::check_submission(tx, principal)?;
        let count: i64 = tx
            .query_one("SELECT count(*) FROM composition_instances WHERE archived_at IS NULL", &[])
            .map_err(db_error)?
            .get(0);
        let maximum: i64 = tx
            .query_one(
                "SELECT max_instances FROM composition_policy WHERE singleton",
                &[],
            )
            .map_err(db_error)?
            .get(0);
        if count >= maximum {
            return Err("composition instance limit".into());
        }
        let timestamp = now(tx)?;
        let clock = shared_timing::start(tx, id, "", &p["timing_policy"])?;
        let seconds = if clock.is_null() {
            integer(&p["limits"], "wall_seconds", 1)?
        } else {
            integer(&clock["policy"], "business_seconds", 1)?
        };
        let deadline = timestamp.checked_add(seconds).ok_or("deadline overflow")?;
        tx.execute("INSERT INTO composition_instances(id,principal,request_namespace,request_epoch,request_key,plan,identity,input,metadata,binding,limits,state,created,deadline,updated) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,'queued',$12,$13,$14)",&[&id,&principal,&namespace,&epoch,&key,&plan,&identity,&p["input"].to_string(),&p["metadata"].to_string(),&p["binding"].to_string(),&p["limits"].to_string(),&timestamp,&deadline,&(timestamp*1000)]).map_err(db_error)?;
        tx.execute(
            "INSERT INTO composition_statistics(instance) VALUES($1)",
            &[&id],
        )
        .map_err(db_error)?;
        crate::shared_tasks::remember_request(
            tx,
            principal,
            namespace,
            epoch,
            key,
            &digest,
            "composition",
            id,
        )?;
        scope(
            tx,
            id,
            "root",
            &format!("ancestor/{id}"),
            &p["limits"],
            deadline,
        )?;
        event(tx, id, None, "submitted", "queued", None)?;
        return instance(tx, id);
    }
    if operation == "composition.claim" {
        let id = text(p, "instance")?;
        let claim = text(p, "owner")?;
        let saved=instance(tx,id)?;
        if saved["archived"]==true || terminal(saved["state"].as_str().unwrap_or("")) {return Err("composition_not_runnable".into())}
        let row=tx.query_opt("SELECT i.owner,i.owner_epoch,a.epoch FROM composition_instances i CROSS JOIN authority a WHERE i.id=$1 AND a.singleton",&[&id]).map_err(db_error)?.ok_or("composition not found")?;
        let epoch: i64 = row.get(2);
        if row.get::<_, Option<i64>>(1) == Some(epoch)
            && row.get::<_, Option<&str>>(0).is_some_and(|v| v != claim)
        {
            return Err("composition busy".into());
        }
        tx.execute(
            "UPDATE composition_instances SET owner=$2,owner_epoch=$3 WHERE id=$1",
            &[&id, &claim, &epoch],
        )
        .map_err(db_error)?;
        // Interrupted assignments retain the same run identity. The native leaf
        // checkpoint owner classifies escaped invocation intents before replay.
        tx.execute(
            "UPDATE composition_attempts SET state='queued',queued_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint,started_ms=NULL WHERE instance=$1 AND state='running'",
            &[&id],
        )
        .map_err(db_error)?;
        event(tx, id, None, "claimed", "running", None)?;
        return instance(tx, id);
    }
    if operation == "composition.cancel" {
        let id = text(p, "instance")?;
        let saved = instance(tx, id)?;
        if saved["principal"] != p["principal"] {
            return Err("composition not found".into());
        }
        if !terminal(saved["state"].as_str().unwrap_or("")) {
            tx.execute(
                "UPDATE composition_instances SET cancel_requested=true WHERE id=$1",
                &[&id],
            )
            .map_err(db_error)?;
            event(
                tx,
                id,
                None,
                "cancel_requested",
                saved["state"].as_str().unwrap_or("unknown"),
                None,
            )?;
        }
        return instance(tx, id);
    }
    if operation == "composition.extend_deadline" {
        let id = text(p, "instance")?;
        let saved = instance(tx, id)?;
        let command = &p["command"];
        if saved["principal"] != p["principal"] || command["owner"] != p["principal"] {
            return Err("composition not found".into());
        }
        if let Some(receipt) = shared_timing::amendment_replay(tx, id, command)? {
            return Ok(receipt);
        }
        if saved["cancel_requested"] == true
            || !matches!(
                saved["state"].as_str(),
                Some("queued" | "blocked_definition" | "input_required" | "auth_required")
            )
        {
            return Err("deadline extension unavailable".into());
        }
        let uncertain:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM composition_attempts WHERE instance=$1 AND state IN ('running','unknown')) OR EXISTS(SELECT 1 FROM composition_instances i CROSS JOIN authority a WHERE i.id=$1 AND i.owner IS NOT NULL AND i.owner_epoch=a.epoch)",&[&id]).map_err(db_error)?.get(0);
        if uncertain || integer(&saved, "deadline", 1)? <= now(tx)? {
            return Err("deadline extension unavailable".into());
        }
        for r in tx
            .query("SELECT path FROM service_clocks WHERE owner=$1", &[&id])
            .map_err(db_error)?
        {
            shared_timing::require_open(tx, id, r.get(0))?;
        }
        for r in tx
            .query(
                "SELECT deadline FROM composition_scopes WHERE instance=$1",
                &[&id],
            )
            .map_err(db_error)?
        {
            if r.get::<_, i64>(0) <= now(tx)? {
                return Err("scope expired".into());
            }
        }
        let path = command["path"].as_str().unwrap_or("");
        let receipt = shared_timing::amend(tx, id, path, command)?;
        let deadline = integer(&receipt, "business_expires_ms", 0)? / 1000;
        if path.is_empty() {
            tx.execute(
                "UPDATE composition_instances SET deadline=$2 WHERE id=$1",
                &[&id, &deadline],
            )
            .map_err(db_error)?;
            tx.execute(
                "UPDATE composition_scopes SET deadline=$2 WHERE instance=$1 AND path='root'",
                &[&id, &deadline],
            )
            .map_err(db_error)?;
        } else {
            tx.execute(
                "UPDATE composition_scopes SET deadline=$3 WHERE instance=$1 AND path=$2",
                &[&id, &path, &deadline],
            )
            .map_err(db_error)?;
        }
        event(
            tx,
            id,
            Some(path),
            "deadline_extended",
            saved["state"].as_str().unwrap(),
            Some(&receipt),
        )?;
        return Ok(receipt);
    }
    let id = owned(tx, p)?;
    let path = p["path"].as_str().unwrap_or("root");
    match operation {
        "composition.protocol" => {
            let run = text(p, "run_id")?;
            let supplied = &p["event"];
            let event_fields = supplied.as_object().ok_or("invalid protocol event")?;
            if event_fields.len() != 6
                || [
                    "run_id",
                    "sender",
                    "recipient",
                    "kind",
                    "message_id",
                    "task_id",
                ]
                .iter()
                .any(|k| {
                    supplied[k]
                        .as_str()
                        .is_none_or(|v| v.is_empty() || v.len() > 256)
                })
                || supplied["run_id"] != run
            {
                return Err("invalid protocol event".into());
            }
            let row=tx.query_opt("SELECT path,revision FROM composition_attempts WHERE instance=$1 AND run_id=$2",&[&id,&run]).map_err(db_error)?.ok_or("unknown composition run")?;
            let path = row.get::<_, &str>(0);
            let revision: i64 = row.get(1);
            let message = text(supplied, "message_id")?;
            let envelope = tx
                .query_opt(
                    "SELECT content FROM messages WHERE run=$1 AND id=$2",
                    &[&run, &message],
                )
                .map_err(db_error)?
                .ok_or("unaccepted protocol event")?;
            let envelope = decoded(envelope.get(0))?;
            if ["run_id", "sender", "recipient", "kind", "message_id"]
                .iter()
                .any(|k| envelope[k] != supplied[k])
                || envelope["payload"]["task_id"] != supplied["task_id"]
            {
                return Err("protocol event conflict".into());
            }
            let mut data = supplied.clone();
            data["revision"] = json!(revision);
            if let Some(saved)=tx.query_opt("SELECT data FROM composition_protocol_events WHERE run_id=$1 AND message_id=$2",&[&run,&message]).map_err(db_error)?{if decoded(saved.get(0))?!=data{return Err("protocol event conflict".into())}return Ok(json!({"published":true}))}
            tx.execute(
                "INSERT INTO composition_protocol_events VALUES($1,$2,$3)",
                &[&run, &message, &data.to_string()],
            )
            .map_err(db_error)?;
            event(tx, &id, Some(path), "protocol", "running", Some(&data))?;
            Ok(json!({"published":true}))
        }
        "composition.release" => {
            tx.execute(
                "UPDATE composition_instances SET owner=NULL,owner_epoch=NULL WHERE id=$1",
                &[&id],
            )
            .map_err(db_error)?;
            Ok(json!({"released":true}))
        }
        "composition.node" => {
            let saved = node(tx, &id, path)?;
            if !saved.is_null() {
                if saved["kind"] != p["kind"] || saved["input"] != p["input"] {
                    return Err("composition node conflict".into());
                }
                return Ok(saved);
            }
            if obsolete(tx, &id, path)? {
                return Err("obsolete composition path".into());
            }
            let kind = text(p, "kind")?;
            let count: i64 = tx
                .query_one(
                    "SELECT nodes FROM composition_statistics WHERE instance=$1",
                    &[&id],
                )
                .map_err(db_error)?
                .get(0);
            let item = instance(tx, &id)?;
            if count >= integer(&item["limits"], "max_steps", 1)? {
                return Err("composition step limit".into());
            }
            tx.execute("INSERT INTO composition_nodes(instance,path,kind,input,state) VALUES($1,$2,$3,$4,'waiting')",&[&id,&path,&kind,&p["input"].to_string()]).map_err(db_error)?;
            tx.execute(
                "UPDATE composition_statistics SET nodes=nodes+1 WHERE instance=$1",
                &[&id],
            )
            .map_err(db_error)?;
            node(tx, &id, path)
        }
        "composition.finish_node" => {
            let state = text(p, "state")?;
            if !valid_state(state) {
                return Err("invalid node state".into());
            }
            let saved = node(tx, &id, path)?;
            if saved.is_null() {
                return Err("node not found".into());
            }
            if saved["state"] == "completed"
                && (state != "completed" || saved["output"] != p["output"])
            {
                return Err("completed node is immutable".into());
            }
            if saved["state"] != state
                || saved["output"] != p["output"]
                || saved["error"] != p["error"]
            {
                tx.execute("UPDATE composition_nodes SET state=$3,output=$4,error=$5 WHERE instance=$1 AND path=$2",&[&id,&path,&state,&p["output"].to_string(),&p["error"].to_string()]).map_err(db_error)?;
                if state == "completed" && saved["state"] != "completed" {
                    tx.execute(
                        "UPDATE composition_statistics SET completed=completed+1 WHERE instance=$1",
                        &[&id],
                    )
                    .map_err(db_error)?;
                }
                event(tx, &id, Some(path), "node_state", state, None)?;
            }
            let running:i64=tx.query_one("SELECT count(*) FROM composition_attempts WHERE instance=$1 AND state='running' AND (path=$2 OR starts_with(path,$2||'/'))",&[&id,&path]).map_err(db_error)?.get(0);
            if running == 0
                && !shared_timing::read(tx, &id, path)?.is_null()
                && (terminal(state)
                    || matches!(state, "unknown" | "input_required" | "auth_required"))
            {
                shared_timing::transition(
                    tx,
                    &id,
                    path,
                    if terminal(state) {
                        "terminal"
                    } else {
                        "paused"
                    },
                    0,
                )?;
            }
            node(tx, &id, path)
        }
        "composition.scope" => {
            let limits = &p["limits"];
            if let Some(r) = tx
                .query_opt(
                    "SELECT limits,deadline FROM composition_scopes WHERE instance=$1 AND path=$2",
                    &[&id, &path],
                )
                .map_err(db_error)?
            {
                if decoded(r.get(0))? != *limits {
                    return Err("scope conflict".into());
                }
                return Ok(json!(r.get::<_, i64>(1)));
            }
            let clock = shared_timing::start(tx, &id, path, &p["timing_policy"])?;
            let seconds = if clock.is_null() {
                integer(limits, "wall_seconds", 1)?
            } else {
                integer(&clock["policy"], "business_seconds", 1)?
            };
            let deadline = integer(p, "deadline", 1)?
                .min(now(tx)?.checked_add(seconds).ok_or("deadline overflow")?);
            scope(tx, &id, path, text(p, "scope_id")?, limits, deadline)?;
            Ok(json!(deadline))
        }
        "composition.attempt" => {
            let saved = attempt(tx, &id, path)?;
            if !saved.is_null() {
                if saved["workflow"] != p["workflow"] || saved["identity"] != p["identity"] {
                    return Err("attempt definition conflict".into());
                }
                return Ok(saved);
            }
            let item = instance(tx, &id)?;
            if item["cancel_requested"] == true
                || terminal(item["state"].as_str().unwrap_or(""))
                || obsolete(tx, &id, path)?
            {
                return Err("composition dispatch closed".into());
            }
            let deadline = integer(p, "deadline", 1)?.min(integer(&item, "deadline", 1)?);
            let wall = integer(p, "wall_seconds", 1)?.min(deadline - now(tx)?);
            if wall <= 0 {
                return Ok(Value::Null);
            }
            let lineage = ancestors(tx, &id, path)?;
            tx.execute("INSERT INTO composition_attempts(instance,path,revision,workflow,identity,run_id,input,wall_seconds,ancestors,state) VALUES($1,$2,0,$3,$4,$5,$6,$7,$8,'queued')",&[&id,&path,&text(p,"workflow")?,&text(p,"identity")?,&text(p,"run_id")?,&p["input"].to_string(),&wall,&lineage.to_string()]).map_err(db_error)?;
            event(tx, &id, Some(path), "attempt_allocated", "queued", None)?;
            attempt(tx, &id, path)
        }
        "composition.start_attempt" => {
            require_path_open(tx, &id, path)?;
            let saved = attempt(tx, &id, path)?;
            let item = instance(tx, &id)?;
            for r in tx
                .query(
                    "SELECT path,limits FROM composition_scopes WHERE instance=$1",
                    &[&id],
                )
                .map_err(db_error)?
            {
                let prefix = r.get::<_, &str>(0);
                if path == prefix || path.starts_with(&format!("{prefix}/")) {
                    let count:i64=tx.query_one("SELECT count(*) FROM composition_attempts WHERE instance=$1 AND state='running' AND (path=$2 OR starts_with(path,$2||'/'))",&[&id,&prefix]).map_err(db_error)?.get(0);
                    if count >= integer(&decoded(r.get(1))?, "max_parallel", 1)? {
                        return Err("composition parallel capacity exhausted".into());
                    }
                }
            }
            if saved["state"] != "queued"
                || saved["run_id"] != p["run_id"]
                || item["cancel_requested"] == true
                || integer(&item, "deadline", 1)? <= now(tx)?
                || obsolete(tx, &id, path)?
            {
                return Err("composition admission closed".into());
            }
            for r in tx
                .query(
                    "SELECT path,deadline FROM composition_scopes WHERE instance=$1",
                    &[&id],
                )
                .map_err(db_error)?
            {
                let prefix = r.get::<_, &str>(0);
                if (path == prefix || path.starts_with(&format!("{prefix}/")))
                    && r.get::<_, i64>(1) <= now(tx)?
                {
                    return Err("composition scope expired".into());
                }
            }
            if !crate::shared_tasks::acquire_slot(
                tx,
                text(&saved, "run_id")?,
                text(&item, "principal")?,
                text(&saved, "workflow")?,
                text(&saved, "identity")?,
                text(p, "owner")?,
            )? {
                return Ok(Value::Null);
            }
            tx.execute("UPDATE composition_attempts SET state='running',started_ms=floor(extract(epoch FROM clock_timestamp())*1000)::bigint WHERE instance=$1 AND path=$2 AND revision=$3",&[&id,&path,&integer(&saved,"revision",0)?]).map_err(db_error)?;
            activity(tx, &id)?;
            event(tx, &id, Some(path), "attempt_started", "running", None)?;
            attempt(tx, &id, path)
        }
        "composition.save_attempt" => {
            let saved = attempt(tx, &id, path)?;
            let state = text(p, "state")?;
            if !valid_state(state)
                || saved["run_id"] != p["run_id"]
                || !matches!(
                    saved["state"].as_str(),
                    Some("running" | "queued" | "unknown")
                )
            {
                return Err("attempt transition conflict".into());
            }
            tx.execute("UPDATE composition_attempts SET state=$4,output=$5,error=$6 WHERE instance=$1 AND path=$2 AND revision=$3",&[&id,&path,&integer(&saved,"revision",0)?,&state,&p["output"].to_string(),&p["error"].to_string()]).map_err(db_error)?;
            crate::shared_tasks::release_slot(tx, text(&saved, "run_id")?, text(p, "owner")?)?;
            activity(tx, &id)?;
            event(tx, &id, Some(path), "attempt_finished", state, None)?;
            attempt(tx, &id, path)
        }
        "composition.checkpoint" => {
            let kind = text(p, "kind")?;
            let prefix = match kind {
                "sequence" => "s",
                "reduce" => "r",
                "loop" | "while" => "i",
                _ => return Err("invalid control checkpoint".into()),
            };
            let next = integer(p, "next_index", 1)?;
            let consumed = integer(p, "consumed", 0)?;
            let saved = progress(tx, &id, path)?;
            if saved["kind"] == kind
                && saved["next_index"] == next
                && saved["output"] == p["output"]
                && saved["consumed"] == consumed
            {
                return Ok(saved);
            }
            if next != saved["next_index"].as_i64().unwrap_or(0) + 1
                || consumed < saved["consumed"].as_i64().unwrap_or(0)
            {
                return Err("progress conflict".into());
            }
            let child = node(tx, &id, &format!("{path}/{prefix}{}", next - 1))?;
            if node(tx, &id, path)?["kind"] != kind
                || child["state"] != "completed"
                || child["output"] != p["output"]
            {
                return Err("unverified progress".into());
            }
            tx.execute("INSERT INTO composition_progress(instance,path,kind,next_index,output,consumed) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(instance,path) DO UPDATE SET next_index=excluded.next_index,output=excluded.output,consumed=excluded.consumed",&[&id,&path,&kind,&next,&p["output"].to_string(),&consumed]).map_err(db_error)?;
            progress(tx, &id, path)
        }
        "composition.decide_join" => {
            let saved = join(tx, &id, path)?;
            if !p["winners"].is_array() || !p["obsolete"].is_array() {
                return Err("invalid join".into());
            }
            if !saved.is_null() {
                if saved["winners"] != p["winners"] || saved["obsolete"] != p["obsolete"] {
                    return Err("join conflict".into());
                }
                return Ok(saved);
            }
            for winner in p["winners"].as_array().unwrap() {
                let child = node(
                    tx,
                    &id,
                    &format!("{path}/p-{}", winner.as_str().ok_or("invalid winner")?),
                )?;
                if child["state"] != "completed" {
                    return Err("unverified join winner".into());
                }
            }
            tx.execute(
                "INSERT INTO composition_joins VALUES($1,$2,$3,$4)",
                &[
                    &id,
                    &path,
                    &p["winners"].to_string(),
                    &p["obsolete"].to_string(),
                ],
            )
            .map_err(db_error)?;
            event(
                tx,
                &id,
                Some(path),
                "join_decided",
                "waiting",
                Some(&json!({"winners":p["winners"],"obsolete":p["obsolete"]})),
            )?;
            join(tx, &id, path)
        }
        "composition.finish" => {
            let requested = text(p, "state")?;
            let state = requested;
            if !valid_state(state) {
                return Err("invalid composition state".into());
            }
            let saved = instance(tx, &id)?;
            if terminal(saved["state"].as_str().unwrap_or("")) && saved["state"] != state {
                return Err("terminal composition immutable".into());
            }
            let uncertain:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM composition_attempts WHERE instance=$1 AND state IN ('running','unknown'))",&[&id]).map_err(db_error)?.get(0);
            let close = !terminal(saved["state"].as_str().unwrap_or(""))
                && (saved["cancel_requested"] == true || expired(tx, &id)?);
            let state = if close {
                if uncertain {
                    "unknown"
                } else if saved["cancel_requested"] == true {
                    "cancelled"
                } else {
                    "expired"
                }
            } else {
                state
            };
            if terminal(state) && uncertain {
                return Err("uncertain attempts cannot be concealed".into());
            }
            let output = if state == requested {
                p["output"].clone()
            } else {
                Value::Null
            };
            let error = if state == requested {
                p["error"].clone()
            } else {
                json!({"code":if uncertain{"reconciliation_required"}else{"deadline_exceeded"}})
            };
            tx.execute(
                "UPDATE composition_instances SET state=$2,output=$3,error=$4 WHERE id=$1",
                &[&id, &state, &output.to_string(), &error.to_string()],
            )
            .map_err(db_error)?;
            shared_timing::transition(
                tx,
                &id,
                "",
                if terminal(state) {
                    "terminal"
                } else if matches!(state, "unknown" | "input_required" | "auth_required") {
                    "paused"
                } else {
                    "queued"
                },
                0,
            )?;
            event(tx, &id, None, "state", state, None)?;
            instance(tx, &id)
        }
        "composition.recover" => {
            let item = instance(tx, &id)?;
            if item["state"] != "unknown" {
                return Err("composition recovery unavailable".into());
            }
            let closed = if item["cancel_requested"] == true {
                Some("cancelled")
            } else if expired(tx, &id)? {
                Some("expired")
            } else {
                None
            };
            let rows=tx.query("SELECT path,run_id FROM composition_attempts WHERE instance=$1 AND state='unknown'",&[&id]).map_err(db_error)?;
            for row in rows {
                let path = row.get::<_, &str>(0);
                let run = row.get::<_, &str>(1);
                let unsafe_effect:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM host_invocations WHERE run=$1 AND state IN ('executing','unknown'))",&[&run]).map_err(db_error)?.get(0);
                if unsafe_effect {
                    return Err("shared effect resolution required".into());
                }
                if closed.is_none() {
                    require_path_open(tx, &id, path)?;
                }
                tx.execute("UPDATE composition_attempts SET state=$3,error=NULL WHERE instance=$1 AND path=$2 AND state='unknown'",&[&id,&path,&closed.unwrap_or("queued")]).map_err(db_error)?;
            }
            let state = closed.unwrap_or("queued");
            tx.execute(
                "UPDATE composition_instances SET state=$2,error=NULL WHERE id=$1",
                &[&id, &state],
            )
            .map_err(db_error)?;
            shared_timing::transition(
                tx,
                &id,
                "",
                if closed.is_some() {
                    "terminal"
                } else {
                    "queued"
                },
                0,
            )?;
            event(tx, &id, None, "reconciled", state, None)?;
            instance(tx, &id)
        }
        "composition.continue" => {
            let key = text(p, "request_key")?;
            let digest = text(p, "digest")?;
            let parent = instance(tx, &id)?;
            let principal = text(&parent, "principal")?;
            let ns = text(&parent, "request_namespace")?;
            let ep = parent["request_epoch"].as_str().unwrap();
            if let Some(target) =
                crate::shared_tasks::request(tx, principal, ns, ep, key, digest, "composition")?
            {
                if target != id {
                    return Err("continuation request conflict".into());
                }
            }
            if let Some(r)=tx.query_opt("SELECT digest,receipt FROM composition_continuations WHERE instance=$1 AND request_key=$2",&[&id,&key]).map_err(db_error)?{if r.get::<_,&str>(0)!=digest{return Err("continuation conflict".into())}return decoded(r.get(1))}
            let saved = attempt(tx, &id, path)?;
            let item = instance(tx, &id)?;
            if !matches!(
                saved["state"].as_str(),
                Some("input_required" | "auth_required")
            ) || item["cancel_requested"] == true
                || integer(&item, "deadline", 1)? <= now(tx)?
            {
                return Err("continuation unavailable".into());
            }
            require_path_open(tx, &id, path)?;
            shared_timing::transition(tx, &id, "", "queued", 0)?;
            let revision = integer(&saved, "revision", 0)? + 1;
            if revision >= integer(&item["limits"], "max_steps", 1)? {
                return Err("continuation revision limit".into());
            }
            tx.execute(
                "UPDATE composition_attempts SET latest=false WHERE instance=$1 AND path=$2",
                &[&id, &path],
            )
            .map_err(db_error)?;
            tx.execute("UPDATE composition_statistics SET revision=greatest(revision,$2) WHERE instance=$1",&[&id,&revision]).map_err(db_error)?;
            tx.execute("INSERT INTO composition_attempts(instance,path,revision,workflow,identity,run_id,input,wall_seconds,ancestors,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'queued')",&[&id,&path,&revision,&text(&saved,"workflow")?,&text(&saved,"identity")?,&text(p,"run_id")?,&p["input"].to_string(),&integer(&saved,"wall_seconds",1)?,&saved["ancestors"].to_string()]).map_err(db_error)?;
            tx.execute("UPDATE composition_nodes SET state='waiting',output=NULL,error=NULL WHERE instance=$1 AND (path=$2 OR starts_with($2,path||'/'))",&[&id,&path]).map_err(db_error)?;
            tx.execute("UPDATE composition_instances SET state='queued',output=NULL,error=NULL WHERE id=$1",&[&id]).map_err(db_error)?;
            let receipt = attempt(tx, &id, path)?;
            crate::shared_tasks::remember_request(
                tx,
                principal,
                ns,
                ep,
                key,
                digest,
                "composition",
                &id,
            )?;
            tx.execute(
                "INSERT INTO composition_continuations VALUES($1,$2,$3,$4)",
                &[&id, &key, &digest, &receipt.to_string()],
            )
            .map_err(db_error)?;
            event(tx, &id, Some(path), "continued", "queued", None)?;
            Ok(receipt)
        }
        _ => Err("unknown composition command".into()),
    }
}
// Read selection avoids durable no-op receipts. start_attempt independently
// checks the same assignment/capacity in its mutating admission transaction.
fn slot_available(tx:&mut Transaction<'_>, item:&Value, attempt:&Value, owner:Option<&str>)->Result<bool> {
    let epoch:i64=tx.query_one("SELECT epoch FROM authority WHERE singleton",&[]).map_err(db_error)?.get(0);
    if let Some(row)=tx.query_opt("SELECT principal,workflow,identity,owner,epoch FROM service_slots WHERE assignment=$1",&[&text(attempt,"run_id")?]).map_err(db_error)? {
        if row.get::<_,&str>(0)!=text(item,"principal")? || row.get::<_,&str>(1)!=text(attempt,"workflow")? || row.get::<_,&str>(2)!=text(attempt,"identity")? {return Err("scheduler_assignment_conflict".into())}
        return Ok(row.get::<_,i64>(4)!=epoch || owner==Some(row.get::<_,&str>(3)))
    }
    Ok(crate::shared_tasks::available_resources(tx,text(item,"principal")?,text(attempt,"workflow")?,text(attempt,"identity")?)?.is_some())
}
pub(crate) fn query(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    if operation == "composition.expiring" {
        let after=p["after"].as_str();
        let instance=p["instance"].as_str();
        let rows=tx.query("SELECT id FROM composition_instances WHERE (owner IS NULL OR owner_epoch<>(SELECT epoch FROM authority WHERE singleton)) AND state IN ('queued','waiting','blocked_definition','input_required','auth_required') AND ($1::text IS NULL OR id>$1) AND ($2::text IS NULL OR id=$2) ORDER BY id LIMIT 1001",&[&after,&instance]).map_err(db_error)?;
        let mut candidates=Vec::new();
        for row in rows.iter().take(1000) { let id=row.get::<_,&str>(0); if expired(tx,id)? { candidates.push(id.to_owned()); } }
        let next=if rows.len()>1000 { json!(rows[999].get::<_,&str>(0)) } else { Value::Null };
        return Ok(json!({"instances":candidates,"next":next}));
    }
    if operation == "composition.pending" {
        let after=p["after"].as_str();
        let rows=tx.query("SELECT i.id,i.principal,i.created,i.plan FROM composition_instances i CROSS JOIN authority a WHERE i.state IN ('queued','running','waiting','blocked_definition') AND (i.owner IS NULL OR i.owner_epoch<>a.epoch) AND ($1::text IS NULL OR i.id>$1) ORDER BY i.id LIMIT 1001",&[&after]).map_err(db_error)?;
        return Ok(json!({"items":rows.iter().take(1000).map(|r|json!({"id":r.get::<_,&str>(0),"principal":r.get::<_,&str>(1),"created":r.get::<_,i64>(2),"plan":r.get::<_,&str>(3)})).collect::<Vec<_>>(),"next":if rows.len()>1000{json!(rows[999].get::<_,&str>(0))}else{Value::Null}}));
    }
    if operation == "composition.list" {
        let principal = text(p, "principal")?;
        let limit = integer(p, "limit", 1)?.min(1000);
        let names = p["workflows"]
            .as_array()
            .ok_or("invalid workflows")?
            .iter()
            .map(|v| v.as_str().map(str::to_owned).ok_or("invalid workflow"))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let state = p["state"].as_str();
        let updated = p["updated_after"].as_i64();
        let after_time = p["after"][0].as_i64();
        let after_id = p["after"][1].as_str();
        let metadata = p["metadata"].to_string();
        let filter=" WHERE principal=$1 AND plan=ANY($2) AND ($3::text IS NULL OR state=$3 OR ($3='failed' AND state='expired')) AND ($4::bigint IS NULL OR updated>=$4) AND metadata::jsonb @> $5::text::jsonb";
        let count: i64 = tx
            .query_one(
                &("SELECT count(*) FROM composition_instances".to_owned() + filter),
                &[&principal, &names, &state, &updated, &metadata],
            )
            .map_err(db_error)?
            .get(0);
        let rows=tx.query(&("SELECT id FROM composition_instances".to_owned()+filter+" AND ($6::bigint IS NULL OR updated<$6 OR (updated=$6 AND id<$7)) ORDER BY updated DESC,id DESC LIMIT $8"),&[&principal,&names,&state,&updated,&metadata,&after_time,&after_id,&(limit+1)]).map_err(db_error)?;
        let more = rows.len() > limit as usize;
        let mut out = Vec::new();
        for r in rows.iter().take(limit as usize) {
            out.push(instance(tx, r.get(0))?)
        }
        let next = if more {
            let last = out.last().unwrap();
            json!([last["updated"], last["id"]])
        } else {
            Value::Null
        };
        return Ok(json!({"tasks":out,"total_size":count,"next":next}));
    }
    let id = text(p, "instance")?;
    let item = instance(tx, id)?;
    if !p["principal"].is_null() && p["principal"] != item["principal"] {
        return Err("composition not found".into());
    }
    let path = p["path"].as_str().unwrap_or("root");
    match operation {
        "composition.instance" => Ok(item),
        "composition.claimable" => {
            if terminal(item["state"].as_str().unwrap_or("")) || matches!(item["state"].as_str(),Some("unknown"|"input_required"|"auth_required")) {return Ok(json!(false))}
            let owned:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM composition_instances i CROSS JOIN authority a WHERE i.id=$1 AND i.owner IS NOT NULL AND i.owner_epoch=a.epoch)",&[&id]).map_err(db_error)?.get(0);
            if owned {return Ok(json!(false))}
            if item["cancel_requested"]==true || expired(tx,id)? {return Ok(json!(true))}
            let rows=tx.query("SELECT path FROM composition_attempts WHERE instance=$1 AND latest AND state IN ('queued','running') ORDER BY path",&[&id]).map_err(db_error)?;
            if rows.is_empty(){return Ok(json!(true))}
            for row in rows {let saved=attempt(tx,id,row.get(0))?;if slot_available(tx,&item,&saved,None)? {return Ok(json!(true))}}
            Ok(json!(false))
        }
        "composition.attempt_ready" => {
            let saved=attempt(tx,id,path)?;
            if saved.is_null() || saved["state"]!="queued" {return Ok(json!(false))}
            Ok(json!(slot_available(tx,&item,&saved,p["owner"].as_str())?))
        },
        "composition.cancelled" => {
            let root = node(tx, id, "root")?;
            Ok(json!(
                obsolete(tx, id, path)?
                    || matches!(
                        root["state"].as_str(),
                        Some("failed" | "unknown" | "expired" | "cancelled")
                    )
            ))
        }
        "composition.node" => node(tx, id, path),
        "composition.clock" => shared_timing::read(tx,id,path),
        "composition.attempt" => attempt_at(tx, id, path, p["revision"].as_i64()),
        "composition.progress" => progress(tx, id, path),
        "composition.join" => join(tx, id, path),
        "composition.scope" => tx.query_opt("SELECT limits,deadline FROM composition_scopes WHERE instance=$1 AND path=$2",&[&id,&path]).map_err(db_error)?.map(|r|Ok(json!({"limits":decoded(r.get(0))?,"deadline":r.get::<_,i64>(1)}))).unwrap_or(Ok(Value::Null)),
        "composition.scopes" => {
            let mut out = Vec::new();
            for r in tx.query("SELECT s.path,s.id,s.limits,s.deadline,n.state FROM composition_scopes s LEFT JOIN composition_nodes n ON n.instance=s.instance AND n.path=s.path WHERE s.instance=$1 ORDER BY s.path",&[&id]).map_err(db_error)?{out.push(json!({"path":r.get::<_,&str>(0),"id":r.get::<_,&str>(1),"limits":decoded(r.get(2))?,"deadline":r.get::<_,i64>(3),"state":r.get::<_,Option<&str>>(4),"clock":shared_timing::read(tx,id,r.get(0))?,"timing":shared_timing::status(tx,id,r.get(0))?}))}
            Ok(json!(out))
        }
        "composition.runs" => {
            let rows=tx.query("SELECT path,revision,run_id,state FROM composition_attempts WHERE instance=$1 ORDER BY path,revision",&[&id]).map_err(db_error)?;
            Ok(json!(rows.iter().map(|r|json!({"path":r.get::<_,&str>(0),"revision":r.get::<_,i64>(1),"run_id":r.get::<_,&str>(2),"run_directory":r.get::<_,&str>(2),"state":r.get::<_,&str>(3)})).collect::<Vec<_>>()))
        }
        "composition.frontier" => {
            let mut frontier = item;
            let stats = tx
                .query_one(
                    "SELECT nodes,completed,revision FROM composition_statistics WHERE instance=$1",
                    &[&id],
                )
                .map_err(db_error)?;
            frontier["progress"] =
                json!({"nodes":stats.get::<_,i64>(0),"completed_nodes":stats.get::<_,i64>(1)});
            frontier["revision"] = json!(stats.get::<_, i64>(2));
            let rows=tx.query("SELECT path,revision,run_id,state FROM composition_attempts WHERE instance=$1 AND latest AND state IN ('queued','running','unknown','input_required','auth_required') ORDER BY path",&[&id]).map_err(db_error)?;
            frontier["attempts"]=json!(rows.iter().map(|r|json!({"path":r.get::<_,&str>(0),"revision":r.get::<_,i64>(1),"run_id":r.get::<_,&str>(2),"state":r.get::<_,&str>(3)})).collect::<Vec<_>>());
            Ok(frontier)
        }
        "composition.graph" => {
            let mut graph = item;
            let mut nodes = Vec::new();
            for r in tx
                .query(
                    "SELECT path FROM composition_nodes WHERE instance=$1 ORDER BY path",
                    &[&id],
                )
                .map_err(db_error)?
            {
                nodes.push(node(tx, id, r.get(0))?)
            }
            let mut attempts = Vec::new();
            for r in tx.query("SELECT DISTINCT path FROM composition_attempts WHERE instance=$1 ORDER BY path",&[&id]).map_err(db_error)?{attempts.push(attempt(tx,id,r.get(0))?)}
            graph["nodes"] = json!(nodes);
            graph["attempts"] = json!(attempts);
            Ok(graph)
        }
        "composition.events" => {
            let after = integer(p, "after", 0)?;
            if item["archived"]==true && after<item["event_floor"].as_i64().unwrap_or(0) {return Err("event_cursor_expired".into())}
            let limit = integer(p, "limit", 1)?.min(1000);
            let mut out = Vec::new();
            for r in tx.query("SELECT id,path,event,state,data,created FROM composition_events WHERE instance=$1 AND id>$2 ORDER BY id LIMIT $3",&[&id,&after,&limit]).map_err(db_error)?{out.push(json!({"id":r.get::<_,i64>(0),"path":r.get::<_,Option<&str>>(1),"event":r.get::<_,&str>(2),"state":r.get::<_,&str>(3),"data":optional(r.get(4))?,"created":r.get::<_,i64>(5)}))}
            Ok(json!(out))
        }
        _ => Err("unknown composition query".into()),
    }
}

/// Compact settled payloads while retaining identity, allocation and run references.
/// The operator repository owns the permanent request-key receipt around this call.
pub(crate) fn archive(tx: &mut Transaction<'_>, p: &Value) -> Result<Value> {
    let id=text(p,"instance")?;
    let principal=text(p,"principal")?;
    let saved=instance(tx,id)?;
    if saved["principal"]!=principal {return Err("not_found".into())}
    if saved["archived"]==true {return Ok(saved)}
    let age=integer(p,"older_than_seconds",0)?;
    let current=shared_timing::now(tx)?;
    let cutoff=current.saturating_sub(age.checked_mul(1000).ok_or("invalid_archive_age")?);
    if !terminal(saved["state"].as_str().unwrap_or("")) || saved["updated"].as_i64().unwrap_or(current)>cutoff {
        return Err("workflow_not_archivable".into())
    }
    let owned:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM composition_instances i CROSS JOIN authority a WHERE i.id=$1 AND i.owner IS NOT NULL AND i.owner_epoch=a.epoch)",&[&id]).map_err(db_error)?.get(0);
    let attempts=tx.query("SELECT run_id,state,latest FROM composition_attempts WHERE instance=$1",&[&id]).map_err(db_error)?;
    let runs:Vec<String>=attempts.iter().map(|r|r.get(0)).collect();
    if owned || crate::shared_artifacts::retained_for(tx,"workflow",id,&runs)? {return Err("workflow_not_archivable".into())}
    for row in attempts {
        let state=row.get::<_,&str>(1);
        let superseded_pause=!row.get::<_,bool>(2)&&matches!(state,"input_required"|"auth_required");
        if (!terminal(state)&&!superseded_pause) || !crate::shared_control::settled_run(tx,row.get(0))? {return Err("workflow_not_archivable".into())}
    }
    let floor:i64=tx.query_one("SELECT coalesce(max(id),0) FROM composition_events WHERE instance=$1",&[&id]).map_err(db_error)?.get(0);
    let digest=|value:&Value|->String{Sha256::digest(value.to_string().as_bytes()).iter().map(|b|format!("{b:02x}")).collect()};
    let input_digest=digest(&saved["input"]);
    let output_digest=if saved["output"].is_null(){None}else{Some(digest(&saved["output"]))};
    let error_digest=if saved["error"].is_null(){None}else{Some(digest(&saved["error"]))};
    let archived_error=error_digest.as_ref().map(|digest|json!({"archived":true,"sha256":digest}).to_string());
    // Compact payloads, never allocation counters, request digests, clocks,
    // amendment audit or the original run and definition identities.
    tx.execute("UPDATE composition_instances SET input='{}',output=NULL,error=$6,binding='{}',archived_at=$2,input_digest=$3,output_digest=$4,event_floor=$5,owner=NULL,owner_epoch=NULL WHERE id=$1",&[&id,&current,&input_digest,&output_digest,&floor,&archived_error]).map_err(db_error)?;
    for row in tx.query("SELECT path,revision,input,output,error FROM composition_attempts WHERE instance=$1",&[&id]).map_err(db_error)? {
        let input_hash=digest(&decoded(row.get(2))?);
        let output=optional(row.get(3))?;let error=optional(row.get(4))?;
        let output_hash=if output.is_null(){None}else{Some(digest(&output))};
        let error_hash=if error.is_null(){None}else{Some(digest(&error))};
        tx.execute("UPDATE composition_attempts SET input='{}',output=NULL,error=NULL,input_digest=$4,output_digest=$5,error_digest=$6 WHERE instance=$1 AND path=$2 AND revision=$3",&[&id,&row.get::<_,&str>(0),&row.get::<_,i64>(1),&input_hash,&output_hash,&error_hash]).map_err(db_error)?;
    }
    for row in tx.query("SELECT request_key,receipt FROM composition_continuations WHERE instance=$1",&[&id]).map_err(db_error)? {
        let receipt=decoded(row.get(1))?;
        let tombstone=json!({"format":"mas-archived-continuation/1","archived":true,"instance":id,"path":receipt["path"],"revision":receipt["revision"],"run_id":receipt["run_id"],"receipt_sha256":digest(&receipt)});
        tx.execute("UPDATE composition_continuations SET receipt=$3 WHERE instance=$1 AND request_key=$2",&[&id,&row.get::<_,&str>(0),&tombstone.to_string()]).map_err(db_error)?;
    }
    tx.execute("DELETE FROM composition_protocol_events WHERE run_id IN (SELECT run_id FROM composition_attempts WHERE instance=$1)",&[&id]).map_err(db_error)?;
    for table in ["composition_nodes","composition_progress","composition_joins","composition_events"] {
        tx.execute(&format!("DELETE FROM {table} WHERE instance=$1"),&[&id]).map_err(db_error)?;
    }
    let audit=json!({"operator":text(p,"operator")?,"reason":text(p,"reason")?,"request_key":text(p,"request_key")?,"input_digest":input_digest,"output_digest":output_digest,"error_digest":error_digest,"event_floor":floor,"allocation_changed":false});
    event(tx,id,None,"archived",saved["state"].as_str().unwrap(),Some(&audit))?;
    instance(tx,id)
}

/// Definition retention follows every frozen dependency, including nested plans.
pub(crate) fn definition_in_use(
    tx: &mut Transaction<'_>,
    kind: &str,
    name: &str,
    identity: &str,
) -> Result<bool> {
    for r in tx.query("SELECT plan,identity,binding FROM composition_instances i WHERE state NOT IN ('completed','failed','cancelled','expired') OR EXISTS(SELECT 1 FROM composition_attempts a WHERE a.instance=i.id AND a.state IN ('running','unknown'))",&[]).map_err(db_error)? {
        if kind=="plan"&&r.get::<_,&str>(0)==name&&r.get::<_,&str>(1)==identity{return Ok(true)}
        let binding=decoded(r.get(2))?;let section=match kind{"workflow"=>"workflows","plan_source"=>"plans",_=>continue};
        if binding["binding"][section][name]==identity{return Ok(true)}
    }
    Ok(false)
}

/// The final effect-admission transaction rechecks every enclosing deadline and
/// cancellation intent. Scheduler polling is only a promptness optimisation.
pub(crate) fn admission_open(tx: &mut Transaction<'_>, run_id: &str) -> Result<()> {
    let row=tx.query_opt("SELECT a.instance,a.path,a.state,i.cancel_requested,i.deadline,i.owner,i.owner_epoch,z.epoch FROM composition_attempts a JOIN composition_instances i ON i.id=a.instance CROSS JOIN authority z WHERE a.run_id=$1 AND z.singleton",&[&run_id]).map_err(db_error)?;
    let Some(row) = row else { return Ok(()) };
    let id = row.get::<_, &str>(0);
    let path = row.get::<_, &str>(1);
    if row.get::<_, &str>(2) != "running"
        || row.get::<_, bool>(3)
        || row.get::<_, i64>(4) <= now(tx)?
        || row.get::<_, Option<&str>>(5).is_none()
        || row.get::<_, Option<i64>>(6) != Some(row.get(7))
        || obsolete(tx, id, path)?
    {
        return Err("composition effect admission closed".into());
    }
    let root = node(tx, id, "root")?;
    if matches!(
        root["state"].as_str(),
        Some("failed" | "unknown" | "expired" | "cancelled" | "input_required" | "auth_required")
    ) {
        return Err("composition control admission closed".into());
    }
    require_path_open(tx, id, path)?;
    for row in tx
        .query(
            "SELECT path,deadline FROM composition_scopes WHERE instance=$1",
            &[&id],
        )
        .map_err(db_error)?
    {
        let prefix = row.get::<_, &str>(0);
        if (path == prefix || path.starts_with(&format!("{prefix}/")))
            && row.get::<_, i64>(1) <= now(tx)?
        {
            return Err("composition ancestor expired".into());
        }
    }
    Ok(())
}
