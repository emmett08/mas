//! Transactional composition graph and ready frontier in the shared authority.
//! No local journal or translated SQLite database participates in ownership.
use crate::{postgres_repository::db_error, shared_timing};
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;

pub(crate) const DDL: &str = "
CREATE TABLE composition_instances(id text PRIMARY KEY,principal text NOT NULL,request_namespace text NOT NULL,request_epoch text NOT NULL,request_key text NOT NULL,plan text NOT NULL,identity text NOT NULL,input text NOT NULL,metadata text NOT NULL,binding text NOT NULL,limits text NOT NULL,state text NOT NULL,output text,error text,created bigint NOT NULL,updated bigint NOT NULL,deadline bigint NOT NULL,cancel_requested boolean NOT NULL DEFAULT false,owner text,owner_epoch bigint,UNIQUE(principal,request_namespace,request_epoch,request_key));
CREATE TABLE composition_nodes(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,kind text NOT NULL,input text NOT NULL,state text NOT NULL,output text,error text,PRIMARY KEY(instance,path));
CREATE TABLE composition_attempts(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,revision bigint NOT NULL,workflow text NOT NULL,identity text NOT NULL,run_id text UNIQUE NOT NULL,input text NOT NULL,wall_seconds bigint NOT NULL,ancestors text NOT NULL,state text NOT NULL,output text,error text,PRIMARY KEY(instance,path,revision));
CREATE INDEX composition_ready ON composition_attempts(instance,state,path);
CREATE TABLE composition_scopes(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,id text UNIQUE NOT NULL,limits text NOT NULL,deadline bigint NOT NULL,PRIMARY KEY(instance,path));
CREATE TABLE composition_progress(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,kind text NOT NULL,next_index bigint NOT NULL,output text NOT NULL,consumed bigint NOT NULL,PRIMARY KEY(instance,path));
CREATE TABLE composition_joins(instance text NOT NULL REFERENCES composition_instances(id),path text NOT NULL,winners text NOT NULL,obsolete text NOT NULL,PRIMARY KEY(instance,path));
CREATE TABLE composition_continuations(instance text NOT NULL REFERENCES composition_instances(id),request_key text NOT NULL,digest text NOT NULL,receipt text NOT NULL,PRIMARY KEY(instance,request_key));
CREATE TABLE composition_events(id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,instance text NOT NULL REFERENCES composition_instances(id),path text,event text NOT NULL,state text NOT NULL,data text,created bigint NOT NULL);
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
    let r=tx.query_opt("SELECT id,principal,request_namespace,request_epoch,request_key,plan,identity,input,metadata,binding,limits,state,output,error,created,deadline,cancel_requested,updated FROM composition_instances WHERE id=$1",&[&id]).map_err(db_error)?.ok_or("composition not found")?;
    Ok(
        json!({"id":r.get::<_,&str>(0),"principal":r.get::<_,&str>(1),"request_namespace":r.get::<_,&str>(2),"request_epoch":r.get::<_,&str>(3),"request_key":r.get::<_,&str>(4),"plan":r.get::<_,&str>(5),"identity":r.get::<_,&str>(6),"input":decoded(r.get(7))?,"metadata":decoded(r.get(8))?,"binding":decoded(r.get(9))?,"limits":decoded(r.get(10))?,"state":r.get::<_,&str>(11),"output":optional(r.get(12))?,"error":optional(r.get(13))?,"created":r.get::<_,i64>(14),"deadline":r.get::<_,i64>(15),"cancel_requested":r.get::<_,bool>(16),"updated":r.get::<_,i64>(17),"clock":shared_timing::read(tx,id,"")?,"timing":shared_timing::status(tx,id,"")?}),
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
    tx.query_opt("SELECT revision,workflow,identity,run_id,input,wall_seconds,ancestors,state,output,error FROM composition_attempts WHERE instance=$1 AND path=$2 ORDER BY revision DESC LIMIT 1",&[&id,&path]).map_err(db_error)?.map(|r|Ok(json!({"instance":id,"path":path,"revision":r.get::<_,i64>(0),"workflow":r.get::<_,&str>(1),"identity":r.get::<_,&str>(2),"run_id":r.get::<_,&str>(3),"run_directory":r.get::<_,&str>(3),"input":decoded(r.get(4))?,"wall_seconds":r.get::<_,i64>(5),"ancestors":decoded(r.get(6))?,"state":r.get::<_,&str>(7),"output":optional(r.get(8))?,"error":optional(r.get(9))?}))).unwrap_or(Ok(Value::Null))
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
pub(crate) fn apply(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
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
        if !p["input"].is_object() || !p["metadata"].is_object() || !p["binding"].is_object() {
            return Err("invalid composition submission".into());
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
            "UPDATE composition_attempts SET state='queued' WHERE instance=$1 AND state='running'",
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
        let uncertain:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM composition_attempts WHERE instance=$1 AND state IN ('running','unknown')) OR EXISTS(SELECT 1 FROM composition_instances WHERE id=$1 AND owner IS NOT NULL)",&[&id]).map_err(db_error)?.get(0);
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
                    "SELECT count(*) FROM composition_nodes WHERE instance=$1",
                    &[&id],
                )
                .map_err(db_error)?
                .get(0);
            let item = instance(tx, &id)?;
            if count >= integer(&item["limits"], "max_steps", 1)? {
                return Err("composition step limit".into());
            }
            tx.execute("INSERT INTO composition_nodes(instance,path,kind,input,state) VALUES($1,$2,$3,$4,'waiting')",&[&id,&path,&kind,&p["input"].to_string()]).map_err(db_error)?;
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
            tx.execute("UPDATE composition_attempts SET state='running' WHERE instance=$1 AND path=$2 AND revision=$3",&[&id,&path,&integer(&saved,"revision",0)?]).map_err(db_error)?;
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
            event(tx, &id, Some(path), "join_decided", "waiting", Some(p))?;
            join(tx, &id, path)
        }
        "composition.finish" => {
            let state = text(p, "state")?;
            if !valid_state(state) {
                return Err("invalid composition state".into());
            }
            let saved = instance(tx, &id)?;
            if terminal(saved["state"].as_str().unwrap_or("")) && saved["state"] != state {
                return Err("terminal composition immutable".into());
            }
            let uncertain:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM composition_attempts WHERE instance=$1 AND state IN ('running','unknown'))",&[&id]).map_err(db_error)?.get(0);
            if terminal(state) && uncertain {
                return Err("uncertain attempts cannot be concealed".into());
            }
            tx.execute(
                "UPDATE composition_instances SET state=$2,output=$3,error=$4 WHERE id=$1",
                &[
                    &id,
                    &state,
                    &p["output"].to_string(),
                    &p["error"].to_string(),
                ],
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
            shared_timing::require_open(tx, &id, "")?;
            let rows=tx.query("SELECT path,run_id FROM composition_attempts WHERE instance=$1 AND state='unknown'",&[&id]).map_err(db_error)?;
            for r in rows {
                let run = r.get::<_, &str>(1);
                let unsafe_effect:bool=tx.query_one("SELECT EXISTS(SELECT 1 FROM host_invocations WHERE run=$1 AND state IN ('executing','unknown'))",&[&run]).map_err(db_error)?.get(0);
                if unsafe_effect {
                    return Err("shared effect resolution required".into());
                }
                tx.execute("UPDATE composition_attempts SET state='queued',error=NULL WHERE instance=$1 AND path=$2 AND state='unknown'",&[&id,&r.get::<_,&str>(0)]).map_err(db_error)?;
            }
            tx.execute("UPDATE composition_instances SET state='queued',error=NULL WHERE id=$1 AND state='unknown'",&[&id]).map_err(db_error)?;
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
pub(crate) fn query(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    if operation == "composition.pending" {
        let rows=tx.query("SELECT id FROM composition_instances WHERE state IN ('queued','running','waiting') ORDER BY created,id LIMIT 1000",&[]).map_err(db_error)?;
        return Ok(json!(rows
            .iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<Vec<_>>()));
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
        "composition.attempt" => attempt(tx, id, path),
        "composition.progress" => progress(tx, id, path),
        "composition.join" => join(tx, id, path),
        "composition.scopes" => {
            let mut out = Vec::new();
            for r in tx.query("SELECT s.path,s.id,s.limits,s.deadline,n.state FROM composition_scopes s LEFT JOIN composition_nodes n ON n.instance=s.instance AND n.path=s.path WHERE s.instance=$1 ORDER BY s.path",&[&id]).map_err(db_error)?{out.push(json!({"path":r.get::<_,&str>(0),"id":r.get::<_,&str>(1),"limits":decoded(r.get(2))?,"deadline":r.get::<_,i64>(3),"state":r.get::<_,Option<&str>>(4),"clock":shared_timing::read(tx,id,r.get(0))?,"timing":shared_timing::status(tx,id,r.get(0))?}))}
            Ok(json!(out))
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
            let limit = integer(p, "limit", 1)?.min(1000);
            let mut out = Vec::new();
            for r in tx.query("SELECT id,path,event,state,data,created FROM composition_events WHERE instance=$1 AND id>$2 ORDER BY id LIMIT $3",&[&id,&after,&limit]).map_err(db_error)?{out.push(json!({"id":r.get::<_,i64>(0),"path":r.get::<_,Option<&str>>(1),"event":r.get::<_,&str>(2),"state":r.get::<_,&str>(3),"data":optional(r.get(4))?,"created":r.get::<_,i64>(5)}))}
            Ok(json!(out))
        }
        _ => Err("unknown composition query".into()),
    }
}

/// Definition retention follows every frozen dependency, including nested plans.
pub(crate) fn definition_in_use(
    tx: &mut Transaction<'_>,
    kind: &str,
    name: &str,
    identity: &str,
) -> Result<bool> {
    for r in tx.query("SELECT plan,identity,binding FROM composition_instances WHERE state NOT IN ('completed','failed','cancelled','expired')",&[]).map_err(db_error)? {
        if kind=="plan"&&r.get::<_,&str>(0)==name&&r.get::<_,&str>(1)==identity{return Ok(true)}
        let binding=decoded(r.get(2))?;let section=match kind{"workflow"=>"workflows","plan_source"=>"plans",_=>continue};
        if binding["binding"][section][name]==identity{return Ok(true)}
    }
    Ok(false)
}
