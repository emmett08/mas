//! Cursor Cloud intents and exact remote identities under shared authority.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;

pub(crate) const DDL: &str = "
CREATE TABLE cloud_jobs(principal text NOT NULL,job_id text NOT NULL,spec text NOT NULL,binding text NOT NULL,agent_id text NOT NULL UNIQUE,run_id text UNIQUE,state text NOT NULL CHECK(state IN ('prepared','launch_unknown','active','terminal')),snapshot text,host_run text REFERENCES host_runs(run),call_id text,cancel_requested boolean NOT NULL DEFAULT false,revision bigint NOT NULL DEFAULT 0,created bigint NOT NULL,updated bigint NOT NULL,PRIMARY KEY(principal,job_id),FOREIGN KEY(host_run,call_id) REFERENCES host_invocations(run,call_id));
CREATE TABLE cloud_operations(principal text NOT NULL,job_id text NOT NULL,operation_id text NOT NULL,action text NOT NULL,epoch bigint NOT NULL,revision bigint NOT NULL,request text NOT NULL,state text NOT NULL DEFAULT 'pending',PRIMARY KEY(principal,job_id,operation_id),FOREIGN KEY(principal,job_id) REFERENCES cloud_jobs(principal,job_id));
CREATE TABLE cloud_audit(seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,principal text NOT NULL,job_id text NOT NULL,event text NOT NULL,evidence text NOT NULL,created bigint NOT NULL,FOREIGN KEY(principal,job_id) REFERENCES cloud_jobs(principal,job_id));
";
pub(crate) const ARCHIVE_DDL: &str =
    "ALTER TABLE cloud_jobs ADD COLUMN archived boolean NOT NULL DEFAULT false;";

fn reject_archived(tx: &mut Transaction<'_>, principal: &str, job: &str) -> Result<()> {
    let version: i64 = tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if version >= 10
        && tx
            .query_opt(
                "SELECT archived FROM cloud_jobs WHERE principal=$1 AND job_id=$2",
                &[&principal, &job],
            )
            .map_err(db_error)?
            .is_some_and(|r| r.get::<_, bool>(0))
    {
        return Err("payload_archived".into());
    }
    Ok(())
}
fn archived_payload(value: &str) -> Value {
    json!({"format":"mas-archived-payload/1","sha256":Sha256::digest(value.as_bytes()).iter().map(|b|format!("{b:02x}")).collect::<String>()})
}
pub(crate) fn archive_run(tx: &mut Transaction<'_>, run: &str) -> Result<Value> {
    let mut saved = Vec::new();
    for r in tx.query("SELECT principal,job_id,spec,snapshot,agent_id,run_id,state,archived FROM cloud_jobs WHERE host_run=$1 ORDER BY principal,job_id",&[&run]).map_err(db_error)? {
        if r.get::<_,bool>(7) { continue }
        if r.get::<_,&str>(6)!="terminal" { return Err("run_not_settled".into()) }
        let principal:&str=r.get(0); let job:&str=r.get(1);
        let spec=archived_payload(r.get(2));
        let snapshot=r.get::<_,Option<&str>>(3).map(|s| {
            let mut retained=archived_payload(s);
            retained["remote_status"]=decoded(s)?["remote_status"].clone();
            Ok::<Value,String>(retained)
        }).transpose()?;
        tx.execute("UPDATE cloud_jobs SET spec=$3,snapshot=$4,archived=true WHERE principal=$1 AND job_id=$2",&[&principal,&job,&spec.to_string(),&snapshot.as_ref().map(Value::to_string)]).map_err(db_error)?;
        for operation in tx.query("SELECT operation_id,request FROM cloud_operations WHERE principal=$1 AND job_id=$2",&[&principal,&job]).map_err(db_error)? {
            tx.execute("UPDATE cloud_operations SET request=$4 WHERE principal=$1 AND job_id=$2 AND operation_id=$3",&[&principal,&job,&operation.get::<_,&str>(0),&archived_payload(operation.get(1)).to_string()]).map_err(db_error)?;
        }
        for event in tx.query("SELECT seq,evidence FROM cloud_audit WHERE principal=$1 AND job_id=$2",&[&principal,&job]).map_err(db_error)? {
            tx.execute("UPDATE cloud_audit SET evidence=$2 WHERE seq=$1",&[&event.get::<_,i64>(0),&archived_payload(event.get(1)).to_string()]).map_err(db_error)?;
        }
        saved.push(json!({"principal":principal,"job_id":job,"agent_id":r.get::<_,String>(4),"run_id":r.get::<_,Option<String>>(5),"spec":spec,"snapshot":snapshot}));
    }
    Ok(json!(saved))
}

fn text<'a>(p: &'a Value, key: &str) -> Result<&'a str> {
    p[key]
        .as_str()
        .filter(|s| !s.is_empty() && !s.contains('\0'))
        .ok_or_else(|| format!("invalid cloud {key}"))
}
fn id<'a>(p: &'a Value, key: &str) -> Result<&'a str> {
    let value = text(p, key)?;
    if !crate::identity(value) {
        return Err(format!("invalid cloud {key}"));
    }
    Ok(value)
}
fn fields(p: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let o = p.as_object().ok_or("invalid cloud command")?;
    if required.iter().any(|k| !o.contains_key(*k))
        || o.keys()
            .any(|k| !required.contains(&k.as_str()) && !optional.contains(&k.as_str()))
    {
        return Err("unsupported cloud command fields".into());
    }
    Ok(())
}
fn decoded(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| "invalid retained cloud state".into())
}
fn epoch(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one("SELECT epoch FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0))
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
fn remote_id(value: &str, prefix: &str) -> Result<()> {
    if !value.strip_prefix(prefix).is_some_and(|s| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    }) {
        return Err("invalid cloud remote identity".into());
    }
    Ok(())
}
fn owner<'a>(p: &'a Value) -> Result<(&'a str, &'a str)> {
    let principal = id(p, "principal")?;
    let job = id(p, "job_id")?;
    if job.len() > 100
        || !job
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err("invalid cloud job identity".into());
    }
    Ok((principal, job))
}
fn evidence(p: &Value) -> Result<()> {
    if !p["evidence"].as_object().is_some_and(|o| !o.is_empty())
        || p["evidence"].to_string().len() > 8192
    {
        return Err("cloud reconciliation requires bounded evidence".into());
    }
    Ok(())
}
fn audit(
    tx: &mut Transaction<'_>,
    principal: &str,
    job: &str,
    event: &str,
    evidence: &Value,
) -> Result<()> {
    let timestamp = now(tx)?;
    tx.execute(
        "INSERT INTO cloud_audit(principal,job_id,event,evidence,created) VALUES($1,$2,$3,$4,$5)",
        &[&principal, &job, &event, &evidence.to_string(), &timestamp],
    )
    .map_err(db_error)?;
    Ok(())
}
fn row(tx: &mut Transaction<'_>, principal: &str, job: &str) -> Result<Value> {
    reject_archived(tx, principal, job)?;
    let r=tx.query_opt("SELECT spec,binding,agent_id,run_id,state,snapshot,host_run,call_id,cancel_requested,revision,created,updated FROM cloud_jobs WHERE principal=$1 AND job_id=$2",&[&principal,&job]).map_err(db_error)?.ok_or("unknown cloud job")?;
    Ok(
        json!({"principal":principal,"job_id":job,"spec":r.get::<_,String>(0),"binding":r.get::<_,String>(1),"agent_id":r.get::<_,String>(2),"run_id":r.get::<_,Option<String>>(3),"state":r.get::<_,String>(4),"snapshot":r.get::<_,Option<String>>(5),"host_run_dir":r.get::<_,Option<String>>(6),"call_id":r.get::<_,Option<String>>(7),"cancel_requested":r.get::<_,bool>(8),"revision":r.get::<_,i64>(9),"created":r.get::<_,i64>(10),"updated":r.get::<_,i64>(11)}),
    )
}
fn admission(
    tx: &mut Transaction<'_>,
    principal: &str,
    run: &str,
    call: &str,
    reservation: i64,
) -> Result<()> {
    let r=tx.query_opt("SELECT h.principal,h.owner,h.owner_epoch,i.state,i.reservation_microusd FROM host_invocations i JOIN host_runs h ON h.run=i.run JOIN calls c ON c.run=i.run AND c.id=i.call_id WHERE i.run=$1 AND i.call_id=$2",&[&run,&call]).map_err(db_error)?.ok_or("cloud job requires native admitted invocation")?;
    if r.get::<_, String>(0) != principal
        || r.get::<_, Option<String>>(1).is_none()
        || r.get::<_, Option<i64>>(2) != Some(epoch(tx)?)
        || r.get::<_, String>(3) != "executing"
        || r.get::<_, i64>(4) != reservation
    {
        return Err("cloud job admission or owner differs".into());
    }
    Ok(())
}
fn spec(encoded: &str) -> Result<Value> {
    let p = decoded(encoded)?;
    fields(
        &p,
        &[
            "repository",
            "ref",
            "model",
            "prompt",
            "reservation_microusd",
            "mode",
            "auto_create_pr",
        ],
        &[],
    )?;
    for (k, max) in [
        ("repository", 2048),
        ("ref", 256),
        ("model", 256),
        ("prompt", 32768),
    ] {
        if text(&p, k)?.len() > max {
            return Err("cloud spec exceeds bound".into());
        }
    }
    if !text(&p, "repository")?.starts_with("https://")
        || !p["reservation_microusd"]
            .as_i64()
            .is_some_and(|n| (1..=100_000_000).contains(&n))
        || !matches!(p["mode"].as_str(), Some("plan" | "agent"))
        || !p["auto_create_pr"].is_boolean()
    {
        return Err("invalid cloud spec".into());
    }
    Ok(p)
}
pub(crate) fn apply(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    let (principal, job) = owner(p)?;
    reject_archived(tx, principal, job)?;
    match operation {
        "cursor_job.prepare" => {
            fields(
                p,
                &["principal", "job_id", "spec", "binding", "agent_id"],
                &[],
            )?;
            let encoded = text(p, "spec")?;
            spec(encoded)?;
            let fingerprint = Sha256::digest(encoded.as_bytes())
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect::<String>();
            if text(p, "binding")? != fingerprint {
                return Err("cloud spec binding mismatch".into());
            }
            if tx
                .query_opt(
                    "SELECT 1 FROM cloud_jobs WHERE principal=$1 AND job_id=$2",
                    &[&principal, &job],
                )
                .map_err(db_error)?
                .is_some()
            {
                let old = row(tx, principal, job)?;
                if old["spec"] != encoded || old["binding"] != fingerprint {
                    return Err("cloud job configuration conflict".into());
                }
                return Ok(old);
            }
            if tx
                .query_one("SELECT count(*)::bigint FROM cloud_jobs", &[])
                .map_err(db_error)?
                .get::<_, i64>(0)
                >= 100000
            {
                return Err("cloud job capacity exhausted".into());
            }
            let agent = text(p, "agent_id")?;
            remote_id(agent, "bc-")?;
            let time = now(tx)?;
            tx.execute("INSERT INTO cloud_jobs(principal,job_id,spec,binding,agent_id,state,created,updated) VALUES($1,$2,$3,$4,$5,'prepared',$6,$6)",&[&principal,&job,&encoded,&fingerprint,&agent,&time]).map_err(db_error)?;
            audit(
                tx,
                principal,
                job,
                "intent",
                &json!({"binding":fingerprint}),
            )?;
            row(tx, principal, job)
        }
        "cursor_job.begin" => {
            fields(
                p,
                &["principal", "job_id", "operation_id", "action"],
                &["run", "call_id", "binding", "run_id", "evidence", "retry"],
            )?;
            let action = text(p, "action")?;
            let operation_id = id(p, "operation_id")?;
            let old = row(tx, principal, job)?;
            let state = text(&old, "state")?;
            if action == "submit" {
                crate::shared_leadership::check_admission(tx)?;
                let run = id(p, "run")?;
                let call = id(p, "call_id")?;
                if old["binding"] != p["binding"] {
                    return Err("cloud job configuration differs".into());
                }
                if state != "prepared" {
                    if old["host_run_dir"] != run || old["call_id"] != call {
                        return Err("cloud job belongs to another invocation".into());
                    }
                    if old["run_id"].is_null() {
                        return Err("cloud launch unknown; attach exact evidenced run".into());
                    }
                    return Ok(json!({"dispatch":false,"row":old}));
                }
                let reservation = spec(text(&old, "spec")?)?["reservation_microusd"]
                    .as_i64()
                    .unwrap();
                admission(tx, principal, run, call, reservation)?;
                tx.execute("UPDATE cloud_jobs SET state='launch_unknown',host_run=$1,call_id=$2 WHERE principal=$3 AND job_id=$4",&[&run,&call,&principal,&job]).map_err(db_error)?;
                audit(
                    tx,
                    principal,
                    job,
                    "dispatch_intent",
                    &json!({"run":run,"call_id":call}),
                )?;
            } else if state == "terminal" {
                return Ok(json!({"dispatch":false,"row":old}));
            } else if action == "attach" {
                if state != "launch_unknown" || !old["run_id"].is_null() {
                    return Err("only uncertain cloud launches can be attached".into());
                }
                remote_id(text(p, "run_id")?, "run-")?;
                evidence(p)?;
            } else if action == "poll" || action == "cancel" {
                if old["run_id"].is_null() {
                    return Err("cloud launch unknown; attach exact evidenced run".into());
                }
                if action == "cancel" {
                    let retry = p["retry"]
                        .as_bool()
                        .ok_or("cloud cancellation requires explicit retry choice")?;
                    if old["cancel_requested"] == true && !retry {
                        return Err("cloud cancellation already requested".into());
                    }
                    let timestamp = now(tx)?;
                    tx.execute("UPDATE cloud_jobs SET cancel_requested=true,updated=$3 WHERE principal=$1 AND job_id=$2",&[&principal,&job,&timestamp]).map_err(db_error)?;
                    audit(
                        tx,
                        principal,
                        job,
                        "cancel_requested",
                        &json!({"run_id":old["run_id"],"retry":retry}),
                    )?;
                }
            } else {
                return Err("unsupported cloud operation".into());
            }
            if tx.query_one("SELECT count(*)::bigint FROM cloud_operations WHERE principal=$1 AND job_id=$2",&[&principal,&job]).map_err(db_error)?.get::<_,i64>(0)>=10000{return Err("cloud operation history exhausted".into())}
            let revision = old["revision"]
                .as_i64()
                .unwrap()
                .checked_add(1)
                .ok_or("cloud revision exhausted")?;
            tx.execute(
                "UPDATE cloud_jobs SET revision=$1 WHERE principal=$2 AND job_id=$3",
                &[&revision, &principal, &job],
            )
            .map_err(db_error)?;
            let current_epoch = epoch(tx)?;
            tx.execute("INSERT INTO cloud_operations(principal,job_id,operation_id,action,epoch,revision,request) VALUES($1,$2,$3,$4,$5,$6,$7)",&[&principal,&job,&operation_id,&action,&current_epoch,&revision,&p.to_string()]).map_err(db_error)?;
            Ok(json!({"dispatch":true,"row":row(tx,principal,job)?}))
        }
        "cursor_job.complete" => {
            fields(
                p,
                &[
                    "principal",
                    "job_id",
                    "operation_id",
                    "snapshot",
                    "evidence",
                ],
                &[],
            )?;
            evidence(p)?;
            let op = id(p, "operation_id")?;
            let old = row(tx, principal, job)?;
            let operation=tx.query_opt("SELECT action,epoch,revision,state,request FROM cloud_operations WHERE principal=$1 AND job_id=$2 AND operation_id=$3",&[&principal,&job,&op]).map_err(db_error)?.ok_or("unknown cloud operation")?;
            if operation.get::<_, i64>(1) != epoch(tx)?
                || operation.get::<_, String>(3) != "pending"
            {
                return Err("stale or settled cloud operation".into());
            }
            let action: String = operation.get(0);
            let snapshot = &p["snapshot"];
            let request = decoded(operation.get::<_, &str>(4))?;
            if action == "cancel" {
                if !snapshot.is_null() {
                    return Err("cancel acknowledgement cannot supply completion".into());
                }
                audit(tx, principal, job, "cancel_acknowledged", &p["evidence"])?;
            } else {
                fields(
                    snapshot,
                    &["agent_id", "run_id", "remote_status"],
                    &["result", "duration_ms"],
                )?;
                remote_id(text(snapshot, "run_id")?, "run-")?;
                if snapshot["agent_id"] != old["agent_id"]
                    || !old["run_id"].is_null() && snapshot["run_id"] != old["run_id"]
                {
                    return Err("cloud snapshot identity differs".into());
                }
                if action == "attach"
                    && (snapshot["run_id"] != request["run_id"]
                        || p["evidence"] != request["evidence"])
                {
                    return Err("cloud attachment differs from evidenced intent".into());
                }
                let status = text(snapshot, "remote_status")?;
                let terminal = matches!(status, "FINISHED" | "ERROR" | "CANCELLED" | "EXPIRED");
                if !terminal && !matches!(status, "CREATING" | "RUNNING")
                    || snapshot
                        .get("result")
                        .is_some_and(|v| !v.as_str().is_some_and(|s| s.len() <= 65536))
                    || snapshot
                        .get("duration_ms")
                        .is_some_and(|v| !v.as_i64().is_some_and(|n| n >= 0))
                {
                    return Err("unsupported cloud snapshot".into());
                }
                if action == "submit" {
                    let reservation = spec(text(&old, "spec")?)?["reservation_microusd"]
                        .as_i64()
                        .unwrap();
                    admission(
                        tx,
                        principal,
                        text(&old, "host_run_dir")?,
                        text(&old, "call_id")?,
                        reservation,
                    )?;
                }
                if old["state"] == "terminal" || old["revision"].as_i64() != Some(operation.get(2))
                {
                    audit(
                        tx,
                        principal,
                        job,
                        "late_observation",
                        &json!({"operation_id":op,"snapshot":snapshot,"evidence":p["evidence"]}),
                    )?;
                } else {
                    let state = if terminal { "terminal" } else { "active" };
                    let encoded = snapshot.to_string();
                    if old["snapshot"] != encoded {
                        let event = match action.as_str() {
                            "submit" => "submitted",
                            "attach" => "operator_attached",
                            _ => "observed",
                        };
                        let timestamp = now(tx)?;
                        tx.execute("UPDATE cloud_jobs SET run_id=$1,state=$2,snapshot=$3,updated=$4 WHERE principal=$5 AND job_id=$6",&[&text(snapshot,"run_id")?,&state,&encoded,&timestamp,&principal,&job]).map_err(db_error)?;
                        audit(tx, principal, job, event, &p["evidence"])?;
                    }
                }
            }
            tx.execute("UPDATE cloud_operations SET state='completed' WHERE principal=$1 AND job_id=$2 AND operation_id=$3",&[&principal,&job,&op]).map_err(db_error)?;
            row(tx, principal, job)
        }
        _ => Err("unsupported cloud job command".into()),
    }
}
pub(crate) fn query(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    fields(p, &["principal", "job_id"], &[])?;
    let (principal, job) = owner(p)?;
    let mut value = row(tx, principal, job)?;
    match operation {
        "cursor_job.row" => Ok(value),
        "cursor_job.inspect" => {
            let mut events = Vec::new();
            for r in tx.query("SELECT event,evidence,created FROM cloud_audit WHERE principal=$1 AND job_id=$2 ORDER BY seq",&[&principal,&job]).map_err(db_error)?{events.push(json!({"event":r.get::<_,String>(0),"evidence":decoded(r.get::<_,&str>(1))?,"created":r.get::<_,i64>(2)}));}
            value["audit"] = json!(events);
            Ok(value)
        }
        _ => Err("unsupported cloud job query".into()),
    }
}
