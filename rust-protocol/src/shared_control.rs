//! Shared worker claims and receipts. Every operation executes inside the
//! controller epoch transaction; workers never supply controller authority.
use crate::postgres_repository::db_error;
use postgres::{Row, Transaction};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;
pub(crate) const DDL: &str = "
CREATE TABLE worker_policy(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),policy text NOT NULL);
CREATE TABLE worker_nodes(principal text PRIMARY KEY,boot text NOT NULL,generation bigint NOT NULL,descriptor text NOT NULL,draining boolean NOT NULL DEFAULT false,updated bigint NOT NULL);
CREATE TABLE worker_boots(principal text NOT NULL,boot text NOT NULL,generation bigint NOT NULL,PRIMARY KEY(principal,boot));
CREATE TABLE worker_jobs(call_id text PRIMARY KEY,run_id text NOT NULL REFERENCES host_runs(run),principal text NOT NULL,binding text NOT NULL,state text NOT NULL,fence bigint,worker text,boot text,expires bigint,output text,evidence text,created bigint NOT NULL);
CREATE TABLE worker_claims(fence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,call_id text NOT NULL REFERENCES worker_jobs(call_id),worker text NOT NULL,boot text NOT NULL,dispatched boolean NOT NULL DEFAULT false);
CREATE TABLE worker_receipts(id text PRIMARY KEY,call_id text NOT NULL REFERENCES worker_jobs(call_id),fence bigint NOT NULL REFERENCES worker_claims(fence),worker text NOT NULL,boot text NOT NULL,payload text NOT NULL,authoritative boolean NOT NULL,created bigint NOT NULL);
CREATE TABLE worker_reconciliations(call_id text PRIMARY KEY REFERENCES worker_jobs(call_id),receipt text NOT NULL);
CREATE INDEX worker_pending ON worker_jobs(state,created,call_id);
CREATE INDEX worker_observations ON worker_receipts(call_id,fence);
CREATE TABLE operator_budget_base(scope text PRIMARY KEY,max_runs bigint NOT NULL,max_calls bigint NOT NULL,max_microusd bigint NOT NULL);
CREATE TABLE operator_budgets(revision bigint PRIMARY KEY,request_key text UNIQUE NOT NULL,digest text NOT NULL,receipt text NOT NULL);
CREATE TABLE operator_control(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),revision bigint NOT NULL,draining boolean NOT NULL);
CREATE TABLE operator_changes(revision bigint PRIMARY KEY,request_key text UNIQUE NOT NULL,digest text NOT NULL,receipt text NOT NULL);
CREATE TABLE event_policy(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),max_subscriptions bigint NOT NULL,page_size bigint NOT NULL);
CREATE TABLE event_subscriptions(id text PRIMARY KEY,descriptor text NOT NULL,principal text NOT NULL,task_id text NOT NULL,cursor bigint NOT NULL,state text NOT NULL,created bigint NOT NULL);
CREATE TABLE event_pending(subscription text PRIMARY KEY REFERENCES event_subscriptions(id),delivery_id text UNIQUE NOT NULL,body text NOT NULL,next_after bigint NOT NULL,attempts bigint NOT NULL DEFAULT 0,next_attempt bigint NOT NULL DEFAULT 0,last_error text,attempt_owner text,attempt_until bigint);
CREATE TABLE event_acknowledgements(subscription text PRIMARY KEY REFERENCES event_subscriptions(id),delivery_id text NOT NULL,through_event bigint NOT NULL,acknowledged bigint NOT NULL);

";
fn err<T>(code: &str) -> Result<T> {
    Err(code.into())
}
fn field<'a>(p: &'a Value, k: &str) -> Result<&'a str> {
    p[k].as_str().ok_or_else(|| format!("invalid {k}"))
}
fn id<'a>(p: &'a Value, k: &str) -> Result<&'a str> {
    let s = field(p, k)?;
    if s.is_empty()
        || s.len() > 200
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:/@-".contains(&b))
    {
        return err("invalid_worker_request");
    }
    Ok(s)
}
fn num(p: &Value, k: &str, min: i64, max: i64) -> Result<i64> {
    p[k].as_i64()
        .filter(|v| *v >= min && *v <= max)
        .ok_or_else(|| format!("invalid {k}"))
}
fn fields(p: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let o = p.as_object().ok_or("invalid_worker_request")?;
    if required.iter().any(|k| !o.contains_key(*k))
        || o.keys()
            .any(|k| !required.contains(&k.as_str()) && !optional.contains(&k.as_str()))
    {
        return err("invalid_worker_request");
    }
    Ok(())
}
fn decode(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| "invalid retained worker record".into())
}
fn hash(v: &Value) -> String {
    Sha256::digest(v.to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn now(tx: &mut Transaction) -> Result<i64> {
    Ok(tx
        .query_one(
            "SELECT floor(extract(epoch from clock_timestamp())*1000)::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0))
}
fn policy(tx: &mut Transaction) -> Result<Value> {
    decode(
        tx.query_opt("SELECT policy FROM worker_policy WHERE singleton", &[])
            .map_err(db_error)?
            .ok_or("worker controller not configured")?
            .get::<_, &str>(0),
    )
}
fn grant<'a>(policy: &'a Value, principal: &str) -> Result<&'a Value> {
    policy["worker_grants"]
        .get(principal)
        .ok_or("worker_forbidden".into())
}
fn worker(tx: &mut Transaction, p: &Value, policy: &Value) -> Result<(Value, bool)> {
    let principal = id(p, "principal")?;
    grant(policy, principal)?;
    let row = tx
        .query_opt(
            "SELECT boot,descriptor,draining FROM worker_nodes WHERE principal=$1",
            &[&principal],
        )
        .map_err(db_error)?
        .ok_or("stale_worker_epoch")?;
    if row.get::<_, &str>(0) != id(p, "boot")? {
        return err("stale_worker_epoch");
    }
    let d = decode(row.get::<_, &str>(1))?;
    versions(&d)?;
    Ok((d, row.get(2)))
}
fn versions(p: &Value) -> Result<()> {
    if p["protocol"] != "mas-worker/2" {
        return err("unsupported_worker_protocol");
    }
    let runtime = field(p, "runtime")?;
    let suffix = runtime
        .strip_prefix("python-3.")
        .ok_or("unsupported_worker_runtime")?;
    let (minor, patch) = suffix.split_once('.').ok_or("unsupported_worker_runtime")?;
    if !matches!(minor, "10" | "11" | "12" | "13" | "14")
        || patch.is_empty()
        || !patch.bytes().all(|b| b.is_ascii_digit())
    {
        return err("unsupported_worker_runtime");
    }
    let profiles = p["output_profiles"]
        .as_array()
        .ok_or("unsupported_worker_output_contract")?;
    if profiles.is_empty()
        || profiles.len() > 2
        || profiles
            .iter()
            .any(|v| v != "inline-json/1" && v != "durable-artifacts/1")
        || (profiles.len() == 2 && profiles[0] == profiles[1])
    {
        return err("unsupported_worker_output_contract");
    }
    Ok(())
}
fn binding(p: &Value) -> Result<()> {
    fields(
        p,
        &[
            "format",
            "principal",
            "operation",
            "implementation",
            "trust_zone",
            "input",
            "reservation_microusd",
            "output_profile",
        ],
        &["required_outputs"],
    )?;
    for k in ["principal", "operation", "trust_zone"] {
        id(p, k)?;
    }
    let digest = field(p, "implementation")?;
    if p["format"] != "mas-remote-invocation/1"
        || !p["input"].is_object()
        || digest.len() != 64
        || !digest
            .bytes()
            .all(|b| b.is_ascii_digit() || b"abcdef".contains(&b))
    {
        return err("invalid_worker_request");
    }
    num(p, "reservation_microusd", 0, 100_000_000)?;
    match field(p, "output_profile")? {
        "inline-json/1" => {
            if p.get("required_outputs").is_some() {
                return err("invalid_worker_output_contract");
            }
        }
        "durable-artifacts/1" => {
            let names = p["required_outputs"]
                .as_array()
                .ok_or("invalid_worker_output_contract")?;
            if names.is_empty() || names.len() > 32 {
                return err("invalid_worker_output_contract");
            }
            let mut previous = "";
            for name in names {
                let s = name.as_str().ok_or("invalid_worker_output_contract")?;
                id(&json!({"name":s}), "name")?;
                if s <= previous {
                    return err("invalid_worker_output_contract");
                }
                previous = s;
            }
        }
        _ => return err("unsupported_worker_output_contract"),
    };
    Ok(())
}
fn admitted(tx: &mut Transaction, c: &str, b: &Value) -> Result<String> {
    let rows=tx.query("SELECT i.run,i.state,i.reservation_microusd,s.input,h.principal FROM host_invocations i JOIN host_steps s ON s.run=i.run AND s.key=i.step_key JOIN host_runs h ON h.run=i.run JOIN calls c ON c.run=i.run AND c.id=i.call_id WHERE i.call_id=$1",&[&c]).map_err(db_error)?;
    if rows.len() != 1 {
        return err("invocation_not_admitted");
    }
    let row = &rows[0];
    if row.get::<_, &str>(1) != "executing"
        || row.get::<_, i64>(2) < num(b, "reservation_microusd", 0, 100_000_000)?
        || decode(row.get::<_, &str>(3))?["input"] != *b
        || row.get::<_, &str>(4) != field(b, "principal")?
    {
        return err("invocation_not_admitted");
    }
    Ok(row.get(0))
}
fn expire(tx: &mut Transaction) -> Result<()> {
    let n = now(tx)?;
    tx.execute("UPDATE worker_jobs SET state='pending',worker=NULL,boot=NULL,expires=NULL WHERE state='claimed' AND expires<=$1",&[&n]).map_err(db_error)?;
    tx.execute(
        "UPDATE worker_jobs SET state='unknown' WHERE state='dispatched' AND expires<=$1",
        &[&n],
    )
    .map_err(db_error)?;
    Ok(())
}
const JOB:&str="SELECT call_id,run_id,principal,binding,state,fence,worker,boot,expires,output,evidence FROM worker_jobs WHERE call_id=$1";
fn job(tx: &mut Transaction, c: &str) -> Result<Row> {
    tx.query_opt(JOB, &[&c])
        .map_err(db_error)?
        .ok_or("not_found".into())
}
fn view(row: &Row) -> Result<Value> {
    Ok(
        json!({"call_id":row.get::<_,String>(0),"state":row.get::<_,String>(4),"output":row.get::<_,Option<String>>(9).as_deref().map(decode).transpose()?,"evidence":row.get::<_,Option<String>>(10).as_deref().map(decode).transpose()?}),
    )
}
fn assignment(tx: &mut Transaction, p: &Value, policy: &Value) -> Result<Row> {
    worker(tx, p, policy)?;
    expire(tx)?;
    let c = id(p, "call_id")?;
    let row = job(tx, c)?;
    if row.get::<_, Option<&str>>(6) != Some(id(p, "principal")?)
        || row.get::<_, Option<&str>>(7) != Some(id(p, "boot")?)
        || row.get::<_, Option<i64>>(5) != Some(num(p, "fence", 1, i64::MAX)?)
        || !matches!(row.get::<_, &str>(4), "claimed" | "dispatched")
    {
        return err("stale_worker_assignment");
    }
    let b = decode(row.get::<_, &str>(3))?;
    let g = grant(policy, id(p, "principal")?)?;
    if g["capabilities"][field(&b, "operation")?] != b["implementation"]
        || g["trust_zone"] != b["trust_zone"]
    {
        return err("worker_capability_denied");
    }
    Ok(row)
}
fn registration(principal: &str, boot: &str, generation: i64, d: &Value, policy: &Value) -> Value {
    json!({"worker":principal,"boot":boot,"generation":generation,"lease_ms":policy["lease_ms"],"protocol":"mas-worker/2","output_profiles":d["output_profiles"]})
}
fn retained_control(tx: &mut Transaction) -> Result<Value> {
    let row = tx
        .query_opt(
            "SELECT receipt FROM operator_changes ORDER BY revision DESC LIMIT 1",
            &[],
        )
        .map_err(db_error)?;
    if let Some(row) = row {
        decode(row.get::<_, &str>(0))
    } else {
        Ok(
            json!({"format":"mas-worker-control/1","revision":0,"draining":false,"active_effects_cancelled":false}),
        )
    }
}

fn control(tx: &mut Transaction) -> Result<Value> {
    let mut result = retained_control(tx)?;
    result["draining"] = json!(tx
        .query_one("SELECT draining FROM controller_lease WHERE singleton", &[])
        .map_err(db_error)?
        .get::<_, bool>(0));
    Ok(result)
}

pub(crate) fn apply(tx: &mut Transaction, operation: &str, p: &Value) -> Result<Value> {
    if operation.starts_with("event.") {
        return event_apply(tx, operation, p);
    }
    if operation == "operator.budget_amend" {
        return budget_amend(tx, p);
    }
    if operation == "operator.drain" {
        fields(
            p,
            &[
                "operator",
                "request_key",
                "reason",
                "expected_revision",
                "draining",
            ],
            &[],
        )?;
        let operator = id(p, "operator")?;
        let key = id(p, "request_key")?;
        let reason = field(p, "reason")?;
        if reason.trim().is_empty() || reason.len() > 2048 || reason.chars().any(|c| c.is_control())
        {
            return err("invalid_worker_control");
        }
        let draining = p["draining"].as_bool().ok_or("invalid_worker_control")?;
        let expected = num(p, "expected_revision", 0, i64::MAX)?;
        let digest = hash(p);
        if let Some(row) = tx
            .query_opt(
                "SELECT digest,receipt FROM operator_changes WHERE request_key=$1",
                &[&key],
            )
            .map_err(db_error)?
        {
            if row.get::<_, &str>(0) != digest {
                return err("worker_control_conflict");
            }
            return decode(row.get::<_, &str>(1));
        }
        let prior = control(tx)?;
        if prior["revision"] != expected {
            return err("worker_control_revision_conflict");
        }
        if expected >= 10000 {
            return err("worker_control_history_full");
        }
        let revision = expected + 1;
        let receipt = json!({"format":"mas-worker-control/1","revision":revision,"request_key":key,"operator":operator,"reason":reason,"draining":draining,"created":now(tx)?,"active_effects_cancelled":false});
        tx.execute(
            "INSERT INTO operator_changes VALUES($1,$2,$3,$4)",
            &[&revision, &key, &digest, &receipt.to_string()],
        )
        .map_err(db_error)?;
        tx.execute("INSERT INTO operator_control VALUES(true,$1,$2) ON CONFLICT(singleton) DO UPDATE SET revision=$1,draining=$2",&[&revision,&draining]).map_err(db_error)?;
        tx.execute(
            "UPDATE controller_lease SET draining=$1 WHERE singleton",
            &[&draining],
        )
        .map_err(db_error)?;
        return Ok(receipt);
    }
    if operation == "worker.configure" {
        fields(
            p,
            &[
                "max_outstanding",
                "per_principal",
                "lease_ms",
                "worker_grants",
                "artifact_store_id",
            ],
            &[],
        )?;
        let maximum = num(p, "max_outstanding", 1, 10000)?;
        num(p, "per_principal", 1, maximum)?;
        num(p, "lease_ms", 100, 300000)?;
        let grants = p["worker_grants"]
            .as_object()
            .ok_or("invalid_worker_grants")?;
        if grants.len() > 10000 {
            return err("invalid_worker_grants");
        }
        for (name, g) in grants {
            id(&json!({"principal":name}), "principal")?;
            fields(g, &["trust_zone", "capacity", "capabilities"], &[])?;
            id(g, "trust_zone")?;
            num(g, "capacity", 1, 64)?;
            let caps = g["capabilities"]
                .as_object()
                .ok_or("invalid_worker_grants")?;
            if caps.is_empty() || caps.len() > 128 {
                return err("invalid_worker_grants");
            }
            for (name, digest) in caps {
                id(&json!({"name":name}), "name")?;
                let s = digest.as_str().ok_or("invalid_worker_grants")?;
                if s.len() != 64
                    || !s
                        .bytes()
                        .all(|b| b.is_ascii_digit() || b"abcdef".contains(&b))
                {
                    return err("invalid_worker_grants");
                }
            }
        }
        if !p["artifact_store_id"].is_null() {
            id(p, "artifact_store_id")?;
        }
        if let Some(row) = tx
            .query_opt("SELECT policy FROM worker_policy WHERE singleton", &[])
            .map_err(db_error)?
        {
            if decode(row.get::<_, &str>(0))? != *p {
                return err("worker_policy_changed");
            }
        } else {
            tx.execute(
                "INSERT INTO worker_policy VALUES(true,$1)",
                &[&p.to_string()],
            )
            .map_err(db_error)?;
        }
        return Ok(json!({"configured":true}));
    }
    let policy = policy(tx)?;
    match operation {
        "worker.submit" => {
            fields(p, &["call_id", "binding"], &[])?;
            let c = id(p, "call_id")?;
            let b = &p["binding"];
            binding(b)?;
            if b["output_profile"] == "durable-artifacts/1" && policy["artifact_store_id"].is_null()
            {
                return err("worker_artifact_storage_unconfigured");
            }
            let r = admitted(tx, c, b)?;
            if let Ok(row) = job(tx, c) {
                if row.get::<_, &str>(1) != r || decode(row.get::<_, &str>(3))? != *b {
                    return err("invocation_binding_conflict");
                }
                return view(&row);
            }
            let principal = field(b, "principal")?;
            let count=tx.query_one("SELECT count(*),count(*) FILTER(WHERE principal=$1) FROM worker_jobs WHERE state IN ('pending','claimed','dispatched','unknown')",&[&principal]).map_err(db_error)?;
            if count.get::<_, i64>(0) >= num(&policy, "max_outstanding", 1, 10000)?
                || count.get::<_, i64>(1) >= num(&policy, "per_principal", 1, 10000)?
            {
                return err("worker_queue_full");
            }
            let timestamp = now(tx)?;
            tx.execute("INSERT INTO worker_jobs(call_id,run_id,principal,binding,state,created) VALUES($1,$2,$3,$4,'pending',$5)",&[&c,&r,&principal,&b.to_string(),&timestamp]).map_err(db_error)?;
            view(&job(tx, c)?)
        }
        "worker.register" => {
            fields(p, &["principal", "boot", "descriptor"], &[])?;
            let principal = id(p, "principal")?;
            let boot = id(p, "boot")?;
            let d = &p["descriptor"];
            fields(
                d,
                &[
                    "capabilities",
                    "architecture",
                    "runtime",
                    "protocol",
                    "output_profiles",
                    "trust_zone",
                    "capacity",
                ],
                &[],
            )?;
            versions(d)?;
            id(d, "architecture")?;
            let g = grant(&policy, principal)?;
            if g["trust_zone"] != d["trust_zone"] {
                return err("worker_capability_denied");
            }
            num(d, "capacity", 1, num(g, "capacity", 1, 64)?)?;
            let caps = d["capabilities"]
                .as_object()
                .ok_or("worker_capability_denied")?;
            if caps.is_empty()
                || caps
                    .iter()
                    .any(|(k, v)| g["capabilities"].get(k) != Some(v))
            {
                return err("worker_capability_denied");
            }
            let old = tx
                .query_opt(
                    "SELECT boot,generation,descriptor FROM worker_nodes WHERE principal=$1",
                    &[&principal],
                )
                .map_err(db_error)?;
            if let Some(row) = &old {
                if row.get::<_, &str>(0) == boot {
                    if decode(row.get::<_, &str>(2))? != *d {
                        return err("worker_registration_conflict");
                    }
                    return Ok(registration(principal, boot, row.get(1), d, &policy));
                }
            }
            if tx
                .query_opt(
                    "SELECT 1 FROM worker_boots WHERE principal=$1 AND boot=$2",
                    &[&principal, &boot],
                )
                .map_err(db_error)?
                .is_some()
            {
                return err("stale_worker_epoch");
            }
            let generation = old.map_or(1, |r| r.get::<_, i64>(1) + 1);
            tx.execute("UPDATE worker_jobs SET state='pending',worker=NULL,boot=NULL,expires=NULL WHERE worker=$1 AND state='claimed'",&[&principal]).map_err(db_error)?;
            tx.execute(
                "UPDATE worker_jobs SET state='unknown' WHERE worker=$1 AND state='dispatched'",
                &[&principal],
            )
            .map_err(db_error)?;
            let timestamp = now(tx)?;
            tx.execute("INSERT INTO worker_nodes VALUES($1,$2,$3,$4,false,$5) ON CONFLICT(principal) DO UPDATE SET boot=$2,generation=$3,descriptor=$4,draining=false,updated=$5",&[&principal,&boot,&generation,&d.to_string(),&timestamp]).map_err(db_error)?;
            tx.execute(
                "INSERT INTO worker_boots VALUES($1,$2,$3)",
                &[&principal, &boot, &generation],
            )
            .map_err(db_error)?;
            Ok(registration(principal, boot, generation, d, &policy))
        }
        "worker.claim" => {
            fields(p, &["principal", "boot"], &[])?;
            let principal = id(p, "principal")?;
            let boot = id(p, "boot")?;
            let (d, draining) = worker(tx, p, &policy)?;
            expire(tx)?;
            if draining || control(tx)?["draining"] == true {
                return Ok(json!({"assignment":null}));
            }
            let g = grant(&policy, principal)?;
            let active:i64=tx.query_one("SELECT count(*) FROM worker_jobs WHERE worker=$1 AND state IN ('claimed','dispatched','unknown')",&[&principal]).map_err(db_error)?.get(0);
            if active >= num(g, "capacity", 1, 64)?.min(num(&d, "capacity", 1, 64)?) {
                return Ok(json!({"assignment":null}));
            }
            for row in tx.query("SELECT call_id,binding FROM worker_jobs WHERE state='pending' ORDER BY created,call_id",&[]).map_err(db_error)?{let b=decode(row.get::<_,&str>(1))?;let op=field(&b,"operation")?;if !d["output_profiles"].as_array().ok_or("invalid worker profile")?.contains(&b["output_profile"])||b["trust_zone"]!=g["trust_zone"]||b["implementation"]!=g["capabilities"][op]||b["implementation"]!=d["capabilities"][op]{continue;}let c=row.get::<_,&str>(0);let fence:i64=tx.query_one("INSERT INTO worker_claims(call_id,worker,boot) VALUES($1,$2,$3) RETURNING fence",&[&c,&principal,&boot]).map_err(db_error)?.get(0);let expires=now(tx)?+num(&policy,"lease_ms",100,300000)?;tx.execute("UPDATE worker_jobs SET state='claimed',worker=$1,boot=$2,fence=$3,expires=$4 WHERE call_id=$5",&[&principal,&boot,&fence,&expires,&c]).map_err(db_error)?;return Ok(json!({"assignment":{"call_id":c,"boot":boot,"fence":fence,"binding":b,"lease_ms":policy["lease_ms"]}}));}
            Ok(json!({"assignment":null}))
        }
        "worker.dispatch" | "worker.heartbeat" => {
            fields(p, &["principal", "boot", "call_id", "fence"], &[])?;
            let row = assignment(tx, p, &policy)?;
            let c = id(p, "call_id")?;
            if operation == "worker.heartbeat" {
                let expires = now(tx)? + num(&policy, "lease_ms", 100, 300000)?;
                tx.execute(
                    "UPDATE worker_jobs SET expires=$1 WHERE call_id=$2",
                    &[&expires, &c],
                )
                .map_err(db_error)?;
                return Ok(json!({"lease_ms":policy["lease_ms"]}));
            }
            if row.get::<_, &str>(4) == "dispatched" {
                return Ok(json!({"execute":false,"reason":"dispatch_already_granted"}));
            }
            if control(tx)?["draining"] == true {
                return err("controller_admission_closed");
            }
            admitted(tx, c, &decode(row.get::<_, &str>(3))?)?;
            tx.execute(
                "UPDATE worker_jobs SET state='dispatched' WHERE call_id=$1",
                &[&c],
            )
            .map_err(db_error)?;
            tx.execute(
                "UPDATE worker_claims SET dispatched=true WHERE fence=$1",
                &[&num(p, "fence", 1, i64::MAX)?],
            )
            .map_err(db_error)?;
            Ok(json!({"execute":true,"call_id":c,"fence":p["fence"],"lease_ms":policy["lease_ms"]}))
        }
        "worker.drain" => {
            fields(p, &["principal", "boot", "draining"], &[])?;
            worker(tx, p, &policy)?;
            let draining = p["draining"].as_bool().ok_or("invalid_worker_request")?;
            tx.execute(
                "UPDATE worker_nodes SET draining=$1 WHERE principal=$2",
                &[&draining, &id(p, "principal")?],
            )
            .map_err(db_error)?;
            Ok(json!({"draining":draining}))
        }
        "worker.abandon" => {
            fields(p, &["principal", "call_id"], &[])?;
            tx.execute("UPDATE worker_jobs SET state=CASE WHEN state IN ('pending','claimed') THEN 'cancelled' ELSE 'unknown' END WHERE call_id=$1 AND principal=$2 AND state IN ('pending','claimed','dispatched')",&[&id(p,"call_id")?,&id(p,"principal")?]).map_err(db_error)?;
            Ok(json!({"abandoned":true}))
        }
        "worker.inspect" => {
            fields(p, &["principal", "call_id"], &[])?;
            expire(tx)?;
            owned_view(tx, p)
        }
        "worker.receipt" => receipt(tx, p, &policy),
        "worker.reconcile" => reconcile(tx, p),
        _ => err("unknown shared control command"),
    }
}
fn owned_view(tx: &mut Transaction, p: &Value) -> Result<Value> {
    let row = job(tx, id(p, "call_id")?)?;
    if row.get::<_, &str>(2) != id(p, "principal")? {
        return err("not_found");
    }
    view(&row)
}
fn receipt(tx: &mut Transaction, p: &Value, policy: &Value) -> Result<Value> {
    fields(p, &["principal", "receipt", "artifacts_verified"], &[])?;
    let principal = id(p, "principal")?;
    let grant = grant(policy, principal)?;
    let r = &p["receipt"];
    let artifact = r["output_profile"] == "durable-artifacts/1";
    fields(
        r,
        &[
            "id",
            "call_id",
            "boot",
            "fence",
            "status",
            "output",
            "evidence",
            "output_profile",
        ],
        if artifact { &["artifacts"] } else { &[] },
    )?;
    let receipt_id = id(r, "id")?;
    let c = id(r, "call_id")?;
    let boot = id(r, "boot")?;
    let fence = num(r, "fence", 1, i64::MAX)?;
    let status = field(r, "status")?;
    if !r["evidence"].is_object()
        || !matches!(status, "completed" | "unknown" | "failed_known")
        || status == "completed" && !r["output"].is_object()
        || status == "unknown" && !r["output"].is_null()
        || status == "failed_known" && !r["output"].is_null() && !r["output"].is_object()
    {
        return err("invalid_worker_receipt");
    }
    if status == "failed_known" {
        let evidence = r["evidence"].as_object().ok_or("invalid_worker_receipt")?;
        if evidence.len() != 1 || !evidence.contains_key("provider_outcome") {
            return err("invalid_worker_receipt");
        }
        let certainty = field(&evidence["provider_outcome"], "certainty")?;
        if !matches!(certainty, "not_sent" | "completed")
            || certainty == "not_sent" && !r["output"].is_null()
        {
            return err("invalid_worker_receipt");
        }
    }
    let job = job(tx, c)?;
    let b = decode(job.get::<_, &str>(3))?;
    if r["output_profile"] != b["output_profile"] {
        return err("worker_output_contract_mismatch");
    }
    if artifact {
        if p["artifacts_verified"] != true || policy["artifact_store_id"].is_null() {
            return err("worker_artifact_unavailable");
        }
        let artifacts = r["artifacts"]
            .as_object()
            .ok_or("worker_output_contract_mismatch")?;
        let required = if r["output"].is_null() {
            Vec::new()
        } else {
            b["required_outputs"]
                .as_array()
                .ok_or("worker_output_contract_mismatch")?
                .iter()
                .map(|n| n.as_str().ok_or("worker_output_contract_mismatch"))
                .collect::<std::result::Result<Vec<_>, _>>()?
        };
        if artifacts.len() != required.len() || required.iter().any(|k| !artifacts.contains_key(*k))
        {
            return err("worker_output_contract_mismatch");
        }
        if !r["output"].is_null()
            && (r["output"]["format"] != "mas-worker-artifact-output/1"
                || r["output"]["artifacts"] != r["artifacts"]
                || !r["output"]["result"].is_object())
        {
            return err("worker_output_contract_mismatch");
        }
    }
    if r.to_string().len() > 131072 {
        return err("invalid_worker_receipt");
    }
    if let Some(old) = tx
        .query_opt(
            "SELECT worker,payload,authoritative FROM worker_receipts WHERE id=$1",
            &[&receipt_id],
        )
        .map_err(db_error)?
    {
        if old.get::<_, &str>(0) != principal || decode(old.get::<_, &str>(1))? != *r {
            return err("receipt_conflict");
        }
        return Ok(
            json!({"stored":true,"receipt_id":receipt_id,"authoritative":old.get::<_,bool>(2)}),
        );
    }
    let claim=tx.query_opt("SELECT dispatched FROM worker_claims WHERE fence=$1 AND call_id=$2 AND worker=$3 AND boot=$4",&[&fence,&c,&principal,&boot]).map_err(db_error)?.ok_or("receipt_without_dispatch")?;
    if !claim.get::<_, bool>(0) && status != "unknown" {
        return err("receipt_without_dispatch");
    }
    expire(tx)?;
    let current = job_by_id(tx, c)?;
    let worker = tx
        .query_opt(
            "SELECT boot FROM worker_nodes WHERE principal=$1",
            &[&principal],
        )
        .map_err(db_error)?;
    let authoritative = current.get::<_, &str>(4) == "dispatched"
        && current.get::<_, Option<i64>>(5) == Some(fence)
        && worker.as_ref().map(|w| w.get::<_, &str>(0)) == Some(boot)
        && grant["capabilities"][field(&b, "operation")?] == b["implementation"]
        && grant["trust_zone"] == b["trust_zone"]
        && admitted(tx, c, &b).is_ok();
    let count: i64 = tx
        .query_one(
            "SELECT count(*) FROM worker_receipts WHERE call_id=$1",
            &[&c],
        )
        .map_err(db_error)?
        .get(0);
    if count >= 32 {
        return err("receipt_limit");
    }
    let timestamp = now(tx)?;
    tx.execute(
        "INSERT INTO worker_receipts VALUES($1,$2,$3,$4,$5,$6,$7,$8)",
        &[
            &receipt_id,
            &c,
            &fence,
            &principal,
            &boot,
            &r.to_string(),
            &authoritative,
            &timestamp,
        ],
    )
    .map_err(db_error)?;
    if authoritative {
        let mut evidence = json!({"worker":principal,"receipt_id":receipt_id,"fence":fence,"observation":r["evidence"]});
        if artifact {
            evidence["output_profile"] = json!("durable-artifacts/1");
            evidence["artifacts"] = r["artifacts"].clone();
        }
        let output = if r["output"].is_null() {
            None
        } else {
            Some(r["output"].to_string())
        };
        tx.execute(
            "UPDATE worker_jobs SET state=$1,output=$2,evidence=$3 WHERE call_id=$4",
            &[&status, &output, &evidence.to_string(), &c],
        )
        .map_err(db_error)?;
    }
    Ok(json!({"stored":true,"receipt_id":receipt_id,"authoritative":authoritative}))
}
fn job_by_id(tx: &mut Transaction, c: &str) -> Result<Row> {
    job(tx, c)
}
fn reconcile(tx: &mut Transaction, p: &Value) -> Result<Value> {
    fields(p, &["run_id", "call_id", "operator"], &[])?;
    let c = id(p, "call_id")?;
    let r = id(p, "run_id")?;
    let operator = id(p, "operator")?;
    if let Some(row) = tx
        .query_opt(
            "SELECT receipt FROM worker_reconciliations WHERE call_id=$1",
            &[&c],
        )
        .map_err(db_error)?
    {
        return decode(row.get::<_, &str>(0));
    }
    expire(tx)?;
    let row = job(tx, c)?;
    if row.get::<_, &str>(1) != r || row.get::<_, &str>(4) != "unknown" {
        return err("remote_resolution_not_ready");
    }
    let resolution=tx.query_opt("SELECT i.state,i.output,s.input,r.seq,r.decision,r.evidence,r.principal FROM host_invocations i JOIN host_steps s ON s.run=i.run AND s.key=i.step_key JOIN host_resolutions r ON r.run=i.run AND r.call_id=i.call_id WHERE i.run=$1 AND i.call_id=$2 ORDER BY r.seq DESC LIMIT 1",&[&r,&c]).map_err(db_error)?.ok_or("remote_resolution_not_ready")?;
    let state = resolution.get::<_, &str>(0);
    let decision = resolution.get::<_, &str>(4);
    if !matches!(
        (state, decision),
        ("resolved", "completed") | ("retry_allowed", "confirmed_no_effect")
    ) || decode(resolution.get::<_, &str>(2))?["input"] != decode(row.get::<_, &str>(3))?
    {
        return err("remote_resolution_not_ready");
    }
    let observation = json!({"run_id":r,"call_id":c,"resolution_id":resolution.get::<_,i64>(3),"decision":decision,"evidence":decode(resolution.get::<_,&str>(5))?,"principal":resolution.get::<_,&str>(6),"output":resolution.get::<_,Option<&str>>(1).map(decode).transpose()?,"binding":decode(row.get::<_,&str>(3))?});
    let receipt = json!({"reconciled":true,"execution_resumed":false,"call_id":c,"run_id":r,"operator":operator,"resolution_id":resolution.get::<_,i64>(3),"decision":decision,"evidence_sha256":hash(&observation),"created":now(tx)?});
    tx.execute(
        "INSERT INTO worker_reconciliations VALUES($1,$2)",
        &[&c, &receipt.to_string()],
    )
    .map_err(db_error)?;
    tx.execute(
        "UPDATE worker_jobs SET state='reconciled' WHERE call_id=$1",
        &[&c],
    )
    .map_err(db_error)?;
    Ok(receipt)
}
pub(crate) fn query(tx: &mut Transaction, operation: &str, p: &Value) -> Result<Value> {
    if operation.starts_with("event.") {
        return event_query(tx, operation, p);
    }
    match operation {
        "operator.budget" => budget(tx, p),
        "operator.storage" => {
            fields(p, &[], &[])?;
            health(tx)
        }
        "operator.archive_preview" => archive_preview(tx, p),
        "operator.status" => {
            fields(p, &[], &[])?;
            control(tx)
        }
        "worker.job" => {
            fields(p, &["principal", "call_id"], &[])?;
            owned_view(tx, p)
        }
        "worker.evidence" => {
            fields(p, &["principal", "call_id"], &[])?;
            owned_view(tx, p)?;
            let mut out = Vec::new();
            for row in tx
                .query(
                    "SELECT payload FROM worker_receipts WHERE call_id=$1 ORDER BY created,id",
                    &[&id(p, "call_id")?],
                )
                .map_err(db_error)?
            {
                out.push(decode(row.get::<_, &str>(0))?);
            }
            Ok(json!(out))
        }
        "worker.receipt_context" => {
            fields(p, &["principal", "receipt"], &[])?;
            let policy = policy(tx)?;
            let principal = id(p, "principal")?;
            grant(&policy, principal)?;
            let r = &p["receipt"];
            let c = id(r, "call_id")?;
            let receipt_id = id(r, "id")?;
            let prior = tx
                .query_opt(
                    "SELECT worker,payload FROM worker_receipts WHERE id=$1",
                    &[&receipt_id],
                )
                .map_err(db_error)?;
            if let Some(row) = &prior {
                if row.get::<_, &str>(0) != principal || decode(row.get::<_, &str>(1))? != *r {
                    return err("receipt_conflict");
                }
            } else {
                let exists=tx.query_opt("SELECT dispatched FROM worker_claims WHERE fence=$1 AND call_id=$2 AND worker=$3 AND boot=$4",&[&num(r,"fence",1,i64::MAX)?,&c,&principal,&id(r,"boot")?]).map_err(db_error)?.ok_or("receipt_without_dispatch")?;
                if !exists.get::<_, bool>(0) && r["status"] != "unknown" {
                    return err("receipt_without_dispatch");
                }
            }
            Ok(json!({"binding":decode(job(tx,c)?.get::<_,&str>(3))?,"prior":prior.is_some()}))
        }
        _ => err("unknown shared control query"),
    }
}
fn event_record(tx: &mut Transaction, p: &Value) -> Result<Value> {
    let subscription_id = id(p, "subscription_id")?;
    let principal = id(p, "principal")?;
    let row=tx.query_opt("SELECT id,descriptor,principal,task_id,cursor,state,created FROM event_subscriptions WHERE id=$1 AND principal=$2",&[&subscription_id,&principal]).map_err(db_error)?.ok_or("subscription_not_found")?;
    Ok(
        json!({"subscription_id":row.get::<_,&str>(0),"descriptor":decode(row.get::<_,&str>(1))?,"principal":row.get::<_,&str>(2),"task_id":row.get::<_,&str>(3),"cursor":row.get::<_,i64>(4),"state":row.get::<_,&str>(5),"created":row.get::<_,i64>(6)}),
    )
}
fn event_pending(tx: &mut Transaction, s: &str) -> Result<Value> {
    let row=tx.query_opt("SELECT delivery_id,body,next_after,attempts,next_attempt,last_error,attempt_owner,attempt_until FROM event_pending WHERE subscription=$1",&[&s]).map_err(db_error)?;
    if let Some(row) = row {
        Ok(
            json!({"delivery_id":row.get::<_,&str>(0),"body":row.get::<_,&str>(1),"next_after":row.get::<_,i64>(2),"attempts":row.get::<_,i64>(3),"next_attempt":row.get::<_,i64>(4),"last_error":row.get::<_,Option<&str>>(5),"attempt_owner":row.get::<_,Option<&str>>(6),"attempt_until":row.get::<_,Option<i64>>(7)}),
        )
    } else {
        Ok(Value::Null)
    }
}
fn event_query(tx: &mut Transaction, operation: &str, p: &Value) -> Result<Value> {
    fields(p, &["principal", "subscription_id"], &[])?;
    let record = event_record(tx, p)?;
    let pending = event_pending(tx, id(p, "subscription_id")?)?;
    match operation {
        "event.read" => Ok(json!({"record":record,"pending":pending})),
        _ => err("unknown event query"),
    }
}
fn event_apply(tx: &mut Transaction, operation: &str, p: &Value) -> Result<Value> {
    if operation == "event.configure" {
        fields(p, &["max_subscriptions", "page_size"], &[])?;
        let maximum = num(p, "max_subscriptions", 1, 10000)?;
        let page = num(p, "page_size", 1, 1000)?;
        if let Some(row) = tx
            .query_opt(
                "SELECT max_subscriptions,page_size FROM event_policy WHERE singleton",
                &[],
            )
            .map_err(db_error)?
        {
            if row.get::<_, i64>(0) != maximum || row.get::<_, i64>(1) != page {
                return err("event_policy_changed");
            }
        } else {
            tx.execute(
                "INSERT INTO event_policy VALUES(true,$1,$2)",
                &[&maximum, &page],
            )
            .map_err(db_error)?;
        }
        return Ok(json!({"configured":true}));
    }
    let s = id(p, "subscription_id")?;
    let principal = id(p, "principal")?;
    if operation == "event.subscribe" {
        fields(
            p,
            &[
                "principal",
                "subscription_id",
                "task_id",
                "descriptor",
                "after",
            ],
            &[],
        )?;
        let task = id(p, "task_id")?;
        let after = num(p, "after", 0, i64::MAX)?;
        if !p["descriptor"].is_object()
            || p["descriptor"]["task_id"] != task
            || p["descriptor"]["principal"] != principal
            || p["descriptor"]["after"] != after
        {
            return err("invalid_subscription");
        }
        if let Some(row) = tx
            .query_opt(
                "SELECT principal,descriptor FROM event_subscriptions WHERE id=$1",
                &[&s],
            )
            .map_err(db_error)?
        {
            if row.get::<_, &str>(0) != principal {
                return err("forbidden");
            }
            if decode(row.get::<_, &str>(1))? != p["descriptor"] {
                return err("subscription_conflict");
            }
        } else {
            let row=tx.query_one("SELECT (SELECT count(*) FROM event_subscriptions),max_subscriptions FROM event_policy WHERE singleton",&[]).map_err(db_error)?;
            if row.get::<_, i64>(0) >= row.get::<_, i64>(1) {
                return err("subscription_limit");
            }
            let timestamp = now(tx)?;
            tx.execute(
                "INSERT INTO event_subscriptions VALUES($1,$2,$3,$4,$5,'active',$6)",
                &[
                    &s,
                    &p["descriptor"].to_string(),
                    &principal,
                    &task,
                    &after,
                    &timestamp,
                ],
            )
            .map_err(db_error)?;
        }
        return event_record(tx, p);
    }
    let record = event_record(tx, p)?;
    match operation {
        "event.stop" | "event.expire" => {
            fields(p, &["principal", "subscription_id"], &[])?;
            let state = if operation == "event.stop" {
                "stopped"
            } else {
                "resync_required"
            };
            tx.execute(
                "UPDATE event_subscriptions SET state=$1 WHERE id=$2",
                &[&state, &s],
            )
            .map_err(db_error)?;
            event_record(tx, p)
        }
        "event.stage" => {
            fields(
                p,
                &[
                    "principal",
                    "subscription_id",
                    "expected_cursor",
                    "delivery_id",
                    "body",
                    "next_after",
                ],
                &[],
            )?;
            let cursor = num(p, "expected_cursor", 0, i64::MAX)?;
            let next = num(
                p,
                "next_after",
                cursor.checked_add(1).ok_or("event_stage_conflict")?,
                i64::MAX,
            )?;
            let delivery = id(p, "delivery_id")?;
            let body = field(p, "body")?;
            let decoded_body = decode(body)?;
            if body.len() > 262144
                || decoded_body["delivery_id"] != delivery
                || decoded_body["subscription_id"] != s
                || decoded_body["task_id"] != record["task_id"]
                || decoded_body["after"] != cursor
                || decoded_body["next_after"] != next
                || decoded_body["format"] != "mas-events/1"
                || !decoded_body["events"]
                    .as_array()
                    .is_some_and(|items| !items.is_empty())
            {
                return err("invalid_delivery_body");
            }
            let prior = event_pending(tx, s)?;
            if !prior.is_null() {
                if prior["delivery_id"] != delivery || prior["body"] != body {
                    return err("event_stage_conflict");
                }
                return Ok(prior);
            }
            if record["state"] != "active" || record["cursor"] != cursor {
                return err("event_stage_conflict");
            }
            tx.execute("INSERT INTO event_pending(subscription,delivery_id,body,next_after) VALUES($1,$2,$3,$4)",&[&s,&delivery,&body,&next]).map_err(db_error)?;
            event_pending(tx, s)
        }
        "event.begin_attempt" => {
            fields(
                p,
                &[
                    "principal",
                    "subscription_id",
                    "delivery_id",
                    "attempt_owner",
                    "timeout_seconds",
                ],
                &[],
            )?;
            let owner = id(p, "attempt_owner")?;
            let delivery = id(p, "delivery_id")?;
            let timeout = num(p, "timeout_seconds", 1, 60)?;
            let pending = event_pending(tx, s)?;
            let n = now(tx)?;
            if record["state"] != "active"
                || pending.is_null()
                || pending["delivery_id"] != delivery
                || pending["next_attempt"]
                    .as_i64()
                    .ok_or("invalid pending delivery")?
                    > n
                || pending["attempt_until"].as_i64().is_some_and(|v| v > n)
            {
                return Ok(json!({"send":false}));
            }
            let attempt = num(&pending, "attempts", 0, i64::MAX - 1)? + 1;
            let next = n + (1_i64 << attempt.min(12)).min(3600) * 1000;
            let until = n + (timeout + 2) * 1000;
            tx.execute("UPDATE event_pending SET attempts=$1,next_attempt=$2,last_error='acknowledgement_unknown',attempt_owner=$3,attempt_until=$4 WHERE subscription=$5",&[&attempt,&next,&owner,&until,&s]).map_err(db_error)?;
            Ok(json!({"send":true,"pending":event_pending(tx,s)?}))
        }
        "event.ack" => {
            fields(
                p,
                &[
                    "principal",
                    "subscription_id",
                    "delivery_id",
                    "attempt_owner",
                ],
                &[],
            )?;
            let delivery = id(p, "delivery_id")?;
            let owner = id(p, "attempt_owner")?;
            let pending = event_pending(tx, s)?;
            if pending.is_null() {
                let old = tx
                    .query_opt(
                        "SELECT delivery_id FROM event_acknowledgements WHERE subscription=$1",
                        &[&s],
                    )
                    .map_err(db_error)?;
                if old.as_ref().map(|r| r.get::<_, &str>(0)) == Some(delivery) {
                    return Ok(json!({"acknowledged":true}));
                }
                return err("event_ack_conflict");
            }
            if pending["delivery_id"] != delivery || pending["attempt_owner"] != owner {
                return err("event_ack_conflict");
            }
            let through = num(&pending, "next_after", 0, i64::MAX)?;
            let timestamp = now(tx)?;
            tx.execute("INSERT INTO event_acknowledgements VALUES($1,$2,$3,$4) ON CONFLICT(subscription) DO UPDATE SET delivery_id=$2,through_event=$3,acknowledged=$4",&[&s,&delivery,&through,&timestamp]).map_err(db_error)?;
            tx.execute(
                "UPDATE event_subscriptions SET cursor=$1 WHERE id=$2",
                &[&through, &s],
            )
            .map_err(db_error)?;
            tx.execute("DELETE FROM event_pending WHERE subscription=$1", &[&s])
                .map_err(db_error)?;
            Ok(json!({"acknowledged":true}))
        }
        _ => err("unknown event command"),
    }
}
fn health(tx: &mut Transaction) -> Result<Value> {
    let row=tx.query_one("SELECT max_runs,max_calls,max_microusd,runs,calls,microusd FROM host_scopes WHERE id='deployment'",&[]).map_err(db_error)?;
    let policy = json!({"max_runs":row.get::<_,i64>(0),"max_calls":row.get::<_,i64>(1),"max_reserved_microusd":row.get::<_,i64>(2)});
    let usage = json!({"runs":row.get::<_,i64>(3),"calls":row.get::<_,i64>(4),"reserved_microusd":row.get::<_,i64>(5)});
    let identity = tx
        .query_one(
            "SELECT deployment,epoch FROM authority WHERE singleton",
            &[],
        )
        .map_err(db_error)?;
    let draining = control(tx)?["draining"] == true;
    let database_bytes:i64=tx.query_one("SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','m')",&[]).map_err(db_error)?.get(0);
    let content=tx.query_one("SELECT count(*),coalesce(sum(size),0)::bigint,count(*) FILTER(WHERE NOT verified) FROM artifact_objects",&[]).map_err(db_error)?;
    let retained: i64 = tx
        .query_one("SELECT count(*) FROM host_runs", &[])
        .map_err(db_error)?
        .get(0);
    let pending: i64 = tx
        .query_one(
            "SELECT count(*) FROM artifact_links WHERE state<>'released'",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    Ok(
        json!({"format":"mas-shared-health/1","deployment_id":identity.get::<_,&str>(0),"epoch":identity.get::<_,i64>(1),"policy":policy,"usage":usage,"retained_runs":retained,"admission_state":if draining{"draining"}else{"active"},"admission_open":!draining&&row.get::<_,i64>(3)<row.get::<_,i64>(0)&&row.get::<_,i64>(4)<row.get::<_,i64>(1)&&row.get::<_,i64>(5)<row.get::<_,i64>(2),"state_bytes":database_bytes,"storage":{"database_bytes":database_bytes,"artifact_objects":content.get::<_,i64>(0),"artifact_reserved_bytes":content.get::<_,i64>(1),"unverified_artifact_objects":content.get::<_,i64>(2),"retained_artifact_links":pending},"expenditure_semantics":"reserved allocation; actual provider invoices require separate account controls"}),
    )
}
fn pagination(p: &Value) -> Result<(i64, i64)> {
    let offset = if p.get("offset").is_some() {
        num(p, "offset", 0, i64::MAX)?
    } else {
        0
    };
    let limit = if p.get("limit").is_some() {
        num(p, "limit", 1, 20)?
    } else {
        20
    };
    Ok((offset, limit))
}
fn budget(tx: &mut Transaction, p: &Value) -> Result<Value> {
    fields(p, &[], &["offset", "limit"])?;
    let (offset, limit) = pagination(p)?;
    let count: i64 = tx
        .query_one("SELECT count(*) FROM operator_budgets", &[])
        .map_err(db_error)?
        .get(0);
    let mut items = Vec::new();
    for row in tx
        .query(
            "SELECT receipt FROM operator_budgets ORDER BY revision OFFSET $1 LIMIT $2",
            &[&offset, &limit],
        )
        .map_err(db_error)?
    {
        items.push(decode(row.get::<_, &str>(0))?);
    }
    let next = offset + items.len() as i64;
    Ok(
        json!({"health":health(tx)?,"revision":count,"amendments":{"items":items,"total":count,"next_offset":if next<count{Some(next)}else{None}}}),
    )
}
fn budget_amend(tx: &mut Transaction, p: &Value) -> Result<Value> {
    fields(
        p,
        &[
            "principal",
            "request_key",
            "reason",
            "expected_revision",
            "limits",
        ],
        &[],
    )?;
    let principal = id(p, "principal")?;
    let key = id(p, "request_key")?;
    let reason = field(p, "reason")?;
    if reason.trim().is_empty() || reason.len() > 2048 || reason.chars().any(|c| c.is_control()) {
        return err("invalid_budget_amendment");
    }
    let expected = num(p, "expected_revision", 0, i64::MAX)?;
    let limits = p["limits"].as_object().ok_or("invalid_budget_amendment")?;
    if limits.is_empty()
        || limits.keys().any(|k| {
            !matches!(
                k.as_str(),
                "max_runs" | "max_calls" | "max_reserved_microusd"
            )
        })
    {
        return err("invalid_budget_amendment");
    }
    let digest = hash(p);
    if let Some(row) = tx
        .query_opt(
            "SELECT digest,receipt FROM operator_budgets WHERE request_key=$1",
            &[&key],
        )
        .map_err(db_error)?
    {
        if row.get::<_, &str>(0) != digest {
            return err("budget_amendment_conflict");
        }
        return decode(row.get::<_, &str>(1));
    }
    let revision: i64 = tx
        .query_one(
            "SELECT coalesce(max(revision),0) FROM operator_budgets",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    if revision != expected {
        return err("budget_revision_conflict");
    }
    if revision >= 10000 {
        return err("budget_history_full");
    }
    let old = tx
        .query_one(
            "SELECT max_runs,max_calls,max_microusd FROM host_scopes WHERE id='deployment'",
            &[],
        )
        .map_err(db_error)?;
    let mut new = [
        old.get::<_, i64>(0),
        old.get::<_, i64>(1),
        old.get::<_, i64>(2),
    ];
    let mut changed = false;
    for (index, name) in ["max_runs", "max_calls", "max_reserved_microusd"]
        .iter()
        .enumerate()
    {
        if limits.contains_key(*name) {
            let value = num(&p["limits"], name, 1, i64::MAX)?;
            if value < new[index] {
                return err("budget_must_increase");
            }
            changed |= value > new[index];
            new[index] = value;
        }
    }
    if !changed {
        return err("budget_must_increase");
    }
    tx.execute("INSERT INTO operator_budget_base SELECT id,max_runs,max_calls,max_microusd FROM host_scopes WHERE id='deployment' ON CONFLICT(scope) DO NOTHING",&[]).map_err(db_error)?;
    tx.execute(
        "UPDATE host_scopes SET max_runs=$1,max_calls=$2,max_microusd=$3 WHERE id='deployment'",
        &[&new[0], &new[1], &new[2]],
    )
    .map_err(db_error)?;
    let receipt = json!({"revision":revision+1,"request_key":key,"principal":principal,"reason":reason,"policy":{"max_runs":new[0],"max_calls":new[1],"max_reserved_microusd":new[2]},"created":now(tx)?/1000});
    tx.execute(
        "INSERT INTO operator_budgets VALUES($1,$2,$3,$4)",
        &[&(revision + 1), &key, &digest, &receipt.to_string()],
    )
    .map_err(db_error)?;
    Ok(receipt)
}
fn settled_run(tx: &mut Transaction, r: &str) -> Result<bool> {
    let unfinished=tx.query_one("SELECT EXISTS(SELECT 1 FROM host_runs h CROSS JOIN authority a WHERE h.run=$1 AND h.owner IS NOT NULL AND h.owner_epoch=a.epoch) OR EXISTS(SELECT 1 FROM tasks WHERE run=$1 AND state='open') OR EXISTS(SELECT 1 FROM host_invocations WHERE run=$1 AND state NOT IN ('completed','resolved','retry_allowed','not_executed','failed_known')) OR EXISTS(SELECT 1 FROM calls c WHERE c.run=$1 AND NOT EXISTS(SELECT 1 FROM host_invocations i WHERE i.run=c.run AND i.call_id=c.id)) OR EXISTS(SELECT 1 FROM worker_jobs WHERE run_id=$1 AND state IN ('pending','claimed','dispatched','unknown')) OR EXISTS(SELECT 1 FROM remote_effects WHERE run=$1 AND state NOT IN ('completed','failed','cancelled')) OR EXISTS(SELECT 1 FROM remote_operations WHERE run=$1 AND state<>'completed')",&[&r]).map_err(db_error)?.get::<_,bool>(0);
    Ok(!unfinished)
}
fn archive_preview(tx: &mut Transaction, p: &Value) -> Result<Value> {
    fields(p, &["kind", "older_than_seconds"], &["offset", "limit"])?;
    let kind = field(p, "kind")?;
    let age = num(p, "older_than_seconds", 0, i64::MAX / 1000)?;
    let (offset, limit) = pagination(p)?;
    let cutoff = now(tx)?.saturating_sub(age * 1000);
    let retained_links: i64 = tx
        .query_one(
            "SELECT count(*) FROM artifact_links WHERE state<>'released'",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    let (sql,count_sql,key)=match kind{"tasks"=>("SELECT id,state,updated,owner IS NOT NULL AND owner_epoch=(SELECT epoch FROM authority WHERE singleton) FROM service_tasks ORDER BY created,id OFFSET $1 LIMIT $2","SELECT count(*) FROM service_tasks","task_id"),"workflows"=>("SELECT id,state,updated,owner IS NOT NULL AND owner_epoch=(SELECT epoch FROM authority WHERE singleton) FROM composition_instances ORDER BY created,id OFFSET $1 LIMIT $2","SELECT count(*) FROM composition_instances","instance_id"),"runs"=>("SELECT h.run,'completed',((r.deadline-(r.config::jsonb->>'wall_seconds')::bigint)*1000)::bigint,h.owner IS NOT NULL AND h.owner_epoch=(SELECT epoch FROM authority WHERE singleton) FROM host_runs h JOIN runs r ON r.id=h.run ORDER BY h.run OFFSET $1 LIMIT $2","SELECT count(*) FROM host_runs","run_id"),_=>return err("invalid_archive_kind")};
    let total: i64 = tx.query_one(count_sql, &[]).map_err(db_error)?.get(0);
    let mut items = Vec::new();
    for row in tx.query(sql, &[&offset, &limit]).map_err(db_error)? {
        let entity = row.get::<_, &str>(0);
        let state = row.get::<_, &str>(1);
        let mut reason = if !matches!(state, "completed" | "failed" | "cancelled" | "expired") {
            Some("work is not terminal")
        } else if row.get::<_, i64>(2) > cutoff {
            Some("retention age not reached")
        } else if row.get::<_, bool>(3) {
            Some("current controller still owns work")
        } else if retained_links > 0 {
            Some("shared artefact retention links remain")
        } else {
            None
        };
        if reason.is_none() {
            let references = match kind {
                "tasks" => tx
                    .query(
                        "SELECT run,state FROM service_executions WHERE task=$1",
                        &[&entity],
                    )
                    .map_err(db_error)?,
                "workflows" => tx
                    .query(
                        "SELECT run_id,state FROM composition_attempts WHERE instance=$1",
                        &[&entity],
                    )
                    .map_err(db_error)?,
                _ => Vec::new(),
            };
            if kind == "runs" && !settled_run(tx, entity)? {
                reason = Some("unsettled invocation or remote effect retained");
            }
            for reference in references {
                if !matches!(
                    reference.get::<_, &str>(1),
                    "completed" | "failed" | "cancelled" | "expired"
                ) || !settled_run(tx, reference.get::<_, &str>(0))?
                {
                    reason = Some("unsettled invocation or remote effect retained");
                    break;
                }
            }
        }
        let mut result = json!({"eligible":reason.is_none(),"archived":false});
        result[key] = json!(entity);
        if let Some(reason) = reason {
            result["reason"] = json!(reason);
        }
        items.push(result);
    }
    let next = offset + items.len() as i64;
    Ok(
        json!({"kind":kind,"dry_run":true,"items":items,"total":total,"next_offset":if next<total{Some(next)}else{None},"allocation_changed":false}),
    )
}
