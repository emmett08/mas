//! Shared peer, provider circuit and remote-effect repositories.
//! Domain commands run under the same fenced authority transaction as native
//! host admission. An immutable command receipt is never dispatch permission.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;

pub(crate) const DDL: &str = "
CREATE TABLE federation_peers(name text NOT NULL,version text NOT NULL,identity text NOT NULL UNIQUE,descriptor text NOT NULL,revoked boolean NOT NULL DEFAULT false,PRIMARY KEY(name,version));
CREATE TABLE federation_aliases(name text PRIMARY KEY,version text NOT NULL);
CREATE TABLE federation_changes(revision bigint PRIMARY KEY,request_key text UNIQUE NOT NULL,digest text NOT NULL,receipt text NOT NULL,operator text NOT NULL,reason text NOT NULL,created bigint NOT NULL);
CREATE TABLE provider_circuits(resource text PRIMARY KEY,policy text NOT NULL,failures bigint NOT NULL DEFAULT 0,open_until_ms bigint NOT NULL DEFAULT 0,generation bigint NOT NULL DEFAULT 0,probe_call text,probe_deadline_ms bigint);
CREATE TABLE provider_permits(call_id text PRIMARY KEY,resource text NOT NULL REFERENCES provider_circuits(resource),generation bigint NOT NULL,probe boolean NOT NULL,observation text,created_ms bigint NOT NULL,observed_ms bigint);
CREATE TABLE remote_effects(key text PRIMARY KEY,namespace text NOT NULL,logical_key text NOT NULL,binding text NOT NULL,descriptor text NOT NULL,input text NOT NULL,state text NOT NULL,remote_id text,context_id text,result text,run text NOT NULL REFERENCES host_runs(run),call_id text NOT NULL,cancel_requested boolean NOT NULL DEFAULT false,pending_action text,active_operation text,active_epoch bigint,created bigint NOT NULL,updated bigint NOT NULL);
CREATE TABLE remote_operations(key text NOT NULL REFERENCES remote_effects(key),operation_key text NOT NULL,action text NOT NULL,binding text NOT NULL,state text NOT NULL,output text,run text NOT NULL,call_id text NOT NULL,epoch bigint NOT NULL,selected text,PRIMARY KEY(key,operation_key),FOREIGN KEY(run,call_id) REFERENCES host_invocations(run,call_id));
CREATE TABLE remote_audit(seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,key text NOT NULL REFERENCES remote_effects(key),event text NOT NULL,evidence text NOT NULL,created bigint NOT NULL);
";
pub(crate) const ARCHIVE_DDL: &str =
    "ALTER TABLE remote_effects ADD COLUMN archived boolean NOT NULL DEFAULT false;";

pub(crate) fn archive_run(tx: &mut Transaction<'_>, run: &str) -> Result<Value> {
    let mut saved = Vec::new();
    for r in tx.query("SELECT key,input,result,state,remote_id,archived FROM remote_effects WHERE run=$1 ORDER BY key",&[&run]).map_err(db_error)? {
        if r.get::<_,bool>(5) { continue }
        if !terminal(r.get(3)) { return Err("run_not_settled".into()) }
        let key:&str=r.get(0);
        let input=json!({"format":"mas-archived-payload/1","sha256":digest(r.get(1))});
        let output=r.get::<_,Option<&str>>(2).map(|s|json!({"format":"mas-archived-payload/1","sha256":digest(s)}));
        tx.execute("UPDATE remote_effects SET input=$2,result=$3,archived=true WHERE key=$1",&[&key,&input.to_string(),&output.as_ref().map(Value::to_string)]).map_err(db_error)?;
        for operation in tx.query("SELECT operation_key,output FROM remote_operations WHERE key=$1 AND output IS NOT NULL",&[&key]).map_err(db_error)? {
            let retained=json!({"format":"mas-archived-payload/1","sha256":digest(operation.get(1))});
            tx.execute("UPDATE remote_operations SET output=$3 WHERE key=$1 AND operation_key=$2",&[&key,&operation.get::<_,&str>(0),&retained.to_string()]).map_err(db_error)?;
        }
        for event in tx.query("SELECT seq,evidence FROM remote_audit WHERE key=$1",&[&key]).map_err(db_error)? {
            let retained=json!({"format":"mas-archived-payload/1","sha256":digest(event.get(1))});
            tx.execute("UPDATE remote_audit SET evidence=$2 WHERE seq=$1",&[&event.get::<_,i64>(0),&retained.to_string()]).map_err(db_error)?;
        }
        saved.push(json!({"key":key,"remote_id":r.get::<_,Option<String>>(4),"input":input,"result":output}));
    }
    Ok(json!(saved))
}

fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or_else(|| format!("invalid {k}"))
}
fn integer(v: &Value, k: &str, min: i64, max: i64) -> Result<i64> {
    v[k].as_i64()
        .filter(|n| *n >= min && *n <= max)
        .ok_or_else(|| format!("invalid {k}"))
}
fn decoded(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| "invalid retained remote state".into())
}
fn digest(s: &str) -> String {
    Sha256::digest(s.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn fields(v: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let o = v.as_object().ok_or("invalid remote parameters")?;
    if required.iter().any(|k| !o.contains_key(*k))
        || o.keys()
            .any(|k| !required.contains(&k.as_str()) && !optional.contains(&k.as_str()))
    {
        return Err("unsupported remote parameters".into());
    }
    Ok(())
}
fn now(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0))
}
fn epoch(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one("SELECT epoch FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0))
}
fn terminal(s: &str) -> bool {
    matches!(s, "completed" | "failed" | "cancelled")
}

fn peer_change(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    let required = if op == "peer.register" {
        vec![
            "operator",
            "request_key",
            "reason",
            "expected_revision",
            "encoded",
        ]
    } else {
        vec![
            "operator",
            "request_key",
            "reason",
            "expected_revision",
            "name",
            "version",
        ]
    };
    fields(p, &required, &[])?;
    text(p, "operator")?;
    text(p, "reason")?;
    let key = text(p, "request_key")?;
    let expected = integer(p, "expected_revision", 0, 10000)?;
    let fingerprint = digest(&json!([op, p]).to_string());
    if let Some(row) = tx
        .query_opt(
            "SELECT digest,receipt FROM federation_changes WHERE request_key=$1",
            &[&key],
        )
        .map_err(db_error)?
    {
        if row.get::<_, String>(0) != fingerprint {
            return Err("peer change conflict".into());
        }
        return decoded(row.get::<_, &str>(1));
    }
    let revision: i64 = tx
        .query_one(
            "SELECT coalesce(max(revision),0)::bigint FROM federation_changes",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    if revision != expected || revision >= 10000 {
        return Err("peer revision conflict or registry full".into());
    }
    let command = if op == "peer.register" {
        let encoded = p["encoded"]
            .as_str()
            .filter(|s| s.len() <= 32768)
            .ok_or("invalid peer encoding")?;
        let reg = decoded(encoded)?;
        fields(
            &reg,
            &[
                "format",
                "name",
                "version",
                "profile",
                "endpoint",
                "outbound_principal",
                "deployment_id",
                "workflows",
                "callers",
                "capabilities",
                "artifact_scopes",
                "allow_loopback_http",
            ],
            &[],
        )?;
        if reg["format"] != "mas-peer-registration/1"
            || !matches!(
                text(&reg, "profile")?,
                "mas-http/1" | "mas-mcp/1" | "mas-a2a/1"
            )
            || reg["workflows"]
                .as_object()
                .is_none_or(|w| w.is_empty() || w.len() > 128)
            || reg["callers"]
                .as_array()
                .is_none_or(|v| v.is_empty() || v.len() > 256)
        {
            return Err("invalid peer registration".into());
        }
        let name = text(&reg, "name")?;
        let version = text(&reg, "version")?;
        let identity = digest(encoded);
        if let Some(row) = tx
            .query_opt(
                "SELECT identity,revoked FROM federation_peers WHERE name=$1 AND version=$2",
                &[&name, &version],
            )
            .map_err(db_error)?
        {
            if row.get::<_, String>(0) != identity || row.get::<_, bool>(1) {
                return Err("peer version immutable".into());
            }
        } else {
            tx.execute("INSERT INTO federation_peers(name,version,identity,descriptor) VALUES($1,$2,$3,$4)",&[&name,&version,&identity,&encoded]).map_err(db_error)?;
        }
        tx.execute("INSERT INTO federation_aliases VALUES($1,$2) ON CONFLICT(name) DO UPDATE SET version=EXCLUDED.version",&[&name,&version]).map_err(db_error)?;
        json!({"action":"register","registration":reg})
    } else {
        let name = text(p, "name")?;
        let version = text(p, "version")?;
        if tx
            .execute(
                "UPDATE federation_peers SET revoked=true WHERE name=$1 AND version=$2",
                &[&name, &version],
            )
            .map_err(db_error)?
            != 1
        {
            return Err("peer unavailable".into());
        }
        json!({"action":"revoke","name":name,"version":version})
    };
    let receipt = json!({"revision":revision+1,"request_key":key,"command":command});
    let timestamp = now(tx)?;
    tx.execute(
        "INSERT INTO federation_changes VALUES($1,$2,$3,$4,$5,$6,$7)",
        &[
            &(revision + 1),
            &key,
            &fingerprint,
            &receipt.to_string(),
            &text(p, "operator")?,
            &text(p, "reason")?,
            &timestamp,
        ],
    )
    .map_err(db_error)?;
    Ok(receipt)
}

fn circuit_policy(p: &Value) -> Result<(String, i64, i64, i64)> {
    let policy = &p["policy"];
    fields(
        policy,
        &[
            "format",
            "resource_key",
            "failure_threshold",
            "open_seconds",
            "half_open_seconds",
        ],
        &[],
    )?;
    if policy["format"] != "mas-provider-circuit/1" {
        return Err("invalid circuit profile".into());
    }
    Ok((
        text(policy, "resource_key")?.into(),
        integer(policy, "failure_threshold", 1, 100)?,
        integer(policy, "open_seconds", 1, 3600)? * 1000,
        integer(policy, "half_open_seconds", 1, 600)? * 1000,
    ))
}
fn circuit(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    fields(
        p,
        &["policy", "call_id"],
        if op == "circuit.observe" {
            &["category"]
        } else {
            &[]
        },
    )?;
    let (resource, threshold, cooldown, lease) = circuit_policy(p)?;
    let call = text(p, "call_id")?;
    let timestamp = now(tx)?;
    let policy = p["policy"].to_string();
    tx.execute(
        "INSERT INTO provider_circuits(resource,policy) VALUES($1,$2) ON CONFLICT DO NOTHING",
        &[&resource, &policy],
    )
    .map_err(db_error)?;
    let row=tx.query_one("SELECT policy,failures,open_until_ms,generation,probe_call,probe_deadline_ms FROM provider_circuits WHERE resource=$1 FOR UPDATE",&[&resource]).map_err(db_error)?;
    if row.get::<_, String>(0) != policy {
        return Err("provider circuit policy conflict".into());
    }
    let failures: i64 = row.get(1);
    let until: i64 = row.get(2);
    let generation: i64 = row.get(3);
    let probe: Option<String> = row.get(4);
    let deadline: Option<i64> = row.get(5);
    let permit = tx
        .query_opt(
            "SELECT resource,generation,probe,observation FROM provider_permits WHERE call_id=$1",
            &[&call],
        )
        .map_err(db_error)?;
    if op == "circuit.admit" {
        if let Some(permit) = permit {
            return Ok(
                json!({"admitted":permit.get::<_,String>(0)==resource&&permit.get::<_,i64>(1)==generation&&permit.get::<_,Option<String>>(3).is_none()}),
            );
        }
        if until > timestamp {
            return Ok(json!({"admitted":false}));
        }
        let probing = failures >= threshold;
        if probing && probe.is_some() {
            if deadline.is_some_and(|d| d <= timestamp) {
                tx.execute("UPDATE provider_permits SET observation='abandoned',observed_ms=$1 WHERE call_id=$2 AND observation IS NULL",&[&timestamp,&probe]).map_err(db_error)?;
                tx.execute("UPDATE provider_circuits SET open_until_ms=$1,generation=generation+1,probe_call=NULL,probe_deadline_ms=NULL WHERE resource=$2",&[&(timestamp+cooldown),&resource]).map_err(db_error)?;
            }
            return Ok(json!({"admitted":false}));
        }
        tx.execute("INSERT INTO provider_permits(call_id,resource,generation,probe,created_ms) VALUES($1,$2,$3,$4,$5)",&[&call,&resource,&generation,&probing,&timestamp]).map_err(db_error)?;
        if probing {
            tx.execute(
                "UPDATE provider_circuits SET probe_call=$1,probe_deadline_ms=$2 WHERE resource=$3",
                &[&call, &(timestamp + lease), &resource],
            )
            .map_err(db_error)?;
        }
        return Ok(json!({"admitted":true}));
    }
    let category = text(p, "category")?;
    if !matches!(category, "success" | "provider_failure" | "aborted") {
        return Err("invalid circuit observation".into());
    }
    let permit = permit.ok_or("provider circuit permit missing")?;
    if permit.get::<_, String>(0) != resource {
        return Err("provider circuit permit mismatch".into());
    }
    if permit.get::<_, Option<String>>(3).is_some() {
        return Ok(json!({"observed":true}));
    }
    tx.execute(
        "UPDATE provider_permits SET observation=$1,observed_ms=$2 WHERE call_id=$3",
        &[&category, &timestamp, &call],
    )
    .map_err(db_error)?;
    if permit.get::<_, i64>(1) != generation {
        return Ok(json!({"observed":true}));
    }
    if permit.get::<_, bool>(2) {
        if probe.as_deref() != Some(call) {
            return Err("provider probe ownership changed".into());
        }
        let success = category == "success";
        tx.execute("UPDATE provider_circuits SET failures=$1,open_until_ms=$2,generation=generation+1,probe_call=NULL,probe_deadline_ms=NULL WHERE resource=$3",&[&(if success{0}else{failures.max(threshold)}),&(if success{0}else{timestamp+cooldown}),&resource]).map_err(db_error)?;
    } else if category == "success" {
        tx.execute(
            "UPDATE provider_circuits SET failures=0 WHERE resource=$1",
            &[&resource],
        )
        .map_err(db_error)?;
    } else if category == "provider_failure" {
        let count = failures + 1;
        let opened = count >= threshold;
        tx.execute("UPDATE provider_circuits SET failures=$1,open_until_ms=$2,generation=$3 WHERE resource=$4",&[&count,&(if opened{timestamp+cooldown}else{0}),&(generation+i64::from(opened)),&resource]).map_err(db_error)?;
    }
    Ok(json!({"observed":true}))
}

pub(crate) fn apply(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    match op {
        "peer.register" | "peer.revoke" => peer_change(tx, op, p),
        "circuit.admit" | "circuit.observe" => circuit(tx, op, p),
        _ => remote_command(tx, op, p),
    }
}
pub(crate) fn query(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    match op {
        "peer.status" => {
            fields(p, &[], &[])?;
            let revision: i64 = tx
                .query_one(
                    "SELECT coalesce(max(revision),0)::bigint FROM federation_changes",
                    &[],
                )
                .map_err(db_error)?
                .get(0);
            let mut peers = Vec::new();
            for row in tx.query("SELECT p.identity,p.revoked,coalesce(a.version=p.version,false),p.descriptor FROM federation_peers p LEFT JOIN federation_aliases a USING(name) ORDER BY p.name,p.version",&[]).map_err(db_error)?{peers.push(json!({"identity":row.get::<_,String>(0),"revoked":row.get::<_,bool>(1),"current":row.get::<_,bool>(2),"registration":decoded(row.get::<_,&str>(3))?}));}
            Ok(json!({"format":"mas-peer-registry/1","revision":revision,"peers":peers}))
        }
        "peer.resolve" => {
            fields(p, &["name", "caller"], &["version", "identity"])?;
            let name = text(p, "name")?;
            let caller = text(p, "caller")?;
            let version = if let Some(v) = p.get("version") {
                v.as_str().ok_or("invalid peer version")?.to_owned()
            } else {
                tx.query_opt(
                    "SELECT version FROM federation_aliases WHERE name=$1",
                    &[&name],
                )
                .map_err(db_error)?
                .ok_or("peer unavailable")?
                .get(0)
            };
            let row=tx.query_opt("SELECT identity,descriptor,revoked FROM federation_peers WHERE name=$1 AND version=$2",&[&name,&version]).map_err(db_error)?.ok_or("peer unavailable")?;
            if row.get::<_, bool>(2) {
                return Err("peer unavailable".into());
            }
            let identity: String = row.get(0);
            let descriptor = decoded(row.get::<_, &str>(1))?;
            if p.get("identity")
                .is_some_and(|v| v.as_str() != Some(&identity))
            {
                return Err("peer binding changed".into());
            }
            if !descriptor["callers"]
                .as_array()
                .is_some_and(|v| v.iter().any(|item| item.as_str() == Some(caller)))
            {
                return Err("peer forbidden".into());
            }
            Ok(json!({"identity":identity,"registration":descriptor}))
        }
        "circuit.status" => {
            fields(p, &["policy"], &[])?;
            let (resource, threshold, _, _) = circuit_policy(p)?;
            if let Some(row)=tx.query_opt("SELECT policy,failures,open_until_ms,probe_call FROM provider_circuits WHERE resource=$1",&[&resource]).map_err(db_error)?{
            if row.get::<_,String>(0)!=p["policy"].to_string(){return Err("provider circuit policy conflict".into())}let failures:i64=row.get(1);let probe:Option<String>=row.get(3);Ok(json!({"resource_key":resource,"state":if probe.is_some(){"half_open"}else if failures>=threshold{"open"}else{"closed"},"failures":failures,"open_until_ms":row.get::<_,i64>(2),"probe_call":probe}))
        }else{Ok(json!({"resource_key":resource,"state":"closed","failures":0,"open_until_ms":0,"probe_call":null}))}
        }
        _ => remote_query(tx, op, p),
    }
}

fn remote_key(p: &Value) -> Result<String> {
    let namespace = text(p, "namespace")?;
    let logical = text(p, "logical_key")?;
    if namespace.len() != 64
        || !namespace.bytes().all(|b| b.is_ascii_hexdigit())
        || logical.len() > 200
        || !logical
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
    {
        return Err("invalid remote identity".into());
    }
    Ok(digest(&format!("{namespace}\0{logical}")))
}
fn admitted(tx: &mut Transaction<'_>, p: &Value) -> Result<()> {
    let r = text(p, "run")?;
    let c = text(p, "call_id")?;
    let reservation = integer(p, "reservation", 0, 100_000_000)?;
    let row=tx.query_opt("SELECT i.state,i.reservation_microusd,h.owner_epoch,a.epoch,h.owner FROM host_invocations i JOIN host_runs h ON h.run=i.run CROSS JOIN authority a WHERE i.run=$1 AND i.call_id=$2 AND a.singleton",&[&r,&c]).map_err(db_error)?.ok_or("remote effect requires native admission")?;
    if row.get::<_, String>(0) != "executing"
        || row.get::<_, i64>(1) != reservation
        || row.get::<_, Option<i64>>(2) != Some(row.get(3))
        || row.get::<_, Option<String>>(4).is_none()
    {
        return Err("remote effect admission or owner is stale".into());
    }
    Ok(())
}
fn remote_row(tx: &mut Transaction<'_>, key: &str) -> Result<Value> {
    let version: i64 = tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if version >= 10
        && tx
            .query_opt("SELECT archived FROM remote_effects WHERE key=$1", &[&key])
            .map_err(db_error)?
            .is_some_and(|r| r.get::<_, bool>(0))
    {
        return Err("payload_archived".into());
    }
    let row=tx.query_opt("SELECT namespace,logical_key,binding,descriptor,state,remote_id,context_id,result,run,call_id,cancel_requested,pending_action,active_operation,active_epoch,created,updated FROM remote_effects WHERE key=$1",&[&key]).map_err(db_error)?.ok_or("remote task not found")?;
    Ok(
        json!({"namespace":row.get::<_,String>(0),"key":row.get::<_,String>(1),"binding":row.get::<_,String>(2),"descriptor":decoded(row.get::<_,&str>(3))?,"state":row.get::<_,String>(4),"remote_id":row.get::<_,Option<String>>(5),"context_id":row.get::<_,Option<String>>(6),"result":row.get::<_,Option<String>>(7).as_deref().map(decoded).transpose()?,"host_run_dir":row.get::<_,String>(8),"call_id":row.get::<_,String>(9),"cancel_requested":row.get::<_,bool>(10),"pending_action":row.get::<_,Option<String>>(11),"active_operation":row.get::<_,Option<String>>(12),"active_epoch":row.get::<_,Option<i64>>(13),"created":row.get::<_,i64>(14),"updated":row.get::<_,i64>(15)}),
    )
}
fn remote_public(row: &Value) -> Value {
    let mut value = row.clone();
    let o = value.as_object_mut().unwrap();
    for key in [
        "namespace",
        "descriptor",
        "active_operation",
        "active_epoch",
    ] {
        o.remove(key);
    }
    value
}
fn remote_identity(row: &Value, p: &Value) -> Result<()> {
    if row["namespace"] != p["namespace"] || row["descriptor"] != p["descriptor"] {
        return Err("remote identity conflict".into());
    }
    Ok(())
}
fn audit(tx: &mut Transaction<'_>, key: &str, event: &str, evidence: Value) -> Result<()> {
    let timestamp = now(tx)? / 1000;
    tx.execute(
        "INSERT INTO remote_audit(key,event,evidence,created) VALUES($1,$2,$3,$4)",
        &[&key, &event, &evidence.to_string(), &timestamp],
    )
    .map_err(db_error)?;
    Ok(())
}
fn remote_command(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    if op == "remote.submit_begin" {
        fields(
            p,
            &[
                "namespace",
                "logical_key",
                "descriptor",
                "payload",
                "run",
                "call_id",
                "reservation",
            ],
            &[],
        )?;
        if !p["descriptor"].is_object() || !p["payload"].is_object() {
            return Err("invalid remote binding".into());
        }
        admitted(tx, p)?;
        let key = remote_key(p)?;
        let fingerprint =
            digest(&json!({"descriptor":p["descriptor"],"input":p["payload"]}).to_string());
        if tx
            .query_opt("SELECT key FROM remote_effects WHERE key=$1", &[&key])
            .map_err(db_error)?
            .is_some()
        {
            let row = remote_row(tx, &key)?;
            remote_identity(&row, p)?;
            if row["binding"] != fingerprint {
                return Err("remote idempotency conflict".into());
            }
            if row["remote_id"].is_null() && !terminal(text(&row, "state")?) {
                return Err("remote outcome unknown".into());
            }
            return Ok(json!({"dispatch":false,"output":remote_public(&row)}));
        }
        if tx
            .query_one("SELECT count(*)::bigint FROM remote_effects", &[])
            .map_err(db_error)?
            .get::<_, i64>(0)
            >= 100_000
        {
            return Err("remote intent capacity exceeded".into());
        }
        let timestamp = now(tx)? / 1000;
        let generation = epoch(tx)?;
        let r = text(p, "run")?;
        let c = text(p, "call_id")?;
        tx.execute("INSERT INTO remote_effects(key,namespace,logical_key,binding,descriptor,input,state,run,call_id,pending_action,active_operation,active_epoch,created,updated) VALUES($1,$2,$3,$4,$5,$6,'unknown',$7,$8,'submit','submit',$9,$10,$10)",&[&key,&text(p,"namespace")?,&text(p,"logical_key")?,&fingerprint,&p["descriptor"].to_string(),&p["payload"].to_string(),&r,&c,&generation,&timestamp]).map_err(db_error)?;
        tx.execute("INSERT INTO remote_operations(key,operation_key,action,binding,state,run,call_id,epoch) VALUES($1,'submit','submit',$2,'unknown',$3,$4,$5)",&[&key,&fingerprint,&r,&c,&generation]).map_err(db_error)?;
        audit(tx, &key, "submit_intent", json!({"binding":fingerprint}))?;
        return Ok(json!({"dispatch":true,"operation_key":"submit"}));
    }
    if op == "remote.operation_begin" {
        fields(
            p,
            &[
                "namespace",
                "logical_key",
                "descriptor",
                "run",
                "call_id",
                "reservation",
                "action",
                "operation_key",
                "payload",
                "evidence",
            ],
            &["remote_id"],
        )?;
        admitted(tx, p)?;
        let key = remote_key(p)?;
        let row = remote_row(tx, &key)?;
        remote_identity(&row, p)?;
        let action = text(p, "action")?;
        let operation = text(p, "operation_key")?;
        if !matches!(action, "poll" | "attach" | "update" | "cancel")
            || !p["payload"].is_object()
            || !p["evidence"].is_object()
        {
            return Err("invalid remote operation".into());
        }
        let fingerprint=digest(&json!({"action":action,"payload":p["payload"],"remote_id":p["remote_id"],"evidence":p["evidence"]}).to_string());
        if let Some(previous)=tx.query_opt("SELECT binding,state,output FROM remote_operations WHERE key=$1 AND operation_key=$2",&[&key,&operation]).map_err(db_error)?{
            if previous.get::<_,String>(0)!=fingerprint{return Err("remote idempotency conflict".into())}
            if previous.get::<_,String>(1)!="completed"{return Err("remote outcome unknown".into())}
            return Ok(json!({"dispatch":false,"output":decoded(previous.get::<_,&str>(2))?}))
        }
        if terminal(text(&row, "state")?) {
            return Ok(json!({"dispatch":false,"output":remote_public(&row)}));
        }
        let generation = epoch(tx)?;
        let r = text(p, "run")?;
        let c = text(p, "call_id")?;
        let mut selected = row["remote_id"].as_str();
        if action == "attach" {
            if selected.is_some() || p["evidence"].as_object().is_none_or(|v| v.is_empty()) {
                return Err("reconciliation evidence required".into());
            }
            selected = Some(text(p, "remote_id")?);
        }
        let selected = selected.ok_or("remote outcome unknown")?;
        if action == "update"
            && (!matches!(text(&row, "state")?, "input_required" | "auth_required")
                || !row["pending_action"].is_null()
                || row["cancel_requested"] == true)
        {
            return Err("remote task not continuable".into());
        }
        if action == "cancel"
            && (!row["pending_action"].is_null() || row["cancel_requested"] == true)
        {
            return Err("remote cancellation pending".into());
        }
        // Same-epoch concurrent dispatch is excluded. New-authority observation
        // can repair evidence, but never repeat an unresolved mutation.
        if !row["active_operation"].is_null()
            && action != "attach"
            && (row["active_epoch"] == generation || !matches!(action, "poll"))
        {
            return Err("remote task busy or uncertain".into());
        }
        let count: i64 = tx
            .query_one(
                "SELECT count(*)::bigint FROM remote_operations WHERE key=$1",
                &[&key],
            )
            .map_err(db_error)?
            .get(0);
        if count >= 10000 {
            return Err("remote operation capacity exceeded".into());
        }
        let timestamp = now(tx)? / 1000;
        tx.execute("INSERT INTO remote_operations(key,operation_key,action,binding,state,run,call_id,epoch,selected) VALUES($1,$2,$3,$4,'unknown',$5,$6,$7,$8)",&[&key,&operation,&action,&fingerprint,&r,&c,&generation,&selected]).map_err(db_error)?;
        tx.execute("UPDATE remote_effects SET active_operation=$1,active_epoch=$2,pending_action=CASE WHEN $3 IN ('update','cancel') THEN $3 ELSE pending_action END,cancel_requested=cancel_requested OR $3='cancel',updated=$4 WHERE key=$5",&[&operation,&generation,&action,&timestamp,&key]).map_err(db_error)?;
        audit(
            tx,
            &key,
            &format!("{action}_intent"),
            json!({"operation_key":operation,"remote_id":selected}),
        )?;
        return Ok(json!({"dispatch":true,"operation_key":operation,"remote_id":selected}));
    }
    if op == "remote.complete" {
        fields(
            p,
            &[
                "namespace",
                "logical_key",
                "descriptor",
                "operation_key",
                "run",
                "call_id",
                "snapshot",
            ],
            &[],
        )?;
        let key = remote_key(p)?;
        let row = remote_row(tx, &key)?;
        remote_identity(&row, p)?;
        let operation = text(p, "operation_key")?;
        let saved=tx.query_opt("SELECT action,state,run,call_id,epoch,selected FROM remote_operations WHERE key=$1 AND operation_key=$2",&[&key,&operation]).map_err(db_error)?.ok_or("remote operation missing")?;
        let generation = epoch(tx)?;
        if saved.get::<_, String>(1) != "unknown"
            || saved.get::<_, String>(2) != text(p, "run")?
            || saved.get::<_, String>(3) != text(p, "call_id")?
            || saved.get::<_, i64>(4) != generation
            || row["active_operation"] != operation
            || row["active_epoch"] != generation
        {
            return Err("remote operation changed".into());
        }
        let invocation = tx
            .query_one(
                "SELECT state FROM host_invocations WHERE run=$1 AND call_id=$2",
                &[&text(p, "run")?, &text(p, "call_id")?],
            )
            .map_err(db_error)?;
        if invocation.get::<_, String>(0) != "executing" {
            return Err("remote invocation already settled or unknown".into());
        }
        let action: String = saved.get(0);
        let selected: Option<String> = saved.get(5);
        let snapshot = &p["snapshot"];
        fields(
            snapshot,
            &["remote_id", "state", "result", "context_id"],
            &[],
        )?;
        let state = text(snapshot, "state")?;
        if !matches!(
            state,
            "queued"
                | "running"
                | "blocked"
                | "input_required"
                | "auth_required"
                | "completed"
                | "failed"
                | "cancelled"
                | "unknown"
        ) || (!snapshot["result"].is_null() && !snapshot["result"].is_object())
        {
            return Err("invalid remote snapshot".into());
        }
        let remote_id = if snapshot["remote_id"].is_null() {
            None
        } else {
            Some(text(snapshot, "remote_id")?)
        };
        let context_id = if snapshot["context_id"].is_null() {
            row["context_id"].as_str()
        } else {
            Some(text(snapshot, "context_id")?)
        };
        if remote_id.is_none() && !terminal(state)
            || selected.as_deref().is_some_and(|v| Some(v) != remote_id)
            || row["remote_id"]
                .as_str()
                .is_some_and(|v| Some(v) != remote_id)
            || row["context_id"]
                .as_str()
                .is_some_and(|v| Some(v) != context_id)
        {
            return Err("remote identity conflict".into());
        }
        let timestamp = now(tx)? / 1000;
        if !terminal(text(&row, "state")?) {
            let result = if snapshot["result"].is_null() {
                None
            } else {
                Some(snapshot["result"].to_string())
            };
            let pending = if action == "poll" {
                row["pending_action"].as_str()
            } else {
                None
            };
            tx.execute("UPDATE remote_effects SET remote_id=$1,context_id=$2,state=$3,result=$4,pending_action=$5,active_operation=NULL,active_epoch=NULL,updated=$6 WHERE key=$7",&[&remote_id,&context_id,&state,&result,&pending,&timestamp,&key]).map_err(db_error)?;
        }
        let output = remote_public(&remote_row(tx, &key)?);
        tx.execute("UPDATE remote_operations SET state='completed',output=$1 WHERE key=$2 AND operation_key=$3",&[&output.to_string(),&key,&operation]).map_err(db_error)?;
        audit(
            tx,
            &key,
            &format!("{action}_received"),
            json!({"remote_id":remote_id,"state":state}),
        )?;
        return Ok(output);
    }
    if op == "remote.resolve" {
        fields(
            p,
            &[
                "namespace",
                "logical_key",
                "descriptor",
                "operation_key",
                "evidence",
                "decision",
            ],
            &[],
        )?;
        let key = remote_key(p)?;
        let row = remote_row(tx, &key)?;
        remote_identity(&row, p)?;
        let operation = text(p, "operation_key")?;
        let decision = text(p, "decision")?;
        if p["evidence"].as_object().is_none_or(|v| v.is_empty()) {
            return Err("reconciliation evidence required".into());
        }
        let saved = tx
            .query_opt(
                "SELECT action,state FROM remote_operations WHERE key=$1 AND operation_key=$2",
                &[&key, &operation],
            )
            .map_err(db_error)?
            .ok_or("remote operation missing")?;
        let action: String = saved.get(0);
        if saved.get::<_, String>(1) != "unknown"
            || !matches!(action.as_str(), "poll" | "attach" | "update" | "cancel")
        {
            return Err("operation not resolvable".into());
        }
        let observation = matches!(action.as_str(), "poll" | "attach");
        if observation && decision != "observation_unavailable"
            || !observation
                && (!matches!(decision, "applied" | "not_applied")
                    || row["pending_action"] != action)
        {
            return Err("operation not resolvable".into());
        }
        let pending = if observation {
            row["pending_action"].as_str()
        } else {
            None
        };
        let cancel =
            row["cancel_requested"] == true && !(action == "cancel" && decision == "not_applied");
        let state =
            if action == "update" && decision == "applied" && !terminal(text(&row, "state")?) {
                "unknown"
            } else {
                text(&row, "state")?
            };
        let timestamp = now(tx)? / 1000;
        tx.execute("UPDATE remote_effects SET pending_action=$1,cancel_requested=$2,state=$3,active_operation=CASE WHEN active_operation=$4 THEN NULL ELSE active_operation END,active_epoch=CASE WHEN active_operation=$4 THEN NULL ELSE active_epoch END,updated=$5 WHERE key=$6",&[&pending,&cancel,&state,&operation,&timestamp,&key]).map_err(db_error)?;
        let output = remote_public(&remote_row(tx, &key)?);
        tx.execute("UPDATE remote_operations SET state='completed',output=$1 WHERE key=$2 AND operation_key=$3",&[&output.to_string(),&key,&operation]).map_err(db_error)?;
        audit(
            tx,
            &key,
            "operator_resolved",
            json!({"operation_key":operation,"decision":decision,"evidence":p["evidence"]}),
        )?;
        return Ok(output);
    }
    Err("unknown shared remote command".into())
}
fn remote_query(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    if !matches!(op, "remote.status" | "remote.inspect") {
        return Err("unknown shared remote query".into());
    }
    fields(p, &["namespace", "logical_key", "descriptor"], &[])?;
    let key = remote_key(p)?;
    let row = remote_row(tx, &key)?;
    remote_identity(&row, p)?;
    let public = remote_public(&row);
    if op == "remote.status" {
        return Ok(public);
    }
    let mut operations = Vec::new();
    for saved in tx.query("SELECT operation_key,action,state,run,call_id FROM remote_operations WHERE key=$1 ORDER BY operation_key LIMIT 10001",&[&key]).map_err(db_error)?{
        operations.push(json!({"operation_key":saved.get::<_,String>(0),"action":saved.get::<_,String>(1),"state":saved.get::<_,String>(2),"host_run_dir":saved.get::<_,String>(3),"call_id":saved.get::<_,String>(4)}));
    }
    let mut events = Vec::new();
    for event in tx
        .query(
            "SELECT event,evidence,created FROM remote_audit WHERE key=$1 ORDER BY seq LIMIT 20002",
            &[&key],
        )
        .map_err(db_error)?
    {
        events.push(json!({"event":event.get::<_,String>(0),"evidence":decoded(event.get::<_,&str>(1))?,"created":event.get::<_,i64>(2)}));
    }
    Ok(json!({"receipt":public,"operations":operations,"audit":events}))
}
