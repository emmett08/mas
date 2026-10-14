//! Store-owned elapsed clocks. Every timestamp comes from PostgreSQL, and
//! lifecycle writers update the clock in their existing authority transaction.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;
pub(crate) const DDL: &str = "
CREATE TABLE service_clocks(owner text NOT NULL,path text NOT NULL,clock text NOT NULL,PRIMARY KEY(owner,path));
CREATE TABLE service_deadline_amendments(owner text NOT NULL,request_key text NOT NULL,path text NOT NULL,digest text NOT NULL,revision bigint NOT NULL,receipt text NOT NULL,PRIMARY KEY(owner,request_key),UNIQUE(owner,path,revision));
";
const LIMITS: [&str; 4] = [
    "business_seconds",
    "active_seconds",
    "queue_seconds",
    "pause_seconds",
];
fn n(v: &Value, key: &str) -> Result<i64> {
    v[key]
        .as_i64()
        .filter(|v| *v >= 0)
        .ok_or_else(|| format!("invalid clock {key}"))
}
fn decode(v: &str) -> Result<Value> {
    serde_json::from_str(v).map_err(|_| "invalid shared clock".into())
}
pub(crate) fn now(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0))
}
fn policy(value: &Value) -> Result<()> {
    let fields = value.as_object().ok_or("invalid timing policy")?;
    if fields.len() != 5
        || value["format"] != "mas-timing-policy/1"
        || LIMITS
            .iter()
            .any(|k| !matches!(value[k].as_i64(), Some(1..=315_360_000)))
    {
        return Err("invalid timing policy".into());
    }
    Ok(())
}
pub(crate) fn evaluate(clock: &Value, timestamp: i64) -> Result<Value> {
    if clock["format"] != "mas-execution-clock/2" {
        return Err("unsupported shared clock".into());
    }
    policy(&clock["policy"])?;
    let created = n(clock, "created_ms")?;
    let observed = n(clock, "observed_ms")?;
    let started = n(clock, "phase_started_ms")?;
    let mut active = n(clock, "active_ms")?;
    let mut worker = n(clock, "worker_ms")?;
    let branches = n(clock, "active_branches")?;
    let phase = clock["phase"].as_str().ok_or("invalid clock phase")?;
    if !matches!(phase, "queued" | "running" | "paused" | "terminal")
        || created > started
        || started > observed
        || observed > timestamp
        || branches > 1024
        || (phase == "running") != (branches > 0)
        || worker < active
        || clock["worker_time_complete"] != true
    {
        return Err("clock regression or invalid accounting".into());
    }
    let timestamp = if phase == "terminal" {
        observed
    } else {
        timestamp
    };
    let extensions = clock["extensions"]
        .as_object()
        .ok_or("invalid clock extensions")?;
    let mut limits = clock["policy"].clone();
    for (k, v) in extensions {
        if !matches!(
            k.as_str(),
            "business_seconds" | "queue_seconds" | "pause_seconds"
        ) || v
            .as_i64()
            .is_none_or(|v| v < n(&limits, k).unwrap_or(i64::MAX) || v > 315_360_000)
        {
            return Err("invalid clock extension".into());
        }
        limits[k] = v.clone()
    }
    if phase == "running" {
        let interval = timestamp - observed;
        active = active.checked_add(interval).ok_or("clock overflow")?;
        worker = worker
            .checked_add(interval.checked_mul(branches).ok_or("clock overflow")?)
            .ok_or("clock overflow")?;
    }
    let mut expired = Vec::new();
    if timestamp - created >= n(&limits, "business_seconds")? * 1000 {
        expired.push("business_deadline")
    }
    if active >= n(&limits, "active_seconds")? * 1000 {
        expired.push("active_budget")
    }
    if phase == "queued" && timestamp - started >= n(&limits, "queue_seconds")? * 1000 {
        expired.push("queue_deadline")
    }
    if phase == "paused" && timestamp - started >= n(&limits, "pause_seconds")? * 1000 {
        expired.push("pause_deadline")
    }
    Ok(
        json!({"expired":expired,"active_ms":active,"worker_ms":worker,"worker_ms_lower_bound":worker,"worker_time_complete":true,"deadline_revision":n(clock,"deadline_revision")?,"remaining_active_ms":(n(&limits,"active_seconds")?*1000-active).max(0),"business_expires_ms":created.checked_add(n(&limits,"business_seconds")?*1000).ok_or("clock overflow")?,"phase":phase,"observed_at_ms":timestamp}),
    )
}
pub(crate) fn read(tx: &mut Transaction<'_>, owner: &str, path: &str) -> Result<Value> {
    tx.query_opt(
        "SELECT clock FROM service_clocks WHERE owner=$1 AND path=$2",
        &[&owner, &path],
    )
    .map_err(db_error)?
    .map(|r| decode(r.get(0)))
    .unwrap_or(Ok(Value::Null))
}
pub(crate) fn start(tx: &mut Transaction<'_>, owner: &str, path: &str, p: &Value) -> Result<Value> {
    if p.is_null() {
        return Ok(Value::Null);
    }
    policy(p)?;
    let saved = read(tx, owner, path)?;
    if !saved.is_null() {
        if saved["policy"] != *p {
            return Err("frozen clock conflict".into());
        }
        return Ok(saved);
    }
    let timestamp = now(tx)?;
    let clock = json!({"format":"mas-execution-clock/2","policy":p,"created_ms":timestamp,"observed_ms":timestamp,"phase":"queued","phase_started_ms":timestamp,"active_ms":0,"active_branches":0,"worker_ms":0,"worker_time_complete":true,"extensions":{},"deadline_revision":0});
    tx.execute(
        "INSERT INTO service_clocks VALUES($1,$2,$3)",
        &[&owner, &path, &clock.to_string()],
    )
    .map_err(db_error)?;
    Ok(clock)
}
pub(crate) fn status(tx: &mut Transaction<'_>, owner: &str, path: &str) -> Result<Value> {
    let clock = read(tx, owner, path)?;
    if clock.is_null() {
        Ok(Value::Null)
    } else {
        evaluate(&clock, now(tx)?)
    }
}
pub(crate) fn require_open(tx: &mut Transaction<'_>, owner: &str, path: &str) -> Result<()> {
    let state = status(tx, owner, path)?;
    if state["expired"].as_array().is_some_and(|v| !v.is_empty()) {
        return Err(state["expired"][0]
            .as_str()
            .unwrap_or("execution expired")
            .into());
    }
    Ok(())
}
pub(crate) fn transition(
    tx: &mut Transaction<'_>,
    owner: &str,
    path: &str,
    phase: &str,
    branches: i64,
) -> Result<Value> {
    let mut clock = read(tx, owner, path)?;
    if clock.is_null() {
        return Ok(clock);
    }
    let timestamp = now(tx)?;
    let state = evaluate(&clock, timestamp)?;
    if !matches!(phase, "queued" | "running" | "paused" | "terminal")
        || (phase == "running") != (branches > 0)
        || !(0..=1024).contains(&branches)
        || (clock["phase"] == "terminal" && phase != "terminal")
    {
        return Err("invalid clock transition".into());
    }
    if !state["expired"].as_array().unwrap().is_empty()
        && (phase == "queued"
            || phase == "running"
                && (clock["phase"] != "running" || branches > n(&clock, "active_branches")?))
    {
        return Err("execution clock expired".into());
    }
    clock["active_ms"] = state["active_ms"].clone();
    clock["worker_ms"] = state["worker_ms"].clone();
    clock["observed_ms"] = json!(timestamp);
    if clock["phase"] != phase {
        clock["phase_started_ms"] = json!(timestamp)
    }
    clock["phase"] = json!(phase);
    clock["active_branches"] = json!(branches);
    evaluate(&clock, timestamp)?;
    tx.execute(
        "UPDATE service_clocks SET clock=$3 WHERE owner=$1 AND path=$2",
        &[&owner, &path, &clock.to_string()],
    )
    .map_err(db_error)?;
    Ok(clock)
}
/// Replay is checked before lifecycle state: a lost reply remains recoverable
/// after a subsequent transition or amendment. Caller supplies owner authority.
pub(crate) fn amendment_replay(
    tx: &mut Transaction<'_>,
    owner: &str,
    command: &Value,
) -> Result<Option<Value>> {
    let key = command["request_key"]
        .as_str()
        .ok_or("invalid amendment identity")?;
    let digest = hash(command);
    tx.query_opt(
        "SELECT digest,receipt FROM service_deadline_amendments WHERE owner=$1 AND request_key=$2",
        &[&owner, &key],
    )
    .map_err(db_error)?
    .map(|r| {
        if r.get::<_, &str>(0) != digest {
            return Err("deadline amendment conflict".into());
        }
        decode(r.get(1))
    })
    .transpose()
}
fn hash(value: &Value) -> String {
    Sha256::digest(value.to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub(crate) fn amend(
    tx: &mut Transaction<'_>,
    owner: &str,
    path: &str,
    command: &Value,
) -> Result<Value> {
    if let Some(saved) = amendment_replay(tx, owner, command)? {
        return Ok(saved);
    }
    for key in ["operator", "owner", "request_key", "reason"] {
        if command[key]
            .as_str()
            .is_none_or(|v| v.trim().is_empty() || v.len() > 512)
        {
            return Err("invalid deadline amendment".into());
        }
    }
    let mut clock = read(tx, owner, path)?;
    if clock.is_null() {
        return Err("legacy deadline immutable".into());
    }
    let timestamp = now(tx)?;
    let state = evaluate(&clock, timestamp)?;
    if matches!(clock["phase"].as_str(), Some("running" | "terminal"))
        || !state["expired"].as_array().unwrap().is_empty()
    {
        return Err("deadline extension unavailable".into());
    }
    let revision = n(&clock, "deadline_revision")?;
    if command["expected_revision"].as_i64() != Some(revision) {
        return Err("deadline revision conflict".into());
    }
    let limits = command["limits"]
        .as_object()
        .filter(|v| !v.is_empty())
        .ok_or("invalid deadline limits")?;
    let mut before = json!({});
    for key in ["business_seconds", "queue_seconds", "pause_seconds"] {
        before[key] = clock["extensions"]
            .get(key)
            .unwrap_or(&clock["policy"][key])
            .clone()
    }
    let mut increase = false;
    for (key, value) in limits {
        if !matches!(
            key.as_str(),
            "business_seconds" | "queue_seconds" | "pause_seconds"
        ) {
            return Err("active budget immutable".into());
        }
        let amount = value
            .as_i64()
            .filter(|v| *v >= n(&before, key).unwrap_or(i64::MAX) && *v <= 315_360_000)
            .ok_or("deadline extension must increase")?;
        increase |= amount > n(&before, key)?;
        clock["extensions"][key] = value.clone()
    }
    if !increase {
        return Err("deadline extension must increase".into());
    }
    clock["deadline_revision"] = json!(revision + 1);
    clock["observed_ms"] = json!(timestamp);
    let after = evaluate(&clock, timestamp)?;
    let mut effective = before.clone();
    for (key, value) in limits {
        effective[key] = value.clone()
    }
    let mut receipt = command.clone();
    let receipt_obj = receipt.as_object_mut().ok_or("invalid amendment")?;
    receipt_obj.extend(json!({"format":"mas-deadline-amendment/1","task_id":owner,"revision":revision+1,"observed_at_ms":timestamp,"previous_limits":before,"effective_limits":effective,"business_expires_ms":after["business_expires_ms"],"execution_resumed":false}).as_object().unwrap().clone());
    tx.execute(
        "UPDATE service_clocks SET clock=$3 WHERE owner=$1 AND path=$2",
        &[&owner, &path, &clock.to_string()],
    )
    .map_err(db_error)?;
    tx.execute(
        "INSERT INTO service_deadline_amendments VALUES($1,$2,$3,$4,$5,$6)",
        &[
            &owner,
            &command["request_key"].as_str().unwrap(),
            &path,
            &hash(command),
            &(revision + 1),
            &receipt.to_string(),
        ],
    )
    .map_err(db_error)?;
    Ok(receipt)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn clock() -> Value {
        json!({"format":"mas-execution-clock/2","policy":{"format":"mas-timing-policy/1","business_seconds":30,"active_seconds":10,"queue_seconds":5,"pause_seconds":7},"created_ms":1000,"observed_ms":2000,"phase":"running","phase_started_ms":2000,"active_ms":0,"active_branches":3,"worker_ms":0,"worker_time_complete":true,"extensions":{},"deadline_revision":0})
    }
    #[test]
    fn elapsed_is_distinct_from_worker_sum() {
        let s = evaluate(&clock(), 5000).unwrap();
        assert_eq!(s["active_ms"], 3000);
        assert_eq!(s["worker_ms"], 9000);
        assert_eq!(s["remaining_active_ms"], 7000)
    }
    #[test]
    fn expired_and_backwards_clocks_fail_closed() {
        assert!(evaluate(&clock(), 1000).is_err());
        assert_eq!(
            evaluate(&clock(), 12000).unwrap()["expired"],
            json!(["active_budget"])
        )
    }
    #[test]
    fn terminal_clock_does_not_expire_on_later_inspection() {
        let mut c = clock();
        c["phase"] = json!("terminal");
        c["active_branches"] = json!(0);
        assert_eq!(evaluate(&c, 100000).unwrap()["expired"], json!([]))
    }
}
