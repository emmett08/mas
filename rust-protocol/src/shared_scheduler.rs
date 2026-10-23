//! Version scheduler admission allowances while preserving occupied assignments.
pub(crate) mod queue_admission;
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;

pub fn policy_digest(policy: &Value) -> String {
    Sha256::digest(policy.to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

pub(crate) fn limit(policy: &Value) -> Result<i64> {
    match policy.get("format") {
        None => Ok(256),
        Some(v) if v == "mas-scheduler-policy/2" => Ok(16_384),
        _ => Err("unsupported_scheduler_policy".into()),
    }
}

pub(crate) fn validate(policy: &Value) -> Result<()> {
    let cap = limit(policy)?;
    let object = policy.as_object().ok_or("invalid_scheduler_policy")?;
    if cap > 256
        && (object.len() != if object.contains_key("queue_admission") { 7 } else { 6 }
            || object.keys().any(|k| {
                ![
                    "format",
                    "revision",
                    "max_workers",
                    "max_per_principal",
                    "workflows",
                    "resources",
                    "queue_admission",
                ]
                .contains(&k.as_str())
            }))
    {
        return Err("invalid_scheduler_policy".into());
    }
    if cap > 256 && !policy["revision"].as_i64().is_some_and(|n| n >= 0) {
        return Err("invalid_scheduler_revision".into());
    }
    for k in ["max_workers", "max_per_principal"] {
        if !policy[k].as_i64().is_some_and(|n| (1..=cap).contains(&n)) {
            return Err("invalid_scheduler_policy".into());
        }
    }
    for k in ["workflows", "resources"] {
        let limits = policy[k].as_object().ok_or("invalid_scheduler_policy")?;
        if limits.len() > 256
            || limits
                .values()
                .any(|n| !n.as_i64().is_some_and(|n| (1..=cap).contains(&n)))
        {
            return Err("invalid_scheduler_policy".into());
        }
    }
    queue_admission::validate(policy)?;
    Ok(())
}

pub(crate) fn amend(tx: &mut Transaction<'_>, p: &Value) -> Result<Value> {
    let fields = p.as_object().ok_or("invalid_scheduler_amendment")?;
    if fields.len() != 5
        || fields.keys().any(|k| {
            ![
                "expected_policy_digest",
                "expected_revision",
                "policy",
                "reason",
                "authoriser",
            ]
            .contains(&k.as_str())
        })
    {
        return Err("invalid_scheduler_amendment".into());
    }
    let policy = &p["policy"];
    if policy["format"] != "mas-scheduler-policy/2" {
        return Err("scheduler_amendment_requires_v2".into());
    }
    validate(policy)?;
    for k in ["reason", "authoriser"] {
        if !p[k]
            .as_str()
            .is_some_and(|s| !s.is_empty() && s.len() <= 1024 && !s.chars().any(char::is_control))
        {
            return Err("invalid_scheduler_amendment".into());
        }
    }
    let current: String = tx
        .query_opt("SELECT policy FROM service_scheduler WHERE singleton", &[])
        .map_err(db_error)?
        .ok_or("scheduler_policy_unavailable")?
        .get(0);
    let current: Value = serde_json::from_str(&current).map_err(|_| "invalid_scheduler_policy")?;
    let previous = policy_digest(&current);
    if p["expected_policy_digest"].as_str() != Some(previous.as_str()) {
        return Err("scheduler_policy_revision_conflict".into());
    }
    let revision = current["revision"].as_i64().unwrap_or(0);
    let next = revision
        .checked_add(1)
        .ok_or("scheduler_revision_exhausted")?;
    if p["expected_revision"].as_i64() != Some(revision)
        || policy["revision"].as_i64() != Some(next)
    {
        return Err("scheduler_policy_revision_conflict".into());
    }
    queue_admission::activate(tx,Some(&current),policy)?;
    // Lower allowances govern future admissions. Existing assignments retain
    // their ownership and reservations while occupancy drains naturally.
    let occupied: i64 = tx
        .query_one("SELECT count(*) FROM service_slots", &[])
        .map_err(db_error)?
        .get(0);
    tx.execute(
        "UPDATE service_scheduler SET policy=$1 WHERE singleton",
        &[&policy.to_string()],
    )
    .map_err(db_error)?;
    Ok(
        json!({"previous_policy_digest":previous,"policy_digest":policy_digest(policy),"revision":next,"occupied_at_amendment":occupied,"policy":policy,"reason":p["reason"],"authoriser":p["authoriser"]}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn extended_limits_require_explicit_version_and_exact_shape() {
        let mut p =
            json!({"max_workers":512,"max_per_principal":300,"workflows":{},"resources":{}});
        assert!(validate(&p).is_err());
        p["format"] = json!("mas-scheduler-policy/2");
        p["revision"] = json!(0);
        assert!(validate(&p).is_ok());
        p["ignored_limit"] = json!(1);
        assert!(validate(&p).is_err());
        p.as_object_mut().unwrap().remove("ignored_limit");
        p["max_workers"] = json!(16_385);
        assert!(validate(&p).is_err());
    }
    #[test]
    fn digest_binds_every_limit() {
        let mut p = json!({"max_workers":16,"max_per_principal":4,"workflows":{},"resources":{}});
        let before = policy_digest(&p);
        p["resources"]["github"] = json!(2);
        assert_ne!(before, policy_digest(&p));
    }
}
