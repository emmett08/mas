//! Registered provider allowances supplement the original native permission.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
type Result<T> = std::result::Result<T, String>;

pub(super) fn policy(tx: &mut Transaction<'_>, run: &str) -> Result<Option<Value>> {
    let version: i64 = tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if version < 4 {
        return Ok(None);
    }
    let row = tx.query_opt("SELECT d.descriptor,d.retired FROM service_executions e JOIN service_tasks t ON t.id=e.task JOIN service_definitions d ON d.kind='workflow' AND d.name=t.workflow AND d.identity=t.identity WHERE e.run=$1", &[&run]).map_err(db_error)?;
    let Some(row) = row else { return Ok(None) };
    let descriptor: Value = serde_json::from_str(
        row.get::<_, Option<&str>>(0)
            .ok_or("registered native definition unavailable")?,
    )
    .map_err(|_| "invalid registered native definition")?;
    let Some(policy) = descriptor.get("provider_quota") else {
        return Ok(None);
    };
    if row.get::<_, bool>(1) || policy["format"] != "mas-provider-circuit/2" {
        return Err("registered native provider contract unavailable".into());
    }
    Ok(Some(policy.clone()))
}

pub(super) fn check(tx: &mut Transaction<'_>, run: &str, call: &str) -> Result<Option<i64>> {
    match policy(tx, run)? {
        Some(policy) => crate::shared_remote::native_provider_eligibility(tx, &policy, call),
        None => Ok(None),
    }
}

/// Called by the actual admit_invocation transaction, including its legacy JSON
/// entry point. A caller cannot bypass a registered requirement by selecting a
/// different facade. Failure rolls back both provider and native reservations.
pub(crate) fn reserve(tx: &mut Transaction<'_>, run: &str, call: &str) -> Result<Option<Value>> {
    let Some(policy) = policy(tx, run)? else {
        return Ok(None);
    };
    let receipt = crate::shared_remote::apply(
        tx,
        "circuit.admit",
        &json!({"policy":policy,"call_id":call}),
    )?;
    if receipt["admitted"] != true || receipt["fresh"] != true || receipt["call_id"] != call {
        return Err("registered provider admission unavailable".into());
    }
    Ok(Some(json!({"policy":policy,"receipt":receipt})))
}
