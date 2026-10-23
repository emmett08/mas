//! Optional bounded accounting over the existing native task/execution records.
//! No counters, second quota owner or caller-supplied cheap estimates are used.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::Value;
type Result<T> = std::result::Result<T, String>;
const LIMITS: [&str; 3] = [
    "max_retained_input_bytes",
    "max_pending_work_units",
    "max_pending_estimated_cost_microusd",
];

fn exact(value: &Value, keys: &[&str]) -> Result<()> {
    let fields = value.as_object().ok_or("invalid_queue_admission")?;
    if fields.len() != keys.len() || fields.keys().any(|key| !keys.contains(&key.as_str())) {
        return Err("invalid_queue_admission".into());
    }
    Ok(())
}
fn limits(value: &Value) -> Result<[u64; 3]> {
    exact(value, &LIMITS)?;
    let mut result = [0; 3];
    for (i, key) in LIMITS.iter().enumerate() {
        result[i] = value[key]
            .as_u64()
            .filter(|n| *n <= i64::MAX as u64)
            .ok_or("invalid_queue_admission_limit")?;
    }
    Ok(result)
}
pub(crate) fn validate(policy: &Value) -> Result<()> {
    let Some(queue) = policy.get("queue_admission") else {
        return Ok(());
    };
    if policy["format"] != "mas-scheduler-policy/2" {
        return Err("queue_admission_requires_scheduler_v2".into());
    }
    exact(
        queue,
        &["format", "max_accounting_records", "global", "principals"],
    )?;
    if queue["format"] != "mas-queue-admission/1"
        || !queue["max_accounting_records"]
            .as_u64()
            .is_some_and(|n| (1..=16384).contains(&n))
    {
        return Err("invalid_queue_admission".into());
    }
    limits(&queue["global"])?;
    let principals = queue["principals"]
        .as_object()
        .filter(|v| !v.is_empty() && v.len() <= 256)
        .ok_or("invalid_queue_admission_principals")?;
    for (principal, value) in principals {
        if principal.is_empty() || principal.len() > 4096 || principal.chars().any(char::is_control)
        {
            return Err("invalid_queue_admission_principal".into());
        }
        limits(value)?;
    }
    Ok(())
}
fn estimate(value: &Value) -> Result<(u64, u64, u64)> {
    exact(
        value,
        &["work_units", "estimated_cost_microusd", "max_input_bytes"],
    )?;
    let work = value["work_units"]
        .as_u64()
        .filter(|n| (1..=1_000_000_000_000).contains(n))
        .ok_or("registered_queue_estimate_required")?;
    let cost = value["estimated_cost_microusd"]
        .as_u64()
        .filter(|n| *n <= 1_000_000_000_000)
        .ok_or("registered_queue_estimate_required")?;
    let bytes = value["max_input_bytes"]
        .as_u64()
        .filter(|n| (1..=65536).contains(n))
        .ok_or("registered_queue_estimate_required")?;
    Ok((work, cost, bytes))
}
fn current(tx: &mut Transaction<'_>) -> Result<Option<Value>> {
    let row = tx
        .query_opt("SELECT policy FROM service_scheduler WHERE singleton", &[])
        .map_err(db_error)?;
    row.map(|r| {
        serde_json::from_str::<Value>(r.get(0)).map_err(|_| "invalid_scheduler_policy".to_owned())
    })
    .transpose()
    .map(|p| p.and_then(|p| p.get("queue_admission").cloned()))
}
#[derive(Default)]
struct Usage {
    global: [u64; 3],
    principal: [u64; 3],
    records: u64,
}
impl Usage {
    fn add(&mut self, own: bool, index: usize, amount: u64) -> Result<()> {
        self.global[index] = self.global[index]
            .checked_add(amount)
            .ok_or("queue_admission_overflow")?;
        if own {
            self.principal[index] = self.principal[index]
                .checked_add(amount)
                .ok_or("queue_admission_overflow")?;
        }
        Ok(())
    }
}
fn usage(
    tx: &mut Transaction<'_>,
    queue: &Value,
    principal: &str,
    replacing: Option<&str>,
) -> Result<Usage> {
    // Composition estimate/retention parity is deliberately unsupported in this
    // first contract. Activation and submission cannot silently omit that work.
    if tx
        .query_opt(
            "SELECT 1 FROM composition_instances WHERE archived_at IS NULL LIMIT 1",
            &[],
        )
        .map_err(db_error)?
        .is_some()
    {
        return Err("queue_admission_requires_native_tasks".into());
    }
    let maximum = queue["max_accounting_records"]
        .as_u64()
        .ok_or("invalid_queue_admission")?;
    let page = (maximum + 1) as i64;
    let mut result = Usage::default();
    // At most max+1 scalar records, never application input/output. Estimates
    // are frozen in the registered definition and bounded before transfer.
    let rows=tx.query("SELECT t.principal=$1,CASE WHEN t.id=$2 OR t.archived_at IS NOT NULL THEN 0 ELSE octet_length(t.input) END::bigint,(t.id IS DISTINCT FROM $2) AND (t.state NOT IN ('completed','failed','cancelled') OR EXISTS(SELECT 1 FROM service_executions e WHERE e.task=t.id AND (e.state IN ('running','unknown') OR EXISTS(SELECT 1 FROM host_invocations i WHERE i.run=e.run AND i.state IN ('executing','unknown'))))),CASE WHEN octet_length((d.descriptor::jsonb->'admission_estimate')::text)<=1024 THEN (d.descriptor::jsonb->'admission_estimate')::text ELSE NULL END,(SELECT count(*) FROM service_executions e WHERE e.task=t.id AND (e.state IN ('running','unknown') OR EXISTS(SELECT 1 FROM host_invocations i WHERE i.run=e.run AND i.state IN ('executing','unknown')))),t.id=$2 FROM service_tasks t LEFT JOIN service_definitions d ON d.kind='workflow' AND d.name=t.workflow AND d.identity=t.identity WHERE t.archived_at IS NULL OR EXISTS(SELECT 1 FROM service_executions e WHERE e.task=t.id AND (e.state IN ('running','unknown') OR EXISTS(SELECT 1 FROM host_invocations i WHERE i.run=e.run AND i.state IN ('executing','unknown')))) LIMIT $3",&[&principal,&replacing,&page]).map_err(db_error)?;
    if rows.len() as u64 > maximum {
        return Err("queue_accounting_record_limit".into());
    }
    result.records = rows.len() as u64;
    for row in rows {
        let unresolved: i64 = row.get(4);
        if unresolved > 1 {
            return Err("multiple_unresolved_queue_estimates_unavailable".into());
        }
        if row.get::<_, Option<bool>>(5) == Some(true) && unresolved > 0 {
            return Err("queue_update_has_unresolved_execution".into());
        }
        let own: bool = row.get(0);
        let bytes: i64 = row.get(1);
        result.add(
            own,
            0,
            u64::try_from(bytes).map_err(|_| "invalid_retained_input_size")?,
        )?;
        if row.get::<_, bool>(2) {
            let text: Option<String> = row.get(3);
            let value: Value = serde_json::from_str(
                text.as_deref()
                    .ok_or("registered_queue_estimate_required")?,
            )
            .map_err(|_| "registered_queue_estimate_required")?;
            let (work, cost, _) = estimate(&value)?;
            result.add(own, 1, work)?;
            result.add(own, 2, cost)?;
        }
    }
    let rows=tx.query("SELECT t.principal=$1,octet_length(e.input)::bigint FROM service_executions e JOIN service_tasks t ON t.id=e.task WHERE t.archived_at IS NULL LIMIT $2",&[&principal,&page]).map_err(db_error)?;
    result.records = result
        .records
        .checked_add(rows.len() as u64)
        .ok_or("queue_admission_overflow")?;
    if result.records > maximum {
        return Err("queue_accounting_record_limit".into());
    }
    for row in rows {
        result.add(
            row.get(0),
            0,
            u64::try_from(row.get::<_, i64>(1)).map_err(|_| "invalid_retained_input_size")?,
        )?;
    }
    Ok(result)
}
pub(crate) fn activate(
    tx: &mut Transaction<'_>,
    previous: Option<&Value>,
    next: &Value,
) -> Result<()> {
    let before = previous.and_then(|p| p.get("queue_admission"));
    let after = next.get("queue_admission");
    if before.is_some() && after.is_none() {
        return Err("queue_admission_cannot_be_removed".into());
    }
    if before.is_none() {
        if let Some(queue) = after {
            usage(tx, queue, "", None)?;
        }
    }
    Ok(())
}
pub(crate) fn composition(tx: &mut Transaction<'_>) -> Result<()> {
    if current(tx)?.is_some() {
        return Err("queue_admission_requires_native_tasks".into());
    }
    Ok(())
}
pub(crate) fn admit(
    tx: &mut Transaction<'_>,
    principal: &str,
    input: &Value,
    descriptor: &Value,
    replacing: Option<&str>,
) -> Result<()> {
    let Some(queue) = current(tx)? else {
        return Ok(());
    };
    let allowed = limits(&queue["global"])?;
    let own_allowed = limits(&queue["principals"][principal])
        .map_err(|_| "principal_queue_admission_unavailable")?;
    let (work, cost, max_bytes) = estimate(&descriptor["admission_estimate"])?;
    let bytes = input.to_string().len() as u64;
    if bytes > max_bytes {
        return Err("registered_queue_input_size_exceeded".into());
    }
    let mut used = usage(tx, &queue, principal, replacing)?;
    let added_records = if replacing.is_some() { 1 } else { 2 };
    if used
        .records
        .checked_add(added_records)
        .ok_or("queue_admission_overflow")?
        > queue["max_accounting_records"].as_u64().unwrap()
    {
        return Err("queue_accounting_record_limit".into());
    }
    used.add(
        true,
        0,
        bytes.checked_mul(2).ok_or("queue_admission_overflow")?,
    )?;
    used.add(true, 1, work)?;
    used.add(true, 2, cost)?;
    for i in 0..3 {
        if used.global[i] > allowed[i] {
            return Err(format!("queue_{}_exhausted", LIMITS[i]));
        }
        if used.principal[i] > own_allowed[i] {
            return Err(format!("principal_queue_{}_exhausted", LIMITS[i]));
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn queue_budget_requires_exact_versioned_registered_contracts() {
        let mut policy = json!({"format":"mas-scheduler-policy/2","queue_admission":{"format":"mas-queue-admission/1","max_accounting_records":10,"global":{"max_retained_input_bytes":100,"max_pending_work_units":10,"max_pending_estimated_cost_microusd":10},"principals":{"alice":{"max_retained_input_bytes":100,"max_pending_work_units":10,"max_pending_estimated_cost_microusd":10}}}});
        assert!(validate(&policy).is_ok());
        policy["queue_admission"]["ignored"] = json!(1);
        assert!(validate(&policy).is_err());
        assert!(estimate(
            &json!({"work_units":1,"estimated_cost_microusd":0,"max_input_bytes":65536})
        )
        .is_ok());
        for value in [
            json!({}),
            json!({"work_units":0,"estimated_cost_microusd":0,"max_input_bytes":1}),
            json!({"work_units":1,"estimated_cost_microusd":0,"max_input_bytes":65537}),
        ] {
            assert!(estimate(&value).is_err());
        }
    }
}
