//! Fixed-window request allowances in the existing provider circuit authority.
use crate::postgres_repository::{db_error, layout, PostgresRepository};
use postgres::Transaction;
use serde_json::{json, Value};
type Result<T> = std::result::Result<T, String>;
const INDEX: &str = "provider_quota_window";

/// One successful response to a fresh provider-credit admission. This value is
/// deliberately neither Clone nor serialisable. It supplements, and never
/// replaces, the original host invocation's execution permission.
#[derive(Debug)]
pub struct ProviderDispatchPermit {
    resource: String,
    call_id: String,
}
impl ProviderDispatchPermit {
    pub fn resource(&self) -> &str {
        &self.resource
    }
    pub fn into_call_id(self) -> String {
        self.call_id
    }
}
#[derive(Debug)]
pub enum ProviderAdmission {
    FreshDispatch(ProviderDispatchPermit),
    Retained,
    Deferred { not_before_ms: i64 },
    Uncertain { command_id: String },
}

#[derive(Clone, Copy)]
pub(super) struct Policy {
    pub window_ms: i64,
    pub max_requests: i64,
}
pub(super) fn policy(value: &Value) -> Result<Option<Policy>> {
    if value["format"] == "mas-provider-circuit/1" {
        return Ok(None);
    }
    if value["format"] != "mas-provider-circuit/2" {
        return Err("invalid circuit profile".into());
    }
    let window_ms = value["window_ms"]
        .as_i64()
        .filter(|n| (1..=86_400_000).contains(n))
        .ok_or("invalid provider quota window")?;
    let max_requests = value["max_requests"]
        .as_i64()
        .filter(|n| (1..=10_000).contains(n))
        .ok_or("invalid provider request allowance")?;
    Ok(Some(Policy {
        window_ms,
        max_requests,
    }))
}
fn qualified(tx: &mut Transaction<'_>) -> Result<bool> {
    let row=tx.query_opt("SELECT t.relname,a.amname,i.indisvalid,i.indisready,i.indisunique,i.indnkeyatts,i.indnatts,i.indpred IS NULL,i.indexprs IS NULL,pg_get_indexdef(c.oid,1,false),pg_get_indexdef(c.oid,2,false) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_index i ON i.indexrelid=c.oid JOIN pg_class t ON t.oid=i.indrelid JOIN pg_namespace tn ON tn.oid=t.relnamespace JOIN pg_am a ON a.oid=c.relam WHERE n.nspname=current_schema() AND tn.nspname=current_schema() AND c.relname=$1",&[&INDEX]).map_err(db_error)?;
    let Some(r) = row else { return Ok(false) };
    if r.get::<_, String>(0) != "provider_permits"
        || r.get::<_, String>(1) != "btree"
        || !r.get::<_, bool>(2)
        || !r.get::<_, bool>(3)
        || r.get::<_, bool>(4)
        || r.get::<_, i16>(5) != 2
        || r.get::<_, i16>(6) != 2
        || !r.get::<_, bool>(7)
        || !r.get::<_, bool>(8)
        || r.get::<_, String>(9) != "resource"
        || r.get::<_, String>(10) != "created_ms"
    {
        return Err("provider quota index definition differs".into());
    }
    Ok(true)
}
impl PostgresRepository {
    /// Obtain fresh v2 provider credit. A lost database response, unsupported
    /// policy, failed command or replay never becomes dispatch permission.
    /// Bind command_id and call_id to the already admitted original effect.
    pub fn admit_provider_request(
        &mut self,
        command_id: &str,
        policy: &Value,
        call_id: &str,
    ) -> ProviderAdmission {
        let uncertain = || ProviderAdmission::Uncertain {
            command_id: command_id.into(),
        };
        let Some(resource) = policy["resource_key"].as_str() else {
            return uncertain();
        };
        if !matches!(super::quota::policy(policy), Ok(Some(_))) {
            return uncertain();
        }
        let Ok(reply) = self.host_command(
            command_id,
            "circuit.admit",
            &json!({"policy":policy,"call_id":call_id}),
        ) else {
            return uncertain();
        };
        if reply["replayed"] == true {
            return ProviderAdmission::Retained;
        }
        if reply["replayed"] != false {
            return uncertain();
        }
        let receipt = &reply["receipt"];
        if receipt["fresh"] == true
            && receipt["admitted"] == true
            && receipt["resource_key"] == policy["resource_key"]
            && receipt["call_id"] == call_id
        {
            return ProviderAdmission::FreshDispatch(ProviderDispatchPermit {
                resource: resource.into(),
                call_id: call_id.into(),
            });
        }
        if receipt["fresh"] == false {
            if receipt["disposition"] == "retained" || receipt["disposition"] == "identity_conflict"
            {
                return ProviderAdmission::Retained;
            }
            if let Some(not_before_ms) = receipt["not_before_ms"].as_i64() {
                return ProviderAdmission::Deferred { not_before_ms };
            }
        }
        uncertain()
    }
    /// Explicit additive index preparation. Existing version-one policies retain
    /// their meaning; the table/column layout and authority version stay intact.
    pub fn install_provider_quota_index(&mut self) -> Result<()> {
        self.transaction(|tx| {
            let row = tx.query_one("SELECT version,layout_digest FROM authority WHERE singleton", &[]).map_err(db_error)?;
            if row.get::<_, i64>(0) < 4 {
                return Err("service repository required before provider quota index".into());
            }
            if row.get::<_, String>(1) != layout(tx)? {
                return Err("unexpected provider quota installation layout".into());
            }
            if !qualified(tx)? {
                // A same-named non-index object must also prevent installation.
                let occupied: bool = tx.query_one("SELECT EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1)", &[&INDEX]).map_err(db_error)?.get(0);
                if occupied {
                    return Err("provider quota index name already occupied".into());
                }
                tx.batch_execute("CREATE INDEX provider_quota_window ON provider_permits(resource,created_ms)").map_err(db_error)?;
            }
            if !qualified(tx)? {
                return Err("provider quota index unavailable".into());
            }
            let digest = layout(tx)?;
            tx.execute("UPDATE authority SET layout_digest=$1 WHERE singleton", &[&digest]).map_err(db_error)?;
            Ok(())
        })
    }
}

pub(super) fn usage(
    tx: &mut Transaction<'_>,
    resource: &str,
    policy: Policy,
    timestamp: i64,
) -> Result<Value> {
    if !qualified(tx)? {
        return Err("provider quota index not installed".into());
    }
    if timestamp < 0 {
        return Err("invalid provider quota clock".into());
    }
    if tx.query_opt("SELECT created_ms FROM provider_permits WHERE resource=$1 ORDER BY created_ms DESC LIMIT 1",&[&resource]).map_err(db_error)?.is_some_and(|r|r.get::<_,i64>(0)>timestamp){return Err("provider quota clock regressed".into());}
    let start = timestamp / policy.window_ms * policy.window_ms;
    let reset = start
        .checked_add(policy.window_ms)
        .ok_or("provider quota clock overflow")?;
    let used: i64 = tx
        .query_one(
            "SELECT count(*) FROM provider_permits WHERE resource=$1 AND created_ms>=$2",
            &[&resource, &start],
        )
        .map_err(db_error)?
        .get(0);
    Ok(
        json!({"window_start_ms":start,"window_reset_ms":reset,"max_requests":policy.max_requests,"used_requests":used,"remaining_requests":(policy.max_requests-used).max(0)}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn quota_requires_explicit_bounded_version_two() {
        assert!(policy(&json!({"format":"mas-provider-circuit/1"}))
            .unwrap()
            .is_none());
        assert!(policy(
            &json!({"format":"mas-provider-circuit/2","window_ms":1000,"max_requests":2})
        )
        .unwrap()
        .is_some());
        for value in [
            json!({"format":"mas-provider-circuit/2","window_ms":0,"max_requests":2}),
            json!({"format":"mas-provider-circuit/2","window_ms":1000,"max_requests":10001}),
            json!({"format":"other"}),
        ] {
            assert!(policy(&value).is_err());
        }
    }
}
