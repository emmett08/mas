//! Typed dispatch permission for the existing shared host invocation admission.
//! Typed refusals precede charges within the existing authority transaction.
//! Database or commit failures preserve
//! uncertainty; only a fresh committed admission yields a consumable permit.
use crate::postgres_repository::{db_error, PostgresRepository};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
pub(crate) mod provider;

#[derive(Debug, PartialEq, Eq)]
pub enum NativeRejection {
    InvalidIdentity,
    IdentityConflict,
    UnknownRun,
    StaleOwner,
    UnknownInvocation,
    AlreadyAdmitted,
    DeadlineExceeded,
    CallBudgetExceeded,
    ScopeBudgetExceeded,
    AdmissionClosed,
    ReceiptCapacityExceeded,
    IncompleteDeadline,
    UnsupportedComposition,
    ProviderDeferred { not_before_ms: i64 },
}
impl NativeRejection {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::InvalidIdentity => "invalid_identity",
            Self::IdentityConflict => "identity_conflict",
            Self::UnknownRun => "unknown_run",
            Self::StaleOwner => "stale_owner",
            Self::UnknownInvocation => "unknown_invocation",
            Self::AlreadyAdmitted => "already_admitted",
            Self::DeadlineExceeded => "deadline_exceeded",
            Self::CallBudgetExceeded => "call_budget_exceeded",
            Self::ScopeBudgetExceeded => "scope_budget_exceeded",
            Self::AdmissionClosed => "admission_closed",
            Self::ReceiptCapacityExceeded => "receipt_capacity_exceeded",
            Self::IncompleteDeadline => "incomplete_deadline",
            Self::UnsupportedComposition => "unsupported_composition",
            Self::ProviderDeferred { .. } => "provider_deferred",
        }
    }
}

/// This value is deliberately neither Clone nor deserialisable. Receiving a
/// retained JSON receipt cannot construct another fresh execution permission.
#[derive(Debug)]
pub struct FreshDispatchPermit {
    command_id: String,
    run: String,
    call_id: String,
    deadline_ms: i64,
    provider_policy: Option<Value>,
}
impl FreshDispatchPermit {
    pub fn command_id(&self) -> &str {
        &self.command_id
    }
    pub fn run(&self) -> &str {
        &self.run
    }
    pub fn call_id(&self) -> &str {
        &self.call_id
    }
    pub fn deadline_ms(&self) -> i64 {
        self.deadline_ms
    }
    pub fn provider_policy(&self) -> Option<&Value> {
        self.provider_policy.as_ref()
    }
    pub fn into_call_id(self) -> String {
        self.call_id
    }
}

#[derive(Debug)]
pub enum NativeAdmission {
    FreshDispatch(FreshDispatchPermit),
    /// A command receipt or an existing invocation observation, never permission.
    Retained {
        receipt: Value,
    },
    Rejected {
        code: NativeRejection,
    },
    Uncertain {
        command_id: String,
    },
}

/// Use the protocol owner's canonical defaults when freezing shared run metadata.
pub fn normalise_configuration(value: &Value) -> Result<Value, String> {
    crate::store::config(value)
}

impl PostgresRepository {
    /// Admit exactly one prepared native invocation through the existing
    /// `admit_invocation` command. Retry the identical command identity after an
    /// uncertain reply; an old receipt remains Retained rather than a permit.
    /// Unclassified storage/commit failures produce Uncertain. This
    /// conservative outcome must be reconciled before dispatch or replacement.
    pub fn admit_native_invocation(
        &mut self,
        command_id: &str,
        run: &str,
        owner: &str,
        call_id: &str,
    ) -> NativeAdmission {
        if [command_id, run, owner, call_id]
            .iter()
            .any(|x| !crate::identity(x))
        {
            return NativeAdmission::Rejected {
                code: NativeRejection::InvalidIdentity,
            };
        }
        let parameters = json!({"run":run,"owner":owner,"call_id":call_id});
        let digest: String = Sha256::digest(
            json!(["admit_invocation", parameters])
                .to_string()
                .as_bytes(),
        )
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
        let epoch = self.binding.epoch;
        let outcome = self.host_command_checked(
            command_id,
            "admit_invocation",
            &parameters,
            |tx| preflight(tx, command_id, &digest, run, owner, call_id, epoch),
            |tx, mut receipt| {
                receipt["deadline_ms"] =
                    json!(execution_deadline(tx, run)?
                        .ok_or("incomplete native execution deadline")?);
                Ok(receipt)
            },
        );
        match outcome {
            Ok(Ok(reply))
                if reply["replayed"] == false
                    && reply["receipt"]["admitted"] == true
                    && reply["receipt"]["call_id"] == call_id
                    && reply["receipt"]["deadline_ms"]
                        .as_i64()
                        .is_some_and(|n| n > 0) =>
            {
                NativeAdmission::FreshDispatch(FreshDispatchPermit {
                    command_id: command_id.into(),
                    run: run.into(),
                    call_id: call_id.into(),
                    deadline_ms: reply["receipt"]["deadline_ms"].as_i64().unwrap(),
                    provider_policy: reply["receipt"]["provider_quota"].get("policy").cloned(),
                })
            }
            Ok(Ok(reply)) if reply["replayed"] == true => NativeAdmission::Retained {
                receipt: reply["receipt"].clone(),
            },
            Ok(Err(NativeRejection::AlreadyAdmitted)) => {
                match self.host_query("invocation", &json!({"run":run,"call_id":call_id})) {
                    Ok(receipt) => NativeAdmission::Retained { receipt },
                    Err(_) => NativeAdmission::Uncertain {
                        command_id: command_id.into(),
                    },
                }
            }
            Ok(Err(code)) => NativeAdmission::Rejected { code },
            _ => NativeAdmission::Uncertain {
                command_id: command_id.into(),
            },
        }
    }
}

// This callback runs after the controller guard and before any command charge,
// in the same authority transaction as the actual invocation admission.
fn preflight(
    tx: &mut postgres::Transaction<'_>,
    command_id: &str,
    digest: &str,
    run: &str,
    owner: &str,
    call_id: &str,
    epoch: i64,
) -> Result<Option<NativeRejection>, String> {
    use NativeRejection as R;
    if let Some(row) = tx
        .query_opt(
            "SELECT digest FROM host_commands WHERE id=$1",
            &[&command_id],
        )
        .map_err(db_error)?
    {
        return Ok(if row.get::<_, String>(0) == digest {
            None
        } else {
            Some(R::IdentityConflict)
        });
    }
    crate::shared_archive::require_open(tx, run)?;
    let Some(row) = tx
        .query_opt(
            "SELECT owner,owner_epoch,scopes FROM host_runs WHERE run=$1",
            &[&run],
        )
        .map_err(db_error)?
    else {
        return Ok(Some(R::UnknownRun));
    };
    if row.get::<_, Option<String>>(0).as_deref() != Some(owner)
        || row.get::<_, Option<i64>>(1) != Some(epoch)
    {
        return Ok(Some(R::StaleOwner));
    }
    let scopes: Value =
        serde_json::from_str(row.get::<_, &str>(2)).map_err(|_| "invalid retained scopes")?;
    let Some(inv) = tx
        .query_opt(
            "SELECT state,reservation_microusd FROM host_invocations WHERE run=$1 AND call_id=$2",
            &[&run, &call_id],
        )
        .map_err(db_error)?
    else {
        return Ok(Some(R::UnknownInvocation));
    };
    if inv.get::<_, String>(0) != "prepared" {
        return Ok(Some(R::AlreadyAdmitted));
    }
    let amount: i64 = inv.get(1);
    let row=tx.query_one("SELECT config,call_count,expired OR deadline<=extract(epoch FROM clock_timestamp()) FROM runs WHERE id=$1",&[&run]).map_err(db_error)?;
    if row.get::<_, bool>(2) {
        return Ok(Some(R::DeadlineExceeded));
    }
    let config: Value =
        serde_json::from_str(row.get::<_, &str>(0)).map_err(|_| "invalid retained run")?;
    if row.get::<_, i64>(1)
        >= config["max_calls"]
            .as_i64()
            .ok_or("invalid retained budget")?
    {
        return Ok(Some(R::CallBudgetExceeded));
    }
    for scope in scopes.as_array().ok_or("invalid retained scopes")? {
        let scope = scope.as_str().ok_or("invalid retained scope")?;
        let row = tx
            .query_opt(
                "SELECT calls<max_calls AND $1<=max_microusd-microusd FROM host_scopes WHERE id=$2",
                &[&amount, &scope],
            )
            .map_err(db_error)?;
        if !row.is_some_and(|r| r.get::<_, bool>(0)) {
            return Ok(Some(R::ScopeBudgetExceeded));
        }
    }
    let version: i64 = tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if version >= 3
        && tx
            .query_one("SELECT draining FROM controller_lease WHERE singleton", &[])
            .map_err(db_error)?
            .get::<_, bool>(0)
    {
        return Ok(Some(R::AdmissionClosed));
    }
    if version >= 4 && !crate::shared_tasks::admission_allowed(tx, run)? {
        return Ok(Some(R::AdmissionClosed));
    }
    if version >= 4
        && tx
            .query_one(
                "SELECT EXISTS(SELECT 1 FROM composition_attempts WHERE run_id=$1)",
                &[&run],
            )
            .map_err(db_error)?
            .get::<_, bool>(0)
    {
        return Ok(Some(R::UnsupportedComposition));
    }
    if execution_deadline(tx, run)?.is_none() {
        return Ok(Some(R::IncompleteDeadline));
    }
    if version >= 8
        && !tx
            .query_one(
                "SELECT normal_used<normal_limit FROM command_capacity WHERE singleton",
                &[],
            )
            .map_err(db_error)?
            .get::<_, bool>(0)
    {
        return Ok(Some(R::ReceiptCapacityExceeded));
    }
    if let Some(not_before_ms) = provider::check(tx, run, call_id)? {
        return Ok(Some(R::ProviderDeferred { not_before_ms }));
    }
    Ok(None)
}

fn execution_deadline(
    tx: &mut postgres::Transaction<'_>,
    run: &str,
) -> Result<Option<i64>, String> {
    let mut deadline: i64 = tx
        .query_one(
            "SELECT floor(deadline*1000)::bigint FROM runs WHERE id=$1",
            &[&run],
        )
        .map_err(db_error)?
        .get(0);
    let version: i64 = tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if version >= 4 {
        if let Some(row) = tx
            .query_opt("SELECT task FROM service_executions WHERE run=$1", &[&run])
            .map_err(db_error)?
        {
            let task: String = row.get(0);
            let clock = crate::shared_timing::status(tx, &task, "")?;
            let (Some(business), Some(observed), Some(remaining)) = (
                clock["business_expires_ms"].as_i64(),
                clock["observed_at_ms"].as_i64(),
                clock["remaining_active_ms"].as_i64(),
            ) else {
                return Ok(None);
            };
            deadline = deadline.min(business).min(
                observed
                    .checked_add(remaining)
                    .ok_or("native deadline overflow")?,
            );
        }
    }
    Ok(Some(deadline))
}
