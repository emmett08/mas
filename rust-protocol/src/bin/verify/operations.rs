//! Finite native-host admission, observation and external-operation cases.
use super::{run_case, sha, CaseOptions, Options, Result};
use serde_json::{json, Value};

pub(super) fn expected_assertion(defect: Option<&str>) -> Option<&'static str> {
    Some(match defect? {
        "BUG_ADMISSION_DEADLINE" => "ADMISSION_LIVE_AT_RETRY_CHECK",
        "BUG_ADMISSION_UNKNOWN_RETRY" => "ADMISSION_UNKNOWN_NOT_RETRIED",
        "BUG_ADMISSION_IDENTITY" => "ADMISSION_IDENTITY_RETAINED",
        "BUG_ADMISSION_BOUND" => "ADMISSION_ATTEMPT_BOUND",
        "BUG_ADMISSION_EXPIRED_LAUNCH" => "ADMISSION_LAUNCH_REQUIRES_ACTIONABLE",
        "BUG_OBSERVER_MUTATION" => "OBSERVER_PRESERVES_PROTOCOL_STATE",
        "WITNESS_ADMISSION_RETRY_SUCCESS" => "REACH_ADMISSION_RETRY_SUCCESS",
        "WITNESS_ADMISSION_LOST_COMMIT" => "REACH_ADMISSION_LOST_COMMIT",
        "WITNESS_ADMISSION_RESUME_DUPLICATE" => "REACH_ADMISSION_RESUME_DUPLICATE",
        "BUG_OPERATION_REDISPATCH" => "OPERATION_ORIGINAL_RESERVATION",
        "BUG_OPERATION_CAUSALITY" => "OPERATION_REPORT_IMMUTABLE",
        "BUG_OPERATION_EARLY_PASS" => "OPERATION_VERIFICATION_REQUIRES_EVIDENCE",
        "BUG_OPERATION_HISTORY" => "OPERATION_OBSERVATION_IMMUTABLE",
        "BUG_OPERATION_BINDING" => "OPERATION_OBSERVATION_BOUND",
        "WITNESS_OPERATION_VERIFIED" => "REACH_OPERATION_VERIFIED",
        "WITNESS_OPERATION_LATE_EFFECT" => "REACH_OPERATION_LATE_EFFECT",
        "WITNESS_OPERATION_RECONCILED_UNKNOWN" => "REACH_OPERATION_RECONCILED_UNKNOWN",
        _ => return None,
    })
}

pub(super) fn run(options: &Options, cases: &mut Vec<Value>) -> Result<()> {
    for (model, property, controls) in [
        ("admission-observation.pml", "admission_settles", &[
            "BUG_ADMISSION_DEADLINE", "BUG_ADMISSION_UNKNOWN_RETRY",
            "BUG_ADMISSION_IDENTITY", "BUG_ADMISSION_BOUND", "BUG_ADMISSION_EXPIRED_LAUNCH",
            "BUG_OBSERVER_MUTATION", "BUG_ADMISSION_STALL",
            "WITNESS_ADMISSION_RETRY_SUCCESS", "WITNESS_ADMISSION_LOST_COMMIT",
            "WITNESS_ADMISSION_RESUME_DUPLICATE",
        ][..]),
        ("operation-outcomes.pml", "operation_settles", &[
            "BUG_OPERATION_REDISPATCH", "BUG_OPERATION_CAUSALITY", "BUG_OPERATION_EARLY_PASS",
            "BUG_OPERATION_HISTORY", "BUG_OPERATION_BINDING", "BUG_OPERATION_STALL",
            "WITNESS_OPERATION_VERIFIED", "WITNESS_OPERATION_LATE_EFFECT",
            "WITNESS_OPERATION_RECONCILED_UNKNOWN",
        ][..]),
    ] {
        let name=model.trim_end_matches(".pml");
        for (suffix, defect, no_reduce) in std::iter::once(("positive".to_owned(), None, false))
            .chain(std::iter::once(("no-reduction".to_owned(), None, true)))
            .chain(controls.iter().map(|control| (control.to_ascii_lowercase(), Some(*control), false)))
        {
            let case=run_case(&options.root.join("formal").join(model),
                &options.out.join(format!("{name}-{suffix}")), options,
                CaseOptions{standalone:true, defect, no_reduce, progress_property:Some(property),
                    reachability_witness:defect.is_some_and(|value|value.starts_with("WITNESS_")),
                    ..Default::default()})?;
            println!("{name}-{suffix} {}",case["status"]);
            cases.push(case);
        }
    }
    Ok(())
}

pub(super) fn contract(options: &Options) -> Result<Value> {
    Ok(json!({
        "schema":"mas-operation-verification/1",
        "models":{
            "admission-observation.pml":sha(&options.root.join("formal/admission-observation.pml"))?,
            "operation-outcomes.pml":sha(&options.root.join("formal/operation-outcomes.pml"))?
        },
        "runner_module_sha256":sha(&options.root.join("rust-protocol/src/bin/verify/operations.rs"))?,
        "bounds":{
            "logical_requests":1,"request_identity_values":2,"mailbox_capacity":1,
            "attempts_per_invocation":2,"explicit_resumes":1,"snapshot_reads":2,
            "operations":1,"reservations":1,"external_dispatches":1,"reconciliation_records":2
        },
        "assumptions":[
            "Atomic durable SQLite/Kubernetes transactions and exact identity/content comparisons",
            "Error dispositions truthfully distinguish known nonadmission from uncertain commit",
            "Host continues execution; API outcomes are finitely observed or become unknown",
            "Production host uses eight attempts; model uses two to expose retry exhaustion and resume",
            "No aggregate retry bound over arbitrary explicit resumes or restarts is claimed",
            "Expiry and cancellation are observations, not a real-time or scheduling guarantee",
            "An already reserved owner may cause its original effect after unknown terminal publication",
            "A live resource observation does not establish historical causation or fence a delayed owner",
            "No cryptographic, storage corruption, unrestricted effect exactly-once or refinement proof"
        ]
    }))
}
