//! Assessment identity, feedback disclosure and bounded revision verification.
use super::{run_case, sha, CaseOptions, Options, Result};
use serde_json::{json, Value};

pub(super) fn expected_assertion(defect: Option<&str>) -> Option<&'static str> {
    Some(match defect? {
        "BUG_ASSESSMENT_CONFLICT" => "ASSESSMENT_CONFLICT_REJECTED",
        "WITNESS_ASSESSMENT_ACCEPTED" => "REACH_ASSESSMENT_ACCEPTED",
        "BUG_ASSESSMENT_AUTHORITY" => "ASSESSMENT_AUTHORITY",
        "BUG_ASSESSMENT_BINDING" => "ASSESSMENT_CANDIDATE_BINDING",
        "BUG_ASSESSMENT_HISTORY" => "ASSESSMENT_HISTORY_IMMUTABLE",
        "BUG_ASSESSMENT_DUPLICATE" => "ASSESSMENT_IDENTITY_DEDUPLICATED",
        "BUG_ASSESSMENT_ERROR_PASS" => "ASSESSMENT_ERROR_NOT_PASS",
        "BUG_REVISION_BOUND" => "REVISION_EXECUTION_BOUND",
        "BUG_REVISION_REPLAY_UNKNOWN" => "REVISION_UNKNOWN_NOT_REPLAYED",
        "BUG_FEEDBACK_PERMISSION" => "FEEDBACK_PERMISSION_FROZEN",
        "BUG_FEEDBACK_DEADLINE" => "FEEDBACK_DEADLINE_FROZEN",
        "BUG_FEEDBACK_REMEDIATION" => "REMEDIATION_PRECONDITIONS_PRESERVED",
        "BUG_DISCLOSURE_PEER" => "WITHHELD_PEER_NONINTERFERENCE",
        "BUG_DISCLOSURE_REPLAY" => "CONTEXT_REPLAY_PRESERVED",
        "WITNESS_REVISION_SUCCESS" => "REACH_REVISION_SUCCESS",
        _ => return None,
    })
}

pub(super) fn run(options: &Options, cases: &mut Vec<Value>) -> Result<()> {
    let controls = [
        "BUG_ASSESSMENT_CONFLICT",
        "WITNESS_ASSESSMENT_ACCEPTED",
        "BUG_ASSESSMENT_AUTHORITY",
        "BUG_ASSESSMENT_BINDING",
        "BUG_ASSESSMENT_HISTORY",
        "BUG_ASSESSMENT_DUPLICATE",
        "BUG_ASSESSMENT_ERROR_PASS",
        "BUG_REVISION_BOUND",
        "BUG_REVISION_REPLAY_UNKNOWN",
        "BUG_FEEDBACK_PERMISSION",
        "BUG_FEEDBACK_DEADLINE",
        "BUG_FEEDBACK_REMEDIATION",
        "BUG_DISCLOSURE_PEER",
        "BUG_DISCLOSURE_REPLAY",
        "BUG_REVISION_STALL",
        "WITNESS_REVISION_SUCCESS",
    ];
    for (name, defect, no_reduce) in std::iter::once(("positive".to_owned(), None, false))
        .chain(std::iter::once(("no-reduction".to_owned(), None, true)))
        .chain(
            controls
                .iter()
                .map(|d| (d.to_ascii_lowercase(), Some(*d), false)),
        )
    {
        let case = run_case(
            &options.root.join("formal/assessment-revision.pml"),
            &options.out.join(format!("assessment-{name}")),
            options,
            CaseOptions {
                standalone: true,
                defect,
                no_reduce,
                progress_property: Some("revision_settles"),
                reachability_witness: defect.is_some_and(|d| d.starts_with("WITNESS_")),
                ..Default::default()
            },
        )?;
        println!("assessment-{name} {}", case["status"]);
        cases.push(case);
    }
    Ok(())
}
pub(super) fn contract(options: &Options) -> Result<Value> {
    Ok(json!({"schema":"mas-assessment-verification/1",
        "model_sha256":sha(&options.root.join("formal/assessment-revision.pml"))?,
        "bounds":{"assessment_identities":2,"content_values":2,"candidate_identities":2,"evaluator_identities":2,"deliveries":3,"revision_reservations":2,"context_worlds":2,"restart":1},
        "assumptions":["Atomic committed host transitions; durable storage and collision-free digests assumed",
            "Finite evaluator response or unknown outcome; explicit host cancellation and expiry observations",
            "Disclosure covers host-managed input assembly under trusted-worker boundary only",
            "Control properties do not prove diagnosis quality, physical deadlines or measured improvement"],
        "correspondence":"formal/assessment-revision-notes.md"}))
}
