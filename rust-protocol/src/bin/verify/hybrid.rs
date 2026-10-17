//! Finite preflight, gateway projection, bound quorum and durable-host cases.
use super::{run_case, sha, CaseOptions, Options, Result};
use serde_json::{json, Value};

pub(super) fn expected_assertion(defect: Option<&str>) -> Option<&'static str> {
    Some(match defect? {
        "BUG_REPLY_PREFLIGHT" => "REQUEST_REPLY_ROUTE_PREFLIGHT",
        "BUG_GATEWAY_CLASSIFICATION" => "GATEWAY_CLASSIFICATION_PRESERVED",
        "BUG_GATEWAY_UNKNOWN" => "GATEWAY_UNKNOWN_PRESERVED",
        "BUG_GATEWAY_DISCLOSURE" => "GATEWAY_DIAGNOSTIC_WITHHELD",
        "BUG_GATEWAY_ACTOR_SCOPE" | "BUG_GATEWAY_RUN_SCOPE" => "GATEWAY_OBSERVATION_SCOPED",
        "BUG_GATEWAY_OBSERVER_WRITE" => "GATEWAY_OBSERVATION_READ_ONLY",
        "BUG_QUORUM_CANDIDATE" => "QUORUM_CANDIDATE_BOUND",
        "BUG_QUORUM_CONTEXT" => "QUORUM_CONTEXT_BOUND",
        "BUG_QUORUM_EVIDENCE" => "QUORUM_EVIDENCE_BOUND",
        "BUG_QUORUM_DISTINCT" => "QUORUM_DISTINCT_EVALUATORS",
        "BUG_DURABLE_HISTORY" | "BUG_DURABLE_HISTORY_LOSS" => "DURABLE_ASSESSMENT_IMMUTABLE",
        "BUG_DURABLE_RESERVATION" => "DURABLE_RESERVATION_RESTORED",
        "BUG_DURABLE_UNKNOWN_REPLAY" => "DURABLE_UNKNOWN_NOT_REDISPATCHED",
        "BUG_DURABLE_AUTHORITY" => "DURABLE_AUTHORITY_FROZEN",
        "BUG_ASSESSMENT_AUTHORISES_EFFECT" => "ASSESSMENT_NOT_EXECUTION_AUTHORITY",
        "WITNESS_REPLY_REQUEST" => "REACH_ROUTABLE_REQUEST",
        "WITNESS_ONE_WAY_NOTICE" => "REACH_ONE_WAY_NOTICE",
        "WITNESS_GATEWAY_COMMITTED_UNKNOWN" => "REACH_COMMITTED_UNKNOWN_RECONCILED",
        "WITNESS_BOUND_QUORUM" => "REACH_BOUND_DISTINCT_QUORUM",
        "WITNESS_DURABLE_ASSESSMENT_REPLAY" => "REACH_DURABLE_ASSESSMENT_REPLAY",
        "WITNESS_DURABLE_REVISION" => "REACH_DURABLE_REVISION_SUCCESS",
        _ => return None,
    })
}

pub(super) fn run(options: &Options, cases: &mut Vec<Value>) -> Result<()> {
    let controls = [
        "BUG_REPLY_PREFLIGHT",
        "BUG_GATEWAY_CLASSIFICATION",
        "BUG_GATEWAY_UNKNOWN",
        "BUG_GATEWAY_DISCLOSURE",
        "BUG_GATEWAY_ACTOR_SCOPE",
        "BUG_GATEWAY_RUN_SCOPE",
        "BUG_GATEWAY_OBSERVER_WRITE",
        "BUG_QUORUM_CANDIDATE",
        "BUG_QUORUM_CONTEXT",
        "BUG_QUORUM_EVIDENCE",
        "BUG_QUORUM_DISTINCT",
        "BUG_DURABLE_HISTORY",
        "BUG_DURABLE_HISTORY_LOSS",
        "BUG_DURABLE_RESERVATION",
        "BUG_DURABLE_UNKNOWN_REPLAY",
        "BUG_DURABLE_AUTHORITY",
        "BUG_ASSESSMENT_AUTHORISES_EFFECT",
        "BUG_HYBRID_STALL",
        "WITNESS_REPLY_REQUEST",
        "WITNESS_ONE_WAY_NOTICE",
        "WITNESS_GATEWAY_COMMITTED_UNKNOWN",
        "WITNESS_BOUND_QUORUM",
        "WITNESS_DURABLE_REVISION",
        "WITNESS_DURABLE_ASSESSMENT_REPLAY",
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
            &options.root.join("formal/hybrid-contracts.pml"),
            &options.out.join(format!("hybrid-{name}")),
            options,
            CaseOptions {
                standalone: true,
                defect,
                no_reduce,
                progress_property: Some("hybrid_contract_settles"),
                reachability_witness: defect.is_some_and(|d| d.starts_with("WITNESS_")),
                ..Default::default()
            },
        )?;
        println!("hybrid-{name} {}", case["status"]);
        cases.push(case);
    }
    Ok(())
}

pub(super) fn contract(options: &Options) -> Result<Value> {
    Ok(json!({"schema":"mas-hybrid-contract-verification/1",
        "model_sha256":sha(&options.root.join("formal/hybrid-contracts.pml"))?,
        "runner_module_sha256":sha(&options.root.join("rust-protocol/src/bin/verify/hybrid.rs"))?,
        "bounds":{"services_per_trace":1,"service_choices":4,"request_identities":1,
            "actors":2,"runs":2,"candidate_values":2,"context_values":2,"evidence_values":2,
            "quorum_deliveries":3,"quorum_minimum":2,"revision_reservations":2,
            "assessment_identities":1,"history_values":2,"restart":1},
        "assumptions":["Separate independent service scenarios; no cross-service concurrency property",
            "Exact typed equality and collision-free whole-record digests",
            "Durable storage and atomic repository, journal and reservation commits",
            "Operational error may occur before or after commit; reconciliation is one snapshot only",
            "Restart conservatively marks an incomplete reservation unknown without redispatch",
            "Finite responses and continued host execution; cancellation and expiry are observed events",
            "No LLM quality, measured improvement, real-time, unrestricted exactly-once or refinement claim"],
        "correspondence":"formal/hybrid-contracts-notes.md"}))
}
