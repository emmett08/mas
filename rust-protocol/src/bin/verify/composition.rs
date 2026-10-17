//! Interleaved admission/assessment/revision checks and shared native traces.
use super::{run_case, sha, CaseOptions, Options, Result};
use serde_json::{json, Value};
use std::fs;

pub(super) fn expected_assertion(defect: Option<&str>) -> Option<&'static str> {
    Some(match defect? {
        "BUG_COMPOSE_ABSENCE" => "COMPOSE_ABSENCE_IS_SNAPSHOT",
        "BUG_COMPOSE_OBSERVE_WRITE" => "COMPOSE_OBSERVATION_READ_ONLY",
        "BUG_COMPOSE_SCOPE" => "COMPOSE_OBSERVATION_SCOPED",
        "BUG_COMPOSE_IDENTITY" => "COMPOSE_REQUEST_IDENTITY",
        "BUG_COMPOSE_UNRESERVED" => "COMPOSE_DISPATCH_RESERVED",
        "BUG_COMPOSE_REPLAY" => "COMPOSE_UNKNOWN_NOT_REDISPATCHED",
        "BUG_COMPOSE_HISTORY" => "COMPOSE_RETAINED_ASSESSMENT",
        "BUG_COMPOSE_TERMINAL" => "COMPOSE_TERMINAL_IMMUTABLE",
        "BUG_COMPOSE_FEEDBACK" => "COMPOSE_FEEDBACK_BOUND",
        "BUG_COMPOSE_STOP" => "COMPOSE_STOP_PREVENTS_REVISION",
        "WITNESS_COMPOSE_SUCCESS" => "REACH_COMPOSE_REVISION",
        "WITNESS_COMPOSE_LATE_COMMIT" => "REACH_COMPOSE_LATE_COMMIT",
        "WITNESS_COMPOSE_LATE_EVIDENCE" => "REACH_COMPOSE_LATE_EVIDENCE",
        "WITNESS_COMPOSE_RETAINED_RESTART" => "REACH_COMPOSE_RETAINED_RESTART",
        _ => return None,
    })
}

pub(super) fn run(options: &Options, cases: &mut Vec<Value>) -> Result<()> {
    let source = options.root.join("formal/assessment-composition.pml");
    let controls = ["BUG_COMPOSE_ABSENCE", "BUG_COMPOSE_OBSERVE_WRITE", "BUG_COMPOSE_SCOPE",
        "BUG_COMPOSE_IDENTITY", "BUG_COMPOSE_UNRESERVED", "BUG_COMPOSE_REPLAY",
        "BUG_COMPOSE_HISTORY", "BUG_COMPOSE_TERMINAL", "BUG_COMPOSE_FEEDBACK",
        "BUG_COMPOSE_STOP", "BUG_COMPOSE_STALL", "WITNESS_COMPOSE_SUCCESS",
        "WITNESS_COMPOSE_LATE_COMMIT", "WITNESS_COMPOSE_LATE_EVIDENCE",
        "WITNESS_COMPOSE_RETAINED_RESTART"];
    for (name, defect, no_reduce) in std::iter::once(("positive".to_owned(), None, false))
        .chain(std::iter::once(("no-reduction".to_owned(), None, true)))
        .chain(controls.iter().map(|d| (d.to_ascii_lowercase(), Some(*d), false))) {
        let case = run_case(&source, &options.out.join(format!("composition-{name}")), options,
            CaseOptions { standalone: true, defect, no_reduce,
                progress_property: Some("assessment_composition_settles"),
                reachability_witness: defect.is_some_and(|d| d.starts_with("WITNESS_")),
                ..Default::default() })?;
        println!("composition-{name} {}", case["status"]);
        cases.push(case);
    }
    let corpus: Value = serde_json::from_slice(&fs::read(options.root.join("formal/assessment-composition-traces.json"))?)?;
    let model = fs::read_to_string(source)?;
    for trace in corpus["traces"].as_array().ok_or("missing composition traces")? {
        let name = trace["name"].as_str().ok_or("trace name required")?;
        if !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'-') { return Err("invalid trace name".into()); }
        let mut generated = format!("#define SHARED_TRACE\n{model}\ninit {{\n");
        for action in trace["actions"].as_array().ok_or("trace actions required")? {
            let action = action.as_str().ok_or("action name required")?;
            if !["Commit","Observe","ForeignObserve","Resume","Receive","Reserve","Dispatch","Assess","Reply","Revise","RevDispatch","RevComplete","Restart","Cancel","Expire"].contains(&action) {
                return Err("unknown composition trace action".into());
            }
            generated.push_str(&format!("atomic {{ apply({action}) }};\n"));
        }
        for (field, value) in trace["expected"].as_object().ok_or("trace expectations required")? {
            if !["stored","received","terminal","reserved","assessment","revision","unknown"].contains(&field.as_str()) { return Err("unknown expectation".into()); }
            if !value.is_boolean() && !value.is_u64() { return Err("invalid expectation type".into()); }
            generated.push_str(&format!("assert({field} == {value}); /* SHARED_TRACE_OBSERVATION */\n"));
        }
        generated.push_str("}\n");
        let input = options.out.join(format!("trace-{name}.pml"));
        fs::write(&input, generated)?;
        let mut case = run_case(&input, &options.out.join(format!("composition-trace-{name}")), options,
            CaseOptions { standalone:true, ..Default::default() })?;
        case["purpose"] = json!("shared_implementation_trace");
        case["trace_name"] = json!(name);
        cases.push(case);
    }
    Ok(())
}

pub(super) fn contract(options: &Options) -> Result<Value> {
    Ok(json!({"schema":"mas-assessment-composition-verification/1",
        "model_sha256":sha(&options.root.join("formal/assessment-composition.pml"))?,
        "corpus_sha256":sha(&options.root.join("formal/assessment-composition-traces.json"))?,
        "runner_module_sha256":sha(&options.root.join("rust-protocol/src/bin/verify/composition.rs"))?,
        "bounds":{"request_identities":1,"actors":2,"candidate_values":2,"evaluation_calls":1,"revision_calls":1,"restart":1,"same_identity_retry":1,"scoped_observations":2},
        "assumptions":["Atomic durable repository and host journal commits; exact typed equality and collision-free digests",
            "Finite participant actions and provider responses, or terminal unknown after restart; no physical deadline claim",
            "Cancellation is a host workflow stop signal as well as an attempted task cancellation",
            "A valid assessment is retained evidence, not permission for another call or external effect",
            "Native shared traces check Store and assessment APIs; dispatch and cancellation policy remain explicit host-adapter obligations, not whole-system refinement"],
        "correspondence":"formal/assessment-composition-notes.md"}))
}
