//! Bounded shared-authority cases and their executable observation corpus.
//!
//! This module owns the changed authority contract. Generic SPIN execution and
//! acceptance remain in the parent verifier; Python executes the same corpus
//! against the native PostgreSQL implementation.
use super::{run_case, CaseOptions, Options, Result};
use serde_json::{json, Value};
use std::fs;

pub(super) fn expected_assertion(defect: Option<&str>) -> Option<&'static str> {
    Some(match defect? {
        "BUG_AUTH_STALE_ADMISSION" => "AUTH_CURRENT_ADMISSION",
        "BUG_AUTH_STALE_DISPATCH" => "AUTH_CURRENT_DISPATCH",
        "BUG_AUTH_WORKER_FENCE" => "AUTH_WORKER_FENCED",
        "BUG_AUTH_DOUBLE_CLAIM" => "AUTH_ONE_CLAIM",
        "BUG_AUTH_REDISPATCH" => "AUTH_NO_REDISPATCH",
        "BUG_AUTH_DEPLOYMENT" => "AUTH_DEPLOYMENT_LIMIT",
        "BUG_AUTH_PRINCIPAL" => "AUTH_PRINCIPAL_LIMIT",
        "BUG_AUTH_PROVIDER" => "AUTH_PROVIDER_LIMIT",
        "BUG_AUTH_ANCESTOR" => "AUTH_ANCESTOR_LIMIT",
        "BUG_AUTH_CHILD_ANCESTOR" => "AUTH_CHILD_ANCESTOR_LIMIT",
        "BUG_AUTH_DROP_LATE" => "AUTH_RECEIPT_RETAINED",
        "BUG_AUTH_STALE_COMPLETION" => "AUTH_CURRENT_COMPLETION",
        "BUG_AUTH_EXPIRED_COMPLETION" => "AUTH_CURRENT_COMPLETION",
        "BUG_AUTH_REFUND" => "AUTH_CHARGES_RETAINED",
        "BUG_AUTH_ARCHIVE_UNKNOWN" => "AUTH_UNKNOWN_RETAINED",
        "WITNESS_AUTH_TWO_WORKERS" => "REACH_AUTH_TWO_WORKERS",
        "WITNESS_AUTH_STALE_REJECTED" => "REACH_AUTH_STALE_REJECTED",
        "WITNESS_AUTH_LATE_EVIDENCE" => "REACH_AUTH_LATE_EVIDENCE",
        "WITNESS_AUTH_HANDOVER_COMPLETION" => "REACH_AUTH_HANDOVER_COMPLETION",
        "WITNESS_AUTH_EXPIRED_EVIDENCE" => "REACH_AUTH_EXPIRED_EVIDENCE",
        "BUG_RECOVERY_SEALED_SOURCE" => "RECOVERY_SEALED_SOURCE",
        "BUG_RECOVERY_UNFENCED" => "RECOVERY_SOURCE_FENCED",
        "BUG_RECOVERY_STALE_SNAPSHOT" => "RECOVERY_FINAL_SNAPSHOT",
        "BUG_RECOVERY_OPEN_RESTORE" => "RECOVERY_ACTIVATION_GATE",
        "BUG_RECOVERY_DROP_UNKNOWN" => "RECOVERY_UNKNOWN_RETAINED",
        "BUG_RECOVERY_DUPLICATE_ADMISSION" => "RECOVERY_IDENTITY_UNIQUE",
        "BUG_RECOVERY_ARCHIVE_REFUND" => "RECOVERY_CHARGES_RETAINED",
        "WITNESS_RECOVERY_STALE_AUDIT" => "REACH_RECOVERY_STALE_AUDIT",
        "WITNESS_RECOVERY_ACTIVE" => "REACH_RECOVERY_ACTIVE",
        "WITNESS_RECOVERY_LATE_RESULT" => "REACH_RECOVERY_LATE_RESULT",
        _ => return None,
    })
}

// SPIN can report a preprocessor-branch line for an inline assertion. Bind the
// exact expanded expression to its actual source assertion, never to another
// property which happens to occupy that line in the unprocessed file.
pub(super) fn replay_assertion(
    model_name: &str,
    model: &str,
    expression: &str,
) -> Option<&'static str> {
    if model_name != "shared-authority.pml" {
        return None;
    }
    let compact: String = model.chars().filter(|c| !c.is_whitespace()).collect();
    for (variable, marker) in [
        ("completed_after_handover", "REACH_AUTH_HANDOVER_COMPLETION"),
        ("expired_evidence", "REACH_AUTH_EXPIRED_EVIDENCE"),
    ] {
        if expression == format!("assert(!({variable}))")
            && compact.contains(&format!("assert(!{variable});/*{marker}*/"))
        {
            return Some(marker);
        }
    }
    if compact.contains(
        "assert(assigned_boot[t]==boot[w]&&state[t]==Dispatched);/*AUTH_CURRENT_COMPLETION*/",
    ) {
        for task in 0..2 {
            for worker in 0..2 {
                if expression == format!("assert(((assigned_boot[{task}]==boot[{worker}])&&(state[{task}]==Dispatched)))") {
                    return Some("AUTH_CURRENT_COMPLETION");
                }
            }
        }
    }
    None
}

fn model_case(
    options: &Options,
    cases: &mut Vec<Value>,
    source: &str,
    name: &str,
    defect: Option<&'static str>,
    no_reduce: bool,
    definitions: &[&str],
) -> Result<()> {
    let case = run_case(
        &options.root.join("formal").join(source),
        &options.out.join(name),
        options,
        CaseOptions {
            standalone: true,
            defect,
            no_reduce,
            definitions: definitions.iter().map(|value| value.to_string()).collect(),
            reachability_witness: defect.is_some_and(|value| value.starts_with("WITNESS_")),
            ..Default::default()
        },
    )?;
    println!("{name} {}", case["status"]);
    cases.push(case);
    Ok(())
}

fn run_models(options: &Options, cases: &mut Vec<Value>) -> Result<()> {
    for (name, no_reduce, definitions) in [
        ("shared-authority", false, &[][..]),
        ("shared-authority-no-reduction", true, &[][..]),
        (
            "shared-authority-split-scopes",
            false,
            &["SPLIT_SCOPES"][..],
        ),
        (
            "shared-authority-deployment-limit",
            false,
            &["DEPLOYMENT_LIMIT=1"][..],
        ),
        (
            "shared-authority-principal-limit",
            false,
            &["PRINCIPAL_LIMIT=1"][..],
        ),
        (
            "shared-authority-provider-limit",
            false,
            &["PROVIDER_LIMIT=1"][..],
        ),
        (
            "shared-authority-ancestor-limit",
            false,
            &["ANCESTOR_LIMIT=1"][..],
        ),
        (
            "shared-authority-child-ancestor-limit",
            false,
            &["LEAF_LIMIT=1"][..],
        ),
    ] {
        model_case(
            options,
            cases,
            "shared-authority.pml",
            name,
            None,
            no_reduce,
            definitions,
        )?;
    }
    for (name, defect, definitions) in [
        ("stale-admission", "BUG_AUTH_STALE_ADMISSION", &[][..]),
        ("stale-dispatch", "BUG_AUTH_STALE_DISPATCH", &[][..]),
        ("worker-fence", "BUG_AUTH_WORKER_FENCE", &[][..]),
        ("double-claim", "BUG_AUTH_DOUBLE_CLAIM", &[][..]),
        ("redispatch", "BUG_AUTH_REDISPATCH", &[][..]),
        (
            "deployment",
            "BUG_AUTH_DEPLOYMENT",
            &["DEPLOYMENT_LIMIT=1"][..],
        ),
        (
            "principal",
            "BUG_AUTH_PRINCIPAL",
            &["PRINCIPAL_LIMIT=1"][..],
        ),
        ("provider", "BUG_AUTH_PROVIDER", &["PROVIDER_LIMIT=1"][..]),
        ("ancestor", "BUG_AUTH_ANCESTOR", &["ANCESTOR_LIMIT=1"][..]),
        (
            "child-ancestor",
            "BUG_AUTH_CHILD_ANCESTOR",
            &["LEAF_LIMIT=1"][..],
        ),
        ("drop-late", "BUG_AUTH_DROP_LATE", &[][..]),
        ("stale-completion", "BUG_AUTH_STALE_COMPLETION", &[][..]),
        ("expired-completion", "BUG_AUTH_EXPIRED_COMPLETION", &[][..]),
        ("refund", "BUG_AUTH_REFUND", &[][..]),
        ("archive-unknown", "BUG_AUTH_ARCHIVE_UNKNOWN", &[][..]),
    ] {
        model_case(
            options,
            cases,
            "shared-authority.pml",
            &format!("negative-authority-{name}"),
            Some(defect),
            false,
            definitions,
        )?;
    }
    for (name, defect) in [
        ("two-workers", "WITNESS_AUTH_TWO_WORKERS"),
        ("stale-rejected", "WITNESS_AUTH_STALE_REJECTED"),
        ("late-evidence", "WITNESS_AUTH_LATE_EVIDENCE"),
        ("handover-completion", "WITNESS_AUTH_HANDOVER_COMPLETION"),
        ("expired-evidence", "WITNESS_AUTH_EXPIRED_EVIDENCE"),
    ] {
        model_case(
            options,
            cases,
            "shared-authority.pml",
            &format!("reach-authority-{name}"),
            Some(defect),
            false,
            &[],
        )?;
    }
    for (name, no_reduce) in [
        ("authority-recovery", false),
        ("authority-recovery-no-reduction", true),
    ] {
        model_case(
            options,
            cases,
            "authority-recovery.pml",
            name,
            None,
            no_reduce,
            &[],
        )?;
    }
    for (name, defect) in [
        ("sealed-source", "BUG_RECOVERY_SEALED_SOURCE"),
        ("unfenced", "BUG_RECOVERY_UNFENCED"),
        ("stale-snapshot", "BUG_RECOVERY_STALE_SNAPSHOT"),
        ("open-restore", "BUG_RECOVERY_OPEN_RESTORE"),
        ("drop-unknown", "BUG_RECOVERY_DROP_UNKNOWN"),
        ("duplicate-admission", "BUG_RECOVERY_DUPLICATE_ADMISSION"),
        ("archive-refund", "BUG_RECOVERY_ARCHIVE_REFUND"),
    ] {
        model_case(
            options,
            cases,
            "authority-recovery.pml",
            &format!("negative-authority-recovery-{name}"),
            Some(defect),
            false,
            &[],
        )?;
    }
    for (name, defect) in [
        ("stale-audit", "WITNESS_RECOVERY_STALE_AUDIT"),
        ("active", "WITNESS_RECOVERY_ACTIVE"),
        ("late-result", "WITNESS_RECOVERY_LATE_RESULT"),
    ] {
        model_case(
            options,
            cases,
            "authority-recovery.pml",
            &format!("reach-authority-recovery-{name}"),
            Some(defect),
            false,
            &[],
        )?;
    }
    Ok(())
}

fn bounded(value: &Value, maximum: u64) -> Result<u64> {
    value
        .as_u64()
        .filter(|value| *value <= maximum)
        .ok_or_else(|| "authority trace integer out of bounds".into())
}

fn boolean(value: &Value) -> Result<&'static str> {
    match value.as_bool() {
        Some(true) => Ok("true"),
        Some(false) => Ok("false"),
        None => Err("authority trace requires a boolean".into()),
    }
}

fn state(value: &Value) -> Result<&'static str> {
    match value.as_str() {
        Some("absent") => Ok("Absent"),
        Some("admitted") => Ok("Admitted"),
        Some("claimed") => Ok("Claimed"),
        Some("dispatched") => Ok("Dispatched"),
        Some("completed") => Ok("Completed"),
        Some("unknown") => Ok("Unknown"),
        _ => Err("unsupported authority trace state".into()),
    }
}

fn trace_model(source: &str, trace: &Value) -> Result<String> {
    let mut body = String::from("\ninit { initialise();\n");
    let actions = trace["actions"]
        .as_array()
        .ok_or("authority trace needs actions")?;
    if actions.is_empty() {
        return Err("authority trace cannot be empty".into());
    }
    for (ordinal, action) in actions.iter().enumerate() {
        let operation = action["op"]
            .as_str()
            .ok_or("authority trace needs operation")?;
        let arguments = action["args"]
            .as_array()
            .ok_or("authority trace needs arguments")?;
        let arity = match operation {
            "admit" | "drain" => 2,
            "claim" | "dispatch" | "replay_dispatch" | "receipt" => 3,
            "handover" | "reboot" => 1,
            _ => return Err("unsupported authority trace operation".into()),
        };
        if arguments.len() != arity {
            return Err("invalid authority trace arity".into());
        }
        let mut values = Vec::new();
        for (index, argument) in arguments.iter().enumerate() {
            values.push(if operation == "drain" && index == 1 {
                boolean(argument)?.to_owned()
            } else {
                bounded(argument, 1)?.to_string()
            });
        }
        let model_operation = if operation == "replay_dispatch" {
            "dispatch"
        } else {
            operation
        };
        body += &format!("    {model_operation}({});\n", values.join(","));
        let expected = &action["expect"];
        if expected["accepted"] == false {
            let rejection = match action["reject_code"].as_str() {
                Some("stale_protocol_authority") => "StaleAuthority",
                Some("admission_draining") => "AdmissionDraining",
                Some("no_assignment") => "NoAssignment",
                Some("dispatch_already_granted") => "DispatchAlreadyGranted",
                _ => {
                    return Err(
                        "rejected authority trace action needs a known rejection code".into(),
                    )
                }
            };
            body += &format!("    assert(rejection == {rejection}); /* AUTH_TRACE_REJECTION */\n");
        }
        for name in ["accepted", "draining"] {
            body += &format!(
                "    assert({name} == {}); /* AUTH_TRACE_RESPONSE */\n",
                boolean(&expected[name])?
            );
        }
        for (name, maximum) in [("epoch", 2), ("controller", 1), ("charges", 2)] {
            let value = bounded(&expected[name], maximum)?;
            if name == "epoch" && value == 0 {
                return Err("authority epoch must be positive".into());
            }
            body += &format!("    assert({name} == {value}); /* AUTH_TRACE_OBSERVATION */\n");
        }
        for (name, model_name) in [
            ("states", "state"),
            ("authoritative", "authoritative"),
            ("stored_evidence", "stored_evidence"),
        ] {
            let observations = expected[name]
                .as_array()
                .ok_or("authority trace needs task observations")?;
            if observations.len() != 2 {
                return Err("authority trace requires two task observations".into());
            }
            for (task, observation) in observations.iter().enumerate() {
                let value = match name {
                    "states" => state(observation)?.to_owned(),
                    "authoritative" => boolean(observation)?.to_owned(),
                    _ => bounded(observation, 1)?.to_string(),
                };
                body += &format!(
                    "    assert({model_name}[{task}] == {value}); /* AUTH_TRACE_OBSERVATION */\n"
                );
            }
        }
        body += &format!("    printf(\"AUTH TRACE step {ordinal} {operation}\\n\");\n");
    }
    body += "}\n";
    Ok(format!("#define TRACE_ONLY\n{source}{body}"))
}

fn corpus_traces(corpus: &Value) -> Result<&[Value]> {
    if corpus["version"] != 1 {
        return Err("unsupported authority trace corpus version".into());
    }
    let traces = corpus["traces"]
        .as_array()
        .ok_or("authority corpus needs traces")?;
    if traces.is_empty() {
        return Err("authority corpus cannot be empty".into());
    }
    let mut names = std::collections::BTreeSet::new();
    for trace in traces {
        let name = trace["name"]
            .as_str()
            .ok_or("authority trace needs a name")?;
        if name.is_empty()
            || !name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
            || !names.insert(name)
        {
            return Err("invalid or duplicate authority trace name".into());
        }
    }
    Ok(traces)
}

pub(super) fn run(options: &Options, cases: &mut Vec<Value>) -> Result<()> {
    run_models(options, cases)?;
    let corpus: Value = serde_json::from_slice(&fs::read(
        options.root.join("formal/authority-traces.json"),
    )?)?;
    let source = fs::read_to_string(options.root.join("formal/shared-authority.pml"))?;
    let generated = options.out.join("authority-trace-sources");
    fs::create_dir_all(&generated)?;
    for trace in corpus_traces(&corpus)? {
        let name = trace["name"].as_str().unwrap();
        let name = format!("trace-authority-{name}");
        let path = generated.join(format!("{name}.pml"));
        fs::write(&path, trace_model(&source, trace)?)?;
        let mut case = run_case(
            &path,
            &options.out.join(&name),
            options,
            CaseOptions {
                standalone: true,
                ..Default::default()
            },
        )?;
        case["purpose"] = json!("shared_authority_trace_conformance");
        println!("{name} {}", case["status"]);
        cases.push(case);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn example() -> Value {
        json!({"actions":[{"op":"drain","args":[0,true],"expect":{
            "accepted":true,"epoch":1,"controller":0,"draining":true,"charges":0,
            "states":["absent","absent"],"authoritative":[false,false],"stored_evidence":[0,0]}}]})
    }

    #[test]
    fn trace_generation_preserves_observations_and_rejects_invalid_domains() {
        let trace = example();
        let generated = trace_model("/* model */", &trace).unwrap();
        assert!(generated.contains("drain(0,true)"));
        assert!(generated.contains("assert(state[1] == Absent)"));
        assert!(generated.contains("assert(accepted == true)"));
        for (field, value) in [
            ("epoch", json!(0)),
            ("charges", json!(3)),
            ("states", json!(["absent", "invalid"])),
            ("stored_evidence", json!([0, 2])),
            ("authoritative", json!([false])),
        ] {
            let mut invalid = trace.clone();
            invalid["actions"][0]["expect"][field] = value;
            assert!(trace_model("", &invalid).is_err(), "{field}");
        }
        for arguments in [json!([2, true]), json!([0, 1]), json!([0, true, 1])] {
            let mut invalid = trace.clone();
            invalid["actions"][0]["args"] = arguments;
            assert!(trace_model("", &invalid).is_err());
        }
        let mut invalid = trace;
        invalid["actions"][0]["op"] = json!("arbitrary_code");
        assert!(trace_model("", &invalid).is_err());
        assert!(trace_model("", &json!({"actions":[]})).is_err());
    }

    #[test]
    fn corpus_rejects_missing_or_duplicate_trace_coverage() {
        assert!(corpus_traces(&json!({"version":1,"traces":[]})).is_err());
        assert!(
            corpus_traces(&json!({"version":1,"traces":[{"name":"same"},{"name":"same"}]}))
                .is_err()
        );
        assert!(corpus_traces(&json!({"version":2,"traces":[{"name":"valid"}]})).is_err());
        assert!(corpus_traces(&json!({"version":1,"traces":[{"name":"../outside"}]})).is_err());
        assert_eq!(
            corpus_traces(&json!({"version":1,"traces":[{"name":"valid"}]}))
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn conditional_replays_bind_exact_source_properties() {
        let source = "assert(assigned_boot[t] == boot[w] && state[t] == Dispatched); /* AUTH_CURRENT_COMPLETION */\nassert(!expired_evidence); /* REACH_AUTH_EXPIRED_EVIDENCE */";
        let expression = "assert(((assigned_boot[1]==boot[0])&&(state[1]==Dispatched)))";
        assert_eq!(
            replay_assertion("shared-authority.pml", source, expression),
            Some("AUTH_CURRENT_COMPLETION")
        );
        assert_eq!(
            replay_assertion("shared-authority.pml", "", expression),
            None
        );
        assert_eq!(replay_assertion("another.pml", source, expression), None);
        assert_eq!(
            replay_assertion(
                "shared-authority.pml",
                source,
                &expression.replace("state[1]", "state[0]")
            ),
            None
        );
        assert_eq!(
            replay_assertion(
                "shared-authority.pml",
                source,
                "assert(!(expired_evidence))"
            ),
            Some("REACH_AUTH_EXPIRED_EVIDENCE")
        );
    }

    #[test]
    #[ignore = "requires installed SPIN and C compiler; included in the full verify command"]
    fn shared_authority_models_mutations_and_traces() {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .unwrap()
            .to_path_buf();
        let out = super::super::tests::scratch();
        let options = Options {
            root: root.clone(),
            examples: root.join("examples"),
            out: out.clone(),
            seconds: 30,
            memory: 512,
            depth: 100000,
            only: None,
            generate_only: false,
        };
        let mut cases = Vec::new();
        run(&options, &mut cases).unwrap();
        for case in cases {
            assert_eq!(case["status"], "PASS", "{case}; evidence {}", out.display());
        }
        fs::remove_dir_all(out).unwrap();
    }
}
