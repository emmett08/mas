use mas_protocol_codec::{assessment::*, parse_message, MAX_BYTES, MAX_DEPTH};
use serde_json::{json, Value};

fn fixture() -> (Candidate, Rubric, Evidence, Evaluator, Assessment) {
    let output = json!({"diagnosis":"missing configuration","abstain":false});
    let candidate=Candidate::from_value(&json!({"schema_version":1,"id":"candidate-0","output_sha256":canonical_digest(&output),"output":output,"parent":null,"feedback":[]})).unwrap();
    let rubric=Rubric::from_value(&json!({"schema_version":1,"id":"rubric-1","criteria":[{"id":"diagnosis","measurement":"deterministic","minimum":0,"maximum":100}]})).unwrap();
    let content =
        json!({"test":"diagnosis-equals-frozen-answer","observed":"missing configuration"});
    let evidence=Evidence::from_value(&json!({"schema_version":1,"id":"evidence-0","content_sha256":canonical_digest(&content),"content":content})).unwrap();
    let evaluator =
        Evaluator::from_value(&json!({"id":"reviewer","version":"1","kind":"deterministic"}))
            .unwrap();
    let assessment=Assessment::from_value(&json!({"schema_version":1,"id":"assessment-0","candidate":candidate.reference().to_value(),"evaluator":evaluator.to_value(),"rubric":rubric.reference().to_value(),"status":"completed","findings":[{"criterion_id":"diagnosis","verdict":"pass","score":100,"evidence":[evidence.reference().to_value()],"explanation":"Expected answer matched"}],"error":null})).unwrap();
    (candidate, rubric, evidence, evaluator, assessment)
}

#[test]
fn exact_assessment_roundtrips_inside_existing_envelope() {
    let (c, r, e, ev, a) = fixture();
    validate_assessment(&a, &c, &r, &[e], &ev).unwrap();
    assert!(a.passes());
    let wire=format!("message \"assessment-0\" run \"run\" from \"reviewer\" to \"coordinator\" version \"1\" kind \"Result\" payload {};",a.payload());
    let message = parse_message(&wire, MAX_BYTES, MAX_DEPTH).unwrap();
    assert_eq!(Assessment::from_message(&message).unwrap(), a);
    let mut changed = message;
    changed.sender = "intruder".into();
    assert_eq!(
        Assessment::from_message(&changed).unwrap_err().code,
        "AUTHORITY"
    );
}

#[test]
fn public_validator_rejects_wrong_references_authority_metrics_and_criteria() {
    let (c, r, e, ev, a) = fixture();
    for change in 0..8 {
        let mut bad = a.clone();
        match change {
            0 => bad.candidate.sha256 = "0".repeat(64),
            1 => bad.rubric.id = "other".into(),
            2 => bad.evaluator.id = "intruder".into(),
            3 => bad.findings[0].score = Some(101),
            4 => bad.findings[0].criterion_id = "invented".into(),
            5 => bad.findings[0].evidence[0].sha256 = "0".repeat(64),
            6 => bad.evaluator.kind = EvaluatorKind::Model,
            7 => bad.schema_version = 2,
            _ => unreachable!(),
        }
        assert!(
            validate_assessment(&bad, &c, &r, std::slice::from_ref(&e), &ev).is_err(),
            "case {change}"
        );
    }
    let mut incomplete = a.clone();
    incomplete.findings.clear();
    assert!(validate_assessment(&incomplete, &c, &r, std::slice::from_ref(&e), &ev).is_err());
    let mut bigger_rubric = r.clone();
    let mut criterion = r.criteria[0].clone();
    criterion.id = "safety".into();
    bigger_rubric.criteria.push(criterion);
    incomplete = a;
    incomplete.rubric = bigger_rubric.reference();
    assert_eq!(
        validate_assessment(&incomplete, &c, &bigger_rubric, &[e], &ev)
            .unwrap_err()
            .code,
        "RUBRIC"
    );
}

#[test]
fn evaluator_error_inconclusive_and_failed_criterion_remain_distinct() {
    let (c, r, e, ev, a) = fixture();
    let mut failed = a.clone();
    failed.findings[0].verdict = Verdict::Fail;
    failed.findings[0].score = Some(0);
    validate_assessment(&failed, &c, &r, std::slice::from_ref(&e), &ev).unwrap();
    assert!(!failed.passes());
    let mut inconclusive = a.clone();
    inconclusive.status = AssessmentStatus::Inconclusive;
    inconclusive.findings[0].verdict = Verdict::Inconclusive;
    inconclusive.findings[0].score = None;
    inconclusive.findings[0].evidence.clear();
    validate_assessment(&inconclusive, &c, &r, std::slice::from_ref(&e), &ev).unwrap();
    assert!(!inconclusive.passes());
    let mut error = a;
    error.status = AssessmentStatus::Error;
    error.error = Some("evaluator timeout".into());
    assert!(validate_assessment(&error, &c, &r, std::slice::from_ref(&e), &ev).is_err());
    assert!(!error.passes());
    error.findings.clear();
    validate_assessment(&error, &c, &r, &[e], &ev).unwrap();
    assert!(!error.passes());
}

#[test]
fn history_deduplicates_exact_content_and_retains_conflicting_findings() {
    let (_, _, _, _, a) = fixture();
    let mut history = AssessmentHistory::default();
    assert!(history.insert(a.clone()).unwrap());
    assert!(!history.insert(a.clone()).unwrap());
    assert_eq!(history.len(), 1);
    let mut contrary = a.clone();
    contrary.findings[0].verdict = Verdict::Fail;
    contrary.findings[0].score = Some(0);
    assert_eq!(
        history.insert(contrary.clone()).unwrap_err().code,
        "IDENTITY_CONFLICT"
    );
    contrary.id = "assessment-1".into();
    assert!(history.insert(contrary).unwrap());
    assert_eq!(history.len(), 2);
    assert_eq!(history.get(&a.id), Some(&a));
}

#[test]
fn revision_binds_exact_parent_and_supplied_feedback() {
    let (c, _, _, _, a) = fixture();
    let mut revised = c.clone();
    revised.id = "candidate-1".into();
    revised.parent = Some(c.reference());
    revised.feedback = vec![a.reference()];
    validate_candidate_revision(&revised, Some(&c), std::slice::from_ref(&a)).unwrap();
    assert!(validate_candidate_revision(&revised, Some(&c), &[]).is_err());
    let mut other = c.clone();
    other.output = json!({"diagnosis":"other"});
    other.output_sha256 = canonical_digest(&other.output);
    assert!(validate_candidate_revision(&revised, Some(&other), std::slice::from_ref(&a)).is_err());
    let mut stale = a.clone();
    stale.candidate = other.reference();
    revised.feedback = vec![stale.reference()];
    assert!(validate_candidate_revision(&revised, Some(&c), &[stale]).is_err());
    revised.id = c.id.clone();
    assert!(Candidate::from_value(&revised.to_value()).is_err());
    validate_candidate_revision(&c, None, &[]).unwrap();
}

#[test]
fn versions_extra_fields_float_metrics_and_stale_hashes_are_rejected() {
    let (c, r, _, _, a) = fixture();
    let mut v = a.to_value();
    v["schema_version"] = json!(2);
    assert_eq!(Assessment::from_value(&v).unwrap_err().code, "VERSION");
    let mut v = a.to_value();
    v["permission"] = json!("admin");
    assert_eq!(Assessment::from_value(&v).unwrap_err().code, "FIELDS");
    let mut v = a.to_value();
    v["findings"][0]["score"] = json!(0.5);
    assert!(Assessment::from_value(&v).is_err());
    let mut v = c.to_value();
    v["output"] = json!("changed");
    assert_eq!(Candidate::from_value(&v).unwrap_err().code, "DIGEST");
    let mut v = r.to_value();
    v["criteria"][0]["maximum"] = json!(-1);
    assert_eq!(Rubric::from_value(&v).unwrap_err().code, "METRIC");
    let mut v = r.to_value();
    v["criteria"][0]["maximum"] = json!(u64::MAX);
    assert_eq!(Rubric::from_value(&v).unwrap_err().code, "METRIC");
    let v: Value = serde_json::from_str("{\"z\":0,\"a\":{\"y\":2,\"b\":1}}").unwrap();
    assert_eq!(
        canonical_digest(&v),
        canonical_digest(&json!({"a":{"b":1,"y":2},"z":0}))
    );
}
