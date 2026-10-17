//! The host-facing pure CLI shares the authoritative assessment validators.
use mas_protocol_codec::assessment::{canonical_digest, DigestRef};
use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};

fn cli(source: &str) -> (bool, Value) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_mas"))
        .args(["assessment", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(source.as_bytes())
        .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.stderr.is_empty());
    (
        output.status.success(),
        serde_json::from_slice(&output.stdout).unwrap(),
    )
}

fn fixtures() -> (Value, Value, Value, Value, Value) {
    let output = json!({"answer":9007199254740993_u64,"unicode":"λ"});
    let candidate = json!({"schema_version":1,"id":"candidate-1","output":output,
        "output_sha256":canonical_digest(&output),"parent":null,"feedback":[]});
    let rubric = json!({"schema_version":1,"id":"rubric-1","criteria":[
        {"id":"exact","measurement":"deterministic","minimum":0,"maximum":1}]});
    let authority = json!({"id":"checker","version":"1","kind":"deterministic"});
    let evidence = json!({"schema_version":1,"id":"evidence-1","content":{"verified":true},
        "content_sha256":canonical_digest(&json!({"verified":true}))});
    let assessment = json!({"schema_version":1,"id":"assessment-1",
        "candidate":DigestRef::of("candidate-1",&candidate).to_value(),"evaluator":authority,
        "rubric":DigestRef::of("rubric-1",&rubric).to_value(),"status":"completed",
        "findings":[{"criterion_id":"exact","verdict":"pass","score":1,
            "evidence":[DigestRef::of("evidence-1",&evidence).to_value()],"explanation":"Exact check passed"}],"error":null});
    (candidate, rubric, authority, evidence, assessment)
}

#[test]
fn contextual_validation_preserves_exact_records_and_rejects_foreign_authority() {
    let (candidate, rubric, authority, evidence, assessment) = fixtures();
    let source = json!({"operation":"record","kind":"candidate","record":candidate});
    let (ok, response) = cli(&source.to_string());
    assert!(ok);
    assert_eq!(response["result"]["record"], candidate);
    assert_eq!(
        response["result"]["reference"],
        DigestRef::of("candidate-1", &candidate).to_value()
    );
    let mut command = json!({"operation":"assessment","assessment":assessment,"candidate":candidate,
        "rubric":rubric,"evidence":[evidence],"authority":authority});
    let (ok, response) = cli(&command.to_string());
    assert!(ok);
    assert_eq!(response["result"]["passes"], true);
    command["authority"]["id"] = json!("foreign-checker");
    let (ok, response) = cli(&command.to_string());
    assert!(!ok);
    assert_eq!(
        response["error"]["message"],
        "assessment operation rejected"
    );
    command["authority"]["id"] = json!("checker");
    command["evidence"][0]["content"] = json!({"verified":false});
    assert!(!cli(&command.to_string()).0);
}

#[test]
fn revision_requires_exact_feedback_and_errors_never_pass() {
    let (candidate, rubric, authority, evidence, assessment) = fixtures();
    let output = json!({"answer":42});
    let revised = json!({"schema_version":1,"id":"candidate-2","output":output,
        "output_sha256":canonical_digest(&output),"parent":DigestRef::of("candidate-1", &candidate).to_value(),
        "feedback":[DigestRef::of("assessment-1", &assessment).to_value()]});
    let mut command = json!({"operation":"revision","candidate":revised,"parent":candidate,"feedback":[assessment]});
    assert!(cli(&command.to_string()).0);
    command["feedback"] = json!([]);
    assert!(!cli(&command.to_string()).0);
    let mut error = assessment;
    error["status"] = json!("error");
    error["findings"] = json!([]);
    error["error"] = json!("evaluation unavailable");
    let command = json!({"operation":"assessment","assessment":error,"candidate":candidate,
        "rubric":rubric,"evidence":[evidence],"authority":authority});
    let (ok, response) = cli(&command.to_string());
    assert!(ok);
    assert_eq!(response["result"]["passes"], false);
}

#[test]
fn malformed_commands_are_bounded_and_never_echo_inputs() {
    for source in [
        r#"{"operation":"digest","operation":"record","value":1}"#.into(),
        r#"{"operation":"digest","value":1.5}"#.into(),
        r#"{"operation":"digest","value":1,"secret":"private marker"}"#.into(),
        format!(
            r#"{{"operation":"digest","value":"{}"}}"#,
            "x".repeat(65536)
        ),
    ] {
        let (ok, response) = cli(&source);
        assert!(!ok);
        assert!(!response.to_string().contains("private marker"));
    }
    let (ok, response) = cli(r#"{"operation":"digest","value":{"z":9007199254740993,"a":"λ"}}"#);
    assert!(ok);
    assert_eq!(
        response["result"]["sha256"],
        canonical_digest(&json!({"a":"λ","z":9007199254740993_u64}))
    );
}

#[test]
fn scalar_digest_preserves_every_integer_digit() {
    for number in [
        "12",
        "9007199254740993",
        "340282366920938463463374607431768211456",
    ] {
        let source = format!(r#"{{"operation":"digest","value":{number}}}"#);
        let (ok, response) = cli(&source);
        assert!(ok);
        let value: Value = serde_json::from_str(number).unwrap();
        assert_eq!(response["result"]["sha256"], canonical_digest(&value));
    }
}
