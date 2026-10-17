//! Pure assessment validation for host adapters; supplied authority is host-owned.
use mas_protocol_codec::assessment::{
    canonical_digest, validate_assessment, validate_candidate_revision, Assessment,
    AssessmentError, Candidate, DigestRef, Evaluator, Evidence, Rubric,
};
use mas_protocol_codec::parse_json;
use serde_json::{json, Map, Value};

const REJECTED: &str = "assessment operation rejected";

fn checked<T>(result: Result<T, AssessmentError>) -> Result<T, String> {
    result.map_err(|_| REJECTED.into())
}

fn fields<'a>(value: &'a Value, expected: &[&str]) -> Result<&'a Map<String, Value>, String> {
    let object = value.as_object().ok_or(REJECTED)?;
    if object.len() != expected.len() || expected.iter().any(|name| !object.contains_key(*name)) {
        return Err(REJECTED.into());
    }
    Ok(object)
}

fn records<T>(
    value: &Value,
    decode: impl Fn(&Value) -> Result<T, AssessmentError>,
) -> Result<Vec<T>, String> {
    let values = value
        .as_array()
        .filter(|items| items.len() <= 128)
        .ok_or(REJECTED)?;
    values
        .iter()
        .map(|record| checked(decode(record)))
        .collect()
}

fn record(kind: &str, value: &Value) -> Result<Value, String> {
    let (record, reference) = match kind {
        "candidate" => {
            let record = checked(Candidate::from_value(value))?;
            (record.to_value(), record.reference())
        }
        "rubric" => {
            let record = checked(Rubric::from_value(value))?;
            (record.to_value(), record.reference())
        }
        "evidence" => {
            let record = checked(Evidence::from_value(value))?;
            (record.to_value(), record.reference())
        }
        "assessment" => {
            let record = checked(Assessment::from_value(value))?;
            (record.to_value(), record.reference())
        }
        "evaluator" => {
            let record = checked(Evaluator::from_value(value))?;
            let value = record.to_value();
            let reference = DigestRef::of(&record.id, &value);
            (value, reference)
        }
        _ => return Err(REJECTED.into()),
    };
    Ok(json!({"record":record,"reference":reference.to_value()}))
}

pub(super) fn execute(source: &str) -> Result<Value, String> {
    // parse_json enforces complete input, duplicate rejection, exact integers,
    // total command bytes and nesting before any operation is selected.
    let command = parse_json(source).map_err(|_| REJECTED)?;
    match command.get("operation").and_then(Value::as_str) {
        Some("record") => {
            let command = fields(&command, &["operation", "kind", "record"])?;
            record(
                command["kind"].as_str().ok_or(REJECTED)?,
                &command["record"],
            )
        }
        Some("digest") => {
            let command = fields(&command, &["operation", "value"])?;
            Ok(json!({"sha256":canonical_digest(&command["value"])}))
        }
        Some("assessment") => {
            let command = fields(
                &command,
                &[
                    "operation",
                    "assessment",
                    "candidate",
                    "rubric",
                    "evidence",
                    "authority",
                ],
            )?;
            let assessment = checked(Assessment::from_value(&command["assessment"]))?;
            let candidate = checked(Candidate::from_value(&command["candidate"]))?;
            let rubric = checked(Rubric::from_value(&command["rubric"]))?;
            let evidence = records(&command["evidence"], Evidence::from_value)?;
            let authority = checked(Evaluator::from_value(&command["authority"]))?;
            checked(validate_assessment(
                &assessment,
                &candidate,
                &rubric,
                &evidence,
                &authority,
            ))?;
            Ok(json!({"reference":assessment.reference().to_value(),"passes":assessment.passes()}))
        }
        Some("revision") => {
            let command = fields(&command, &["operation", "candidate", "parent", "feedback"])?;
            let candidate = checked(Candidate::from_value(&command["candidate"]))?;
            let parent = if command["parent"].is_null() {
                None
            } else {
                Some(checked(Candidate::from_value(&command["parent"]))?)
            };
            let feedback = records(&command["feedback"], Assessment::from_value)?;
            checked(validate_candidate_revision(
                &candidate,
                parent.as_ref(),
                &feedback,
            ))?;
            Ok(json!({"reference":candidate.reference().to_value()}))
        }
        _ => Err(REJECTED.into()),
    }
}
