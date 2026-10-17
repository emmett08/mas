//! Version 1 assessment payload contract. The existing MAS envelope is unchanged.
//!
//! Structural validity does not establish a finding's truth. Hosts bind exact
//! records and the authorised evaluator before retaining an assessment.
use crate::parse_json;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

pub const SCHEMA_VERSION: u64 = 1;
pub(super) type Result<T> = std::result::Result<T, AssessmentError>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AssessmentError {
    pub code: &'static str,
    pub message: String,
}
impl std::fmt::Display for AssessmentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}
impl std::error::Error for AssessmentError {}
pub(super) fn reject<T>(code: &'static str, message: impl Into<String>) -> Result<T> {
    Err(AssessmentError {
        code,
        message: message.into(),
    })
}
pub(super) fn object<'a>(v: &'a Value, fields: &[&str]) -> Result<&'a Map<String, Value>> {
    let o = v.as_object().ok_or_else(|| AssessmentError {
        code: "TYPE",
        message: "record must be an object".into(),
    })?;
    if o.len() != fields.len() || fields.iter().any(|k| !o.contains_key(*k)) {
        return reject("FIELDS", format!("expected exactly {}", fields.join(", ")));
    }
    Ok(o)
}
pub(super) fn text(v: &Value) -> Result<String> {
    match v.as_str() {
        Some(s) if !s.is_empty() && s.len() <= 8192 => Ok(s.into()),
        _ => reject("TYPE", "nonempty bounded string required"),
    }
}
pub(super) fn id(v: &Value) -> Result<String> {
    let s = text(v)?;
    if s.len() > 128
        || !s.as_bytes()[0].is_ascii_alphanumeric()
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:/-".contains(&b))
    {
        return reject("IDENTITY", "invalid record identity");
    }
    Ok(s)
}
pub(super) fn hash(v: &Value) -> Result<String> {
    let s = text(v)?;
    if s.len() != 64
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return reject("DIGEST", "SHA256 requires 64 lowercase hexadecimal digits");
    }
    Ok(s)
}
pub(super) fn version(v: &Value) -> Result<()> {
    if v.as_u64() != Some(SCHEMA_VERSION) {
        return reject("VERSION", "unsupported assessment schema version");
    }
    Ok(())
}
pub(super) fn integer(v: &Value) -> Result<i64> {
    v.as_i64().ok_or_else(|| AssessmentError {
        code: "METRIC",
        message: "metric must be an exact signed 64-bit integer".into(),
    })
}
pub(super) fn list<T>(v: &Value, parse: impl Fn(&Value) -> Result<T>) -> Result<Vec<T>> {
    let items = v.as_array().ok_or_else(|| AssessmentError {
        code: "TYPE",
        message: "array required".into(),
    })?;
    if items.len() > 128 {
        return reject("SIZE", "at most 128 entries permitted");
    }
    items.iter().map(parse).collect()
}
pub(super) fn unique<'a>(ids: impl Iterator<Item = &'a str>) -> Result<()> {
    let mut seen = BTreeSet::new();
    for id in ids {
        if !seen.insert(id) {
            return reject("DUPLICATE", "duplicate record reference or criterion");
        }
    }
    Ok(())
}
pub(super) fn bounded_value(v: &Value) -> Result<()> {
    parse_json(&v.to_string())
        .map(|_| ())
        .map_err(|e| AssessmentError {
            code: e.code,
            message: e.message,
        })
}

/// SHA256 of compact serde_json with lexicographically sorted object keys.
/// Canonicalisation is specific to this profile, not RFC 8785 floating point JSON.
pub fn canonical_digest(value: &Value) -> String {
    fn sorted(value: &Value) -> Value {
        match value {
            Value::Object(o) => {
                let ordered: BTreeMap<_, _> =
                    o.iter().map(|(k, v)| (k.clone(), sorted(v))).collect();
                Value::Object(ordered.into_iter().collect())
            }
            Value::Array(a) => Value::Array(a.iter().map(sorted).collect()),
            _ => value.clone(),
        }
    }
    Sha256::digest(sorted(value).to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
