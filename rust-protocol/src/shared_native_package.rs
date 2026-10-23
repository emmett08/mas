//! Verify retained native source package bytes without asserting build attestation.
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;

pub(crate) fn validate(value: &Value) -> Result<(), String> {
    let object = value.as_object().ok_or("invalid_native_package")?;
    if object.len() != 3
        || object
            .keys()
            .any(|k| !["format", "bundle_digest", "files"].contains(&k.as_str()))
        || value["format"] != "mas-native-implementation/1"
        || !digest(&value["bundle_digest"])
    {
        return Err("invalid_native_package".into());
    }
    let files = value["files"]
        .as_array()
        .filter(|f| !f.is_empty() && f.len() <= 4096)
        .ok_or("invalid_native_package")?;
    let mut paths = BTreeSet::new();
    for file in files {
        let fields = file.as_object().ok_or("invalid_native_package")?;
        let path = file["path"].as_str().ok_or("invalid_native_package")?;
        if fields.len() != 3
            || fields
                .keys()
                .any(|k| !["path", "sha256", "bytes"].contains(&k.as_str()))
            || path.is_empty()
            || path.len() > 4096
            || path.contains('\\')
            || path.chars().any(char::is_control)
            || path
                .split('/')
                .any(|part| part.is_empty() || part == "." || part == "..")
            || !paths.insert(path)
            || !digest(&file["sha256"])
        {
            return Err("invalid_native_package".into());
        }
        let bytes = file["bytes"]
            .as_array()
            .ok_or("invalid_native_package")?
            .iter()
            .map(|v| {
                v.as_u64()
                    .filter(|n| *n <= 255)
                    .map(|n| n as u8)
                    .ok_or("invalid_native_package")
            })
            .collect::<Result<Vec<u8>, _>>()?;
        let hash: String = Sha256::digest(&bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        if file["sha256"] != hash {
            return Err("native_package_file_digest_mismatch".into());
        }
    }
    Ok(())
}
fn digest(v: &Value) -> bool {
    v.as_str().is_some_and(|s| {
        s.len() == 64
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn native_package_binds_exact_file_bytes_and_unique_relative_names() {
        let hash: String = Sha256::digest(b"x")
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let mut v = json!({"format":"mas-native-implementation/1","bundle_digest":"a".repeat(64),"files":[{"path":"source/main.rs","sha256":hash,"bytes":[120]}]});
        assert!(validate(&v).is_ok());
        v["files"][0]["bytes"] = json!([121]);
        assert!(validate(&v).is_err());
        v["files"][0]["bytes"] = json!([120]);
        v["files"][0]["path"] = json!("../source");
        assert!(validate(&v).is_err());
        v["files"][0]["path"] = json!("source");
        let duplicate = v["files"][0].clone();
        v["files"].as_array_mut().unwrap().push(duplicate);
        assert!(validate(&v).is_err());
    }
}
