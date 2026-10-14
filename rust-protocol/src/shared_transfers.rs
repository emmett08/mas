//! Acknowledged transfer descriptors and upload chunks are authority, not cache.
//! S3 publication remains outside transactions; receipts never contain bytes.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
type Result<T> = std::result::Result<T, String>;
const CHUNK: i64 = 32768;
pub(crate) const DDL: &str = "
CREATE TABLE transfer_configuration(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),descriptor text NOT NULL);
CREATE TABLE artifact_transfers(id text PRIMARY KEY,owner text NOT NULL,scope text NOT NULL,kind text NOT NULL CHECK(kind IN ('upload','download')),descriptor text NOT NULL,size bigint NOT NULL CHECK(size>=0),offset_bytes bigint NOT NULL DEFAULT 0 CHECK(offset_bytes>=0 AND offset_bytes<=size),reference text,expires_at bigint NOT NULL,state text NOT NULL DEFAULT 'open' CHECK(state IN ('open','closed','expired')),chunks bigint NOT NULL DEFAULT 0 CHECK(chunks>=0 AND chunks<=65536));
CREATE TABLE artifact_transfer_chunks(transfer_id text NOT NULL REFERENCES artifact_transfers(id),offset_bytes bigint NOT NULL CHECK(offset_bytes>=0),bytes bytea NOT NULL CHECK(octet_length(bytes)>0 AND octet_length(bytes)<=32768),PRIMARY KEY(transfer_id,offset_bytes));
";
fn err() -> String {
    "invalid_artifact_request".into()
}
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str().ok_or_else(err)
}
fn number(v: &Value, k: &str, maximum: i64) -> Result<i64> {
    v[k].as_i64()
        .filter(|x| *x >= 0 && *x <= maximum)
        .ok_or_else(err)
}
fn fields(v: &Value, names: &[&str]) -> Result<()> {
    let m = v.as_object().ok_or_else(err)?;
    if m.len() != names.len() || names.iter().any(|k| !m.contains_key(*k)) {
        return Err(err());
    }
    Ok(())
}
fn decode(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| err())
}
fn digest(s: &str, n: usize) -> bool {
    s.len() == n
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn identity<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    let s = text(v, k)?;
    if !crate::identity(s) {
        return Err(err());
    }
    Ok(s)
}
fn now(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp()))::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0))
}
fn config(tx: &mut Transaction<'_>) -> Result<Value> {
    decode(
        tx.query_opt(
            "SELECT descriptor FROM transfer_configuration WHERE singleton",
            &[],
        )
        .map_err(db_error)?
        .ok_or("artifact_unavailable")?
        .get(0),
    )
}
fn scope<'a>(configuration: &'a Value, principal: &str, name: &str) -> Result<&'a Value> {
    let value = &configuration["scopes"][name];
    if value["principals"][principal].as_str().is_none() {
        return Err("artifact_unavailable".into());
    }
    Ok(value)
}
fn grant(tx: &mut Transaction<'_>, s: &Value, owner: &str, reference: &Value) -> Result<()> {
    crate::shared_artifacts::query(
        tx,
        "artifact.inspect",
        &json!({"namespace":s["namespace"],
        "principal":s["principals"][owner],"reference":reference}),
    )
    .map_err(|_| "artifact_unavailable".to_string())?;
    Ok(())
}
fn session(tx: &mut Transaction<'_>, p: &Value, allow_closed: bool) -> Result<Value> {
    let owner = identity(p, "principal")?;
    let id = text(p, "transfer_id")?;
    if !digest(id, 32) {
        return Err("artifact_unavailable".into());
    }
    let row = tx.query_opt("SELECT owner,scope,kind,descriptor,size,offset_bytes,reference,expires_at,state FROM artifact_transfers WHERE id=$1", &[&id]).map_err(db_error)?.ok_or("artifact_unavailable")?;
    if row.get::<_, String>(0) != owner {
        return Err("artifact_unavailable".into());
    }
    let configuration = config(tx)?;
    let s = scope(&configuration, owner, row.get(1))?;
    let mut d = decode(row.get(3))?;
    if d["scope_binding"] != s["binding"]
        || row.get::<_, i64>(7) <= now(tx)?
        || (!allow_closed && row.get::<_, &str>(8) != "open")
    {
        return Err("artifact_unavailable".into());
    }
    d["reference"] = row
        .get::<_, Option<&str>>(6)
        .map(decode)
        .transpose()?
        .unwrap_or(Value::Null);
    d["next_offset"] = json!(row.get::<_, i64>(5));
    d["expires_at"] = json!(row.get::<_, i64>(7));
    d["state"] = json!(row.get::<_, String>(8));
    if d["kind"] == "download" {
        grant(tx, s, owner, &d["reference"])?;
    }
    Ok(d)
}
fn slice(tx: &mut Transaction<'_>, id: &str, start: i64, length: i64) -> Result<Vec<u8>> {
    let end = start.checked_add(length).ok_or_else(err)?;
    let mut bytes = Vec::with_capacity(length as usize);
    for row in tx.query("SELECT offset_bytes,bytes FROM artifact_transfer_chunks WHERE transfer_id=$1 AND offset_bytes<$3 AND offset_bytes+octet_length(bytes)>$2 ORDER BY offset_bytes", &[&id,&start,&end]).map_err(db_error)? {
        let offset:i64 = row.get(0); let chunk:Vec<u8> = row.get(1);
        let left = start.max(offset); let right = end.min(offset+chunk.len() as i64);
        if left != start+bytes.len() as i64 { return Err("artifact_integrity_mismatch".into()) }
        bytes.extend_from_slice(&chunk[(left-offset) as usize..(right-offset) as usize]);
    }
    if bytes.len() as i64 != length {
        return Err("artifact_integrity_mismatch".into());
    }
    Ok(bytes)
}
fn metadata(v: &Value, s: &Value) -> Result<()> {
    let size = number(v, "size", 1_i64 << 40)?;
    if size > s["max_artifact_bytes"].as_i64().ok_or_else(err)? || !digest(text(v, "sha256")?, 64) {
        return Err("artifact_size_limit".into());
    }
    let media = text(v, "media_type")?;
    if media.len() > 128
        || media.split('/').count() != 2
        || media.split('/').any(|s| {
            s.is_empty()
                || !s.bytes().all(|c| {
                    c.is_ascii_lowercase() || c.is_ascii_digit() || b"!#$&^_.+-".contains(&c)
                })
        })
    {
        return Err(err());
    }
    for (key, is_digest) in [("input_sha256s", true), ("readers", false)] {
        let values = v[key].as_array().ok_or_else(err)?;
        if values.len() > 32 {
            return Err(err());
        }
        let mut previous = "";
        for value in values {
            let text = value.as_str().ok_or_else(err)?;
            if text <= previous
                || if is_digest {
                    !digest(text, 64)
                } else {
                    s["principals"][text].as_str().is_none()
                }
            {
                return Err(err());
            }
            previous = text;
        }
    }
    Ok(())
}
pub(crate) fn apply(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    match op {
        "transfer.configure" => {
            fields(
                p,
                &["scopes", "ttl_seconds", "max_sessions", "max_staging_bytes"],
            )?;
            if number(p, "ttl_seconds", 86400)? == 0
                || number(p, "max_sessions", 256)? == 0
                || number(p, "max_staging_bytes", 1_i64 << 40)? == 0
            {
                return Err(err());
            }
            let scopes = p["scopes"].as_object().ok_or_else(err)?;
            if scopes.is_empty() || scopes.len() > 64 {
                return Err(err());
            }
            for (name, s) in scopes {
                fields(
                    s,
                    &["namespace", "principals", "binding", "max_artifact_bytes"],
                )?;
                if !crate::identity(name)
                    || !digest(text(s, "binding")?, 64)
                    || number(s, "max_artifact_bytes", 5 * 1024 * 1024 * 1024)? == 0
                {
                    return Err(err());
                }
                identity(s, "namespace")?;
                let principals = s["principals"].as_object().ok_or_else(err)?;
                let mut actors = std::collections::BTreeSet::new();
                if principals.is_empty() || principals.len() > 256 {
                    return Err(err());
                }
                for (principal, actor) in principals {
                    let actor = actor.as_str().ok_or_else(err)?;
                    if !crate::identity(principal)
                        || !crate::identity(actor)
                        || !actors.insert(actor)
                    {
                        return Err(err());
                    }
                }
            }
            let encoded = p.to_string();
            if let Some(row) = tx
                .query_opt(
                    "SELECT descriptor FROM transfer_configuration WHERE singleton",
                    &[],
                )
                .map_err(db_error)?
            {
                if row.get::<_, &str>(0) != encoded {
                    return Err("artifact_configuration_conflict".into());
                }
            } else {
                tx.execute(
                    "INSERT INTO transfer_configuration(descriptor) VALUES($1)",
                    &[&encoded],
                )
                .map_err(db_error)?;
            }
            Ok(json!({"configured":true}))
        }
        "transfer.begin" => {
            fields(
                p,
                &[
                    "principal",
                    "transfer_id",
                    "scope",
                    "kind",
                    "sha256",
                    "size",
                    "media_type",
                    "input_sha256s",
                    "readers",
                    "reference",
                ],
            )?;
            let owner = identity(p, "principal")?;
            let id = text(p, "transfer_id")?;
            let name = identity(p, "scope")?;
            let kind = text(p, "kind")?;
            if !digest(id, 32) || !["upload", "download"].contains(&kind) {
                return Err(err());
            }
            let c = config(tx)?;
            let s = scope(&c, owner, name)?;
            metadata(p, s)?;
            let size = number(p, "size", 1_i64 << 40)?;
            if kind == "upload" && size > 65536 * CHUNK {
                return Err("artifact_size_limit".into());
            }
            if kind == "upload" && !p["reference"].is_null()
                || kind == "download" && !p["readers"].as_array().ok_or_else(err)?.is_empty()
            {
                return Err(err());
            }
            if kind == "download" {
                grant(tx, s, owner, &p["reference"])?;
                for key in ["sha256", "size", "media_type", "input_sha256s"] {
                    if p[key] != p["reference"][key] {
                        return Err(err());
                    }
                }
            }
            let now = now(tx)?;
            tx.execute("DELETE FROM artifact_transfer_chunks WHERE transfer_id IN (SELECT id FROM artifact_transfers WHERE expires_at<=$1)",&[&now]).map_err(db_error)?;
            tx.execute("UPDATE artifact_transfers SET state='expired' WHERE state='open' AND expires_at<=$1",&[&now]).map_err(db_error)?;
            let totals=tx.query_one("SELECT count(*),count(*) FILTER(WHERE state='open'),COALESCE(sum(size) FILTER(WHERE state='open'),0)::bigint FROM artifact_transfers",&[]).map_err(db_error)?;
            if totals.get::<_, i64>(0) >= 100000
                || totals.get::<_, i64>(1) >= c["max_sessions"].as_i64().unwrap()
                || size > c["max_staging_bytes"].as_i64().unwrap() - totals.get::<_, i64>(2)
            {
                return Err("artifact_staging_full".into());
            }
            let expiry = now + c["ttl_seconds"].as_i64().unwrap();
            let mut d = p.clone();
            d.as_object_mut().unwrap().remove("principal");
            d["owner"] = json!(owner);
            d["scope_binding"] = s["binding"].clone();
            let reference = (!p["reference"].is_null()).then(|| p["reference"].to_string());
            let offset = if kind == "download" { size } else { 0 };
            tx.execute("INSERT INTO artifact_transfers(id,owner,scope,kind,descriptor,size,offset_bytes,reference,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&id,&owner,&name,&kind,&d.to_string(),&size,&offset,&reference,&expiry]).map_err(db_error)?;
            session(tx, &json!({"principal":owner,"transfer_id":id}), false)
        }
        "transfer.chunk" => {
            fields(p, &["principal", "transfer_id", "offset", "hex"])?;
            let d = session(tx, p, false)?;
            let id = text(p, "transfer_id")?;
            let offset = number(p, "offset", 1_i64 << 40)?;
            let encoded = text(p, "hex")?;
            if d["kind"] != "upload"
                || !d["reference"].is_null()
                || encoded.is_empty()
                || encoded.len() > CHUNK as usize * 2
                || encoded.len() % 2 != 0
                || !digest(encoded, encoded.len())
            {
                return Err("invalid_artifact_chunk".into());
            }
            let bytes: Vec<u8> = (0..encoded.len())
                .step_by(2)
                .map(|i| u8::from_str_radix(&encoded[i..i + 2], 16).unwrap())
                .collect();
            let length = bytes.len() as i64;
            let acknowledged = d["next_offset"].as_i64().unwrap();
            if offset + length > d["size"].as_i64().unwrap() {
                return Err("invalid_artifact_chunk".into());
            }
            if offset < acknowledged {
                if offset + length > acknowledged || slice(tx, id, offset, length)? != bytes {
                    return Err("artifact_chunk_conflict".into());
                }
                return Ok(json!({"next_offset":acknowledged}));
            }
            if offset != acknowledged {
                return Err("artifact_chunk_conflict".into());
            }
            let changed=tx.execute("UPDATE artifact_transfers SET chunks=chunks+1,offset_bytes=$2 WHERE id=$1 AND chunks<65536",&[&id,&(offset+length)]).map_err(db_error)?;
            if changed != 1 {
                return Err("artifact_staging_full".into());
            }
            tx.execute("INSERT INTO artifact_transfer_chunks(transfer_id,offset_bytes,bytes) VALUES($1,$2,$3)",&[&id,&offset,&bytes]).map_err(db_error)?;
            Ok(json!({"next_offset":offset+length}))
        }
        "transfer.complete" => {
            fields(p, &["principal", "transfer_id", "reference"])?;
            let d = session(tx, p, false)?;
            let id = text(p, "transfer_id")?;
            if d["kind"] != "upload" || d["next_offset"] != d["size"] {
                return Err("artifact_integrity_mismatch".into());
            }
            let c = config(tx)?;
            let s = scope(&c, text(p, "principal")?, text(&d, "scope")?)?;
            grant(tx, s, text(p, "principal")?, &p["reference"])?;
            for key in ["sha256", "size", "media_type", "input_sha256s"] {
                if d[key] != p["reference"][key] {
                    return Err("artifact_integrity_mismatch".into());
                }
            }
            if p["reference"]["producer"] != s["principals"][text(p, "principal")?]
                || (!d["reference"].is_null() && d["reference"] != p["reference"])
            {
                return Err("artifact_unavailable".into());
            }
            let actor = &s["principals"][text(p, "principal")?];
            let mut readers: Vec<Value> = d["readers"]
                .as_array()
                .ok_or_else(err)?
                .iter()
                .map(|v| s["principals"][v.as_str().unwrap()].clone())
                .filter(|v| v != actor)
                .collect();
            readers.sort_by(|a, b| a.as_str().cmp(&b.as_str()));
            let retained = tx
                .query_one(
                    "SELECT readers FROM artifact_manifests WHERE namespace=$1 AND identity=$2",
                    &[
                        &text(s, "namespace")?,
                        &text(&p["reference"], "artifact_id")?,
                    ],
                )
                .map_err(db_error)?;
            if decode(retained.get(0))? != json!(readers) {
                return Err("artifact_integrity_mismatch".into());
            }
            tx.execute(
                "UPDATE artifact_transfers SET reference=$2 WHERE id=$1",
                &[&id, &p["reference"].to_string()],
            )
            .map_err(db_error)?;
            tx.execute(
                "DELETE FROM artifact_transfer_chunks WHERE transfer_id=$1",
                &[&id],
            )
            .map_err(db_error)?;
            Ok(json!({"reference":p["reference"]}))
        }
        "transfer.close" => {
            fields(p, &["principal", "transfer_id"])?;
            session(tx, p, true)?;
            let id = text(p, "transfer_id")?;
            tx.execute(
                "UPDATE artifact_transfers SET state='closed' WHERE id=$1",
                &[&id],
            )
            .map_err(db_error)?;
            tx.execute(
                "DELETE FROM artifact_transfer_chunks WHERE transfer_id=$1",
                &[&id],
            )
            .map_err(db_error)?;
            Ok(json!({"closed":true}))
        }
        _ => Err(err()),
    }
}
pub(crate) fn query(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    match op {
        "transfer.session" => {
            fields(p, &["principal", "transfer_id"])?;
            session(tx, p, false)
        }
        "transfer.read" => {
            fields(p, &["principal", "transfer_id", "offset"])?;
            let d = session(tx, p, false)?;
            let offset = number(p, "offset", 1_i64 << 40)?;
            if d["kind"] != "upload"
                || !d["reference"].is_null()
                || offset > d["next_offset"].as_i64().unwrap()
            {
                return Err("artifact_unavailable".into());
            }
            let data = slice(
                tx,
                text(p, "transfer_id")?,
                offset,
                CHUNK.min(d["next_offset"].as_i64().unwrap() - offset),
            )?;
            let encoded: String = data.iter().map(|b| format!("{b:02x}")).collect();
            Ok(json!({"hex":encoded,"next_offset":offset+data.len() as i64}))
        }
        _ => Err(err()),
    }
}
