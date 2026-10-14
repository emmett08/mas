//! Epoch-fenced artefact metadata. Object bytes are verified by the trusted
//! content port between reservation and publication; no SQL lock spans I/O.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;
pub(crate) const DDL: &str = "
CREATE TABLE artifact_namespaces(namespace text PRIMARY KEY,policy text NOT NULL,backend text NOT NULL,store_id text NOT NULL,closed boolean NOT NULL DEFAULT false);
CREATE TABLE artifact_objects(namespace text NOT NULL REFERENCES artifact_namespaces(namespace),digest text NOT NULL,size bigint NOT NULL CHECK(size>=0),verified boolean NOT NULL DEFAULT false,PRIMARY KEY(namespace,digest));
CREATE TABLE artifact_manifests(namespace text NOT NULL,identity text NOT NULL,digest text NOT NULL,reference text NOT NULL,readers text NOT NULL,revoked text NOT NULL DEFAULT '[]',PRIMARY KEY(namespace,identity),FOREIGN KEY(namespace,digest) REFERENCES artifact_objects(namespace,digest));
CREATE TABLE artifact_links(namespace text NOT NULL REFERENCES artifact_namespaces(namespace),identity text NOT NULL,owner text NOT NULL,state text NOT NULL CHECK(state IN ('live','unresolved','released')),refs text NOT NULL,PRIMARY KEY(namespace,identity));
";
fn err() -> String {
    "invalid shared artefact request".into()
}
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str().ok_or_else(err)
}
fn identity<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    let s = text(v, k)?;
    if !crate::identity(s) {
        return Err(err());
    }
    Ok(s)
}
fn namespace<'a>(p: &'a Value) -> Result<&'a str> {
    let s = identity(p, "namespace")?;
    if s.contains('/') {
        return Err(err());
    }
    Ok(s)
}
fn digest(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}
fn value(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| "invalid shared artefact record".into())
}
fn fields(p: &Value, keys: &[&str]) -> Result<()> {
    let m = p.as_object().ok_or_else(err)?;
    if m.len() != keys.len() || keys.iter().any(|k| !m.contains_key(*k)) {
        return Err(err());
    }
    Ok(())
}
fn strings(v: &Value, maximum: usize, check: fn(&str) -> bool) -> Result<Vec<String>> {
    let a = v.as_array().ok_or_else(err)?;
    if a.len() > maximum {
        return Err(err());
    }
    let mut r = Vec::new();
    for x in a {
        let s = x.as_str().ok_or_else(err)?;
        if !check(s) || r.last().is_some_and(|x: &String| x.as_str() >= s) {
            return Err(err());
        }
        r.push(s.to_owned());
    }
    Ok(r)
}
fn policy(v: &Value) -> Result<()> {
    fields(
        v,
        &[
            "max_artifact_bytes",
            "max_total_bytes",
            "max_artifacts",
            "max_links",
        ],
    )?;
    for k in [
        "max_artifact_bytes",
        "max_total_bytes",
        "max_artifacts",
        "max_links",
    ] {
        if v[k].as_i64().filter(|x| *x > 0).is_none() {
            return Err(err());
        }
    }
    if v["max_artifact_bytes"].as_i64() > v["max_total_bytes"].as_i64()
        || v["max_artifact_bytes"].as_i64().unwrap() > 5 * 1024 * 1024 * 1024
        || v["max_artifacts"].as_i64().unwrap() > 4096
        || v["max_links"].as_i64().unwrap() > 16384
    {
        return Err(err());
    }
    Ok(())
}
fn manifest(p: &Value) -> Result<(String, String, i64, Vec<String>)> {
    let r = &p["reference"];
    fields(
        r,
        &[
            "namespace",
            "artifact_id",
            "sha256",
            "size",
            "media_type",
            "producer",
            "input_sha256s",
            "format",
        ],
    )?;
    let ns = namespace(p)?;
    let principal = identity(p, "principal")?;
    if r["namespace"] != ns || r["producer"] != principal || r["format"] != "mas-artifact/1" {
        return Err(err());
    }
    let id = text(r, "artifact_id")?;
    let d = text(r, "sha256")?;
    let size = r["size"].as_i64().filter(|x| *x >= 0).ok_or_else(err)?;
    if !digest(id) || !digest(d) {
        return Err(err());
    }
    let media = text(r, "media_type")?;
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
    strings(&r["input_sha256s"], 32, digest)?;
    let readers = strings(&p["readers"], 32, crate::identity)?;
    if readers.iter().any(|x| x == principal) {
        return Err(err());
    }
    let mut frozen = r.clone();
    frozen.as_object_mut().unwrap().remove("artifact_id");
    let hash: String = Sha256::digest(
        json!({"reference":frozen,"readers":readers})
            .to_string()
            .as_bytes(),
    )
    .iter()
    .map(|b| format!("{b:02x}"))
    .collect();
    if hash != id {
        return Err("artefact manifest digest differs".into());
    }
    Ok((id.into(), d.into(), size, readers))
}
fn ns(tx: &mut Transaction<'_>, name: &str) -> Result<Value> {
    let r = tx
        .query_opt(
            "SELECT policy,backend,store_id,closed FROM artifact_namespaces WHERE namespace=$1",
            &[&name],
        )
        .map_err(db_error)?
        .ok_or("unknown artefact namespace")?;
    Ok(
        json!({"namespace":name,"policy":value(r.get(0))?,"backend":value(r.get(1))?,"store_id":r.get::<_,String>(2),"closed":r.get::<_,bool>(3)}),
    )
}
fn authorised(
    tx: &mut Transaction<'_>,
    name: &str,
    principal: &str,
    reference: &Value,
) -> Result<Value> {
    let id = text(reference, "artifact_id")?;
    let row=tx.query_opt("SELECT reference,readers,revoked FROM artifact_manifests WHERE namespace=$1 AND identity=$2",&[&name,&id]).map_err(db_error)?.ok_or("artefact unavailable or denied")?;
    let r = value(row.get(0))?;
    let readers = strings(&value(row.get(1))?, 32, crate::identity)?;
    let revoked = strings(&value(row.get(2))?, 32, crate::identity)?;
    if &r != reference
        || r["namespace"] != name
        || r["producer"] != principal
            && (!readers.iter().any(|x| x == principal) || revoked.iter().any(|x| x == principal))
    {
        return Err("artefact unavailable or denied".into());
    }
    Ok(
        json!({"reference":r,"readers":readers.into_iter().filter(|x|!revoked.contains(x)).collect::<Vec<_>>()}),
    )
}
pub(crate) fn apply(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    let name = namespace(p)?;
    match operation {
        "artifact.configure" => {
            fields(p, &["namespace", "policy", "backend", "store_id"])?;
            policy(&p["policy"])?;
            let backend = p["backend"].as_object().ok_or_else(err)?;
            if backend.get("format") != Some(&json!("mas-s3-content/1"))
                || p["backend"].to_string().len() > 8192
            {
                return Err(err());
            }
            let store_id = text(p, "store_id")?;
            if store_id.len() != 32
                || !store_id
                    .bytes()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            {
                return Err(err());
            }
            if tx
                .query_opt(
                    "SELECT namespace FROM artifact_namespaces WHERE namespace=$1",
                    &[&name],
                )
                .map_err(db_error)?
                .is_some()
            {
                let current = ns(tx, name)?;
                if current["policy"] != p["policy"]
                    || current["backend"] != p["backend"]
                    || current["store_id"] != store_id
                {
                    return Err("immutable artefact backend differs".into());
                }
                return Ok(current);
            }
            let count: i64 = tx
                .query_one("SELECT count(*) FROM artifact_namespaces", &[])
                .map_err(db_error)?
                .get(0);
            if count >= 4096 {
                return Err("artefact namespace quota exhausted".into());
            }
            tx.execute("INSERT INTO artifact_namespaces(namespace,policy,backend,store_id) VALUES($1,$2,$3,$4)",&[&name,&p["policy"].to_string(),&p["backend"].to_string(),&store_id]).map_err(db_error)?;
            ns(tx, name)
        }
        "artifact.reserve" | "artifact.complete" => {
            fields(p, &["namespace", "principal", "reference", "readers"])?;
            let (id, d, size, readers) = manifest(p)?;
            let current = ns(tx, name)?;
            if current["closed"] == true {
                return Err("artefact namespace is retired".into());
            }
            let pol = &current["policy"];
            if size > pol["max_artifact_bytes"].as_i64().unwrap() {
                return Err("artefact per-object quota exhausted".into());
            }
            let previous = tx
                .query_opt(
                    "SELECT size FROM artifact_objects WHERE namespace=$1 AND digest=$2",
                    &[&name, &d],
                )
                .map_err(db_error)?;
            if let Some(row) = previous {
                if row.get::<_, i64>(0) != size {
                    return Err("artefact reservation conflicts".into());
                }
            } else {
                if operation == "artifact.complete" {
                    return Err("artefact reservation absent".into());
                }
                let row=tx.query_one("SELECT count(*),COALESCE(sum(size),0)::bigint FROM artifact_objects WHERE namespace=$1",&[&name]).map_err(db_error)?;
                let count: i64 = row.get(0);
                let used: i64 = row.get(1);
                if count >= pol["max_artifacts"].as_i64().unwrap()
                    || used
                        .checked_add(size)
                        .filter(|v| *v <= pol["max_total_bytes"].as_i64().unwrap())
                        .is_none()
                {
                    return Err("artefact content quota exhausted".into());
                }
                tx.execute(
                    "INSERT INTO artifact_objects(namespace,digest,size) VALUES($1,$2,$3)",
                    &[&name, &d, &size],
                )
                .map_err(db_error)?;
            }
            if operation == "artifact.complete" {
                let count: i64 = tx
                    .query_one(
                        "SELECT count(*) FROM artifact_manifests WHERE namespace=$1",
                        &[&name],
                    )
                    .map_err(db_error)?
                    .get(0);
                let previous=tx.query_opt("SELECT reference,readers FROM artifact_manifests WHERE namespace=$1 AND identity=$2",&[&name,&id]).map_err(db_error)?;
                if let Some(row) = previous {
                    if value(row.get(0))? != p["reference"] || value(row.get(1))? != json!(readers)
                    {
                        return Err("artefact manifest conflict".into());
                    }
                } else {
                    if count >= pol["max_artifacts"].as_i64().unwrap() {
                        return Err("artefact manifest quota exhausted".into());
                    }
                    tx.execute("INSERT INTO artifact_manifests(namespace,identity,digest,reference,readers) VALUES($1,$2,$3,$4,$5)",&[&name,&id,&d,&p["reference"].to_string(),&json!(readers).to_string()]).map_err(db_error)?;
                }
                tx.execute(
                    "UPDATE artifact_objects SET verified=true WHERE namespace=$1 AND digest=$2",
                    &[&name, &d],
                )
                .map_err(db_error)?;
            }
            Ok(
                json!({"reference":p["reference"],"store_id":current["store_id"],"state":if operation=="artifact.complete"{"published"}else{"reserved"}}),
            )
        }
        "artifact.link" => {
            fields(
                p,
                &[
                    "namespace",
                    "principal",
                    "link_id",
                    "references",
                    "state",
                    "preserve_released",
                ],
            )?;
            let principal = identity(p, "principal")?;
            let link = identity(p, "link_id")?;
            let state = text(p, "state")?;
            if !["live", "unresolved", "released"].contains(&state)
                || p["preserve_released"].as_bool().is_none()
            {
                return Err(err());
            }
            let references = p["references"]
                .as_array()
                .filter(|x| !x.is_empty() && x.len() <= 64)
                .ok_or_else(err)?;
            let mut ids = Vec::new();
            for r in references {
                ids.push(text(r, "artifact_id")?.to_owned());
            }
            ids.sort();
            ids.dedup();
            let current=tx.query_opt("SELECT owner,state,refs FROM artifact_links WHERE namespace=$1 AND identity=$2",&[&name,&link]).map_err(db_error)?;
            let existed = current.is_some();
            let mut target = state.to_owned();
            if let Some(row) = current {
                if row.get::<_, String>(0) != principal || value(row.get(2))? != json!(ids) {
                    return Err("artefact retention identity conflict".into());
                }
                if row.get::<_, String>(1) == "released" && p["preserve_released"] == true {
                    target = "released".into();
                }
            } else {
                let count: i64 = tx
                    .query_one(
                        "SELECT count(*) FROM artifact_links WHERE namespace=$1",
                        &[&name],
                    )
                    .map_err(db_error)?
                    .get(0);
                if count >= ns(tx, name)?["policy"]["max_links"].as_i64().unwrap() {
                    return Err("artefact retention quota exhausted".into());
                }
            }
            if target != "released" || !existed {
                if ns(tx, name)?["closed"] == true {
                    return Err("artefact namespace is retired".into());
                }
                for r in references {
                    authorised(tx, name, principal, r)?;
                }
            } else {
                for r in references {
                    let row=tx.query_opt("SELECT reference FROM artifact_manifests WHERE namespace=$1 AND identity=$2",&[&name,&text(r,"artifact_id")?]).map_err(db_error)?.ok_or("artefact unavailable")?;
                    if value(row.get(0))? != *r {
                        return Err("artefact reference conflict".into());
                    }
                }
            }
            tx.execute("INSERT INTO artifact_links(namespace,identity,owner,state,refs) VALUES($1,$2,$3,$4,$5) ON CONFLICT(namespace,identity) DO UPDATE SET state=EXCLUDED.state",&[&name,&link,&principal,&target,&json!(ids).to_string()]).map_err(db_error)?;
            Ok(json!({"link_id":link,"owner":principal,"state":target,"artifacts":ids}))
        }
        "artifact.revoke" => {
            fields(p, &["namespace", "principal", "reference", "reader"])?;
            let principal = identity(p, "principal")?;
            let reader = identity(p, "reader")?;
            authorised(tx, name, principal, &p["reference"])?;
            if p["reference"]["producer"] != principal || reader == principal {
                return Err("artefact grant revocation denied".into());
            }
            let id = text(&p["reference"], "artifact_id")?;
            let row=tx.query_one("SELECT readers,revoked FROM artifact_manifests WHERE namespace=$1 AND identity=$2",&[&name,&id]).map_err(db_error)?;
            let readers = strings(&value(row.get(0))?, 32, crate::identity)?;
            let mut revoked = strings(&value(row.get(1))?, 32, crate::identity)?;
            if !readers.iter().any(|x| x == reader) {
                return Err("artefact reader not granted".into());
            }
            if !revoked.iter().any(|x| x == reader) {
                revoked.push(reader.into());
                revoked.sort();
                tx.execute(
                    "UPDATE artifact_manifests SET revoked=$3 WHERE namespace=$1 AND identity=$2",
                    &[&name, &id, &json!(revoked).to_string()],
                )
                .map_err(db_error)?;
            }
            Ok(json!({"revoked":true}))
        }
        _ => Err("unsupported shared artefact command".into()),
    }
}
pub(crate) fn query(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    let name = namespace(p)?;
    match operation {
        "artifact.namespace" => {
            fields(p, &["namespace"])?;
            ns(tx, name)
        }
        "artifact.inspect" => {
            fields(p, &["namespace", "principal", "reference"])?;
            let principal = identity(p, "principal")?;
            let mut result = authorised(tx, name, principal, &p["reference"])?;
            let id = text(&p["reference"], "artifact_id")?;
            let mut links = Vec::new();
            for row in tx
                .query(
                    "SELECT identity,refs FROM artifact_links WHERE namespace=$1 ORDER BY identity",
                    &[&name],
                )
                .map_err(db_error)?
            {
                if value(row.get(1))?
                    .as_array()
                    .ok_or_else(err)?
                    .contains(&json!(id))
                {
                    links.push(row.get::<_, String>(0));
                }
            }
            result["links"] = json!(links);
            Ok(result)
        }
        "artifact.status" => {
            fields(p, &["namespace", "principal"])?;
            let principal = identity(p, "principal")?;
            let mut refs = Vec::new();
            for row in tx
                .query(
                    "SELECT reference FROM artifact_manifests WHERE namespace=$1 ORDER BY identity",
                    &[&name],
                )
                .map_err(db_error)?
            {
                let r = value(row.get(0))?;
                if authorised(tx, name, principal, &r).is_ok() {
                    refs.push(r)
                }
            }
            let mut links = serde_json::Map::new();
            for row in tx.query("SELECT identity,state,refs FROM artifact_links WHERE namespace=$1 AND owner=$2 ORDER BY identity",&[&name,&principal]).map_err(db_error)?{links.insert(row.get::<_,String>(0),json!({"owner":principal,"state":row.get::<_,String>(1),"artifacts":value(row.get(2))?}));}
            let totals=tx.query_one("SELECT count(*),COALESCE(sum(size),0)::bigint,count(*) FILTER (WHERE NOT verified) FROM artifact_objects WHERE namespace=$1",&[&name]).map_err(db_error)?;
            Ok(
                json!({"namespace":name,"policy":ns(tx,name)?["policy"],"artifacts":refs,"links":links,"content":{"objects":totals.get::<_,i64>(0),"reserved_bytes":totals.get::<_,i64>(1),"unverified":totals.get::<_,i64>(2)}}),
            )
        }
        _ => Err("unsupported shared artefact query".into()),
    }
}
