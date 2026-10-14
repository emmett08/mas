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
pub(crate) const RETENTION_DDL:&str="
ALTER TABLE artifact_links ADD COLUMN retention_target text;
CREATE TABLE artifact_retention_commands(request_key text PRIMARY KEY,digest text NOT NULL,receipt text NOT NULL);
";
fn hash(v:&Value)->String { Sha256::digest(v.to_string().as_bytes()).iter().map(|b|format!("{b:02x}")).collect() }
fn target_owner(tx:&mut Transaction<'_>,target:&Value,allow_archived:bool)->Result<String> {
    fields(target,&["kind","id","principal"])?;
    let id=identity(target,"id")?;let owner=identity(target,"principal")?;
    let row=match text(target,"kind")? {
        "run"=>tx.query_opt("SELECT principal,EXISTS(SELECT 1 FROM host_run_archives WHERE run=$1) FROM host_runs WHERE run=$1",&[&id]),
        "task"=>tx.query_opt("SELECT principal,archived_at IS NOT NULL FROM service_tasks WHERE id=$1",&[&id]),
        "workflow"=>tx.query_opt("SELECT principal,archived_at IS NOT NULL FROM composition_instances WHERE id=$1",&[&id]),
        _=>return Err("artifact_target_invalid".into())
    }.map_err(db_error)?.ok_or("artifact_target_denied")?;
    if row.get::<_,&str>(0)!=owner || !allow_archived && row.get::<_,bool>(1) { return Err("artifact_target_denied".into()); }
    Ok(owner.to_owned())
}
fn actor_owner(tx:&mut Transaction<'_>,namespace:&str,actor:&str,owner:&str)->Result<()> {
    if legacy_owner(tx,namespace,actor,owner)? {Ok(())} else {Err("artifact_target_denied".into())}
}
fn legacy_owner(tx:&mut Transaction<'_>,namespace:&str,actor:&str,owner:&str)->Result<bool> {
    let mut owners=std::collections::BTreeSet::new();
    if let Some(row)=tx.query_opt("SELECT descriptor FROM transfer_configuration WHERE singleton",&[]).map_err(db_error)? {
        let config=value(row.get(0))?;
        if let Some(scopes)=config["scopes"].as_object() {for scope in scopes.values() {
            if scope["namespace"]==namespace {if let Some(principals)=scope["principals"].as_object() {for (principal,mapped) in principals {if mapped==actor {owners.insert(principal.clone());}}}}
        }}
    }
    if owners.is_empty() {owners.insert(actor.to_owned());}
    Ok(owners.len()==1 && owners.contains(owner))
}
fn validate_target(tx:&mut Transaction<'_>,namespace:&str,actor:&str,target:&Value,allow_archived:bool)->Result<()> {
    if target.is_null() {return Ok(())}
    let owner=target_owner(tx,target,allow_archived)?;actor_owner(tx,namespace,actor,&owner)
}
/// Legacy unbound links remain conservative; typed edges block only their parent
/// or a run in that parent's retained execution graph. No opaque ID inference.
pub(crate) fn retained_for(tx:&mut Transaction<'_>,kind:&str,id:&str,runs:&[String])->Result<bool> {
    Ok(tx.query_opt("SELECT 1 FROM artifact_links WHERE state<>'released' AND (retention_target IS NULL OR (retention_target::jsonb->>'kind'=$1 AND retention_target::jsonb->>'id'=$2) OR (retention_target::jsonb->>'kind'='run' AND retention_target::jsonb->>'id'=ANY($3))) LIMIT 1",&[&kind,&id,&runs]).map_err(db_error)?.is_some())
}
fn retention_command(tx:&mut Transaction<'_>,operation:&str,p:&Value)->Result<Value> {
    let name=namespace(p)?;
    let required=match operation {
        "artifact.retire"=>vec!["namespace","operator","request_key","reason"],
        "artifact.release"=>vec!["namespace","operator","request_key","reason","principal","artifact_principal","link_id"],
        "artifact.bind"=>vec!["namespace","operator","request_key","reason","principal","artifact_principal","link_id","retention_target"],
        _=>return Err(err())
    };fields(p,&required)?;
    let operator=identity(p,"operator")?;let key=identity(p,"request_key")?;let reason=text(p,"reason")?;
    if reason.trim().is_empty() || reason.len()>2048 || reason.chars().any(char::is_control) {return Err(err())}
    let digest=hash(&json!({"operation":operation,"request":p}));
    if let Some(row)=tx.query_opt("SELECT digest,receipt FROM artifact_retention_commands WHERE request_key=$1",&[&key]).map_err(db_error)? {
        if row.get::<_,&str>(0)!=digest {return Err("artifact_retention_conflict".into())}return value(row.get(1));
    }
    ns(tx,name)?;
    if operation=="artifact.retire" {
        if tx.query_opt("SELECT 1 FROM artifact_links WHERE namespace=$1 AND state<>'released' LIMIT 1",&[&name]).map_err(db_error)?.is_some() {return Err("artifact_namespace_retained".into())}
        if tx.query_opt("SELECT 1 FROM artifact_transfers t CROSS JOIN transfer_configuration c WHERE c.singleton AND c.descriptor::jsonb->'scopes'->t.scope->>'namespace'=$1 AND t.state='open' AND t.expires_at>floor(extract(epoch FROM clock_timestamp())) LIMIT 1",&[&name]).map_err(db_error)?.is_some() {return Err("artifact_namespace_retained".into())}
        tx.execute("UPDATE artifact_namespaces SET closed=true WHERE namespace=$1",&[&name]).map_err(db_error)?;
    } else {
        let actor=identity(p,"artifact_principal")?;let owner=identity(p,"principal")?;let link=identity(p,"link_id")?;
        actor_owner(tx,name,actor,owner)?;
        let row=tx.query_opt("SELECT owner,retention_target FROM artifact_links WHERE namespace=$1 AND identity=$2",&[&name,&link]).map_err(db_error)?.ok_or("artifact_target_denied")?;
        if row.get::<_,&str>(0)!=actor {return Err("artifact_target_denied".into())}
        if row.get::<_,Option<&str>>(1).is_none() && !legacy_owner(tx,name,actor,owner)? {return Err("artifact_target_denied".into())}
        if operation=="artifact.bind" {
            let target=&p["retention_target"];if target["principal"]!=owner {return Err("artifact_target_denied".into())}
            validate_target(tx,name,actor,target,false)?;
            if target.is_null() {return Err("artifact_target_invalid".into())}
            if let Some(old)=row.get::<_,Option<&str>>(1) {if value(old)?!=*target {return Err("artifact_retention_conflict".into())}}
            tx.execute("UPDATE artifact_links SET retention_target=$3 WHERE namespace=$1 AND identity=$2",&[&name,&link,&target.to_string()]).map_err(db_error)?;
        } else {
            if let Some(target)=row.get::<_,Option<&str>>(1) {if value(target)?["principal"]!=owner {return Err("artifact_target_denied".into())}}
            tx.execute("UPDATE artifact_links SET state='released' WHERE namespace=$1 AND identity=$2",&[&name,&link]).map_err(db_error)?;
        }
    }
    let timestamp:i64=tx.query_one("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",&[]).map_err(db_error)?.get(0);
    let receipt=json!({"format":"mas-artifact-retention/1","operation":operation,"command":p,"operator":operator,"created":timestamp,"content_deleted":false,"allocation_changed":false});
    tx.execute("INSERT INTO artifact_retention_commands VALUES($1,$2,$3)",&[&key,&digest,&receipt.to_string()]).map_err(db_error)?;Ok(receipt)
}
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
    if matches!(operation,"artifact.retire"|"artifact.release"|"artifact.bind") {return retention_command(tx,operation,p);}
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
            let mut lifecycle=p.get("retention_target").cloned().unwrap_or(Value::Null);
            let mut base=p.clone();base.as_object_mut().ok_or_else(err)?.remove("retention_target");
            fields(
                &base,
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
            let current=tx.query_opt("SELECT owner,state,refs,retention_target FROM artifact_links WHERE namespace=$1 AND identity=$2",&[&name,&link]).map_err(db_error)?;
            let existed = current.is_some();
            let mut target = state.to_owned();
            if let Some(row) = current {
                if state=="released" && lifecycle.is_null() { lifecycle=row.get::<_,Option<&str>>(3).map(value).transpose()?.unwrap_or(Value::Null); }
                if row.get::<_, String>(0) != principal || value(row.get(2))? != json!(ids) || row.get::<_,Option<&str>>(3).map(value).transpose()?.unwrap_or(Value::Null)!=lifecycle {
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
            validate_target(tx,name,principal,&lifecycle,target=="released")?;
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
            tx.execute("INSERT INTO artifact_links(namespace,identity,owner,state,refs,retention_target) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(namespace,identity) DO UPDATE SET state=EXCLUDED.state",&[&name,&link,&principal,&target,&json!(ids).to_string(),&if lifecycle.is_null(){None}else{Some(lifecycle.to_string())}]).map_err(db_error)?;
            Ok(json!({"link_id":link,"owner":principal,"state":target,"artifacts":ids,"retention_target":lifecycle}))
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
    if operation=="artifact.namespaces" {
        fields(p,&["offset","limit"])?;
        let offset=p["offset"].as_i64().filter(|n|*n>=0).ok_or_else(err)?;
        let limit=p["limit"].as_i64().filter(|n|(1..=20).contains(n)).ok_or_else(err)?;
        let mut items=vec![];
        for row in tx.query("SELECT namespace,closed FROM artifact_namespaces ORDER BY namespace OFFSET $1 LIMIT $2",&[&offset,&limit]).map_err(db_error)? {
            let name:&str=row.get(0);let counts=tx.query_one("SELECT count(*),count(*) FILTER(WHERE state<>'released'),count(*) FILTER(WHERE retention_target IS NULL AND state<>'released') FROM artifact_links WHERE namespace=$1",&[&name]).map_err(db_error)?;
            items.push(json!({"namespace":name,"closed":row.get::<_,bool>(1),"links":counts.get::<_,i64>(0),"retained_links":counts.get::<_,i64>(1),"unmapped_retained_links":counts.get::<_,i64>(2)}));
        }
        let total:i64=tx.query_one("SELECT count(*) FROM artifact_namespaces",&[]).map_err(db_error)?.get(0);let next=offset+items.len() as i64;
        return Ok(json!({"items":items,"total":total,"next_offset":if next<total{Some(next)}else{None}}));
    }
    if operation=="artifact.links" {
        fields(p,&["namespace","principal","artifact_principal","offset","limit"])?;
        let name=namespace(p)?;let owner=identity(p,"principal")?;let actor=identity(p,"artifact_principal")?;actor_owner(tx,name,actor,owner)?;
        let legacy=legacy_owner(tx,name,actor,owner)?;
        let offset=p["offset"].as_i64().filter(|n|*n>=0).ok_or_else(err)?;let limit=p["limit"].as_i64().filter(|n|(1..=20).contains(n)).ok_or_else(err)?;
        let mut items=vec![];let mut bytes=1024;
        for row in tx.query("SELECT identity,state,refs,retention_target FROM artifact_links WHERE namespace=$1 AND owner=$2 AND (retention_target::jsonb->>'principal'=$5 OR retention_target IS NULL AND $6) ORDER BY identity OFFSET $3 LIMIT $4",&[&name,&actor,&offset,&limit,&owner,&legacy]).map_err(db_error)? {
            let item=json!({"link_id":row.get::<_,&str>(0),"state":row.get::<_,&str>(1),"artifacts":value(row.get(2))?,"retention_target":row.get::<_,Option<&str>>(3).map(value).transpose()?});
            let length=item.to_string().len();if !items.is_empty() && bytes+length>24000 {break}bytes+=length;items.push(item);
        }
        let total:i64=tx.query_one("SELECT count(*) FROM artifact_links WHERE namespace=$1 AND owner=$2 AND (retention_target::jsonb->>'principal'=$3 OR retention_target IS NULL AND $4)",&[&name,&actor,&owner,&legacy]).map_err(db_error)?.get(0);let next=offset+items.len() as i64;
        return Ok(json!({"items":items,"total":total,"next_offset":if next<total{Some(next)}else{None}}));
    }
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
            for row in tx.query("SELECT identity,state,refs,retention_target FROM artifact_links WHERE namespace=$1 AND owner=$2 ORDER BY identity",&[&name,&principal]).map_err(db_error)?{links.insert(row.get::<_,String>(0),json!({"owner":principal,"state":row.get::<_,String>(1),"artifacts":value(row.get(2))?,"retention_target":row.get::<_,Option<&str>>(3).map(value).transpose()?}));}
            let totals=tx.query_one("SELECT count(*),COALESCE(sum(size),0)::bigint,count(*) FILTER (WHERE NOT verified) FROM artifact_objects WHERE namespace=$1",&[&name]).map_err(db_error)?;
            Ok(
                json!({"namespace":name,"closed":ns(tx,name)?["closed"],"policy":ns(tx,name)?["policy"],"artifacts":refs,"links":links,"content":{"objects":totals.get::<_,i64>(0),"reserved_bytes":totals.get::<_,i64>(1),"unverified":totals.get::<_,i64>(2)}}),
            )
        }
        _ => Err("unsupported shared artefact query".into()),
    }
}
