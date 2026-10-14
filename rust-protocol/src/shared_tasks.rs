//! Shared task queue, immutable definitions, request namespaces and receipts.
//! Domain commands run inside the controller-epoch checked authority transaction.
use crate::postgres_repository::db_error;
use postgres::{Row, Transaction};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;
pub const DDL: &str = "
CREATE TABLE service_definitions(kind text NOT NULL,name text NOT NULL,identity text NOT NULL,descriptor text,implementation_digest text,retired boolean NOT NULL DEFAULT false,created bigint NOT NULL,PRIMARY KEY(kind,name,identity));
CREATE TABLE service_aliases(kind text NOT NULL,name text NOT NULL,identity text NOT NULL,PRIMARY KEY(kind,name),FOREIGN KEY(kind,name,identity) REFERENCES service_definitions(kind,name,identity));
CREATE TABLE service_manifest_chunks(digest text NOT NULL,part bigint NOT NULL CHECK(part>=0),data text NOT NULL,PRIMARY KEY(digest,part));
CREATE TABLE service_manifests(digest text PRIMARY KEY,parts bigint NOT NULL CHECK(parts>0));
CREATE TABLE service_requests(principal text NOT NULL,namespace text NOT NULL,epoch text NOT NULL,key text NOT NULL,digest text NOT NULL,target text NOT NULL,kind text NOT NULL,PRIMARY KEY(principal,namespace,epoch,key));
CREATE TABLE service_epochs(principal text NOT NULL,namespace text NOT NULL,epoch text NOT NULL,reason text NOT NULL,authoriser text NOT NULL,created bigint NOT NULL,PRIMARY KEY(principal,namespace,epoch));
CREATE TABLE service_task_policy(singleton boolean PRIMARY KEY CHECK(singleton),max_tasks bigint NOT NULL,max_pending bigint NOT NULL,max_revisions bigint NOT NULL);
CREATE TABLE service_tasks(id text PRIMARY KEY,principal text NOT NULL,namespace text NOT NULL,epoch text NOT NULL,request_key text NOT NULL,workflow text NOT NULL,identity text NOT NULL,state text NOT NULL,input text NOT NULL,metadata text NOT NULL,result text,error text,revision bigint NOT NULL DEFAULT 0,cancel_requested boolean NOT NULL DEFAULT false,created bigint NOT NULL,updated bigint NOT NULL,owner text,owner_epoch bigint,UNIQUE(principal,namespace,epoch,request_key));
CREATE TABLE service_executions(seq bigint GENERATED ALWAYS AS IDENTITY,task text NOT NULL REFERENCES service_tasks(id),revision bigint NOT NULL,run text NOT NULL UNIQUE,input text NOT NULL,state text NOT NULL,PRIMARY KEY(task,revision));
CREATE TABLE service_task_events(seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,task text NOT NULL REFERENCES service_tasks(id),event text NOT NULL,data text NOT NULL,created bigint NOT NULL);
CREATE TABLE service_principal_limits(principal text PRIMARY KEY,max_pending bigint NOT NULL,max_tasks bigint NOT NULL);
CREATE TABLE service_scheduler(singleton boolean PRIMARY KEY CHECK(singleton),policy text NOT NULL,last_principal text NOT NULL DEFAULT '');
CREATE TABLE service_slots(assignment text PRIMARY KEY,principal text NOT NULL,workflow text NOT NULL,identity text NOT NULL,resources text NOT NULL,owner text NOT NULL,epoch bigint NOT NULL);
CREATE INDEX service_task_queue ON service_executions(state,seq);
CREATE INDEX service_task_history ON service_tasks(principal,updated DESC,id DESC);
";
fn text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .filter(|x| !x.is_empty() && x.len() <= 4096 && !x.chars().any(char::is_control))
        .ok_or_else(|| format!("invalid_{k}"))
}
fn optional_text<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    v[k].as_str()
        .filter(|x| x.len() <= 256 && !x.chars().any(char::is_control))
        .ok_or_else(|| format!("invalid_{k}"))
}
fn number(v: &Value, k: &str, min: i64, max: i64) -> Result<i64> {
    v[k].as_i64()
        .filter(|x| *x >= min && *x <= max)
        .ok_or_else(|| format!("invalid_{k}"))
}
fn obj(v: &Value, k: &str) -> Result<String> {
    if !v[k].is_object() {
        return Err(format!("invalid_{k}"));
    }
    Ok(v[k].to_string())
}
fn decode(v: &str) -> Result<Value> {
    serde_json::from_str(v).map_err(|_| "invalid_shared_record".into())
}
fn hash(v: &str) -> String {
    Sha256::digest(v.as_bytes())
        .iter()
        .map(|x| format!("{x:02x}"))
        .collect()
}
fn digest<'a>(v: &'a Value, k: &str) -> Result<&'a str> {
    let s = text(v, k)?;
    if s.len() != 64
        || !s
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err("invalid_definition".into());
    }
    Ok(s)
}
fn timestamp(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0))
}
fn event(tx: &mut Transaction<'_>, task: &str, kind: &str, data: Value) -> Result<()> {
    let t = timestamp(tx)?;
    tx.execute(
        "INSERT INTO service_task_events(task,event,data,created) VALUES($1,$2,$3,$4)",
        &[&task, &kind, &data.to_string(), &t],
    )
    .map_err(db_error)?;
    Ok(())
}
pub fn open_epoch(
    tx: &mut Transaction<'_>,
    principal: &str,
    namespace: &str,
    epoch: &str,
) -> Result<()> {
    if namespace != "legacy" && namespace != "mas-unified/1"
        || namespace == "legacy" && !epoch.is_empty()
    {
        return Err("invalid_request_namespace".into());
    }
    if tx
        .query_opt(
            "SELECT 1 FROM service_epochs WHERE principal=$1 AND namespace=$2 AND epoch=$3",
            &[&principal, &namespace, &epoch],
        )
        .map_err(db_error)?
        .is_some()
    {
        return Err("request_epoch_retired".into());
    }
    Ok(())
}
pub fn request(
    tx: &mut Transaction<'_>,
    principal: &str,
    namespace: &str,
    epoch: &str,
    key: &str,
    digest: &str,
    kind: &str,
) -> Result<Option<String>> {
    open_epoch(tx, principal, namespace, epoch)?;
    tx.query_opt("SELECT digest,target,kind FROM service_requests WHERE principal=$1 AND namespace=$2 AND epoch=$3 AND key=$4",&[&principal,&namespace,&epoch,&key]).map_err(db_error)?.map(|r|{if r.get::<_,String>(0)!=digest||r.get::<_,String>(2)!=kind{Err("request_conflict".into())}else{Ok(r.get(1))}}).transpose()
}
pub fn remember_request(
    tx: &mut Transaction<'_>,
    principal: &str,
    namespace: &str,
    epoch: &str,
    key: &str,
    digest: &str,
    kind: &str,
    target: &str,
) -> Result<()> {
    tx.execute("INSERT INTO service_requests(principal,namespace,epoch,key,digest,kind,target) VALUES($1,$2,$3,$4,$5,$6,$7)",&[&principal,&namespace,&epoch,&key,&digest,&kind,&target]).map_err(db_error)?;
    Ok(())
}
pub(crate) fn check_submission(tx:&mut Transaction<'_>,principal:&str)->Result<()> {
    let global=tx.query_one("SELECT max_tasks,max_pending FROM service_task_policy WHERE singleton",&[]).map_err(db_error)?;
    let policy=tx.query_opt("SELECT max_tasks,max_pending FROM service_principal_limits WHERE principal=$1",&[&principal]).map_err(db_error)?.ok_or("principal_quota_unavailable")?;
    let rows=tx.query_one("SELECT (SELECT count(*) FROM service_tasks)+(SELECT count(*) FROM composition_instances),(SELECT count(*) FROM service_tasks WHERE principal=$1)+(SELECT count(*) FROM composition_instances WHERE principal=$1)",&[&principal]).map_err(db_error)?;
    if rows.get::<_,i64>(0)>=global.get::<_,i64>(0){return Err("task_limit".into());}
    if rows.get::<_,i64>(1)>=policy.get::<_,i64>(0){return Err("principal_task_limit".into());}
    let rows=tx.query_one("WITH pending AS (SELECT principal FROM service_tasks t WHERE state NOT IN ('completed','failed','cancelled') OR EXISTS(SELECT 1 FROM service_executions e WHERE e.task=t.id AND e.state IN ('running','unknown')) UNION ALL SELECT principal FROM composition_instances i WHERE state NOT IN ('completed','failed','cancelled','expired') OR EXISTS(SELECT 1 FROM composition_attempts a WHERE a.instance=i.id AND a.state IN ('running','unknown'))) SELECT count(*),count(*) FILTER(WHERE principal=$1) FROM pending",&[&principal]).map_err(db_error)?;
    if rows.get::<_,i64>(0)>=global.get::<_,i64>(1){return Err("queue_full".into());}
    if rows.get::<_,i64>(1)>=policy.get::<_,i64>(1){return Err("principal_pending_limit".into());}Ok(())
}
pub(crate) fn acquire_slot(tx:&mut Transaction<'_>,assignment:&str,principal:&str,workflow:&str,identity:&str,owner:&str)->Result<bool> {
    let epoch=current_epoch(tx)?;
    if let Some(row)=tx.query_opt("SELECT principal,workflow,identity,owner,epoch FROM service_slots WHERE assignment=$1",&[&assignment]).map_err(db_error)?{
        if row.get::<_,String>(0)!=principal||row.get::<_,String>(1)!=workflow||row.get::<_,String>(2)!=identity{return Err("scheduler_assignment_conflict".into());}
        if row.get::<_,i64>(4)==epoch&&row.get::<_,String>(3)!=owner{return Ok(false);}
        tx.execute("UPDATE service_slots SET owner=$2,epoch=$3 WHERE assignment=$1",&[&assignment,&owner,&epoch]).map_err(db_error)?;return Ok(true);
    }
    let Some(resources)=available_resources(tx,principal,workflow,identity)? else{return Ok(false)};
    tx.execute("INSERT INTO service_slots VALUES($1,$2,$3,$4,$5,$6,$7)",&[&assignment,&principal,&workflow,&identity,&json!(resources).to_string(),&owner,&epoch]).map_err(db_error)?;
    tx.execute("UPDATE service_scheduler SET last_principal=$1 WHERE singleton",&[&principal]).map_err(db_error)?;Ok(true)
}
fn available_resources(tx:&mut Transaction<'_>,principal:&str,workflow:&str,identity:&str)->Result<Option<Vec<Value>>> {
    let row=tx.query_one("SELECT policy FROM service_scheduler WHERE singleton",&[]).map_err(db_error)?;let policy=decode(row.get(0))?;
    let active=tx.query("SELECT principal,workflow,resources FROM service_slots",&[]).map_err(db_error)?;
    if active.len()>=number(&policy,"max_workers",1,256)? as usize||active.iter().filter(|r|r.get::<_,String>(0)==principal).count()>=number(&policy,"max_per_principal",1,256)? as usize||active.iter().filter(|r|r.get::<_,String>(1)==workflow).count()>=policy["workflows"][workflow].as_i64().unwrap_or(number(&policy,"max_workers",1,256)?) as usize{return Ok(None);}
    let def=definition(tx,"workflow",workflow,identity)?;
    if def.is_null()||def["retired"]==true{return Err("definition_unavailable".into());}
    let resources=def["descriptor"]["resource_keys"].as_array().cloned().unwrap_or_default();
    for key in &resources{let name=key.as_str().ok_or("invalid_resource_keys")?;if let Some(cap)=policy["resources"][name].as_i64(){let mut used=0;for row in &active{if decode(row.get(2))?.as_array().is_some_and(|v|v.contains(key)){used+=1;}}if used>=cap{return Ok(None);}}}
    Ok(Some(resources))
}
pub(crate) fn release_slot(tx:&mut Transaction<'_>,assignment:&str,owner:&str)->Result<()> {
    let epoch=current_epoch(tx)?;tx.execute("DELETE FROM service_slots WHERE assignment=$1 AND owner=$2 AND epoch=$3",&[&assignment,&owner,&epoch]).map_err(db_error)?;Ok(())
}
fn task_row(row: Row) -> Result<Value> {
    Ok(
        json!({"task_id":row.get::<_,String>(0),"principal":row.get::<_,String>(1),"request_namespace":row.get::<_,String>(2),"request_epoch":row.get::<_,String>(3),"request_key":row.get::<_,String>(4),"workflow":row.get::<_,String>(5),"workflow_identity":row.get::<_,String>(6),"state":row.get::<_,String>(7),"input":decode(row.get::<_,&str>(8))?,"metadata":decode(row.get::<_,&str>(9))?,"result":row.get::<_,Option<String>>(10).as_deref().map(decode).transpose()?,"error":row.get::<_,Option<String>>(11).as_deref().map(decode).transpose()?,"revision":row.get::<_,i64>(12),"cancel_requested":row.get::<_,bool>(13),"created_at":row.get::<_,i64>(14),"updated_at":row.get::<_,i64>(15)}),
    )
}
const TASK:&str="SELECT id,principal,namespace,epoch,request_key,workflow,identity,state,input,metadata,result,error,revision,cancel_requested,created,updated FROM service_tasks";
fn task(tx: &mut Transaction<'_>, p: &str, t: &str) -> Result<Value> {
    let mut value = task_row(
        tx.query_opt(
            &(TASK.to_string() + " WHERE id=$1 AND principal=$2"),
            &[&t, &p],
        )
        .map_err(db_error)?
        .ok_or("task_not_found")?,
    )?;
    value["clock"] = crate::shared_timing::read(tx,t,"")?;
    value["timing"] = crate::shared_timing::status(tx,t,"")?;
    Ok(value)
}
pub(crate) fn admission_open(tx:&mut Transaction<'_>,run:&str)->Result<()> {
    let row=tx.query_opt("SELECT t.id,t.state,t.cancel_requested,t.owner_epoch,e.revision,t.revision,a.epoch FROM service_executions e JOIN service_tasks t ON t.id=e.task CROSS JOIN authority a WHERE e.run=$1 AND a.singleton",&[&run]).map_err(db_error)?;
    let Some(row)=row else{return Ok(())};
    if row.get::<_,String>(1)!="running"||row.get::<_,bool>(2)||row.get::<_,Option<i64>>(3)!=Some(row.get(6))||row.get::<_,i64>(4)!=row.get::<_,i64>(5){return Err("task_admission_closed".into());}
    crate::shared_timing::require_open(tx,row.get(0),"")
}
fn current_epoch(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one("SELECT epoch FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0))
}
fn own(tx: &mut Transaction<'_>, p: &Value) -> Result<Value> {
    let t = task(tx, text(p, "principal")?, text(p, "task")?)?;
    let r = tx
        .query_one(
            "SELECT owner,owner_epoch FROM service_tasks WHERE id=$1",
            &[&text(p, "task")?],
        )
        .map_err(db_error)?;
    if r.get::<_, Option<String>>(0).as_deref() != Some(text(p, "owner")?)
        || r.get::<_, Option<i64>>(1) != Some(current_epoch(tx)?)
    {
        return Err("stale_task_ownership".into());
    }
    Ok(t)
}
fn definition(tx: &mut Transaction<'_>, kind: &str, name: &str, identity: &str) -> Result<Value> {
    tx.query_opt("SELECT descriptor,implementation_digest,retired,created FROM service_definitions WHERE kind=$1 AND name=$2 AND identity=$3",&[&kind,&name,&identity]).map_err(db_error)?.map(|r|{let d:Option<String>=r.get(0);let imp:Option<String>=r.get(1);let verified=if let Some(ref x)=imp{tx.query_opt("SELECT 1 FROM service_manifests WHERE digest=$1",&[x]).map_err(db_error)?.is_some()}else{false};Ok(json!({"kind":kind,"name":name,"identity":identity,"descriptor":d.as_deref().map(decode).transpose()?,"implementation_digest":imp,"implementation_provenance":if verified{"verified_source_package"}else if imp.is_some(){"deployment_asserted_digest"}else{"not_recorded"},"retired":r.get::<_,bool>(2),"created":r.get::<_,i64>(3)}))}).unwrap_or(Ok(Value::Null))
}
pub fn apply(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    match op {
        "definition.chunk" => {
            let d = digest(p, "digest")?;
            let part = number(p, "part", 0, 65535)?;
            let data = p["data"]
                .as_str()
                .filter(|s| s.len() <= 24000)
                .ok_or("invalid_manifest_chunk")?;
            if let Some(r) = tx
                .query_opt(
                    "SELECT data FROM service_manifest_chunks WHERE digest=$1 AND part=$2",
                    &[&d, &part],
                )
                .map_err(db_error)?
            {
                if r.get::<_, String>(0) != data {
                    return Err("definition_identity_conflict".into());
                }
            } else {
                let used:i64=tx.query_one("SELECT COALESCE(sum(octet_length(data)),0)::bigint FROM service_manifest_chunks WHERE digest=$1",&[&d]).map_err(db_error)?.get(0);
                if used + data.len() as i64 > 16 * 1024 * 1024 {
                    return Err("implementation_package_too_large".into());
                }
                tx.execute(
                    "INSERT INTO service_manifest_chunks VALUES($1,$2,$3)",
                    &[&d, &part, &data],
                )
                .map_err(db_error)?;
            }
            Ok(json!({"digest":d,"part":part}))
        }
        "definition.manifest" => {
            let d = digest(p, "digest")?;
            let n = number(p, "parts", 1, 65536)?;
            let rows = tx
                .query(
                    "SELECT part,data FROM service_manifest_chunks WHERE digest=$1 ORDER BY part",
                    &[&d],
                )
                .map_err(db_error)?;
            if rows.len() != n as usize {
                return Err("implementation_digest_mismatch".into());
            }
            let mut source = String::new();
            for (i, r) in rows.iter().enumerate() {
                if r.get::<_, i64>(0) != i as i64 {
                    return Err("implementation_digest_mismatch".into());
                }
                source.push_str(r.get::<_, &str>(1));
                if source.len() > 16 * 1024 * 1024 {
                    return Err("implementation_package_too_large".into());
                }
            }
            if hash(&source) != d || decode(&source)?["format"] != "mas-python-implementation/1" {
                return Err("implementation_digest_mismatch".into());
            }
            tx.execute(
                "INSERT INTO service_manifests VALUES($1,$2) ON CONFLICT(digest) DO NOTHING",
                &[&d, &n],
            )
            .map_err(db_error)?;
            Ok(json!({"digest":d}))
        }
        "definition.register" => {
            let k = text(p, "kind")?;
            if !["workflow", "plan", "plan_source"].contains(&k) {
                return Err("invalid_definition".into());
            }
            let n = text(p, "name")?;
            let i = digest(p, "identity")?;
            let d = if p["descriptor"].is_null() {
                None
            } else {
                Some(obj(p, "descriptor")?)
            };
            if d.as_ref().is_some_and(|s| hash(s) != i) {
                return Err("definition_identity_conflict".into());
            }
            let imp = p["implementation_digest"].as_str();
            if imp.is_some() && p["descriptor"]["implementation_digest"].as_str() != imp {
                return Err("definition_identity_conflict".into());
            }
            let old = definition(tx, k, n, i)?;
            if !old.is_null() {
                if old["retired"] == true && p["current"] == true {
                    return Err("definition_retired".into());
                }
                if !old["descriptor"].is_null() && old["descriptor"] != p["descriptor"] {
                    return Err("definition_identity_conflict".into());
                }
                tx.execute("UPDATE service_definitions SET descriptor=COALESCE(descriptor,$4),implementation_digest=COALESCE(implementation_digest,$5) WHERE kind=$1 AND name=$2 AND identity=$3",&[&k,&n,&i,&d,&imp]).map_err(db_error)?;
            } else {
                let t = timestamp(tx)?;
                tx.execute("INSERT INTO service_definitions(kind,name,identity,descriptor,implementation_digest,created) VALUES($1,$2,$3,$4,$5,$6)",&[&k,&n,&i,&d,&imp,&t]).map_err(db_error)?;
            }
            if p["current"] == true {
                tx.execute("INSERT INTO service_aliases VALUES($1,$2,$3) ON CONFLICT(kind,name) DO UPDATE SET identity=excluded.identity",&[&k,&n,&i]).map_err(db_error)?;
            }
            definition(tx, k, n, i)
        }
        "definition.retire" => {
            let k = text(p, "kind")?;
            let n = text(p, "name")?;
            let i = digest(p, "identity")?;
            if tx
                .query_opt(
                    "SELECT 1 FROM service_aliases WHERE kind=$1 AND name=$2 AND identity=$3",
                    &[&k, &n, &i],
                )
                .map_err(db_error)?
                .is_some()
            {
                return Err("definition_is_current".into());
            }
            if tx.query_opt("SELECT 1 FROM service_tasks WHERE workflow=$1 AND identity=$2 AND state NOT IN ('completed','failed','cancelled') LIMIT 1",&[&n,&i]).map_err(db_error)?.is_some(){return Err("definition_in_use".into());}
            if crate::shared_composition::definition_in_use(tx, k, n, i)? {
                return Err("definition_in_use".into());
            }
            if tx.execute("UPDATE service_definitions SET retired=true WHERE kind=$1 AND name=$2 AND identity=$3",&[&k,&n,&i]).map_err(db_error)?==0{return Err("definition_not_found".into());}
            definition(tx, k, n, i)
        }
        "task.configure_principal" => {
            let principal=text(p,"principal")?;let pending=number(p,"max_pending",1,i64::MAX)?;let total=number(p,"max_tasks",1,i64::MAX)?;
            if let Some(row)=tx.query_opt("SELECT max_pending,max_tasks FROM service_principal_limits WHERE principal=$1",&[&principal]).map_err(db_error)?{if (row.get::<_,i64>(0),row.get::<_,i64>(1))!=(pending,total){return Err("principal_quota_changed".into());}}
            else{tx.execute("INSERT INTO service_principal_limits VALUES($1,$2,$3)",&[&principal,&pending,&total]).map_err(db_error)?;}Ok(json!({"principal":principal}))
        }
        "task.configure_scheduler" => {
            let policy=&p["policy"];number(policy,"max_workers",1,256)?;number(policy,"max_per_principal",1,256)?;
            for name in ["workflows","resources"]{let limits=policy[name].as_object().ok_or("invalid_scheduler_policy")?;if limits.len()>256||limits.values().any(|n|n.as_i64().is_none_or(|n|n<1||n>256)){return Err("invalid_scheduler_policy".into());}}
            if let Some(row)=tx.query_opt("SELECT policy FROM service_scheduler WHERE singleton",&[]).map_err(db_error)?{if decode(row.get(0))?!=*policy{return Err("scheduler_policy_changed".into());}}
            else{tx.execute("INSERT INTO service_scheduler(singleton,policy) VALUES(true,$1)",&[&policy.to_string()]).map_err(db_error)?;}Ok(policy.clone())
        }
        "task.configure" => {
            let total = number(p, "max_tasks", 1, 10000000)?;
            let pending = number(p, "max_pending", 1, 10000000)?;
            let revisions = number(p, "max_revisions", 1, 1000000)?;
            if let Some(r)=tx.query_opt("SELECT max_tasks,max_pending,max_revisions FROM service_task_policy WHERE singleton",&[]).map_err(db_error)?{if (r.get::<_,i64>(0),r.get::<_,i64>(1),r.get::<_,i64>(2))!=(total,pending,revisions){return Err("service_policy_conflict".into());}}else{tx.execute("INSERT INTO service_task_policy VALUES(true,$1,$2,$3)",&[&total,&pending,&revisions]).map_err(db_error)?;}
            Ok(json!({}))
        }
        "task.submit" => {
            let principal = text(p, "principal")?;
            let ns = text(p, "namespace")?;
            let ep = optional_text(p, "epoch")?;
            let key = text(p, "key")?;
            let dig = digest(p, "digest")?;
            let id = text(p, "task")?;
            let name = text(p, "workflow")?;
            let ident = digest(p, "identity")?;
            if let Some(old) = request(tx, principal, ns, ep, key, dig, "task")? {
                let saved = task(tx, principal, &old)?;
                if p["expected_identity"]
                    .as_str()
                    .is_some_and(|i| saved["workflow_identity"] != i)
                {
                    return Err("workflow_identity_mismatch".into());
                }
                return Ok(saved);
            }
            let def = definition(tx, "workflow", name, ident)?;
            if def.is_null()
                || def["retired"] == true
                || def["implementation_provenance"] != "verified_source_package"
            {
                return Err("definition_unavailable".into());
            }
            if p["expected_identity"].as_str().is_some_and(|i| i != ident) {
                return Err("workflow_identity_mismatch".into());
            }
            check_submission(tx,principal)?;
            let t = timestamp(tx)?;
            tx.execute("INSERT INTO service_tasks(id,principal,namespace,epoch,request_key,workflow,identity,state,input,metadata,created,updated) VALUES($1,$2,$3,$4,$5,$6,$7,'queued',$8,$9,$10,$10)",&[&id,&principal,&ns,&ep,&key,&name,&ident,&obj(p,"input")?,&obj(p,"metadata")?,&t]).map_err(db_error)?;
            crate::shared_timing::start(tx,id,"",&def["descriptor"]["timing_policy"])?;
            let run = format!("run-{}", hash(&format!("{id}:0")));
            tx.execute("INSERT INTO service_executions(task,revision,run,input,state) VALUES($1,0,$2,$3,'queued')",&[&id,&run,&obj(p,"input")?]).map_err(db_error)?;
            remember_request(tx, principal, ns, ep, key, dig, "task", id)?;
            event(tx, id, "submitted", json!({"state":"queued","revision":0}))?;
            task(tx, principal, id)
        }
        "task.claim" => {
            let owner = text(p, "owner")?;
            let requested = p["task"].as_str();
            let epoch = current_epoch(tx)?;
            let rows=tx.query("SELECT t.id,t.principal,t.workflow,t.identity,e.run,t.state,t.updated,EXISTS(SELECT 1 FROM host_runs h WHERE h.run=e.run) FROM service_tasks t JOIN service_executions e ON e.task=t.id AND e.revision=t.revision WHERE e.state IN ('queued','running','blocked') AND (t.owner IS NULL OR t.owner_epoch<>$1) AND ($2::text IS NULL OR t.id=$2) ORDER BY e.seq LIMIT 1000",&[&epoch,&requested]).map_err(db_error)?;
            let last:String=tx.query_one("SELECT last_principal FROM service_scheduler WHERE singleton",&[]).map_err(db_error)?.get(0);
            let mut rows=rows;rows.sort_by_key(|row|(row.get::<_,String>(1)<=last,row.get::<_,String>(1)));
            let mut selected=None;
            for row in rows{if acquire_slot(tx,row.get(4),row.get(1),row.get(2),row.get(3),owner)?{selected=Some(row);break;}}
            let Some(r)=selected else{return Ok(Value::Null)};
            let id:String=r.get(0);let principal:String=r.get(1);
            crate::shared_timing::require_open(tx,&id,"")?;
            crate::shared_timing::transition(tx,&id,"","running",1)?;
            let t = timestamp(tx)?;
            tx.execute("UPDATE service_tasks SET state='running',owner=$2,owner_epoch=$3,updated=$4 WHERE id=$1",&[&id,&owner,&epoch,&t]).map_err(db_error)?;
            tx.execute("UPDATE service_executions SET state='running' WHERE task=$1 AND revision=(SELECT revision FROM service_tasks WHERE id=$1)",&[&id]).map_err(db_error)?;
            event(tx, &id, "running", json!({"state":"running"}))?;
            let mut saved = task(tx, &principal, &id)?;
            let original_state:String=r.get(5);let queued_at:i64=r.get(6);let started:bool=r.get(7);
            let r = tx
                .query_one(
                    "SELECT run,input FROM service_executions WHERE task=$1 AND revision=$2",
                    &[&id, &saved["revision"].as_i64().unwrap()],
                )
                .map_err(db_error)?;
            saved["run_id"] = json!(r.get::<_, String>(0));
            saved["input"] = decode(r.get::<_, &str>(1))?;
            saved["owner"] = json!(owner);
            saved["started"] = json!(started);
            if matches!(original_state.as_str(),"queued"|"blocked") {
                saved["queue_wait_ms"] = json!((t-queued_at).max(0));
            }
            Ok(saved)
        }
        "task.protocol" => {
            let saved=own(tx,p)?;let id=text(p,"task")?;let run=text(p,"run")?;
            if tx.query_opt("SELECT 1 FROM service_executions WHERE task=$1 AND revision=$2 AND run=$3",&[&id,&saved["revision"].as_i64().unwrap(),&run]).map_err(db_error)?.is_none(){return Err("invalid_protocol_event".into());}
            let item=p["event"].as_object().ok_or("invalid_protocol_event")?;
            let mut data=serde_json::Map::new();
            for key in ["sender","recipient","kind","message_id","task_id"]{if let Some(value)=item.get(key){let text=value.as_str().filter(|s|s.len()<=256).ok_or("invalid_protocol_event")?;data.insert(key.into(),json!(text));}}
            let message=data.get("message_id").and_then(Value::as_str).ok_or("invalid_protocol_event")?.to_string();
            if tx.query_opt("SELECT 1 FROM service_task_events WHERE task=$1 AND event='protocol' AND data::jsonb->>'run_id'=$2 AND data::jsonb->>'message_id'=$3 LIMIT 1",&[&id,&run,&message]).map_err(db_error)?.is_none(){
                data.insert("run_id".into(),json!(run));data.insert("revision".into(),saved["revision"].clone());
                event(tx,id,"protocol",Value::Object(data))?;
                let t=timestamp(tx)?;tx.execute("UPDATE service_tasks SET updated=$2 WHERE id=$1",&[&id,&t]).map_err(db_error)?;
            }Ok(json!({"recorded":true}))
        }
        "task.complete" => {
            let saved = own(tx, p)?;
            let id = text(p, "task")?;
            let mut state = text(p, "state")?;
            if ![
                "completed",
                "failed",
                "cancelled",
                "unknown",
                "blocked",
                "input_required",
                "auth_required",
            ]
            .contains(&state)
            {
                return Err("invalid_task_state".into());
            }
            if saved["cancel_requested"] == true
                && ["input_required", "auth_required"].contains(&state)
            {
                state = "cancelled";
            }
            let output = if p["result"].is_null() {
                None
            } else {
                Some(obj(p, "result")?)
            };
            let mut error = if p["error"].is_null() {
                None
            } else {
                Some(obj(p, "error")?)
            };
            let timing = crate::shared_timing::status(tx,id,"")?;
            if state!="unknown" && timing["expired"].as_array().is_some_and(|v| !v.is_empty()) {
                state="failed"; error=Some(json!({"code":timing["expired"][0],"expired":timing["expired"]}).to_string());
            }
            crate::shared_timing::transition(tx,id,"",if ["unknown","blocked","input_required","auth_required"].contains(&state){"paused"}else{"terminal"},0)?;
            let t = timestamp(tx)?;
            tx.execute("UPDATE service_tasks SET state=$2,result=$3,error=$4,updated=$5,owner=NULL,owner_epoch=NULL WHERE id=$1",&[&id,&state,&output,&error,&t]).map_err(db_error)?;
            tx.execute(
                "UPDATE service_executions SET state=$2 WHERE task=$1 AND revision=$3",
                &[&id, &state, &saved["revision"].as_i64().unwrap()],
            )
            .map_err(db_error)?;
            let run:String=tx.query_one("SELECT run FROM service_executions WHERE task=$1 AND revision=$2",&[&id,&saved["revision"].as_i64().unwrap()]).map_err(db_error)?.get(0);
            release_slot(tx,&run,text(p,"owner")?)?;
            event(
                tx,
                id,
                "state_changed",
                json!({"state":state,"revision":saved["revision"]}),
            )?;
            task(tx, text(p, "principal")?, id)
        }
        "task.cancel" => {
            let principal = text(p, "principal")?;
            let id = text(p, "task")?;
            let saved = task(tx, principal, id)?;
            let state = saved["state"].as_str().unwrap();
            let next = if ["queued", "blocked", "input_required", "auth_required"].contains(&state)
            {
                "cancelled"
            } else {
                state
            };
            let t = timestamp(tx)?;
            tx.execute(
                "UPDATE service_tasks SET cancel_requested=true,state=$2,updated=$3 WHERE id=$1",
                &[&id, &next, &t],
            )
            .map_err(db_error)?;
            if next == "cancelled" {
                crate::shared_timing::transition(tx,id,"","terminal",0)?;
                tx.execute(
                    "UPDATE service_executions SET state='cancelled' WHERE task=$1 AND revision=$2",
                    &[&id, &saved["revision"].as_i64().unwrap()],
                )
                .map_err(db_error)?;
            }
            event(tx, id, "cancellation_requested", json!({"state":next}))?;
            task(tx, principal, id)
        }
        "task.update" => {
            let principal = text(p, "principal")?;
            let id = text(p, "task")?;
            let saved = task(tx, principal, id)?;
            let ns = saved["request_namespace"].as_str().unwrap();
            let ep = saved["request_epoch"].as_str().unwrap();
            let key = text(p, "key")?;
            let dig = digest(p, "digest")?;
            if let Some(old) = request(tx, principal, ns, ep, key, dig, "task")? {
                return task(tx, principal, &old);
            }
            if !["input_required", "auth_required"].contains(&saved["state"].as_str().unwrap())
                || saved["cancel_requested"] == true
            {
                return Err("invalid_task_state".into());
            }
            if p["expected_revision"].as_i64() != saved["revision"].as_i64() {
                return Err("request_conflict".into());
            }
            if p["expected_identity"]
                .as_str()
                .is_some_and(|i| saved["workflow_identity"] != i)
            {
                return Err("workflow_identity_mismatch".into());
            }
            crate::shared_timing::require_open(tx,id,"")?;
            crate::shared_timing::transition(tx,id,"","queued",0)?;
            let rev = saved["revision"].as_i64().unwrap() + 1;
            let maximum: i64 = tx
                .query_one(
                    "SELECT max_revisions FROM service_task_policy WHERE singleton",
                    &[],
                )
                .map_err(db_error)?
                .get(0);
            if rev >= maximum {
                return Err("revision_limit".into());
            }
            let run = format!("run-{}", hash(&format!("{id}:{rev}")));
            let input = obj(p, "input")?;
            tx.execute("INSERT INTO service_executions(task,revision,run,input,state) VALUES($1,$2,$3,$4,'queued')",&[&id,&rev,&run,&input]).map_err(db_error)?;
            let t = timestamp(tx)?;
            tx.execute("UPDATE service_tasks SET state='queued',revision=$2,input=$3,result=NULL,error=NULL,updated=$4 WHERE id=$1",&[&id,&rev,&input,&t]).map_err(db_error)?;
            remember_request(tx, principal, ns, ep, key, dig, "task", id)?;
            event(tx, id, "updated", json!({"state":"queued","revision":rev}))?;
            task(tx, principal, id)
        }
        "task.recover" => {
            let principal = text(p, "principal")?;
            let id = text(p, "task")?;
            let saved = task(tx, principal, id)?;
            if saved["state"] != "unknown" {
                return Err("invalid_task_state".into());
            }
            let r = tx
                .query_one(
                    "SELECT run FROM service_executions WHERE task=$1 AND revision=$2",
                    &[&id, &saved["revision"].as_i64().unwrap()],
                )
                .map_err(db_error)?;
            let run: &str = r.get(0);
            if tx.query_opt("SELECT 1 FROM host_invocations WHERE run=$1 AND state IN ('executing','unknown') LIMIT 1",&[&run]).map_err(db_error)?.is_some(){return Err("reconciliation_required".into());}
            let timing=crate::shared_timing::status(tx,id,"")?;
            let expired=timing["expired"].as_array().is_some_and(|v|!v.is_empty());
            let state=if saved["cancel_requested"]==true{"cancelled"}else if expired{"failed"}else{"queued"};
            let error=if expired{Some(json!({"code":timing["expired"][0],"expired":timing["expired"],"effect_reconciliation":"resolved"}).to_string())}else{None};
            crate::shared_timing::transition(tx,id,"",if state=="queued"{"queued"}else{"terminal"},0)?;
            let t = timestamp(tx)?;
            tx.execute("UPDATE service_tasks SET state=$2,error=$3,updated=$4 WHERE id=$1",&[&id,&state,&error,&t]).map_err(db_error)?;
            tx.execute("UPDATE service_executions SET state=$2 WHERE task=$1 AND revision=$3",&[&id,&state,&saved["revision"].as_i64().unwrap()]).map_err(db_error)?;
            event(tx,id,"recovery_queued",json!({"state":state}))?;
            task(tx, principal, id)
        }
        "task.extend_deadline" => {
            let principal=text(p,"principal")?; let id=text(p,"task")?;let saved=task(tx,principal,id)?;
            let command=&p["command"];
            if command["owner"]!=principal||!command["path"].is_null(){return Err("invalid_deadline_amendment".into());}
            if let Some(receipt)=crate::shared_timing::amendment_replay(tx,id,command)?{return Ok(receipt);}
            if saved["cancel_requested"]==true||!matches!(saved["state"].as_str(),Some("queued"|"blocked"|"input_required"|"auth_required")){return Err("deadline_extension_not_available".into());}
            crate::shared_timing::require_open(tx,id,"")?;
            let receipt=crate::shared_timing::amend(tx,id,"",command)?;
            event(tx,id,"deadline_extended",receipt.clone())?;Ok(receipt)
        }
        "task.expire" => {
            let epoch=current_epoch(tx)?;
            let targets=p["tasks"].as_array().filter(|v|v.len()<=1000).ok_or("invalid_expiry_batch")?.iter().map(|x|x.as_str().map(str::to_owned).ok_or("invalid_expiry_batch")).collect::<std::result::Result<Vec<_>,_>>()?;
            let rows=tx.query("SELECT t.id,t.principal,t.revision FROM service_tasks t JOIN service_clocks c ON c.owner=t.id AND c.path='' WHERE t.state IN ('queued','running','blocked','input_required','auth_required') AND (t.owner IS NULL OR t.owner_epoch<>$1) AND t.id=ANY($2)",&[&epoch,&targets]).map_err(db_error)?;
            let mut expired=vec![];
            for row in rows{
                let id:String=row.get(0);let principal:String=row.get(1);let revision:i64=row.get(2);
                let timing=crate::shared_timing::status(tx,&id,"")?;
                if !timing["expired"].as_array().is_some_and(|v|!v.is_empty()){continue;}
                let uncertain=tx.query_opt("SELECT 1 FROM host_invocations h JOIN service_executions e ON e.run=h.run WHERE e.task=$1 AND e.revision=$2 AND h.state IN ('executing','unknown') LIMIT 1",&[&id,&revision]).map_err(db_error)?.is_some();
                let state=if uncertain{"unknown"}else{"failed"};
                let error=json!({"code":if uncertain{json!("reconciliation_required")}else{timing["expired"][0].clone()},"expired":timing["expired"]});
                crate::shared_timing::transition(tx,&id,"",if uncertain{"paused"}else{"terminal"},0)?;
                let t=timestamp(tx)?;tx.execute("UPDATE service_tasks SET state=$2,error=$3,updated=$4,owner=NULL,owner_epoch=NULL WHERE id=$1",&[&id,&state,&error.to_string(),&t]).map_err(db_error)?;
                tx.execute("UPDATE service_executions SET state=$2 WHERE task=$1 AND revision=$3",&[&id,&state,&revision]).map_err(db_error)?;
                tx.execute("DELETE FROM service_slots WHERE assignment IN (SELECT run FROM service_executions WHERE task=$1 AND revision=$2) AND epoch<>$3",&[&id,&revision,&epoch]).map_err(db_error)?;
                event(tx,&id,"deadline_expired",error)?;expired.push(json!({"task_id":id,"principal":principal}));
            }Ok(json!(expired))
        }
        "epoch.retire" => {
            let principal = text(p, "principal")?;
            let ns = text(p, "namespace")?;
            let ep = text(p, "epoch")?;
            if ns != "mas-unified/1" {
                return Err("invalid_request_namespace".into());
            }
            if tx.query_opt("SELECT 1 FROM service_tasks t JOIN service_executions e ON e.task=t.id WHERE t.principal=$1 AND t.namespace=$2 AND t.epoch=$3 AND (t.state NOT IN ('completed','failed','cancelled') OR e.state IN ('running','unknown')) LIMIT 1",&[&principal,&ns,&ep]).map_err(db_error)?.is_some(){return Err("request_epoch_in_use".into());}
            if tx.query_opt("SELECT 1 FROM composition_instances i LEFT JOIN composition_attempts a ON a.instance=i.id WHERE i.principal=$1 AND i.request_namespace=$2 AND i.request_epoch=$3 AND (i.state NOT IN ('completed','failed','cancelled','expired') OR a.state IN ('running','unknown')) LIMIT 1",&[&principal,&ns,&ep]).map_err(db_error)?.is_some(){return Err("request_epoch_in_use".into());}
            let t = timestamp(tx)?;
            let reason = text(p, "reason")?;
            let actor = text(p, "authoriser")?;
            tx.execute("INSERT INTO service_epochs VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(principal,namespace,epoch) DO NOTHING",&[&principal,&ns,&ep,&reason,&actor,&t]).map_err(db_error)?;
            query(tx, "epoch.status", p)
        }
        _ => Err("unknown_shared_task_command".into()),
    }
}
pub fn query(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    match op{
 "task.expiring"=>{
    let epoch=current_epoch(tx)?;let after=p["after"].as_str();let requested=p["task"].as_str();
    let rows=tx.query("SELECT t.id,c.clock FROM service_tasks t JOIN service_clocks c ON c.owner=t.id AND c.path='' WHERE t.state IN ('queued','running','blocked','input_required','auth_required') AND (t.owner IS NULL OR t.owner_epoch<>$1) AND ($2::text IS NULL OR t.id>$2) AND ($3::text IS NULL OR t.id=$3) ORDER BY t.id LIMIT 1001",&[&epoch,&after,&requested]).map_err(db_error)?;
    let now=timestamp(tx)?;let mut expired=vec![];
    for row in rows.iter().take(1000){let status=crate::shared_timing::evaluate(&decode(row.get(1))?,now)?;if status["expired"].as_array().is_some_and(|v|!v.is_empty()){expired.push(row.get::<_,String>(0));}}
    Ok(json!({"tasks":expired,"next":if rows.len()>1000{json!(rows[999].get::<_,String>(0))}else{Value::Null}}))
 },
 "task.claimable"=>{
    let epoch=current_epoch(tx)?;let requested=p["task"].as_str();
    let rows=tx.query("SELECT t.principal,t.workflow,t.identity,s.epoch FROM service_tasks t JOIN service_executions e ON e.task=t.id AND e.revision=t.revision LEFT JOIN service_slots s ON s.assignment=e.run WHERE e.state IN ('queued','running','blocked') AND (t.owner IS NULL OR t.owner_epoch<>$1) AND ($2::text IS NULL OR t.id=$2) ORDER BY e.seq LIMIT 1000",&[&epoch,&requested]).map_err(db_error)?;
    for row in rows{let owned:Option<i64>=row.get(3);if owned.is_some_and(|old|old!=epoch)||owned.is_none()&&available_resources(tx,row.get(0),row.get(1),row.get(2))?.is_some(){return Ok(json!(true));}}
    Ok(json!(false))
 },
 "definition.inspect"=>definition(tx,text(p,"kind")?,text(p,"name")?,digest(p,"identity")?),
 "definition.current"=>Ok(tx.query_opt("SELECT identity FROM service_aliases WHERE kind=$1 AND name=$2",&[&text(p,"kind")?,&text(p,"name")?]).map_err(db_error)?.map(|r|json!(r.get::<_,String>(0))).unwrap_or(Value::Null)),
 "definition.names"=>Ok(json!(tx.query("SELECT DISTINCT name FROM service_definitions WHERE kind=$1 ORDER BY name",&[&text(p,"kind")?]).map_err(db_error)?.iter().map(|r|r.get::<_,String>(0)).collect::<Vec<_>>())),
 "definition.manifest"=>{let d=digest(p,"digest")?;if tx.query_opt("SELECT 1 FROM service_manifests WHERE digest=$1",&[&d]).map_err(db_error)?.is_none(){return Ok(Value::Null);}let mut s=String::new();for r in tx.query("SELECT data FROM service_manifest_chunks WHERE digest=$1 ORDER BY part",&[&d]).map_err(db_error)?{s.push_str(r.get::<_,&str>(0));}decode(&s)},
 "task.policy"=>Ok(tx.query_opt("SELECT max_tasks,max_pending,max_revisions FROM service_task_policy WHERE singleton",&[]).map_err(db_error)?.map(|r|json!({"max_tasks":r.get::<_,i64>(0),"max_pending":r.get::<_,i64>(1),"max_revisions":r.get::<_,i64>(2)})).unwrap_or(Value::Null)),
 "task.principal_policy"=>Ok(tx.query_opt("SELECT max_tasks,max_pending FROM service_principal_limits WHERE principal=$1",&[&text(p,"principal")?]).map_err(db_error)?.map(|r|json!({"max_tasks":r.get::<_,i64>(0),"max_pending":r.get::<_,i64>(1)})).unwrap_or(Value::Null)),
 "task.scheduler"=>{let Some(r)=tx.query_opt("SELECT policy,last_principal FROM service_scheduler WHERE singleton",&[]).map_err(db_error)? else{return Ok(Value::Null)};Ok(json!({"policy":decode(r.get(0))?,"last_principal":r.get::<_,String>(1),"occupied":tx.query_one("SELECT count(*) FROM service_slots",&[]).map_err(db_error)?.get::<_,i64>(0)}))},
 "task.pending"=>{let epoch=current_epoch(tx)?;Ok(json!(tx.query("SELECT t.id,t.principal,t.created FROM service_tasks t JOIN service_executions e ON e.task=t.id AND e.revision=t.revision WHERE e.state IN ('queued','running','blocked') AND (t.owner IS NULL OR t.owner_epoch<>$1) ORDER BY e.seq LIMIT 1000",&[&epoch]).map_err(db_error)?.iter().map(|r|json!({"task_id":r.get::<_,String>(0),"principal":r.get::<_,String>(1),"created_at":r.get::<_,i64>(2)})).collect::<Vec<_>>()))},
 "task.inspect"=>task(tx,text(p,"principal")?,text(p,"task")?),
 "task.request"=>{let principal=text(p,"principal")?;let ns=text(p,"namespace")?;let ep=optional_text(p,"epoch")?;open_epoch(tx,principal,ns,ep)?;Ok(tx.query_opt("SELECT digest,target,kind FROM service_requests WHERE principal=$1 AND namespace=$2 AND epoch=$3 AND key=$4",&[&principal,&ns,&ep,&text(p,"key")?]).map_err(db_error)?.map(|r|json!({"digest":r.get::<_,String>(0),"target":r.get::<_,String>(1),"kind":r.get::<_,String>(2)})).unwrap_or(Value::Null))},
 "task.events"=>{let principal=text(p,"principal")?;let id=text(p,"task")?;task(tx,principal,id)?;let after=number(p,"after",0,i64::MAX)?;let limit=number(p,"limit",1,1000)?;let rows=tx.query("SELECT seq,event,data,created FROM service_task_events WHERE task=$1 AND seq>$2 ORDER BY seq LIMIT $3",&[&id,&after,&(limit+1)]).map_err(db_error)?;let mut out=vec![];for r in rows.iter().take(limit as usize){out.push(json!({"id":r.get::<_,i64>(0),"event":r.get::<_,String>(1),"data":decode(r.get::<_,&str>(2))?,"created_at":r.get::<_,i64>(3)}));}Ok(json!({"has_more":rows.len()>limit as usize,"next_after":out.last().map(|x|x["id"].clone()).unwrap_or(json!(after)),"events":out}))},
 "task.list"=>{let principal=text(p,"principal")?;let limit=number(p,"limit",1,1000)?;let names=p["workflows"].as_array().ok_or("invalid_workflows")?.iter().map(|x|x.as_str().map(str::to_owned).ok_or("invalid_workflows")).collect::<std::result::Result<Vec<String>,_>>()?;let state=p["state"].as_str();let updated=p["updated_after"].as_i64();let after_time=p["after"][0].as_i64();let after_id=p["after"][1].as_str();let metadata=obj(p,"metadata")?;let where_clause=" WHERE principal=$1 AND workflow=ANY($2) AND ($3::text IS NULL OR state=$3) AND ($4::bigint IS NULL OR updated>=$4) AND metadata::jsonb @> $5::text::jsonb";let count:i64=tx.query_one(&("SELECT count(*) FROM service_tasks".to_string()+where_clause),&[&principal,&names,&state,&updated,&metadata]).map_err(db_error)?.get(0);let rows=tx.query(&(TASK.to_string()+where_clause+" AND ($6::bigint IS NULL OR updated<$6 OR (updated=$6 AND id<$7)) ORDER BY updated DESC,id DESC LIMIT $8"),&[&principal,&names,&state,&updated,&metadata,&after_time,&after_id,&(limit+1)]).map_err(db_error)?;let mut more=rows.len()>limit as usize;let mut values:Vec<Value>=vec![];let mut size=1024;for row in rows.into_iter().take(limit as usize){let mut value=task_row(row)?;value.as_object_mut().unwrap().remove("input");let id=value["task_id"].as_str().unwrap().to_owned();value["timing"]=crate::shared_timing::status(tx,&id,"")?;let bytes=value.to_string().len();if size+bytes>512*1024{more=true;break;}size+=bytes;values.push(value);}let next=if more{let v=values.last().unwrap();json!([v["updated_at"],v["task_id"]])}else{Value::Null};Ok(json!({"tasks":values,"total_size":count,"next":next}))},
 "task.executions"=>{let principal=text(p,"principal")?;let id=text(p,"task")?;task(tx,principal,id)?;Ok(json!(tx.query("SELECT revision,run,state FROM service_executions WHERE task=$1 ORDER BY revision",&[&id]).map_err(db_error)?.iter().map(|r|json!({"revision":r.get::<_,i64>(0),"run_id":r.get::<_,String>(1),"state":r.get::<_,String>(2)})).collect::<Vec<_>>()))},
 "epoch.status"=>Ok(tx.query_opt("SELECT reason,authoriser,created FROM service_epochs WHERE principal=$1 AND namespace=$2 AND epoch=$3",&[&text(p,"principal")?,&text(p,"namespace")?,&text(p,"epoch")?]).map_err(db_error)?.map(|r|json!({"state":"retired","reason":r.get::<_,String>(0),"authoriser":r.get::<_,String>(1),"created":r.get::<_,i64>(2)})).unwrap_or(json!({"state":"open"}))),
 _=>Err("unknown_shared_task_query".into())}
}
