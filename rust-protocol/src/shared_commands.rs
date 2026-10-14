//! Retained command receipts have separate bounded workload/recovery/admin pools.
//! Exact replay never consumes a slot; no receipt is deleted or silently reused.
use crate::postgres_repository::{db_error, layout, PostgresRepository};
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
type Result<T> = std::result::Result<T, String>;
/// Only structural domain fields participate; never search actor payloads for IDs.
pub(crate) fn targets(
    tx: &mut Transaction<'_>,
    operation: &str,
    p: &Value,
    receipt: &Value,
) -> Result<Vec<String>> {
    let mut targets = BTreeSet::new();
    let mut add = |value: &Value| {
        if let Some(id) = value.as_str() {
            targets.insert(id.to_owned());
        }
    };
    if matches!(
        operation,
        "create_run"
            | "claim_run"
            | "release_run"
            | "step"
            | "set_step"
            | "prepare_invocation"
            | "admit_invocation"
            | "observe_invocation"
            | "resolve_invocation"
    ) {
        add(&p["run"]);
    }
    if operation.starts_with("task.") {
        add(&p["task"]);
        add(&receipt["task_id"]);
        add(&receipt["run_id"]);
    }
    if operation.starts_with("composition.") {
        add(&p["instance"]);
        add(&receipt["id"]);
        add(&receipt["instance"]);
        add(&receipt["run_id"]);
    }
    if operation.starts_with("remote.") {
        add(&p["run"]);
        add(&receipt["host_run_dir"]);
    }
    if operation.starts_with("worker.") {
        let call = p["call_id"]
            .as_str()
            .or_else(|| p["receipt"]["call_id"].as_str())
            .or_else(|| receipt["call_id"].as_str())
            .or_else(|| receipt["assignment"]["call_id"].as_str());
        if let Some(call) = call {
            if let Some(row) = tx
                .query_opt("SELECT run_id FROM worker_jobs WHERE call_id=$1", &[&call])
                .map_err(db_error)?
            {
                targets.insert(row.get(0));
            }
        }
    }
    if operation.starts_with("event.") {
        if let Some(subscription) = p["subscription_id"].as_str() {
            if let Some(row) = tx
                .query_opt(
                    "SELECT task_id FROM event_subscriptions WHERE id=$1",
                    &[&subscription],
                )
                .map_err(db_error)?
            {
                targets.insert(row.get(0));
            }
        }
    }
    if operation.starts_with("cursor_job.") {
        if let (Some(principal), Some(job)) = (p["principal"].as_str(), p["job_id"].as_str()) {
            targets.insert(format!("cloud/{principal}/{job}"));
        }
    }
    Ok(targets.into_iter().collect())
}
pub(crate) fn archive(tx: &mut Transaction<'_>, targets: &[String]) -> Result<Value> {
    // Early experimental schemas did not retain structural request targets. Do
    // not guess which active run owns a legacy receipt containing payload data.
    if tx
        .query_opt(
            "SELECT 1 FROM host_commands WHERE operation IS NULL LIMIT 1",
            &[],
        )
        .map_err(db_error)?
        .is_some()
    {
        return Err("legacy_archive_mapping_required".into());
    }
    let mut all = targets.to_vec();
    for row in tx
        .query(
            "SELECT principal,job_id FROM cloud_jobs WHERE host_run=ANY($1)",
            &[&targets],
        )
        .map_err(db_error)?
    {
        all.push(format!(
            "cloud/{}/{}",
            row.get::<_, String>(0),
            row.get::<_, String>(1)
        ));
    }
    let now: i64 = tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    let mut count = 0_u64;
    let mut hash = Sha256::new();
    loop {
        let rows=tx.query("SELECT id,digest,receipt_digest,targets FROM host_commands WHERE targets && $1 AND archived_at IS NULL ORDER BY id LIMIT 1000",&[&all]).map_err(db_error)?;
        if rows.is_empty() {
            break;
        }
        count += rows.len() as u64;
        for row in rows {
            let id: &str = row.get(0);
            let digest: &str = row.get(1);
            let receipt_digest: &str = row.get(2);
            let targets: Vec<String> = row.get(3);
            let tombstone = json!({"format":"mas-archived-command/1","command_id":id,"request_digest":digest,"receipt_digest":receipt_digest,"targets":targets,"archived_at":now,"result_available":false});
            let text = tombstone.to_string();
            hash.update((text.len() as u64).to_be_bytes());
            hash.update(text.as_bytes());
            tx.execute(
                "UPDATE host_commands SET receipt=$2,archived_at=$3 WHERE id=$1",
                &[&id, &text, &now],
            )
            .map_err(db_error)?;
        }
    }
    let digest: String = hash.finalize().iter().map(|b| format!("{b:02x}")).collect();
    Ok(
        json!({"archived_command_receipts":count,"tombstone_digest":digest,"identity_deleted":false}),
    )
}
const DDL:&str="
ALTER TABLE host_commands ADD COLUMN capacity_class text NOT NULL DEFAULT 'normal' CHECK(capacity_class IN ('normal','recovery','administration'));
CREATE TABLE command_capacity(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),initial_policy text NOT NULL,normal_limit bigint NOT NULL CHECK(normal_limit>0),recovery_limit bigint NOT NULL CHECK(recovery_limit>0),administration_limit bigint NOT NULL CHECK(administration_limit>0),normal_used bigint NOT NULL CHECK(normal_used>=0),recovery_used bigint NOT NULL DEFAULT 0 CHECK(recovery_used>=0),administration_used bigint NOT NULL DEFAULT 0 CHECK(administration_used>=0),revision bigint NOT NULL DEFAULT 0 CHECK(revision>=0),CHECK(normal_used<=normal_limit AND recovery_used<=recovery_limit AND administration_used<=administration_limit));
CREATE TABLE command_capacity_amendments(request_key text PRIMARY KEY,digest text NOT NULL,receipt text NOT NULL);
";
fn policy(p: &Value) -> Result<(i64, i64, i64)> {
    if p.as_object().is_none_or(|o| o.len() != 3) {
        return Err("invalid_command_capacity".into());
    }
    let value = |k: &str| {
        p[k].as_i64()
            .filter(|v| *v > 0)
            .ok_or_else(|| "invalid_command_capacity".to_owned())
    };
    Ok((
        value("normal_limit")?,
        value("recovery_limit")?,
        value("administration_limit")?,
    ))
}
pub(crate) fn class(operation: &str) -> &'static str {
    match operation {
        "command_capacity.amend" => "administration",
        "claim_run"
        | "release_run"
        | "set_step"
        | "observe_invocation"
        | "resolve_invocation"
        | "authority.drain"
        | "task.cancel"
        | "task.complete"
        | "task.recover"
        | "task.expire"
        | "task.extend_deadline"
        | "composition.cancel"
        | "composition.release"
        | "composition.finish_node"
        | "composition.save_attempt"
        | "composition.checkpoint"
        | "composition.decide_join"
        | "composition.finish"
        | "composition.recover"
        | "composition.expire"
        | "composition.extend_deadline"
        | "composition.protocol"
        | "remote.complete"
        | "remote.resolve"
        | "cursor_job.complete"
        | "circuit.observe"
        | "peer.revoke"
        | "artifact.complete"
        | "artifact.revoke"
        | "artifact.release"
        | "artifact.bind"
        | "artifact.retire"
        | "transfer.complete"
        | "transfer.close"
        | "worker.receipt"
        | "worker.abandon"
        | "worker.reconcile"
        | "worker.drain"
        | "worker.heartbeat"
        | "operator.drain"
        | "operator.budget_amend"
        | "operator.archive"
        | "epoch.retire"
        | "event.ack"
        | "event.stop"
        | "event.expire"
        | "accounting.record_charge" => "recovery",
        _ => "normal",
    }
}
pub(crate) fn reserve(tx: &mut Transaction<'_>, class: &str) -> Result<()> {
    let sql=match class {
        "normal"=>"UPDATE command_capacity SET normal_used=normal_used+1 WHERE singleton AND normal_used<normal_limit",
        "recovery"=>"UPDATE command_capacity SET recovery_used=recovery_used+1 WHERE singleton AND recovery_used<recovery_limit",
        "administration"=>"UPDATE command_capacity SET administration_used=administration_used+1 WHERE singleton AND administration_used<administration_limit",
        _=>return Err("invalid command capacity class".into())
    };
    if tx.execute(sql, &[]).map_err(db_error)? != 1 {
        return Err(match class {
            "normal" => "command_capacity_exhausted",
            "recovery" => "recovery_capacity_exhausted",
            _ => "administration_capacity_exhausted",
        }
        .into());
    }
    Ok(())
}
pub(crate) fn query(tx: &mut Transaction<'_>, operation: &str, _p: &Value) -> Result<Value> {
    if operation != "command_capacity.status" {
        return Err("invalid_command_capacity".into());
    }
    let r=tx.query_one("SELECT normal_limit,recovery_limit,administration_limit,normal_used,recovery_used,administration_used,revision,initial_policy FROM command_capacity WHERE singleton",&[]).map_err(db_error)?;
    Ok(
        json!({"format":"mas-command-capacity/1","normal":{"limit":r.get::<_,i64>(0),"used":r.get::<_,i64>(3),"remaining":r.get::<_,i64>(0)-r.get::<_,i64>(3)},"recovery":{"limit":r.get::<_,i64>(1),"used":r.get::<_,i64>(4),"remaining":r.get::<_,i64>(1)-r.get::<_,i64>(4)},"administration":{"limit":r.get::<_,i64>(2),"used":r.get::<_,i64>(5),"remaining":r.get::<_,i64>(2)-r.get::<_,i64>(5)},"revision":r.get::<_,i64>(6),"initial_policy":serde_json::from_str::<Value>(r.get::<_,&str>(7)).map_err(|_|"invalid command capacity policy")?,"receipts_pruned":false}),
    )
}
pub(crate) fn apply(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    if operation != "command_capacity.amend" || p.as_object().is_none_or(|o| o.len() != 6) {
        return Err("invalid_command_capacity".into());
    }
    let request = p["request_key"]
        .as_str()
        .filter(|s| crate::identity(s))
        .ok_or("invalid_command_capacity")?;
    p["operator"]
        .as_str()
        .filter(|s| crate::identity(s))
        .ok_or("invalid_command_capacity")?;
    if p["evidence"].as_object().is_none_or(|o| o.is_empty()) {
        return Err("invalid_command_capacity".into());
    }
    let digest: String = Sha256::digest(p.to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    if let Some(r) = tx
        .query_opt(
            "SELECT digest,receipt FROM command_capacity_amendments WHERE request_key=$1",
            &[&request],
        )
        .map_err(db_error)?
    {
        if r.get::<_, String>(0) != digest {
            return Err("command_capacity_conflict".into());
        }
        return serde_json::from_str(r.get::<_, &str>(1))
            .map_err(|_| "invalid capacity amendment receipt".into());
    }
    let normal = p["normal_limit"]
        .as_i64()
        .ok_or("invalid_command_capacity")?;
    let recovery = p["recovery_limit"]
        .as_i64()
        .ok_or("invalid_command_capacity")?;
    let revision = p["expected_revision"]
        .as_i64()
        .ok_or("invalid_command_capacity")?;
    let changed=tx.execute("UPDATE command_capacity SET normal_limit=$1,recovery_limit=$2,revision=revision+1 WHERE singleton AND revision=$3 AND $1>=normal_limit AND $2>=recovery_limit AND ($1>normal_limit OR $2>recovery_limit)",&[&normal,&recovery,&revision]).map_err(db_error)?;
    if changed != 1 {
        return Err("command_capacity_conflict".into());
    }
    let receipt = json!({"request":p,"status":query(tx,"command_capacity.status",&json!({}))?});
    tx.execute(
        "INSERT INTO command_capacity_amendments(request_key,digest,receipt) VALUES($1,$2,$3)",
        &[&request, &digest, &receipt.to_string()],
    )
    .map_err(db_error)?;
    Ok(receipt)
}
impl PostgresRepository {
    pub fn install_command_repository(&mut self, supplied: Option<&Value>) -> Result<()> {
        let default =
            json!({"normal_limit":1_000_000,"recovery_limit":20_000,"administration_limit":1_024});
        let p = supplied.unwrap_or(&default);
        let (normal, recovery, administration) = policy(p)?;
        self.transaction(|tx|{
            let version:i64=tx.query_one("SELECT version FROM authority WHERE singleton",&[]).map_err(db_error)?.get(0);
            if version>=8 {
                if supplied.is_some()&&tx.query_one("SELECT initial_policy FROM command_capacity WHERE singleton",&[]).map_err(db_error)?.get::<_,String>(0)!=p.to_string(){return Err("command_capacity_conflict".into());}return Ok(());
            }
            if version!=7{return Err("shared jobs must be installed before receipt capacity".into());}
            let existing:i64=tx.query_one("SELECT count(*) FROM host_commands",&[]).map_err(db_error)?.get(0);
            if existing>normal{return Err("retained receipts exceed proposed command capacity".into());}
            tx.batch_execute(DDL).map_err(db_error)?;
            tx.execute("INSERT INTO command_capacity(singleton,initial_policy,normal_limit,recovery_limit,administration_limit,normal_used) VALUES(true,$1,$2,$3,$4,$5)",&[&p.to_string(),&normal,&recovery,&administration,&existing]).map_err(db_error)?;
            let digest=layout(tx)?;tx.execute("UPDATE authority SET version=8,layout_digest=$1 WHERE singleton",&[&digest]).map_err(db_error)?;Ok(())
        })
    }
}
