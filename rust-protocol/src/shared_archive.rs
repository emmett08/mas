//! Authorised terminal-run payload compaction. Identities and charges survive.
use crate::postgres_repository::{db_error, layout, PostgresRepository};
use postgres::{fallible_iterator::FallibleIterator, Transaction};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;
const DDL: &str = "
ALTER TABLE host_commands ADD COLUMN operation text;
ALTER TABLE host_commands ADD COLUMN targets text[] NOT NULL DEFAULT '{}';
ALTER TABLE host_commands ADD COLUMN receipt_digest text;
ALTER TABLE host_commands ADD COLUMN archived_at bigint;
CREATE INDEX host_command_targets ON host_commands USING gin(targets);
CREATE TABLE host_run_archives(run text PRIMARY KEY REFERENCES host_runs(run),archived_at bigint NOT NULL,receipt text NOT NULL);
";
pub(crate) fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
pub(crate) fn require_open(tx: &mut Transaction<'_>, run: &str) -> Result<()> {
    let version: i64 = tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if version >= 10
        && tx
            .query_opt("SELECT 1 FROM host_run_archives WHERE run=$1", &[&run])
            .map_err(db_error)?
            .is_some()
    {
        return Err("run_archived".into());
    }
    Ok(())
}
pub(crate) fn archive_run(tx: &mut Transaction<'_>, run: &str) -> Result<Value> {
    if let Some(row) = tx
        .query_opt(
            "SELECT receipt FROM host_run_archives WHERE run=$1",
            &[&run],
        )
        .map_err(db_error)?
    {
        return serde_json::from_str(row.get(0)).map_err(|_| "invalid archived run receipt".into());
    }
    if !crate::shared_control::settled_run(tx, run)? {
        return Err("run_not_archivable".into());
    }
    let now: i64 = tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    let mut evidence = Vec::new();
    // Hash complete ordered records before replacing their payload columns.
    // Identifiers, original states, protocol relations and counters stay in place.
    for (domain,sql) in [
        ("steps","SELECT row_to_json(x)::text FROM (SELECT key,kind,input,data,revision FROM host_steps WHERE run=$1 ORDER BY seq) x"),
        ("invocations","SELECT row_to_json(x)::text FROM (SELECT call_id,step_key,actor,state,output,evidence,reservation_microusd FROM host_invocations WHERE run=$1 ORDER BY seq) x"),
        ("resolutions","SELECT row_to_json(x)::text FROM (SELECT seq,call_id,principal,decision,evidence,created FROM host_resolutions WHERE run=$1 ORDER BY seq) x"),
        ("messages","SELECT row_to_json(x)::text FROM (SELECT id,sender,recipient,content,received,acked FROM messages WHERE run=$1 ORDER BY seq) x"),
        ("calls","SELECT row_to_json(x)::text FROM (SELECT id,actor,state,result FROM calls WHERE run=$1 ORDER BY seq) x")
    ] {
        let mut hash=Sha256::new();let mut count=0_i64;
        let mut rows=tx.query_raw(sql,[&run]).map_err(db_error)?;
        while let Some(row)=rows.next().map_err(db_error)? {let value:&str=row.get(0);hash.update((value.len() as u64).to_be_bytes());hash.update(value.as_bytes());count+=1;}
        let digest:String=hash.finalize().iter().map(|b|format!("{b:02x}")).collect();
        evidence.push(json!({"domain":domain,"rows":count,"sha256":digest}));
    }
    let worker = crate::shared_control::archive_worker_run(tx, run)?;
    let remote = crate::shared_remote::archive_run(tx, run)?;
    let cloud = crate::shared_jobs::archive_run(tx, run)?;
    let receipt = json!({"workers":worker,"remote":remote,"cloud_jobs":cloud,"format":"mas-archived-run/1","run_id":run,"archived_at":now,"payload_evidence":evidence,"result_available":false,"allocation_changed":false});
    tx.execute(
        "INSERT INTO host_run_archives(run,archived_at,receipt) VALUES($1,$2,$3)",
        &[&run, &now, &receipt.to_string()],
    )
    .map_err(db_error)?;
    tx.execute(
        "UPDATE host_runs SET owner=NULL,owner_epoch=NULL WHERE run=$1",
        &[&run],
    )
    .map_err(db_error)?;
    tx.execute(
        "UPDATE host_steps SET input='{}',data='{}' WHERE run=$1",
        &[&run],
    )
    .map_err(db_error)?;
    tx.execute(
        "UPDATE host_invocations SET output=NULL,evidence=NULL WHERE run=$1",
        &[&run],
    )
    .map_err(db_error)?;
    tx.execute(
        "UPDATE host_resolutions SET evidence='{}' WHERE run=$1",
        &[&run],
    )
    .map_err(db_error)?;
    tx.execute("UPDATE messages SET content='{}' WHERE run=$1", &[&run])
        .map_err(db_error)?;
    tx.execute("UPDATE calls SET result=NULL WHERE run=$1", &[&run])
        .map_err(db_error)?;
    Ok(receipt)
}
pub(crate) fn inspect(tx: &mut Transaction<'_>, run: &str) -> Result<Value> {
    tx.query_opt(
        "SELECT receipt FROM host_run_archives WHERE run=$1",
        &[&run],
    )
    .map_err(db_error)?
    .map(|r| serde_json::from_str(r.get(0)).map_err(|_| "invalid archived run receipt".into()))
    .unwrap_or(Ok(Value::Null))
}
impl PostgresRepository {
    pub fn install_archive_repository(&mut self) -> Result<()> {
        self.transaction(|tx| {
            let version: i64 = tx
                .query_one("SELECT version FROM authority WHERE singleton", &[])
                .map_err(db_error)?
                .get(0);
            if version >= 10 {
                return Ok(());
            }
            if version != 9 {
                return Err("retention must be installed before native archive".into());
            }
            tx.batch_execute(DDL).map_err(db_error)?;
            tx.batch_execute(crate::shared_remote::ARCHIVE_DDL)
                .map_err(db_error)?;
            tx.batch_execute(crate::shared_jobs::ARCHIVE_DDL)
                .map_err(db_error)?;
            tx.batch_execute(crate::shared_control::WORKER_ARCHIVE_DDL)
                .map_err(db_error)?;
            let digest = layout(tx)?;
            tx.execute(
                "UPDATE authority SET version=10,layout_digest=$1 WHERE singleton",
                &[&digest],
            )
            .map_err(db_error)?;
            Ok(())
        })
    }
}
