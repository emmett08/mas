//! Database-clock controller leases and explicit same-authority handover.
//! A restored database must never be activated alongside its source; this module
//! cannot fence another independently writable copy of PostgreSQL.
use crate::postgres_repository::{db_error, layout, AuthorityBinding, PostgresRepository};
use postgres::Transaction;
use serde_json::{json, Value};
type Result<T> = std::result::Result<T, String>;
const DDL: &str = "
CREATE TABLE controller_lease(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),lease_seconds bigint NOT NULL CHECK(lease_seconds BETWEEN 1 AND 3600),lease_until timestamptz NOT NULL,draining boolean NOT NULL DEFAULT false);
INSERT INTO controller_lease(singleton,lease_seconds,lease_until) VALUES(true,60,clock_timestamp()+interval '60 seconds');
CREATE TABLE controller_audit(seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,epoch bigint NOT NULL,operator text NOT NULL,operation text NOT NULL,evidence text NOT NULL,created timestamptz NOT NULL DEFAULT clock_timestamp());
";
pub(crate) fn check_lease(tx: &mut Transaction<'_>) -> Result<()> {
    let live: bool = tx
        .query_one(
            "SELECT lease_until>clock_timestamp() FROM controller_lease WHERE singleton",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    if !live {
        return Err("controller_lease_expired".into());
    }
    Ok(())
}
pub(crate) fn check_admission(tx: &mut Transaction<'_>) -> Result<()> {
    let version: i64 = tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if version >= 3
        && tx
            .query_one("SELECT draining FROM controller_lease WHERE singleton", &[])
            .map_err(db_error)?
            .get::<_, bool>(0)
    {
        return Err("admission_draining".into());
    }
    Ok(())
}
fn snapshot(tx: &mut Transaction<'_>) -> Result<Value> {
    let r=tx.query_one("SELECT a.deployment,a.controller,a.epoch,l.lease_seconds,(extract(epoch FROM l.lease_until)*1000)::bigint,(extract(epoch FROM clock_timestamp())*1000)::bigint,l.draining FROM authority a CROSS JOIN controller_lease l WHERE a.singleton AND l.singleton",&[]).map_err(db_error)?;
    Ok(
        json!({"deployment":r.get::<_,String>(0),"controller":r.get::<_,String>(1),"epoch":r.get::<_,i64>(2),"lease_seconds":r.get::<_,i64>(3),"lease_until_ms":r.get::<_,i64>(4),"database_time_ms":r.get::<_,i64>(5),"draining":r.get::<_,bool>(6)}),
    )
}
pub(crate) fn handed_over(tx: &mut Transaction<'_>, next: &str, epoch: i64) -> Result<()> {
    tx.execute("UPDATE controller_lease SET lease_until=clock_timestamp()+lease_seconds*interval '1 second' WHERE singleton",&[]).map_err(db_error)?;
    tx.execute("INSERT INTO controller_audit(epoch,operator,operation,evidence) VALUES($1,$2,'handover',$3)",&[&epoch,&next,&json!({"controller":next}).to_string()]).map_err(db_error)?;
    Ok(())
}
impl PostgresRepository {
    pub fn install_controller_repository(&mut self) -> Result<()> {
        self.transaction(|tx| {
            let version: i64 = tx
                .query_one("SELECT version FROM authority WHERE singleton", &[])
                .map_err(db_error)?
                .get(0);
            if version >= 3 {
                return Ok(());
            }
            if version != 2 {
                return Err("host repository must be installed before controller leases".into());
            }
            tx.batch_execute(DDL).map_err(db_error)?;
            let digest = layout(tx)?;
            tx.execute(
                "UPDATE authority SET version=3,layout_digest=$1 WHERE singleton",
                &[&digest],
            )
            .map_err(db_error)?;
            Ok(())
        })
    }
    pub fn authority_status(&self) -> Result<Value> {
        self.transaction(snapshot)
    }
    pub fn renew_authority(&self, lease_seconds: i64) -> Result<Value> {
        if !(1..=3600).contains(&lease_seconds) {
            return Err("invalid controller lease interval".into());
        }
        self.transaction(|tx|{
            tx.execute("UPDATE controller_lease SET lease_seconds=$1,lease_until=clock_timestamp()+$1::bigint*interval '1 second' WHERE singleton",&[&lease_seconds]).map_err(db_error)?;
            snapshot(tx)
        })
    }
    /// CAS acquisition after the database clock says the old lease expired.
    /// The repository binding supplies the exact prior deployment/controller/epoch.
    /// It never retries, changes deployment identity, or opens a restored copy.
    pub fn takeover(&mut self, next: &str, lease_seconds: i64) -> Result<AuthorityBinding> {
        if !crate::identity(next)
            || next == self.binding.controller
            || !(1..=3600).contains(&lease_seconds)
        {
            return Err("invalid controller takeover".into());
        }
        let mut client = self
            .client
            .try_borrow_mut()
            .map_err(|_| "repository transaction already active")?;
        let mut tx = client.transaction().map_err(db_error)?;
        let r=tx.query_one("SELECT deployment,controller,epoch,version FROM authority WHERE singleton FOR UPDATE",&[]).map_err(db_error)?;
        if r.get::<_, String>(0) != self.binding.deployment
            || r.get::<_, String>(1) != self.binding.controller
            || r.get::<_, i64>(2) != self.binding.epoch
            || r.get::<_, i64>(3) < 3
        {
            return Err("stale protocol authority".into());
        }
        let expired: bool = tx
            .query_one(
                "SELECT lease_until<=clock_timestamp() FROM controller_lease WHERE singleton",
                &[],
            )
            .map_err(db_error)?
            .get(0);
        if !expired {
            return Err("controller lease remains active".into());
        }
        let epoch = self
            .binding
            .epoch
            .checked_add(1)
            .ok_or("authority epoch exhausted")?;
        tx.execute(
            "UPDATE authority SET controller=$1,epoch=$2 WHERE singleton",
            &[&next, &epoch],
        )
        .map_err(db_error)?;
        tx.execute("UPDATE controller_lease SET lease_seconds=$1,lease_until=clock_timestamp()+$1::bigint*interval '1 second' WHERE singleton",&[&lease_seconds]).map_err(db_error)?;
        tx.execute("INSERT INTO controller_audit(epoch,operator,operation,evidence) VALUES($1,$2,'takeover',$3)",&[&epoch,&next,&json!({"previous_epoch":self.binding.epoch}).to_string()]).map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(AuthorityBinding {
            deployment: self.binding.deployment.clone(),
            controller: next.into(),
            epoch,
        })
    }
}
pub(crate) fn apply(tx: &mut Transaction<'_>, op: &str, p: &Value) -> Result<Value> {
    match op {
        "authority.drain" => {
            let operator = p["operator"]
                .as_str()
                .filter(|v| crate::identity(v))
                .ok_or("invalid operator")?;
            let draining = p["draining"].as_bool().ok_or("invalid drain policy")?;
            if !p["evidence"].as_object().is_some_and(|v| !v.is_empty()) {
                return Err("operator evidence required".into());
            }
            tx.execute(
                "UPDATE controller_lease SET draining=$1 WHERE singleton",
                &[&draining],
            )
            .map_err(db_error)?;
            tx.execute("INSERT INTO controller_audit(epoch,operator,operation,evidence) SELECT epoch,$1,$2,$3 FROM authority WHERE singleton",&[&operator,&op,&p["evidence"].to_string()]).map_err(db_error)?;
            snapshot(tx)
        }
        _ => Err("unsupported authority command".into()),
    }
}
pub(crate) fn query(tx: &mut Transaction<'_>, op: &str, _p: &Value) -> Result<Value> {
    match op {
        "authority.status" => snapshot(tx),
        "authority.audit" => {
            let mut out = Vec::new();
            for r in tx.query("SELECT seq,epoch,operator,operation,evidence,(extract(epoch FROM created)*1000)::bigint FROM controller_audit ORDER BY seq",&[]).map_err(db_error)?{out.push(json!({"seq":r.get::<_,i64>(0),"epoch":r.get::<_,i64>(1),"operator":r.get::<_,String>(2),"operation":r.get::<_,String>(3),"evidence":serde_json::from_str::<Value>(r.get::<_,&str>(4)).map_err(|_|"invalid authority audit")?,"created_ms":r.get::<_,i64>(5)}));}
            Ok(json!(out))
        }
        _ => Err("unsupported authority query".into()),
    }
}
