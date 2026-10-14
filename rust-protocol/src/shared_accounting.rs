//! Atomic period allocations and separately attributed provider observations.
//! Allocation history is permanent; neither reconciliation nor an observation
//! changes the native lifetime charges. Database time owns period placement.
use crate::postgres_repository::db_error;
use postgres::Transaction;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;

pub(crate) const DDL: &str = "
CREATE TABLE accounting_calls(run text NOT NULL,call_id text NOT NULL,principal text NOT NULL,reserved_microusd bigint NOT NULL CHECK(reserved_microusd>=0),admitted_at bigint CHECK(admitted_at IS NULL OR admitted_at>=0),PRIMARY KEY(run,call_id),FOREIGN KEY(run,call_id) REFERENCES host_invocations(run,call_id));
INSERT INTO accounting_calls(run,call_id,principal,reserved_microusd,admitted_at) SELECT i.run,i.call_id,h.principal,i.reservation_microusd,NULL FROM host_invocations i JOIN host_runs h ON h.run=i.run JOIN calls c ON c.run=i.run AND c.id=i.call_id;
CREATE TABLE accounting_period_policies(scope text PRIMARY KEY,policy text NOT NULL,high_water bigint NOT NULL,request_key text NOT NULL UNIQUE,actor text NOT NULL,reason text NOT NULL,created bigint NOT NULL);
CREATE TABLE accounting_period_allocations(scope text NOT NULL REFERENCES accounting_period_policies(scope),period bigint NOT NULL,run text NOT NULL,call_id text NOT NULL,reserved_microusd bigint NOT NULL CHECK(reserved_microusd>=0),timestamp_basis text NOT NULL CHECK(timestamp_basis IN ('admission_timestamp','legacy_current_period')),PRIMARY KEY(scope,run,call_id),FOREIGN KEY(run,call_id) REFERENCES accounting_calls(run,call_id));
CREATE INDEX accounting_period_usage ON accounting_period_allocations(scope,period);
CREATE TABLE accounting_provider_charges(provider text NOT NULL,charge_id text NOT NULL,run text NOT NULL,call_id text NOT NULL,actual_microusd bigint NOT NULL CHECK(actual_microusd>=0),observed_at bigint NOT NULL CHECK(observed_at>=0),evidence text NOT NULL,actor text NOT NULL,digest text NOT NULL,PRIMARY KEY(provider,charge_id),FOREIGN KEY(run,call_id) REFERENCES accounting_calls(run,call_id));
";

fn fields(value: &Value, required: &[&str]) -> Result<()> {
    let o = value.as_object().ok_or("invalid accounting parameters")?;
    if o.len() != required.len() || required.iter().any(|key| !o.contains_key(*key)) {
        return Err("unsupported accounting parameters".into());
    }
    Ok(())
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value[key]
        .as_str()
        .filter(|s| !s.is_empty() && s.len() <= 4096)
        .ok_or_else(|| format!("invalid {key}"))
}
fn identity<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    let s = text(value, key)?;
    if s.len() > 200
        || !s.as_bytes()[0].is_ascii_alphanumeric()
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_.:/@-".contains(&b))
    {
        return Err(format!("invalid {key}"));
    }
    Ok(s)
}
fn scope(value: &Value) -> Result<&str> {
    let s = text(value, "scope")?;
    if s != "deployment" {
        let principal = s
            .strip_prefix("principal:")
            .ok_or("invalid accounting scope")?;
        identity(&json!({"principal":principal}), "principal")?;
    }
    Ok(s)
}
fn integer(value: &Value, key: &str, min: i64, max: i64) -> Result<i64> {
    value[key]
        .as_i64()
        .filter(|n| *n >= min && *n <= max)
        .ok_or_else(|| format!("invalid {key}"))
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
fn decoded(source: &str) -> Result<Value> {
    serde_json::from_str(source).map_err(|_| "invalid retained accounting state".into())
}
fn digest(value: &Value) -> String {
    Sha256::digest(value.to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn number(value: i128) -> Value {
    serde_json::from_str(&value.to_string()).expect("i128 is integer JSON")
}

struct Policy {
    period: i64,
    calls: i64,
    amount: i64,
    start: i64,
}
impl Policy {
    fn parse(value: &Value) -> Result<Self> {
        fields(
            value,
            &[
                "period_seconds",
                "max_calls",
                "max_reserved_microusd",
                "start_epoch",
            ],
        )?;
        Ok(Self {
            period: integer(value, "period_seconds", 1, 366 * 86400)?,
            calls: integer(value, "max_calls", 1, i64::MAX)?,
            amount: integer(value, "max_reserved_microusd", 0, i64::MAX)?,
            start: integer(value, "start_epoch", 0, i64::MAX)?,
        })
    }
    fn bucket(&self, timestamp: i64) -> i64 {
        ((i128::from(timestamp) - i128::from(self.start)).div_euclid(i128::from(self.period)))
            as i64
    }
}
fn usage(tx: &mut Transaction<'_>, scope: &str, period: i64) -> Result<(i64, i128, i64, i128)> {
    let row=tx.query_one("SELECT count(*)::bigint,coalesce(sum(reserved_microusd),0)::text,count(*) FILTER(WHERE timestamp_basis='legacy_current_period')::bigint,coalesce(sum(reserved_microusd) FILTER(WHERE timestamp_basis='legacy_current_period'),0)::text FROM accounting_period_allocations WHERE scope=$1 AND period=$2",&[&scope,&period]).map_err(db_error)?;
    Ok((
        row.get(0),
        row.get::<_, String>(1)
            .parse()
            .map_err(|_| "accounting sum overflow")?,
        row.get(2),
        row.get::<_, String>(3)
            .parse()
            .map_err(|_| "accounting sum overflow")?,
    ))
}
fn policy_receipt(tx: &mut Transaction<'_>, scope: &str) -> Result<Value> {
    let row=tx.query_one("SELECT policy,high_water,request_key,actor,reason,created FROM accounting_period_policies WHERE scope=$1",&[&scope]).map_err(db_error)?;
    Ok(
        json!({"scope":scope,"policy":decoded(row.get::<_,&str>(0))?,"high_water":row.get::<_,i64>(1),"request_key":row.get::<_,String>(2),"actor":row.get::<_,String>(3),"reason":row.get::<_,String>(4),"created":row.get::<_,i64>(5)}),
    )
}

/// Called only within native admit_invocation's transaction after all lifetime
/// scope checks. A failure rolls back every charge and native reservation.
pub(crate) fn admit(tx: &mut Transaction<'_>, run: &str, call: &str, amount: i64) -> Result<()> {
    let row=tx.query_opt("SELECT h.principal,i.reservation_microusd FROM host_invocations i JOIN host_runs h ON h.run=i.run WHERE i.run=$1 AND i.call_id=$2",&[&run,&call]).map_err(db_error)?.ok_or("accounting invocation missing")?;
    let principal: String = row.get(0);
    if amount < 0 || row.get::<_, i64>(1) != amount {
        return Err("accounting allocation mismatch".into());
    }
    if let Some(prior) = tx
        .query_opt(
            "SELECT principal,reserved_microusd FROM accounting_calls WHERE run=$1 AND call_id=$2",
            &[&run, &call],
        )
        .map_err(db_error)?
    {
        if prior.get::<_, String>(0) != principal || prior.get::<_, i64>(1) != amount {
            return Err("retained accounting allocation conflict".into());
        }
        return Ok(());
    }
    let timestamp = now(tx)?;
    tx.execute(
        "INSERT INTO accounting_calls VALUES($1,$2,$3,$4,$5)",
        &[&run, &call, &principal, &amount, &timestamp],
    )
    .map_err(db_error)?;
    let principal_scope = format!("principal:{principal}");
    for row in tx.query("SELECT scope,policy,high_water FROM accounting_period_policies WHERE scope='deployment' OR scope=$1 ORDER BY scope",&[&principal_scope]).map_err(db_error)?{
        let selected:String=row.get(0);let policy=Policy::parse(&decoded(row.get::<_,&str>(1))?)?;let bucket=policy.bucket(timestamp);
        if bucket<row.get::<_,i64>(2){return Err("allocation clock moved backwards".into())}
        let(calls,used,_,_)=usage(tx,&selected,bucket)?;
        if calls>=policy.calls||i128::from(amount)>i128::from(policy.amount)-used{return Err("allocation period limit exhausted".into())}
        tx.execute("INSERT INTO accounting_period_allocations VALUES($1,$2,$3,$4,$5,'admission_timestamp')",&[&selected,&bucket,&run,&call,&amount]).map_err(db_error)?;
        tx.execute("UPDATE accounting_period_policies SET high_water=$1 WHERE scope=$2",&[&bucket,&selected]).map_err(db_error)?;
    }
    Ok(())
}

pub(crate) fn apply(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    match operation {
        "accounting.configure_period" => {
            fields(p, &["scope", "policy", "request_key", "actor", "reason"])?;
            let selected = scope(p)?;
            let policy = Policy::parse(&p["policy"])?;
            let key = identity(p, "request_key")?;
            let actor = identity(p, "actor")?;
            let reason = text(p, "reason")?;
            if reason.chars().count() > 500 {
                return Err("invalid accounting reason".into());
            }
            let existing=tx.query("SELECT scope,policy,request_key,actor,reason FROM accounting_period_policies WHERE scope=$1 OR request_key=$2",&[&selected,&key]).map_err(db_error)?;
            if !existing.is_empty() {
                let row = &existing[0];
                if existing.len() != 1
                    || row.get::<_, String>(0) != selected
                    || row.get::<_, String>(1) != p["policy"].to_string()
                    || row.get::<_, String>(2) != key
                    || row.get::<_, String>(3) != actor
                    || row.get::<_, String>(4) != reason
                {
                    return Err("allocation period policy conflict".into());
                }
                return policy_receipt(tx, selected);
            }
            if tx
                .query_one(
                    "SELECT count(*)::bigint FROM accounting_period_policies",
                    &[],
                )
                .map_err(db_error)?
                .get::<_, i64>(0)
                >= 10000
            {
                return Err("allocation period policy limit reached".into());
            }
            let timestamp = now(tx)?;
            let current = policy.bucket(timestamp);
            let mut high_water = current;
            tx.execute(
                "INSERT INTO accounting_period_policies VALUES($1,$2,$3,$4,$5,$6,$7)",
                &[
                    &selected,
                    &p["policy"].to_string(),
                    &high_water,
                    &key,
                    &actor,
                    &reason,
                    &timestamp,
                ],
            )
            .map_err(db_error)?;
            let principal = selected.strip_prefix("principal:");
            for row in tx.query("SELECT run,call_id,reserved_microusd,admitted_at FROM accounting_calls WHERE $1::text IS NULL OR principal=$1 ORDER BY run,call_id",&[&principal]).map_err(db_error)?{
            let admitted:Option<i64>=row.get(3);let bucket=admitted.map_or(current,|t|policy.bucket(t));high_water=high_water.max(bucket);
            let basis=if admitted.is_some(){"admission_timestamp"}else{"legacy_current_period"};
            tx.execute("INSERT INTO accounting_period_allocations VALUES($1,$2,$3,$4,$5,$6)",&[&selected,&bucket,&row.get::<_,String>(0),&row.get::<_,String>(1),&row.get::<_,i64>(2),&basis]).map_err(db_error)?;
        }
            tx.execute(
                "UPDATE accounting_period_policies SET high_water=$1 WHERE scope=$2",
                &[&high_water, &selected],
            )
            .map_err(db_error)?;
            policy_receipt(tx, selected)
        }
        "accounting.record_charge" => {
            fields(
                p,
                &[
                    "provider",
                    "charge_id",
                    "run_id",
                    "call_id",
                    "actual_microusd",
                    "observed_at",
                    "evidence",
                    "actor",
                ],
            )?;
            let provider = identity(p, "provider")?;
            let charge = identity(p, "charge_id")?;
            let run = identity(p, "run_id")?;
            let call = identity(p, "call_id")?;
            let evidence = identity(p, "evidence")?;
            let actor = identity(p, "actor")?;
            let amount = integer(p, "actual_microusd", 0, i64::MAX)?;
            let observed = integer(p, "observed_at", 0, now(tx)?)?;
            let fingerprint = digest(p);
            if let Some(row)=tx.query_opt("SELECT digest FROM accounting_provider_charges WHERE provider=$1 AND charge_id=$2",&[&provider,&charge]).map_err(db_error)?{
            if row.get::<_,String>(0)!=fingerprint{return Err("provider charge observation conflict".into())}
        }else{
            if tx.query_opt("SELECT 1 FROM accounting_calls WHERE run=$1 AND call_id=$2",&[&run,&call]).map_err(db_error)?.is_none(){return Err("provider charge requires an admitted invocation".into())}
            if tx.query_one("SELECT count(*)::bigint FROM accounting_provider_charges WHERE run=$1 AND call_id=$2",&[&run,&call]).map_err(db_error)?.get::<_,i64>(0)>=1024{return Err("provider charge observation limit reached".into())}
            tx.execute("INSERT INTO accounting_provider_charges VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",&[&provider,&charge,&run,&call,&amount,&observed,&evidence,&actor,&fingerprint]).map_err(db_error)?;
        }
            let mut receipt = p.clone();
            receipt["allocation_changed"] = json!(false);
            Ok(receipt)
        }
        _ => Err("unknown accounting command".into()),
    }
}

pub(crate) fn query(tx: &mut Transaction<'_>, operation: &str, p: &Value) -> Result<Value> {
    match operation {
        "accounting.periods" => {
            fields(p, &[])?;
            let timestamp = now(tx)?;
            let mut result = Vec::new();
            for row in tx
                .query(
                    "SELECT scope,policy,high_water FROM accounting_period_policies ORDER BY scope",
                    &[],
                )
                .map_err(db_error)?
            {
                let selected: String = row.get(0);
                let policy = Policy::parse(&decoded(row.get::<_, &str>(1))?)?;
                let period = policy.bucket(timestamp).max(row.get(2));
                let (calls, amount, unknown, unknown_amount) = usage(tx, &selected, period)?;
                let mut receipt = policy_receipt(tx, &selected)?;
                receipt["period"] = json!(period);
                receipt["calls"] = json!(calls);
                receipt["reserved_microusd"] = number(amount);
                receipt["legacy_unknown_calls"] = json!(unknown);
                receipt["legacy_unknown_reserved_microusd"] = number(unknown_amount);
                result.push(receipt);
            }
            Ok(json!(result))
        }
        "accounting.reconcile" => {
            fields(p, &["run_id", "call_id"])?;
            let run = identity(p, "run_id")?;
            let call = identity(p, "call_id")?;
            let row=tx.query_opt("SELECT reserved_microusd,admitted_at FROM accounting_calls WHERE run=$1 AND call_id=$2",&[&run,&call]).map_err(db_error)?.ok_or("admitted invocation not found")?;
            let allocation: i64 = row.get(0);
            let admitted: Option<i64> = row.get(1);
            let mut observations = Vec::new();
            let mut total: i128 = 0;
            for observation in tx.query("SELECT provider,charge_id,actual_microusd,observed_at,evidence,actor FROM accounting_provider_charges WHERE run=$1 AND call_id=$2 ORDER BY provider,charge_id",&[&run,&call]).map_err(db_error)?{
            let amount:i64=observation.get(2);total+=i128::from(amount);observations.push(json!({"provider":observation.get::<_,String>(0),"charge_id":observation.get::<_,String>(1),"actual_microusd":amount,"observed_at":observation.get::<_,i64>(3),"evidence":observation.get::<_,String>(4),"actor":observation.get::<_,String>(5)}));
        }
            Ok(
                json!({"run_id":run,"call_id":call,"reserved_microusd":allocation,"admitted_at":admitted,
            "timestamp_basis":if admitted.is_some(){"admission_timestamp"}else{"legacy_timestamp_unknown"},
            "observed_charge_microusd":number(total),"status":if observations.is_empty(){"unobserved"}else if total>i128::from(allocation){"exceeds_reservation"}else{"within_reservation"},
            "observations":observations,"invoice_complete":false,"allocation_changed":false}),
            )
        }
        _ => Err("unknown accounting query".into()),
    }
}
