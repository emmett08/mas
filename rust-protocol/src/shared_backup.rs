//! Sealed-source recovery. A normal/old snapshot is audit-only. Activation needs
//! a final source seal, exact full-state digest and an externally signed fencing
//! proof tied to the target database and verified shared-object inventory.
use crate::postgres_repository::{db_error, layout, AuthorityBinding, PostgresRepository};
use hmac::{Hmac, KeyInit, Mac};
use postgres::{fallible_iterator::FallibleIterator, Client, IsolationLevel, Transaction};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
type Result<T> = std::result::Result<T, String>;
const DDL:&str="
CREATE TABLE recovery_guard(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),mode text NOT NULL CHECK(mode IN ('active','sealed','audit')),key_fingerprint text,seal text,restore text);
INSERT INTO recovery_guard(singleton,mode) VALUES(true,'active');
CREATE TABLE recovery_events(seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,operation text NOT NULL,evidence text NOT NULL,created timestamptz NOT NULL DEFAULT clock_timestamp());
";
fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn decode(s: &str) -> Result<Value> {
    serde_json::from_str(s).map_err(|_| "invalid recovery metadata".into())
}
fn text<'a>(p: &'a Value, k: &str) -> Result<&'a str> {
    p[k].as_str()
        .filter(|v| !v.is_empty())
        .ok_or_else(|| format!("invalid recovery {k}"))
}
fn identity<'a>(p: &'a Value, k: &str) -> Result<&'a str> {
    let v = text(p, k)?;
    if !crate::identity(v) {
        return Err("invalid recovery identity".into());
    }
    Ok(v)
}
fn key() -> Result<Vec<u8>> {
    let s = std::env::var("MAS_RECOVERY_KEY_HEX").map_err(|_| "external recovery key required")?;
    if s.len() < 64
        || s.len() > 1024
        || s.len() % 2 != 0
        || !s.bytes().all(|b| b.is_ascii_hexdigit())
    {
        return Err("invalid external recovery key".into());
    }
    (0..s.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&s[i..i + 2], 16).map_err(|_| "invalid external recovery key".into())
        })
        .collect()
}
fn sign(p: &Value, key: &[u8]) -> Result<String> {
    let mut mac = <Hmac<Sha256> as KeyInit>::new_from_slice(key)
        .map_err(|_| "invalid recovery signing key")?;
    mac.update(p.to_string().as_bytes());
    Ok(mac
        .finalize()
        .into_bytes()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}
fn verify(p: &Value, signature: &str, key: &[u8]) -> Result<()> {
    use subtle::ConstantTimeEq;
    let expected = sign(p, key)?;
    if expected.as_bytes().ct_eq(signature.as_bytes()).unwrap_u8() != 1 {
        return Err("invalid external recovery signature".into());
    }
    Ok(())
}
fn database(tx: &mut Transaction<'_>) -> Result<Value> {
    let r=tx.query_one("SELECT (pg_control_system()).system_identifier::text,current_database(),(SELECT oid::bigint FROM pg_database WHERE datname=current_database())",&[]).map_err(db_error)?;
    Ok(
        json!({"system_id":r.get::<_,String>(0),"database":r.get::<_,String>(1),"database_oid":r.get::<_,i64>(2)}),
    )
}
fn now(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .map_err(db_error)?
        .get(0))
}
fn schema_version(tx: &mut Transaction<'_>) -> Result<i64> {
    Ok(tx
        .query_one("SELECT version FROM authority WHERE singleton", &[])
        .map_err(db_error)?
        .get(0))
}
pub(crate) fn check_active(tx: &mut Transaction<'_>) -> Result<()> {
    let mode: String = tx
        .query_one("SELECT mode FROM recovery_guard WHERE singleton", &[])
        .map_err(db_error)?
        .get(0);
    if mode != "active" {
        return Err("recovery audit or sealed authority forbids service access".into());
    }
    Ok(())
}
fn check_binding(tx: &mut Transaction<'_>, binding: &AuthorityBinding, lock: bool) -> Result<()> {
    let query = if lock {
        "SELECT deployment,controller,epoch,version,layout_digest FROM authority WHERE singleton FOR UPDATE"
    } else {
        "SELECT deployment,controller,epoch,version,layout_digest FROM authority WHERE singleton"
    };
    let r = tx.query_one(query, &[]).map_err(db_error)?;
    if r.get::<_, String>(0) != binding.deployment
        || r.get::<_, String>(1) != binding.controller
        || r.get::<_, i64>(2) != binding.epoch
        || !matches!(r.get::<_, i64>(3), 6..=12)
        || r.get::<_, String>(4) != layout(tx)?
    {
        return Err("incompatible recovery authority".into());
    }
    Ok(())
}
fn inventory(tx: &mut Transaction<'_>) -> Result<Value> {
    let mut tables = Vec::new();
    for row in tx.query("SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relkind IN ('r','p') AND c.relname<>'recovery_guard' ORDER BY c.relname",&[]).map_err(db_error)?{
        let name:String=row.get(0);let quoted=format!("\"{}\"",name.replace('"',"\"\""));let sql=format!("SELECT row_to_json(t)::text FROM {quoted} t ORDER BY row_to_json(t)::text COLLATE \"C\"");
        let mut digest=Sha256::new();let mut count=0i64;
        let mut rows=tx.query_raw(&sql,std::iter::empty::<&str>()).map_err(db_error)?;
        while let Some(r)=rows.next().map_err(db_error)?{let value:String=r.get(0);digest.update((value.len()as u64).to_be_bytes());digest.update(value.as_bytes());count=count.checked_add(1).ok_or("recovery row count exhausted")?;}
        let digest:String=digest.finalize().iter().map(|b|format!("{b:02x}")).collect();tables.push(json!({"table":name,"rows":count,"digest":digest}));
    }
    let mut sequences = Vec::new();
    for row in tx.query("SELECT sequencename,last_value FROM pg_sequences WHERE schemaname=current_schema() ORDER BY sequencename",&[]).map_err(db_error)?{sequences.push(json!([row.get::<_,String>(0),row.get::<_,Option<i64>>(1)]));}
    let mut objects = Sha256::new();
    let mut object_count = 0i64;
    for row in tx.query("SELECT namespace,digest,size FROM artifact_objects WHERE verified ORDER BY namespace,digest",&[]).map_err(db_error)?{
        let record=json!({"namespace":row.get::<_,String>(0),"digest":row.get::<_,String>(1),"size":row.get::<_,i64>(2)}).to_string();objects.update((record.len()as u64).to_be_bytes());objects.update(record.as_bytes());object_count+=1;
    }
    let object_digest: String = objects
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    Ok(
        json!({"state_digest":hash(json!({"tables":tables,"sequences":sequences}).to_string().as_bytes()),"tables":tables,"sequences":sequences,"object_digest":object_digest,"object_count":object_count}),
    )
}
/// Only an empty isolated database is a permissible archive target. This never
/// creates, drops, cleans or overwrites a database/schema.
pub fn target(client: &mut Client) -> Result<Value> {
    let mut tx = client
        .build_transaction()
        .read_only(true)
        .start()
        .map_err(db_error)?;
    let count:i64=tx.query_one("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname!~'^pg_' AND n.nspname<>'information_schema' AND c.relkind IN ('r','p','v','m','f','S')",&[]).map_err(db_error)?.get(0);
    if count != 0 {
        return Err("restore target must be an empty isolated database".into());
    }
    let result = database(&mut tx)?;
    tx.commit().map_err(db_error)?;
    Ok(result)
}
impl PostgresRepository {
    pub fn install_recovery_repository(&mut self) -> Result<()> {
        self.transaction(|tx| {
            let version = schema_version(tx)?;
            if version >= 6 {
                return Ok(());
            }
            if version != 5 {
                return Err("shared accounting must be installed before recovery".into());
            }
            tx.batch_execute(DDL).map_err(db_error)?;
            let digest = layout(tx)?;
            tx.execute(
                "UPDATE authority SET version=6,layout_digest=$1 WHERE singleton",
                &[&digest],
            )
            .map_err(db_error)?;
            Ok(())
        })
    }
    /// Permanently fence the source before issuing a restorable final snapshot.
    /// Losing the reply is safe: the exact seal key returns the retained seal.
    pub fn recovery_seal(&mut self, p: &Value) -> Result<Value> {
        let operator = identity(p, "operator")?;
        let request = identity(p, "request_key")?;
        let key_reference = identity(p, "key_reference")?;
        if p["evidence"].as_object().is_none_or(|v| v.is_empty()) {
            return Err("source sealing evidence required".into());
        }
        let key = key()?;
        let fingerprint = hash(&key);
        let mut client = self
            .client
            .try_borrow_mut()
            .map_err(|_| "recovery transaction busy")?;
        let mut tx = client.transaction().map_err(db_error)?;
        check_binding(&mut tx, &self.binding, true)?;
        let prior = tx
            .query_one(
                "SELECT mode,key_fingerprint,seal FROM recovery_guard WHERE singleton",
                &[],
            )
            .map_err(db_error)?;
        if let Some(previous) = prior.get::<_, Option<String>>(1) {
            if previous != fingerprint {
                return Err("recovery signing key differs".into());
            }
        }
        if prior.get::<_, String>(0) == "sealed" {
            let seal = decode(prior.get::<_, &str>(2))?;
            if seal["payload"]["request"] != *p {
                return Err("source seal identity conflict".into());
            }
            return Ok(seal);
        }
        if prior.get::<_, String>(0) != "active" {
            return Err("an audit clone cannot seal itself as an original source".into());
        }
        crate::shared_leadership::check_lease(&mut tx)?;
        tx.execute(
            "UPDATE controller_lease SET draining=true WHERE singleton",
            &[],
        )
        .map_err(db_error)?;
        tx.execute(
            "INSERT INTO recovery_events(operation,evidence) VALUES('source_sealed',$1)",
            &[&p.to_string()],
        )
        .map_err(db_error)?;
        let state = inventory(&mut tx)?;
        let payload = json!({"format":"mas-shared-seal/1","request":p,"source":database(&mut tx)?,"namespace":tx.query_one("SELECT current_schema()",&[]).map_err(db_error)?.get::<_,String>(0),"deployment":self.binding.deployment,"controller":self.binding.controller,"epoch":self.binding.epoch,"schema_version":schema_version(&mut tx)?,"layout_digest":layout(&mut tx)?,"state":state,"created_ms":now(&mut tx)?,"operator":operator,"request_key":request,"key_reference":key_reference});
        let seal = json!({"payload":payload,"signature":sign(&payload,&key)?});
        tx.execute(
            "UPDATE recovery_guard SET mode='sealed',key_fingerprint=$1,seal=$2 WHERE singleton",
            &[&fingerprint, &seal.to_string()],
        )
        .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(seal)
    }
    /// Read-only inspection stays available for sealed/audit databases even
    /// after the controller lease expires. It cannot reconcile or mutate state.
    pub fn recovery_inspect(&self) -> Result<Value> {
        let mut client = self
            .client
            .try_borrow_mut()
            .map_err(|_| "recovery transaction busy")?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(db_error)?;
        check_binding(&mut tx, &self.binding, false)?;
        let r = tx
            .query_one(
                "SELECT mode,seal,restore FROM recovery_guard WHERE singleton",
                &[],
            )
            .map_err(db_error)?;
        let result = json!({"mode":r.get::<_,String>(0),"seal":r.get::<_,Option<String>>(1).as_deref().map(decode).transpose()?,"restore":r.get::<_,Option<String>>(2).as_deref().map(decode).transpose()?,"database":database(&mut tx)?,"state":inventory(&mut tx)?});
        tx.commit().map_err(db_error)?;
        Ok(result)
    }
    pub fn recovery_objects(&self, after_namespace: &str, after_digest: &str) -> Result<Value> {
        let mut client = self
            .client
            .try_borrow_mut()
            .map_err(|_| "recovery transaction busy")?;
        let mut tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .map_err(db_error)?;
        check_binding(&mut tx, &self.binding, false)?;
        let mut out = Vec::new();
        for r in tx.query("SELECT namespace,digest,size FROM artifact_objects WHERE verified AND (namespace,digest)>($1,$2) ORDER BY namespace,digest LIMIT 1000",&[&after_namespace,&after_digest]).map_err(db_error)?{out.push(json!({"namespace":r.get::<_,String>(0),"digest":r.get::<_,String>(1),"size":r.get::<_,i64>(2)}));}
        tx.commit().map_err(db_error)?;
        Ok(json!(out))
    }
    pub fn recovery_audit(&mut self, p: &Value) -> Result<Value> {
        let seal = &p["seal"];
        let payload = &seal["payload"];
        let key = key()?;
        verify(payload, text(seal, "signature")?, &key)?;
        let mut client = self
            .client
            .try_borrow_mut()
            .map_err(|_| "recovery transaction busy")?;
        let mut tx = client.transaction().map_err(db_error)?;
        check_binding(&mut tx, &self.binding, true)?;
        let db = database(&mut tx)?;
        if db == payload["source"] {
            return Err("cannot restore over the source authority".into());
        }
        let current = inventory(&mut tx)?;
        if current != payload["state"] || layout(&mut tx)? != payload["layout_digest"] {
            return Err("restored state differs from final sealed source".into());
        }
        let row = tx
            .query_one(
                "SELECT mode,key_fingerprint,seal FROM recovery_guard WHERE singleton",
                &[],
            )
            .map_err(db_error)?;
        if row.get::<_, String>(0) != "sealed"
            || row.get::<_, Option<String>>(1).as_deref() != Some(hash(&key).as_str())
            || row.get::<_, Option<String>>(2).as_deref() != Some(seal.to_string().as_str())
        {
            return Err("archive lacks original final source seal".into());
        }
        let restore = json!({"archive_sha256":text(p,"archive_sha256")?,"target":db,"seal_digest":hash(seal.to_string().as_bytes()),"verified_at_ms":now(&mut tx)?});
        tx.execute(
            "UPDATE recovery_guard SET mode='audit',restore=$1 WHERE singleton",
            &[&restore.to_string()],
        )
        .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(restore)
    }
    /// The fencing authority signs the exact target, archive, final source seal,
    /// object-verification digest and expiry. There is no force/boolean bypass.
    pub fn recovery_activate(&mut self, p: &Value) -> Result<Value> {
        let proof = &p["proof"];
        let payload = &proof["payload"];
        let key = key()?;
        verify(payload, text(proof, "signature")?, &key)?;
        let next = identity(payload, "next_controller")?;
        let operator = identity(payload, "operator")?;
        identity(payload, "fencing_evidence_reference")?;
        let mut client = self
            .client
            .try_borrow_mut()
            .map_err(|_| "recovery transaction busy")?;
        let mut tx = client.transaction().map_err(db_error)?;
        check_binding(&mut tx, &self.binding, true)?;
        let row = tx
            .query_one(
                "SELECT mode,key_fingerprint,seal,restore FROM recovery_guard WHERE singleton",
                &[],
            )
            .map_err(db_error)?;
        if row.get::<_, String>(0) != "audit"
            || row.get::<_, Option<String>>(1).as_deref() != Some(hash(&key).as_str())
        {
            return Err("activation requires an audited clone and original recovery key".into());
        }
        let seal = decode(row.get::<_, &str>(2))?;
        let restore = decode(row.get::<_, &str>(3))?;
        let state = inventory(&mut tx)?;
        let current_time = now(&mut tx)?;
        if payload["format"] != "mas-fencing-proof/1"
            || payload["target"] != database(&mut tx)?
            || payload["source"] != seal["payload"]["source"]
            || payload["archive_sha256"] != restore["archive_sha256"]
            || payload["seal_digest"] != restore["seal_digest"]
            || payload["state_digest"] != state["state_digest"]
            || state != seal["payload"]["state"]
            || payload["object_digest"] != state["object_digest"]
            || payload["object_count"] != state["object_count"]
            || payload["verified_object_digest"] != state["object_digest"]
            || !payload["expires_at_ms"]
                .as_i64()
                .is_some_and(|v| v > current_time && v <= current_time + 3_600_000)
            || payload["key_reference"] != seal["payload"]["key_reference"]
        {
            return Err("fencing proof does not establish this final recovery state".into());
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
        tx.execute("UPDATE controller_lease SET draining=true,lease_until=clock_timestamp()+lease_seconds*interval '1 second' WHERE singleton",&[]).map_err(db_error)?;
        tx.execute(
            "INSERT INTO recovery_events(operation,evidence) VALUES('clone_activated',$1)",
            &[&proof.to_string()],
        )
        .map_err(db_error)?;
        tx.execute(
            "UPDATE recovery_guard SET mode='active' WHERE singleton",
            &[],
        )
        .map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Ok(
            json!({"deployment":self.binding.deployment,"controller":next,"epoch":epoch,"operator":operator,"draining":true,"final_allocation_state_verified":true}),
        )
    }
}
