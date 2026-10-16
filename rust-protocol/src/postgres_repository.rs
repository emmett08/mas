//! PostgreSQL protocol repository. All protocol writes lock and validate the
//! authority row in the same transaction as the state change. The caller owns
//! authenticated connection/TLS configuration and database promotion policy.
//!
//! This repository covers Rust protocol state; it does not make Python host
//! journals, artefacts or controller bootstrap highly available.
use crate::{
    parse_message,
    admission::{AdmissionCode as Code, AdmissionError as Admission, AdmissionResult},
    repository::ProtocolRepository,
    store::{config, RunMembership},
    Message, MAX_BYTES, MAX_DEPTH,
};
use postgres::{Client, Transaction};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{cell::RefCell, collections::BTreeSet};

type Result<T> = std::result::Result<T, String>;
pub(crate) fn db_error(_: postgres::Error) -> String {
    "PostgreSQL repository operation failed; reconcile the original identity before retry".into()
}
fn json_error(_: serde_json::Error) -> String {
    "invalid retained protocol JSON".into()
}

const DDL: &str = "
CREATE TABLE authority(singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton), deployment text NOT NULL, controller text NOT NULL, epoch bigint NOT NULL CHECK(epoch>0), version bigint NOT NULL, layout_digest text NOT NULL);
CREATE TABLE runs(id text PRIMARY KEY, config text NOT NULL, deadline double precision NOT NULL, message_count bigint NOT NULL DEFAULT 0 CHECK(message_count>=0), byte_count bigint NOT NULL DEFAULT 0 CHECK(byte_count>=0), call_count bigint NOT NULL DEFAULT 0 CHECK(call_count>=0), active text NOT NULL DEFAULT '1', expired boolean NOT NULL DEFAULT false);
CREATE TABLE messages(seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, run text NOT NULL REFERENCES runs(id), id text NOT NULL, sender text NOT NULL, recipient text NOT NULL, content text NOT NULL, received boolean NOT NULL DEFAULT false, acked boolean NOT NULL DEFAULT false, UNIQUE(run,id));
CREATE INDEX mailbox ON messages(run,recipient,acked,seq);
CREATE TABLE calls(seq bigint GENERATED ALWAYS AS IDENTITY, run text NOT NULL REFERENCES runs(id), id text NOT NULL, actor text NOT NULL, state text NOT NULL DEFAULT 'reserved' CHECK(state IN ('reserved','recorded')), result text, PRIMARY KEY(run,id));
CREATE TABLE tasks(seq bigint GENERATED ALWAYS AS IDENTITY, run text NOT NULL REFERENCES runs(id), id text NOT NULL, request_id text NOT NULL, state text NOT NULL DEFAULT 'open' CHECK(state IN ('open','completed','unresolved','failed','cancelled','expired_outcome_unknown')), terminal_id text, closed_at double precision, PRIMARY KEY(run,id), UNIQUE(run,request_id), FOREIGN KEY(run,request_id) REFERENCES messages(run,id), FOREIGN KEY(run,terminal_id) REFERENCES messages(run,id));
CREATE TABLE admissions(scope text PRIMARY KEY, policy_fingerprint text NOT NULL, limit_count bigint NOT NULL CHECK(limit_count>0), attempts bigint NOT NULL DEFAULT 0 CHECK(attempts>=0 AND attempts<=limit_count));
";

/// Possession of an epoch alone is not caller authentication. These values are
/// resolved by the trusted controller, never from an actor's request body.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorityBinding {
    pub deployment: String,
    pub controller: String,
    pub epoch: i64,
}

pub struct PostgresRepository {
    pub(crate) client: RefCell<Client>,
    pub(crate) binding: AuthorityBinding,
}

fn namespace(name: &str) -> Result<String> {
    if name.is_empty()
        || name.len() > 63
        || !name.as_bytes()[0].is_ascii_lowercase()
        || !name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
        || name.starts_with("pg_")
        || name == "public"
    {
        return Err("invalid dedicated PostgreSQL namespace".into());
    }
    Ok(format!("\"{name}\""))
}
fn binding_valid(binding: &AuthorityBinding) -> Result<()> {
    if !crate::identity(&binding.deployment)
        || !crate::identity(&binding.controller)
        || binding.epoch <= 0
    {
        return Err("invalid protocol authority binding".into());
    }
    Ok(())
}
pub(crate) fn layout(tx: &mut Transaction<'_>) -> Result<String> {
    let mut records = Vec::new();
    for row in tx.query("SELECT c.relname,a.attname,format_type(a.atttypid,a.atttypmod),a.attnotnull,coalesce(pg_get_expr(d.adbin,d.adrelid),''),a.attidentity::text FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace JOIN pg_attribute a ON a.attrelid=c.oid LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum WHERE n.nspname=current_schema() AND c.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped ORDER BY c.relname,a.attnum", &[]).map_err(db_error)? {
        records.push(json!([row.get::<_,String>(0),row.get::<_,String>(1),row.get::<_,String>(2),row.get::<_,bool>(3),row.get::<_,String>(4),row.get::<_,String>(5)]));
    }
    for row in tx.query("SELECT c.relname,k.conname,pg_get_constraintdef(k.oid) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() ORDER BY c.relname,k.conname", &[]).map_err(db_error)? {
        records.push(json!([row.get::<_,String>(0),row.get::<_,String>(1),row.get::<_,String>(2)]));
    }
    for row in tx.query("SELECT indexname,indexdef FROM pg_indexes WHERE schemaname=current_schema() ORDER BY indexname", &[]).map_err(db_error)? {
        records.push(json!([row.get::<_,String>(0),row.get::<_,String>(1)]));
    }
    for row in tx.query("SELECT c.relname,t.tgname,pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND NOT t.tgisinternal ORDER BY c.relname,t.tgname", &[]).map_err(db_error)? {
        records.push(json!([row.get::<_,String>(0),row.get::<_,String>(1),row.get::<_,String>(2)]));
    }
    Ok(Sha256::digest(json!(records).to_string().as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect())
}

impl PostgresRepository {
    /// Provision a *new* dedicated namespace. An existing namespace is rejected;
    /// this operation cannot reset epochs, allocations or retained identities.
    pub fn initialise(mut client: Client, schema: &str, binding: AuthorityBinding) -> Result<Self> {
        binding_valid(&binding)?;
        if binding.epoch != 1 {
            return Err("new authority must start at epoch one".into());
        }
        let quoted = namespace(schema)?;
        let mut tx = client.transaction().map_err(db_error)?;
        tx.batch_execute(&format!(
            "CREATE SCHEMA {quoted}; SET LOCAL search_path TO {quoted},pg_catalog;"
        ))
        .map_err(db_error)?;
        tx.batch_execute(DDL).map_err(db_error)?;
        let digest = layout(&mut tx)?;
        tx.execute("INSERT INTO authority(singleton,deployment,controller,epoch,version,layout_digest) VALUES(true,$1,$2,$3,1,$4)", &[&binding.deployment,&binding.controller,&binding.epoch,&digest]).map_err(db_error)?;
        tx.commit().map_err(db_error)?;
        Self::open(client, schema, binding)
    }

    /// Accept an already authenticated PostgreSQL client. The application may
    /// require TLS using its connector; no network credentials are logged.
    pub fn open(mut client: Client, schema: &str, binding: AuthorityBinding) -> Result<Self> {
        binding_valid(&binding)?;
        client
            .batch_execute(&format!(
                "SET search_path TO {},pg_catalog; SET TIME ZONE 'UTC'; SET datestyle TO ISO,YMD; SET extra_float_digits TO 3;",
                namespace(schema)?
            ))
            .map_err(db_error)?;
        let mut tx = client.transaction().map_err(db_error)?;
        let row=tx.query_opt("SELECT deployment,controller,epoch,version,layout_digest FROM authority WHERE singleton=true FOR UPDATE",&[]).map_err(db_error)?.ok_or("missing protocol authority")?;
        if row.get::<_, String>(0) != binding.deployment
            || row.get::<_, String>(1) != binding.controller
            || row.get::<_, i64>(2) != binding.epoch
        {
            return Err("stale protocol authority".into());
        }
        if !matches!(row.get::<_, i64>(3), 1..=12) || row.get::<_, String>(4) != layout(&mut tx)? {
            return Err("incompatible PostgreSQL protocol schema".into());
        }
        tx.commit().map_err(db_error)?;
        Ok(Self {
            client: RefCell::new(client),
            binding,
        })
    }

    /// Fence handles bound to the old controller atomically in the same shared
    /// database. This is not backup activation or a replicated promotion policy.
    pub fn handover(&mut self, next_controller: &str) -> Result<AuthorityBinding> {
        if !crate::identity(next_controller) || next_controller == self.binding.controller {
            return Err("replacement controller identity must differ".into());
        }
        let epoch = self
            .binding
            .epoch
            .checked_add(1)
            .ok_or("authority epoch exhausted")?;
        self.transaction(|tx| {
            tx.execute(
                "UPDATE authority SET controller=$1,epoch=$2 WHERE singleton=true",
                &[&next_controller, &epoch],
            )
            .map_err(db_error)?;
            let version: i64 = tx
                .query_one("SELECT version FROM authority WHERE singleton", &[])
                .map_err(db_error)?
                .get(0);
            if version >= 3 {
                crate::shared_leadership::handed_over(tx, next_controller, epoch)?;
            }
            Ok(())
        })?;
        Ok(AuthorityBinding {
            deployment: self.binding.deployment.clone(),
            controller: next_controller.into(),
            epoch,
        })
    }

    pub(crate) fn transaction<T>(
        &self,
        apply: impl FnOnce(&mut Transaction<'_>) -> Result<T>,
    ) -> Result<T> {
        let mut client = self
            .client
            .try_borrow_mut()
            .map_err(|_| "repository transaction already active")?;
        let mut tx = client.transaction().map_err(db_error)?;
        // Row ownership serialises all protocol state mutations and rejects an
        // old controller even if its connection survived handover. Database
        // errors are never automatically retried; the caller reconciles IDs.
        let row=tx.query_opt("SELECT deployment,controller,epoch,version FROM authority WHERE singleton=true FOR UPDATE",&[]).map_err(db_error)?.ok_or("missing protocol authority")?;
        if row.get::<_, String>(0) != self.binding.deployment
            || row.get::<_, String>(1) != self.binding.controller
            || row.get::<_, i64>(2) != self.binding.epoch
            || !matches!(row.get::<_, i64>(3), 1..=12)
        {
            return Err("stale protocol authority".into());
        }
        if row.get::<_, i64>(3) >= 6 {
            crate::shared_backup::check_active(&mut tx)?;
        }
        if row.get::<_, i64>(3) >= 3 {
            crate::shared_leadership::check_lease(&mut tx)?;
        }
        let result = apply(&mut tx)?;
        tx.commit().map_err(db_error)?;
        Ok(result)
    }
}

pub(crate) struct Run {
    pub(crate) config: Value,
    deadline: f64,
    messages: i64,
    bytes: i64,
    pub(crate) calls: i64,
    active: String,
    expired: bool,
}
pub(crate) fn run(tx: &mut Transaction<'_>, id: &str, actor: Option<&str>) -> Result<Run> {
    crate::shared_archive::require_open(tx,id)?;
    let row=tx.query_opt("SELECT config,deadline,message_count,byte_count,call_count,active,expired FROM runs WHERE id=$1",&[&id]).map_err(db_error)?.ok_or("unknown run")?;
    let config: Value = serde_json::from_str(row.get::<_, &str>(0)).map_err(json_error)?;
    if actor.is_some_and(|a| {
        !config["agents"]
            .as_array()
            .is_some_and(|v| v.iter().any(|x| x.as_str() == Some(a)))
    }) {
        return Err("unregistered worker".into());
    }
    Ok(Run {
        config,
        deadline: row.get(1),
        messages: row.get(2),
        bytes: row.get(3),
        calls: row.get(4),
        active: row.get(5),
        expired: row.get(6),
    })
}
pub(crate) fn reconcile(tx: &mut Transaction<'_>, id: &str) -> Result<(bool, u64, f64)> {
    let r = run(tx, id, None)?;
    let observed: f64 = tx
        .query_one(
            "SELECT extract(epoch from clock_timestamp())::double precision",
            &[],
        )
        .map_err(db_error)?
        .get(0);
    if r.expired || observed >= r.deadline {
        tx.execute("UPDATE runs SET expired=true WHERE id=$1", &[&id])
            .map_err(db_error)?;
        let closed=tx.execute("UPDATE tasks SET state='expired_outcome_unknown',closed_at=$1 WHERE run=$2 AND state='open'",&[&observed,&id]).map_err(db_error)?;
        Ok((true, closed, observed))
    } else {
        Ok((false, 0, observed))
    }
}
fn inspect(tx: &mut Transaction<'_>, id: &str) -> Result<Value> {
    let r = run(tx, id, None)?;
    let messages=tx.query("SELECT id,sender,recipient,received,acked FROM messages WHERE run=$1 ORDER BY seq",&[&id]).map_err(db_error)?.into_iter().map(|row|json!({"id":row.get::<_,String>(0),"sender":row.get::<_,String>(1),"recipient":row.get::<_,String>(2),"received":row.get::<_,bool>(3),"acked":row.get::<_,bool>(4)})).collect::<Vec<_>>();
    let mut calls = Vec::new();
    for row in tx
        .query(
            "SELECT id,actor,state,result FROM calls WHERE run=$1 ORDER BY seq",
            &[&id],
        )
        .map_err(db_error)?
    {
        let evidence = row
            .get::<_, Option<String>>(3)
            .map(|s| serde_json::from_str::<Value>(&s))
            .transpose()
            .map_err(json_error)?;
        calls.push(json!({"id":row.get::<_,String>(0),"actor":row.get::<_,String>(1),"state":row.get::<_,String>(2),"evidence":evidence}));
    }
    let tasks=tx.query("SELECT id,request_id,state,terminal_id,closed_at FROM tasks WHERE run=$1 ORDER BY seq",&[&id]).map_err(db_error)?.into_iter().map(|row| {
        let state:String=row.get(2);
        json!({"task_id":row.get::<_,String>(0),"request_id":row.get::<_,String>(1),"state":state,"outcome":if state=="open" {Value::Null} else {json!(state)},"terminal_message_id":row.get::<_,Option<String>>(3),"closed_at":row.get::<_,Option<f64>>(4)})
    }).collect::<Vec<_>>();
    Ok(
        json!({"run_id":id,"expired":r.expired,"tasks":tasks,"config":r.config,"active_protocol":r.active,"deadline":r.deadline,"usage":{"messages":r.messages,"bytes":r.bytes,"calls":r.calls},"messages":messages,"calls":calls,"protocols":[],"execution_claim":"Host execution requires external agent records; receive and ack alone do not prove cognition."}),
    )
}

impl ProtocolRepository for PostgresRepository {
    fn register_admission(&mut self, scope: &str, fingerprint: &str, limit: i64) -> Result<()> {
        if !crate::identity(scope)
            || limit <= 0
            || fingerprint.len() != 64
            || !fingerprint
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err("invalid admission scope, fingerprint or limit".into());
        }
        self.transaction(|tx| {
            if let Some(row) = tx
                .query_opt(
                    "SELECT policy_fingerprint,limit_count FROM admissions WHERE scope=$1",
                    &[&scope],
                )
                .map_err(db_error)?
            {
                if row.get::<_, String>(0) != fingerprint || row.get::<_, i64>(1) != limit {
                    return Err("frozen admission policy differs".into());
                }
            } else {
                tx.execute(
                    "INSERT INTO admissions(scope,policy_fingerprint,limit_count) VALUES($1,$2,$3)",
                    &[&scope, &fingerprint, &limit],
                )
                .map_err(db_error)?;
            }
            Ok(())
        })
    }
    fn reserve_admission(&mut self, scope: &str) -> Result<i64> {
        self.transaction(|tx| {
            let row = tx
                .query_opt(
                    "SELECT attempts,limit_count FROM admissions WHERE scope=$1",
                    &[&scope],
                )
                .map_err(db_error)?
                .ok_or("unknown admission scope")?;
            let used: i64 = row.get(0);
            let limit: i64 = row.get(1);
            if used >= limit {
                return Err("admission attempt budget exhausted".into());
            }
            tx.execute(
                "UPDATE admissions SET attempts=attempts+1 WHERE scope=$1",
                &[&scope],
            )
            .map_err(db_error)?;
            Ok(used + 1)
        })
    }
    fn reconcile(&self, id: &str) -> Result<Value> {
        self.transaction(|tx| {
            let (expired, closed, _) = reconcile(tx, id)?;
            Ok(json!({"run_id":id,"expired":expired,"closed_tasks":closed}))
        })
    }
    fn run_membership(&self, id: &str) -> Result<RunMembership> {
        self.reconcile(id)?;
        self.transaction(|tx| {
            let r = run(tx, id, None)?;
            let agents = r.config["agents"]
                .as_array()
                .filter(|a| (1..=32).contains(&a.len()))
                .ok_or("invalid registered workers")?;
            let mut actors = BTreeSet::new();
            for value in agents {
                let actor = value
                    .as_str()
                    .filter(|a| crate::identity(a))
                    .ok_or("invalid registered worker")?;
                if !actors.insert(actor.into()) {
                    return Err("duplicate registered worker".into());
                }
            }
            Ok(RunMembership { actors })
        })
    }
    fn create_run(&mut self, id: &str, supplied: &Value) -> Result<Value> {
        if !crate::identity(id) {
            return Err("invalid run id".into());
        }
        let config = config(supplied)?;
        let encoded = config.to_string();
        self.transaction(|tx| {
            crate::shared_leadership::check_admission(tx)?;
            if let Some(row)=tx.query_opt("SELECT config FROM runs WHERE id=$1",&[&id]).map_err(db_error)? {
                if row.get::<_,String>(0)!=encoded {return Err("frozen run configuration differs".into());}
            } else {
                let wall=config["wall_seconds"].as_i64().unwrap() as f64;
                tx.execute("INSERT INTO runs(id,config,deadline) VALUES($1,$2,extract(epoch from clock_timestamp())::double precision+$3)",&[&id,&encoded,&wall]).map_err(db_error)?;
            }
            Ok(())
        })?;
        self.inspect(id)
    }
    fn send(&mut self, message: &Message, actor: &str) -> Result<Value> {
        self.send_checked(message, actor).map_err(|error|error.message)
    }
    fn send_checked(&mut self, message: &Message, actor: &str) -> AdmissionResult<Value> {
        let exists = self.transaction(|tx| Ok(tx.query_opt("SELECT 1 FROM runs WHERE id=$1", &[&message.run_id]).map_err(db_error)?.is_some()))?;
        if !exists { return Err(Admission::rejected(Code::UnknownRun,"unknown run")); }
        self.reconcile(&message.run_id)?;
        let wire = message.format().map_err(|e|Admission::rejected(Code::InvalidMessage,e.message))?;
        let message = parse_message(&wire, MAX_BYTES, MAX_DEPTH).map_err(|e|Admission::rejected(Code::InvalidMessage,e.message))?;
        let content = message.to_value().to_string();
        let size = content.len() as i64;
        // Return a committed expiry as an inner error so the deadline observation
        // cannot roll back merely because this message has been refused.
        self.transaction(|tx| {
            let r=run(tx,&message.run_id,None)?;
            if !r.config["agents"].as_array().is_some_and(|agents|agents.iter().any(|a|a.as_str()==Some(actor))) {return Ok(Err(Admission::rejected(Code::UnknownActor,"unregistered worker")));}
            if message.sender!=actor {return Ok(Err(Admission::rejected(Code::ActorMismatch,"sender differs from host worker identity")));}
            if !r.config["agents"].as_array().unwrap().iter().any(|a|a.as_str()==Some(&message.recipient)) {return Ok(Err(Admission::rejected(Code::UnknownRecipient,"unregistered recipient")));}
            if r.config["edges"].as_array().is_some_and(|edges|!edges.contains(&json!([message.sender,message.recipient]))) {return Ok(Err(Admission::rejected(Code::ForbiddenRoute,"route forbidden by frozen topology")));}
            if let Some(row)=tx.query_opt("SELECT content,seq FROM messages WHERE run=$1 AND id=$2",&[&message.run_id,&message.message_id]).map_err(db_error)? {
                if row.get::<_,String>(0)!=content {return Ok(Err(Admission::rejected(Code::IdentityConflict,"message identity reused with different content")));}
                return Ok(Ok(json!({"accepted":true,"duplicate":true,"sequence":row.get::<_,i64>(1)})));
            }
            let (expired,_,observed)=reconcile(tx,&message.run_id)?;
            if expired {return Ok(Err(Admission::rejected(Code::DeadlineExceeded,"wall-clock budget exhausted")));}
            if message.version!=r.active {return Ok(Err(Admission::rejected(Code::UnsupportedVersion,"inactive or incompatible protocol version")));}
            if r.messages>=r.config["max_messages"].as_i64().unwrap() || size>r.config["max_bytes"].as_i64().unwrap()-r.bytes {return Ok(Err(Admission::rejected(Code::BudgetExceeded,"communication budget exhausted")));}
            let pending:i64=tx.query_one("SELECT count(*) FROM messages WHERE run=$1 AND recipient=$2 AND NOT acked",&[&message.run_id,&message.recipient]).map_err(db_error)?.get(0);
            if pending>=r.config["mailbox_capacity"].as_i64().unwrap() {return Ok(Err(Admission::mailbox_full()));}
            let task_profile=r.config["application"]=="agent-task/1";
            if task_profile {
                let task=message.payload.get("task_id").and_then(Value::as_str).unwrap_or("");
                let prior=tx.query_opt("SELECT t.request_id,t.state,m.received,m.sender,m.recipient FROM tasks t JOIN messages m ON m.run=t.run AND m.id=t.request_id WHERE t.run=$1 AND t.id=$2",&[&message.run_id,&task]).map_err(db_error)?.map(|row|(row.get(0),row.get(1),row.get(2),row.get(3),row.get(4)));
                if let Err(message)=crate::tasks::validate_prior(&message,prior) {return Ok(Err(Admission::rejected(Code::InvalidTransition,message)));}
            }
            let seq:i64=tx.query_one("INSERT INTO messages(run,id,sender,recipient,content) VALUES($1,$2,$3,$4,$5) RETURNING seq",&[&message.run_id,&message.message_id,&message.sender,&message.recipient,&content]).map_err(db_error)?.get(0);
            if task_profile {
                let task=message.payload["task_id"].as_str().unwrap();
                if message.kind=="Request" {tx.execute("INSERT INTO tasks(run,id,request_id) VALUES($1,$2,$3)",&[&message.run_id,&task,&message.message_id]).map_err(db_error)?;} else {
                    let state=match message.kind.as_str() {"Result"=>message.payload["status"].as_str().unwrap(),"Failure"=>"failed","Cancel"=>"cancelled",_=>return Err("unsupported task kind".into())};
                    let count=tx.execute("UPDATE tasks SET state=$1,terminal_id=$2,closed_at=$3 WHERE run=$4 AND id=$5 AND state='open'",&[&state,&message.message_id,&observed,&message.run_id,&task]).map_err(db_error)?;
                    if count!=1 {return Err("task already terminal".into());}
                }
            }
            tx.execute("UPDATE runs SET message_count=message_count+1,byte_count=byte_count+$1 WHERE id=$2",&[&size,&message.run_id]).map_err(db_error)?;
            Ok(Ok(json!({"accepted":true,"duplicate":false,"sequence":seq})))
        })?
    }
    fn receive(&mut self, id: &str, actor: &str, limit: usize) -> Result<Value> {
        self.reconcile(id)?;
        if !(1..=120).contains(&limit) {
            return Err("limit must be 1..100".into());
        }
        self.transaction(|tx| {
            run(tx,id,Some(actor))?;reconcile(tx,id)?;
            let rows=tx.query("SELECT seq,content,received FROM messages WHERE run=$1 AND recipient=$2 AND NOT acked ORDER BY seq LIMIT $3",&[&id,&actor,&(limit as i64)]).map_err(db_error)?;
            let mut result=Vec::new();
            for row in rows {
                let seq:i64=row.get(0);let message:Value=serde_json::from_str(row.get::<_,&str>(1)).map_err(json_error)?;
                tx.execute("UPDATE messages SET received=true WHERE seq=$1",&[&seq]).map_err(db_error)?;
                let mut delivery=json!({"sequence":seq,"message":message,"redelivered":row.get::<_,bool>(2)});
                if message["kind"]=="Request" {
                    if let Some(task)=message["payload"]["task_id"].as_str() {
                        if let Some(row)=tx.query_opt("SELECT state FROM tasks WHERE run=$1 AND id=$2",&[&id,&task]).map_err(db_error)? {
                            let state:String=row.get(0);delivery["actionable"]=json!(state=="open");delivery["task_state"]=json!(state);
                        }
                    }
                }
                result.push(delivery);
            }
            Ok(json!(result))
        })
    }
    fn ack(&mut self, id: &str, message_id: &str, actor: &str) -> Result<Value> {
        self.reconcile(id)?;
        self.transaction(|tx| {
            run(tx, id, Some(actor))?;
            let row = tx
                .query_opt(
                    "SELECT recipient,received FROM messages WHERE run=$1 AND id=$2",
                    &[&id, &message_id],
                )
                .map_err(db_error)?
                .ok_or("message not addressed to worker")?;
            if row.get::<_, String>(0) != actor {
                return Err("message not addressed to worker".into());
            }
            if !row.get::<_, bool>(1) {
                return Err("message has not been received".into());
            }
            tx.execute(
                "UPDATE messages SET acked=true WHERE run=$1 AND id=$2",
                &[&id, &message_id],
            )
            .map_err(db_error)?;
            Ok(json!({"acknowledged":true,"message_id":message_id}))
        })
    }
    fn reserve_call(&mut self, id: &str, actor: &str, call_id: &str) -> Result<Value> {
        self.reconcile(id)?;
        if !crate::identity(call_id) {
            return Err("invalid call id".into());
        }
        self.transaction(|tx| {
            crate::shared_leadership::check_admission(tx)?;
            let r = run(tx, id, Some(actor))?;
            if reconcile(tx, id)?.0 {
                return Ok(Err("wall-clock budget exhausted".into()));
            }
            if tx
                .query_opt(
                    "SELECT 1 FROM calls WHERE run=$1 AND id=$2",
                    &[&id, &call_id],
                )
                .map_err(db_error)?
                .is_some()
            {
                return Err("call already reserved; reconcile rather than repeat".into());
            }
            if r.calls >= r.config["max_calls"].as_i64().unwrap() {
                return Err("call budget exhausted".into());
            }
            tx.execute(
                "INSERT INTO calls(run,id,actor) VALUES($1,$2,$3)",
                &[&id, &call_id, &actor],
            )
            .map_err(db_error)?;
            tx.execute(
                "UPDATE runs SET call_count=call_count+1 WHERE id=$1",
                &[&id],
            )
            .map_err(db_error)?;
            Ok(Ok(json!({"reserved":true,"call_id":call_id})))
        })?
    }
    fn finish_call(
        &mut self,
        id: &str,
        actor: &str,
        call_id: &str,
        evidence: &Value,
    ) -> Result<Value> {
        self.reconcile(id)?;
        if !evidence.is_object() {
            return Err("evidence object required".into());
        }
        let mut budget = MAX_BYTES;
        crate::validate_value(evidence, 0, &mut budget).map_err(|e| e.message)?;
        let encoded = evidence.to_string();
        crate::parse_json(&encoded).map_err(|e| e.message)?;
        self.transaction(|tx| {
            run(tx, id, Some(actor))?;
            let row = tx
                .query_opt(
                    "SELECT actor,state FROM calls WHERE run=$1 AND id=$2",
                    &[&id, &call_id],
                )
                .map_err(db_error)?
                .ok_or("unknown call reservation")?;
            if row.get::<_, String>(0) != actor {
                return Err("call reservation belongs to another worker".into());
            }
            if row.get::<_, String>(1) != "reserved" {
                return Err("call already completed".into());
            }
            tx.execute(
                "UPDATE calls SET state='recorded',result=$1 WHERE run=$2 AND id=$3",
                &[&encoded, &id, &call_id],
            )
            .map_err(db_error)?;
            Ok(json!({"recorded":true}))
        })
    }
    fn inspect(&self, id: &str) -> Result<Value> {
        self.reconcile(id)?;
        self.transaction(|tx| inspect(tx, id))
    }
    fn observe(&self, id: &str, include_payloads: bool) -> Result<Value> {
        let mut client = self.client.try_borrow_mut().map_err(|_|"repository transaction already active")?;
        let mut tx = client.build_transaction().read_only(true)
            .isolation_level(postgres::IsolationLevel::RepeatableRead).start().map_err(db_error)?;
        // Preserve the repository's authority, recovery and lease requirements.
        // A snapshot needs no row lock and cannot renew a lease or reconcile.
        let authority = tx.query_opt("SELECT deployment,controller,epoch,version FROM authority WHERE singleton=true",&[])
            .map_err(db_error)?.ok_or("missing protocol authority")?;
        let version: i64 = authority.get(3);
        if authority.get::<_,String>(0) != self.binding.deployment
            || authority.get::<_,String>(1) != self.binding.controller
            || authority.get::<_,i64>(2) != self.binding.epoch || !matches!(version,1..=12) {
            return Err("stale protocol authority".into());
        }
        if version >= 6 { crate::shared_backup::check_active(&mut tx)?; }
        if version >= 3 { crate::shared_leadership::check_lease(&mut tx)?; }
        let r = run(&mut tx,id,None)?;
        let run = json!({"config":r.config,"deadline":r.deadline,
            "usage":{"messages":r.messages,"bytes":r.bytes,"calls":r.calls},
            "active_protocol":r.active,"expired":r.expired});
        let mut messages = Vec::new();
        for row in tx.query("SELECT seq,id,sender,recipient,content,received,acked FROM messages WHERE run=$1 ORDER BY seq",&[&id]).map_err(db_error)? {
            messages.push(crate::observation::message(row.get(0),row.get(1),id,row.get(2),row.get(3),row.get(4),row.get(5),row.get(6),include_payloads)?);
        }
        let mut calls = Vec::new();
        for row in tx.query("SELECT id,actor,state,result FROM calls WHERE run=$1 ORDER BY seq",&[&id]).map_err(db_error)? {
            let mut call = json!({"id":row.get::<_,String>(0),"actor":row.get::<_,String>(1),"state":row.get::<_,String>(2)});
            if include_payloads { call["evidence"] = crate::observation::optional_json(row.get(3))?; }
            calls.push(call);
        }
        let tasks = tx.query("SELECT id,request_id,state,terminal_id,closed_at FROM tasks WHERE run=$1 ORDER BY seq",&[&id]).map_err(db_error)?.into_iter().map(|row|
            json!({"task_id":row.get::<_,String>(0),"request_id":row.get::<_,String>(1),
                "state":row.get::<_,String>(2),"terminal_message_id":row.get::<_,Option<String>>(3),
                "closed_at":row.get::<_,Option<f64>>(4)})).collect();
        tx.commit().map_err(db_error)?;
        Ok(crate::observation::snapshot_value(id,include_payloads,run,messages,calls,tasks))
    }
}
