//! Durable bounded transport. Actor identities are asserted by a trusted host;
//! this embedded store is not a network authentication or external-effect layer.
use crate::{parse_message, Message, MAX_BYTES, MAX_DEPTH};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde_json::{json, Value};
use std::{path::Path, time::{Duration, SystemTime, UNIX_EPOCH}};

type Result<T> = std::result::Result<T, String>;
fn err(e: impl std::fmt::Display) -> String { e.to_string() }
fn now() -> Result<f64> { Ok(SystemTime::now().duration_since(UNIX_EPOCH).map_err(err)?.as_secs_f64()) }

/// One SQLite connection. Independent processes/connections serialize writes via
/// BEGIN IMMEDIATE; a successful return means COMMIT completed.
pub struct Store { db: Connection }
struct Run { config: Value, deadline: f64, messages: i64, bytes: i64, calls: i64, active: String, expired: bool }
fn run(db: &Connection, id: &str, actor: Option<&str>, deadline: bool) -> Result<Run> {
    let row = db.query_row("SELECT config,deadline,message_count,byte_count,call_count,active,expired FROM runs WHERE id=?",[id],|r| {
        Ok((r.get::<_,String>(0)?,r.get::<_,f64>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,String>(5)?,r.get::<_,bool>(6)?))
    }).optional().map_err(err)?.ok_or("unknown run")?;
    let config: Value = serde_json::from_str(&row.0).map_err(err)?;
    if actor.is_some_and(|a| !config["agents"].as_array().is_some_and(|v| v.iter().any(|x| x.as_str()==Some(a)))) {
        return Err("unregistered worker".into());
    }
    if deadline && (row.6 || now()? >= row.1) { return Err("wall-clock budget exhausted".into()); }
    Ok(Run { config, deadline:row.1,messages:row.2,bytes:row.3,calls:row.4,active:row.5,expired:row.6 })
}
fn reconcile_transaction(db:&Transaction<'_>,id:&str) -> Result<(bool,usize,f64)> {
    let r=run(db,id,None,false)?;
    let observed=now()?;
    if r.expired || observed>=r.deadline {
        db.execute("UPDATE runs SET expired=1 WHERE id=?",[id]).map_err(err)?;
        let closed=db.execute("UPDATE tasks SET state='expired_outcome_unknown',closed_at=? WHERE run=? AND state='open'",params![observed,id]).map_err(err)?;
        Ok((true,closed,observed))
    } else { Ok((false,0,observed)) }
}
fn config(source: &Value) -> Result<Value> {
    let supplied = source.as_object().ok_or("configuration must be an object")?;
    let allowed = ["agents","edges","mailbox_capacity","application","max_messages","max_bytes","max_calls","wall_seconds"];
    if supplied.keys().any(|k| !allowed.contains(&k.as_str())) { return Err("unknown configuration field".into()); }
    let agents = supplied.get("agents").and_then(Value::as_array).ok_or("agents must be an array")?;
    if !(1..=32).contains(&agents.len()) { return Err("register between 1 and 32 workers".into()); }
    let mut identities = std::collections::BTreeSet::new();
    for agent in agents {
        let name = agent.as_str().filter(|s| crate::identity(s)).ok_or("invalid worker identity")?;
        if !identities.insert(name) { return Err("worker ids must be unique".into()); }
    }
    let mut normal = json!({"agents": agents,"edges":null,"application":null});
    for (key,default) in [("max_messages",100), ("max_bytes",100000), ("max_calls",20), ("wall_seconds",3600), ("mailbox_capacity",100)] {
        let value = supplied.get(key).map_or(Some(default),Value::as_i64).filter(|n| *n > 0).ok_or("budgets and capacity must be positive integers within i64")?;
        if key == "mailbox_capacity" && value > 100 { return Err("mailbox capacity must be 1..100".into()); }
        normal[key] = json!(value);
    }
    if let Some(edges) = supplied.get("edges") {
        // Missing ACL means all registered participants; explicit null is rejected
        // so a malformed topology cannot silently broaden access.
        let edges = edges.as_array().ok_or("edges must be an array; omit to allow all registered routes")?;
        let mut unique = std::collections::BTreeSet::new();
        for edge in edges {
            let pair = edge.as_array().filter(|e| e.len()==2).ok_or("edges must be directed pairs")?;
            let a = pair[0].as_str().ok_or("edge source must be a worker")?;
            let b = pair[1].as_str().ok_or("edge destination must be a worker")?;
            if !identities.contains(a) || !identities.contains(b) || a==b { return Err("edges require distinct registered workers".into()); }
            unique.insert((a,b));
        }
        normal["edges"] = json!(unique.into_iter().map(|(a,b)| vec![a,b]).collect::<Vec<_>>());
    }
    if let Some(application) = supplied.get("application") {
        if !application.is_null() && application.as_str()!=Some("agent-task/1") { return Err("unsupported application profile".into()); }
        normal["application"] = application.clone();
    }
    Ok(normal)
}
impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) { std::fs::create_dir_all(parent).map_err(err)?; }
        let mut db = Connection::open(path).map_err(err)?;
        db.busy_timeout(Duration::from_secs(10)).map_err(err)?;
        db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA synchronous=FULL;").map_err(err)?;
        // Inspect before creating any tables. In particular, the former Python
        // database is not writable by this schema and requires explicit migration.
        // Serialize initialization so concurrent opens cannot see a partial schema.
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let application: i64 = tx.query_row("PRAGMA application_id",[],|r|r.get(0)).map_err(err)?;
        let version: i64 = tx.query_row("PRAGMA user_version",[],|r|r.get(0)).map_err(err)?;
        let objects: i64 = tx.query_row("SELECT COUNT(*) FROM sqlite_master WHERE substr(name,1,7) != 'sqlite_'",[],|r|r.get(0)).map_err(err)?;
        const APPLICATION_ID: i64 = 0x4d415331;
        let fresh = objects == 0 && application == 0 && version == 0;
        if fresh {
            tx.execute_batch("CREATE TABLE runs(id TEXT PRIMARY KEY,config TEXT NOT NULL,deadline REAL NOT NULL,message_count INTEGER NOT NULL DEFAULT 0,byte_count INTEGER NOT NULL DEFAULT 0,call_count INTEGER NOT NULL DEFAULT 0,active TEXT NOT NULL DEFAULT '1');
                CREATE TABLE messages(seq INTEGER PRIMARY KEY AUTOINCREMENT,run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,content TEXT NOT NULL,received INTEGER NOT NULL DEFAULT 0,acked INTEGER NOT NULL DEFAULT 0,UNIQUE(run,id));
                CREATE INDEX mailbox ON messages(run,recipient,acked,seq);
                CREATE TABLE calls(run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,actor TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'reserved',result TEXT,UNIQUE(run,id));
                PRAGMA application_id=1296126769; PRAGMA user_version=1;").map_err(err)?;
        } else {
            if application != APPLICATION_ID || !matches!(version,1|2) || objects != if version == 1 {4} else {6} {
                return Err("incompatible database schema: use a fresh MAS database path; legacy or unrelated databases require explicit migration".into());
            }
            for (table,expected) in [
                ("runs", if version == 1 { &["id","config","deadline","message_count","byte_count","call_count","active"][..] } else { &["id","config","deadline","message_count","byte_count","call_count","active","expired"][..] }),
                ("messages", &["seq","run","id","sender","recipient","content","received","acked"][..]),
                ("calls", &["run","id","actor","state","result"][..]),
                ("tasks", &["run","id","request_id","state","terminal_id","closed_at"][..]),
                ("admissions", &["scope","policy_fingerprint","limit_count","attempts"][..]),
            ] {
                if version == 1 && matches!(table,"tasks"|"admissions") { continue; }
                let mut statement = tx.prepare(&format!("PRAGMA table_info({table})")).map_err(err)?;
                let names = statement.query_map([],|r|r.get::<_,String>(1)).map_err(err)?.collect::<std::result::Result<Vec<_>,_>>().map_err(err)?;
                if names.iter().map(String::as_str).collect::<Vec<_>>() != expected {
                    return Err("incompatible database schema: table layout differs from MAS native schema".into());
                }
            }
        }
        if fresh || version == 1 {
            tx.execute_batch("ALTER TABLE runs ADD COLUMN expired INTEGER NOT NULL DEFAULT 0 CHECK(expired IN (0,1));
                CREATE TABLE tasks(run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,request_id TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'open' CHECK(state IN ('open','completed','unresolved','failed','cancelled','expired_outcome_unknown')),terminal_id TEXT,closed_at REAL,PRIMARY KEY(run,id),UNIQUE(run,request_id),FOREIGN KEY(run,request_id) REFERENCES messages(run,id),FOREIGN KEY(run,terminal_id) REFERENCES messages(run,id));
                CREATE TABLE admissions(scope TEXT PRIMARY KEY,policy_fingerprint TEXT NOT NULL,limit_count INTEGER NOT NULL CHECK(limit_count>0),attempts INTEGER NOT NULL DEFAULT 0 CHECK(attempts>=0 AND attempts<=limit_count));").map_err(err)?;
            // Rebuild task state from native-v1 accepted history in the same
            // migration transaction. Invalid histories leave the old DB intact.
            crate::tasks::migrate(&tx)?;
            tx.execute_batch("PRAGMA user_version=2;").map_err(err)?;
        }
        tx.commit().map_err(err)?;
        Ok(Self {db})
    }
    /// Freeze one gateway quota. Store only a SHA-256 policy fingerprint, never
    /// credentials. Credential and global scopes are charged independently.
    pub fn register_admission(&mut self, scope:&str, policy_fingerprint:&str, limit:i64) -> Result<()> {
        if !crate::identity(scope) || limit<=0 || policy_fingerprint.len()!=64 || !policy_fingerprint.bytes().all(|c|c.is_ascii_digit() || (b'a'..=b'f').contains(&c)) {
            return Err("invalid admission scope, fingerprint or limit".into());
        }
        let tx=self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let old:Option<(String,i64)>=tx.query_row("SELECT policy_fingerprint,limit_count FROM admissions WHERE scope=?",[scope],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(err)?;
        if let Some((fingerprint,prior_limit))=old {
            if fingerprint!=policy_fingerprint || prior_limit!=limit { return Err("frozen admission policy differs".into()); }
        } else {
            tx.execute("INSERT INTO admissions(scope,policy_fingerprint,limit_count) VALUES(?,?,?)",params![scope,policy_fingerprint,limit]).map_err(err)?;
        }
        tx.commit().map_err(err)
    }
    /// Commit a quota reservation before parsing or dispatching an untrusted
    /// request. A later rejection cannot refund the attempt.
    pub fn reserve_admission(&mut self, scope:&str) -> Result<i64> {
        let tx=self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let (used,limit):(i64,i64)=tx.query_row("SELECT attempts,limit_count FROM admissions WHERE scope=?",[scope],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(err)?.ok_or("unknown admission scope")?;
        if used>=limit { return Err("admission attempt budget exhausted".into()); }
        tx.execute("UPDATE admissions SET attempts=attempts+1 WHERE scope=?",[scope]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(used+1)
    }
    /// Persist deadline observation and close open tasks without consuming
    /// mailbox, message, byte or call capacity. Requires host activity and a
    /// usable clock/database; expiry cannot establish an external effect's state.
    pub fn reconcile(&self,id:&str) -> Result<Value> {
        let tx=Transaction::new_unchecked(&self.db,TransactionBehavior::Immediate).map_err(err)?;
        let (expired,closed,_)=reconcile_transaction(&tx,id)?;
        tx.commit().map_err(err)?;
        Ok(json!({"run_id":id,"expired":expired,"closed_tasks":closed}))
    }
    pub fn create_run(&mut self, id: &str, supplied: &Value) -> Result<Value> {
        if !crate::identity(id) { return Err("invalid run id".into()); }
        let config = config(supplied)?;
        let encoded = config.to_string();
        let tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let old: Option<String> = tx.query_row("SELECT config FROM runs WHERE id=?",[id],|r|r.get(0)).optional().map_err(err)?;
        if let Some(old) = old {
            if old != encoded { return Err("frozen run configuration differs".into()); }
        } else {
            let deadline = now()? + config["wall_seconds"].as_i64().unwrap() as f64;
            tx.execute("INSERT INTO runs(id,config,deadline) VALUES(?,?,?)",params![id,encoded,deadline]).map_err(err)?;
        }
        tx.commit().map_err(err)?;
        self.inspect(id)
    }
    pub fn send(&mut self, message: &Message, actor: &str) -> Result<Value> {
        self.reconcile(&message.run_id)?;
        let wire = message.format().map_err(|e|e.message)?;
        let message = parse_message(&wire,MAX_BYTES,MAX_DEPTH).map_err(|e|e.message)?;
        let content = message.to_value().to_string();
        let size = i64::try_from(content.len()).map_err(err)?;
        let tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let r = run(&tx,&message.run_id,Some(actor),false)?;
        if message.sender != actor { return Err("sender differs from host worker identity".into()); }
        if !r.config["agents"].as_array().unwrap().iter().any(|a| a.as_str()==Some(&message.recipient)) { return Err("unregistered recipient".into()); }
        if let Some(edges) = r.config["edges"].as_array() {
            if !edges.contains(&json!([message.sender,message.recipient])) { return Err("route forbidden by frozen topology".into()); }
        }
        let old: Option<(String,i64)> = tx.query_row("SELECT content,seq FROM messages WHERE run=? AND id=?",params![message.run_id,message.message_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(err)?;
        if let Some((prior,seq)) = old {
            if prior != content { return Err("message identity reused with different content".into()); }
            tx.commit().map_err(err)?;
            return Ok(json!({"accepted":true,"duplicate":true,"sequence":seq}));
        }
        // Recheck after acquiring the write lock: preliminary reconciliation
        // may have preceded the deadline. Commit its observation even on refusal.
        let admission = reconcile_transaction(&tx,&message.run_id)?;
        if admission.0 {
            tx.commit().map_err(err)?;
            return Err("wall-clock budget exhausted".into());
        }
        if message.version != r.active { return Err("inactive or incompatible protocol version".into()); }
        if r.messages >= r.config["max_messages"].as_i64().unwrap() || size > r.config["max_bytes"].as_i64().unwrap()-r.bytes { return Err("communication budget exhausted".into()); }
        let pending: i64 = tx.query_row("SELECT COUNT(*) FROM messages WHERE run=? AND recipient=? AND acked=0",params![message.run_id,message.recipient],|r|r.get(0)).map_err(err)?;
        if pending >= r.config["mailbox_capacity"].as_i64().unwrap() { return Err("mailbox full; retry after acknowledgement".into()); }
        if r.config["application"] == "agent-task/1" { crate::tasks::validate_transition(&tx,&message)?; }
        tx.execute("INSERT INTO messages(run,id,sender,recipient,content) VALUES(?,?,?,?,?)",params![message.run_id,message.message_id,message.sender,message.recipient,content]).map_err(err)?;
        let seq = tx.last_insert_rowid();
        if r.config["application"] == "agent-task/1" { crate::tasks::record_transition(&tx,&message,Some(admission.2))?; }
        tx.execute("UPDATE runs SET message_count=message_count+1,byte_count=byte_count+? WHERE id=?",params![size,message.run_id]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(json!({"accepted":true,"duplicate":false,"sequence":seq}))
    }
    pub fn receive(&mut self, id: &str, actor: &str, limit: usize) -> Result<Value> {
        self.reconcile(id)?;
        if !(1..=100).contains(&limit) { return Err("limit must be 1..100".into()); }
        let mut tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        run(&tx,id,Some(actor),false)?;
        if reconcile_transaction(&tx,id)?.0 {
            // A deadline crossed after the preliminary transaction. Persist its
            // observation before any receive error can roll back this operation.
            tx.commit().map_err(err)?;
            tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
            run(&tx,id,Some(actor),false)?;
        }
        let rows = {
            let mut stmt = tx.prepare("SELECT seq,content,received FROM messages WHERE run=? AND recipient=? AND acked=0 ORDER BY seq LIMIT ?").map_err(err)?;
            let collected = stmt.query_map(params![id,actor,limit as i64],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,bool>(2)?))).map_err(err)?.collect::<std::result::Result<Vec<_>,_>>().map_err(err)?;
            collected
        };
        let mut result = Vec::new();
        for (seq,content,received) in rows {
            let message: Value = serde_json::from_str(&content).map_err(err)?;
            tx.execute("UPDATE messages SET received=1 WHERE seq=?",[seq]).map_err(err)?;
            let mut delivery=json!({"sequence":seq,"message":message,"redelivered":received});
            if message["kind"] == "Request" {
                if let Some(task_id)=message["payload"]["task_id"].as_str() {
                    let state:Option<String>=tx.query_row("SELECT state FROM tasks WHERE run=? AND id=?",params![id,task_id],|r|r.get(0)).optional().map_err(err)?;
                    if let Some(state)=state {
                        delivery["actionable"]=json!(state=="open");
                        delivery["task_state"]=json!(state);
                    }
                }
            }
            result.push(delivery);
        }
        tx.commit().map_err(err)?;
        Ok(json!(result))
    }
    pub fn ack(&mut self, id: &str, message_id: &str, actor: &str) -> Result<Value> {
        self.reconcile(id)?;
        let tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        run(&tx,id,Some(actor),false)?;
        let row: Option<(String,bool)> = tx.query_row("SELECT recipient,received FROM messages WHERE run=? AND id=?",params![id,message_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(err)?;
        let (recipient,received) = row.ok_or("message not addressed to worker")?;
        if recipient != actor { return Err("message not addressed to worker".into()); }
        if !received { return Err("message has not been received".into()); }
        tx.execute("UPDATE messages SET acked=1 WHERE run=? AND id=?",params![id,message_id]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(json!({"acknowledged":true,"message_id":message_id}))
    }
    pub fn reserve_call(&mut self,id:&str,actor:&str,call_id:&str) -> Result<Value> {
        self.reconcile(id)?;
        if !crate::identity(call_id) { return Err("invalid call id".into()); }
        let tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let r = run(&tx,id,Some(actor),false)?;
        if reconcile_transaction(&tx,id)?.0 {
            tx.commit().map_err(err)?;
            return Err("wall-clock budget exhausted".into());
        }
        if tx.query_row("SELECT 1 FROM calls WHERE run=? AND id=?",params![id,call_id],|_|Ok(())).optional().map_err(err)?.is_some() { return Err("call already reserved; reconcile rather than repeat".into()); }
        if r.calls >= r.config["max_calls"].as_i64().unwrap() { return Err("call budget exhausted".into()); }
        tx.execute("INSERT INTO calls(run,id,actor) VALUES(?,?,?)",params![id,call_id,actor]).map_err(err)?;
        tx.execute("UPDATE runs SET call_count=call_count+1 WHERE id=?",[id]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(json!({"reserved":true,"call_id":call_id}))
    }
    pub fn finish_call(&mut self,id:&str,actor:&str,call_id:&str,evidence:&Value) -> Result<Value> {
        self.reconcile(id)?;
        if !evidence.is_object() { return Err("evidence object required".into()); }
        // Bound traversal before serialisation, then use the exact Rust/nom JSON
        // grammar so direct library callers cannot bypass CLI type/depth limits.
        let mut budget = MAX_BYTES;
        crate::validate_value(evidence,0,&mut budget).map_err(|e|e.message)?;
        let encoded = evidence.to_string();
        crate::parse_json(&encoded).map_err(|e|e.message)?;
        let tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        run(&tx,id,Some(actor),false)?;
        let row: Option<(String,String)> = tx.query_row("SELECT actor,state FROM calls WHERE run=? AND id=?",params![id,call_id],|r|Ok((r.get(0)?,r.get(1)?))).optional().map_err(err)?;
        let (owner,state) = row.ok_or("unknown call reservation")?;
        if owner != actor { return Err("call reservation belongs to another worker".into()); }
        if state != "reserved" { return Err("call already completed".into()); }
        tx.execute("UPDATE calls SET state='recorded',result=? WHERE run=? AND id=?",params![encoded,id,call_id]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(json!({"recorded":true}))
    }
    pub fn inspect(&self,id:&str) -> Result<Value> {
        // Commit deadline observation before reading evidence, so malformed
        // stored evidence cannot refund expiry. History then uses one snapshot.
        self.reconcile(id)?;
        let tx = self.db.unchecked_transaction().map_err(err)?;
        let r = run(&tx,id,None,false)?;
        let messages = {
            let mut stmt = tx.prepare("SELECT id,sender,recipient,received,acked FROM messages WHERE run=? ORDER BY seq").map_err(err)?;
            let rows = stmt.query_map([id],|r|Ok(json!({"id":r.get::<_,String>(0)?,"sender":r.get::<_,String>(1)?,"recipient":r.get::<_,String>(2)?,"received":r.get::<_,bool>(3)?,"acked":r.get::<_,bool>(4)?}))).map_err(err)?;
            rows.collect::<std::result::Result<Vec<_>,_>>().map_err(err)?
        };
        let calls = {
            let mut stmt = tx.prepare("SELECT id,actor,state,result FROM calls WHERE run=? ORDER BY rowid").map_err(err)?;
            let rows = stmt.query_map([id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<String>>(3)?))).map_err(err)?;
            let mut result = Vec::new();
            for row in rows {
                let (id,actor,state,evidence) = row.map_err(err)?;
                let evidence = evidence.map(|s|serde_json::from_str::<Value>(&s)).transpose().map_err(err)?;
                result.push(json!({"id":id,"actor":actor,"state":state,"evidence":evidence}));
            }
            result
        };
        let tasks=crate::tasks::inspect(&tx,id)?;
        tx.commit().map_err(err)?;
        Ok(json!({"run_id":id,"expired":r.expired,"tasks":tasks,"config":r.config,"active_protocol":r.active,"deadline":r.deadline,"usage":{"messages":r.messages,"bytes":r.bytes,"calls":r.calls},"messages":messages,"calls":calls,"protocols":[],"execution_claim":"Host execution requires external agent records; receive and ack alone do not prove cognition."}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc,Barrier,atomic::{AtomicUsize,Ordering}};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    fn path() -> std::path::PathBuf { std::env::temp_dir().join(format!("mas-native-{}-{}.sqlite",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed))) }
    fn msg(id:&str,from:&str,to:&str,kind:&str,payload:Value) -> Message {
        Message { message_id:id.into(),run_id:"r".into(),sender:from.into(),recipient:to.into(),version:"1".into(),kind:kind.into(),payload:payload.as_object().unwrap().clone() }
    }
    fn setup(p:&Path, application:bool) -> Store {
        let mut store = Store::open(p).unwrap();
        let mut c = json!({"agents":["a","b","c"],"edges":[["a","b"],["b","a"]],"mailbox_capacity":2});
        if application { c["application"] = json!("agent-task/1"); }
        store.create_run("r",&c).unwrap();
        store
    }
    #[test]
    fn restart_redelivery_ack_and_frozen_configuration() {
        let p=path(); let mut s=setup(&p,false);
        let m=msg("m","a","b","Notice",json!({"x":1}));
        let first=s.send(&m,"a").unwrap();
        assert!(s.ack("r","m","b").is_err());
        assert!(s.send(&m,"b").is_err());
        assert_eq!(s.receive("r","b",1).unwrap()[0]["redelivered"],false);
        drop(s); let mut s=Store::open(&p).unwrap();
        assert_eq!(s.receive("r","b",1).unwrap()[0]["redelivered"],true);
        let duplicate=s.send(&m,"a").unwrap();
        assert_eq!(duplicate["sequence"],first["sequence"]);
        assert_eq!(duplicate["duplicate"],true);
        s.ack("r","m","b").unwrap(); s.ack("r","m","b").unwrap();
        assert_eq!(s.receive("r","b",10).unwrap(),json!([]));
        let mut changed=m.clone(); changed.payload.insert("x".into(),json!(2));
        assert!(s.send(&changed,"a").is_err());
        assert!(s.send(&msg("c","a","c","Notice",json!({})),"a").is_err());
        assert!(s.create_run("r",&json!({"agents":["a","b"]})).is_err());
        assert_eq!(s.inspect("r").unwrap()["usage"]["messages"],1);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn mailbox_full_rolls_back_and_ack_releases_capacity() {
        let p=path();let mut s=setup(&p,false);
        for id in ["m1","m2"] { s.send(&msg(id,"a","b","Notice",json!({})),"a").unwrap(); }
        let rejected=msg("m3","a","b","Notice",json!({}));
        assert!(s.send(&rejected,"a").unwrap_err().contains("mailbox full"));
        s.receive("r","b",1).unwrap();
        assert!(s.send(&rejected,"a").is_err());
        s.ack("r","m1","b").unwrap();
        s.send(&rejected,"a").unwrap();
        assert_eq!(s.inspect("r").unwrap()["usage"]["messages"],3);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn task_terminal_race_and_concurrent_dedup() {
        let p=path();let mut s=setup(&p,true);
        let request=msg("q","a","b","Request",json!({"task_id":"t","operation":"check","input":{}}));
        let start=Arc::new(Barrier::new(3));
        let threads=(0..2).map(|_|{
            let p=p.clone();let start=start.clone();let request=request.clone();
            std::thread::spawn(move || { let mut store=Store::open(p).unwrap();start.wait();store.send(&request,"a").unwrap() })
        }).collect::<Vec<_>>();
        start.wait();let results=threads.into_iter().map(|t|t.join().unwrap()).collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|r|r["duplicate"]==false).count(),1);
        assert_eq!(results[0]["sequence"],results[1]["sequence"]);
        let result=msg("r1","b","a","Result",json!({"task_id":"t","reply_to":"q","status":"completed","output":{}}));
        assert!(s.send(&result,"b").unwrap_err().contains("received"));
        s.receive("r","b",1).unwrap();
        let cancel=msg("c1","a","b","Cancel",json!({"task_id":"t","reply_to":"q","reason":"stop"}));
        let start=Arc::new(Barrier::new(3));
        let threads=[(result,"b"),(cancel,"a")].into_iter().map(|(m,a)|{
            let p=p.clone();let start=start.clone();
            std::thread::spawn(move || { let mut store=Store::open(p).unwrap();start.wait();store.send(&m,a) })
        }).collect::<Vec<_>>();
        start.wait();let results=threads.into_iter().map(|t|t.join().unwrap()).collect::<Vec<_>>();
        assert_eq!(results.iter().filter(|r|r.is_ok()).count(),1);
        assert!(results.iter().find_map(|r|r.as_ref().err()).unwrap().contains("terminal"));
        assert_eq!(s.inspect("r").unwrap()["usage"]["messages"],2);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn persistent_call_budget_and_rejected_configuration() {
        let p=path();let mut s=Store::open(&p).unwrap();
        for c in [json!({"agents":["a"],"edges":null}),json!({"agents":["a"],"max_calls":true}),json!({"agents":["a"],"edges":[["a","a"]]}),json!({"agents":["a"],"mailbox_capacity":101}),json!({"agents":["a","a"]})] {
            assert!(s.create_run("r",&c).is_err());
        }
        s.create_run("r",&json!({"agents":["a"],"edges":[],"max_calls":1})).unwrap();
        s.reserve_call("r","a","call").unwrap();drop(s);
        let mut s=Store::open(&p).unwrap();
        assert!(s.reserve_call("r","a","call").is_err());
        assert!(s.reserve_call("r","a","call2").is_err());
        s.finish_call("r","a","call",&json!({"trace":"host-evidence"})).unwrap();
        assert!(s.finish_call("r","a","call",&json!({})).is_err());
        assert_eq!(s.inspect("r").unwrap()["calls"][0]["state"],"recorded");
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn deadline_and_budget_admission_preserve_existing_delivery() {
        let p=path();let mut s=Store::open(&p).unwrap();
        s.create_run("r",&json!({"agents":["a","b"],"max_messages":1})).unwrap();
        let first=msg("m1","a","b","Notice",json!({}));
        s.send(&first,"a").unwrap();
        assert!(s.send(&msg("m2","a","b","Notice",json!({})),"a").unwrap_err().contains("communication budget"));
        s.db.execute("UPDATE runs SET deadline=0 WHERE id='r'",[]).unwrap();
        assert_eq!(s.send(&first,"a").unwrap()["duplicate"],true);
        assert!(s.reserve_call("r","a","call").unwrap_err().contains("wall-clock"));
        assert!(s.send(&msg("m3","a","b","Notice",json!({})),"a").unwrap_err().contains("wall-clock"));
        assert_eq!(s.receive("r","b",1).unwrap().as_array().unwrap().len(),1);
        s.ack("r","m1","b").unwrap();
        assert_eq!(s.inspect("r").unwrap()["usage"]["messages"],1);
        drop(s);std::fs::remove_file(p).unwrap();
    }

    #[test]
    fn incompatible_databases_are_rejected_without_schema_changes() {
        for schema in ["CREATE TABLE unrelated(value TEXT); INSERT INTO unrelated VALUES('preserved');",
            "CREATE TABLE messages(id TEXT,hash TEXT NOT NULL);"] {
            let p=path();let db=Connection::open(&p).unwrap();
            db.execute_batch(schema).unwrap();drop(db);
            let before=std::fs::read(&p).unwrap();
            assert!(matches!(Store::open(&p),Err(e) if e.contains("incompatible database schema")));
            assert_eq!(before,std::fs::read(&p).unwrap());
            std::fs::remove_file(p).unwrap();
        }
    }
    #[test]
    fn call_evidence_is_bounded_and_integer_exact_before_state_change() {
        let p=path();let mut s=setup(&p,false);s.reserve_call("r","a","call").unwrap();
        for evidence in [json!({"n":1.5}),json!({"s":"x".repeat(MAX_BYTES)}),json!({"s":"\n".repeat(MAX_BYTES/2)})] {
            assert!(s.finish_call("r","a","call",&evidence).is_err());
        }
        let mut deep=json!({}); for _ in 0..MAX_DEPTH { deep=json!({"n":deep}); }
        assert!(s.finish_call("r","a","call",&deep).is_err());
        assert_eq!(s.inspect("r").unwrap()["calls"][0]["state"],"reserved");
        let evidence=crate::parse_json(r#"{"n":340282366920938463463374607431768211456}"#).unwrap();
        s.finish_call("r","a","call",&evidence).unwrap();
        assert_eq!(s.inspect("r").unwrap()["calls"][0]["evidence"],evidence);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    fn expire(s:&Store) { s.db.execute("UPDATE runs SET deadline=0 WHERE id='r'",[]).unwrap(); }
    fn request(id:&str,task:&str) -> Message {
        msg(id,"a","b","Request",json!({"task_id":task,"operation":"check","input":{}}))
    }
    fn response(id:&str,task:&str,reply:&str) -> Message {
        msg(id,"b","a","Result",json!({"task_id":task,"reply_to":reply,"status":"completed","output":{}}))
    }
    fn downgrade_native_v1(s:&Store) {
        s.db.execute_batch("DROP TABLE tasks; DROP TABLE admissions; ALTER TABLE runs DROP COLUMN expired; PRAGMA user_version=1;").unwrap();
    }
    #[test]
    fn expiry_bypasses_full_mailbox_and_exhausted_budget_and_survives_rejection() {
        let p=path();let mut s=Store::open(&p).unwrap();
        s.create_run("r",&json!({"agents":["a","b"],"edges":[["a","b"],["b","a"]],"application":"agent-task/1","mailbox_capacity":1,"max_messages":1})).unwrap();
        let q=request("q","t");let accepted=s.send(&q,"a").unwrap();
        let cancel=msg("c","a","b","Cancel",json!({"task_id":"t","reply_to":"q","reason":"stop"}));
        assert!(s.send(&cancel,"a").unwrap_err().contains("communication budget"));
        expire(&s);
        // Expiry must commit despite a rejected request and require no new message.
        assert!(s.send(&cancel,"a").unwrap_err().contains("wall-clock"));
        let task_state:String=s.db.query_row("SELECT state FROM tasks WHERE run='r' AND id='t'",[],|r|r.get(0)).unwrap();
        assert_eq!(task_state,"expired_outcome_unknown");
        assert_eq!(s.inspect("r").unwrap()["usage"]["messages"],1);
        assert_eq!(s.reconcile("r").unwrap()["closed_tasks"],0);
        assert_eq!(s.send(&q,"a").unwrap()["sequence"],accepted["sequence"]);
        drop(s);let mut s=Store::open(&p).unwrap();
        let delivered=s.receive("r","b",1).unwrap();
        assert_eq!(delivered[0]["actionable"],false);
        assert_eq!(delivered[0]["task_state"],"expired_outcome_unknown");
        assert_eq!(delivered[0]["message"],q.to_value());
        assert_eq!(delivered[0]["redelivered"],false);
        assert!(s.send(&response("late","t","q"),"b").is_err());
        s.ack("r","q","b").unwrap();
        assert_eq!(s.receive("r","b",1).unwrap(),json!([]));
        // Even a later wall clock cannot reopen a run after deadline observation.
        s.db.execute("UPDATE runs SET deadline=? WHERE id='r'",[now().unwrap()+3600.0]).unwrap();
        assert!(s.send(&request("new","newtask"),"a").unwrap_err().contains("wall-clock"));
        assert!(s.reserve_call("r","a","late_call").unwrap_err().contains("wall-clock"));
        let inspection=s.inspect("r").unwrap();
        assert_eq!(inspection["expired"],true);
        assert_eq!(inspection["tasks"][0]["terminal_message_id"],Value::Null);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn terminal_acceptance_survives_expiry_and_lost_response_resend() {
        let p=path();let mut s=setup(&p,true);
        let q=request("q","t");s.send(&q,"a").unwrap();
        assert_eq!(s.receive("r","b",1).unwrap()[0]["actionable"],true);
        let response=response("answer","t","q");let accepted=s.send(&response,"b").unwrap();
        expire(&s);assert_eq!(s.reconcile("r").unwrap()["closed_tasks"],0);
        drop(s);let mut s=Store::open(&p).unwrap();
        assert_eq!(s.send(&response,"b").unwrap()["sequence"],accepted["sequence"]);
        assert_eq!(s.send(&response,"b").unwrap()["duplicate"],true);
        let inspection=s.inspect("r").unwrap();
        assert_eq!(inspection["tasks"][0]["state"],"completed");
        assert_eq!(inspection["tasks"][0]["terminal_message_id"],"answer");
        assert_eq!(s.receive("r","b",1).unwrap()[0]["actionable"],false);
        assert_eq!(inspection["usage"]["messages"],2);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn concurrent_expiry_and_late_terminal_have_one_closure() {
        let p=path();let mut s=setup(&p,true);
        s.send(&request("q","t"),"a").unwrap();s.receive("r","b",1).unwrap();expire(&s);
        let start=Arc::new(Barrier::new(4));
        let reconciliations=(0..2).map(|_|{
            let p=p.clone();let start=start.clone();
            std::thread::spawn(move||{let store=Store::open(p).unwrap();start.wait();store.reconcile("r").unwrap()})
        }).collect::<Vec<_>>();
        let p2=p.clone();let start2=start.clone();
        let response=std::thread::spawn(move||{let mut store=Store::open(p2).unwrap();start2.wait();store.send(&response("late","t","q"),"b")});
        start.wait();
        let closed=reconciliations.into_iter().map(|t|t.join().unwrap()["closed_tasks"].as_u64().unwrap()).sum::<u64>();
        // The send's preliminary reconciliation may have won the closure race.
        assert!(closed<=1);assert!(response.join().unwrap().is_err());
        assert_eq!(s.inspect("r").unwrap()["tasks"][0]["state"],"expired_outcome_unknown");
        assert_eq!(s.inspect("r").unwrap()["usage"]["messages"],1);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn task_ledger_and_messages_roll_back_together_on_storage_error() {
        let p=path();let mut s=setup(&p,true);
        s.db.execute_batch("CREATE TRIGGER inject_fault BEFORE INSERT ON tasks BEGIN SELECT RAISE(ABORT,'injected storage failure'); END;").unwrap();
        assert!(s.send(&request("q","t"),"a").unwrap_err().contains("injected storage failure"));
        let state=s.inspect("r").unwrap();
        assert_eq!(state["messages"],json!([]));assert_eq!(state["tasks"],json!([]));assert_eq!(state["usage"]["messages"],0);
        s.db.execute_batch("DROP TRIGGER inject_fault;").unwrap();
        s.send(&request("q","t"),"a").unwrap();
        assert_eq!(s.inspect("r").unwrap()["tasks"][0]["state"],"open");
        expire(&s);
        s.db.execute_batch("CREATE TRIGGER inject_receive_fault BEFORE UPDATE ON messages BEGIN SELECT RAISE(ABORT,'injected receive failure'); END;").unwrap();
        assert!(s.receive("r","b",1).unwrap_err().contains("injected receive failure"));
        let state=s.inspect("r").unwrap();
        assert_eq!(state["tasks"][0]["state"],"expired_outcome_unknown");
        assert_eq!(state["messages"][0]["received"],false);
        s.db.execute_batch("DROP TRIGGER inject_receive_fault;").unwrap();
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn native_v1_migration_reconstructs_open_and_terminal_tasks() {
        let p=path();let mut s=setup(&p,true);
        s.send(&request("q1","t1"),"a").unwrap();s.send(&request("q2","t2"),"a").unwrap();
        s.receive("r","b",2).unwrap();s.send(&response("r1","t1","q1"),"b").unwrap();
        downgrade_native_v1(&s);drop(s);
        let s=Store::open(&p).unwrap();let state=s.inspect("r").unwrap();
        assert_eq!(state["tasks"][0]["state"],"completed");assert_eq!(state["tasks"][0]["terminal_message_id"],"r1");
        assert_eq!(state["tasks"][0]["closed_at"],Value::Null);
        assert_eq!(state["tasks"][1]["state"],"open");
        assert_eq!(s.db.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),2);
        expire(&s);assert_eq!(s.reconcile("r").unwrap()["closed_tasks"],1);
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn native_v1_migration_accepts_maximal_wire_with_larger_json_envelope() {
        let p=path();let mut s=setup(&p,true);
        let mut q=request("q","t");let empty_size=q.format().unwrap().len();
        q.payload["input"]=json!({"text":"x".repeat(MAX_BYTES-empty_size-9)});
        assert_eq!(q.format().unwrap().len(),MAX_BYTES);
        assert!(q.to_value().to_string().len()>MAX_BYTES);
        s.send(&q,"a").unwrap();downgrade_native_v1(&s);drop(s);
        let mut s=Store::open(&p).unwrap();
        assert_eq!(s.receive("r","b",1).unwrap()[0]["message"],q.to_value());
        assert_eq!(s.inspect("r").unwrap()["tasks"][0]["state"],"open");
        drop(s);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn invalid_native_v1_history_leaves_schema_unmodified() {
        let p=path();let mut s=setup(&p,true);s.send(&request("q","t"),"a").unwrap();downgrade_native_v1(&s);
        // Mimic inconsistent legacy retained state; migration must fail atomically.
        s.db.execute("UPDATE messages SET content=? WHERE id='q'",[response("q","t","absent").to_value().to_string()]).unwrap();
        drop(s);let before=std::fs::read(&p).unwrap();
        assert!(Store::open(&p).is_err());assert_eq!(before,std::fs::read(&p).unwrap());
        let db=Connection::open(&p).unwrap();assert_eq!(db.query_row("PRAGMA user_version",[],|r|r.get::<_,i64>(0)).unwrap(),1);
        drop(db);std::fs::remove_file(p).unwrap();
    }
    #[test]
    fn admission_reservations_are_durable_bounded_and_policy_frozen() {
        let p=path();let mut s=Store::open(&p).unwrap();let fingerprint="a".repeat(64);
        s.register_admission("gateway/global",&fingerprint,2).unwrap();
        assert_eq!(s.reserve_admission("gateway/global").unwrap(),1);drop(s);
        let mut s=Store::open(&p).unwrap();s.register_admission("gateway/global",&fingerprint,2).unwrap();
        assert!(s.register_admission("gateway/global",&"b".repeat(64),2).is_err());
        assert!(s.register_admission("gateway/global",&fingerprint,3).is_err());
        assert_eq!(s.reserve_admission("gateway/global").unwrap(),2);
        assert!(s.reserve_admission("gateway/global").is_err());
        assert!(s.register_admission("other","plaintext-secret",10).is_err());
        drop(s);std::fs::remove_file(p).unwrap();
    }

}
