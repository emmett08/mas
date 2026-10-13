//! Durable bounded transport. Actor identities are asserted by a trusted host;
//! this embedded store is not a network authentication or external-effect layer.
use crate::{parse_message, Message, MAX_BYTES, MAX_DEPTH};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};
use std::{path::Path, time::{Duration, SystemTime, UNIX_EPOCH}};

type Result<T> = std::result::Result<T, String>;
fn err(e: impl std::fmt::Display) -> String { e.to_string() }
fn now() -> Result<f64> { Ok(SystemTime::now().duration_since(UNIX_EPOCH).map_err(err)?.as_secs_f64()) }

/// One SQLite connection. Independent processes/connections serialize writes via
/// BEGIN IMMEDIATE; a successful return means COMMIT completed.
pub struct Store { db: Connection }
struct Run { config: Value, deadline: f64, messages: i64, bytes: i64, calls: i64, active: String }
fn run(db: &Connection, id: &str, actor: Option<&str>, deadline: bool) -> Result<Run> {
    let row = db.query_row("SELECT config,deadline,message_count,byte_count,call_count,active FROM runs WHERE id=?",[id],|r| {
        Ok((r.get::<_,String>(0)?,r.get::<_,f64>(1)?,r.get::<_,i64>(2)?,r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,String>(5)?))
    }).optional().map_err(err)?.ok_or("unknown run")?;
    let config: Value = serde_json::from_str(&row.0).map_err(err)?;
    if actor.is_some_and(|a| !config["agents"].as_array().is_some_and(|v| v.iter().any(|x| x.as_str()==Some(a)))) {
        return Err("unregistered worker".into());
    }
    if deadline && now()? >= row.1 { return Err("wall-clock budget exhausted".into()); }
    Ok(Run { config, deadline:row.1,messages:row.2,bytes:row.3,calls:row.4,active:row.5 })
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
        if objects == 0 && application == 0 && version == 0 {
            tx.execute_batch("CREATE TABLE runs(id TEXT PRIMARY KEY,config TEXT NOT NULL,deadline REAL NOT NULL,message_count INTEGER NOT NULL DEFAULT 0,byte_count INTEGER NOT NULL DEFAULT 0,call_count INTEGER NOT NULL DEFAULT 0,active TEXT NOT NULL DEFAULT '1');
                CREATE TABLE messages(seq INTEGER PRIMARY KEY AUTOINCREMENT,run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,sender TEXT NOT NULL,recipient TEXT NOT NULL,content TEXT NOT NULL,received INTEGER NOT NULL DEFAULT 0,acked INTEGER NOT NULL DEFAULT 0,UNIQUE(run,id));
                CREATE INDEX mailbox ON messages(run,recipient,acked,seq);
                CREATE TABLE calls(run TEXT NOT NULL REFERENCES runs(id),id TEXT NOT NULL,actor TEXT NOT NULL,state TEXT NOT NULL DEFAULT 'reserved',result TEXT,UNIQUE(run,id));
                PRAGMA application_id=1296126769; PRAGMA user_version=1;").map_err(err)?;
        } else {
            if application != APPLICATION_ID || version != 1 || objects != 4 {
                return Err("incompatible database schema: use a fresh MAS database path; legacy or unrelated databases require explicit migration".into());
            }
            for (table,expected) in [
                ("runs", &["id","config","deadline","message_count","byte_count","call_count","active"][..]),
                ("messages", &["seq","run","id","sender","recipient","content","received","acked"][..]),
                ("calls", &["run","id","actor","state","result"][..]),
            ] {
                let mut statement = tx.prepare(&format!("PRAGMA table_info({table})")).map_err(err)?;
                let names = statement.query_map([],|r|r.get::<_,String>(1)).map_err(err)?.collect::<std::result::Result<Vec<_>,_>>().map_err(err)?;
                if names.iter().map(String::as_str).collect::<Vec<_>>() != expected {
                    return Err("incompatible database schema: table layout differs from MAS schema version 1".into());
                }
            }
        }
        tx.commit().map_err(err)?;
        Ok(Self {db})
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
        if now()? >= r.deadline { return Err("wall-clock budget exhausted".into()); }
        if message.version != r.active { return Err("inactive or incompatible protocol version".into()); }
        if r.messages >= r.config["max_messages"].as_i64().unwrap() || size > r.config["max_bytes"].as_i64().unwrap()-r.bytes { return Err("communication budget exhausted".into()); }
        let pending: i64 = tx.query_row("SELECT COUNT(*) FROM messages WHERE run=? AND recipient=? AND acked=0",params![message.run_id,message.recipient],|r|r.get(0)).map_err(err)?;
        if pending >= r.config["mailbox_capacity"].as_i64().unwrap() { return Err("mailbox full; retry after acknowledgement".into()); }
        if r.config["application"] == "agent-task/1" { crate::tasks::validate_transition(&tx,&message)?; }
        tx.execute("INSERT INTO messages(run,id,sender,recipient,content) VALUES(?,?,?,?,?)",params![message.run_id,message.message_id,message.sender,message.recipient,content]).map_err(err)?;
        let seq = tx.last_insert_rowid();
        tx.execute("UPDATE runs SET message_count=message_count+1,byte_count=byte_count+? WHERE id=?",params![size,message.run_id]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(json!({"accepted":true,"duplicate":false,"sequence":seq}))
    }
    pub fn receive(&mut self, id: &str, actor: &str, limit: usize) -> Result<Value> {
        if !(1..=100).contains(&limit) { return Err("limit must be 1..100".into()); }
        let tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        run(&tx,id,Some(actor),false)?;
        let rows = {
            let mut stmt = tx.prepare("SELECT seq,content,received FROM messages WHERE run=? AND recipient=? AND acked=0 ORDER BY seq LIMIT ?").map_err(err)?;
            let collected = stmt.query_map(params![id,actor,limit as i64],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,r.get::<_,bool>(2)?))).map_err(err)?.collect::<std::result::Result<Vec<_>,_>>().map_err(err)?;
            collected
        };
        let mut result = Vec::new();
        for (seq,content,received) in rows {
            let message: Value = serde_json::from_str(&content).map_err(err)?;
            tx.execute("UPDATE messages SET received=1 WHERE seq=?",[seq]).map_err(err)?;
            result.push(json!({"sequence":seq,"message":message,"redelivered":received}));
        }
        tx.commit().map_err(err)?;
        Ok(json!(result))
    }
    pub fn ack(&mut self, id: &str, message_id: &str, actor: &str) -> Result<Value> {
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
        if !crate::identity(call_id) { return Err("invalid call id".into()); }
        let tx = self.db.transaction_with_behavior(TransactionBehavior::Immediate).map_err(err)?;
        let r = run(&tx,id,Some(actor),true)?;
        if tx.query_row("SELECT 1 FROM calls WHERE run=? AND id=?",params![id,call_id],|_|Ok(())).optional().map_err(err)?.is_some() { return Err("call already reserved; reconcile rather than repeat".into()); }
        if r.calls >= r.config["max_calls"].as_i64().unwrap() { return Err("call budget exhausted".into()); }
        tx.execute("INSERT INTO calls(run,id,actor) VALUES(?,?,?)",params![id,call_id,actor]).map_err(err)?;
        tx.execute("UPDATE runs SET call_count=call_count+1 WHERE id=?",[id]).map_err(err)?;
        tx.commit().map_err(err)?;
        Ok(json!({"reserved":true,"call_id":call_id}))
    }
    pub fn finish_call(&mut self,id:&str,actor:&str,call_id:&str,evidence:&Value) -> Result<Value> {
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
        // A read transaction gives configuration, counters and history one snapshot.
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
        tx.commit().map_err(err)?;
        Ok(json!({"run_id":id,"config":r.config,"active_protocol":r.active,"deadline":r.deadline,"usage":{"messages":r.messages,"bytes":r.bytes,"calls":r.calls},"messages":messages,"calls":calls,"protocols":[],"execution_claim":"Host execution requires external agent records; receive and ack alone do not prove cognition."}))
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

}
