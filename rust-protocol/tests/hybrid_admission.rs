//! Hybrid adapters need a return route and actionable, private admission evidence.
use mas_protocol_codec::{
    admission::{AdmissionCode as Code, AdmissionDisposition as Disposition, PROFILE},
    gateway::Gateway, store::Store, Message,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Database(PathBuf);
impl Database {
    fn new() -> Self { Self(std::env::temp_dir().join(format!("mas-hybrid-admission-{}-{}.sqlite",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)))) }
    fn open(&self) -> Store { Store::open(&self.0).unwrap() }
}
impl Drop for Database { fn drop(&mut self) { let _=std::fs::remove_file(&self.0); } }
fn request(id: &str) -> Message {
    Message {message_id:id.into(),run_id:"r".into(),sender:"a".into(),recipient:"b".into(),version:"1".into(),kind:"Request".into(),
        payload:json!({"task_id":id,"operation":"check","input":{"private":"secret evidence"}}).as_object().unwrap().clone()}
}
fn configuration(reverse: bool) -> Value {
    let edges=if reverse {json!([["a","b"],["b","a"]])} else {json!([["a","b"]])};
    json!({"agents":["a","b"],"edges":edges,"application":"agent-task/1","mailbox_capacity":1})
}
fn gateway(store: Store) -> Gateway {
    Gateway::open(store,&json!({"gateway_id":"hybrid","max_attempts":100,"credentials":[
        {"credential_id":"a","run_id":"r","actor":"a","token":"aa".repeat(32),"max_attempts":100},
        {"credential_id":"b","run_id":"r","actor":"b","token":"bb".repeat(32),"max_attempts":100},
        {"credential_id":"other","run_id":"other","actor":"a","token":"cc".repeat(32),"max_attempts":100}
    ]})).unwrap()
}
fn call(gateway: &mut Gateway, token: &str, mut command: Value) -> Value {
    command["token"]=json!(token.repeat(32));
    gateway.handle_frame(command.to_string().as_bytes()).response
}
fn checked(message: &Message) -> Value { json!({"command":"send_checked","profile":PROFILE,"wire":message.format().unwrap()}) }
fn lookup(id: &str) -> Value { json!({"command":"reconcile_message","profile":PROFILE,"message_id":id}) }
fn setup(db: &Database) -> Store {
    let mut store=db.open();
    store.create_run("r",&configuration(true)).unwrap();
    store.create_run("other",&configuration(true)).unwrap();
    store
}

#[test]
fn one_way_task_request_refuses_before_message_task_or_call_allocation() {
    let db=Database::new(); let mut store=db.open();
    store.create_run("r",&configuration(false)).unwrap();
    let error=store.send_checked(&request("q"),"a").unwrap_err();
    assert_eq!((error.code,error.disposition),(Code::ForbiddenRoute,Disposition::Rejected));
    let snapshot=store.observe("r",true).unwrap();
    assert_eq!(snapshot["messages"],json!([])); assert_eq!(snapshot["tasks"],json!([])); assert_eq!(snapshot["calls"],json!([]));
    assert_eq!(snapshot["run"]["usage"],json!({"messages":0,"bytes":0,"calls":0}));

    let mut generic=configuration(false); generic["application"]=Value::Null;
    store.create_run("generic",&generic).unwrap();
    let mut notice=request("notice"); notice.run_id="generic".into(); notice.kind="Notice".into();
    assert_eq!(store.send_checked(&notice,"a").unwrap()["accepted"],true);
}

#[test]
fn retained_one_way_task_request_remains_an_identical_resend_after_restart() {
    let db=Database::new(); let mut store=db.open();
    store.create_run("r",&configuration(false)).unwrap();
    let message=request("legacy"); let content=message.to_value().to_string();
    // Seed the exact native-v2 history accepted before reverse-route admission
    // existed. Opening it must not rewrite or migrate its retained identities.
    let admin=Connection::open(&db.0).unwrap();
    admin.execute("INSERT INTO messages(run,id,sender,recipient,content) VALUES('r','legacy','a','b',?)",[&content]).unwrap();
    let sequence=admin.last_insert_rowid();
    admin.execute("INSERT INTO tasks(run,id,request_id) VALUES('r','legacy','legacy')",[]).unwrap();
    admin.execute("UPDATE runs SET message_count=1,byte_count=? WHERE id='r'",[content.len() as i64]).unwrap();
    drop(admin); drop(store);
    let mut store=db.open();
    assert_eq!(store.send_checked(&message,"a").unwrap(),json!({"accepted":true,"duplicate":true,"sequence":sequence}));
    assert_eq!(store.send_checked(&request("fresh"),"a").unwrap_err().code,Code::ForbiddenRoute);
    assert_eq!(store.observe("r",false).unwrap()["run"]["usage"]["messages"],1);
}

#[test]
fn checked_gateway_distinguishes_capacity_identity_and_uncertain_storage_without_diagnostics() {
    let db=Database::new(); let mut gateway=gateway(setup(&db));
    let first=call(&mut gateway,"aa",checked(&request("one")));
    assert_eq!(first["result"]["profile"],PROFILE); assert_eq!(first["result"]["duplicate"],false);
    let refused=call(&mut gateway,"aa",checked(&request("two")));
    assert_eq!(refused,json!({"ok":false,"error":{"profile":PROFILE,"code":"mailbox_full","disposition":"retryable"}}));
    let mut changed=request("one"); changed.payload["input"]=json!({"changed":true});
    assert_eq!(call(&mut gateway,"aa",checked(&changed))["error"],json!({"profile":PROFILE,"code":"identity_conflict","disposition":"rejected"}));
    assert_eq!(call(&mut gateway,"aa",json!({"command":"send","wire":changed.format().unwrap()})),json!({"ok":false,"error":{"code":"operation_rejected"}}));
    call(&mut gateway,"bb",json!({"command":"receive"})); call(&mut gateway,"bb",json!({"command":"ack","message_id":"one"}));
    let admin=Connection::open(&db.0).unwrap();
    admin.execute_batch("CREATE TRIGGER fail_insert BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'private database path and secret'); END;").unwrap();
    let unknown=call(&mut gateway,"aa",checked(&request("two")));
    assert_eq!(unknown,json!({"ok":false,"error":{"profile":PROFILE,"code":"outcome_unknown","disposition":"unknown"}}));
    assert!(!unknown.to_string().contains("private")); assert!(!unknown.to_string().contains("secret"));
}

#[test]
fn actor_scoped_reconciliation_discloses_only_own_receipt_and_absence_allows_later_commit() {
    let db=Database::new(); let mut gateway=gateway(setup(&db));
    let absent=call(&mut gateway,"aa",lookup("one"));
    assert_eq!(absent,json!({"ok":true,"result":{"profile":PROFILE,"message_id":"one","state":"absent"}}));
    call(&mut gateway,"aa",checked(&request("one")));
    let present=call(&mut gateway,"aa",lookup("one"));
    assert_eq!(present["result"],json!({"profile":PROFILE,"message_id":"one","state":"present","accepted":true,"sequence":1,"received":false,"acknowledged":false}));
    assert_eq!(call(&mut gateway,"bb",lookup("one")),absent);
    assert_eq!(call(&mut gateway,"cc",lookup("one")),absent);
    assert!(!present.to_string().contains("secret")); assert!(present["result"].get("payload").is_none());
    call(&mut gateway,"bb",json!({"command":"receive"})); call(&mut gateway,"bb",json!({"command":"ack","message_id":"one"}));
    let receipt=call(&mut gateway,"aa",lookup("one"));
    assert_eq!(receipt["result"]["received"],true); assert_eq!(receipt["result"]["acknowledged"],true);
    let mut wrong_scope=lookup("one"); wrong_scope["run_id"]=json!("other");
    assert_eq!(call(&mut gateway,"aa",wrong_scope)["error"]["code"],"invalid_message");
    let mut wrong_profile=lookup("one"); wrong_profile["profile"]=json!("mas-admission/2");
    assert_eq!(call(&mut gateway,"aa",wrong_profile)["error"]["disposition"],"rejected");
}

#[test]
fn identity_observation_does_not_receive_acknowledge_or_expire_and_validates_actor() {
    let db=Database::new(); let mut store=setup(&db); store.send_checked(&request("one"),"a").unwrap();
    let admin=Connection::open(&db.0).unwrap(); admin.execute("UPDATE runs SET deadline=0 WHERE id=?",params!["r"]).unwrap();
    let before=std::fs::read(&db.0).unwrap();
    assert_eq!(store.reconcile_message("r","a","one").unwrap()["received"],false);
    assert_eq!(store.reconcile_message("r","b","one").unwrap()["state"],"absent");
    assert_eq!(store.reconcile_message("r","unknown","one").unwrap_err().code,Code::UnknownActor);
    assert_eq!(store.reconcile_message("missing","a","one").unwrap_err().code,Code::UnknownRun);
    assert_eq!(store.reconcile_message("r","a","bad id").unwrap_err().code,Code::InvalidMessage);
    assert_eq!(before,std::fs::read(&db.0).unwrap());
    assert_eq!(store.observe("r",false).unwrap()["tasks"][0]["state"],"open");
}
