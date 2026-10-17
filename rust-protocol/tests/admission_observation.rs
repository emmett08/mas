//! Admission evidence and observation must not be confused with execution.
use mas_protocol_codec::{
    admission::{AdmissionCode as Code, AdmissionDisposition as Disposition},
    observation::Observer, store::Store, Message,
};
use rusqlite::Connection;
use serde_json::json;
use std::{path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Database(PathBuf);
impl Database {
    fn new() -> Self { Self(std::env::temp_dir().join(format!("mas-admission-{}-{}.sqlite",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)))) }
    fn store(&self) -> Store {
        let mut store = Store::open(&self.0).unwrap();
        store.create_run("r",&json!({"agents":["a","b","c"],"edges":[["a","b"],["b","a"]],
            "mailbox_capacity":1,"application":"agent-task/1"})).unwrap();
        store
    }
}
impl Drop for Database { fn drop(&mut self) { let _ = std::fs::remove_file(&self.0); } }
fn request(id: &str) -> Message {
    Message { message_id:id.into(),run_id:"r".into(),sender:"a".into(),recipient:"b".into(),version:"1".into(),
        kind:"Request".into(),payload:json!({"task_id":id,"operation":"investigate","input":{"secret":"evidence"}}).as_object().unwrap().clone() }
}

#[test]
fn capacity_refusal_allows_identical_retry_without_changing_identity_or_budget() {
    let db = Database::new(); let mut store = db.store();
    store.send_checked(&request("one"),"a").unwrap();
    let refused = store.send_checked(&request("two"),"a").unwrap_err();
    assert_eq!((refused.code,refused.disposition),(Code::MailboxFull,Disposition::Retryable));
    assert_eq!(refused.to_value()["code"],"mailbox_full");
    let snapshot = store.observe("r",false).unwrap();
    assert_eq!(snapshot["run"]["usage"]["messages"],1);
    assert_eq!(snapshot["tasks"].as_array().unwrap().len(),1);
    store.receive("r","b",1).unwrap(); store.ack("r","one","b").unwrap();
    assert_eq!(store.send_checked(&request("two"),"a").unwrap()["duplicate"],false);
    assert_eq!(store.send_checked(&request("two"),"a").unwrap()["duplicate"],true);
    assert_eq!(store.observe("r",false).unwrap()["run"]["usage"]["messages"],2);
}

#[test]
fn definitive_admission_errors_have_stable_codes() {
    let db = Database::new(); let mut store = db.store();
    let mut cases = Vec::new();
    let mut m = request("bad-wire"); m.message_id="has space".into(); cases.push((m,"a",Code::InvalidMessage));
    let mut m = request("unknown-run"); m.run_id="missing".into(); cases.push((m,"a",Code::UnknownRun));
    cases.push((request("unknown-actor"),"missing",Code::UnknownActor));
    cases.push((request("wrong-actor"),"b",Code::ActorMismatch));
    let mut m = request("unknown-recipient"); m.recipient="missing".into(); cases.push((m,"a",Code::UnknownRecipient));
    let mut m = request("wrong-route"); m.recipient="c".into(); cases.push((m,"a",Code::ForbiddenRoute));
    let mut m = request("wrong-version"); m.version="2".into(); cases.push((m,"a",Code::UnsupportedVersion));
    let mut m = request("wrong-transition"); m.kind="Failure".into(); cases.push((m,"a",Code::InvalidTransition));
    for (message,actor,code) in cases {
        let error = store.send_checked(&message,actor).unwrap_err();
        assert_eq!((error.code,error.disposition),(code,Disposition::Rejected));
    }
    store.send_checked(&request("same"),"a").unwrap();
    let mut changed = request("same"); changed.payload["input"] = json!({"different":true});
    assert_eq!(store.send_checked(&changed,"a").unwrap_err().code,Code::IdentityConflict);
}

#[test]
fn operational_failure_is_unknown_even_if_its_diagnostic_looks_retryable() {
    let db = Database::new(); let mut store = db.store();
    let admin = Connection::open(&db.0).unwrap();
    admin.execute_batch("CREATE TRIGGER fail_insert BEFORE INSERT ON messages BEGIN SELECT RAISE(ABORT,'mailbox full; retry after acknowledgement'); END;").unwrap();
    let error = store.send_checked(&request("failed"),"a").unwrap_err();
    assert_eq!((error.code,error.disposition),(Code::OutcomeUnknown,Disposition::Unknown));
    assert_eq!(admin.query_row("SELECT COUNT(*) FROM messages",[],|r|r.get::<_,i64>(0)).unwrap(),0);
    assert_eq!(admin.query_row("SELECT COUNT(*) FROM tasks",[],|r|r.get::<_,i64>(0)).unwrap(),0);
}

#[test]
fn observation_preserves_expired_unreconciled_state_and_redacts_payloads() {
    let db = Database::new(); let mut store = db.store();
    store.send_checked(&request("one"),"a").unwrap();
    store.reserve_call("r","b","call").unwrap();
    store.finish_call("r","b","call",&json!({"secret":"call evidence"})).unwrap();
    let admin = Connection::open(&db.0).unwrap();
    admin.execute("UPDATE runs SET deadline=0",[]).unwrap(); drop(admin);
    let before = std::fs::read(&db.0).unwrap();
    let observer = Observer::open(&db.0).unwrap();
    let redacted = observer.snapshot("r",false).unwrap();
    assert_eq!(redacted["version"],"mas-protocol-observation/1");
    assert_eq!(redacted["run"]["expired"],false);
    assert_eq!(redacted["tasks"][0]["state"],"open");
    assert_eq!(redacted["messages"][0]["received"],false);
    assert_eq!(redacted["messages"][0]["acked"],false);
    assert!(redacted["messages"][0].get("payload").is_none());
    assert!(redacted["calls"][0].get("evidence").is_none());
    assert!(!redacted.to_string().contains("secret"));
    let full = observer.snapshot("r",true).unwrap();
    assert_eq!(full["messages"][0]["payload"]["input"]["secret"],"evidence");
    assert_eq!(full["calls"][0]["evidence"]["secret"],"call evidence");
    assert_eq!(before,std::fs::read(&db.0).unwrap());
    let refused = store.send_checked(&request("late"),"a").unwrap_err();
    assert_eq!((refused.code,refused.disposition),(Code::DeadlineExceeded,Disposition::Rejected));
    assert_eq!(observer.snapshot("r",false).unwrap()["tasks"][0]["state"],"expired_outcome_unknown");
}

#[test]
fn observer_never_creates_migrates_or_accepts_tampered_envelopes() {
    let db = Database::new();
    assert!(Observer::open(&db.0).is_err()); assert!(!db.0.exists());
    let admin = Connection::open(&db.0).unwrap();
    admin.execute_batch("PRAGMA application_id=1296126769; PRAGMA user_version=1; CREATE TABLE legacy(value TEXT);").unwrap();
    drop(admin); let before=std::fs::read(&db.0).unwrap();
    assert!(Observer::open(&db.0).is_err()); assert_eq!(before,std::fs::read(&db.0).unwrap());
    std::fs::remove_file(&db.0).unwrap();
    let mut store=db.store(); store.send_checked(&request("one"),"a").unwrap();
    let admin=Connection::open(&db.0).unwrap(); admin.execute("UPDATE messages SET sender='c'",[]).unwrap();
    assert!(Observer::open(&db.0).unwrap().snapshot("r",false).unwrap_err().contains("columns differ"));
}

#[test]
fn legacy_repository_implementers_keep_conservative_defaults() {
    use mas_protocol_codec::repository::{ProtocolRepository, RepositoryResult};
    use mas_protocol_codec::store::RunMembership;
    use serde_json::Value;
    // Implement exactly the original trait surface. This is also a compile-time
    // check that downstream repositories need not claim new guarantees.
    struct Legacy;
    impl ProtocolRepository for Legacy {
        fn register_admission(&mut self,_:&str,_:&str,_:i64)->RepositoryResult<()> { unimplemented!() }
        fn reserve_admission(&mut self,_:&str)->RepositoryResult<i64> { unimplemented!() }
        fn reconcile(&self,_:&str)->RepositoryResult<Value> { unimplemented!() }
        fn run_membership(&self,_:&str)->RepositoryResult<RunMembership> { unimplemented!() }
        fn create_run(&mut self,_:&str,_:&Value)->RepositoryResult<Value> { unimplemented!() }
        fn send(&mut self,_:&Message,_:&str)->RepositoryResult<Value> { Err("mailbox full; retry after acknowledgement".into()) }
        fn receive(&mut self,_:&str,_:&str,_:usize)->RepositoryResult<Value> { unimplemented!() }
        fn ack(&mut self,_:&str,_:&str,_:&str)->RepositoryResult<Value> { unimplemented!() }
        fn reserve_call(&mut self,_:&str,_:&str,_:&str)->RepositoryResult<Value> { unimplemented!() }
        fn finish_call(&mut self,_:&str,_:&str,_:&str,_:&Value)->RepositoryResult<Value> { unimplemented!() }
        fn inspect(&self,_:&str)->RepositoryResult<Value> { panic!("observation must not use mutating inspect") }
    }
    let mut legacy=Legacy;
    assert_eq!(legacy.send_checked(&request("one"),"a").unwrap_err().disposition,Disposition::Unknown);
    assert!(legacy.observe("r",false).unwrap_err().contains("not supported"));
    assert_eq!(legacy.reconcile_message("r","a","one").unwrap_err().disposition,Disposition::Unknown);
}
