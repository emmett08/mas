//! Independent regressions for expiry and the capability admission boundary.
use mas_protocol_codec::{gateway::Gateway, store::Store, Message};
use rusqlite::Connection;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::{Arc, Barrier, atomic::{AtomicUsize, Ordering}}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("mas-assumptions-review-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap(); Self(path)
    }
    fn database(&self) -> PathBuf { self.0.join("state.sqlite") }
}
impl Drop for Temporary { fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); } }
fn setup(tmp: &Temporary, capacity: i64, max_messages: i64) -> Store {
    let mut store = Store::open(tmp.database()).unwrap();
    store.create_run("r", &json!({"agents":["a","b"],"edges":[["a","b"],["b","a"]],
        "application":"agent-task/1","mailbox_capacity":capacity,"max_messages":max_messages})).unwrap();
    store
}
fn request(id: &str, task: &str) -> Message {
    Message { message_id:id.into(),run_id:"r".into(),sender:"a".into(),recipient:"b".into(),
        version:"1".into(),kind:"Request".into(),payload:json!({"task_id":task,"operation":"check","input":{}}).as_object().unwrap().clone() }
}
fn reply() -> Message {
    Message { message_id:"answer".into(),run_id:"r".into(),sender:"b".into(),recipient:"a".into(),
        version:"1".into(),kind:"Result".into(),payload:json!({"task_id":"t","reply_to":"q","status":"completed","output":{}}).as_object().unwrap().clone() }
}

#[test]
fn rejected_late_work_latches_expiry_without_capacity_and_delivery_is_not_actionable() {
    let tmp=Temporary::new();let mut store=setup(&tmp,1,1);
    let q=request("q","t");store.send(&q,"a").unwrap();
    let before=store.inspect("r").unwrap();let original_deadline=before["deadline"].as_f64().unwrap();
    let control=Connection::open(tmp.database()).unwrap();
    // Deterministic clock-condition injection: make the deadline due, then
    // restore a future condition. This tests the persisted latch, not OS clocks.
    control.execute("UPDATE runs SET deadline=0 WHERE id='r'",[]).unwrap();
    assert!(store.send(&request("new","other"),"a").is_err());
    control.execute("UPDATE runs SET deadline=? WHERE id='r'",[original_deadline]).unwrap();
    drop(store);let mut store=Store::open(tmp.database()).unwrap();
    let after=store.inspect("r").unwrap();
    assert_eq!(after["expired"],true);
    assert_eq!(after["tasks"][0]["state"],"expired_outcome_unknown");
    assert_eq!(after["tasks"][0]["terminal_message_id"],Value::Null);
    assert_eq!(after["usage"],before["usage"]);
    assert!(store.reserve_call("r","b","late-call").is_err());
    let delivered=store.receive("r","b",1).unwrap();
    assert_eq!(delivered[0]["actionable"],false);
    assert_eq!(delivered[0]["task_state"],"expired_outcome_unknown");
    assert_eq!(delivered[0]["message"],q.to_value());
    store.ack("r","q","b").unwrap();
    assert_eq!(store.receive("r","b",1).unwrap(),json!([]));
    assert_eq!(store.send(&q,"a").unwrap()["duplicate"],true);
    assert_eq!(store.inspect("r").unwrap()["tasks"][0]["state"],"expired_outcome_unknown");
}

#[test]
fn reconciliation_preserves_the_first_terminal_and_only_closes_open_tasks() {
    let tmp=Temporary::new();let mut store=setup(&tmp,3,10);
    store.send(&request("q","t"),"a").unwrap();store.receive("r","b",1).unwrap();
    store.send(&reply(),"b").unwrap();
    store.send(&request("waiting","waiting"),"a").unwrap();
    Connection::open(tmp.database()).unwrap().execute("UPDATE runs SET deadline=0 WHERE id='r'",[]).unwrap();
    assert_eq!(store.reconcile("r").unwrap()["closed_tasks"],1);
    assert_eq!(store.reconcile("r").unwrap()["closed_tasks"],0);
    let tasks=store.inspect("r").unwrap()["tasks"].clone();
    assert_eq!(tasks[0]["state"],"completed");assert_eq!(tasks[0]["terminal_message_id"],"answer");
    assert_eq!(tasks[1]["state"],"expired_outcome_unknown");
    for delivery in store.receive("r","b",10).unwrap().as_array().unwrap() {
        assert_eq!(delivery["actionable"],false);
    }
}

fn policy(global: i64, actor: i64) -> Value {
    json!({"gateway_id":"review","max_attempts":global,"credentials":[
        {"credential_id":"alice","token":"11".repeat(32),"run_id":"r","actor":"a","max_attempts":actor},
        {"credential_id":"bob","token":"22".repeat(32),"run_id":"r","actor":"b","max_attempts":actor}]})
}
fn frame(gateway: &mut Gateway, request: Value) -> Value {
    gateway.handle_frame(request.to_string().as_bytes()).response
}

#[test]
fn forged_identity_and_malformed_attempts_remain_charged_across_gateway_restarts() {
    let tmp=Temporary::new();let store=setup(&tmp,2,10);let config=policy(6,3);
    let mut gateway=Gateway::open(store,&config).unwrap();
    assert_eq!(gateway.handle_frame(b"{").response["error"]["code"],"invalid_frame");
    let mut forged=request("q","t");forged.sender="b".into();forged.recipient="a".into();
    assert_eq!(frame(&mut gateway,json!({"token":"11".repeat(32),"command":"send","wire":forged.format().unwrap()}))["ok"],false);
    assert_eq!(frame(&mut gateway,json!({"token":"11".repeat(32),"command":"receive","actor":"b"}))["ok"],false);
    drop(gateway);
    let mut gateway=Gateway::open(Store::open(tmp.database()).unwrap(),&config).unwrap();
    assert_eq!(frame(&mut gateway,json!({"token":"11".repeat(32),"command":"receive"}))["ok"],true);
    // Alice's fourth authenticated attempt fails; Bob still has capacity.
    assert_eq!(frame(&mut gateway,json!({"token":"11".repeat(32),"command":"receive"}))["error"]["code"],"credential_admission_closed");
    let last=gateway.handle_frame(json!({"token":"22".repeat(32),"command":"receive"}).to_string().as_bytes());
    assert_eq!(last.response["ok"],true);assert!(last.close);
    drop(gateway);
    let mut gateway=Gateway::open(Store::open(tmp.database()).unwrap(),&config).unwrap();
    assert_eq!(frame(&mut gateway,json!({"token":"22".repeat(32),"command":"receive"}))["error"]["code"],"admission_closed");
    assert_eq!(Store::open(tmp.database()).unwrap().inspect("r").unwrap()["usage"]["messages"],0);
}

#[test]
fn concurrent_gateway_instances_share_one_durable_global_cap() {
    let tmp=Temporary::new();let store=setup(&tmp,2,10);let config=policy(1,1);
    let first=Gateway::open(store,&config).unwrap();
    let second=Gateway::open(Store::open(tmp.database()).unwrap(),&config).unwrap();
    let barrier=Arc::new(Barrier::new(3));
    let threads=[first,second].into_iter().map(|mut gateway| {
        let barrier=barrier.clone();std::thread::spawn(move || {
            barrier.wait();frame(&mut gateway,json!({"token":"11".repeat(32),"command":"receive"}))
        })
    }).collect::<Vec<_>>();
    barrier.wait();
    let results=threads.into_iter().map(|t|t.join().unwrap()).collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|v|v["ok"]==true).count(),1);
    assert_eq!(results.iter().filter(|v|v["error"]["code"]=="admission_closed").count(),1);
}
