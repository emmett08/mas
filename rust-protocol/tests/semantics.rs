//! Independent public-interface regressions for admission and task correlation.
use mas_protocol_codec::{store::Store, Message};
use serde_json::{json, Value};
use std::{path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Temporary(PathBuf);
impl Temporary {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("mas-semantic-review-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temporary {
    fn drop(&mut self) { let _ = std::fs::remove_dir_all(&self.0); }
}
fn message(id: &str, run: &str, from: &str, to: &str, kind: &str, payload: Value) -> Message {
    Message { message_id:id.into(), run_id:run.into(), sender:from.into(), recipient:to.into(),
        version:"1".into(), kind:kind.into(), payload:payload.as_object().unwrap().clone() }
}
fn rejected_without_spending(store: &mut Store, m: &Message, actor: &str, expected: &str) {
    let before = store.inspect(&m.run_id).unwrap()["usage"].clone();
    let error = store.send(m, actor).unwrap_err();
    assert!(error.contains(expected), "expected {expected:?}, got {error:?}");
    assert_eq!(store.inspect(&m.run_id).unwrap()["usage"], before);
}

#[test]
fn ownership_version_and_run_boundaries_survive_acknowledgement() {
    let tmp = Temporary::new(); let path = tmp.0.join("state.sqlite");
    let mut store = Store::open(&path).unwrap();
    let config = json!({"agents":["a","b","c"],"edges":[["a","b"],["b","a"]]});
    for run in ["first","second"] { store.create_run(run, &config).unwrap(); }
    let m = message("same-id","first","a","b","Notice",json!({"value":7}));
    rejected_without_spending(&mut store, &m, "c", "sender differs");
    let mut wrong_version = m.clone(); wrong_version.version = "2".into();
    rejected_without_spending(&mut store, &wrong_version, "a", "incompatible protocol");
    store.send(&m,"a").unwrap();
    assert!(store.ack("first","same-id","a").unwrap_err().contains("not addressed"));
    assert!(store.ack("first","same-id","b").unwrap_err().contains("not been received"));
    assert_eq!(store.receive("first","c",10).unwrap(), json!([]));
    store.receive("first","b",1).unwrap(); store.ack("first","same-id","b").unwrap();
    drop(store);
    let mut store = Store::open(&path).unwrap();
    let mut changed = m.clone(); changed.payload.insert("value".into(),json!(8));
    rejected_without_spending(&mut store,&changed,"a","different content");
    assert_eq!(store.send(&m,"a").unwrap()["duplicate"],true);
    let mut other = m.clone(); other.run_id="second".into();
    assert_eq!(store.send(&other,"a").unwrap()["duplicate"],false);
    assert_eq!(store.receive("first","b",10).unwrap(),json!([]));
    assert_eq!(store.receive("second","b",10).unwrap()[0]["message"]["run_id"],"second");
}

#[test]
fn task_correlation_rejects_wrong_runs_participants_and_terminal_reuse() {
    let tmp=Temporary::new(); let mut store=Store::open(tmp.0.join("state.sqlite")).unwrap();
    let config=json!({"agents":["a","b","c"],"edges":[["a","b"],["b","a"],["c","a"]],"application":"agent-task/1"});
    for run in ["first","second"] {store.create_run(run,&config).unwrap();}
    let request=message("q","first","a","b","Request",json!({"task_id":"t","operation":"check","input":{}}));
    store.send(&request,"a").unwrap();
    let response=message("z","first","b","a","Result",json!({"task_id":"t","reply_to":"q","status":"unresolved","output":{}}));
    rejected_without_spending(&mut store,&response,"b","received");
    store.receive("first","b",1).unwrap();
    let mut stale=response.clone();stale.payload.insert("reply_to".into(),json!("old"));
    rejected_without_spending(&mut store,&stale,"b","exact request");
    let mut other=response.clone();other.run_id="second".into();
    rejected_without_spending(&mut store,&other,"b","unknown task");
    let mut impostor=response.clone();impostor.sender="c".into();
    rejected_without_spending(&mut store,&impostor,"c","wrong participants");
    let mut reused=request.clone();reused.message_id="q2".into();
    rejected_without_spending(&mut store,&reused,"a","task identity already used");
    for (key,value) in [("status",json!("proved")),("output",json!([])),("extra",json!(1))] {
        let mut malformed=response.clone();malformed.payload.insert(key.into(),value);
        rejected_without_spending(&mut store,&malformed,"b",if key=="extra"{"payload fields"}else{"completed/unresolved"});
    }
    store.send(&response,"b").unwrap();
    let cancel=message("cancel","first","a","b","Cancel",json!({"task_id":"t","reply_to":"q","reason":"stop"}));
    rejected_without_spending(&mut store,&cancel,"a","terminal");
    assert_eq!(store.send(&response,"b").unwrap()["duplicate"],true);
    assert_eq!(store.inspect("first").unwrap()["usage"]["messages"],2);
}

#[test]
fn byte_and_call_limits_preserve_state_and_actor_ownership() {
    let tmp=Temporary::new();let mut store=Store::open(tmp.0.join("state.sqlite")).unwrap();
    store.create_run("small",&json!({"agents":["a","b"],"max_bytes":1,"max_calls":1})).unwrap();
    let m=message("m","small","a","b","Notice",json!({}));
    rejected_without_spending(&mut store,&m,"a","communication budget");
    store.reserve_call("small","a","call").unwrap();
    assert!(store.finish_call("small","b","call",&json!({})).unwrap_err().contains("another worker"));
    assert!(store.reserve_call("small","b","second").unwrap_err().contains("call budget"));
    assert_eq!(store.inspect("small").unwrap()["calls"][0]["state"],"reserved");
    store.finish_call("small","a","call",&json!({"result":"unresolved"})).unwrap();
    assert_eq!(store.inspect("small").unwrap()["usage"]["calls"],1);
}
