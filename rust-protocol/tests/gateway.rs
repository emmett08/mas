use mas_protocol_codec::{gateway::{Gateway, MAX_FRAME_BYTES}, store::Store};
use serde_json::{json, Value};
use std::{io::Cursor, path::PathBuf, sync::atomic::{AtomicUsize, Ordering}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
fn path() -> PathBuf {
    std::env::temp_dir().join(format!("mas-gateway-test-{}-{}.sqlite",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)))
}
fn config(global:i64, each:i64) -> Value {
    json!({"gateway_id":"test","max_attempts":global,"credentials":[
        {"credential_id":"alpha","token":"a1".repeat(32),"run_id":"r","actor":"a","max_attempts":each},
        {"credential_id":"beta","token":"b2".repeat(32),"run_id":"r","actor":"b","max_attempts":each}
    ]})
}
fn setup(path:&PathBuf) -> Store {
    let mut store = Store::open(path).unwrap();
    store.create_run("r", &json!({"agents":["a","b"],"edges":[["a","b"],["b","a"]],"max_calls":5})).unwrap();
    store.create_run("other", &json!({"agents":["a","b"]})).unwrap();
    store
}
fn request(gateway:&mut Gateway, token:&str, mut request:Value) -> Value {
    request["token"] = json!(token.repeat(32));
    gateway.handle_frame(request.to_string().as_bytes()).response
}
fn wire(id:&str,run:&str,sender:&str,recipient:&str) -> String {
    format!("message \"{id}\" run \"{run}\" from \"{sender}\" to \"{recipient}\" version \"1\" kind \"Notice\" payload {{\"text\":\"hello\"}};")
}

#[test]
fn actor_and_run_are_derived_from_capability_and_mailboxes_are_private() {
    let db = path();
    let mut gateway = Gateway::open(setup(&db), &config(30,20)).unwrap();
    for attempted in [
        json!({"command":"send","wire":wire("m","r","b","a")}),
        json!({"command":"send","wire":wire("m","other","a","b")}),
        json!({"command":"receive","actor":"b"}),
        json!({"command":"receive","run_id":"other"}),
        json!({"command":"status"})
    ] { assert_eq!(request(&mut gateway,"a1",attempted)["ok"],false); }
    assert_eq!(request(&mut gateway,"a1",json!({"command":"send","wire":wire("m","r","a","b")}))["ok"],true);
    assert_eq!(request(&mut gateway,"a1",json!({"command":"receive"}))["result"],json!([]));
    assert_eq!(request(&mut gateway,"a1",json!({"command":"ack","message_id":"m"}))["ok"],false);
    let received = request(&mut gateway,"b2",json!({"command":"receive"}));
    assert_eq!(received["result"][0]["message"]["message_id"],"m");
    assert_eq!(request(&mut gateway,"b2",json!({"command":"ack","message_id":"m"}))["ok"],true);
    let _ = std::fs::remove_file(db);
}

#[test]
fn malformed_and_unauthenticated_attempts_survive_restart_and_cap_closes() {
    let db = path();
    let configuration = config(3,3);
    let mut gateway = Gateway::open(setup(&db), &configuration).unwrap();
    assert_eq!(gateway.handle_frame(b"{").response["error"]["code"],"invalid_frame");
    assert_eq!(request(&mut gateway,"ff",json!({"command":"receive"}))["error"]["code"],"unauthorised");
    drop(gateway);
    let mut gateway = Gateway::open(Store::open(&db).unwrap(), &configuration).unwrap();
    let frame = json!({"token":"a1".repeat(32),"command":"receive"}).to_string();
    let last = gateway.handle_frame(frame.as_bytes());
    assert_eq!(last.response["ok"],true);
    assert!(last.close);
    drop(gateway);
    let mut reopened = Gateway::open(Store::open(&db).unwrap(), &configuration).unwrap();
    let exhausted = reopened.handle_frame(frame.as_bytes());
    assert!(exhausted.close);
    assert_eq!(exhausted.response["error"]["code"],"admission_closed");
    let _ = std::fs::remove_file(db);
}

#[test]
fn rejected_operations_consume_credential_quota_and_configuration_is_frozen() {
    let db = path();
    let configuration = config(10,1);
    let mut gateway = Gateway::open(setup(&db), &configuration).unwrap();
    assert_eq!(request(&mut gateway,"a1",json!({"command":"receive","actor":"b"}))["error"]["code"],"operation_rejected");
    assert_eq!(request(&mut gateway,"a1",json!({"command":"receive"}))["error"]["code"],"credential_admission_closed");
    assert_eq!(request(&mut gateway,"b2",json!({"command":"receive"}))["ok"],true);
    drop(gateway);
    let mut reordered = configuration.clone();
    reordered["credentials"].as_array_mut().unwrap().reverse();
    let mut reopened = Gateway::open(Store::open(&db).unwrap(), &reordered).unwrap();
    assert_eq!(request(&mut reopened,"a1",json!({"command":"receive"}))["error"]["code"],"credential_admission_closed");
    drop(reopened);
    let mut changed = configuration.clone();
    changed["max_attempts"] = json!(20);
    assert!(Gateway::open(Store::open(&db).unwrap(), &changed).is_err());
    changed = configuration.clone();
    changed["credentials"][0]["token"] = json!("ef".repeat(32));
    assert!(Gateway::open(Store::open(&db).unwrap(), &changed).is_err());
    let _ = std::fs::remove_file(db);
}

#[test]
fn invalid_encoding_duplicate_keys_and_oversize_are_bounded_and_metered() {
    let db = path();
    let configuration = config(4,4);
    let mut gateway = Gateway::open(setup(&db), &configuration).unwrap();
    assert_eq!(gateway.handle_frame(&[0xff]).response["error"]["code"],"invalid_frame");
    let duplicate = format!("{{\"token\":\"{}\",\"token\":\"{}\",\"command\":\"receive\"}}", "a1".repeat(32),"b2".repeat(32));
    assert_eq!(gateway.handle_frame(duplicate.as_bytes()).response["error"]["code"],"invalid_frame");
    let mut input = Cursor::new(vec![b'x'; MAX_FRAME_BYTES * 2]);
    let mut output = Vec::new();
    gateway.serve(&mut input, &mut output).unwrap();
    assert_eq!(input.position(), (MAX_FRAME_BYTES+1) as u64);
    assert_eq!(serde_json::from_slice::<Value>(&output).unwrap()["error"]["code"],"frame_too_large");
    assert!(!String::from_utf8(output).unwrap().contains(&"a1".repeat(32)));
    drop(gateway);
    let mut reopened = Gateway::open(Store::open(&db).unwrap(), &configuration).unwrap();
    assert_eq!(request(&mut reopened,"a1",json!({"command":"receive"}))["ok"],true);
    assert!(reopened.handle_frame(b"{}").close);
    let _ = std::fs::remove_file(db);
}

#[test]
fn call_reservations_remain_bound_to_the_authenticated_actor() {
    let db = path();
    let mut gateway = Gateway::open(setup(&db), &config(20,10)).unwrap();
    assert_eq!(request(&mut gateway,"a1",json!({"command":"reserve","call_id":"call1"}))["ok"],true);
    assert_eq!(request(&mut gateway,"b2",json!({"command":"finish","call_id":"call1","evidence":{"done":true}}))["ok"],false);
    assert_eq!(request(&mut gateway,"a1",json!({"command":"finish","call_id":"call1","evidence":{"done":true}}))["ok"],true);
    let _ = std::fs::remove_file(db);
}

#[test]
fn maximum_actor_bindings_remain_valid_with_large_retained_call_history() {
    let db=path();let mut store=Store::open(&db).unwrap();
    let actors=(0..32).map(|i|format!("actor-{i}")).collect::<Vec<_>>();
    store.create_run("r",&json!({"agents":actors,"edges":[],"max_calls":100})).unwrap();
    let evidence=json!({"retained":"x".repeat(50_000)});
    for index in 0..100 {
        let call=format!("call-{index}");
        store.reserve_call("r","actor-0",&call).unwrap();
        store.finish_call("r","actor-0",&call,&evidence).unwrap();
    }
    let credentials=actors.iter().enumerate().map(|(i,actor)|json!({
        "credential_id":format!("credential-{i}"),"token":format!("{i:064x}"),
        "run_id":"r","actor":actor,"max_attempts":2
    })).collect::<Vec<_>>();
    let configuration=json!({"gateway_id":"large-history","max_attempts":64,"credentials":credentials});
    let mut gateway=Gateway::open(store,&configuration).unwrap();
    for i in 0..32 {
        let frame=json!({"token":format!("{i:064x}"),"command":"receive"}).to_string();
        assert_eq!(gateway.handle_frame(frame.as_bytes()).response,json!({"ok":true,"result":[]}));
    }
    drop(gateway);
    let mut invalid_actor=configuration.clone();invalid_actor["credentials"][0]["actor"]=json!("outsider");
    assert!(Gateway::open(Store::open(&db).unwrap(),&invalid_actor).is_err());
    let mut invalid_run=configuration;invalid_run["credentials"][0]["run_id"]=json!("missing");
    assert!(Gateway::open(Store::open(&db).unwrap(),&invalid_run).is_err());
    std::fs::remove_file(db).unwrap();
}

#[test]
fn stdin_stream_flushes_one_reply_per_frame_then_stops_at_global_cap() {
    let db = path();
    let mut gateway = Gateway::open(setup(&db), &config(2,10)).unwrap();
    let frame = json!({"token":"a1".repeat(32),"command":"receive"}).to_string();
    let input = format!("{frame}\n{frame}\n{frame}\n");
    let mut reader = Cursor::new(input.as_bytes());
    let mut output = Vec::new();
    gateway.serve(&mut reader, &mut output).unwrap();
    assert_eq!(output.iter().filter(|b| **b==b'\n').count(),2);
    assert_eq!(reader.position(),((frame.len()+1)*2) as u64);
    let _ = std::fs::remove_file(db);
}

#[cfg(unix)]
#[test]
fn executable_provisions_private_unique_capabilities_and_rejects_public_credentials() {
    use std::{os::unix::fs::PermissionsExt, process::Command};
    let root = path().with_extension("dir");
    std::fs::create_dir(&root).unwrap();
    std::fs::set_permissions(&root,std::fs::Permissions::from_mode(0o700)).unwrap();
    let policy = root.join("policy.json");
    let private = root.join("config.json");
    let session = root.join("session.json");
    let mut value = config(10,5);
    for item in value["credentials"].as_array_mut().unwrap() { item.as_object_mut().unwrap().remove("token"); }
    std::fs::write(&policy,value.to_string()).unwrap();
    let binary = env!("CARGO_BIN_EXE_mas-gateway");
    let result = Command::new(binary).args(["provision",policy.to_str().unwrap(),private.to_str().unwrap()]).output().unwrap();
    assert!(result.status.success());
    let config:Value = serde_json::from_slice(&std::fs::read(&private).unwrap()).unwrap();
    let a = config["credentials"][0]["token"].as_str().unwrap();
    let b = config["credentials"][1]["token"].as_str().unwrap();
    assert_eq!(a.len(),64); assert_ne!(a,b);
    assert_eq!(std::fs::metadata(&private).unwrap().permissions().mode() & 0o077,0);
    assert!(!String::from_utf8(result.stdout).unwrap().contains(a));
    let exported = Command::new(binary).args(["export",private.to_str().unwrap(),"alpha",session.to_str().unwrap()]).output().unwrap();
    assert!(exported.status.success());
    let session_value:Value = serde_json::from_slice(&std::fs::read(&session).unwrap()).unwrap();
    assert_eq!(session_value["actor"],"a");
    assert_eq!(session_value.as_object().unwrap().len(),3);
    assert!(!Command::new(binary).args(["provision",policy.to_str().unwrap(),private.to_str().unwrap()]).output().unwrap().status.success());
    std::fs::set_permissions(&private,std::fs::Permissions::from_mode(0o644)).unwrap();
    let rejected = Command::new(binary).args(["export",private.to_str().unwrap(),"beta",root.join("other.json").to_str().unwrap()]).output().unwrap();
    assert!(!rejected.status.success());
    assert!(!String::from_utf8(rejected.stderr).unwrap().contains(a));
    let _ = std::fs::remove_dir_all(root);
}
