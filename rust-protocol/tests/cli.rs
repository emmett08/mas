use std::{fs, path::PathBuf, process::Command, sync::atomic::{AtomicUsize,Ordering}};
use serde_json::{json,Value};
static SERIAL:AtomicUsize=AtomicUsize::new(0);
fn directory()->PathBuf {
    let p=std::env::temp_dir().join(format!("mas-cli-{}-{}",std::process::id(),SERIAL.fetch_add(1,Ordering::SeqCst)));
    fs::create_dir_all(&p).unwrap(); p
}
fn call(args:&[&str])->(bool,Value) {
    let out=Command::new(env!("CARGO_BIN_EXE_mas")).args(args).output().unwrap();
    (out.status.success(),serde_json::from_slice(&out.stdout).expect("CLI always emits JSON"))
}
#[test]
fn explicit_topology_never_fails_open() {
    let p=directory();let db=p.join("state.sqlite");let config=p.join("spec.json");
    for invalid in ["[]","{}",r#"{"agents":["a","b"]}"#,r#"{"agents":["a","b"],"edges":null,"mailbox_capacity":2,"application":"agent-task/1"}"#,
      r#"{"agents":["a","b"],"edges":[],"mailbox_capacity":2,"application":"agent-task/1","mailbox_capacity":3}"#] {
        fs::write(&config,invalid).unwrap();
        let (ok,value)=call(&["init",db.to_str().unwrap(),"r",config.to_str().unwrap()]);
        assert!(!ok);assert_eq!(value["ok"],false);
    }
    fs::remove_dir_all(p).unwrap();
}
#[test]
fn wire_cli_roundtrip_and_restart() {
    let p=directory();let db=p.join("state.sqlite");let config=p.join("spec.json");let file=p.join("message.txt");
    fs::write(&config,json!({"agents":["a","b"],"edges":[["a","b"],["b","a"]],"mailbox_capacity":1,"application":"agent-task/1"}).to_string()).unwrap();
    assert!(call(&["init",db.to_str().unwrap(),"r",config.to_str().unwrap()]).0);
    fs::write(&file,"message \"q1\" run \"r\" from \"a\" to \"b\" version \"1\" kind \"Request\" payload {\"task_id\":\"t\",\"operation\":\"sum\",\"input\":{\"values\":[2,3]}};").unwrap();
    assert!(call(&["send",db.to_str().unwrap(),"a",file.to_str().unwrap()]).0);
    assert!(!call(&["ack",db.to_str().unwrap(),"r","q1","b"]).0);
    assert!(call(&["receive",db.to_str().unwrap(),"r","b"]).0);
    assert_eq!(call(&["receive",db.to_str().unwrap(),"r","b"]).1["result"][0]["redelivered"],true);
    assert!(call(&["ack",db.to_str().unwrap(),"r","q1","b"]).0);
    assert_eq!(call(&["receive",db.to_str().unwrap(),"r","b"]).1["result"],json!([]));
    assert_eq!(call(&["send",db.to_str().unwrap(),"a",file.to_str().unwrap()]).1["result"]["duplicate"],true);
    fs::remove_dir_all(p).unwrap();
}
#[test]
fn cli_errors_are_structured() {
    for args in [vec!["init"],vec!["unknown"],vec!["status","relative.sqlite","r"],vec!["receive","/tmp/unused-mas.sqlite","r","a","not-a-number"]] {
        assert!(!call(&args).0);
    }
}
