use mas_protocol_codec::{gateway::Gateway, host_ipc::{HostSession, MAX_HEADER_BYTES}, store::Store};
use serde_json::{json, Value};
use std::{io::{BufRead, Cursor, Read}, path::PathBuf,
          sync::atomic::{AtomicUsize, Ordering}, time::{SystemTime, UNIX_EPOCH}};

static NEXT: AtomicUsize = AtomicUsize::new(0);
fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as u64 }
fn database() -> PathBuf {
    let path = std::env::temp_dir().join(format!("mas-host-ipc-{}-{}.sqlite", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    Store::open(&path).unwrap().create_run("r", &json!({"agents":["a","b"],"max_calls":5})).unwrap();
    path
}
fn config() -> Value {
    json!({"gateway_id":"host","max_attempts":100,"credentials":[
        {"credential_id":"a","token":"ab".repeat(32),"run_id":"r","actor":"a","max_attempts":100}
    ]})
}
fn frame(id: u64, operation: &str, body: &str, deadline: u64) -> Vec<u8> {
    let mut bytes = format!("{}\n", json!({"version":1,"request_id":id,"operation":operation,
        "body_bytes":body.len(),"deadline_ms":deadline})).into_bytes();
    bytes.extend_from_slice(body.as_bytes());
    bytes
}
fn replies(bytes: Vec<u8>) -> Vec<Value> {
    let mut cursor = Cursor::new(bytes);
    let mut line = String::new();
    cursor.read_line(&mut line).unwrap();
    assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["version"],1);
    let mut values = Vec::new();
    loop {
        line.clear();
        if cursor.read_line(&mut line).unwrap() == 0 { break; }
        let header: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(header["request_id"], values.len() + 1);
        let mut body = vec![0; header["body_bytes"].as_u64().unwrap() as usize];
        cursor.read_exact(&mut body).unwrap();
        values.push(serde_json::from_slice(&body).unwrap());
    }
    values
}
fn admissions(path: &PathBuf) -> i64 {
    rusqlite::Connection::open(path).unwrap().query_row("SELECT attempts FROM admissions WHERE scope='gateway/host'", [], |row| row.get(0)).unwrap()
}

#[test]
fn persistent_codec_gateway_and_status_match_direct_contract() {
    let path = database();
    let mut session = HostSession::open(&path, &config(), "r").unwrap();
    let envelope = json!({"message_id":"m","run_id":"r","sender":"a","recipient":"b",
        "version":"1","kind":"Notice","payload":{"text":"🧠\nquoted \"data\""}}).to_string();
    let wire = mas_protocol_codec::encode_message(&envelope, 65_536,32).unwrap();
    let command = json!({"token":"ab".repeat(32),"command":"send","wire":wire}).to_string();
    let mut input = frame(1,"encode",&envelope,now()+10_000);
    input.extend(frame(2,"gateway",&command,now()+10_000));
    input.extend(frame(3,"status","",now()+10_000));
    input.extend(frame(4,"close","",now()+10_000));
    let mut output = Vec::new();
    session.serve(&mut Cursor::new(input),&mut output).unwrap();
    let results = replies(output);
    assert_eq!(results[0]["result"]["text"],wire);
    let direct_path = database();
    let direct = Gateway::open(Store::open(&direct_path).unwrap(), &config()).unwrap().handle_frame(command.as_bytes()).response;
    assert_eq!(results[1],direct);
    let snapshot = Store::open(&path).unwrap().inspect("r").unwrap();
    assert_eq!(results[2]["result"],snapshot);
    assert_eq!(results[3]["result"]["closed"],true);
    assert_eq!(admissions(&path),1);
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(direct_path);
}

#[test]
fn persistent_gateway_preserves_versioned_admission_and_private_identity_observation() {
    let path=database(); let mut session=HostSession::open(&path,&config(),"r").unwrap();
    let wire="message \"checked\" run \"r\" from \"a\" to \"b\" version \"1\" kind \"Notice\" payload {\"private\":\"secret\"};";
    let profile=mas_protocol_codec::admission::PROFILE;
    let send=json!({"token":"ab".repeat(32),"command":"send_checked","profile":profile,"wire":wire}).to_string();
    let observe=json!({"token":"ab".repeat(32),"command":"reconcile_message","profile":profile,"message_id":"checked"}).to_string();
    let mut input=frame(1,"gateway",&send,now()+10_000);
    input.extend(frame(2,"gateway",&observe,now()+10_000));
    input.extend(frame(3,"close","",now()+10_000));
    let mut output=Vec::new(); session.serve(&mut Cursor::new(input),&mut output).unwrap();
    let results=replies(output);
    assert_eq!(results[0]["result"],json!({"profile":profile,"accepted":true,"duplicate":false,"sequence":1}));
    assert_eq!(results[1]["result"],json!({"profile":profile,"message_id":"checked","state":"present","accepted":true,"sequence":1,"received":false,"acknowledged":false}));
    assert!(!results[1].to_string().contains("secret"));
    assert_eq!(admissions(&path),2);
    let _=std::fs::remove_file(path);
}

#[test]
fn incomplete_or_malformed_ipc_never_reaches_admission() {
    let command = json!({"token":"ab".repeat(32),"command":"reserve","call_id":"c"}).to_string();
    let mut truncated = frame(1,"gateway",&command,now()+10_000);
    truncated.pop();
    let invalids = vec![truncated, b"{\"version\":1,\"version\":1}\n".to_vec(),
        vec![b'x';MAX_HEADER_BYTES+1], b"{\"version\":1".to_vec()];
    for bytes in invalids {
        let path = database();
        let mut session = HostSession::open(&path,&config(),"r").unwrap();
        assert!(session.serve(&mut Cursor::new(bytes),&mut Vec::new()).is_err());
        assert_eq!(admissions(&path),0);
        assert_eq!(Store::open(&path).unwrap().inspect("r").unwrap()["calls"],json!([]));
        let _ = std::fs::remove_file(path);
    }
}

#[test]
fn expired_ipc_has_no_admission_and_bad_gateway_body_has_original_charge() {
    let path = database();
    let mut session = HostSession::open(&path,&config(),"r").unwrap();
    let mut input = frame(1,"gateway","{}",1);
    input.extend(frame(2,"gateway","{",now()+10_000));
    let mut output = Vec::new();
    session.serve(&mut Cursor::new(input),&mut output).unwrap();
    let results = replies(output);
    assert_eq!(results[0]["error"]["code"],"ipc_deadline");
    assert_eq!(results[1]["error"]["code"],"invalid_frame");
    assert_eq!(admissions(&path),1);
    let _ = std::fs::remove_file(path);
}

#[test]
fn reusing_request_sequence_is_rejected_without_redispatch() {
    let path = database();
    let mut session = HostSession::open(&path,&config(),"r").unwrap();
    let mut input = frame(1,"gateway","{}",now()+10_000);
    input.extend(frame(1,"gateway","{}",now()+10_000));
    assert!(session.serve(&mut Cursor::new(input),&mut Vec::new()).is_err());
    assert_eq!(admissions(&path),1);
    let _ = std::fs::remove_file(path);
}

#[test]
fn response_write_failure_does_not_reverse_committed_identity() {
    struct LoseReply { writes: usize }
    impl std::io::Write for LoseReply {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> { Ok(bytes.len()) }
        fn flush(&mut self) -> std::io::Result<()> {
            self.writes+=1;
            if self.writes>1 { Err(std::io::Error::from(std::io::ErrorKind::BrokenPipe)) } else { Ok(()) }
        }
    }
    let path=database();
    let mut session=HostSession::open(&path,&config(),"r").unwrap();
    let command=json!({"token":"ab".repeat(32),"command":"reserve","call_id":"c"}).to_string();
    assert!(session.serve(&mut Cursor::new(frame(1,"gateway",&command,now()+10_000)),
        &mut LoseReply {writes:0}).is_err());
    assert_eq!(Store::open(&path).unwrap().inspect("r").unwrap()["calls"][0]["id"],"c");
    assert_eq!(admissions(&path),1);
    let _=std::fs::remove_file(path);
}
