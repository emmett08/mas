#![cfg(feature = "postgres-repository")]
//! Real backend tests, deliberately opt-in: no mock can qualify PostgreSQL.
use mas_protocol_codec::{Message, gateway::Gateway, repository::ProtocolRepository,
    postgres_repository::{AuthorityBinding,PostgresRepository}, store::Store};
use postgres::{Client,NoTls};
use serde_json::{json,Value};
use std::sync::{Arc,Barrier,atomic::{AtomicU64,Ordering}};

static NEXT:AtomicU64=AtomicU64::new(0);
struct Fixture {dsn:String,schema:String,binding:AuthorityBinding}
impl Fixture {
    fn new()->Self {
        let dsn=std::env::var("MAS_TEST_POSTGRES_DSN").expect("real PostgreSQL test connection required");
        let schema=format!("mas_test_{}_{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed));
        let binding=AuthorityBinding {deployment:"test-deployment".into(),controller:"controller-one".into(),epoch:1};
        PostgresRepository::initialise(Client::connect(&dsn,NoTls).unwrap(),&schema,binding.clone()).unwrap();
        Self {dsn,schema,binding}
    }
    fn open(&self)->PostgresRepository {self.open_as(self.binding.clone())}
    fn open_as(&self,binding:AuthorityBinding)->PostgresRepository {
        PostgresRepository::open(Client::connect(&self.dsn,NoTls).unwrap(),&self.schema,binding).unwrap()
    }
    fn admin(&self)->Client {
        let mut client=Client::connect(&self.dsn,NoTls).unwrap();
        client.batch_execute(&format!("SET search_path TO {},pg_catalog",self.schema)).unwrap();client
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {if let Ok(mut c)=Client::connect(&self.dsn,NoTls) {let _=c.batch_execute(&format!("DROP SCHEMA {} CASCADE",self.schema));}}
}
fn message(id:&str,from:&str,to:&str,kind:&str,payload:Value)->Message {
    Message {message_id:id.into(),run_id:"r".into(),sender:from.into(),recipient:to.into(),version:"1".into(),kind:kind.into(),payload:payload.as_object().unwrap().clone()}
}
fn request()->Message {message("q","a","b","Request",json!({"task_id":"task","operation":"check","input":{}}))}
fn result()->Message {message("answer","b","a","Result",json!({"task_id":"task","reply_to":"q","status":"completed","output":{"answer":42}}))}
fn configuration()->Value {json!({"agents":["a","b"],"edges":[["a","b"],["b","a"]],"application":"agent-task/1","max_calls":2,"mailbox_capacity":2})}
fn trace<R:ProtocolRepository>(repository:&mut R)->Vec<Value> {
    let mut output=Vec::new();
    repository.create_run("r",&configuration()).unwrap();
    repository.register_admission("gateway/test",&"a".repeat(64),2).unwrap();
    output.push(json!(repository.reserve_admission("gateway/test").unwrap()));
    output.push(repository.send(&request(),"a").unwrap());
    output.push(repository.send(&request(),"a").unwrap());
    assert!(repository.ack("r","q","b").is_err());
    output.push(repository.receive("r","b",2).unwrap());
    output.push(repository.receive("r","b",2).unwrap());
    output.push(repository.ack("r","q","b").unwrap());
    output.push(repository.reserve_call("r","b","call").unwrap());
    assert!(repository.reserve_call("r","b","call").is_err());
    assert!(repository.finish_call("r","a","call",&json!({"status":"ok"})).is_err());
    output.push(repository.finish_call("r","b","call",&json!({"status":"ok"})).unwrap());
    output.push(repository.send(&result(),"b").unwrap());
    let mut inspection=repository.inspect("r").unwrap();
    inspection.as_object_mut().unwrap().remove("deadline");
    for task in inspection["tasks"].as_array_mut().unwrap() {task.as_object_mut().unwrap().remove("closed_at");}
    output.push(inspection);output
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn sqlite_postgres_complete_protocol_trace_agrees() {
    let fixture=Fixture::new();
    let path=std::env::temp_dir().join(format!("mas-pg-conformance-{}.sqlite",std::process::id()));
    let mut local=Store::open(&path).unwrap();
    assert_eq!(trace(&mut local),trace(&mut fixture.open()));
    drop(local);std::fs::remove_file(path).unwrap();
    assert_eq!(fixture.open().inspect("r").unwrap()["calls"][0]["state"],"recorded");
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn concurrent_connections_cannot_over_allocate_or_reuse_identity() {
    let fixture=Arc::new(Fixture::new());fixture.open().create_run("r",&configuration()).unwrap();
    let barrier=Arc::new(Barrier::new(9));
    let threads=(0..8).map(|n| {let f=fixture.clone();let b=barrier.clone();std::thread::spawn(move|| {
        let mut repo=f.open();b.wait();repo.reserve_call("r","a",&format!("call-{n}")).is_ok()
    })}).collect::<Vec<_>>();
    barrier.wait();
    assert_eq!(threads.into_iter().map(|t|usize::from(t.join().unwrap())).sum::<usize>(),2);
    let state=fixture.open().inspect("r").unwrap();assert_eq!(state["usage"]["calls"],2);assert_eq!(state["calls"].as_array().unwrap().len(),2);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn handover_fences_every_old_handle_and_retains_original_effects() {
    let fixture=Fixture::new();let mut old=fixture.open();
    old.create_run("r",&configuration()).unwrap();old.register_admission("gateway/test",&"a".repeat(64),3).unwrap();
    old.reserve_call("r","a","escaped").unwrap();old.send(&request(),"a").unwrap();
    let next=fixture.open().handover("controller-two").unwrap();
    assert_eq!(next.epoch,2);
    assert!(old.reserve_admission("gateway/test").unwrap_err().contains("stale"));
    assert!(old.reserve_call("r","a","new").is_err());assert!(old.finish_call("r","a","escaped",&json!({})).is_err());
    assert!(old.receive("r","b",1).is_err());assert!(old.send(&request(),"a").is_err());assert!(old.inspect("r").is_err());
    let mut current=fixture.open_as(next);
    assert_eq!(current.inspect("r").unwrap()["usage"]["calls"],1);
    assert!(current.reserve_call("r","a","escaped").is_err());
    current.finish_call("r","a","escaped",&json!({"observed":"completed"})).unwrap();
    assert_eq!(current.inspect("r").unwrap()["calls"][0]["state"],"recorded");
    assert!(PostgresRepository::initialise(Client::connect(&fixture.dsn,NoTls).unwrap(),&fixture.schema,fixture.binding.clone()).is_err());
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn task_message_and_quota_commit_atomically_and_schema_drift_fails_closed() {
    let fixture=Fixture::new();let mut repo=fixture.open();repo.create_run("r",&configuration()).unwrap();
    fixture.admin().batch_execute("CREATE FUNCTION fail_task() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected task insertion failure'; END $$; CREATE TRIGGER fail_task BEFORE INSERT ON tasks FOR EACH ROW EXECUTE FUNCTION fail_task();").unwrap();
    assert!(repo.send(&request(),"a").is_err());
    let state=repo.inspect("r").unwrap();assert_eq!(state["usage"]["messages"],0);assert_eq!(state["messages"],json!([]));assert_eq!(state["tasks"],json!([]));
    assert!(PostgresRepository::open(Client::connect(&fixture.dsn,NoTls).unwrap(),&fixture.schema,fixture.binding.clone()).is_err());
    fixture.admin().batch_execute("DROP TRIGGER fail_task ON tasks; DROP FUNCTION fail_task();").unwrap();
    repo.send(&request(),"a").unwrap();assert_eq!(repo.inspect("r").unwrap()["tasks"].as_array().unwrap().len(),1);
    fixture.admin().batch_execute("ALTER TABLE calls DROP CONSTRAINT calls_state_check").unwrap();
    assert!(PostgresRepository::open(Client::connect(&fixture.dsn,NoTls).unwrap(),&fixture.schema,fixture.binding.clone()).is_err());
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn concurrent_terminal_outcomes_leave_one_native_transition() {
    let fixture=Arc::new(Fixture::new());let mut repo=fixture.open();repo.create_run("r",&configuration()).unwrap();repo.send(&request(),"a").unwrap();repo.receive("r","b",1).unwrap();
    let cancel=message("cancel","a","b","Cancel",json!({"task_id":"task","reply_to":"q","reason":"stop"}));
    let barrier=Arc::new(Barrier::new(3));
    let threads=[(result(),"b"),(cancel,"a")].into_iter().map(|(m,a)| {let f=fixture.clone();let b=barrier.clone();std::thread::spawn(move||{let mut repo=f.open();b.wait();repo.send(&m,a).is_ok()})}).collect::<Vec<_>>();
    barrier.wait();assert_eq!(threads.into_iter().map(|t|usize::from(t.join().unwrap())).sum::<usize>(),1);
    let state=repo.inspect("r").unwrap();assert_eq!(state["usage"]["messages"],2);assert_eq!(state["tasks"].as_array().unwrap().len(),1);
    assert_ne!(state["tasks"][0]["state"],"open");
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn lost_connection_keeps_acknowledged_allocations_and_expiry_never_refunds() {
    let fixture=Fixture::new();let mut client=Client::connect(&fixture.dsn,NoTls).unwrap();
    let pid:i32=client.query_one("SELECT pg_backend_pid()",&[]).unwrap().get(0);
    let mut repo=PostgresRepository::open(client,&fixture.schema,fixture.binding.clone()).unwrap();
    repo.create_run("r",&configuration()).unwrap();repo.reserve_call("r","a","escaped").unwrap();repo.send(&request(),"a").unwrap();
    fixture.admin().query_one("SELECT pg_terminate_backend($1)",&[&pid]).unwrap();
    assert!(repo.reserve_call("r","a","other").is_err());
    fixture.admin().execute("UPDATE runs SET deadline=0 WHERE id='r'",&[]).unwrap();
    let mut recovered=fixture.open();let state=recovered.inspect("r").unwrap();
    assert_eq!(state["expired"],true);assert_eq!(state["usage"]["calls"],1);assert_eq!(state["calls"][0]["state"],"reserved");assert_eq!(state["tasks"][0]["state"],"expired_outcome_unknown");
    assert!(recovered.reserve_call("r","a","other").is_err());
    recovered.finish_call("r","a","escaped",&json!({"observed":"completed"})).unwrap();
    assert_eq!(recovered.inspect("r").unwrap()["usage"]["calls"],1);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn postgres_gateway_preserves_rejected_but_charged_attempts() {
    let fixture=Fixture::new();fixture.open().create_run("r",&configuration()).unwrap();
    let configuration=json!({"gateway_id":"test","max_attempts":2,"credentials":[{"credential_id":"a","token":"aa".repeat(32),"run_id":"r","actor":"a","max_attempts":2}]});
    let mut gateway=Gateway::open(fixture.open(),&configuration).unwrap();
    assert_eq!(gateway.handle_frame(b"bad-json").response["ok"],false);
    drop(gateway);
    let mut gateway=Gateway::open(fixture.open(),&configuration).unwrap();
    assert_eq!(gateway.handle_frame(b"bad-json").response["ok"],false);
    assert_eq!(gateway.handle_frame(b"bad-json").response["error"]["code"],"admission_closed");
}
