#![cfg(feature = "postgres-repository")]
//! Real backend tests, deliberately opt-in: no mock can qualify PostgreSQL.
use mas_protocol_codec::{
    gateway::Gateway,
    postgres_repository::{AuthorityBinding, PostgresRepository},
    repository::ProtocolRepository,
    store::Store,
    Message,
};
use postgres::{Client, NoTls};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Barrier,
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    dsn: String,
    schema: String,
    binding: AuthorityBinding,
}
impl Fixture {
    fn new() -> Self {
        let dsn = std::env::var("MAS_TEST_POSTGRES_DSN")
            .expect("real PostgreSQL test connection required");
        let schema = format!(
            "mas_test_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        let binding = AuthorityBinding {
            deployment: "test-deployment".into(),
            controller: "controller-one".into(),
            epoch: 1,
        };
        PostgresRepository::initialise(
            Client::connect(&dsn, NoTls).unwrap(),
            &schema,
            binding.clone(),
        )
        .unwrap();
        Self {
            dsn,
            schema,
            binding,
        }
    }
    fn open(&self) -> PostgresRepository {
        self.open_as(self.binding.clone())
    }
    fn open_as(&self, binding: AuthorityBinding) -> PostgresRepository {
        PostgresRepository::open(
            Client::connect(&self.dsn, NoTls).unwrap(),
            &self.schema,
            binding,
        )
        .unwrap()
    }
    fn admin(&self) -> Client {
        let mut client = Client::connect(&self.dsn, NoTls).unwrap();
        client
            .batch_execute(&format!("SET search_path TO {},pg_catalog", self.schema))
            .unwrap();
        client
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if let Ok(mut c) = Client::connect(&self.dsn, NoTls) {
            let _ = c.batch_execute(&format!("DROP SCHEMA {} CASCADE", self.schema));
        }
    }
}
fn message(id: &str, from: &str, to: &str, kind: &str, payload: Value) -> Message {
    Message {
        message_id: id.into(),
        run_id: "r".into(),
        sender: from.into(),
        recipient: to.into(),
        version: "1".into(),
        kind: kind.into(),
        payload: payload.as_object().unwrap().clone(),
    }
}
fn request() -> Message {
    message(
        "q",
        "a",
        "b",
        "Request",
        json!({"task_id":"task","operation":"check","input":{}}),
    )
}
fn result() -> Message {
    message(
        "answer",
        "b",
        "a",
        "Result",
        json!({"task_id":"task","reply_to":"q","status":"completed","output":{"answer":42}}),
    )
}
fn configuration() -> Value {
    json!({"agents":["a","b"],"edges":[["a","b"],["b","a"]],"application":"agent-task/1","max_calls":2,"mailbox_capacity":2})
}
fn trace<R: ProtocolRepository>(repository: &mut R) -> Vec<Value> {
    let mut output = Vec::new();
    repository.create_run("r", &configuration()).unwrap();
    repository
        .register_admission("gateway/test", &"a".repeat(64), 2)
        .unwrap();
    output.push(json!(repository.reserve_admission("gateway/test").unwrap()));
    output.push(repository.send(&request(), "a").unwrap());
    output.push(repository.send(&request(), "a").unwrap());
    assert!(repository.ack("r", "q", "b").is_err());
    output.push(repository.receive("r", "b", 2).unwrap());
    output.push(repository.receive("r", "b", 2).unwrap());
    output.push(repository.ack("r", "q", "b").unwrap());
    output.push(repository.reserve_call("r", "b", "call").unwrap());
    assert!(repository.reserve_call("r", "b", "call").is_err());
    assert!(repository
        .finish_call("r", "a", "call", &json!({"status":"ok"}))
        .is_err());
    output.push(
        repository
            .finish_call("r", "b", "call", &json!({"status":"ok"}))
            .unwrap(),
    );
    output.push(repository.send(&result(), "b").unwrap());
    let mut inspection = repository.inspect("r").unwrap();
    inspection.as_object_mut().unwrap().remove("deadline");
    for task in inspection["tasks"].as_array_mut().unwrap() {
        task.as_object_mut().unwrap().remove("closed_at");
    }
    output.push(inspection);
    output
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn sqlite_postgres_complete_protocol_trace_agrees() {
    let fixture = Fixture::new();
    let path =
        std::env::temp_dir().join(format!("mas-pg-conformance-{}.sqlite", std::process::id()));
    let mut local = Store::open(&path).unwrap();
    assert_eq!(trace(&mut local), trace(&mut fixture.open()));
    drop(local);
    std::fs::remove_file(path).unwrap();
    assert_eq!(
        fixture.open().inspect("r").unwrap()["calls"][0]["state"],
        "recorded"
    );
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn typed_admission_and_read_only_observation_agree_across_backends() {
    fn typed_trace<R: ProtocolRepository>(repository: &mut R) -> Vec<Value> {
        let mut c = configuration(); c["mailbox_capacity"] = json!(1);
        repository.create_run("r",&c).unwrap();
        let mut output = vec![repository.send_checked(&request(),"missing").unwrap_err().to_value()];
        output.push(repository.send_checked(&request(),"a").unwrap());
        let mut second = request(); second.message_id="second".into(); second.payload["task_id"]=json!("second");
        output.push(repository.send_checked(&second,"a").unwrap_err().to_value());
        output.push(repository.send_checked(&request(),"a").unwrap());
        repository.receive("r","b",1).unwrap(); repository.ack("r","q","b").unwrap();
        output.push(repository.send_checked(&second,"a").unwrap());
        for include in [false,true] {
            let mut snapshot=repository.observe("r",include).unwrap();
            snapshot["run"].as_object_mut().unwrap().remove("deadline");
            output.push(snapshot);
        }
        output
    }
    let fixture=Fixture::new();
    let path=std::env::temp_dir().join(format!("mas-pg-typed-{}.sqlite",std::process::id()));
    let mut local=Store::open(&path).unwrap();
    assert_eq!(typed_trace(&mut local),typed_trace(&mut fixture.open()));
    drop(local); std::fs::remove_file(path).unwrap();
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn hybrid_return_routes_and_scoped_identity_observations_agree_across_backends() {
    fn contract<R: ProtocolRepository>(repository: &mut R) -> Vec<Value> {
        let mut one_way=configuration(); one_way["edges"]=json!([["a","b"]]);
        repository.create_run("r",&one_way).unwrap();
        let mut output=vec![repository.send_checked(&request(),"a").unwrap_err().to_value()];
        let untouched=repository.observe("r",false).unwrap();
        assert_eq!(untouched["run"]["usage"],json!({"messages":0,"bytes":0,"calls":0}));
        assert_eq!(untouched["messages"],json!([])); assert_eq!(untouched["tasks"],json!([])); assert_eq!(untouched["calls"],json!([]));
        one_way["application"]=Value::Null;
        repository.create_run("generic",&one_way).unwrap();
        let mut notice=request(); notice.run_id="generic".into(); notice.kind="Notice".into();
        output.push(repository.send_checked(&notice,"a").unwrap());
        output.push(repository.reconcile_message("generic","a","q").unwrap());
        output.push(repository.reconcile_message("generic","b","q").unwrap());
        output.push(repository.reconcile_message("r","a","q").unwrap());
        repository.receive("generic","b",1).unwrap(); repository.ack("generic","q","b").unwrap();
        output.push(repository.reconcile_message("generic","a","q").unwrap());
        output
    }
    let fixture=Fixture::new();
    let path=std::env::temp_dir().join(format!("mas-pg-hybrid-{}.sqlite",std::process::id()));
    let mut local=Store::open(&path).unwrap();
    assert_eq!(contract(&mut local),contract(&mut fixture.open()));
    drop(local); std::fs::remove_file(path).unwrap();
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn retained_postgres_one_way_request_remains_idempotent_without_migration() {
    let fixture=Fixture::new(); let mut repository=fixture.open();
    let mut one_way=configuration(); one_way["edges"]=json!([["a","b"]]);
    repository.create_run("r",&one_way).unwrap();
    let content=request().to_value().to_string();
    let mut admin=fixture.admin();
    let sequence:i64=admin.query_one("INSERT INTO messages(run,id,sender,recipient,content) VALUES('r','q','a','b',$1) RETURNING seq",&[&content]).unwrap().get(0);
    admin.execute("INSERT INTO tasks(run,id,request_id) VALUES('r','task','q')",&[]).unwrap();
    admin.execute("UPDATE runs SET message_count=1,byte_count=$1 WHERE id='r'",&[&(content.len() as i64)]).unwrap();
    assert_eq!(repository.send_checked(&request(),"a").unwrap(),json!({"accepted":true,"duplicate":true,"sequence":sequence}));
    let mut fresh=request(); fresh.message_id="fresh".into(); fresh.payload["task_id"]=json!("fresh");
    assert_eq!(repository.send_checked(&fresh,"a").unwrap_err().code,mas_protocol_codec::admission::AdmissionCode::ForbiddenRoute);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn postgres_observation_retains_expiry_and_enforces_authority_and_lease() {
    let fixture=Fixture::new(); let mut repository=fixture.open();
    repository.create_run("r",&configuration()).unwrap();
    repository.send_checked(&request(),"a").unwrap();
    fixture.admin().execute("UPDATE runs SET deadline=0 WHERE id='r'",&[]).unwrap();
    let snapshot=repository.observe("r",false).unwrap();
    assert_eq!(snapshot["run"]["expired"],false);
    assert_eq!(snapshot["tasks"][0]["state"],"open");
    assert_eq!(snapshot["messages"][0]["received"],false);
    assert!(!fixture.admin().query_one("SELECT expired FROM runs WHERE id='r'",&[]).unwrap().get::<_,bool>(0));
    repository.install_host_repository().unwrap();
    repository.install_controller_repository().unwrap();
    fixture.admin().execute("UPDATE controller_lease SET lease_until=clock_timestamp()-interval '1 second'",&[]).unwrap();
    assert!(repository.observe("r",false).unwrap_err().contains("lease_expired"));
    fixture.admin().execute("UPDATE controller_lease SET lease_until=clock_timestamp()+interval '60 seconds'",&[]).unwrap();
    repository.handover("controller-two").unwrap();
    assert!(repository.observe("r",false).unwrap_err().contains("stale protocol authority"));
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn concurrent_connections_cannot_over_allocate_or_reuse_identity() {
    let fixture = Arc::new(Fixture::new());
    fixture.open().create_run("r", &configuration()).unwrap();
    let barrier = Arc::new(Barrier::new(9));
    let threads = (0..8)
        .map(|n| {
            let f = fixture.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                let mut repo = f.open();
                b.wait();
                repo.reserve_call("r", "a", &format!("call-{n}")).is_ok()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        2
    );
    let state = fixture.open().inspect("r").unwrap();
    assert_eq!(state["usage"]["calls"], 2);
    assert_eq!(state["calls"].as_array().unwrap().len(), 2);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn handover_fences_every_old_handle_and_retains_original_effects() {
    let fixture = Fixture::new();
    let mut old = fixture.open();
    old.create_run("r", &configuration()).unwrap();
    old.register_admission("gateway/test", &"a".repeat(64), 3)
        .unwrap();
    old.reserve_call("r", "a", "escaped").unwrap();
    old.send(&request(), "a").unwrap();
    let next = fixture.open().handover("controller-two").unwrap();
    assert_eq!(next.epoch, 2);
    assert!(old
        .reserve_admission("gateway/test")
        .unwrap_err()
        .contains("stale"));
    assert!(old.reserve_call("r", "a", "new").is_err());
    assert!(old.finish_call("r", "a", "escaped", &json!({})).is_err());
    assert!(old.receive("r", "b", 1).is_err());
    assert!(old.send(&request(), "a").is_err());
    assert!(old.inspect("r").is_err());
    let mut current = fixture.open_as(next);
    assert_eq!(current.inspect("r").unwrap()["usage"]["calls"], 1);
    assert!(current.reserve_call("r", "a", "escaped").is_err());
    current
        .finish_call("r", "a", "escaped", &json!({"observed":"completed"}))
        .unwrap();
    assert_eq!(
        current.inspect("r").unwrap()["calls"][0]["state"],
        "recorded"
    );
    assert!(PostgresRepository::initialise(
        Client::connect(&fixture.dsn, NoTls).unwrap(),
        &fixture.schema,
        fixture.binding.clone()
    )
    .is_err());
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn task_message_and_quota_commit_atomically_and_schema_drift_fails_closed() {
    let fixture = Fixture::new();
    let mut repo = fixture.open();
    repo.create_run("r", &configuration()).unwrap();
    fixture.admin().batch_execute("CREATE FUNCTION fail_task() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected task insertion failure'; END $$; CREATE TRIGGER fail_task BEFORE INSERT ON tasks FOR EACH ROW EXECUTE FUNCTION fail_task();").unwrap();
    assert!(repo.send(&request(), "a").is_err());
    let state = repo.inspect("r").unwrap();
    assert_eq!(state["usage"]["messages"], 0);
    assert_eq!(state["messages"], json!([]));
    assert_eq!(state["tasks"], json!([]));
    assert!(PostgresRepository::open(
        Client::connect(&fixture.dsn, NoTls).unwrap(),
        &fixture.schema,
        fixture.binding.clone()
    )
    .is_err());
    fixture
        .admin()
        .batch_execute("DROP TRIGGER fail_task ON tasks; DROP FUNCTION fail_task();")
        .unwrap();
    repo.send(&request(), "a").unwrap();
    assert_eq!(
        repo.inspect("r").unwrap()["tasks"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    fixture
        .admin()
        .batch_execute("ALTER TABLE calls DROP CONSTRAINT calls_state_check")
        .unwrap();
    assert!(PostgresRepository::open(
        Client::connect(&fixture.dsn, NoTls).unwrap(),
        &fixture.schema,
        fixture.binding.clone()
    )
    .is_err());
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn concurrent_terminal_outcomes_leave_one_native_transition() {
    let fixture = Arc::new(Fixture::new());
    let mut repo = fixture.open();
    repo.create_run("r", &configuration()).unwrap();
    repo.send(&request(), "a").unwrap();
    repo.receive("r", "b", 1).unwrap();
    let cancel = message(
        "cancel",
        "a",
        "b",
        "Cancel",
        json!({"task_id":"task","reply_to":"q","reason":"stop"}),
    );
    let barrier = Arc::new(Barrier::new(3));
    let threads = [(result(), "b"), (cancel, "a")]
        .into_iter()
        .map(|(m, a)| {
            let f = fixture.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                let mut repo = f.open();
                b.wait();
                repo.send(&m, a).is_ok()
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    assert_eq!(
        threads
            .into_iter()
            .map(|t| usize::from(t.join().unwrap()))
            .sum::<usize>(),
        1
    );
    let state = repo.inspect("r").unwrap();
    assert_eq!(state["usage"]["messages"], 2);
    assert_eq!(state["tasks"].as_array().unwrap().len(), 1);
    assert_ne!(state["tasks"][0]["state"], "open");
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn lost_connection_keeps_acknowledged_allocations_and_expiry_never_refunds() {
    let fixture = Fixture::new();
    let mut client = Client::connect(&fixture.dsn, NoTls).unwrap();
    let pid: i32 = client
        .query_one("SELECT pg_backend_pid()", &[])
        .unwrap()
        .get(0);
    let mut repo =
        PostgresRepository::open(client, &fixture.schema, fixture.binding.clone()).unwrap();
    repo.create_run("r", &configuration()).unwrap();
    repo.reserve_call("r", "a", "escaped").unwrap();
    repo.send(&request(), "a").unwrap();
    fixture
        .admin()
        .query_one("SELECT pg_terminate_backend($1)", &[&pid])
        .unwrap();
    assert!(repo.reserve_call("r", "a", "other").is_err());
    fixture
        .admin()
        .execute("UPDATE runs SET deadline=0 WHERE id='r'", &[])
        .unwrap();
    let mut recovered = fixture.open();
    let state = recovered.inspect("r").unwrap();
    assert_eq!(state["expired"], true);
    assert_eq!(state["usage"]["calls"], 1);
    assert_eq!(state["calls"][0]["state"], "reserved");
    assert_eq!(state["tasks"][0]["state"], "expired_outcome_unknown");
    assert!(recovered.reserve_call("r", "a", "other").is_err());
    recovered
        .finish_call("r", "a", "escaped", &json!({"observed":"completed"}))
        .unwrap();
    assert_eq!(recovered.inspect("r").unwrap()["usage"]["calls"], 1);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn postgres_gateway_preserves_rejected_but_charged_attempts() {
    let fixture = Fixture::new();
    fixture.open().create_run("r", &configuration()).unwrap();
    let configuration = json!({"gateway_id":"test","max_attempts":2,"credentials":[{"credential_id":"a","token":"aa".repeat(32),"run_id":"r","actor":"a","max_attempts":2}]});
    let mut gateway = Gateway::open(fixture.open(), &configuration).unwrap();
    assert_eq!(gateway.handle_frame(b"bad-json").response["ok"], false);
    drop(gateway);
    let mut gateway = Gateway::open(fixture.open(), &configuration).unwrap();
    assert_eq!(gateway.handle_frame(b"bad-json").response["ok"], false);
    assert_eq!(
        gateway.handle_frame(b"bad-json").response["error"]["code"],
        "admission_closed"
    );
}

fn host_setup(f: &Fixture) -> PostgresRepository {
    let mut repo = f.open();
    repo.install_host_repository().unwrap();
    for scope in ["deployment", "principal/alice", "ancestor/parent"] {
        repo.host_command(
            &format!("scope-{}", scope.replace('/', "-")),
            "configure_scope",
            &json!({"scope":scope,"max_runs":5,"max_calls":2,"max_microusd":100}),
        )
        .unwrap();
    }
    let mut c = configuration();
    c["edges"] = json!([["a", "b"], ["b", "a"]]);
    // Metadata binds the normalised Rust configuration rather than an alias.
    let native = repo.create_run("normalisation", &c).unwrap()["config"].clone();
    repo.host_command("create-host-run","create_run",&json!({"run":"r","principal":"alice","ancestors":["ancestor/parent"],"configuration":c,"metadata":{"run_id":"r","workflow_version":"test/1","topology":native,"max_depth":8,"secret_reference":"secret/1","principal":"alice"}})).unwrap();
    repo.host_command(
        "claim-host-run",
        "claim_run",
        &json!({"run":"r","owner":"owner-one"}),
    )
    .unwrap();
    repo
}
fn prepare(repo: &mut PostgresRepository, key: &str, call: &str, amount: i64) {
    repo.host_command(
        &format!("step-{key}"),
        "step",
        &json!({"run":"r","owner":"owner-one","key":key,"kind":"invoke","input":{},"initial":{}}),
    )
    .unwrap();
    repo.host_command(&format!("prepare-{call}"),"prepare_invocation",&json!({"run":"r","owner":"owner-one","step_key":key,"actor":"a","call_id":call,"reservation_microusd":amount})).unwrap();
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn native_typed_admission_never_turns_retained_receipt_into_dispatch() {
    use mas_protocol_codec::shared_native::{NativeAdmission, NativeRejection};
    let fixture = Fixture::new();
    let mut repo = host_setup(&fixture);
    prepare(&mut repo, "native-step", "native-call", 10);
    let first=repo.admit_native_invocation("native-admit", "r", "owner-one", "native-call");
    let NativeAdmission::FreshDispatch(permit)=first else { panic!("expected fresh admission: {first:?}"); };
    assert_eq!(permit.run(), "r");
    assert_eq!(permit.command_id(), "native-admit");
    assert_eq!(permit.into_call_id(), "native-call");
    assert!(matches!(repo.admit_native_invocation("native-admit", "r", "owner-one", "native-call"),NativeAdmission::Retained{..}));
    assert!(matches!(repo.admit_native_invocation("replacement", "r", "owner-one", "native-call"),NativeAdmission::Retained{..}));
    assert!(matches!(repo.admit_native_invocation("native-admit", "r", "owner-one", "other-call"),NativeAdmission::Rejected{code:NativeRejection::IdentityConflict}));
    assert!(matches!(repo.admit_native_invocation("new-id", "r", "wrong-owner", "native-call"),NativeAdmission::Rejected{code:NativeRejection::StaleOwner}));
    assert!(matches!(repo.admit_native_invocation("bad id", "r", "owner-one", "native-call"),NativeAdmission::Rejected{code:NativeRejection::InvalidIdentity}));
    assert_eq!(repo.host_query("scope",&json!({"scope":"deployment"})).unwrap()["calls"],1);
    drop(repo);
    let mut reopened=fixture.open();
    assert!(matches!(reopened.admit_native_invocation("native-admit", "r", "owner-one", "native-call"),NativeAdmission::Retained{..}));
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn native_typed_admission_reconciles_lost_reply_without_dispatch() {
    use mas_protocol_codec::shared_native::NativeAdmission;
    let fixture=Fixture::new(); let mut repo=host_setup(&fixture);
    prepare(&mut repo,"lost-step","lost-call",10);
    // The caller loses this actual committed response.
    repo.host_command("lost-command","admit_invocation",&json!({"run":"r","owner":"owner-one","call_id":"lost-call"})).unwrap();
    assert!(matches!(repo.admit_native_invocation("lost-command","r","owner-one","lost-call"),NativeAdmission::Retained{..}));
    let _next=repo.handover("controller-two").unwrap();
    assert!(matches!(repo.admit_native_invocation("lost-command","r","owner-one","lost-call"),NativeAdmission::Uncertain{..}));
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn observational_host_query_does_not_wait_for_authority_writer_or_expire_run() {
    let fixture=Fixture::new(); let _repo=host_setup(&fixture);
    let mut client=Client::connect(&fixture.dsn,NoTls).unwrap();
    client.batch_execute("SET statement_timeout='500ms'").unwrap();
    let repo=PostgresRepository::open(client,&fixture.schema,fixture.binding.clone()).unwrap();
    let mut admin=fixture.admin();
    admin.execute("UPDATE runs SET deadline=0,expired=false WHERE id='r'",&[]).unwrap();
    let mut lock=admin.transaction().unwrap();
    lock.query_one("SELECT epoch FROM authority WHERE singleton FOR UPDATE",&[]).unwrap();
    assert_eq!(repo.host_query("scope",&json!({"scope":"deployment"})).unwrap()["calls"],0);
    assert!(!lock.query_one("SELECT expired FROM runs WHERE id='r'",&[]).unwrap().get::<_,bool>(0));
    lock.rollback().unwrap();
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn scheduler_amendment_is_exact_replay_safe_and_preserves_occupied_allowances() {
    use mas_protocol_codec::shared_scheduler::policy_digest;
    let fixture=Fixture::new(); let mut repo=host_setup(&fixture);
    repo.install_controller_repository().unwrap(); repo.install_service_repository().unwrap();
    let before=json!({"max_workers":16,"max_per_principal":8,"workflows":{},"resources":{"github":2}});
    repo.host_command("scheduler-initial","task.configure_scheduler",&json!({"policy":before})).unwrap();
    let after=json!({"format":"mas-scheduler-policy/2","revision":1,"max_workers":512,"max_per_principal":256,"workflows":{},"resources":{"github":4}});
    let p=json!({"expected_policy_digest":policy_digest(&before),"expected_revision":0,"policy":after,"reason":"qualified capacity increase","authoriser":"operator/alice"});
    assert_eq!(repo.host_command("scheduler-amend","task.amend_scheduler",&p).unwrap()["replayed"],false);
    assert_eq!(repo.host_command("scheduler-amend","task.amend_scheduler",&p).unwrap()["replayed"],true);
    assert!(repo.host_command("scheduler-stale","task.amend_scheduler",&p).is_err());
    assert_eq!(repo.host_query("task.scheduler",&json!({})).unwrap()["policy"],after);
    let mut admin=fixture.admin();
    for assignment in ["slot-a","slot-b"] {
        admin.execute("INSERT INTO service_slots VALUES($1,'alice','sample',$2,'[\"github\"]','owner',1)",&[&assignment,&"a".repeat(64)]).unwrap();
    }
    let smaller=json!({"format":"mas-scheduler-policy/2","revision":2,"max_workers":1,"max_per_principal":1,"workflows":{},"resources":{"github":1}});
    let p=json!({"expected_policy_digest":policy_digest(&after),"expected_revision":1,"policy":smaller,"reason":"too small","authoriser":"operator/alice"});
    assert_eq!(repo.host_command("scheduler-lower","task.amend_scheduler",&p).unwrap()["receipt"]["occupied_at_amendment"],2);
    let state=repo.host_query("task.scheduler",&json!({})).unwrap();
    assert_eq!(state["policy"],smaller); assert_eq!(state["occupied"],2);
    // Returning to the same capacity values has a distinct revision/digest.
    let mut returned=after.clone(); returned["revision"]=json!(3);
    let p=json!({"expected_policy_digest":policy_digest(&smaller),"expected_revision":2,"policy":returned,"reason":"return capacity","authoriser":"operator/alice"});
    repo.host_command("scheduler-return","task.amend_scheduler",&p).unwrap();
    let stale=json!({"expected_policy_digest":policy_digest(&after),"expected_revision":1,"policy":smaller,"reason":"stale before return","authoriser":"operator/alice"});
    assert!(repo.host_command("scheduler-aba","task.amend_scheduler",&stale).is_err());
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn shared_host_admission_and_known_observation_are_atomic_and_replay_safe() {
    let fixture = Fixture::new();
    let mut repo = host_setup(&fixture);
    prepare(&mut repo, "step-one", "effect-one", 60);
    let p = json!({"run":"r","owner":"owner-one","call_id":"effect-one"});
    assert_eq!(
        repo.host_command("admit-effect-one", "admit_invocation", &p)
            .unwrap()["replayed"],
        false
    );
    assert_eq!(
        repo.host_command("admit-effect-one", "admit_invocation", &p)
            .unwrap()["replayed"],
        true
    );
    assert!(repo
        .host_command("different-admit-id", "admit_invocation", &p)
        .is_err());
    assert_eq!(repo.inspect("r").unwrap()["usage"]["calls"], 1);
    for scope in ["deployment", "principal/alice", "ancestor/parent"] {
        let state = repo.host_query("scope", &json!({"scope":scope})).unwrap();
        assert_eq!(state["calls"], 1);
        assert_eq!(state["microusd"], 60);
    }
    let observed = json!({"run":"r","owner":"owner-one","call_id":"effect-one","state":"completed","output":{"answer":1},"evidence":{"certainty":"completed"}});
    repo.host_command("observe-effect-one", "observe_invocation", &observed)
        .unwrap();
    assert_eq!(repo.inspect("r").unwrap()["calls"][0]["state"], "recorded");
    assert_eq!(
        repo.host_query("invocation", &json!({"run":"r","call_id":"effect-one"}))
            .unwrap()["state"],
        "completed"
    );
    prepare(&mut repo, "step-two", "effect-two", 60);
    assert!(repo
        .host_command(
            "admit-effect-two",
            "admit_invocation",
            &json!({"run":"r","owner":"owner-one","call_id":"effect-two"})
        )
        .is_err());
    assert_eq!(repo.inspect("r").unwrap()["usage"]["calls"], 1);
    assert_eq!(
        repo.host_query("scope", &json!({"scope":"deployment"}))
            .unwrap()["calls"],
        1
    );
    assert_eq!(
        repo.host_query("invocation", &json!({"run":"r","call_id":"effect-two"}))
            .unwrap()["state"],
        "prepared"
    );
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn shared_host_native_failure_rolls_back_all_charges_and_invocation_intent() {
    let fixture = Fixture::new();
    let mut repo = host_setup(&fixture);
    prepare(&mut repo, "step-one", "effect-one", 50);
    fixture.admin().batch_execute("CREATE FUNCTION fail_call() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected native call failure'; END $$; CREATE TRIGGER fail_call BEFORE INSERT ON calls FOR EACH ROW EXECUTE FUNCTION fail_call();").unwrap();
    assert!(repo
        .host_command(
            "admit-effect-one",
            "admit_invocation",
            &json!({"run":"r","owner":"owner-one","call_id":"effect-one"})
        )
        .is_err());
    assert_eq!(repo.inspect("r").unwrap()["usage"]["calls"], 0);
    for scope in ["deployment", "principal/alice", "ancestor/parent"] {
        assert_eq!(
            repo.host_query("scope", &json!({"scope":scope})).unwrap()["microusd"],
            0
        );
    }
    assert_eq!(
        repo.host_query("invocation", &json!({"run":"r","call_id":"effect-one"}))
            .unwrap()["state"],
        "prepared"
    );
    let count: i64 = fixture
        .admin()
        .query_one(
            "SELECT count(*) FROM host_commands WHERE id='admit-effect-one'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!(count, 0);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn shared_host_handover_retains_unknown_effect_and_invalidates_old_run_owner() {
    let fixture = Fixture::new();
    let mut old = host_setup(&fixture);
    prepare(&mut old, "step-one", "effect-one", 50);
    let p = json!({"run":"r","owner":"owner-one","call_id":"effect-one"});
    old.host_command("admit-effect-one", "admit_invocation", &p)
        .unwrap();
    let binding = old.handover("controller-two").unwrap();
    let mut replacement = fixture.open_as(binding);
    replacement
        .host_command(
            "claim-replacement",
            "claim_run",
            &json!({"run":"r","owner":"owner-two"}),
        )
        .unwrap();
    assert!(old
        .host_command(
            "old-mutation",
            "set_step",
            &json!({"run":"r","owner":"owner-one","key":"step-one","expected_revision":2,"data":{}})
        )
        .is_err());
    assert!(replacement
        .host_command(
            "repeated-dispatch",
            "admit_invocation",
            &json!({"run":"r","owner":"owner-two","call_id":"effect-one"})
        )
        .is_err());
    assert_eq!(
        replacement
            .host_query("invocation", &json!({"run":"r","call_id":"effect-one"}))
            .unwrap()["state"],
        "executing"
    );
    assert_eq!(
        replacement
            .host_query("scope", &json!({"scope":"deployment"}))
            .unwrap()["microusd"],
        50
    );
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn controller_lease_takeover_fences_every_old_handle_and_preserves_unknown_effects() {
    let fixture = Fixture::new();
    let mut old = host_setup(&fixture);
    old.install_controller_repository().unwrap();
    old.install_service_repository().unwrap();
    old.renew_authority(30).unwrap();
    prepare(&mut old, "step-one", "effect-one", 50);
    old.host_command(
        "effect-admit",
        "admit_invocation",
        &json!({"run":"r","owner":"owner-one","call_id":"effect-one"}),
    )
    .unwrap();
    let mut contender = fixture.open();
    assert!(contender.takeover("controller-two", 30).is_err());
    fixture.admin().batch_execute("UPDATE controller_lease SET lease_until=clock_timestamp()-interval '1 second' WHERE singleton").unwrap();
    assert!(old.inspect("r").is_err());
    assert!(old.renew_authority(60).is_err());
    let recovery_started = std::time::Instant::now();
    let next = contender.takeover("controller-two", 30).unwrap();
    assert_eq!(next.epoch, 2);
    assert!(contender.takeover("controller-three", 30).is_err());
    let mut current = fixture.open_as(next);
    current
        .host_command(
            "claim-new",
            "claim_run",
            &json!({"run":"r","owner":"owner-two"}),
        )
        .unwrap();
    assert_eq!(
        current.inspect("r").unwrap()["calls"][0]["state"],
        "reserved"
    );
    assert_eq!(
        current
            .host_query("invocation", &json!({"run":"r","call_id":"effect-one"}))
            .unwrap()["state"],
        "executing"
    );
    assert!(old.host_command("old-observe","observe_invocation",&json!({"run":"r","owner":"owner-one","call_id":"effect-one","state":"completed","output":{},"evidence":{}})).is_err());
    assert_eq!(
        current.host_query("authority.audit", &json!({})).unwrap()[0]["operation"],
        "takeover"
    );
    assert!(
        recovery_started.elapsed() < std::time::Duration::from_secs(60),
        "reference same-database recovery gate exceeded 60 seconds"
    );
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn controller_drain_blocks_new_admission_but_retains_observation_and_audited_recovery() {
    let fixture = Fixture::new();
    let mut repo = host_setup(&fixture);
    repo.install_controller_repository().unwrap();
    repo.install_service_repository().unwrap();
    prepare(&mut repo, "step-one", "effect-one", 50);
    repo.host_command(
        "effect-admit",
        "admit_invocation",
        &json!({"run":"r","owner":"owner-one","call_id":"effect-one"}),
    )
    .unwrap();
    repo.host_command(
        "drain",
        "authority.drain",
        &json!({"operator":"operator","draining":true,"evidence":{"change":"maintenance"}}),
    )
    .unwrap();
    assert!(repo.create_run("new-run", &configuration()).is_err());
    assert!(repo.reserve_call("r", "a", "new-effect").is_err());
    assert!(repo.host_command("prepare-blocked","prepare_invocation",&json!({"run":"r","owner":"owner-one","step_key":"step-one","actor":"a","call_id":"new-effect","reservation_microusd":1})).is_err());
    repo.host_command("observe-unknown","observe_invocation",&json!({"run":"r","owner":"owner-one","call_id":"effect-one","state":"unknown","output":null,"evidence":null})).unwrap();
    let p = json!({"run":"r","owner":"owner-one","call_id":"effect-one","operator":"operator","decision":"completed","evidence":{"provider_receipt":"retained"},"output":{"answer":42}});
    repo.host_command("resolve", "resolve_invocation", &p)
        .unwrap();
    assert_eq!(
        repo.host_command("resolve", "resolve_invocation", &p)
            .unwrap()["replayed"],
        true
    );
    assert_eq!(
        repo.host_query("resolutions", &json!({"run":"r"}))
            .unwrap()
            .as_array()
            .unwrap()
            .len(),
        1
    );
    assert_eq!(repo.inspect("r").unwrap()["calls"][0]["state"], "reserved");
    assert_eq!(
        repo.host_query("scope", &json!({"scope":"deployment"}))
            .unwrap()["microusd"],
        50
    );
    assert_eq!(
        repo.host_query("invocation", &json!({"run":"r","call_id":"effect-one"}))
            .unwrap()["state"],
        "resolved"
    );
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn independent_native_callers_never_both_receive_fresh_dispatch() {
    use mas_protocol_codec::shared_native::NativeAdmission;
    for shared_command in [true, false] {
        let fixture = Fixture::new();
        let mut setup = host_setup(&fixture);
        prepare(&mut setup, "race-step", "race-call", 10);
        drop(setup);
        let barrier = Arc::new(Barrier::new(3));
        let mut callers = Vec::new();
        for index in 0..2 {
            let dsn = fixture.dsn.clone();
            let schema = fixture.schema.clone();
            let binding = fixture.binding.clone();
            let barrier = Arc::clone(&barrier);
            callers.push(std::thread::spawn(move || {
                let mut repo = PostgresRepository::open(
                    Client::connect(&dsn, NoTls).unwrap(), &schema, binding,
                ).unwrap();
                let command = if shared_command { "racing-command".into() }
                    else { format!("racing-command-{index}") };
                barrier.wait();
                match repo.admit_native_invocation(&command, "r", "owner-one", "race-call") {
                    NativeAdmission::FreshDispatch(permit) => {
                        assert_eq!(permit.into_call_id(), "race-call");
                        true
                    }
                    NativeAdmission::Retained { .. } => false,
                    other => panic!("unexpected racing admission: {other:?}"),
                }
            }));
        }
        barrier.wait();
        let fresh = callers.into_iter().map(|thread| thread.join().unwrap())
            .filter(|fresh| *fresh).count();
        assert_eq!(fresh, 1);
        let repo = fixture.open();
        assert_eq!(repo.host_query("scope", &json!({"scope":"deployment"})).unwrap()["calls"], 1);
        let mut admin = fixture.admin();
        let receipts: i64 = admin.query_one(
            "SELECT count(*) FROM host_commands WHERE id LIKE 'racing-command%'", &[],
        ).unwrap().get(0);
        assert_eq!(receipts, 1);
        assert_eq!(repo.host_query("invocation", &json!({"run":"r","call_id":"race-call"})).unwrap()["state"], "executing");
    }
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn typed_native_budget_refusals_issue_no_receipt_or_dispatch_charge() {
    use mas_protocol_codec::shared_native::{NativeAdmission, NativeRejection};
    for scope_exhausted in [true, false] {
        let fixture = Fixture::new();
        let mut repo = host_setup(&fixture);
        prepare(&mut repo, "budget-step", "budget-call", if scope_exhausted { 101 } else { 10 });
        let mut admin = fixture.admin();
        if !scope_exhausted {
            admin.execute("UPDATE runs SET call_count=(config::jsonb->>'max_calls')::bigint WHERE id='r'", &[]).unwrap();
        }
        let outcome = repo.admit_native_invocation("budget-refused", "r", "owner-one", "budget-call");
        let expected = if scope_exhausted { NativeRejection::ScopeBudgetExceeded }
            else { NativeRejection::CallBudgetExceeded };
        assert!(matches!(outcome, NativeAdmission::Rejected { code } if code == expected));
        assert_eq!(repo.host_query("scope", &json!({"scope":"deployment"})).unwrap()["calls"], 0);
        assert_eq!(repo.host_query("invocation", &json!({"run":"r","call_id":"budget-call"})).unwrap()["state"], "prepared");
        let receipts: i64 = admin.query_one("SELECT count(*) FROM host_commands WHERE id='budget-refused'", &[]).unwrap().get(0);
        assert_eq!(receipts, 0);
        // An unchanged refusal remains definite on the original identity.
        assert!(matches!(repo.admit_native_invocation("budget-refused", "r", "owner-one", "budget-call"),
            NativeAdmission::Rejected { code } if code == expected));
    }
}

fn provider_setup(f: &Fixture, quota_index: bool) -> PostgresRepository {
    let mut repo = f.open();
    repo.install_host_repository().unwrap();
    repo.install_controller_repository().unwrap();
    repo.install_service_repository().unwrap();
    if quota_index {
        repo.install_provider_quota_index().unwrap();
        repo.install_provider_quota_index().unwrap();
    }
    repo
}
fn provider_policy() -> Value {
    json!({"format":"mas-provider-circuit/2","resource_key":"github/installation/fixture",
        "failure_threshold":2,"open_seconds":10,"half_open_seconds":5,
        "window_ms":86400000,"max_requests":1})
}
fn provider_admit(repo: &mut PostgresRepository, command: &str, call: &str, policy: &Value) -> Value {
    repo.host_command(command, "circuit.admit", &json!({"policy":policy,"call_id":call})).unwrap()
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn provider_quota_serialises_last_credit_and_retains_lost_reply() {
    let fixture = Fixture::new();
    let mut repo = provider_setup(&fixture, true);
    let barrier = Arc::new(Barrier::new(2));
    let mut workers = Vec::new();
    for id in 0..2 {
        let mut other = fixture.open();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            barrier.wait();
            let key = format!("quota-race-{id}");
            (key.clone(), provider_admit(&mut other, &key, &key, &provider_policy()))
        }));
    }
    let replies: Vec<_> = workers.into_iter().map(|worker| worker.join().unwrap()).collect();
    assert_eq!(replies.iter().filter(|(_, r)| r["replayed"] == false && r["receipt"]["fresh"] == true).count(), 1);
    let (fresh_id, fresh) = replies.iter().find(|(_,r)|r["receipt"]["fresh"] == true).unwrap();
    let replay = provider_admit(&mut repo, fresh_id, fresh_id, &provider_policy());
    assert_eq!(replay["replayed"], true); // retained historical fresh receipt is not permission
    assert_eq!(replay["receipt"], fresh["receipt"]);
    let duplicate = provider_admit(&mut repo, "duplicate-command", fresh_id, &provider_policy());
    assert_eq!(duplicate["receipt"]["fresh"], false);
    assert_eq!(duplicate["receipt"]["disposition"], "retained");
    // Unknown execution still holds its provider credit. Even a definite local
    // abort releases no provider credit before the authoritative window resets.
    repo.host_command("abort-provider-call", "circuit.observe", &json!({"policy":provider_policy(),"call_id":fresh_id,"category":"aborted"})).unwrap();
    let blocked = provider_admit(&mut repo, "after-abort-command", "after-abort", &provider_policy());
    assert_eq!(blocked["receipt"]["disposition"], "quota_deferred");
    assert_eq!(blocked["receipt"]["quota"]["used_requests"], 1);
    let mut changed = provider_policy(); changed["max_requests"] = json!(2);
    assert!(repo.host_command("quota-reset", "circuit.admit", &json!({"policy":changed,"call_id":"reset-attempt"})).is_err());
    assert_eq!(fixture.admin().query_one("SELECT count(*) FROM provider_permits", &[]).unwrap().get::<_, i64>(0), 1);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn provider_defer_is_monotone_and_expired_credit_preserves_identity() {
    let fixture = Fixture::new();
    let mut repo = provider_setup(&fixture, true);
    let policy = provider_policy();
    assert_eq!(provider_admit(&mut repo, "admit-provider", "original", &policy)["receipt"]["fresh"], true);
    let now: i64 = fixture.admin().query_one("SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint", &[]).unwrap().get(0);
    let until = now + 3_600_000;
    let defer = json!({"policy":policy,"call_id":"original","not_before_ms":until});
    repo.host_command("retain-provider-delay", "circuit.defer", &defer).unwrap();
    assert_eq!(repo.host_command("retain-provider-delay", "circuit.defer", &defer).unwrap()["replayed"], true);
    assert!(repo.host_command("retain-provider-delay", "circuit.defer", &json!({"policy":policy,"call_id":"original","not_before_ms":0})).is_err());
    repo.host_command("shorter-provider-delay", "circuit.defer", &json!({"policy":policy,"call_id":"original","not_before_ms":0})).unwrap();
    repo.host_command("successful-provider-response", "circuit.observe", &json!({"policy":policy,"call_id":"original","category":"success"})).unwrap();
    let status = repo.host_query("circuit.status", &json!({"policy":policy})).unwrap();
    assert_eq!(status["open_until_ms"], until);
    assert_eq!(status["state"], "deferred");
    assert_eq!(status["execution_permission"], false);
    assert!(repo.host_command("foreign-deferral", "circuit.defer", &json!({"policy":policy,"call_id":"unregistered","not_before_ms":until})).is_err());
    // Controlled fixture ageing tests reset arithmetic without waiting or
    // deleting retained original identities. It is not a database clock test.
    fixture.admin().batch_execute("UPDATE provider_permits SET created_ms=0; UPDATE provider_circuits SET open_until_ms=0").unwrap();
    assert_eq!(provider_admit(&mut repo, "after-window", "new-window-call", &policy)["receipt"]["fresh"], true);
    assert_eq!(provider_admit(&mut repo, "aged-identity", "original", &policy)["receipt"]["fresh"], false);
    assert_eq!(fixture.admin().query_one("SELECT count(*) FROM provider_permits", &[]).unwrap().get::<_, i64>(0), 2);
    fixture.admin().execute("UPDATE provider_permits SET created_ms=$1 WHERE call_id='new-window-call'", &[&(now+86_400_000)]).unwrap();
    assert!(repo.host_command("regressed-clock", "circuit.admit", &json!({"policy":policy,"call_id":"clock-regression"})).is_err());
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn provider_quota_index_upgrade_guards_layout_and_v1_remains_compatible() {
    let fixture = Fixture::new();
    let mut repo = provider_setup(&fixture, false);
    let v1 = json!({"format":"mas-provider-circuit/1","resource_key":"legacy/provider","failure_threshold":2,"open_seconds":10,"half_open_seconds":5});
    assert_eq!(provider_admit(&mut repo, "legacy-provider", "legacy-call", &v1)["receipt"]["admitted"], true);
    assert!(repo.host_command("requires-index", "circuit.admit", &json!({"policy":provider_policy(),"call_id":"quota-call"})).is_err());
    let digest: String = fixture.admin().query_one("SELECT layout_digest FROM authority", &[]).unwrap().get(0);
    fixture.admin().batch_execute("CREATE TABLE unexpected_drift(value bigint)").unwrap();
    assert!(repo.install_provider_quota_index().is_err());
    let mut admin = fixture.admin();
    assert_eq!(admin.query_one("SELECT layout_digest FROM authority", &[]).unwrap().get::<_, String>(0), digest);
    assert_eq!(admin.query_one("SELECT count(*) FROM pg_indexes WHERE schemaname=current_schema() AND indexname='provider_quota_window'", &[]).unwrap().get::<_, i64>(0), 0);
    admin.batch_execute("DROP TABLE unexpected_drift").unwrap();
    repo.install_provider_quota_index().unwrap();
    assert_eq!(provider_admit(&mut repo, "now-qualified", "quota-call", &provider_policy())["receipt"]["fresh"], true);
    assert_eq!(provider_admit(&mut repo, "legacy-duplicate", "legacy-call", &v1)["receipt"]["admitted"], true);
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn typed_provider_permission_never_comes_from_replay_or_unknown_response() {
    use mas_protocol_codec::shared_remote::ProviderAdmission;
    let fixture = Fixture::new();
    let mut repo = provider_setup(&fixture, true);
    match repo.admit_provider_request("typed-provider", &provider_policy(), "typed-call") {
        ProviderAdmission::FreshDispatch(permit) => {
            assert_eq!(permit.resource(), "github/installation/fixture");
            assert_eq!(permit.into_call_id(), "typed-call");
        }
        other => panic!("expected fresh typed permission, got {other:?}"),
    }
    assert!(matches!(repo.admit_provider_request("typed-provider", &provider_policy(), "typed-call"), ProviderAdmission::Retained));
    assert!(matches!(repo.admit_provider_request("typed-duplicate", &provider_policy(), "typed-call"), ProviderAdmission::Retained));
    assert!(matches!(repo.admit_provider_request("typed-deferred", &provider_policy(), "new-call"), ProviderAdmission::Deferred { .. }));
    assert!(matches!(repo.admit_provider_request("typed-uncertain", &json!({}), "unbound-call"), ProviderAdmission::Uncertain { .. }));
}
