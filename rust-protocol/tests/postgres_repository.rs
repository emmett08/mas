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
