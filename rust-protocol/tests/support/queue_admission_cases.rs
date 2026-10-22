//! Actual native queue budgets, independently checked against retained records.
use super::*;
use mas_protocol_codec::shared_scheduler::policy_digest;
use sha2::{Digest, Sha256};
fn hash(value: &str) -> String {
    Sha256::digest(value.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn limits(bytes: u64, work: u64, cost: u64) -> Value {
    json!({"max_retained_input_bytes":bytes,"max_pending_work_units":work,"max_pending_estimated_cost_microusd":cost})
}
fn policy(global: Value, principal: Value) -> Value {
    json!({"format":"mas-scheduler-policy/2","revision":0,"max_workers":2,"max_per_principal":2,"workflows":{},"resources":{},"queue_admission":{"format":"mas-queue-admission/1","max_accounting_records":20,"global":global,"principals":{"alice":principal}}})
}
fn setup(f: &Fixture, policy: &Value) -> (PostgresRepository, String) {
    let mut repo = host_setup(f);
    repo.install_controller_repository().unwrap();
    repo.install_service_repository().unwrap();
    repo.install_accounting_repository().unwrap();
    repo.install_recovery_repository().unwrap();
    repo.install_jobs_repository().unwrap();
    repo.install_command_repository(None).unwrap();
    repo.install_retention_repository().unwrap();
    repo.renew_authority(3600).unwrap();
    let package = json!({"format":"mas-native-implementation/1","bundle_digest":"a".repeat(64),"files":[{"path":"operation.rs","sha256":hash("fixture"),"bytes":b"fixture".to_vec()}]});
    let text = package.to_string();
    let implementation = hash(&text);
    repo.host_command(
        "queue-package-chunk",
        "definition.chunk",
        &json!({"digest":implementation,"part":0,"data":text}),
    )
    .unwrap();
    repo.host_command(
        "queue-package",
        "definition.manifest",
        &json!({"digest":implementation,"parts":1}),
    )
    .unwrap();
    let definition = json!({"implementation_digest":implementation,"bundle_digest":"a".repeat(64),"resource_keys":[],"admission_estimate":{"work_units":5,"estimated_cost_microusd":7,"max_input_bytes":64},"timing_policy":{"format":"mas-timing-policy/1","business_seconds":600,"active_seconds":300,"queue_seconds":600,"pause_seconds":600}});
    let identity = hash(&definition.to_string());
    repo.host_command("queue-definition","definition.register",&json!({"kind":"workflow","name":"budget-native","identity":identity,"descriptor":definition,"implementation_digest":implementation,"current":true})).unwrap();
    repo.host_command(
        "queue-task-policy",
        "task.configure",
        &json!({"max_tasks":20,"max_pending":20,"max_revisions":4}),
    )
    .unwrap();
    repo.host_command(
        "queue-principal-policy",
        "task.configure_principal",
        &json!({"principal":"alice","max_tasks":20,"max_pending":20}),
    )
    .unwrap();
    repo.host_command(
        "queue-scheduler",
        "task.configure_scheduler",
        &json!({"policy":policy}),
    )
    .unwrap();
    (repo, identity)
}
fn submit(
    repo: &mut PostgresRepository,
    id: &str,
    identity: &str,
) -> std::result::Result<Value, String> {
    repo.host_command(&format!("submit-{id}"),"task.submit",&json!({"principal":"alice","namespace":"mas-unified/1","epoch":"queue-budget-epoch","key":id,"digest":hash(id),"task":id,"workflow":"budget-native","identity":identity,"expected_identity":identity,"input":{},"metadata":{},"estimated_cost_microusd":0,"work_units":0}))
}
fn complete(
    repo: &mut PostgresRepository,
    id: &str,
    command: &str,
    state: &str,
    error: Value,
) -> Value {
    repo.host_command(command,"task.complete",&json!({"principal":"alice","task":id,"owner":"owner-one","state":state,"result":null,"error":error})).unwrap()
}
fn pause(repo: &mut PostgresRepository, id: &str, suffix: &str) -> String {
    let claimed = repo
        .host_command(
            &format!("claim-{suffix}"),
            "task.claim",
            &json!({"owner":"owner-one","task":id}),
        )
        .unwrap();
    complete(
        repo,
        id,
        &format!("pause-{suffix}"),
        "input_required",
        Value::Null,
    );
    claimed["receipt"]["run_id"].as_str().unwrap().into()
}
#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn queued_budget_concurrent_last_credit_is_atomic_global_and_per_principal() {
    for tenant_bound in [false, true] {
        let fixture = Fixture::new();
        let global = if tenant_bound {
            limits(1000, 100, 100)
        } else {
            limits(1000, 5, 7)
        };
        let tenant = if tenant_bound {
            limits(1000, 5, 7)
        } else {
            limits(1000, 100, 100)
        };
        let (_repo, identity) = setup(&fixture, &policy(global, tenant));
        let barrier = Arc::new(Barrier::new(2));
        let mut workers = Vec::new();
        for id in 0..2 {
            let mut repo = fixture.open();
            let barrier = barrier.clone();
            let identity = identity.clone();
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                submit(&mut repo, &format!("racing-{id}"), &identity)
            }));
        }
        let results: Vec<_> = workers.into_iter().map(|t| t.join().unwrap()).collect();
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(
            fixture
                .admin()
                .query_one("SELECT count(*) FROM service_tasks", &[])
                .unwrap()
                .get::<_, i64>(0),
            1
        );
        assert_eq!(
            fixture
                .admin()
                .query_one("SELECT count(*) FROM service_executions", &[])
                .unwrap()
                .get::<_, i64>(0),
            1
        );
    }
}
#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn queued_budget_unknown_obligation_and_retained_input_are_not_refunded() {
    let fixture = Fixture::new();
    let (mut repo, identity) = setup(&fixture, &policy(limits(1000, 5, 7), limits(1000, 5, 7)));
    submit(&mut repo, "original", &identity).unwrap();
    fixture.admin().batch_execute("UPDATE service_tasks SET state='failed'; UPDATE service_executions SET state='unknown'").unwrap();
    assert!(submit(&mut repo, "while-unknown", &identity).is_err());
    assert_eq!(
        fixture
            .admin()
            .query_one("SELECT count(*) FROM service_tasks", &[])
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    // Controlled fixture resolution proves the estimate is pending-work credit;
    // native call/cost charges belong to their separate, unchanged accounting.
    fixture
        .admin()
        .batch_execute("UPDATE service_executions SET state='completed'")
        .unwrap();
    submit(&mut repo, "after-resolution", &identity).unwrap();
    let second = Fixture::new();
    let (mut repo, identity) = setup(&second, &policy(limits(4, 100, 100), limits(4, 100, 100)));
    submit(&mut repo, "retained", &identity).unwrap();
    second.admin().batch_execute("UPDATE service_tasks SET state='completed'; UPDATE service_executions SET state='completed'").unwrap();
    assert!(submit(&mut repo, "bytes-still-retained", &identity).is_err());
    assert_eq!(second.admin().query_one("SELECT (SELECT sum(octet_length(input)) FROM service_tasks)+(SELECT sum(octet_length(input)) FROM service_executions)",&[]).unwrap().get::<_,i64>(0),4);
    second.admin().batch_execute("UPDATE service_tasks SET archived_at=1,input='{}'; UPDATE service_executions SET input='{}'").unwrap();
    submit(&mut repo, "after-payload-retirement", &identity).unwrap();
}
#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn queued_budget_amendment_preserves_debt_and_update_accounts_old_input() {
    let fixture = Fixture::new();
    let original = policy(limits(6, 5, 7), limits(6, 5, 7));
    let (mut repo, identity) = setup(&fixture, &original);
    submit(&mut repo, "updatable", &identity).unwrap();
    pause(&mut repo, "updatable", "first");
    let update = |revision: i64| json!({"principal":"alice","task":"updatable","key":format!("update-{revision}"),"digest":hash(&format!("update-{revision}")),"expected_revision":revision,"expected_identity":identity,"input":{}});
    repo.host_command("update-one", "task.update", &update(0))
        .unwrap();
    pause(&mut repo, "updatable", "second");
    assert!(repo
        .host_command("update-two", "task.update", &update(1))
        .is_err());
    assert_eq!(
        repo.host_query(
            "task.inspect",
            &json!({"principal":"alice","task":"updatable"})
        )
        .unwrap()["revision"],
        1
    );
    assert_eq!(
        fixture
            .admin()
            .query_one("SELECT count(*) FROM service_executions", &[])
            .unwrap()
            .get::<_, i64>(0),
        2
    );
    let mut lower = original.clone();
    lower["revision"] = json!(1);
    lower["queue_admission"]["global"] = limits(0, 0, 0);
    lower["queue_admission"]["max_accounting_records"] = json!(1);
    let amendment = json!({"expected_policy_digest":policy_digest(&original),"expected_revision":0,"policy":lower,"reason":"drain without deleting debt","authoriser":"operator"});
    repo.host_command("lower-queue-budget", "task.amend_scheduler", &amendment)
        .unwrap();
    assert_eq!(
        repo.host_command("lower-queue-budget", "task.amend_scheduler", &amendment)
            .unwrap()["replayed"],
        true
    );
    assert!(submit(&mut repo, "blocked-after-lower", &identity).is_err());
    assert_eq!(
        fixture
            .admin()
            .query_one("SELECT count(*) FROM service_tasks", &[])
            .unwrap()
            .get::<_, i64>(0),
        1
    );
    let mut removed = lower.clone();
    removed["revision"] = json!(2);
    removed.as_object_mut().unwrap().remove("queue_admission");
    assert!(repo.host_command("remove-queue-budget","task.amend_scheduler",&json!({"expected_policy_digest":policy_digest(&lower),"expected_revision":1,"policy":removed,"reason":"cannot reset outstanding debt","authoriser":"operator"})).is_err());
}
#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn deferred_provider_task_reuses_original_execution_only_after_hint_due() {
    let fixture = Fixture::new();
    let (mut repo, identity) = setup(
        &fixture,
        &policy(limits(1000, 100, 100), limits(1000, 100, 100)),
    );
    submit(&mut repo, "deferred", &identity).unwrap();
    let first = repo
        .host_command(
            "first-claim",
            "task.claim",
            &json!({"task":"deferred","owner":"owner-one"}),
        )
        .unwrap();
    let run = first["receipt"]["run_id"].clone();
    let now: i64 = fixture
        .admin()
        .query_one(
            "SELECT floor(extract(epoch FROM clock_timestamp())*1000)::bigint",
            &[],
        )
        .unwrap()
        .get(0);
    complete(
        &mut repo,
        "deferred",
        "retain-deferral",
        "blocked",
        json!({"code":"native_provider_deferred","not_before_ms":now+60000}),
    );
    assert_eq!(
        repo.host_query("task.claimable", &json!({"task":"deferred"}))
            .unwrap(),
        false
    );
    assert_eq!(
        repo.host_command(
            "early-claim",
            "task.claim",
            &json!({"task":"deferred","owner":"owner-one"})
        )
        .unwrap()["receipt"],
        Value::Null
    );
    assert_eq!(
        fixture
            .admin()
            .query_one("SELECT count(*) FROM service_slots", &[])
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    // Controlled ageing without wall-clock sleeps. The original execution and
    // prepared native call are retained; eligibility is never a dispatch permit.
    fixture
        .admin()
        .execute(
            "UPDATE service_tasks SET error=$1 WHERE id='deferred'",
            &[&json!({"code":"native_provider_deferred","not_before_ms":0}).to_string()],
        )
        .unwrap();
    assert_eq!(
        repo.host_query("task.claimable", &json!({"task":"deferred"}))
            .unwrap(),
        true
    );
    let next = repo
        .host_command(
            "due-claim",
            "task.claim",
            &json!({"task":"deferred","owner":"owner-one"}),
        )
        .unwrap();
    assert_eq!(next["receipt"]["run_id"], run);
    assert_eq!(next["receipt"]["revision"], 0);
    assert!(repo.host_command("invalid-deferral","task.complete",&json!({"principal":"alice","task":"deferred","owner":"owner-one","state":"blocked","result":null,"error":{"code":"native_provider_deferred","not_before_ms":"invalid"}})).is_err());
    assert_eq!(
        fixture
            .admin()
            .query_one("SELECT count(*) FROM service_executions", &[])
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and an actual PostgreSQL server"]
fn queued_budget_cannot_hide_native_unknown_debt_behind_task_completion_or_update() {
    use mas_protocol_codec::shared_native::NativeAdmission;
    let fixture = Fixture::new();
    let (mut repo, identity) = setup(&fixture, &policy(limits(1000, 5, 7), limits(1000, 5, 7)));
    submit(&mut repo, "native-unknown", &identity).unwrap();
    fixture
        .admin()
        .batch_execute("UPDATE service_executions SET run='r' WHERE task='native-unknown'")
        .unwrap();
    repo.host_command(
        "claim-native-unknown",
        "task.claim",
        &json!({"owner":"owner-one","task":"native-unknown"}),
    )
    .unwrap();
    prepare(&mut repo, "budget-native-step", "budget-native-call", 0);
    assert!(matches!(
        repo.admit_native_invocation(
            "budget-native-admit",
            "r",
            "owner-one",
            "budget-native-call"
        ),
        NativeAdmission::FreshDispatch(_)
    ));
    repo.host_command("native-lost-response","observe_invocation",&json!({"run":"r","owner":"owner-one","call_id":"budget-native-call","state":"unknown","output":null,"evidence":{"fixture":"lost admitted response"}})).unwrap();
    // A task envelope can be settled independently of its original native
    // invocation. It must never return unresolved estimate credit.
    complete(
        &mut repo,
        "native-unknown",
        "misleading-task-completion",
        "input_required",
        Value::Null,
    );
    assert!(repo.host_command("update-unknown-native","task.update",&json!({"principal":"alice","task":"native-unknown","key":"new-input","digest":hash("new-input"),"expected_revision":0,"expected_identity":identity,"input":{}})).is_err());
    assert!(submit(&mut repo, "new-budget-consumer", &identity).is_err());
    assert_eq!(
        fixture
            .admin()
            .query_one(
                "SELECT revision FROM service_tasks WHERE id='native-unknown'",
                &[]
            )
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    assert_eq!(
        repo.host_query("scope", &json!({"scope":"deployment"}))
            .unwrap()["calls"],
        1
    );
    // Even controlled payload retirement cannot replenish that unresolved debt.
    fixture.admin().batch_execute("UPDATE service_tasks SET archived_at=1,input='{}' WHERE id='native-unknown'; UPDATE service_executions SET input='{}' WHERE task='native-unknown'").unwrap();
    assert!(submit(&mut repo, "after-retirement-unknown", &identity).is_err());
}
