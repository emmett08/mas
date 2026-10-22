use super::*;
use mas_protocol_codec::shared_native::{NativeAdmission, NativeRejection};
use sha2::{Digest, Sha256};

fn hash(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}
fn setup(f: &Fixture) -> (PostgresRepository, Value) {
    let mut repo = host_setup(f);
    repo.install_controller_repository().unwrap();
    repo.install_service_repository().unwrap();
    repo.install_provider_quota_index().unwrap();
    repo.renew_authority(3600).unwrap();
    let policy = json!({"format":"mas-provider-circuit/2","resource_key":"registered-provider","failure_threshold":2,"open_seconds":1,"half_open_seconds":1,"window_ms":86_400_000,"max_requests":1});
    let bytes = b"registered-public-read-source";
    let package = json!({"format":"mas-native-implementation/1","bundle_digest":"a".repeat(64),"files":[{"path":"read.rs","sha256":hash(bytes),"bytes":bytes.to_vec()}]});
    let text = package.to_string();
    let implementation = hash(text.as_bytes());
    repo.host_command(
        "package-chunk",
        "definition.chunk",
        &json!({"digest":implementation,"part":0,"data":text}),
    )
    .unwrap();
    repo.host_command(
        "package-manifest",
        "definition.manifest",
        &json!({"digest":implementation,"parts":1}),
    )
    .unwrap();
    let definition = json!({"implementation_digest":implementation,"bundle_digest":"a".repeat(64),"resource_keys":[],"provider_quota":policy,"timing_policy":{"format":"mas-timing-policy/1","business_seconds":600,"active_seconds":300,"queue_seconds":600,"pause_seconds":600}});
    let identity = hash(definition.to_string().as_bytes());
    repo.host_command("definition","definition.register",&json!({"kind":"workflow","name":"public-read","identity":identity,"descriptor":definition,"implementation_digest":implementation,"current":true})).unwrap();
    repo.host_command(
        "task-policy",
        "task.configure",
        &json!({"max_tasks":4,"max_pending":4,"max_revisions":1}),
    )
    .unwrap();
    repo.host_command(
        "principal-policy",
        "task.configure_principal",
        &json!({"principal":"alice","max_tasks":4,"max_pending":4}),
    )
    .unwrap();
    repo.host_command(
        "scheduler",
        "task.configure_scheduler",
        &json!({"policy":{"max_workers":2,"max_per_principal":2,"workflows":{},"resources":{}}}),
    )
    .unwrap();
    repo.host_command("submit","task.submit",&json!({"principal":"alice","namespace":"mas-unified/1","epoch":"test-epoch","key":"original-request","digest":"b".repeat(64),"task":"provider-task","workflow":"public-read","identity":identity,"expected_identity":identity,"input":{},"metadata":{}})).unwrap();
    // Fixture association with host_setup's existing frozen native run. The
    // real HTTP/CLI integration separately exercises generated run identities.
    f.admin()
        .execute(
            "UPDATE service_executions SET run='r' WHERE task='provider-task'",
            &[],
        )
        .unwrap();
    repo.host_command(
        "claim-task",
        "task.claim",
        &json!({"owner":"owner-one","task":"provider-task"}),
    )
    .unwrap();
    (repo, policy)
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and actual PostgreSQL"]
fn registered_provider_deferral_preserves_prepared_native_identity_and_all_charges() {
    let fixture = Fixture::new();
    let (mut repo, policy) = setup(&fixture);
    repo.host_command(
        "upstream-credit",
        "circuit.admit",
        &json!({"policy":policy,"call_id":"prior-provider-request"}),
    )
    .unwrap();
    prepare(&mut repo, "provider-step", "provider-call", 10);
    assert!(matches!(
        repo.admit_native_invocation("provider-admit", "r", "owner-one", "provider-call"),
        NativeAdmission::Rejected {
            code: NativeRejection::ProviderDeferred { .. }
        }
    ));
    assert_eq!(
        repo.host_query("scope", &json!({"scope":"deployment"}))
            .unwrap()["calls"],
        0
    );
    assert_eq!(
        repo.host_query("invocation", &json!({"run":"r","call_id":"provider-call"}))
            .unwrap()["state"],
        "prepared"
    );
    assert!(repo
        .host_query("command_receipt", &json!({"command_id":"provider-admit"}))
        .unwrap()
        .is_null());
    assert!(repo
        .host_command(
            "raw-bypass",
            "admit_invocation",
            &json!({"run":"r","owner":"owner-one","call_id":"provider-call"})
        )
        .is_err());
    fixture
        .admin()
        .execute(
            "UPDATE provider_permits SET created_ms=created_ms-86400000",
            &[],
        )
        .unwrap();
    let admitted =
        repo.admit_native_invocation("provider-admit", "r", "owner-one", "provider-call");
    let NativeAdmission::FreshDispatch(permit) = admitted else {
        panic!("{admitted:?}")
    };
    assert_eq!(permit.provider_policy(), Some(&policy));
    assert_eq!(permit.into_call_id(), "provider-call");
    assert!(matches!(
        repo.admit_native_invocation("provider-admit", "r", "owner-one", "provider-call"),
        NativeAdmission::Retained { .. }
    ));
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and actual PostgreSQL"]
fn provider_and_native_reservations_roll_back_together_then_lost_reply_stays_retained() {
    let fixture = Fixture::new();
    let (mut repo, _) = setup(&fixture);
    prepare(&mut repo, "provider-step", "provider-call", 10);
    fixture.admin().batch_execute("CREATE FUNCTION refuse_native() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected-native-write-failure'; END $$; CREATE TRIGGER refuse_native BEFORE INSERT ON calls FOR EACH ROW EXECUTE FUNCTION refuse_native();").unwrap();
    assert!(matches!(
        repo.admit_native_invocation("atomic-provider", "r", "owner-one", "provider-call"),
        NativeAdmission::Uncertain { .. }
    ));
    let count: i64 = fixture
        .admin()
        .query_one("SELECT count(*) FROM provider_permits", &[])
        .unwrap()
        .get(0);
    assert_eq!(
        count, 0,
        "provider reservation escaped rolled-back native admission"
    );
    assert_eq!(
        repo.host_query("scope", &json!({"scope":"deployment"}))
            .unwrap()["calls"],
        0
    );
    fixture
        .admin()
        .batch_execute("DROP TRIGGER refuse_native ON calls; DROP FUNCTION refuse_native();")
        .unwrap();
    let _lost = repo.admit_native_invocation("atomic-provider", "r", "owner-one", "provider-call");
    drop(repo);
    let mut repo = fixture.open();
    assert!(matches!(
        repo.admit_native_invocation("atomic-provider", "r", "owner-one", "provider-call"),
        NativeAdmission::Retained { .. }
    ));
    assert_eq!(
        fixture
            .admin()
            .query_one("SELECT count(*) FROM provider_permits", &[])
            .unwrap()
            .get::<_, i64>(0),
        1
    );
}

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and actual PostgreSQL"]
fn concurrent_native_admissions_share_one_registered_provider_credit() {
    let fixture = Fixture::new();
    let (mut repo, _) = setup(&fixture);
    prepare(&mut repo, "one", "provider-one", 10);
    prepare(&mut repo, "two", "provider-two", 10);
    drop(repo);
    let barrier = Arc::new(Barrier::new(2));
    let outcomes = std::thread::scope(|scope| {
        let mut threads = Vec::new();
        for call in ["provider-one", "provider-two"] {
            let barrier = barrier.clone();
            let fixture = &fixture;
            threads.push(scope.spawn(move || {
                let mut repo = fixture.open();
                barrier.wait();
                match repo.admit_native_invocation(&format!("admit-{call}"), "r", "owner-one", call)
                {
                    NativeAdmission::FreshDispatch(_) => "fresh",
                    NativeAdmission::Rejected {
                        code: NativeRejection::ProviderDeferred { .. },
                    } => "deferred",
                    other => panic!("{other:?}"),
                }
            }));
        }
        threads
            .into_iter()
            .map(|t| t.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|s| **s == "fresh").count(), 1);
    assert_eq!(outcomes.iter().filter(|s| **s == "deferred").count(), 1);
    assert_eq!(
        fixture
            .open()
            .host_query("scope", &json!({"scope":"deployment"}))
            .unwrap()["calls"],
        1
    );
}
