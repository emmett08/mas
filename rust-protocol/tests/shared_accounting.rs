#![cfg(feature = "postgres-repository")]
//! Migration and clock rollback acceptance against a real PostgreSQL fixture.
use mas_protocol_codec::{
    postgres_repository::{AuthorityBinding, PostgresRepository},
    repository::ProtocolRepository,
};
use postgres::{Client, NoTls};
use serde_json::json;

#[test]
#[ignore = "requires MAS_TEST_POSTGRES_DSN and actual PostgreSQL"]
fn legacy_unknown_time_is_marked_and_clock_rollback_rolls_back_every_charge() {
    let dsn = std::env::var("MAS_TEST_POSTGRES_DSN").unwrap();
    let schema = format!("mas_accounting_{}", std::process::id());
    let binding = AuthorityBinding {
        deployment: "deployment".into(),
        controller: "controller".into(),
        epoch: 1,
    };
    let mut repository =
        PostgresRepository::initialise(Client::connect(&dsn, NoTls).unwrap(), &schema, binding)
            .unwrap();
    repository.install_host_repository().unwrap();
    repository
        .create_run(
            "legacy",
            &json!({"agents":["a","b"],"application":"agent-task/1","max_calls":10}),
        )
        .unwrap();
    repository.reserve_call("legacy", "b", "old").unwrap();
    let mut admin = Client::connect(&dsn, NoTls).unwrap();
    admin
        .batch_execute(&format!("SET search_path TO {schema},pg_catalog"))
        .unwrap();
    // Seed the exact v2 host journal shape. No historical time field existed.
    admin.batch_execute("INSERT INTO host_scopes(id,max_runs,max_calls,max_microusd,runs,calls,microusd) VALUES('deployment',10,10,100,1,1,20),('principal/alice',10,10,100,1,1,20);
        INSERT INTO host_runs(run,principal,scopes,metadata,owner,owner_epoch) VALUES('legacy','alice','[\"deployment\",\"principal/alice\"]','{}','fixture-owner',1);
        INSERT INTO host_steps(run,key,kind,input,data) VALUES('legacy','step-old','invoke','{}','{\"call_id\":\"old\"}'),('legacy','step-new','invoke','{}','{\"call_id\":\"new\"}');
        INSERT INTO host_invocations(run,call_id,step_key,actor,state,reservation_microusd) VALUES('legacy','old','step-old','b','executing',20),('legacy','new','step-new','b','prepared',5);").unwrap();
    repository.install_controller_repository().unwrap();
    repository.install_service_repository().unwrap();
    repository.install_accounting_repository().unwrap();
    let observation = repository
        .host_query(
            "accounting.reconcile",
            &json!({"run_id":"legacy","call_id":"old"}),
        )
        .unwrap();
    assert!(observation["admitted_at"].is_null());
    assert_eq!(observation["timestamp_basis"], "legacy_timestamp_unknown");
    repository.host_command("install-period","accounting.configure_period",&json!({"scope":"deployment","policy":{"period_seconds":86400,"max_calls":10,"max_reserved_microusd":100,"start_epoch":0},"request_key":"policy","actor":"owner","reason":"Backfill unknown legacy time"})).unwrap();
    let periods = repository
        .host_query("accounting.periods", &json!({}))
        .unwrap();
    assert_eq!(periods[0]["calls"], 1);
    assert_eq!(periods[0]["legacy_unknown_calls"], 1);
    assert_eq!(periods[0]["legacy_unknown_reserved_microusd"], 20);
    // Move only the retained high-water mark to simulate rollback of DB wall
    // time; no public API can reset this history or select a caller timestamp.
    admin
        .execute(
            "UPDATE accounting_period_policies SET high_water=high_water+1",
            &[],
        )
        .unwrap();
    assert!(repository
        .host_command(
            "rollback-attempt",
            "admit_invocation",
            &json!({"run":"legacy","owner":"fixture-owner","call_id":"new"})
        )
        .is_err());
    let charged: i64 = admin
        .query_one("SELECT calls FROM host_scopes WHERE id='deployment'", &[])
        .unwrap()
        .get(0);
    let native: i64 = admin
        .query_one("SELECT count(*)::bigint FROM calls WHERE run='legacy'", &[])
        .unwrap()
        .get(0);
    let permanent: i64 = admin
        .query_one(
            "SELECT count(*)::bigint FROM accounting_calls WHERE run='legacy'",
            &[],
        )
        .unwrap()
        .get(0);
    assert_eq!((charged, native, permanent), (1, 1, 1));
    admin
        .batch_execute(&format!("DROP SCHEMA {schema} CASCADE"))
        .unwrap();
}
