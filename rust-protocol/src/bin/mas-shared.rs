//! Private controller command channel. Never expose this host-level interface
//! directly to application actors or an unauthenticated network transport.
use mas_protocol_codec::{
    encode_message,
    gateway::Gateway,
    parse_shared_command,
    postgres_repository::{AuthorityBinding, PostgresRepository},
    repository::ProtocolRepository,
    MAX_BYTES, MAX_DEPTH, SHARED_COMMAND_BYTES,
};
use postgres::{
    config::{Host, SslMode},
    Client, Config, NoTls,
};
use postgres_native_tls::MakeTlsConnector;
use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    str::FromStr,
};
fn field<'a>(v: &'a Value, k: &str) -> Result<&'a str, String> {
    v[k].as_str().ok_or_else(|| format!("invalid {k}"))
}
fn execute(request: Value) -> Result<Value, String> {
    if request["version"].as_u64() != Some(1) {
        return Err("unsupported shared command protocol".into());
    }
    let dsn =
        std::env::var("MAS_POSTGRES_DSN").map_err(|_| "PostgreSQL connection not configured")?;
    let mut config =
        Config::from_str(&dsn).map_err(|_| "invalid PostgreSQL connection configuration")?;
    // Remote endpoints always require verified TLS. A local Unix/loopback
    // endpoint may be a trusted fixture or authenticated tunnel. No option can
    // disable certificate or hostname validation on a remote connection.
    let remote = config.get_hostaddrs().iter().any(|ip| !ip.is_loopback())
        || config.get_hosts().iter().any(|host| match host {
            Host::Tcp(name) => name != "localhost" && name != "127.0.0.1" && name != "::1",
            #[cfg(unix)]
            Host::Unix(_) => false,
        });
    let mut client: Client = if remote || config.get_ssl_mode() == SslMode::Require {
        if remote && config.get_ssl_mode() == SslMode::Disable {
            return Err("remote PostgreSQL requires TLS".into());
        }
        config.ssl_mode(SslMode::Require);
        let mut tls = native_tls::TlsConnector::builder();
        if let Ok(path) = std::env::var("MAS_POSTGRES_CA_FILE") {
            let file = std::fs::File::open(path).map_err(|_| "PostgreSQL CA unavailable")?;
            let mut bytes = Vec::new();
            file.take(1_048_577)
                .read_to_end(&mut bytes)
                .map_err(|_| "PostgreSQL CA unavailable")?;
            if bytes.len() > 1_048_576 {
                return Err("PostgreSQL CA too large".into());
            }
            tls.add_root_certificate(
                native_tls::Certificate::from_pem(&bytes).map_err(|_| "invalid PostgreSQL CA")?,
            );
        }
        let tls = tls.build().map_err(|_| "PostgreSQL TLS unavailable")?;
        config
            .connect(MakeTlsConnector::new(tls))
            .map_err(|_| "PostgreSQL unavailable")?
    } else {
        config
            .connect(NoTls)
            .map_err(|_| "PostgreSQL unavailable")?
    };
    let binding = AuthorityBinding {
        deployment: field(&request["binding"], "deployment")?.into(),
        controller: field(&request["binding"], "controller")?.into(),
        epoch: request["binding"]["epoch"]
            .as_i64()
            .ok_or("invalid epoch")?,
    };
    let operation = field(&request, "operation")?;
    let schema = field(&request, "schema")?;
    if operation == "recovery_target" {
        return mas_protocol_codec::shared_backup::target(&mut client);
    }
    let mut repository = if operation == "initialise" {
        PostgresRepository::initialise(client, schema, binding)?
    } else {
        PostgresRepository::open(client, schema, binding)?
    };
    match operation {
        "initialise" | "install_host" => {
            repository.install_host_repository()?;
            repository.install_controller_repository()?;
            repository.install_service_repository()?;
            repository.install_accounting_repository()?;
            repository.install_recovery_repository()?;
            repository.install_jobs_repository()?;
            repository.install_command_repository(request.get("command_capacity"))?;
            repository.install_retention_repository()?;
            repository.install_archive_repository()?;
            repository.install_transfer_repository()?;
            repository.install_artifact_retention_repository()?;
            Ok(json!({"installed":true}))
        }
        "recovery_seal" => repository.recovery_seal(&request["parameters"]),
        "recovery_inspect" => repository.recovery_inspect(),
        "recovery_objects" => repository.recovery_objects(
            field(&request, "after_namespace")?,
            field(&request, "after_digest")?,
        ),
        "recovery_audit" => repository.recovery_audit(&request["parameters"]),
        "recovery_activate" => repository.recovery_activate(&request["parameters"]),
        "authority_status" => repository.authority_status(),
        "renew" => repository.renew_authority(
            request["lease_seconds"]
                .as_i64()
                .ok_or("invalid lease interval")?,
        ),
        "handover" | "takeover" => {
            let next = field(&request, "next_controller")?;
            let b = if operation == "handover" {
                repository.handover(next)?
            } else {
                repository.takeover(
                    next,
                    request["lease_seconds"]
                        .as_i64()
                        .ok_or("invalid lease interval")?,
                )?
            };
            Ok(json!({"deployment":b.deployment,"controller":b.controller,"epoch":b.epoch}))
        }
        "command" => repository.host_command(
            field(&request, "command_id")?,
            field(&request, "command")?,
            &request["parameters"],
        ),
        "query" => repository.host_query(field(&request, "query")?, &request["parameters"]),
        "status" => repository.inspect(field(&request, "run")?),
        "reconcile" => repository.reconcile(field(&request, "run")?),
        "encode" => encode_message(field(&request, "source")?, MAX_BYTES, MAX_DEPTH)
            .map(|text| json!({"text":text}))
            .map_err(|_| "invalid protocol message".into()),
        "gateway" => {
            let mut gateway = Gateway::open(repository, &request["configuration"])?;
            let reply = gateway.handle_frame(field(&request, "frame")?.as_bytes());
            if reply.response["ok"] == true {
                Ok(reply.response["result"].clone())
            } else {
                Err("gateway operation rejected".into())
            }
        }
        "validate_gateway" => {
            Gateway::open(repository, &request["configuration"])?;
            Ok(json!({"validated":true}))
        }
        _ => Err("unsupported shared host operation".into()),
    }
}
fn main() {
    let mut source = Vec::new();
    let result = std::io::stdin()
        .take((SHARED_COMMAND_BYTES + 1) as u64)
        .read_to_end(&mut source)
        .map_err(|_| "input unavailable".to_owned())
        .and_then(|_| {
            if source.len() > SHARED_COMMAND_BYTES {
                return Err("shared command too large".into());
            }
            let text = std::str::from_utf8(&source).map_err(|_| "invalid input encoding")?;
            execute(parse_shared_command(text).map_err(|_| "invalid shared command")?)
        });
    let success = result.is_ok();
    let response = match result {
        Ok(result) => json!({"version":1,"ok":true,"result":result}),
        Err(error) => {
            let code = match error.as_str() {
                "stale protocol authority" => "stale_protocol_authority",
                "controller_lease_expired" => "controller_lease_expired",
                "controller_admission_closed"
                | "event_ack_conflict"
                | "event_policy_changed"
                | "event_stage_conflict"
                | "invalid_delivery_body"
                | "invalid_subscription"
                | "invalid_worker_control"
                | "invalid_worker_grants"
                | "invalid_worker_output_contract"
                | "invalid_worker_receipt"
                | "invalid_worker_request"
                | "invocation_binding_conflict"
                | "invocation_not_admitted"
                | "not_found"
                | "forbidden"
                | "receipt_conflict"
                | "receipt_limit"
                | "receipt_without_dispatch"
                | "remote_resolution_not_ready"
                | "stale_worker_assignment"
                | "stale_worker_epoch"
                | "subscription_conflict"
                | "subscription_limit"
                | "subscription_not_found"
                | "unsupported_worker_output_contract"
                | "unsupported_worker_protocol"
                | "unsupported_worker_runtime"
                | "worker_artifact_storage_unconfigured"
                | "worker_artifact_unavailable"
                | "worker_capability_denied"
                | "worker_control_conflict"
                | "worker_control_history_full"
                | "worker_control_revision_conflict"
                | "worker_forbidden"
                | "worker_output_contract_mismatch"
                | "worker_policy_changed"
                | "worker_queue_full"
                | "worker_registration_conflict"
                | "principal_quota_unavailable"
                | "principal_task_limit"
                | "principal_pending_limit"
                | "principal_quota_changed"
                | "scheduler_assignment_conflict"
                | "scheduler_policy_changed"
                | "scheduler_capacity"
                | "invalid_scheduler_policy"
                | "invalid_budget_amendment"
                | "budget_amendment_conflict"
                | "budget_revision_conflict"
                | "budget_history_full"
                | "budget_must_increase"
                | "invalid_archive_kind"
                | "invalid_graph_selector"
                | "command_result_archived"
                | "run_archived"
                | "run_not_archivable"
                | "legacy_archive_mapping_required"
                | "invalid_retention_request"
                | "retention_request_conflict"
                | "task_not_archivable"
                | "workflow_not_archivable"
                | "event_cursor_expired"
                | "invalid_epoch_retirement"
                | "payload_archived"
                | "worker_job_archived"
                | "pending_delivery_retained"
                | "run_not_settled"
                | "workflow_changed"
                | "artifact_staging_full"
                | "artifact_unavailable"
                | "artifact_chunk_conflict"
                | "invalid_artifact_chunk"
                | "artifact_integrity_mismatch"
                | "artifact_configuration_conflict"
                | "artifact_target_invalid"
                | "invalid_artifact_configuration"
                | "artifact_target_denied"
                | "artifact_retention_conflict"
                | "artifact_namespace_retained"
                | "invalid_command_capacity"
                | "command_capacity_conflict"
                | "command_capacity_exhausted"
                | "recovery_capacity_exhausted"
                | "administration_capacity_exhausted"
                | "task_admission_closed"
                | "invalid_definition"
                | "definition_identity_conflict"
                | "definition_retired"
                | "definition_unavailable"
                | "definition_is_current"
                | "definition_in_use"
                | "definition_not_found"
                | "implementation_digest_mismatch"
                | "implementation_package_too_large"
                | "request_conflict"
                | "request_epoch_retired"
                | "request_epoch_in_use"
                | "invalid_request_namespace"
                | "invalid_task_state"
                | "task_not_found"
                | "task_limit"
                | "queue_full"
                | "revision_limit"
                | "workflow_identity_mismatch"
                | "reconciliation_required"
                | "service_policy_conflict"
                | "stale_task_ownership"
                | "admission_draining" => error.as_str(),
                _ => "shared_authority_rejected",
            };
            json!({"version":1,"ok":false,"error":{"code":code}})
        }
    };
    let response = response.to_string();
    if response.len() > 16 * 1024 * 1024 {
        let _ = writeln!(
            std::io::stdout(),
            "{}",
            json!({"version":1,"ok":false,"error":{"code":"response_too_large"}})
        );
        std::process::exit(1);
    }
    let _ = writeln!(std::io::stdout(), "{response}");
    if !success {
        std::process::exit(1);
    }
}
