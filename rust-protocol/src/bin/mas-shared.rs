//! Private controller command channel. Never expose this host-level interface
//! directly to application actors or an unauthenticated network transport.
use mas_protocol_codec::{
    encode_message,
    gateway::Gateway,
    parse_json,
    postgres_repository::{AuthorityBinding, PostgresRepository},
    repository::ProtocolRepository,
    MAX_BYTES, MAX_DEPTH,
};
use postgres::{config::Host, Client, Config, NoTls};
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
    let config =
        Config::from_str(&dsn).map_err(|_| "invalid PostgreSQL connection configuration")?;
    // NoTls is deliberately restricted to local sockets/loopback. Deployments
    // requiring direct remote TLS construct a verified Client through the Rust
    // repository API or place their authenticated TLS tunnel on this boundary.
    if config.get_hostaddrs().iter().any(|ip| !ip.is_loopback())
        || config.get_hosts().iter().any(|host| match host {
            Host::Tcp(name) => name != "localhost" && name != "127.0.0.1" && name != "::1",
            #[cfg(unix)]
            Host::Unix(_) => false,
        })
    {
        return Err("this command transport requires a local PostgreSQL endpoint".into());
    }
    let client: Client = config
        .connect(NoTls)
        .map_err(|_| "PostgreSQL unavailable")?;
    let binding = AuthorityBinding {
        deployment: field(&request["binding"], "deployment")?.into(),
        controller: field(&request["binding"], "controller")?.into(),
        epoch: request["binding"]["epoch"]
            .as_i64()
            .ok_or("invalid epoch")?,
    };
    let operation = field(&request, "operation")?;
    let schema = field(&request, "schema")?;
    let mut repository = if operation == "initialise" {
        PostgresRepository::initialise(client, schema, binding)?
    } else {
        PostgresRepository::open(client, schema, binding)?
    };
    match operation {
        "initialise" | "install_host" => {
            repository.install_host_repository()?;
            Ok(json!({"installed":true}))
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
        .take((MAX_BYTES + 1) as u64)
        .read_to_end(&mut source)
        .map_err(|_| "input unavailable".to_owned())
        .and_then(|_| {
            if source.len() > MAX_BYTES {
                return Err("shared command too large".into());
            }
            let text = std::str::from_utf8(&source).map_err(|_| "invalid input encoding")?;
            execute(parse_json(text).map_err(|_| "invalid shared command")?)
        });
    let success = result.is_ok();
    let response = match result {
        Ok(result) => json!({"version":1,"ok":true,"result":result}),
        Err(_) => json!({"version":1,"ok":false,"error":{"code":"shared_authority_rejected"}}),
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
