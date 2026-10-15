//! Trusted host IPC. This is not an agent-facing or network interface.
//!
//! A bounded JSON header precedes an exact-length raw body. Headers are strict
//! protocol JSON; codec and gateway bodies retain their original parsers and
//! admission behaviour. Incomplete IPC never reaches gateway admission.
use crate::{encode_message, gateway::{normalise_config, Gateway}, parse_json, store::Store, repository::ProtocolRepository,
            ENCODE_ENVELOPE_ALLOWANCE, MAX_BYTES, MAX_DEPTH};
use serde_json::{json, Value};
use std::{io::{BufRead, Write}, path::Path, time::{SystemTime, UNIX_EPOCH}};

pub const VERSION: u64 = 1;
pub const MAX_HEADER_BYTES: usize = 1024;
pub const MAX_REQUEST_BYTES: usize = MAX_BYTES + ENCODE_ENVELOPE_ALLOWANCE;
pub const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// A single run with one retained database connection per authority role.
pub struct HostSession<S: ProtocolRepository = Store> {
    store: S,
    gateway: Gateway<S>,
    run_id: String,
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

fn header<R: BufRead>(input: &mut R) -> Result<Option<Value>, String> {
    let mut bytes = Vec::new();
    loop {
        let available = input.fill_buf().map_err(|_| "IPC read failed")?;
        if available.is_empty() {
            return if bytes.is_empty() { Ok(None) } else { Err("truncated IPC header".into()) };
        }
        let count = available.iter().position(|byte| *byte == b'\n').map(|n| n + 1).unwrap_or(available.len());
        if bytes.len() + count > MAX_HEADER_BYTES { return Err("IPC header exceeds limit".into()); }
        bytes.extend_from_slice(&available[..count]);
        input.consume(count);
        if bytes.last() == Some(&b'\n') { break; }
    }
    let source = std::str::from_utf8(&bytes).map_err(|_| "invalid IPC header")?;
    parse_json(source).map(Some).map_err(|_| "invalid IPC header".into())
}

fn write_response<W: Write>(output: &mut W, request_id: u64, value: Value, close: bool) -> Result<(), String> {
    let mut body = serde_json::to_vec(&value).map_err(|_| "IPC encode failed")?;
    // A committed operation must never be retried automatically because its
    // response is too large. Return a fixed failure without leaking its body.
    if body.len() > MAX_RESPONSE_BYTES {
        body = br#"{"ok":false,"error":{"code":"response_too_large"}}"#.to_vec();
    }
    let framing = json!({"version":VERSION,"request_id":request_id,"body_bytes":body.len(),"close":close});
    writeln!(output, "{framing}").and_then(|_| output.write_all(&body))
        .and_then(|_| output.flush()).map_err(|_| "IPC write failed".into())
}

impl HostSession {
    pub fn open(path: &Path, configuration: &Value, run_id: &str) -> Result<Self, String> {
        Self::from_repositories(Store::open(path)?, Store::open(path)?, configuration, run_id)
    }
}

impl<S: ProtocolRepository> HostSession<S> {
    /// The caller supplies two handles to the same authoritative repository.
    /// Their deployment, namespace and epoch must be identical.
    pub fn from_repositories(store: S, gateway_store: S, configuration: &Value, run_id: &str) -> Result<Self, String> {
        let normal = normalise_config(configuration, true)?;
        if !crate::identity(run_id) || normal["credentials"].as_array().unwrap().iter()
            .any(|credential| credential["run_id"].as_str() != Some(run_id)) {
            return Err("host IPC requires one run".into());
        }
        store.run_membership(run_id)?;
        let gateway = Gateway::open(gateway_store, configuration)?;
        Ok(Self { store, gateway, run_id: run_id.into() })
    }

    pub fn serve<R: BufRead, W: Write>(&mut self, input: &mut R, output: &mut W) -> Result<(), String> {
        writeln!(output, "{}", json!({"version":VERSION,"max_header_bytes":MAX_HEADER_BYTES,
            "max_request_bytes":MAX_REQUEST_BYTES,"max_response_bytes":MAX_RESPONSE_BYTES}))
            .and_then(|_| output.flush()).map_err(|_| "IPC write failed")?;
        let mut previous_id = 0;
        while let Some(frame) = header(input)? {
            let fields = frame.as_object().ok_or("invalid IPC header")?;
            let required = ["version", "request_id", "operation", "body_bytes", "deadline_ms"];
            if fields.len() != required.len() || required.iter().any(|key| !fields.contains_key(*key))
                || frame["version"].as_u64() != Some(VERSION) {
                return Err("unsupported IPC header".into());
            }
            let id = frame["request_id"].as_u64().filter(|id| *id > previous_id).ok_or("invalid IPC sequence")?;
            let size = frame["body_bytes"].as_u64().filter(|size| *size <= MAX_REQUEST_BYTES as u64).ok_or("IPC body exceeds limit")? as usize;
            let deadline = frame["deadline_ms"].as_u64().ok_or("invalid IPC deadline")?;
            let operation = frame["operation"].as_str().ok_or("invalid IPC operation")?;
            if !matches!(operation, "status" | "reconcile" | "encode" | "gateway" | "close")
                || (matches!(operation, "status" | "reconcile" | "close") && size != 0)
                || (operation == "gateway" && size > MAX_BYTES) {
                return Err("invalid IPC operation".into());
            }
            let mut body = vec![0; size];
            input.read_exact(&mut body).map_err(|_| "truncated IPC body")?;
            previous_id = id;
            let now = now_ms();
            if deadline <= now || deadline > now.saturating_add(120_000) {
                write_response(output, id, json!({"ok":false,"error":{"code":"ipc_deadline"}}), false)?;
                continue;
            }
            let mut close = operation == "close";
            let value = if operation == "gateway" {
                let reply = self.gateway.handle_frame(&body);
                close = reply.close;
                reply.response
            } else {
                let result = match operation {
                    "status" => self.store.inspect(&self.run_id),
                    "reconcile" => self.store.reconcile(&self.run_id),
                    "encode" => std::str::from_utf8(&body).map_err(|_| "invalid encoding".into())
                        .and_then(|source| encode_message(source, MAX_BYTES, MAX_DEPTH)
                            .map(|wire| json!({"text":wire})).map_err(|_| "invalid message".into())),
                    "close" => Ok(json!({"closed":true})),
                    _ => unreachable!(),
                };
                match result {
                    Ok(result) => json!({"ok":true,"result":result}),
                    Err(_) => json!({"ok":false,"error":{"code":"operation_rejected"}}),
                }
            };
            write_response(output, id, value, close)?;
            if close { break; }
        }
        Ok(())
    }
}
