//! Host-owned admission boundary for private brokered stdio sessions.
//!
//! Capabilities bind one run and actor. The host must keep the database,
//! credential configuration and response routing private from other actors.
//! This module does not provide a network transport or protect a compromised host.
use crate::{parse_json, parse_message, store::Store, repository::ProtocolRepository, MAX_BYTES, MAX_DEPTH};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use std::{collections::{BTreeMap, BTreeSet}, io::{BufRead, Write}};
use subtle::ConstantTimeEq;

#[path = "gateway_checked.rs"]
mod checked;

type Result<T> = std::result::Result<T, String>;

/// JSONL frames, excluding the final newline, are limited independently of wire
/// messages. JSON escaping means the largest valid wire message may not fit.
pub const MAX_FRAME_BYTES: usize = MAX_BYTES;

struct Credential {
    scope: String,
    token: [u8; 32],
    run_id: String,
    actor: String,
    limit: i64,
}

/// The boundary owns its Store; untrusted callers must not receive Store access.
pub struct Gateway<R: ProtocolRepository = Store> {
    store: R,
    scope: String,
    limit: i64,
    credentials: Vec<Credential>,
    stopped: bool,
}

/// A fixed diagnostic or an actor-scoped result, never credential material.
pub struct GatewayReply {
    pub response: Value,
    /// The broker must close this session; the durable global cap also survives
    /// attempts to reopen the same gateway with the same configuration.
    pub close: bool,
}

fn object(value: &Value) -> Result<&Map<String, Value>> {
    value.as_object().ok_or_else(|| "expected object".into())
}
fn fields(value: &Value, required: &[&str], optional: &[&str]) -> Result<()> {
    let obj = object(value)?;
    if required.iter().any(|key| !obj.contains_key(*key))
        || obj.keys().any(|key| !required.contains(&key.as_str()) && !optional.contains(&key.as_str())) {
        return Err("missing or unknown field".into());
    }
    Ok(())
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str> {
    value.get(key).and_then(Value::as_str).ok_or_else(|| "expected string".into())
}
fn positive(value: &Value, key: &str) -> Result<i64> {
    value.get(key).and_then(Value::as_i64).filter(|n| *n > 0).ok_or_else(|| "expected positive integer".into())
}
fn scope_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 32 && id.bytes().all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
}
fn token_bytes(token: &str) -> Result<[u8; 32]> {
    if token.len() != 64 || !token.bytes().all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) {
        return Err("invalid capability".into());
    }
    let mut bytes = [0; 32];
    for (index, pair) in token.as_bytes().chunks_exact(2).enumerate() {
        fn digit(b: u8) -> u8 { if b <= b'9' { b - b'0' } else { b - b'a' + 10 } }
        bytes[index] = digit(pair[0]) * 16 + digit(pair[1]);
    }
    Ok(bytes)
}
fn reject(code: &str, close: bool) -> GatewayReply {
    GatewayReply { response: json!({"ok":false,"error":{"code":code}}), close }
}

/// Validate a trusted policy/configuration and normalise credential ordering so
/// whitespace and array reordering cannot change the frozen admission identity.
/// `with_tokens=false` is solely for host-side provisioning.
pub fn normalise_config(value: &Value, with_tokens: bool) -> Result<Value> {
    fields(value, &["gateway_id", "max_attempts", "credentials"], &[])?;
    let id = text(value, "gateway_id")?;
    if !scope_id(id) { return Err("invalid gateway id".into()); }
    positive(value, "max_attempts")?;
    let credentials = value["credentials"].as_array().filter(|c| (1..=32).contains(&c.len())).ok_or("register 1..32 credentials")?;
    let mut ids = BTreeSet::new();
    let mut bindings = BTreeSet::new();
    let mut tokens = BTreeSet::new();
    let mut normal = Vec::new();
    for credential in credentials {
        let mut required = vec!["credential_id", "run_id", "actor", "max_attempts"];
        if with_tokens { required.push("token"); }
        fields(credential, &required, &[])?;
        let cid = text(credential, "credential_id")?;
        if !scope_id(cid) || !ids.insert(cid) { return Err("invalid or duplicate credential id".into()); }
        let run_id = text(credential, "run_id")?;
        let actor = text(credential, "actor")?;
        if !crate::identity(run_id) || !crate::identity(actor) || !bindings.insert((run_id, actor)) {
            return Err("invalid or repeated actor binding".into());
        }
        positive(credential, "max_attempts")?;
        if with_tokens {
            let token = token_bytes(text(credential, "token")?)?;
            if !tokens.insert(token) { return Err("capabilities must be unique".into()); }
        }
        normal.push(credential.clone());
    }
    normal.sort_by(|a,b| a["credential_id"].as_str().cmp(&b["credential_id"].as_str()));
    Ok(json!({"gateway_id":id,"max_attempts":value["max_attempts"],"credentials":normal}))
}

impl<P: ProtocolRepository> Gateway<P> {
    pub fn open(mut store: P, configuration: &Value) -> Result<Self> {
        // Re-parse configuration so direct library callers obey the same depth,
        // byte, duplicate and integer grammar as the executable's input file.
        let mut budget = MAX_BYTES;
        crate::validate_value(configuration, 0, &mut budget).map_err(|_| "invalid gateway configuration")?;
        let configuration = parse_json(&configuration.to_string()).map_err(|_| "invalid gateway configuration")?;
        let normal = normalise_config(&configuration, true).map_err(|_| "invalid gateway configuration")?;
        let scope = format!("gateway/{}", normal["gateway_id"].as_str().unwrap());
        let limit = normal["max_attempts"].as_i64().unwrap();
        let mut credentials = Vec::new();
        let mut memberships = BTreeMap::new();
        for record in normal["credentials"].as_array().unwrap() {
            let run_id = record["run_id"].as_str().unwrap();
            let actor = record["actor"].as_str().unwrap();
            // Several credentials commonly share one run. Read its bounded
            // identity projection once, independently of retained audit history.
            if !memberships.contains_key(run_id) {
                memberships.insert(run_id, store.run_membership(run_id)
                    .map_err(|_| "gateway binding references an unavailable run")?);
            }
            if !memberships[run_id].contains(actor) {
                return Err("gateway binding references an unregistered actor".into());
            }
            credentials.push(Credential {
                scope: format!("{scope}/{}", record["credential_id"].as_str().unwrap()),
                token: token_bytes(record["token"].as_str().unwrap()).unwrap(),
                run_id: run_id.into(), actor: actor.into(),
                limit: record["max_attempts"].as_i64().unwrap(),
            });
        }
        // This is a fingerprint of high-entropy credentials, not plaintext
        // secrets. It freezes the entire policy even across process restarts.
        let digest = Sha256::digest(format!("MAS-GATEWAY-1\n{normal}").as_bytes());
        let fingerprint: String = digest.iter().map(|byte| format!("{byte:02x}")).collect();
        store.register_admission(&scope, &fingerprint, limit).map_err(|_| "gateway policy registration rejected")?;
        for credential in &credentials {
            store.register_admission(&credential.scope, &fingerprint, credential.limit).map_err(|_| "gateway policy registration rejected")?;
        }
        Ok(Self { store, scope, limit, credentials, stopped: false })
    }

    fn begin_attempt(&mut self) -> std::result::Result<bool, GatewayReply> {
        if self.stopped { return Err(reject("admission_closed", true)); }
        match self.store.reserve_admission(&self.scope) {
            Ok(used) => {
                let close = used >= self.limit;
                // The final admitted frame is processed, then the session stops.
                self.stopped = close;
                Ok(close)
            },
            Err(_) => { self.stopped = true; Err(reject("admission_closed", true)) },
        }
    }

    /// Meter a caller-provided frame before parsing. The caller must bound its
    /// own allocation; `serve` implements this boundary for untrusted input.
    pub fn handle_frame(&mut self, frame: &[u8]) -> GatewayReply {
        match self.begin_attempt() {
            Ok(close) => self.handle_charged_frame(frame, close),
            Err(reply) => reply,
        }
    }

    fn handle_charged_frame(&mut self, frame: &[u8], close: bool) -> GatewayReply {
        if frame.len() > MAX_FRAME_BYTES {
            self.stopped = true;
            return reject("frame_too_large", true);
        }
        let source = match std::str::from_utf8(frame) { Ok(s) => s, Err(_) => return reject("invalid_frame", close) };
        let request = match parse_json(source) { Ok(v) => v, Err(_) => return reject("invalid_frame", close) };
        let token = match text(&request, "token").and_then(token_bytes) { Ok(t) => t, Err(_) => return reject("unauthorised", close) };
        let mut selected = None;
        // Compare every fixed-length credential, with no early token prefix or
        // credential match exit. subtle supplies the constant-time comparison.
        for (index, credential) in self.credentials.iter().enumerate() {
            if bool::from(credential.token.ct_eq(&token)) { selected = Some(index); }
        }
        let index = match selected { Some(i) => i, None => return reject("unauthorised", close) };
        let credential = &self.credentials[index];
        if self.store.reserve_admission(&credential.scope).is_err() { return reject("credential_admission_closed", close); }
        if checked::is_command(&request) {
            return checked::handle(&mut self.store,&credential.run_id,&credential.actor,&request,close);
        }
        // Identity is never accepted from an untrusted actor/run field. A wire
        // envelope also has to match both parts of the capability binding.
        let result = self.execute(index, &request);
        match result {
            Ok(value) => GatewayReply { response: json!({"ok":true,"result":value}), close },
            Err(_) => reject("operation_rejected", close),
        }
    }

    fn execute(&mut self, index: usize, request: &Value) -> Result<Value> {
        let credential = &self.credentials[index];
        let run_id = &credential.run_id;
        let actor = &credential.actor;
        match text(request, "command")? {
            "send" => {
                fields(request, &["token", "command", "wire"], &[])?;
                let message = parse_message(text(request, "wire")?, MAX_BYTES, MAX_DEPTH).map_err(|_| "invalid wire")?;
                if message.run_id != *run_id || message.sender != *actor { return Err("capability binding mismatch".into()); }
                self.store.send(&message, actor)
            },
            "receive" => {
                fields(request, &["token", "command"], &["limit"])?;
                let limit = match request.get("limit") {
                    None => 10,
                    Some(value) => value.as_u64().filter(|n| (1..=100).contains(n)).ok_or("invalid limit")? as usize,
                };
                self.store.receive(run_id, actor, limit)
            },
            "ack" => {
                fields(request, &["token", "command", "message_id"], &[])?;
                self.store.ack(run_id, text(request, "message_id")?, actor)
            },
            "reserve" => {
                fields(request, &["token", "command", "call_id"], &[])?;
                self.store.reserve_call(run_id, actor, text(request, "call_id")?)
            },
            "finish" => {
                fields(request, &["token", "command", "call_id", "evidence"], &[])?;
                self.store.finish_call(run_id, actor, text(request, "call_id")?, &request["evidence"])
            },
            _ => Err("unknown command".into()),
        }
    }

    /// Run a private brokered JSONL session. A partial frame is charged as soon
    /// as input is available, before allocating/parsing it. At most MAX+1 frame
    /// bytes are retained. Oversized input closes the session without draining.
    /// Blocking stdio has no delivery, read-timeout or service-availability claim.
    pub fn serve<R: BufRead, W: Write>(&mut self, reader: &mut R, writer: &mut W) -> Result<()> {
        loop {
            if reader.fill_buf().map_err(|_| "input unavailable")?.is_empty() { return Ok(()); }
            let close = match self.begin_attempt() {
                Ok(close) => close,
                Err(reply) => { write_reply(writer, &reply)?; return Ok(()); },
            };
            let mut frame = Vec::new();
            loop {
                let available = reader.fill_buf().map_err(|_| "input unavailable")?;
                if available.is_empty() { break; }
                let until_newline = available.iter().position(|b| *b == b'\n');
                let end = until_newline.unwrap_or(available.len());
                let copy = end.min(MAX_FRAME_BYTES + 1 - frame.len());
                frame.extend_from_slice(&available[..copy]);
                let newline = until_newline == Some(copy);
                reader.consume(copy + usize::from(newline));
                if newline || frame.len() > MAX_FRAME_BYTES { break; }
            }
            let reply = self.handle_charged_frame(&frame, close);
            write_reply(writer, &reply)?;
            if reply.close { return Ok(()); }
        }
    }
}

fn write_reply(writer: &mut impl Write, reply: &GatewayReply) -> Result<()> {
    serde_json::to_writer(&mut *writer, &reply.response).map_err(|_| "output unavailable")?;
    writer.write_all(b"\n").and_then(|_| writer.flush()).map_err(|_| "output unavailable".into())
}
