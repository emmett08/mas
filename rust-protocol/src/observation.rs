//! Schema-owned, read-only observations of committed protocol state.
//! Observation never migrates, reconciles expiry, receives or acknowledges.
use crate::{encode_message, parse_message, MAX_BYTES, MAX_DEPTH};
use rusqlite::{Connection, OpenFlags, OptionalExtension};
use serde_json::{json, Value};
use std::{path::Path, time::Duration};

type Result<T> = std::result::Result<T, String>;
fn error(e: impl std::fmt::Display) -> String { e.to_string() }

/// An observation handle has no protocol write operations. Filesystem access is
/// the caller's authority boundary; payload access requires explicit opt-in.
pub struct Observer { db: Connection }

impl Observer {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if !path.as_ref().is_file() { return Err("unknown run or run initialization incomplete".into()); }
        let db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(error)?;
        db.busy_timeout(Duration::from_millis(250)).map_err(error)?;
        let tx = db.unchecked_transaction().map_err(error)?;
        validate(&tx)?;
        tx.commit().map_err(error)?;
        Ok(Self { db })
    }

    /// Reports retained states, including an unreconciled deadline. It grants
    /// neither an execution lease nor permission to repeat an external effect.
    pub fn snapshot(&self, run_id: &str, include_payloads: bool) -> Result<Value> {
        sqlite_snapshot(&self.db, run_id, include_payloads)
    }
}

fn validate(db: &Connection) -> Result<()> {
    let application: i64 = db.query_row("PRAGMA application_id", [], |r|r.get(0)).map_err(error)?;
    let version: i64 = db.query_row("PRAGMA user_version", [], |r|r.get(0)).map_err(error)?;
    if application != 0x4d415331 || version != 2 {
        return Err("observation requires MAS native SQLite schema version 2".into());
    }
    crate::store_schema::validate(db, version)
}

pub(crate) fn optional_json(value: Option<String>) -> Result<Value> {
    value.map(|source|serde_json::from_str(&source).map_err(error))
        .transpose().map(|value|value.unwrap_or(Value::Null))
}

/// Backends use the same validation and disclosure rules for retained envelopes.
pub(crate) fn message(sequence: i64, id: &str, run_id: &str, sender: &str,
    recipient: &str, content: &str, received: bool, acked: bool, include_payloads: bool) -> Result<Value> {
    let envelope = encode_message(content, MAX_BYTES, MAX_DEPTH)
        .and_then(|wire|parse_message(&wire,MAX_BYTES,MAX_DEPTH))
        .map_err(|_|"invalid retained protocol envelope")?;
    if envelope.message_id != id || envelope.run_id != run_id
        || envelope.sender != sender || envelope.recipient != recipient {
        return Err("retained protocol envelope columns differ".into());
    }
    let mut value = json!({"sequence":sequence,"message_id":id,"run_id":run_id,
        "sender":sender,"recipient":recipient,"version":envelope.version,
        "kind":envelope.kind,"received":received,"acked":acked,
        "task_id":envelope.payload.get("task_id").and_then(Value::as_str),
        "reply_to":envelope.payload.get("reply_to").and_then(Value::as_str),
        "operation":envelope.payload.get("operation").and_then(Value::as_str)});
    if include_payloads { value["payload"] = Value::Object(envelope.payload); }
    Ok(value)
}

pub(crate) fn snapshot_value(run_id: &str, include_payloads: bool, run: Value,
    messages: Vec<Value>, calls: Vec<Value>, tasks: Vec<Value>) -> Value {
    json!({"version":"mas-protocol-observation/1","run_id":run_id,
        "payloads_included":include_payloads,"run":run,"messages":messages,"calls":calls,"tasks":tasks})
}

pub(crate) fn sqlite_snapshot(db: &Connection, run_id: &str, include_payloads: bool) -> Result<Value> {
    let tx = db.unchecked_transaction().map_err(error)?;
    validate(&tx)?;
    let row = tx.query_row(
        "SELECT config,deadline,message_count,byte_count,call_count,active,expired FROM runs WHERE id=?",
        [run_id], |r|Ok((r.get::<_,String>(0)?,r.get::<_,f64>(1)?,r.get::<_,i64>(2)?,
            r.get::<_,i64>(3)?,r.get::<_,i64>(4)?,r.get::<_,String>(5)?,r.get::<_,bool>(6)?)))
        .optional().map_err(error)?.ok_or("unknown protocol run or run initialization incomplete")?;
    let config: Value = serde_json::from_str(&row.0).map_err(error)?;
    let run = json!({"config":config,"deadline":row.1,"usage":{"messages":row.2,"bytes":row.3,"calls":row.4},
        "active_protocol":row.5,"expired":row.6});
    let messages = {
        let mut statement = tx.prepare("SELECT seq,id,sender,recipient,content,received,acked FROM messages WHERE run=? ORDER BY seq").map_err(error)?;
        let rows = statement.query_map([run_id],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?,
            r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,bool>(5)?,r.get::<_,bool>(6)?))).map_err(error)?;
        let mut messages = Vec::new();
        for row in rows {
            let (seq,id,sender,recipient,content,received,acked) = row.map_err(error)?;
            messages.push(message(seq,&id,run_id,&sender,&recipient,&content,received,acked,include_payloads)?);
        }
        messages
    };
    let calls = {
        let mut statement = tx.prepare("SELECT id,actor,state,result FROM calls WHERE run=? ORDER BY rowid").map_err(error)?;
        let rows = statement.query_map([run_id],|r|Ok((json!({"id":r.get::<_,String>(0)?,
            "actor":r.get::<_,String>(1)?,"state":r.get::<_,String>(2)?}),r.get::<_,Option<String>>(3)?))).map_err(error)?;
        let mut calls = Vec::new();
        for row in rows {
            let (mut call,evidence) = row.map_err(error)?;
            if include_payloads { call["evidence"] = optional_json(evidence)?; }
            calls.push(call);
        }
        calls
    };
    let tasks = {
        let mut statement = tx.prepare("SELECT id,request_id,state,terminal_id,closed_at FROM tasks WHERE run=? ORDER BY rowid").map_err(error)?;
        let rows = statement.query_map([run_id],|r|Ok(json!({"task_id":r.get::<_,String>(0)?,
            "request_id":r.get::<_,String>(1)?,"state":r.get::<_,String>(2)?,
            "terminal_message_id":r.get::<_,Option<String>>(3)?,"closed_at":r.get::<_,Option<f64>>(4)?}))).map_err(error)?;
        rows.collect::<std::result::Result<Vec<_>,_>>().map_err(error)?
    };
    tx.commit().map_err(error)?;
    Ok(snapshot_value(run_id,include_payloads,run,messages,calls,tasks))
}
