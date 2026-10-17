//! Optional task semantics checked in the same transaction as durable insertion.
//! A terminal Result is a participant's claim, not independent proof of execution.
//! Expiry is a host observation, never an invented participant message.
use crate::Message;
use rusqlite::{params, OptionalExtension, Transaction};
use serde_json::{json, Value};

/// A request's worker must be permitted to return a Result or Failure. Generic
/// messages retain their directed route contract; historical duplicates bypass
/// this new-admission check in the repository.
pub(crate) fn response_route_permitted(configuration: &Value, message: &Message) -> bool {
    message.kind != "Request" || configuration["edges"].as_array()
        .is_none_or(|edges| edges.contains(&json!([message.recipient,message.sender])))
}

pub(crate) fn validate_transition(db: &Transaction<'_>, message: &Message) -> Result<(), String> {
    validate_prior(message, prior(db, message)?)
}

pub(crate) fn prior(db: &Transaction<'_>, message: &Message) -> Result<Option<PriorTask>, String> {
    let task_id = message.payload.get("task_id").and_then(Value::as_str).unwrap_or("");
    let prior:Option<(String,String,bool,String,String)>=db.query_row(
        "SELECT t.request_id,t.state,m.received,m.sender,m.recipient FROM tasks t JOIN messages m ON m.run=t.run AND m.id=t.request_id WHERE t.run=? AND t.id=?",
        params![message.run_id,task_id],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?)))
        .optional().map_err(|e|e.to_string())?;
    Ok(prior)
}

pub(crate) type PriorTask = (String, String, bool, String, String);

pub(crate) fn validate_prior(message: &Message, prior: Option<PriorTask>) -> Result<(), String> {
    let p = &message.payload;
    let fields: &[&str] = match message.kind.as_str() {
        "Request" => &["task_id", "operation", "input"],
        "Result" => &["task_id", "reply_to", "status", "output"],
        "Failure" | "Cancel" => &["task_id", "reply_to", "reason"],
        _ => return Err("unsupported task kind".into()),
    };
    if p.len() != fields.len() || fields.iter().any(|k| !p.contains_key(*k)) {
        return Err("unsupported task payload fields".into());
    }
    let _task_id = p["task_id"].as_str().filter(|s| crate::identity(s))
        .ok_or("invalid task identity")?;
    if message.kind == "Request" {
        if prior.is_some() { return Err("task identity already used; resend original message identity".into()); }
        if !p["operation"].as_str().is_some_and(crate::identity) { return Err("invalid operation".into()); }
        if !p["input"].is_object() { return Err("request input must be an object".into()); }
        return Ok(());
    }
    let (request_id,state,received,requester,worker)=prior.ok_or("unknown task request")?;
    if p["reply_to"].as_str() != Some(request_id.as_str()) { return Err("reply must identify the exact request".into()); }
    if state != "open" { return Err("task already terminal".into()); }
    let (sender,recipient) = if message.kind == "Cancel" {
        (&requester,&worker)
    } else { (&worker,&requester) };
    if sender != &message.sender || recipient != &message.recipient {
        return Err("task response has wrong participants".into());
    }
    if message.kind != "Cancel" && !received { return Err("request must be received before responding".into()); }
    if message.kind == "Result" {
        if !matches!(p["status"].as_str(),Some("completed" | "unresolved")) || !p["output"].is_object() {
            return Err("result requires completed/unresolved status and output object".into());
        }
    } else if !p["reason"].as_str().is_some_and(|s| !s.trim().is_empty()) {
        return Err("failure or cancellation needs a reason".into());
    }
    Ok(())
}

/// Called after the message insert but before its transaction commits.
pub(crate) fn record_transition(db:&Transaction<'_>,message:&Message,observed:Option<f64>) -> Result<(),String> {
    let task_id=message.payload["task_id"].as_str().ok_or("invalid task identity")?;
    if message.kind=="Request" {
        db.execute("INSERT INTO tasks(run,id,request_id) VALUES(?,?,?)",params![message.run_id,task_id,message.message_id]).map_err(|e|e.to_string())?;
    } else {
        let state=match message.kind.as_str() {
            "Result"=>message.payload["status"].as_str().ok_or("invalid result status")?,
            "Failure"=>"failed",
            "Cancel"=>"cancelled",
            _=>return Err("unsupported task kind".into()),
        };
        let changed=db.execute("UPDATE tasks SET state=?,terminal_id=?,closed_at=? WHERE run=? AND id=? AND state='open'",params![state,message.message_id,observed,message.run_id,task_id]).map_err(|e|e.to_string())?;
        if changed!=1 { return Err("task already terminal".into()); }
    }
    Ok(())
}

/// Native version 1 retained every accepted message, allowing an atomic rebuild.
/// Existing terminal timestamps are unknown; migration leaves them NULL.
pub(crate) fn migrate(db:&Transaction<'_>) -> Result<(),String> {
    let rows={
        let mut statement=db.prepare("SELECT m.run,m.id,m.sender,m.recipient,m.content,r.config FROM messages m JOIN runs r ON r.id=m.run ORDER BY m.seq").map_err(|e|e.to_string())?;
        let collected=statement.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,String>(3)?,r.get::<_,String>(4)?,r.get::<_,String>(5)?))).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
        collected
    };
    for (run,id,sender,recipient,content,configuration) in rows {
        let configuration:Value=serde_json::from_str(&configuration).map_err(|e|e.to_string())?;
        if configuration["application"]!="agent-task/1" { continue; }
        let wire=crate::encode_message(&content,crate::MAX_BYTES,crate::MAX_DEPTH).map_err(|e|e.message)?;
        let message=crate::parse_message(&wire,crate::MAX_BYTES,crate::MAX_DEPTH).map_err(|e|e.message)?;
        if message.run_id!=run || message.message_id!=id || message.sender!=sender || message.recipient!=recipient {
            return Err("incompatible database task history: envelope columns differ".into());
        }
        validate_transition(db,&message).map_err(|e|format!("incompatible database task history: {e}"))?;
        record_transition(db,&message,None)?;
    }
    Ok(())
}

pub(crate) fn inspect(db:&Transaction<'_>,run:&str) -> Result<Value,String> {
    let mut statement=db.prepare("SELECT id,request_id,state,terminal_id,closed_at FROM tasks WHERE run=? ORDER BY rowid").map_err(|e|e.to_string())?;
    let rows=statement.query_map([run],|r|{
        let state:String=r.get(2)?;
        Ok(json!({"task_id":r.get::<_,String>(0)?,"request_id":r.get::<_,String>(1)?,"state":state,
            "outcome":if state=="open" {Value::Null} else {json!(state)},
            "terminal_message_id":r.get::<_,Option<String>>(3)?,"closed_at":r.get::<_,Option<f64>>(4)?}))
    }).map_err(|e|e.to_string())?.collect::<Result<Vec<_>,_>>().map_err(|e|e.to_string())?;
    Ok(json!(rows))
}
