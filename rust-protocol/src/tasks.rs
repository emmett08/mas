//! Optional task semantics checked in the same transaction as durable insertion.
//! A terminal Result is a participant's claim, not independent proof of execution.
use crate::Message;
use rusqlite::{params, Transaction};
use serde_json::Value;

pub(crate) fn validate_transition(db: &Transaction<'_>, message: &Message) -> Result<(), String> {
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
    let task_id = p["task_id"].as_str().filter(|s| crate::identity(s))
        .ok_or("invalid task identity")?;
    let mut statement = db.prepare("SELECT content,received FROM messages WHERE run=? ORDER BY seq")
        .map_err(|e| e.to_string())?;
    let rows = statement.query_map(params![message.run_id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,bool>(1)?)))
        .map_err(|e| e.to_string())?;
    let mut task = Vec::new();
    for row in rows {
        let (content,received) = row.map_err(|e| e.to_string())?;
        let prior: Value = serde_json::from_str(&content).map_err(|e| e.to_string())?;
        if prior["payload"]["task_id"].as_str() == Some(task_id) { task.push((prior,received)); }
    }
    if message.kind == "Request" {
        if !task.is_empty() { return Err("task identity already used; resend original message identity".into()); }
        if !p["operation"].as_str().is_some_and(crate::identity) { return Err("invalid operation".into()); }
        if !p["input"].is_object() { return Err("request input must be an object".into()); }
        return Ok(());
    }
    let (request,received) = task.first().ok_or("unknown task request")?;
    if request["kind"] != "Request" { return Err("unknown task request".into()); }
    if p["reply_to"] != request["message_id"] { return Err("reply must identify the exact request".into()); }
    if task.len() != 1 { return Err("task already terminal".into()); }
    let (sender,recipient) = if message.kind == "Cancel" {
        (&request["sender"],&request["recipient"])
    } else { (&request["recipient"],&request["sender"]) };
    if sender.as_str() != Some(&message.sender) || recipient.as_str() != Some(&message.recipient) {
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
