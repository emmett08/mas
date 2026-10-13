//! Real SQLite expiry under a full mailbox and exhausted message budget.
//! Deterministic participants; no model, tool side effect or human is invoked.
use mas_protocol_codec::{store::Store, Message};
use serde_json::json;
use std::{thread, time::{Duration, SystemTime, UNIX_EPOCH}};

fn main() -> Result<(), String> {
    let directory = std::env::temp_dir().join(format!("mas-recovery-{}-{}",
        std::process::id(), SystemTime::now().duration_since(UNIX_EPOCH)
            .map_err(|e| e.to_string())?.as_nanos()));
    std::fs::create_dir(&directory).map_err(|e| e.to_string())?;
    let database = directory.join("state.sqlite");
    let result = demonstrate(&database);
    let _ = std::fs::remove_dir_all(directory);
    println!("{}", serde_json::to_string_pretty(&result?).map_err(|e| e.to_string())?);
    Ok(())
}

fn demonstrate(database: &std::path::Path) -> Result<serde_json::Value, String> {
    let mut store = Store::open(database)?;
    store.create_run("recovery", &json!({
        "agents":["requester","worker"], "edges":[["requester","worker"],["worker","requester"]],
        "application":"agent-task/1", "mailbox_capacity":1, "max_messages":1,
        "wall_seconds":1
    }))?;
    let request = Message {
        message_id:"q1".into(), run_id:"recovery".into(), sender:"requester".into(),
        recipient:"worker".into(), version:"1".into(), kind:"Request".into(),
        payload:json!({"task_id":"t1","operation":"review","input":{"candidate":7}})
            .as_object().unwrap().clone()
    };
    store.send(&request,"requester")?;
    // No worker responds. The one available message slot is already consumed.
    thread::sleep(Duration::from_millis(1100));
    drop(store);
    let mut store = Store::open(database)?;
    store.reconcile("recovery")?;
    let status = store.inspect("recovery")?;
    let deliveries = store.receive("recovery","worker",1)?;
    if status["tasks"][0]["state"] != "expired_outcome_unknown"
        || deliveries[0]["actionable"] != false || status["usage"]["messages"] != 1 {
        return Err("expiry did not preserve bounded, non-actionable task state".into());
    }
    let retry = store.send(&request,"requester")?;
    if retry["duplicate"] != true { return Err("identity retention failed".into()); }
    store.ack("recovery","q1","worker")?;
    Ok(json!({"mode":"deterministic runtime example", "deadline_observed_after_restart":true,
        "tasks":status["tasks"], "closed_delivery_actionable":deliveries[0]["actionable"],
        "messages_charged":status["usage"]["messages"], "identical_retry":retry,
        "meaning":"Expiry closes protocol work; it does not prove absence of external effects."}))
}
