//! Versioned, authenticated admission and sent-identity observation. Stable
//! dispositions cross this boundary; private repository diagnostics do not.
use super::{fields, text, GatewayReply};
use crate::{
    admission::{AdmissionCode, AdmissionError, AdmissionResult, PROFILE},
    parse_message, repository::ProtocolRepository, MAX_BYTES, MAX_DEPTH,
};
use serde_json::{json, Value};

pub(super) fn is_command(request: &Value) -> bool {
    matches!(request["command"].as_str(),Some("send_checked" | "reconcile_message"))
}

fn invalid() -> AdmissionError {
    AdmissionError::rejected(AdmissionCode::InvalidMessage,"invalid versioned admission command")
}

fn execute<R: ProtocolRepository>(store: &mut R, run_id: &str, actor: &str, request: &Value) -> AdmissionResult<Value> {
    if request["profile"].as_str() != Some(PROFILE) { return Err(invalid()); }
    match request["command"].as_str() {
        Some("send_checked") => {
            fields(request,&["token","command","profile","wire"],&[]).map_err(|_|invalid())?;
            let wire=text(request,"wire").map_err(|_|invalid())?;
            let message=parse_message(wire,MAX_BYTES,MAX_DEPTH).map_err(|_|invalid())?;
            if message.run_id!=run_id || message.sender!=actor {
                return Err(AdmissionError::rejected(AdmissionCode::ActorMismatch,"capability binding mismatch"));
            }
            let mut receipt=store.send_checked(&message,actor)?;
            receipt["profile"]=json!(PROFILE);
            Ok(receipt)
        },
        Some("reconcile_message") => {
            fields(request,&["token","command","profile","message_id"],&[]).map_err(|_|invalid())?;
            store.reconcile_message(run_id,actor,text(request,"message_id").map_err(|_|invalid())?)
        },
        _ => Err(invalid()),
    }
}

pub(super) fn handle<R: ProtocolRepository>(store: &mut R, run_id: &str, actor: &str, request: &Value, close: bool) -> GatewayReply {
    let response=match execute(store,run_id,actor,request) {
        Ok(result)=>json!({"ok":true,"result":result}),
        Err(error)=>json!({"ok":false,"error":{"profile":PROFILE,"code":error.code.as_str(),
            "disposition":error.disposition.as_str()}}),
    };
    GatewayReply {response,close}
}
