//! Machine-readable message admission outcomes. These describe insertion into
//! the protocol repository, never execution of an external action.
use serde_json::{json, Value};

pub type AdmissionResult<T> = Result<T, AdmissionError>;

/// Additive gateway admission and actor-scoped identity observation contract.
pub const PROFILE: &str = "mas-admission/1";

pub(crate) fn message_observation(message_id: &str, receipt: Option<(i64, bool, bool)>) -> Value {
    match receipt {
        Some((sequence, received, acknowledged)) => json!({
            "profile":PROFILE,"message_id":message_id,"state":"present",
            "accepted":true,"sequence":sequence,"received":received,"acknowledged":acknowledged
        }),
        None => json!({"profile":PROFILE,"message_id":message_id,"state":"absent"}),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionDisposition {
    /// This attempt was refused. Correct its cause before resubmission.
    Rejected,
    /// This attempt was not inserted; an identical bounded retry may succeed.
    Retryable,
    /// Admission could not be established. Reconcile the original identity.
    Unknown,
}

impl AdmissionDisposition {
    pub fn as_str(self) -> &'static str {
        match self { Self::Rejected => "rejected", Self::Retryable => "retryable", Self::Unknown => "unknown" }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionCode {
    InvalidMessage, UnknownRun, UnknownActor, ActorMismatch, UnknownRecipient,
    ForbiddenRoute, IdentityConflict, DeadlineExceeded, UnsupportedVersion,
    BudgetExceeded, MailboxFull, InvalidTransition, OutcomeUnknown,
}

impl AdmissionCode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InvalidMessage => "invalid_message", Self::UnknownRun => "unknown_run",
            Self::UnknownActor => "unknown_actor", Self::ActorMismatch => "actor_mismatch",
            Self::UnknownRecipient => "unknown_recipient", Self::ForbiddenRoute => "forbidden_route",
            Self::IdentityConflict => "identity_conflict", Self::DeadlineExceeded => "deadline_exceeded",
            Self::UnsupportedVersion => "unsupported_version", Self::BudgetExceeded => "budget_exceeded",
            Self::MailboxFull => "mailbox_full", Self::InvalidTransition => "invalid_transition",
            Self::OutcomeUnknown => "outcome_unknown",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdmissionError {
    pub code: AdmissionCode,
    pub disposition: AdmissionDisposition,
    pub message: String,
}

impl AdmissionError {
    pub fn rejected(code: AdmissionCode, message: impl Into<String>) -> Self {
        Self { code, disposition: AdmissionDisposition::Rejected, message: message.into() }
    }
    pub fn mailbox_full() -> Self {
        Self { code: AdmissionCode::MailboxFull, disposition: AdmissionDisposition::Retryable,
            message: "mailbox full; retry after acknowledgement".into() }
    }
    pub fn unknown(message: impl std::fmt::Display) -> Self {
        Self { code: AdmissionCode::OutcomeUnknown, disposition: AdmissionDisposition::Unknown,
            message: message.to_string() }
    }
    pub fn to_value(&self) -> Value {
        json!({"code":self.code.as_str(),"disposition":self.disposition.as_str(),"message":self.message})
    }
}

impl std::fmt::Display for AdmissionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result { f.write_str(&self.message) }
}
impl std::error::Error for AdmissionError {}

// Existing operational helpers return strings. Unclassified failures remain
// uncertain; diagnostic wording must never grant permission to retry execution.
impl From<String> for AdmissionError {
    fn from(message: String) -> Self { Self::unknown(message) }
}
