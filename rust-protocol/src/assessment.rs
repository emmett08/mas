//! Version 1 assessment payload contract, carried by the existing MAS envelope.
//! Structural validity is separate from reference resolution and finding truth.
mod helpers;
mod history;
mod records;
mod validation;

pub const PROFILE: &str = "assessment.v1";
pub use helpers::{canonical_digest, AssessmentError, SCHEMA_VERSION};
pub use history::AssessmentHistory;
pub use records::*;
pub use validation::{validate_assessment, validate_candidate_revision};
