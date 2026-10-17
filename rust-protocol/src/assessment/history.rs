//! Exact identity/content retention semantics for one assessment history.
use super::helpers::*;
use super::records::*;
use std::collections::BTreeMap;

/// A small in-memory retention primitive for tools and tests. Durable hosts use
/// the same exact-content rule inside their database transaction.
#[derive(Default)]
pub struct AssessmentHistory {
    records: BTreeMap<String, Assessment>,
}
impl AssessmentHistory {
    pub fn insert(&mut self, a: Assessment) -> Result<bool> {
        Assessment::from_value(&a.to_value())?;
        if let Some(old) = self.records.get(&a.id) {
            if old != &a {
                return reject(
                    "IDENTITY_CONFLICT",
                    "assessment identity already binds different content",
                );
            }
            return Ok(false);
        }
        self.records.insert(a.id.clone(), a);
        Ok(true)
    }
    pub fn get(&self, id: &str) -> Option<&Assessment> {
        self.records.get(id)
    }
    pub fn len(&self) -> usize {
        self.records.len()
    }
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
}
