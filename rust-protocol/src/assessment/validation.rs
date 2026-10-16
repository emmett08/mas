//! Resolve references against host-retained records and frozen evaluator authority.
use super::helpers::*;
use super::records::*;
use std::collections::BTreeMap;

/// Revalidate even directly constructed Rust values, then resolve every reference
/// against host-retained records and the invocation's frozen evaluator identity.
pub fn validate_assessment(
    assessment: &Assessment,
    candidate: &Candidate,
    rubric: &Rubric,
    evidence: &[Evidence],
    authorised: &Evaluator,
) -> Result<()> {
    Assessment::from_value(&assessment.to_value())?;
    Candidate::from_value(&candidate.to_value())?;
    Rubric::from_value(&rubric.to_value())?;
    Evaluator::from_value(&authorised.to_value())?;
    if &assessment.evaluator != authorised {
        return reject("AUTHORITY", "evaluator differs from invocation authority");
    }
    assessment
        .candidate
        .matches(&candidate.id, &candidate.to_value())?;
    assessment.rubric.matches(&rubric.id, &rubric.to_value())?;
    unique(evidence.iter().map(|e| e.id.as_str()))?;
    for e in evidence {
        Evidence::from_value(&e.to_value())?;
    }
    let records: BTreeMap<_, _> = evidence.iter().map(|e| (e.id.as_str(), e)).collect();
    let criteria: BTreeMap<_, _> = rubric.criteria.iter().map(|c| (c.id.as_str(), c)).collect();
    if assessment.status != AssessmentStatus::Error && assessment.findings.len() != criteria.len() {
        return reject("RUBRIC", "assessment must address every rubric criterion");
    }
    for finding in &assessment.findings {
        let c = criteria
            .get(finding.criterion_id.as_str())
            .ok_or_else(|| AssessmentError {
                code: "REFERENCE",
                message: "unknown criterion".into(),
            })?;
        if c.measurement != authorised.kind {
            return reject(
                "AUTHORITY",
                "evaluator kind differs from criterion measurement kind",
            );
        }
        if finding
            .score
            .is_some_and(|s| s < c.minimum || s > c.maximum)
        {
            return reject("METRIC", "score lies outside declared criterion range");
        }
        for r in &finding.evidence {
            let e = records.get(r.id.as_str()).ok_or_else(|| AssessmentError {
                code: "REFERENCE",
                message: "unknown supporting evidence".into(),
            })?;
            r.matches(&e.id, &e.to_value())?;
        }
    }
    Ok(())
}

pub fn validate_candidate_revision(
    candidate: &Candidate,
    parent: Option<&Candidate>,
    feedback: &[Assessment],
) -> Result<()> {
    Candidate::from_value(&candidate.to_value())?;
    unique(feedback.iter().map(|a| a.id.as_str()))?;
    match (&candidate.parent, parent) {
        (None, None) if feedback.is_empty() => Ok(()),
        (Some(reference), Some(parent)) => {
            Candidate::from_value(&parent.to_value())?;
            reference.matches(&parent.id, &parent.to_value())?;
            if candidate.id == parent.id {
                return reject("REVISION", "revision identity equals parent");
            }
            if candidate.feedback.len() != feedback.len() {
                return reject("REFERENCE", "supplied feedback differs from revision links");
            }
            for r in &candidate.feedback {
                let a = feedback
                    .iter()
                    .find(|a| a.id == r.id)
                    .ok_or_else(|| AssessmentError {
                        code: "REFERENCE",
                        message: "unknown feedback assessment".into(),
                    })?;
                Assessment::from_value(&a.to_value())?;
                r.matches(&a.id, &a.to_value())?;
                a.candidate.matches(&parent.id, &parent.to_value())?;
            }
            Ok(())
        }
        _ => reject(
            "REVISION",
            "parent and feedback context do not match candidate links",
        ),
    }
}
