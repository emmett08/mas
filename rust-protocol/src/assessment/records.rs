//! Typed records and structural decoding for the assessment.v1 payload profile.
use super::helpers::*;
use super::PROFILE;
use crate::Message;
use serde_json::{json, Value};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DigestRef {
    pub id: String,
    pub sha256: String,
}
impl DigestRef {
    pub fn from_value(v: &Value) -> Result<Self> {
        let o = object(v, &["id", "sha256"])?;
        Ok(Self {
            id: id(&o["id"])?,
            sha256: hash(&o["sha256"])?,
        })
    }
    pub fn to_value(&self) -> Value {
        json!({"id":self.id,"sha256":self.sha256})
    }
    pub fn of(id: impl Into<String>, value: &Value) -> Self {
        Self {
            id: id.into(),
            sha256: canonical_digest(value),
        }
    }
    pub(super) fn matches(&self, id: &str, value: &Value) -> Result<()> {
        Self::from_value(&self.to_value())?;
        if self.id != id || self.sha256 != canonical_digest(value) {
            return reject(
                "REFERENCE",
                "reference does not identify the exact retained record",
            );
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Candidate {
    pub schema_version: u64,
    pub id: String,
    pub output: Value,
    pub output_sha256: String,
    pub parent: Option<DigestRef>,
    pub feedback: Vec<DigestRef>,
}
impl Candidate {
    pub fn from_value(v: &Value) -> Result<Self> {
        bounded_value(v)?;
        let o = object(
            v,
            &[
                "schema_version",
                "id",
                "output",
                "output_sha256",
                "parent",
                "feedback",
            ],
        )?;
        version(&o["schema_version"])?;
        let candidate = Self {
            schema_version: SCHEMA_VERSION,
            id: id(&o["id"])?,
            output: o["output"].clone(),
            output_sha256: hash(&o["output_sha256"])?,
            parent: if o["parent"].is_null() {
                None
            } else {
                Some(DigestRef::from_value(&o["parent"])?)
            },
            feedback: list(&o["feedback"], DigestRef::from_value)?,
        };
        if canonical_digest(&candidate.output) != candidate.output_sha256 {
            return reject("DIGEST", "candidate output digest mismatch");
        }
        unique(candidate.feedback.iter().map(|r| r.id.as_str()))?;
        if candidate
            .parent
            .as_ref()
            .is_some_and(|p| p.id == candidate.id)
        {
            return reject("REVISION", "revision requires a distinct identity");
        }
        if candidate.parent.is_none() && !candidate.feedback.is_empty() {
            return reject(
                "REVISION",
                "initial candidate cannot carry revision feedback",
            );
        }
        Ok(candidate)
    }
    pub fn to_value(&self) -> Value {
        json!({"schema_version":self.schema_version,"id":self.id,"output":self.output,"output_sha256":self.output_sha256,
            "parent":self.parent.as_ref().map(DigestRef::to_value),"feedback":self.feedback.iter().map(DigestRef::to_value).collect::<Vec<_>>()})
    }
    pub fn reference(&self) -> DigestRef {
        DigestRef::of(&self.id, &self.to_value())
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Evidence {
    pub schema_version: u64,
    pub id: String,
    pub content: Value,
    pub content_sha256: String,
}
impl Evidence {
    pub fn from_value(v: &Value) -> Result<Self> {
        bounded_value(v)?;
        let o = object(v, &["schema_version", "id", "content", "content_sha256"])?;
        version(&o["schema_version"])?;
        let evidence = Self {
            schema_version: SCHEMA_VERSION,
            id: id(&o["id"])?,
            content: o["content"].clone(),
            content_sha256: hash(&o["content_sha256"])?,
        };
        if canonical_digest(&evidence.content) != evidence.content_sha256 {
            return reject("DIGEST", "evidence content digest mismatch");
        }
        Ok(evidence)
    }
    pub fn to_value(&self) -> Value {
        json!({"schema_version":self.schema_version,"id":self.id,"content":self.content,"content_sha256":self.content_sha256})
    }
    pub fn reference(&self) -> DigestRef {
        DigestRef::of(&self.id, &self.to_value())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvaluatorKind {
    Deterministic,
    Model,
}
impl EvaluatorKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Model => "model",
        }
    }
    fn parse(v: &Value) -> Result<Self> {
        match v.as_str() {
            Some("deterministic") => Ok(Self::Deterministic),
            Some("model") => Ok(Self::Model),
            _ => reject("EVALUATOR", "unknown evaluator kind"),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Evaluator {
    pub id: String,
    pub version: String,
    pub kind: EvaluatorKind,
}
impl Evaluator {
    pub fn from_value(v: &Value) -> Result<Self> {
        let o = object(v, &["id", "version", "kind"])?;
        Ok(Self {
            id: id(&o["id"])?,
            version: id(&o["version"])?,
            kind: EvaluatorKind::parse(&o["kind"])?,
        })
    }
    pub fn to_value(&self) -> Value {
        json!({"id":self.id,"version":self.version,"kind":self.kind.as_str()})
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Criterion {
    pub id: String,
    pub measurement: EvaluatorKind,
    pub minimum: i64,
    pub maximum: i64,
}
impl Criterion {
    pub fn from_value(v: &Value) -> Result<Self> {
        let o = object(v, &["id", "measurement", "minimum", "maximum"])?;
        let c = Self {
            id: id(&o["id"])?,
            measurement: EvaluatorKind::parse(&o["measurement"])?,
            minimum: integer(&o["minimum"])?,
            maximum: integer(&o["maximum"])?,
        };
        if c.minimum > c.maximum {
            return reject("METRIC", "criterion minimum exceeds maximum");
        }
        Ok(c)
    }
    pub fn to_value(&self) -> Value {
        json!({"id":self.id,"measurement":self.measurement.as_str(),"minimum":self.minimum,"maximum":self.maximum})
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rubric {
    pub schema_version: u64,
    pub id: String,
    pub criteria: Vec<Criterion>,
}
impl Rubric {
    pub fn from_value(v: &Value) -> Result<Self> {
        bounded_value(v)?;
        let o = object(v, &["schema_version", "id", "criteria"])?;
        version(&o["schema_version"])?;
        let criteria = list(&o["criteria"], Criterion::from_value)?;
        if criteria.is_empty() {
            return reject("RUBRIC", "rubric must contain criteria");
        }
        unique(criteria.iter().map(|c| c.id.as_str()))?;
        Ok(Self {
            schema_version: SCHEMA_VERSION,
            id: id(&o["id"])?,
            criteria,
        })
    }
    pub fn to_value(&self) -> Value {
        json!({"schema_version":self.schema_version,"id":self.id,"criteria":self.criteria.iter().map(Criterion::to_value).collect::<Vec<_>>()})
    }
    pub fn reference(&self) -> DigestRef {
        DigestRef::of(&self.id, &self.to_value())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Pass,
    Fail,
    Inconclusive,
}
impl Verdict {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Inconclusive => "inconclusive",
        }
    }
    fn parse(v: &Value) -> Result<Self> {
        match v.as_str() {
            Some("pass") => Ok(Self::Pass),
            Some("fail") => Ok(Self::Fail),
            Some("inconclusive") => Ok(Self::Inconclusive),
            _ => reject("VERDICT", "unknown finding verdict"),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Finding {
    pub criterion_id: String,
    pub verdict: Verdict,
    pub score: Option<i64>,
    pub evidence: Vec<DigestRef>,
    pub explanation: String,
}
impl Finding {
    pub fn from_value(v: &Value) -> Result<Self> {
        let o = object(
            v,
            &[
                "criterion_id",
                "verdict",
                "score",
                "evidence",
                "explanation",
            ],
        )?;
        let f = Self {
            criterion_id: id(&o["criterion_id"])?,
            verdict: Verdict::parse(&o["verdict"])?,
            score: if o["score"].is_null() {
                None
            } else {
                Some(integer(&o["score"])?)
            },
            evidence: list(&o["evidence"], DigestRef::from_value)?,
            explanation: text(&o["explanation"])?,
        };
        unique(f.evidence.iter().map(|e| e.id.as_str()))?;
        if (f.verdict == Verdict::Inconclusive) != f.score.is_none() {
            return reject(
                "VERDICT",
                "inconclusive findings have no score; pass/fail findings require a score",
            );
        }
        if f.verdict != Verdict::Inconclusive && f.evidence.is_empty() {
            return reject("EVIDENCE", "decisive findings require evidence");
        }
        Ok(f)
    }
    pub fn to_value(&self) -> Value {
        json!({"criterion_id":self.criterion_id,"verdict":self.verdict.as_str(),"score":self.score,"evidence":self.evidence.iter().map(DigestRef::to_value).collect::<Vec<_>>(),"explanation":self.explanation})
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AssessmentStatus {
    Completed,
    Inconclusive,
    Error,
}
impl AssessmentStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Completed => "completed",
            Self::Inconclusive => "inconclusive",
            Self::Error => "error",
        }
    }
    fn parse(v: &Value) -> Result<Self> {
        match v.as_str() {
            Some("completed") => Ok(Self::Completed),
            Some("inconclusive") => Ok(Self::Inconclusive),
            Some("error") => Ok(Self::Error),
            _ => reject("STATUS", "unknown assessment status"),
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Assessment {
    pub schema_version: u64,
    pub id: String,
    pub candidate: DigestRef,
    pub evaluator: Evaluator,
    pub rubric: DigestRef,
    pub status: AssessmentStatus,
    pub findings: Vec<Finding>,
    pub error: Option<String>,
}
impl Assessment {
    pub fn from_value(v: &Value) -> Result<Self> {
        bounded_value(v)?;
        let o = object(
            v,
            &[
                "schema_version",
                "id",
                "candidate",
                "evaluator",
                "rubric",
                "status",
                "findings",
                "error",
            ],
        )?;
        version(&o["schema_version"])?;
        let a = Self {
            schema_version: SCHEMA_VERSION,
            id: id(&o["id"])?,
            candidate: DigestRef::from_value(&o["candidate"])?,
            evaluator: Evaluator::from_value(&o["evaluator"])?,
            rubric: DigestRef::from_value(&o["rubric"])?,
            status: AssessmentStatus::parse(&o["status"])?,
            findings: list(&o["findings"], Finding::from_value)?,
            error: if o["error"].is_null() {
                None
            } else {
                Some(text(&o["error"])?)
            },
        };
        unique(a.findings.iter().map(|f| f.criterion_id.as_str()))?;
        match a.status {
            AssessmentStatus::Error if a.error.is_none() || !a.findings.is_empty()=>return reject("STATUS","evaluator errors require an error and no findings"),
            AssessmentStatus::Completed if a.error.is_some() || a.findings.is_empty() || a.findings.iter().any(|f|f.verdict==Verdict::Inconclusive)=>return reject("STATUS","completed assessment requires decisive findings and no evaluator error"),
            AssessmentStatus::Inconclusive if a.error.is_some() || !a.findings.iter().any(|f|f.verdict==Verdict::Inconclusive)=>return reject("STATUS","inconclusive assessment requires an inconclusive finding and no evaluator error"),
            _=>(),
        }
        Ok(a)
    }
    pub fn to_value(&self) -> Value {
        json!({"schema_version":self.schema_version,"id":self.id,"candidate":self.candidate.to_value(),"evaluator":self.evaluator.to_value(),"rubric":self.rubric.to_value(),"status":self.status.as_str(),"findings":self.findings.iter().map(Finding::to_value).collect::<Vec<_>>(),"error":self.error})
    }
    pub fn reference(&self) -> DigestRef {
        DigestRef::of(&self.id, &self.to_value())
    }
    /// Report-level aggregation only: call `validate_assessment` against the
    /// frozen rubric, evidence and authority before using this as an outcome.
    /// This is an evaluator judgement, never an independent improvement measure.
    pub fn passes(&self) -> bool {
        Self::from_value(&self.to_value()).is_ok()
            && self.status == AssessmentStatus::Completed
            && self.findings.iter().all(|f| f.verdict == Verdict::Pass)
    }
    /// Application record placed inside the existing task Result `output`.
    pub fn payload(&self) -> Value {
        json!({"profile":PROFILE,"record":self.to_value()})
    }
    pub fn from_message(message: &Message) -> Result<Self> {
        message.validate().map_err(|e| AssessmentError {
            code: e.code,
            message: e.message,
        })?;
        let payload = Value::Object(message.payload.clone());
        if message.kind != "Result" {
            return reject(
                "KIND",
                "assessment carriage requires an existing task Result",
            );
        }
        let task = object(&payload, &["task_id", "reply_to", "status", "output"])?;
        id(&task["task_id"])?;
        id(&task["reply_to"])?;
        if !matches!(task["status"].as_str(), Some("completed" | "unresolved")) {
            return reject(
                "STATUS",
                "task Result requires completed or unresolved status",
            );
        }
        let o = object(&task["output"], &["profile", "record"])?;
        if o["profile"] != PROFILE {
            return reject("VERSION", "unsupported assessment payload profile");
        }
        let a = Self::from_value(&o["record"])?;
        if a.id != message.message_id || a.evaluator.id != message.sender {
            return reject(
                "AUTHORITY",
                "assessment identity and evaluator must match envelope message_id and sender",
            );
        }
        Ok(a)
    }
}
