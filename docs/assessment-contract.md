# Auditable assessment payloads

`mas_protocol_codec::assessment` defines the `assessment.v1` application payload
profile. It preserves the existing MAS message grammar and envelope version `1`.
No new transport kind, peer privilege or delivery guarantee is introduced.

The contract binds a judgement to an exact candidate, rubric, evaluator and
supporting evidence. The host owns invocation permissions, retained context,
durable history and bounded revision. The example owns incident criteria and
the held-out outcome experiment. A valid assessment establishes neither the truth
of its findings nor an improvement in independent outcomes.

## Records and exact references

Every top-level record has integer `schema_version: 1` and a bounded protocol
`id`. Unknown fields and unsupported versions are rejected. Optional fields
are present as JSON `null`; lists may be empty only where their contract permits.

| Record | Required fields beyond version and identity |
| --- | --- |
| Candidate | `output`, `output_sha256`, `parent`, `feedback` |
| Evidence | `content`, `content_sha256` |
| Rubric | `criteria`: nonempty unique criterion records |
| Assessment | `candidate`, `evaluator`, `rubric`, `status`, `findings`, `error` |

A reference is exactly `{"id":"…","sha256":"…"}`. It hashes the **complete
referenced record**, including its version, identity and revision links.
`output_sha256` and `content_sha256` separately bind their contained JSON values.
These two digest meanings are deliberately distinct. A reference with a valid
digest shape still requires resolution against a retained record.

Canonical bytes are UTF-8 compact JSON: object keys sorted by Unicode scalar
value (equivalently UTF-8 lexicographic order for valid strings), array order
preserved, exact integer decimal notation and JSON string escaping as emitted
by the pinned `serde_json`. No Unicode normalisation is performed. SHA-256 is
lowercase hexadecimal. This profile is not RFC 8785's floating point number
profile. `canonical_digest` sorts explicitly, including when a downstream crate
enables `serde_json/preserve_order`.

Readers must preserve integer values before verifying digests. JavaScript
readers using ordinary `JSON.parse` must reject numbers outside the safe-integer
range, or use an exact-integer parser and canonical encoder. Treating rounded
numbers as the original content invalidates the reference claim.

## Criteria and evaluator authority

A criterion is `{id, measurement, minimum, maximum}`. `measurement` is
`deterministic` or `model`; bounds and scores are exact signed 64-bit integers.
The named, versioned rubric and evaluator implementation define the criterion's
meaning and score direction. The contract does not infer a pass threshold from
the numerical range. An evaluator is `{id, version, kind}` with the same two
kinds. Applications must freeze the referenced evaluator implementation and
rubric before an experiment; changing their meaning requires a new version or
identity in the host's frozen configuration.

`deterministic` describes how the evaluator runs. A deterministic syntax check
does not thereby become an independent measure of diagnosis correctness.
`model` identifies a model judgement. Neither kind establishes independence
from generation data, feedback or held-out answers.

A finding contains `criterion_id`, `verdict`, `score`, `evidence` and a nonempty
`explanation`. Every assessment addresses every criterion in its rubric, except
an evaluator error. An evaluator may only judge criteria of its declared kind;
use a separate rubric per evaluator when combining deterministic and model
assessments. Preserve separate assessments when their findings conflict.

| Assessment status | Finding rules | Meaning |
| --- | --- | --- |
| `completed` | Every finding is `pass` or `fail`, with an in-range score and evidence | Evaluation completed; some criteria may have failed |
| `inconclusive` | At least one `inconclusive` finding, whose score is `null` | The evaluator could not decide at least one criterion |
| `error` | Empty findings and a nonempty `error` string | Evaluation failed to produce a usable assessment |

`validate_assessment` revalidates direct Rust constructions, resolves exact
candidate/rubric/evidence references and compares the complete evaluator record
with invocation authority supplied by the host. Supplying an evaluator from the
untrusted payload as its own authority defeats that boundary. The host must use
its frozen invocation configuration. `passes()` aggregates an assessment's
findings after contextual validation; it is not an outcome measurement or an
authorisation to execute remediation.

For message carriage, `Assessment::payload()` returns the application record
`{"profile":"assessment.v1","record":…}` for the existing task Result's
`output` field. The outer task payload retains `task_id`, `reply_to`, `status`
and `output`; its status remains `completed` or `unresolved`. A completed task
may return an inconclusive assessment or evaluator error record; an unresolved
task cannot carry a completed assessment. `Assessment::from_message` validates this nesting and
requires assessment identity and evaluator identity to equal
the envelope's `message_id` and `sender`. Existing repository admission still
checks authenticated actor, route, budget and delivery state. The run context
comes from the envelope and host record scope. The public integration test
inserts the request and Result through the real `agent-task/1` store, verifies
duplicate delivery and rejects a misplaced top-level assessment payload.

## Revision and retention

An initial candidate has `parent:null` and `feedback:[]`. A revised candidate
has a distinct identity, the exact parent reference and references to every
assessment supplied as revision feedback. `validate_candidate_revision`
rejects absent, additional, stale and foreign-parent feedback. Resampling may
have a parent and no feedback. This permits a budget-matched control arm without
pretending it used assessment information.

`AssessmentHistory` specifies exact-content insertion semantics in memory:
identical delivery returns `false`, conflicting content under an existing
identity is rejected, and different identities preserve conflicting findings.
It is not durable storage or an additional execution runtime. Production hosts
apply the same rule in their existing persistence transaction and retain the
invocation's original input and policy. Validation cannot establish that a
worker only read those inputs; the trusted-worker boundary remains explicit.

## Design and verification

The smallest compatible design is a versioned payload profile plus pure
validators. A new wire grammar would require migration without adding useful
assessment semantics; leaving records untyped would make candidate binding and
error handling application-specific. Record decoding, canonical primitives,
contextual validation and identity retention are separate modules with one
public facade. No public envelope or existing database format changes.

The repository remains on Rust edition 2021, minimum manifest Rust 1.85 and
pinned toolchain 1.99.0. New Rust files use `rustfmt`; existing unrelated files
retain their repository formatting. No new crate dependency or lint profile is
introduced. [Formal model notes](../formal/assessment-revision-notes.md) state
the finite assumptions, checked properties and implementation correspondence.

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml --test assessment
cargo run --locked --manifest-path rust-protocol/Cargo.toml --bin verify -- \
  --assessments-only --out formal/evidence/assessments
```

## Pure validation for host adapters

`mas assessment COMMAND.json` (or `mas assessment -` with stdin) exposes the same
Rust validators to hosts that do not link the crate. It performs no database
operation, invocation, admission, disclosure or permission change. Its input is
one strict integer-JSON object, with a total 65,536-byte and 32-container limit.
Invalid input produces a fixed diagnostic without echoing its contents. The
normal administrative response is `{"ok":true,"result":...}`.

| `operation` | Exact additional fields | Result |
| --- | --- | --- |
| `record` | `kind`, `record` | Structurally validated `record` and full-record `reference` |
| `assessment` | `assessment`, `candidate`, `rubric`, `evidence`, `authority` | Contextually validated assessment `reference` and Boolean `passes` |
| `revision` | `candidate`, `parent`, `feedback` | Contextually validated candidate `reference` |
| `digest` | `value` | Canonical `sha256` of the bounded integer-JSON value |

Record kinds are `candidate`, `rubric`, `evidence`, `evaluator` and `assessment`.
`parent` is a complete retained candidate or null; evidence and feedback are
arrays of complete records. Unknown command fields are rejected. Structural
record validation alone does not resolve references or authorise an evaluator.
A host must supply `authority` from its frozen invocation registration, resolve
records from its own retained history, and commit validated history and its
decision atomically. Passing an evaluator's claimed identity back as authority
does not establish authority. The host's feedback and revision policies remain
responsible for permission, disclosure and budget enforcement.

`assessment_cli.rs` exercises the actual administrative process, exact integers,
foreign authority, changed evidence, feedback mismatch, error assessments,
duplicate JSON fields and bounded input rejection.
