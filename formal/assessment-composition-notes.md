# Admission, assessment and revision across commit boundaries

`assessment-composition.pml` adds a composed workflow to the separately checked
hybrid services. One uncertain request, its actor-scoped observations, receipt,
evaluation reservation, dispatch, retained assessment, terminal reply and one
revision share the same state. A pending original send can commit after an
observation found no record. An identical retry retains its original identity.
Cancellation, expiry and one restart can occur between these operations.

This is a finite integration contract, not a proof of every Rust/Python host
execution. The native correspondence test executes the same seven named traces
against real SQLite `Store` operations and the authoritative assessment
validators. Dispatch and workflow cancellation decisions in that test remain
an explicit test adapter. The separate `mas-host` journal and execution policy
need their own implementation checks.

## Service, environment and bounds

The service either records the original assessment/revision result or reaches a
declared stopped/unknown outcome. Cancellation and expiry prevent new revision
reservations. A provider reply already in flight may still become retained
evidence; it cannot overwrite an earlier task terminal. Retained evidence alone
does not authorise another provider call.

The domain contains one run and request identity, the sender and one foreign
observation identity, two distinguishable candidate/content values, one
evaluation call, one revision call, two scoped observations, one identical
resume, and at most one restart. Cancellation, expiry and restart are optional
one-shot choices. Both normal completion and absence of these interruptions
remain possible. Capacity is sufficient for the selected request, response and
cancellation records; capacity contention remains covered by the transport
models. Routes, membership, evaluator authority and rubric are frozen.

Every scheduler branch is atomic only for its named transaction, scoped read,
dispatch or recovery observation. Reservation, dispatch, assessment retention,
task response, revision reservation and revision result are separate branches.
The single nondeterministic scheduler explores their interleavings rather than
executing an entire workflow in one atomic section. Storage recovery, exact
JSON types, trusted bound identities and collision-free record digests are
assumptions. Database corruption, network authentication, physical deadlines,
arbitrary numbers of agents and provider judgement quality are outside scope.

`Cancel` includes a **host workflow stop signal** and an attempted cancellation
of the open protocol task. If a task has already completed, its terminal state
remains completed while the host can still stop later workflow calls. Native
task cancellation alone does not cancel an entire run or block arbitrary
`Store::reserve_call` calls. Hosts must enforce their workflow stop policy.
Expiry is observed explicitly; elapsed time is not represented by Promela
`timeout`.

On restart, a call with a reservation and no retained result becomes unknown;
the model does not redispatch it. A retained assessment is reused with its exact
content. This conservative policy admits unknown outcomes even when a crash
occurred before dispatch. It does not claim exactly-once external effects.

## Properties and correspondence

| Named property | Meaning and implementation boundary |
| --- | --- |
| `COMPOSE_REQUEST_IDENTITY` | Late original commit and same-identity resume retain one request; `Store::send_checked` and retained sequence/content checks. |
| `COMPOSE_ABSENCE_IS_SNAPSHOT` | Observed absence cannot promise that the original send will never commit; `Store::reconcile_message` followed by the scheduled original `send_checked`. |
| `COMPOSE_OBSERVATION_SCOPED`, `COMPOSE_OBSERVATION_READ_ONLY` | Foreign actors see no receipt; an observation does not receive or acknowledge a message. Native tests compare complete before/after snapshots. This model has one run; cross-run isolation remains in the hybrid model. |
| `COMPOSE_RECEIPT_REQUIRES_ADMISSION` | A receipt follows durable admission; `Store::receive`. |
| `COMPOSE_DISPATCH_RESERVED`, `COMPOSE_REVISION_RESERVED` | Calls use an existing durable reservation; real `Store::reserve_call` plus an explicitly modelled host dispatch policy. |
| `COMPOSE_UNKNOWN_NOT_REDISPATCHED` | Restart never causes another dispatch for an unresolved call; native tests establish retained reservations and rejection of reuse, while physical dispatch policy requires host conformance. |
| `COMPOSE_RETAINED_ASSESSMENT` | Retained assessment content survives reopening; `Store::finish_call` stores the exact assessment evidence and `observe` retrieves it. This checks the native call-evidence mechanism, not the separate host journal. |
| `COMPOSE_TERMINAL_IMMUTABLE` | A late assessment Result cannot replace cancellation or expiry; task validation and insertion share the repository transaction. |
| `COMPOSE_FEEDBACK_BOUND` | A revision refers to the exact retained assessment; `validate_candidate_revision` checks feedback and parent digests, including a stale-reference rejection. |
| `COMPOSE_STOP_PREVENTS_REVISION` | Workflow stop or observed expiry prevents another revision reservation. Expiry is enforced by Store; workflow cancellation is a host-adapter obligation. |
| `assessment_composition_settles` | `<> settled`: the finite workflow reaches completed, stopped or unknown. This is not eventual successful diagnosis or delivery. |

Every normal transition consumes a one-shot flag or advances a finite stage.
Together with the no-invalid-end-state search, that finite action bound permits
settlement without a fairness option. A permanently stalled provider, an unfair
external host, or unrelated infinite computation is excluded. The deliberate
stall control introduces an infinite execution and must fail the LTL property.

The reusable `apply` transitions generate deterministic Promela executions from
`assessment-composition-traces.json`. The native test consumes that same corpus,
checks operational results along the path and compares final receipt, terminal,
reservation and evidence observations. This is selected trace correspondence,
not exhaustive refinement. Simulation and corpus results are also separate
from the exhaustive searches. SPIN may report unreachable alternatives inside
each inline expansion because its operation argument is constant; the four
reachability controls independently witness the intended useful scenarios.

## Reproduction and negative controls

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml --test assessment_composition
cargo run --locked --manifest-path rust-protocol/Cargo.toml --bin verify -- \
  --assessment-composition-only --out /tmp/mas-assessment-composition
```

The default verifier includes this model and corpus. Positive safety and LTL
searches run with and without partial-order reduction. Ten isolated mutations
challenge identity retention, observation scope/read-only behaviour, an
invalid promise after absence, reservation-before-dispatch, unknown-call replay,
history retention, terminal immutability, exact feedback and stopping rules.
One liveness mutation must produce a replayed acceptance cycle. Four separate
witnesses establish revision completion, late admission after absence, retained
late evidence after cancellation, and restart with retained assessment.

The runner uses full-state storage, depth 100,000, 512 MiB and 30 seconds per
command; exhaustion is inconclusive. It records models, the corpus and runner
hashes, commands, tool versions, simulations, unreached-state output, trails
and replays. A mutation succeeds as a control only when its named assertion or
LTL cycle is detected and replayed. The [receipt](../evidence/assessment-composition/verification.json)
and [raw logs](../evidence/assessment-composition/spin-checks.zip) retain this
increment's evidence separately from the historical paper evaluation.
