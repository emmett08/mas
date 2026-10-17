# Finite transport, recovery and bridge models

This model applies Holzmann's separation of service, environment, vocabulary,
representation and exchange rules to the durable local message bus. It checks
transport behaviour. It does not model an LLM, establish message truth, verify a
task result, or establish exactly-once application effects.

The same checker also runs separate host-recovery, Cursor-job and durable-bridge
models. Each has its own state variables, assumptions and implementation mapping;
none expands the transport model's scope implicitly. The original bridge-extension
run contained 67 cases and 95 checks. The subsequent adversarial audit adds a
local task-service model, named reachability witnesses and selected shared
implementation traces. Consult the current evidence summary for the exact run
counts; positive safety checks, deliberate defects, reachability witnesses and
liveness assumption failures are distinct evidence categories.

The [workflow composition model](workflow-composition-notes.md) adds two
interleaved leaf invocations with shared admission, stable assignments, unknown
effects, cancellation, expiry and an all-completed join. It checks safety and
bounded settlement with and without reduction, seven safety mutations, an
indefinite-response counterexample and a successful-join reachability witness.
Its finite response assumptions and implementation boundary are stated separately.

The [admission and operation-outcome models](operation-outcomes-notes.md) check
typed admission retries, read-only observation, original execution reservations,
independent effect and verification outcomes, and append-only reconciliation.
They include delayed original effects after uncertain recovery and distinguish
explicit same-identity message resume from replay of an uncertain external effect.
Use `--operations-only` to run this subset.

The [shared-authority contract](shared-authority-notes.md) adds competing
controller/worker commands, independently active hierarchical quota ceilings,
worker boot and lease fencing, retained late evidence and admission-closed
restoration from final sealed archives. Its shared corpus executes selected
observations against the native PostgreSQL authority. These additional models
remain bounded safety checks with explicit transaction and fencing assumptions.

The [assessment and revision model](assessment-revision-notes.md) adds exact
assessment identity, evaluator authority, immutable findings, two-world input
disclosure, replay and bounded revision checks. Use `--assessments-only` for
this subset; the default verifier also includes it.

The [hybrid-host contract model](hybrid-contracts-notes.md) checks new-request
reply-route preflight, typed private gateway outcomes and scoped read-only
reconciliation, distinct-actor candidate/context quorum, and durable assessment
replay without authority escalation or redispatch. Use
`--hybrid-contracts-only`; the default verifier also includes these cases.
Its four independently selected finite services do not establish cross-service
concurrency or unrestricted implementation refinement.

## Reproduce

Install SPIN, a C compiler and Linux utilities `sh` and `sha256sum`, then run
from the repository root:

```sh
cargo run --locked --manifest-path rust-protocol/Cargo.toml --bin verify --
```

The Rust runner reads `examples/*.json`, renders `protocol.pml.in` and
`actions.pml.in` separately for
each topology, samples seeds 17 and 29, compiles assertion and LTL verifiers,
and retains raw logs, model hashes, tool versions, commands and counterexample
trails in `formal/evidence`. Each command has a 30-second, 512-MiB budget;
the verifier uses a 100,000-step search-depth limit and full-state storage.
Individual topology selection is available through `--only star`. To prepare
models without claiming execution, use `--generate-only --out /tmp/mas-models`.
The process exits nonzero for an incomplete search, failed check or unavailable
tool. Inspect `summary.json`; exit status alone is insufficient evidence.

Each topology receives bounded-fault safety and two separately selected LTL
checks, plus safety with unlimited repeated loss, restart and redelivery. The
smallest topology is also checked without partial-order reduction; a star
instance with one outstanding slot per mailbox exercises capacity rejection.
Both special cases receive bounded checks and unlimited-fault safety checks.

The suite also checks task lifecycle, transaction recovery and host expiry
safety, plus [host checkpoint recovery](host-recovery-notes.md). Those core
models contain eleven isolated defects that must produce replayable counterexamples: early
acknowledgement, forbidden routing, a permanently disabled acknowledgement,
duplicate task terminals, partial pre-commit persistence, duplicate insertion
following a lost commit response, expiry overwriting an existing terminal, uncharged host dispatch, unknown-attempt
replay, stale host completion and conflicting result overwrite.
Two additional cases deliberately ask for eventual acknowledgement with
unlimited faults, with and without process fairness. These are labelled
`assumption_counterexample`, distinct from implementation-defect mutations.
A `PASS` for either kind means the expected counterexample was found and
replayed. It does not mean that its violated property holds. `expected_result`,
`purpose` and each check's observed status make that distinction explicit.
Every assertion mutation must replay its named property; a missing or unrelated
assertion fails acceptance. Liveness counterexamples must replay a cycle for
the selected LTL property, with that property identified in both checker and
replay output.

## Contract and bounds

The service accepts a canonically validated message from a registered sender,
retains it under `(run_id, message_id)`, returns addressed pending messages to
the recipient, and records acknowledgement only after a receive transaction.
Identical resends refer to the original record. Changed contents under the
same identity are rejected. Mailbox capacity counts unacknowledged messages;
acknowledgement releases capacity but preserves the identity record.

Each instance has the example's two to four participants, two distinct message
identities and two abstract payload values. The environment chooses each
message's route independently and nondeterministically from *all* declared
directed edges. The frozen directed ACL and registered membership do not
change during a run. The two records are durable slots, explicitly bounded
SQLite mailboxes rather than transient Promela channel queues. A channel would
incorrectly remove a durable message on receive.

Each participant is a separate Promela process. Each state-changing atomic block corresponds
to one successful `BEGIN IMMEDIATE` transaction; no-state-change observations
are collapsed onto the send boundary. The runner is tested on Linux. Its constant POSIX shell wrapper applies
`ulimit -v`; macOS does not provide the same virtual-memory limit semantics.
This verification-host requirement does not constrain the portable Rust
protocol runtime. The parent Rust process enforces wall-clock timeouts.

SQLite provides serialisation for send, receive and acknowledgement. The
transport model places faults between transactions; `transaction-recovery.pml`
separately exposes staged writes, rollback, pre-commit crashes and lost commit
responses. Both models rely on SQLite's isolation and atomic recovery boundary.
Database corruption, permanent storage loss, broken recovery and unauthorised
host identity assertions remain outside their verified scope.

The bounded transport model permits one redelivery, one lost acknowledgement
invocation, one restart and one full-mailbox rejection per identity. These fault
actions are optional. `UNBOUNDED_FAULTS` removes the repetition guards for
redelivery, lost acknowledgement and restart. It preserves the finite state
representation while admitting arbitrarily many repetitions, including infinite
fault sequences. It retains the same message, participant and capacity bounds. Identical resend, changed-content rejection and early-acknowledgement
rejection are stipulated stuttering observations at the send boundary: they
change no persistent state. Their placement abstracts away independently
scheduled client invocations, which the runtime tests exercise. A forbidden
self-route is introduced only by the isolated route mutation; normal routing
selects declared edges. Runtime tests supply enforcement evidence for rejected
forbidden routes.
A full-mailbox rejection leaves the request available for a later send attempt;
there is no silently dropped accepted message. The capacity-one run admits
contention between two messages addressed to the same worker. A lost
acknowledgement means the invocation never enters its database transaction;
acknowledgement-response loss and repeated acknowledgements are separately
covered by runtime tests. A restart clears caller memory while preserving the
durable received flag, so acknowledgement may legally follow restart without
another receive.

Positive transport LTL checks require no process-fairness option. In the
bounded model, every service or fault transition makes a previously false
one-shot flag true; each process eventually exhausts its enabled nonterminal
transitions. A finite ranking function counts remaining one-shot transitions
and live processes. Together with the absence of invalid terminal states, this
rules out infinite non-progress executions without assuming fairness. This
argument concerns the model: it has no unrelated infinite computation or
worker that permanently stops calling the service.

Safety checks admit unlimited repeated faults. Successful eventual
acknowledgement does not follow in that regime. The retained counterexamples
include a receiver that redelivers one message forever, even under weak process
fairness. Process fairness schedules the receiver; it does not force that
receiver to take its acknowledgement branch. The finite message universe and
SQLite correctness boundary remain explicit; no unrestricted implementation
proof is inferred.

The model abstracts `receive(limit=1)` as selecting any unacknowledged addressed
slot. SQLite actually selects by insertion sequence. This adds possible orders
and does not establish a formal refinement proof. Payload integers preserve
distinguishable identities and immutable-content checks only; the actual Rust
codec and full message envelope require implementation tests. The model has
one frozen run and version, sufficient send/byte/time budgets, and no identity
expiry. It does not verify cross-run/version isolation, cancellation, dynamic
membership, priority, real-time deadlines, an application reply's `reply_to`,
or substantive task-state transitions.

## Properties

| Name | Desired property | Assumptions or qualification |
| --- | --- | --- |
| `stored_bound` | At most two durable identities exist. | Finite message universe, independent of per-mailbox capacity. |
| `mailbox_bound` | Unacknowledged occupancy never exceeds configured capacity. | Rejected sends leave state unchanged. |
| `route_safety` | Every accepted slot follows its frozen directed ACL. | Valid-edge environment plus a failing ACL mutation; enforcement tested in Rust. |
| `identity_immutable` | Accepted slot `m` retains abstract content `m`. | Duplicate and collision rejection are specified transitions, supported separately by runtime tests. |
| `receipt_requires_acceptance` | A received message was accepted. | Durable slots are never erased in the model. |
| `ack_requires_receipt` | Acknowledged identity `m` has been received as identity `m`. | No cross-identity substitution; the early-ack mutation must fail. |
| `message_0_progress` | `[] (accepted[0] -> <> acknowledged[0])` | Bounded-fault model; no fairness assumption. |
| `message_1_progress` | `[] (accepted[1] -> <> acknowledged[1])` | Bounded-fault model; independently checked without fairness. |
| Valid termination | A stopped transport participant has observed both identities acknowledged. | Safety checks invalid end states; unlimited-fault executions may run forever. |

Assertions implement the safety properties at every relevant transition. The
simulation report checks that both antecedents become true and both identities
are acknowledged in each recorded normal trace. These traces witness reachable
behaviour; they do not establish exhaustive coverage. The full-state verifier
must report zero errors without resource/depth exhaustion before a positive
complete finite-model claim is permitted. No bitstate or hash-compact mode is
used. The LTL formulae are stutter-invariant, with no `X` operator; they are
compatible with the declared partial-order reduction.

## Implementation correspondence

| Model event/state | Implementation counterpart | Required evidence |
| --- | --- | --- |
| Frozen `allowed` and participant set | `Store::create_run` run configuration | Reject changed configurations; reject prohibited routes. |
| `send`, `accepted`, `content`, `stored` | `Store::send` insert and canonical content equality check | Canonical round trip; identical retry idempotence; changed-content rejection. |
| `occupancy`, `reject_full` | Pending recipient count and mailbox capacity | Full mailbox rejects new identity; acknowledgement frees capacity; retry succeeds. |
| `receive`, `received` | `Store::receive`, persistent `received=1` | Addressed retrieval, durable flag, redelivery until acknowledgement. |
| `ack`, `acknowledged` | `Store::ack`, persistent `acked=1` | Reject wrong recipient/unreceived identity; preserve other identity's state. |
| `restart` | Reopen the same SQLite database | Identity, receive and acknowledgement state survive reopening. |
| Payload identity abstraction | Rust `nom` codec, canonical envelope | Distinct run/message identity and payload preserved; malformed inputs rejected. |
| Private staged writes, commit, rollback and recovery | SQLite `BEGIN IMMEDIATE`, commit/drop and reopened connection | Rollback leaves no durable partial write; committed retry preserves one row. |
| Explicit host expiry event and persisted latch | `Store::reconcile` and reconciliation during service calls | Expiry rejects new work independently of mailbox/budgets; preserves committed outcomes. |

These correspondences and integration tests supply conformance evidence,
not a proof that every Rust/SQLite execution refines this model. In
particular, exactly-once message insertion does not imply exactly-once external
tool execution. Recipients need their own idempotency or transactional mechanism
when applying effects.

## References

- [Holzmann, *Design and Validation of Computer Protocols* (1991)](https://www.theswissbay.ch/pdf/Gentoomen%20Library/Networking/Design_and_Validation_of_Computer_Protocols_-_Gerard_Holzmann.pdf), §§2.2 and 2.7–2.8: service/environment/message/rule specification, simplicity and validation before optimisation.
- [SPIN command and simulation reference](https://spinroot.com/spin/Man/Spin.html).
- [Generated verifier options](https://spinroot.com/spin/Man/Pan.html).
- [Inline LTL and reduction restrictions](https://spinroot.com/spin/Man/ltl.html).
- [Atomic sequences](https://spinroot.com/spin/Man/atomic.html).

SPIN is appropriate for this finite asynchronous protocol question. A timed
model would be needed for deadline guarantees, a probabilistic model for
reliability estimates, and a cryptographic protocol model for remote
authentication; none is implied by this SQLite transport model.

## Task lifecycle safety

`task-lifecycle.pml` independently checks two distinct request identities, two
requesters and a shared worker. Result and Failure require the request to have
been received, reverse the participants, and refer to the exact request identity.
Cancel follows the requester-to-worker direction. Atomic terminal transitions
allow only the first Result, Failure or Cancel for each request; a mutation that
removes that guard must violate the terminal-count assertion. Stale-reply
rejection is a stipulated abstraction, with the actual guard covered by Rust
integration tests. The worker may stutter indefinitely, so this model claims
safety only and makes no eventual task-completion assertion.

## Model simplification record

An initial model stored independent Boolean markers for duplicate/rejected calls.
Their interleavings changed diagnostic state without changing the durable service
state and exhausted the 512-MiB bound in larger topology checks. The retained
initial logs record this as incomplete. The current model removes those markers
and represents the calls as stuttering observations, while retaining all route
choices, distinct participants, both message identities and all bounded
state-changing fault interleavings. This is an explicit abstraction change, not
an increase of the evidence claim or a silent resource-budget change.

## Transaction recovery safety

`transaction-recovery.pml` has two concurrent writers and a committed-snapshot
reader. Each writer acquires the serialising transaction lock, stages a row and
counter in private state, and either commits both atomically or discards its
staged writes. Rollback and a crash followed by recovery are possible at each
pre-commit stage. Losing a commit response leaves the durable row intact; a
retry validates the identity and returns the existing row. Crashes, rollbacks
and lost responses may repeat without a finite attempt bound. Assertions check
row/count agreement, identity immutability and the two-record bound. Mutations
that expose a staged row or increment the count on a duplicate must fail.

This verifies the protocol against an explicit SQLite recovery contract. It
does not establish that SQLite, its filesystem or storage hardware fulfils that
contract. Database rollback and reopen tests provide implementation evidence;
physical persistence failures remain a separate trust boundary. This safety
model makes no eventual-commit or eventual-response claim.

## Host expiry safety

`task-expiry.pml` starts with two accepted Requests. Result, Failure and Cancel
race with an explicit host observation that the run deadline is due. A service
call reconciles that observation before admitting fresh work. Expiry latches
the run permanently and changes only open tasks to `expired_outcome_unknown`.
A task already terminal remains unchanged. A later clock rollback cannot reopen
an expired run. The expiry transition does not require a message insertion,
available mailbox slot or unused communication budget; those conditions are
covered by runtime tests rather than hidden prerequisites to expiry.

Receipt and acknowledgement remain possible for closed tasks, without reopening
them. A received Request's `actionable` annotation is false when closed at that
transaction. An earlier receipt can still be followed by an external effect:
expiry does not prove that previously dispatched work never occurred. The model
therefore permits an effect after expiry while rejecting a new terminal message
for the closed task. A mutation allowing expiry to replace an existing terminal
must violate the single-terminal assertion.

The `due` observation is abstract, not a physical clock. No duration, monotonic
clock behaviour or eventual host invocation is proved. If all host activity
stops, reconciliation need not occur. Expiry records unresolved external work;
it is distinct from a successful result, an application failure, a cancellation
that demonstrably stopped effects, or successful delivery of an acknowledgement.

The optional provider extension adds a separate [Cursor durable-job model](cursor-jobs-notes.md). It checks dispatch and recovery safety without claiming provider liveness, billing bounds or correctness of generated repository work.

## Durable bridge service and event history

The [bridge model contract](bridge-notes.md) specifies three additional models.
`bridge-service.pml` combines one principal-scoped ingress key with one outbound
operation to test duplicate/conflicting submissions, admission before dispatch,
lost-receipt recovery, cancellation intent and stale terminal observations.
SQLite commits are atomic; dispatch and receipt persistence are separate steps.
Its finite scenario contains one principal, one key, two payload values, three
submissions, one restart and two observations. Authentication itself is assumed
at this boundary and is tested separately in the implementation.

`bridge-events.pml` checks atomic task/event commits, bounded task admission and
reads of committed event history across a reader restart. It uses two tasks,
three events and two reads; three events is the scenario size, not a claimed
implementation storage cap. The caller stores its own cursor and can reread
after a lost response. The model does not posit server acknowledgements,
exactly-once consumption or guaranteed push delivery.

Both models run with and without partial-order reduction and sample seeds 17,
29 and 53. Ten isolated defects must fail: duplicate receipt creation,
replacement of accepted content, unadmitted dispatch, replay of an unknown
effect, premature cancellation confirmation, stale terminal overwrite, lost
event commit, inbox overflow, exposure of an uncommitted event and advancing a
cursor after a lost, unobserved page. The normal
searches check assertions and invalid end states. They make no progress claim:
active and unknown work may remain unresolved when the host or peer stops.

`task-service.pml` separates local task state from remote lifecycle state. It
models two principals sharing a key, one task per principal, one scheduled task
with two revisions, two update keys, one worker and one crash. Atomic API
transactions interleave with separate worker claim, reply and final-commit
steps. It checks receipt ownership, stable accepted content, unique execution
registration, current-revision commits, late cancellation of paused work and
atomic lifecycle events. The second task's lifecycle is covered by a selected
shared trace, rather than by the exhaustive concurrent schedule.

The local model runs with and without reduction. Four mutations target specific
guards; five separate witnesses establish that both owners, late cancellation,
continuation, unknown recovery and foreign rejection are reachable. Assertion
mutations must replay at their named assertion; an
unrelated assertion failure cannot satisfy those checks.

The verifier and `test_task_service_conformance.py` both consume
`formal/task-service-traces.json`: 11 selected traces and 77 observed
steps. The verifier generates executions of the same Promela transition inlines;
the test checks the real service and SQLite repository with a controlled executor.
This establishes finite trace correspondence for the specified observations,
not unrestricted trace inclusion or a refinement proof. Host checkpoint recovery,
fixed grants and remote-store separation remain explicit model assumptions.

Protocol versions, HTTP/stdio framing, authentication, process ownership, storage
restoration and SDK behaviour remain implementation-test obligations. No formal
refinement proof connects all Python, Rust, SQLite or external peer executions
to these finite models. See the model contract for the requirement-to-property
mapping and the limits of this bounded sufficiency claim.
