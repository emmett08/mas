# Finite transport model

This model applies Holzmann's separation of service, environment, vocabulary,
representation and exchange rules to the durable local message bus. It checks
transport behaviour. It does not model an LLM, establish message truth, verify a
task result, or establish exactly-once application effects.

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

Each topology receives a safety check and two separately selected LTL checks.
The smallest topology is also checked with partial-order reduction disabled.
An additional star instance has one outstanding slot per mailbox to exercise
capacity rejection. Three isolated mutations must fail: acknowledge before
receipt, attempt a forbidden self-route, and permanently withhold message 0's
acknowledgement. Their trails are replayed and retained. The task lifecycle model adds a fourth negative control: two terminal
outcomes for one task. A negative-control
`PASS` means the deliberate defect produced a counterexample, not that the
mutated protocol was correct.

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

SQLite provides serialisation
for the implementation's send, receive and acknowledgement transitions. Faults
occur between transactions; torn writes, database corruption, process failure
during a transaction, permanent disk errors and unauthorised host identity
assertions are outside this model.

The model permits one redelivery, one lost acknowledgement invocation, one
restart and one full-mailbox rejection per identity. These fault actions are
optional. Identical resend, changed-content rejection and early-acknowledgement
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

The finite fault bounds describe the explored scenarios; they are not claims
that real failures obey these bounds. LTL uses weak process fairness (`-f`): an
indefinitely enabled worker eventually executes. Each duplicate/fault branch
can execute only once, so a worker cannot indefinitely choose faults instead
of acknowledgement. Arbitrarily many restarts, unlimited lost acknowledgements,
unfair scheduling and permanent non-response can defeat progress. The negative
liveness control retains that last counterexample rather than hiding it.

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
| `message_0_progress` | `[] (accepted[0] -> <> acknowledged[0])` | Finite faults and weak process fairness. |
| `message_1_progress` | `[] (accepted[1] -> <> acknowledged[1])` | Independently checked to expose individual-message starvation. |
| Valid termination | Every participant stops after both identities are acknowledged. | Safety build checks invalid end states. |

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
