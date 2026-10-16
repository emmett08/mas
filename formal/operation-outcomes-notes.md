# Admission, observation and guarded operation outcomes

These two finite models cover the generic contracts used by the Rust host and
the incident-response example. MAS wire version 1 is unchanged. External
operation admission, execution and verification are adapter responsibilities;
the message protocol does not acquire Kubernetes policy or a remediation profile.

Run this subset, or omit `--operations-only` to include it in the full suite:

```sh
cargo run --locked --manifest-path rust-protocol/Cargo.toml --bin verify -- \
  --operations-only --out formal/evidence/operations
```

The runner uses SPIN full-state search, a 100,000-step depth bound, 512 MiB and
30 seconds per command. It records commands, tool versions, source hashes, raw
logs, simulation seeds 17/29/53, counterexample trails and their replays. Safety
and LTL are separate checks; both are repeated without partial-order reduction.
There is no process fairness option or probabilistic claim. A missing tool,
resource exhaustion, wrong counterexample or incomplete search fails the run.
The suite works from this repository alone; references to other repositories
below identify implementation correspondences, not runtime dependencies.

## Service and bounds

`admission-observation.pml` represents one logical Request, two possible
identities, one durable message record, one independent mailbox occupant, two
snapshot reads, two admission attempts per invocation and one explicit resume.
The implementation uses eight attempts. The smaller bound exposes retry
exhaustion; it does not prove an arbitrary bound or bound total attempts across
unrestricted explicit resumes. Identity 2 is exercised by a deliberate defect.

Only a typed, definitely-not-admitted mailbox-full outcome enables the automatic
retry loop. A rejected send can still commit expiry reconciliation of existing
tasks. An unknown admission blocks the current invocation. A subsequent explicit
resume may resend the identical Request and receive its original duplicate
receipt before the first worker launch. That is distinct from redispatching an
external effect. A duplicate receipt after expiry is non-actionable and cannot
launch a worker. Deadline and cancellation flags represent observations, not a
proof about physical time between a check and a side effect.

`operation-outcomes.pml` represents one operation and immutable attempt report,
one original owner, one durable reservation, recovery, two concurrent
reconcilers and two append positions. Production retains up to eight observations;
this is a finite abstraction, not an unrestricted-count refinement proof. Each
reconciler reads before its compare-and-swap write, so either can lose the race.
Matching versus mismatched bindings abstract exact operation ID, attempt ID,
report digest, target UID and original-template comparisons. Cryptographic hash
collision resistance and authentication are assumptions, not checked properties.

An acknowledged original reservation is the only execution permission. Recovery
can publish `unknown` before a delayed original owner acts; the original effect
may therefore occur after that report. A later desired-state observation does
not prove that the original attempt caused it and cannot authorise another
dispatch. Original effect and verification outcomes remain immutable while
observations append. The same append rule checks duplicate observation IDs and
rejects changed content. Both cases are explicitly exercised after the two
concurrent writers; arbitrary numbers of writers are outside the model.

SQLite transactions and Kubernetes create/status CAS/guarded patch operations
are assumed atomic at their documented boundaries. Reservation acknowledgement,
dispatch, external effect, effect response and report persistence are separate
crash points. The model admits crashes before and after an effect, a lost
reservation acknowledgement, a lost effect response and late original execution.
It excludes storage loss/corruption, credential compromise and arbitrary API
semantics. The helper's adapter must distinguish definite rejection from an
uncertain execution result truthfully.

## Properties and controls

| Property | Meaning |
| --- | --- |
| `ADMISSION_ATTEMPT_BOUND` | Actual sends within one invocation stay within the configured finite bound. The observer counter resets only on explicit resume. |
| `ADMISSION_UNKNOWN_NOT_RETRIED` | Unknown message admission cannot re-enter the automatic retry loop. |
| `ADMISSION_IDENTITY_RETAINED` | Retries and explicit resume retain the original Request identity. |
| `ADMISSION_LIVE_AT_RETRY_CHECK` | A retry requires an observed live deadline and no observed cancellation. |
| `ADMISSION_LAUNCH_REQUIRES_ACTIONABLE` | An expired duplicate receipt does not permit worker execution. |
| `OBSERVER_PRESERVES_PROTOCOL_STATE` | Snapshot reads do not mutate message, expiry or revision state. |
| `OPERATION_ORIGINAL_RESERVATION` | Dispatch uses the original acknowledged reservation, including when the original owner is delayed. |
| `OPERATION_ONE_DISPATCH` | The finite operation grants at most one execution. |
| `OPERATION_VERIFICATION_REQUIRES_EVIDENCE` | A passed verification requires an acknowledged effect and positive complete postcondition evidence. |
| `OPERATION_REPORT_IMMUTABLE` | Reconciliation preserves both original outcome axes. |
| `OPERATION_OBSERVATION_BOUND` | An appended observation matches its exact report and target binding. |
| `OPERATION_OBSERVATION_IMMUTABLE` | Appending or rejecting a conflicting ID preserves every existing record. |
| `OPERATION_OBSERVATION_REPLAY_IDEMPOTENT` | Exact replay returns a duplicate without appending. |
| `OPERATION_OBSERVATION_CONFLICT_REJECTED` | Changed content under an existing observation ID is rejected. |
| `admission_settles` | A started host invocation eventually returns completion, refusal, uncertainty or exhausted capacity in this finite model. |
| `operation_settles` | The original attempt and the two bounded observations settle, possibly with uncertainty. |

Positive progress relies on continued local execution and finite external
response/failure observations. Every nonterminal path consumes finite work;
permanent stalls deliberately falsify the LTL claims. Settlement includes
uncertainty, rejection and capacity exhaustion. It does not mean successful
delivery, a successful external effect or recovery of the workload.

The 23 cases perform 27 checks: eight positive safety/LTL searches, eleven
deliberate safety defects, two permanent-stall cycles and six reachability
witnesses. The witnesses demonstrate successful capacity retry, a lost commit
reply, explicit duplicate recovery, verified completion, a late original effect
after unknown reporting, and desired state observed while attribution stays
unknown. Expected counterexamples validate the property harness; they are not
positive protocol guarantees. The checked source and tool versions are retained
in the generated `summary.json`.

## Implementation correspondence

| Model action | Implementation and meaningful tests |
| --- | --- |
| Typed send/duplicate/capacity refusal | `rust-protocol/src/store.rs`, `admission.rs`; storage tests distinguish definite rejection, mailbox pressure and unknown storage errors, including misleading diagnostic text. |
| Read-only snapshot | `rust-protocol/src/observation.rs`; observation tests check no expiry reconciliation, receive flag or acknowledgement mutation. |
| Bounded automatic retries and explicit resume | `mas-host/crates/mas-host-core/src/execution.rs::admit_request`; host tests exercise acknowledgement draining, exhaustion, original identity/deadline, cancellation, unknown storage and lost-commit duplicate recovery. |
| Original execution grant and separate outcomes | `mas-host-examples/examples/k3s-live/deploy/guarded-operation.mjs::executeGuardedOperation`; readiness and independent key-value adapter tests distinguish reserved execution, definite rejection, unknown effect, failed postcondition and unavailable evidence. |
| Durable reservation and first terminal report | `mas-host-examples/examples/k3s-live/remediator/controller.ts`; tests cover lost reservation acknowledgement, crash recovery, competing owners and delayed original execution. A helper-local `not_started` result after reservation error is not published over an uncertain journal. |
| Bound append-only observations | The controller, `shared/resolution.ts` and server acceptance tests bind observation IDs, operation/attempt IDs, original report digest and target identity; concurrent CAS and exact/conflicting replay tests provide implementation evidence. |

These named tests connect selected abstract behaviours to executable code. The
Promela model is not generated from Rust or TypeScript and supplies no proof that
all executions refine it. In particular, the message format does not establish
the truth of an agent diagnosis, observed-state causation, external fencing,
unrestricted exactly-once effects, physical durability or real-time progress.
