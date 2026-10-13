# Independent implementation and model review

Review date: 13 October 2026. Scope: the native Rust transport, task ledger,
expiry reconciliation, capability gateway, examples and revised Promela evidence.
The reviewer worked in a separate agent context from the implementation authors
and returned findings directly to them. This review does not claim execution of
a live application LLM MAS.

## Findings and repairs

| Finding | Disposition |
| --- | --- |
| Malformed explicit topology could omit restrictions; nested tree dispatch could reuse reply identifiers. | Entry points require topology fields; replies derive their identity from their captured request. Retained regressions cover both repairs. |
| Rejected work could potentially roll back a newly observed expiry. | Reconciliation persists the run latch independently of rejected work; send and receive recheck inside serialised transactions. Expiry consumes no message, mailbox, byte or call capacity. |
| Native version-1 migration initially applied the payload JSON limit to the larger stored JSON envelope. | Migration now uses the envelope encoder allowance followed by wire validation. The maximal valid wire regression covers this boundary. |
| A terminal timestamp could describe a later clock reading than its admission decision. | Terminal state records the captured deadline-check observation. Admission linearises at that observation, rather than claiming commit completed before the deadline. |
| Interrupted verification could leave an earlier successful summary beside partial new evidence. | The runner marks evidence unfinished before replacing it; successful final results require completed checks and replayed counterexamples. |
| Model generation could accept malformed configurations rejected by the runtime. | Strict JSON parsing, unique identities, distinct endpoints and explicit bounded capacity now apply. |

Native schema version 1 is atomically migrated to version 2 by replaying retained
task history. Invalid migration rolls back. Existing terminal timestamps remain
unknown rather than being invented. Schema markers and recognised layouts do
not certify externally corrupted or maliciously altered databases.

## Independent implementation checks

The reviewer added `rust-protocol/tests/assumptions.rs` and ran all four tests
successfully after the final runtime changes. They establish the tested cases:

- A rejected late operation durably closes an open task despite exhausted message
  and mailbox capacity. Restoring a future deadline condition and reopening the
  database does not reopen the latched run.
- Closed queued Requests remain drainable but carry `actionable: false` and their
  task state. Reconciliation preserves the first committed terminal outcome and
  closes only open tasks.
- Malformed and forged gateway requests consume durable admission attempts.
  Reopening the gateway preserves global and credential-specific usage; one
  exhausted credential does not consume another credential's individual quota.
- Concurrent gateway instances share the same durable global admission cap.

The deadline-condition test deliberately changes the stored deadline to exercise
the latch deterministically; it does not claim to test an operating-system clock.
Earlier independent regressions remain in `tests/semantics.rs`, covering actor
ownership, versions, cross-run isolation, exact task correlation, terminal
conflicts, limits and counters after rejected messages.

Reproduce the focused suite:

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml --test assumptions
```

The gateway derives run and actor from a capability, rejects actor/run overrides,
meters attempts before parsing, compares fixed-length capabilities and returns
sanitised errors. It exposes no unfiltered administrative status command. The
reviewer inspected these paths and the bounded JSONL reader. The direct Store
API and `mas` CLI remain privileged host interfaces, not alternative untrusted
agent entry points.

## Final model evidence

The reviewer independently inspected [`evidence/model-checks.json`](../evidence/model-checks.json)
and selected negative-control logs. The final manifest reports forty-five cases
and seventy-three checks: sixty complete positive checks, eleven deliberate-defect
counterexamples and two assumption counterexamples. All counterexamples replayed
successfully. All twenty-nine normal sampled simulations completed.

The recorded runner hash matches the reviewed source:

`f2373dba5e462521f07906de0e8163f793a65674626c456b9f2cf561f8c2aaa0`.

The new negative controls detect partial commit, duplicate insertion after a lost
response, and expiry overwriting an earlier terminal decision. The assumption
counterexamples retain indefinite non-acknowledgement under unlimited faults,
including a weakly fair process schedule. A successful expected-counterexample
case means the claimed unconditional progress guarantee was disproved.

Bounded transport progress checks no longer require process fairness. Their
finite state-changing actions prevent indefinitely choosing another enabled
normal operation; this is a property of that model, not a guarantee that a real
worker calls receive or acknowledgement. Separate safety checks permit unlimited
redelivery, restart and lost-acknowledgement transitions. The two retained message
identities and other declared finite domains remain verification bounds.

The transaction model permits aborted invocations and lost commit responses while
assuming atomic committed storage. It does not prove SQLite or filesystem crash
recovery. The expiry model checks immutable closure and subsequent delivery
annotations; it makes no real-time or host-availability guarantee. Stipulated
rejection observations, implementation tests and model correspondences are not
a formal refinement proof for every Rust/SQLite execution.

## Remaining operational conditions

Capabilities reduce reliance on caller-supplied identity but retain trust in
credential provisioning, private host storage and response routing. Permission
checks do not isolate mutually untrusted agents sharing the host's operating-system
account. Blocking stdio has no read-timeout guarantee, and malicious requests can
exhaust a shared global attempt allowance. Administrative reconciliation remains
available independently of agent admission limits.

Expiry records `expired_outcome_unknown`; cancellation records protocol closure.
Neither establishes that external work stopped or that an effect did not occur.
An actionable delivery is a snapshot, not a lease or revocable authorisation.
Host activity, a usable clock and functioning storage are needed to observe and
record expiry. Human-review fixtures obtain no human approval, and voting
agreement establishes no message truth.

No unresolved material implementation or model defect was found within this
review's scope. The retained tests and model evidence support the bounded claims
above, with these operational conditions explicit.

## Initial Python host integration review

The independent reviewer inspected the reusable host, provider adapters, mesh
demo and deployment configuration. Offline integration tests exercised the real
Rust binaries. The optional SDK contract test uses a simulated HTTP transport;
no real model request or paid invocation was part of this review. This is an
implementation review, not an extension of the SPIN verification claims.

Independent fault injections discarded responses after committed reservation,
call-evidence recording, Result insertion and Request acknowledgement. Provider
callbacks ran zero, one, one and one times respectively; none was retried.
A lost Result response left a completed durable task with unacknowledged messages.
The resulting diagnostic was corrected to describe an unconfirmed response,
rather than assert that the terminal write had failed. The corrected demo report
distinguishes an unknown host exchange outcome from the authoritative task state;
both response-loss regressions passed independent execution. A durable Failure
does not establish that a provider had no effect.

Review findings also prompted strict duplicate-key rejection for JSON file inputs
and sanitisation of invalid Unicode provider output. The reviewer confirmed that
the peer model receives the delivered proposal as untrusted data. The exact
Python checker validates this invoice calculation; it supplies no general model
quality or consensus guarantee.

Adapters remain trusted Python code. Execution is synchronous and nested, with
no automatic restart, redispatch or provider replay. Credentials are omitted from
Rust child environments, while host-owned private state retains capabilities and
application data. The mesh is a logical routing graph over one durable broker,
not a distributed transport or an isolation boundary between adapters.

Docker was unavailable in the review environment. Container build, mock execution
and dummy-secret readability checks are delegated to the Compose CI workflow;
their success must be assessed from its actual run. Source inspection found that
Compose environment-backed secret injection requires the live override to allow
a writable root filesystem. Mock mode retains a read-only root filesystem; both
modes retain the non-root account, dropped capabilities and protected code paths.

## Deployment hardening review

The subsequent review covered the recovery journal, supervised adapter process,
deployment admission ledger, backup and retention interfaces, and their coupling
to the Rust gateway. The earlier host limitation on all recovery is superseded
by explicit, compatible checkpoint replay. This review does not establish a
general production certification or extend the transport model to arbitrary
Python or provider executions.

The reviewer reproduced a failure in which supervised invocation uncertainty
incorrectly committed a terminal Failure, preventing a later evidenced resolution
from being used. The correction preserves an open recoverable workflow while
recording the unknown invocation. Separate review identified that copying a live
spending ledger into a restored deployment could duplicate spending authority or
rewind post-snapshot charges. Restores now remain audit-only: they permit
inspection and replay of already committed terminal results, but no incomplete
workflow dispatch or new call admission. Quarantine precedes copying and the
verified restored tree is published atomically. A snapshot does not establish
whether external effects or charges occurred after it was taken.

Independent execution exercised recovery, operations and deployment fault tests,
including actual process termination, retained unknown invocations, completed
output replay without repeating effects, storage-write rejection, shared-budget
contention, backup restoration and retention. Supervision tests additionally
exercised parent death, handler timeout, process-group cleanup and malformed IPC.
These are bounded tests on the test host; they are not a capacity or availability
guarantee, and they made no paid model calls.

Recovery requires the original explicit workflow version and original request
order. Invocation inputs and operation order are compared with checkpoints;
adapters must still honour their declared version and route external effects
through the invocation interface. An operator's completed or confirmed-no-effect
resolution is a trusted, evidence-backed assertion. Its truth about a remote
system is not established by the protocol. A confirmed-no-effect resolution
authorises a fresh metered attempt; it does not erase the original attempt.
Creating a fresh business operation is new authorisation, not global duplicate
suppression across deployments.

The deployment ledger retains charges across run pruning and rejects new work
when its configured admission limits are reached. Reserved monetary allocations
are conservative admission units, not measured invoices; provider-account limits
and billing reconciliation remain external. Supervision bounds local execution
and retains uncertainty after interruption, but cannot prove cancellation of a
remote effect. Adapters remain trusted Python code, not sandboxed tenants.

The final host-recovery model search completed with 466 stored states and no
errors. Four deliberate changes each produced a replayed assertion violation:
dispatch without a charge, redispatch of an uncertain attempt, acceptance of an
old owner's response, and replacement of the first recorded result. The reviewer
checked these logs and traces against their recorded hashes and confirmed the
model source hash
`9c0550d66ad0600333664a32cfaef70042a68cf34d537aaa949f29f4ac92a823`.
The model covers one checkpoint, two attempts and at most one crash. It abstracts
exclusive ownership and committed storage; it does not prove Python refinement,
filesystem durability, truthful operator evidence or provider cancellation.

The deployment load exercise exposed a transient filesystem-enumeration race:
SQLite journal files could disappear between inspection calls. The reviewed
correction uses one non-following stat result and tolerates only a missing entry,
while retaining rejection of symlinks and special files. Size remains an
admission-time observation, not a strict filesystem quota.

The native dependency review confirmed that the previous SQLite amalgamation
was version 3.46.0. The independently tested upgrade to rusqlite 0.40.2 and
libsqlite3-sys 0.38.2 supplies patched SQLite 3.53.2 and compiles with the pinned
Rust 1.85.1 toolchain without application changes. All forty-five native tests
passed in the isolated serial probe, followed by the final parallel suite after
integration. This closes the identified outdated bundled-library gap; a clean
Cargo audit alone is not evidence that embedded C dependencies are current.

The parallel probe also exposed a verifier test-fixture naming race. Temporary
directory names now include a process-local atomic counter, and exclusive
directory creation rejects a collision instead of sharing a fixture. The final
formal rerun completed all forty-five cases and seventy-three checks. The
reviewer rechecked the final verifier source hash and the host model's source,
check logs, replay logs and counterexample trail hashes against that manifest.

## Readiness recheck and repairs

A later review challenged the five deployment areas and adapter substitutability.
It reproduced ordinary descendants surviving owner SIGKILL and retaining a run
lock, different catchable-error behaviour between inline and supervised adapters,
acceptance of future host schemas, unrecoverable bootstrap interruption, false
successful watcher reports and deletion of live acceptance state. These findings
supersede the earlier completion assessment for the affected host paths.

The repairs introduce a process-group guardian that retains ownership until
cleanup, typed context/error contracts, version-aware repositories, staged
bootstrap recovery, independent reconciler readiness and persistent live-check
state. Custom provider descriptors and allocations are explicit contract members.
The Rust gateway now queries bounded membership metadata once per run instead
of decoding full retained history for every credential.

Independent follow-up testing found and closed two additional admission-window
failures: interruption after recording execution but before the supervisor
received its ticket, and a handler catching that error then admitting another
effect. Both probes now leave the task open with one uncertain invocation and
prevent further execution. The reviewer reran sixty focused bootstrap, schema,
supervision, readiness and recovery tests successfully and found no unresolved
material defect in those reviewed paths.

The original deadline is established when Rust first commits the run and is
preserved thereafter. Bootstrap stages before that commit have no earlier Rust
deadline to restore. Complete legacy runs migrate through recognised layouts;
legacy partial runs without original workflow metadata remain unresolved. The
guardian contract requires trusted adapters to remain in their process group
and functioning Linux/kernel/storage mechanisms. This review establishes the
tested boundaries, not a formal implementation refinement or universal
production certification. Live and tagged-release execution remain separate
acceptance records.

The subsequent arm64 container CI run exposed a timing-dependent error in a crash-test
helper. It attempted recovery after observing adapter death, while the guardian
could still retain the ownership lock. An independent deterministic probe held
the guardian after reaping the adapter and confirmed that recovery correctly
rejected entry until the guardian exited. The helper now waits for that exact
lock boundary and retries only lock contention; it does not retry general
runner errors. A regression preserves the premature-recovery rejection and
then verifies one resumed invocation without a duplicate effect. No runtime
locking behaviour was weakened. The complete local host suite now passes 132
tests; the new candidate still requires fresh architecture and release gates.
