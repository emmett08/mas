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
and selected negative-control logs. The manifest reports forty cases and
sixty-eight checks: fifty-nine complete positive checks, seven deliberate-defect
counterexamples and two assumption counterexamples. All counterexamples replayed
successfully. All twenty-six normal simulations completed with both message
identities accepted and acknowledged.

The recorded runner hash matches the reviewed source:

`3ab5b43f223490b31d04c6778c763948342ba61b3f703729124b91486160c943`.

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
