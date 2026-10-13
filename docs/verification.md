# Verification results

Recorded on 13 October 2026. **45 Rust tests passed; 59 positive SPIN checks
completed with zero errors; seven deliberate defects and two assumption
counterexamples were detected and replayed; 26 sampled simulations completed.**
All twelve workflow fixtures, the deadline-recovery example and the authenticated
gateway example executed successfully.

These results establish the checked finite-model properties and tested runtime
behaviours. They do not establish unrestricted implementation correctness,
mathematical truth, live AI-agent performance or remote cryptographic security.

## Evidence and reproduction

| Evidence | Contents |
|---|---|
| [`model-checks.json`](../evidence/model-checks.json) | Forty cases, sixty-eight checks; commands, results, hashes, bounds and tool versions. |
| [`model-checks.tar.gz`](../evidence/model-checks.tar.gz) | Generated model instances, raw logs, simulations and replayable counterexamples. |
| [`initial-incomplete-model.tar.gz`](../evidence/initial-incomplete-model.tar.gz) | Retained initial memory-limited searches; classified as incomplete. |
| [`rust-tests.log`](../evidence/rust-tests.log) | Parser, storage, workflow, CLI, gateway, independent and verifier regression results. |
| [`examples/`](../evidence/examples/) | Fourteen executed Rust/SQLite examples, including expiry and gateway restart. |
| [`readme-exchange.log`](../evidence/readme-exchange.log) | Execution of the administrative request/result sequence. |
| [`source-manifest.json`](../evidence/source-manifest.json) | SHA-256 identities for delivered source, configuration and documentation. |

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml
cargo build --release --locked --manifest-path rust-protocol/Cargo.toml
./rust-protocol/target/release/verify
make examples
```

The verifier needs Linux, SPIN, a C compiler, `sh` and `sha256sum`. Recorded tools:
Rust/Cargo 1.85.1, SPIN 6.5.2 and GCC 13.3.0. Dependencies are pinned in Cargo.lock.
Runtime, gateway, examples and verification orchestration are Rust; Promela and
SPIN-generated C provide the formal checks. The evidence's runner hash matches
the delivered source.

## Checked scope

| Cases | Checks | Result |
|---|---:|---|
| Twelve bounded topology configurations | 36: safety and two per-message LTL properties per graph | Complete, zero errors; no fairness option. |
| Bounded no-reduction and capacity-one cases | 6 | Complete, zero errors; no fairness option. |
| Existing two-task lifecycle | 1 safety | Complete, zero errors. |
| Unlimited repeated faults: twelve graphs, capacity one and no reduction | 14 safety | Complete, zero errors. |
| Transaction rollback/crash/retry model | 1 safety | Complete, zero errors. |
| Task terminal/expiry race model | 1 safety | Complete, zero errors. |
| Early ack, forbidden route, missing ack, competing terminals, partial commit, duplicate insertion, expiry overwrite | 7 deliberate defects | Expected violations detected and replayed. |
| Unlimited faults, with and without weak process fairness | 2 assumption counterexamples | Eventual acknowledgement fails; cycles replayed. |

A counterexample expectation requires the appropriate violation type and a
successful trail replay. Resource exhaustion cannot satisfy it. No bitstate or
hash-compact approximate search was used. Every checker has a 30-second,
512-MiB command budget and depth limit 100,000; exceeding a limit prevents a
successful classification. Exact state counts, transitions and unreachable
statements are retained in the raw logs.

Transport models retain two message identities, two to four participants for the
supplied graphs, and the configured capacities. Routes range over every declared
edge. Positive bounded progress allows one restart, lost acknowledgement invocation
and redelivery per message. Every such action is bounded, so those models cannot
sustain a non-progress loop by selecting faults forever; weak process fairness
is unnecessary. This does not prove that a deployed worker eventually runs.

Separate safety variants remove the repetition limits on those faults. They
check routing, retained identity/content, receipt before acknowledgement and
capacity. Repeated non-destructive faults preserve these invariants in the finite
model. With unlimited faults, the liveness counterexample can keep redelivering
one message without acknowledging it, even under weak process fairness.

The transaction model admits rollback and crashes at pre-commit stages, atomic
commit, lost commit responses and identical retries. It verifies the protocol
against SQLite's atomic recovery contract; it does not prove that contract for
SQLite, the filesystem or hardware. Runtime tests inject insertion failures and
check rollback, reopen and deduplication. They do not simulate physical power loss.

The expiry model checks that runtime closure cannot replace an earlier terminal
outcome and cannot be undone by clock-condition rollback. It permits receipt and
acknowledgement of closed work, with a non-actionable delivery annotation. Expiry
is an explicit host observation, not a timed-automata clock: no deadline-in-seconds
or eventual-host-activity theorem is claimed. Earlier dispatched effects may
still occur. Message truth, exactly-once effects and eventual successful work
remain separate obligations.

Gateway authentication, frame limits and durable global/credential attempt caps
are exercised by implementation tests and the subprocess example. They are not
cryptographic or admission-protocol proofs from the Promela models.

## Implementation correspondence and review

The 45 tests include 25 library checks, three verifier checks, three CLI checks,
seven gateway checks, four independent assumption regressions and three earlier
independent semantic regressions. They cover the actual codec/SQLite interfaces,
including concurrent admission, terminal races, expired-run latching, full
mailboxes, exhausted budgets, migration, rollback and forged identities.

Native schema-1 history migrates transactionally to schema 2. Expiry and admission
accounting commit separately from a proposed operation, so rejected work cannot
undo them. Closed task requests remain drainable and carry `actionable: false`;
that annotation is a snapshot, not an execution lease. The recovery example
observes an actual one-second deadline after database reopen; deterministic tests
inject the due condition directly to avoid scheduling-dependent assertions.

The independent reviewer identified and verified fixes for the stored-envelope
migration limit and admission timestamp, and checked final model counts, hashes
and counterexample replays. See [independent review](independent-review.md) and
[model contract](../formal/README.md). No formal refinement proof relates every
Rust execution to Promela.

The original model exhausted memory by distinguishing diagnostic markers for
state-preserving rejections. The current abstraction treats those observations
as stuttering while retaining relevant state changes; initial incomplete evidence
remains available. Earlier successful revisions remain in Git history rather
than being presented as current verification results.
