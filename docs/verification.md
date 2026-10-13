# Verification results

Recorded on 13 October 2026. **25 Rust tests passed; 43 positive SPIN checks
completed with zero errors; four deliberate defects produced replayed
counterexamples; 26 sampled simulations completed.** All twelve deterministic
workflow examples and the README's persistent exchange executed successfully.

These are finite-model and implementation-test results. No live LLM MAS,
mathematical proof-content validation, performance advantage or unrestricted
network guarantee is claimed.

## Evidence and reproduction

| Evidence | Contents |
|---|---|
| [`model-checks.json`](../evidence/model-checks.json) | Nineteen cases; every command, result, source/log hash, bound and tool version. |
| [`model-checks.tar.gz`](../evidence/model-checks.tar.gz) | Final generated Promela instances, raw logs, simulations and replayable counterexample trails. |
| [`initial-incomplete-model.tar.gz`](../evidence/initial-incomplete-model.tar.gz) | Initial memory-limited runs; retained as incomplete evidence, not successful checks. |
| [`rust-tests.log`](../evidence/rust-tests.log) | Native parser, storage, workflow, CLI, independent semantic and verifier regression results. |
| [`examples/`](../evidence/examples/) | Twelve actual Rust/SQLite fixture transcripts. |
| [`readme-exchange.log`](../evidence/readme-exchange.log) | Execution of the documented persistent request/result sequence. |
| [`source-manifest.json`](../evidence/source-manifest.json) | SHA-256 identities for the delivered source, configuration and documentation. |

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml
cargo build --release --locked --manifest-path rust-protocol/Cargo.toml
./rust-protocol/target/release/verify
make examples
```

The verifier needs Linux, SPIN, a C compiler, `sh` and `sha256sum`. Tool versions
recorded here: Rust/Cargo 1.85.1, SPIN 6.5.2, GCC 13.3.0. Rust dependencies are
pinned in Cargo.lock; nom is 8.0.0 and rusqlite is 0.32.1 with bundled SQLite.
The Rust runner, rather than the superseded prototype runner, produced the final
evidence. Its recorded hash matches the delivered source.

## Checked scope

| Cases | Checks | Result |
|---|---:|---|
| Twelve topology configurations | 36: safety plus two individually selected LTL properties per graph | Completed, zero errors. |
| Evaluator graph without partial-order reduction | 3 | Completed, zero errors. |
| Star with capacity one | 3 | Completed, zero errors. |
| Two-task lifecycle | 1 safety check | Completed, zero errors. |
| Early ack, forbidden route, permanently missing ack, competing terminal outcomes | 4 negative controls | Expected counterexamples detected and replayed. |

Normal safety searches stored between 3,089 and 83,089 states across the twelve
graph configurations. The capacity-one case stored 33,415 states; the task model
stored 1,589. Exact LTL state counts, transitions, unreached states and compiler
output remain in the raw logs. No bitstate or hash-compact approximate search
was used. Each checker had a 30-second, 512-MiB command budget and depth limit
100,000; an exhausted budget prevents a successful classification.

Transport bounds are two distinct retained identities, two to four participants
for the supplied examples, each configured mailbox capacity, and at most one
modelled restart, lost acknowledgement invocation and redelivery per identity.
The environment chooses routes from all declared edges. Safety covers identity
retention, receipt before acknowledgement, routing and capacity within the
abstract state space. Progress is checked separately for each accepted message
under weak process fairness and bounded faults. Permanent non-response remains
a counterexample to progress.

The task model examines two request identities with Result/Failure/Cancel races.
It checks terminal-state safety. Correlation rejection is also exercised against
the Rust implementation. Task completion, external-effect reversal and truth of
results are not temporal guarantees of this model.

The initial model's independent bookkeeping markers for state-preserving
rejections multiplied equivalent service states until larger searches exhausted
memory. The final model abstracts those calls as stuttering observations.
It retains route choices and state-changing receive/ack/fault interleavings.
Runtime tests supply rejection evidence; no proof of refinement from every
Rust/SQLite execution to Promela is asserted.

## Independent review

The separate reviewer found and helped repair malformed topology fallbacks,
nested reply-identity reuse, stale success summaries after interrupted checks
and model-generator schema inconsistencies. The reviewer added public-interface
regressions and inspected the model boundaries. See
[`independent-review.md`](independent-review.md).

Rust is the sole shipped implementation language for runtime and automation.
Promela and SPIN-generated C remain verification tools. The example participants
are deterministic fixtures; their successful exchanges establish communication
behaviour rather than the quality or independence of AI reasoning.
