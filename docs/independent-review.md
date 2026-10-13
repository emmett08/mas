# Independent implementation and model review

Review date: 13 October 2026. Scope: native Rust message transport, task transitions,
CLI admission, deterministic examples, Promela abstractions and verification evidence.
The reviewer used a separate agent context from the implementation authors and
sent findings back to those authors. This is an implementation peer review, not
evidence that the example application executed a live LLM MAS.

## Findings and repairs

| Finding | Disposition |
| --- | --- |
| An explicit malformed topology could omit routing restrictions and task validation. | Native CLI and example entry points now require the explicit topology fields and reject malformed configuration. CLI and example regressions cover these failures. |
| Nested hierarchical requests could derive the parent's reply identity from a sequence counter advanced by child requests. | Replies derive their identities and correlations from the captured request. The tree regression checks the final parent reply. |
| A failed or interrupted verifier rerun could leave an earlier successful summary beside partly replaced evidence. | The runner writes `RUNNING_OR_INTERRUPTED` before replacing evidence; a successful summary is written only after checks finish. |
| The finite-model generator admitted participant/capacity configurations rejected by the runtime. | The generator now uses strict JSON parsing and checks unique valid identities, distinct edge endpoints and explicit integer capacity within 1–100. |
| Early model claims could imply independent exploration of duplicate, collision and forbidden-route calls. | The formal documentation distinguishes stipulated stuttering observations, runtime rejection tests and deliberate mutation checks. |

The Python prototype was superseded during the work. Its earlier test outcomes
are excluded from the native Rust acceptance claim.

## Independently exercised behaviours

The reviewer added and ran `rust-protocol/tests/semantics.rs`; all three tests
passed. These public-interface regressions cover actor spoofing, wrong-recipient
and premature acknowledgements, incompatible versions, independent run identities,
changed contents after acknowledgement and reopening, exact reply correlation,
cross-run task references, wrong task participants on an otherwise permitted route,
reused task identities, invalid result fields, second terminal outcomes, byte and
call limits, and call-owner enforcement. Rejected sends must preserve the recorded
message, byte and call counters.

An additional transient CLI exercise checked thirteen rejected task transitions;
all produced structured failures without spending communication budgets. The
retained Rust tests provide the reproducible coverage rather than depending on
that transient exercise. The reviewer also ran the malformed example configuration
regression after the repair; it passed.

Reproduce the independent regression suite:

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml --test semantics
```

The implementation serialises send admission and task validation inside one
SQLite write transaction. The reviewed race test separately covers competing
Result/Cancel outcomes and concurrent insertion of an identical message. Successful
acknowledgement releases mailbox capacity while retaining the message identity.
Database schema markers reject the earlier prototype's database; migration is
outside this release. Externally altered schemas or database corruption remain
outside the storage assumptions.

## Formal evidence and limits

The final native evidence in `formal/evidence-native-final/summary.json` contains
nineteen cases and forty-seven checks: forty-three complete positive checks and
four replayed counterexamples. It covers twelve topology instances, a run without
partial-order reduction, a capacity-one instance and task-lifecycle safety. All
twenty-six recorded normal simulations completed with both messages accepted and
acknowledged. The reviewer independently checked these counts, replay completion
and the runner source hash against the actual final files:

`0f22cdec9781cec597c26f623fb473689e8cd2277ca7c1b6ce43b9e6af42b561`.

The negative-control logs contain the expected early-acknowledgement assertion,
forbidden-route assertion, missing-acknowledgement acceptance cycle and repeated
terminal-state assertion. A negative-control `PASS` means a defect was detected.
The task mutation's final replay shows a Result followed by a competing Cancel
for the same task; each requester attempts cancellation at most once. The final
evidence was generated after the runner repairs and its runner hash matches the
reviewed source.

The transport model covers two retained identities. Progress depends on bounded
faults, successful atomic storage transactions and weak process fairness. Both
accepted-message antecedents and their acknowledgements occur in the recorded
normal simulations. The separate task model checks safety and makes no task
completion claim. Neither model supplies a proof that every Rust/SQLite execution
refines Promela.

Duplicate and changed-content rejection are stipulated state-preserving model
observations backed by implementation tests. The forbidden-route mutation detects
an invalid route at its assertion; baseline route selection already ranges over
allowed edges. Model checking therefore does not independently establish the
implementation's complete input-validation behaviour.

Cancellation becomes terminal only when accepted. Capacity or budget exhaustion
can prevent acceptance, and terminal state does not undo an external effect.
Human-review fixtures obtain no human approval. Voting agreement proves no message
truth. Parallel fixtures demonstrate outstanding requests and different completion
orders; they do not demonstrate concurrent model execution. These boundaries are
necessary qualifications of the delivered protocol and examples.

No unresolved native transport or task-transition defect was found within this
review's scope. Acceptance remains bounded by the recorded tests, models and
their stated assumptions.
