# Host evolution model contract

`host-evolution.pml` checks a deliberately small transition system: one logical
request key, two definition identities, two invocation attempts, two capacity
slots and one worker fence change. Commits are atomic. Authentication and truthful
pre-dispatch evidence are assumptions supplied by the implementation boundary.
The model does not implement a provider, filesystem, Linux guardian or database.

The verifier runs complete safety searches with and without partial-order
reduction. Seven deliberate defects must produce their specific named assertion,
with a replayed counterexample: replacing pinned code with the current alias,
forgetting an archived key, refunding consumed allocation, discarding a late
receipt, accepting a stale state write, duplicating slot ownership and archiving
an unknown effect. An unrelated failure does not satisfy a mutation gate.

| Model property | Implementation boundary and corresponding tests |
| --- | --- |
| Original key retained | Task/composition archival and request digests; `test_task_history.py`, `test_retention_budgets.py` |
| Charges survive archival/amendment | Deployment ledger and audited absolute increases; `test_retention_budgets.py` |
| Execution pins a version | Definition registry and availability resolution; `test_definition_versions.py` |
| One owner per slot | `scheduler_capacity.py`, process-lifetime locks and guardian tests |
| Unknown effects retain evidence | Invocation journals, archival reference checks and distributed controller |
| Late evidence survives fencing | Remote-worker receipt journal; distributed fault tests |
| Stale writes cannot become current | Remote-worker dispatch fences; distributed fault tests |

These mappings identify independently tested boundaries. They do not establish
trace refinement between the entire implementation and the model. Scheduler
fairness, unbounded histories, multi-machine failure, PostgreSQL replication,
provider effects and deployment availability are outside this finite model.

Run the normal Rust `verify` binary for the complete suite, or run the focused
SPIN-backed test with `cargo test --manifest-path rust-protocol/Cargo.toml --bin
verify host_evolution_models_and_mutations -- --ignored --nocapture`.
