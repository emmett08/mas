# Host recovery contract

`host-recovery.pml` checks one logical checkpoint with at most two attempts,
one crash, two ownership generations and two responses per attempt. Intent,
deployment charging, Rust reservation, dispatch and output recording are
separate transitions. Old replies remain pending across a new dispatch.

The safety assertions require a charge and reservation before dispatch, no
second dispatch of the same attempt, current ownership for accepted completion,
and preservation of the first recorded result. Unknown work may remain blocked
forever. A trusted operator can supply an established result or assert no effect
and authorise a new charged attempt. The model cannot establish that evidence.

Four isolated mutations skip charging, replay an unknown attempt, accept a stale
owner's completion, or overwrite a result. Each must produce an assertion
counterexample that the runner replays. Three seeded simulations illustrate
behaviour; they are samples, not coverage or progress proofs.

The generation check abstracts exclusive run locking and supervised IPC
ownership. The implementation does not use a distributed fencing-token service.
Durable local transitions depend on SQLite and the underlying filesystem. The
model does not verify Python, OS process cleanup, provider execution, billing,
clock deadlines or the truth of an operator's resolution. There is no formal
refinement proof from the implementation to this model and no liveness claim.

Implementation evidence comes from `host/tests/test_recovery.py`,
`test_supervision.py` and `test_deployment.py`: actual process death, lost replies,
stable checkpoint identities, unknown-call blocking, conflicting replay inputs,
resolved output reuse, shared admission and storage write failures.
