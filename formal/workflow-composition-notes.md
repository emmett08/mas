# Bounded workflow composition model

`workflow-composition.pml` explores two independent leaf invocations and an
all-completed join. One nondeterministic transition selector represents all
interleavings of their durable intent, shared allowance admission, dispatch and
result recording. The shared allowance is chosen as one or two calls. There
can be one scheduler crash, one cancellation request and one expiry event.
Each dispatched branch has one completion, failure or uncertain observation.

The checked safety properties are:

- Dispatch requires a persisted assignment and a consumed shared allowance.
- The sum of charges never exceeds the allowance; reservations are retained.
- Each leaf assignment dispatches at most once in this abstraction.
- Restart preserves assignment identity; uncertain work blocks fresh dispatch.
- Cancellation and expiry stop new admissions. A stop request does not establish
  that an already dispatched external effect was cancelled.
- Recorded terminal observations remain immutable.
- A successful join requires both branches to have completed successfully.

The LTL property `workflow_settles` establishes eventual settlement in this
finite model. Settlement includes failure, cancellation, expiry and uncertainty;
it does not establish successful completion. The assumption is that every
dispatched branch produces one of the modelled finite observations, and that
execution does not stop indefinitely. The model has no unbounded stuttering or
repeatable faults. Consequently this progress result requires no scheduling
fairness assumption. A deliberate indefinite-response mutation creates an
acceptance cycle, demonstrating why finite observation progress is necessary.

Seven isolated safety mutations remove admission, overspend the shared
allowance, bypass a stop, redispatch an uncertain effect, change a recovered
assignment, overwrite a terminal result, or join after only one success. Each
must fail its named assertion and have its counterexample replayed. Positive
safety and progress are also checked with partial-order reduction disabled.
A separate reachability witness establishes that both branches can complete
and the join can succeed; the success implication is therefore not vacuous.

## Implementation correspondence and limits

| Model transition | Implementation responsibility |
| --- | --- |
| Persist intent and assignment | `WorkflowEngine._attempt`, the unique instance/path/revision key and retained run directory |
| Consume shared allowance | `WorkflowAllocation.admit_call`, serialised SQLite accounting before underlying admission |
| Dispatch | Scheduler worker start followed by the existing Runner admission/journal boundary |
| Resume an assignment | Restart retains the run directory and request identity; Runner replays the existing journal |
| Record uncertainty | Lost worker or interrupted invocation becomes `unknown`, preventing new dispatch |
| Join | The parallel node completes only when all named branches complete |

The abstract dispatch combines effect admission and start into one transition;
it is not an operating-system process fork. The implementation checks stopping
at admission and supervision boundaries. An already-admitted callback can race
a cancellation request and may start after that request's timestamp; its outcome
still needs observation or reconciliation. Recreating a worker to inspect or
replay the same retained journal does not imply repeating an external effect.
Existing host recovery models and tests cover that lower boundary separately.

The model does not encode Python, SQLite failure modes, process supervision,
authentication, provider billing, artefact storage, arbitrary graph sizes,
subworkflow scope budgets, or physical wall-clock time. Expiry is an explicit
environment event. Atomic durable transitions and correct lower-level journal
replay are assumptions supported by separate implementation tests. This is a
bounded design check, not a refinement proof or a universal correctness claim.
The model stops at unresolved uncertainty; later evidence-backed operator
resolution belongs to the existing recovery contract and is outside this model.

Implementation tests in `host/tests/test_workflow_engine_contracts.py` exercise real
SQLite records and process execution. The plan tests independently establish
acyclic references, conservative expansion limits, immutable transitive
bindings and typed condition semantics. Agreement on these finite examples
does not establish unrestricted equivalence with the model.
