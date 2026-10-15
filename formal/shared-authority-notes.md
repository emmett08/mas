# Shared authority, hierarchical allocation and replacement recovery

The two models in this contract extend the existing transport, invocation and
composition checks. They answer bounded questions about competing authority,
allocation retention and recovery. They do not establish availability, SQL
correctness, provider correctness or unrestricted implementation refinement.

## Abstraction contract

| Contract element | Declared meaning |
| --- | --- |
| Concrete boundary | Native PostgreSQL authority commands, worker assignment/dispatch/receipt transitions, and sealed-archive recovery operations. |
| Abstract state | Controller binding; worker boot and assignment fence; two stable logical call identities; admission, dispatch and receipt records; cumulative scope charges; source/snapshot/replacement state. |
| Relation | Selected implementation observations map exactly to the shared JSON corpus. Other model transitions have documented implementation counterparts and independent tests, without a proved trace-inclusion relation. |
| Intended uses | Detect stale fresh admission/dispatch, competing ownership, overspending an active scope limit, lost late evidence, repeated uncertain effects, historical refunds and premature restore activation. |
| Satisfaction | Exhaustive safety over each finite model/profile. Named counterexamples validate mutation detection; named witnesses establish reachable scenarios. |
| Assumptions | Atomic authority transactions, authenticated worker identity, valid/correlated receipt contents, truthful external fencing evidence, durable committed records and verified archive/object content. |
| Exclusions | Arbitrary populations, unbounded histories, numeric overflow, database replication, filesystem or object-store failure semantics, cryptographic authentication, scheduler fairness, physical time, provider progress and production recovery objectives. |

The atomic-command assumption has a concrete implementation basis:
`PostgresRepository::transaction` locks the authority singleton, checks the
controller binding, backup mode and lease, executes the command, then commits.
The model deliberately exposes separate admission, worker claim, execution
grant and receipt commands. It does not combine external provider execution
with a database transaction. The separate recovery model assumes the same
linearisation boundary; checking both models does not itself prove their
composition with the Python, Rust, database or external service implementations.

The models use one selector for all enabled authority commands. This represents
their interleavings at the serialised transaction boundary, including requests
from two controllers and two workers. Running with partial-order reduction
disabled checks the same finite safety obligations; it does not create an
additional SQL concurrency experiment.

## Shared command and allocation model

`shared-authority.pml` has two controller bindings, two workers, two logical
calls, two principal values, two provider values, one common ancestor and two
child-ancestor values. Every call consumes one call and reservation unit. A
worker can reboot once; each call has at most one lease-expiry observation.
Each claim increments its assignment fence. The resulting finite scenario has
at most four assignments per call; candidate request fences range from one to
four. A controller can hand over once, while draining. Drain and resume,
duplicate commands and rejected requests may repeat indefinitely.

The default profile shares principal, provider and child scopes. A separate
profile assigns the second call to different principal, provider and child
scopes. Five further profiles independently lower the deployment, principal,
provider, common-ancestor or child-ancestor ceiling from two units to one,
leaving the other ceilings at two. Thus another limit cannot mask a missing
guard in that dimension. Root and child limits are both contested by distinct
logical calls; a child counter is not merely another name for one call's ID.

| Assertion | Requirement and concrete counterpart |
| --- | --- |
| `AUTH_CURRENT_ADMISSION`, `AUTH_CURRENT_DISPATCH` | A replaced controller cannot grant new work; the native authority validates epoch and controller before command execution or receipt replay. |
| `AUTH_WORKER_FENCED` | A fresh execution grant must match the assigned worker boot and assignment fence. |
| `AUTH_ONE_CLAIM`, `AUTH_WORKER_CAPACITY` | One current claim per assignment and at most one held assignment per modelled worker. |
| `AUTH_NO_REDISPATCH`, `AUTH_ADMITTED_DISPATCH` | Each logical call receives at most one external execution grant, after retained admission. |
| `AUTH_DEPLOYMENT_LIMIT`, `AUTH_PRINCIPAL_LIMIT`, `AUTH_PROVIDER_LIMIT` | The corresponding unit ceilings remain effective under competing admissions. |
| `AUTH_ANCESTOR_LIMIT`, `AUTH_CHILD_ANCESTOR_LIMIT` | Common-parent and child ceilings both apply. |
| `AUTH_SCOPE_ACCOUNTING`, `AUTH_CHARGES_RETAINED` | Scope totals agree with retained admissions; archival cannot refund them. |
| `AUTH_RECEIPT_RETAINED`, `AUTH_CURRENT_COMPLETION` | A valid late receipt is retained; only a still-dispatched current assignment supplies authoritative completion. |
| `AUTH_UNKNOWN_RETAINED` | Archival cannot delete unresolved work. |

Admission precedes worker placement. A claim does not create spending authority.
Controller handover preserves a valid worker claim: the successor can accept
its receipt while the old controller is rejected. Worker reboot or lease expiry
is different. A claimed, undispatched assignment becomes available again; a
dispatched assignment becomes unknown and remains occupied. Its old receipt is
retained as evidence without establishing authoritative completion. In
particular, expiry can invalidate a receipt while its worker boot is unchanged.

Fifteen named defect variants remove those guards or corrupt their consequences.
Each must replay its designated assertion. Five separate witnesses establish
two simultaneous dispatched workers, rejection of the replaced controller,
late evidence after fencing, completion of a pre-handover worker claim through
the successor, and non-authoritative receipt retention after same-boot expiry.
These witnesses prevent a passing safety implication from hiding an unreachable
antecedent; they are not additional positive safety proofs.

No liveness property is claimed. Commands can stop, faults can leave an effect
unknown, or callers can repeat a rejected operation forever. Lease expiry is an
explicit observation; no duration or eventual clock/reconciler invocation is
proved. The two-level ancestor tree is a bounded hierarchy check, not a theorem
for arbitrary depth or arbitrary money/token magnitudes. Generated arithmetic
and implementation tests cover separate numeric and backend obligations.

## Source, snapshot and replacement model

`authority-recovery.pml` contains one source authority, one snapshot, one audit
restore, one replacement and two logical calls. The history counters represent
the preserved reference history against which restoration is assessed; they
are not assumed to be magically available inside an incomplete restored
database. The target can inspect such a snapshot while admission remains shut.

The model distinguishes:

1. Sealing the source against new application mutations.
2. Capturing a final archive after that seal, including original invocation and
   execution-grant identities and cumulative charges.
3. Restoring an admission-closed audit copy.
4. Receiving truthful independent evidence that the source authority is fenced.
5. Activating the replacement only from the final archive with that evidence.

`target_open` represents completed activation followed by authorised reopening
of admission. The implementation initially leaves the activated successor
draining and requires a separate explicit undrain command. The model collapses
those two administrative steps for admission safety; it does not assert that
they are one implementation transaction or that activation alone starts work.

An earlier snapshot can omit a later admission or execution grant. Even if the
source is subsequently fenced, that old snapshot remains audit-only. This is
the implemented final-archive profile; the model does not invent a general
point-in-time roll-forward or a conservative allocation-reissue mechanism.

| Assertion | Obligation |
| --- | --- |
| `RECOVERY_SEALED_SOURCE` | Sealing closes fresh source admission. |
| `RECOVERY_ACTIVATION_GATE`, `RECOVERY_SOURCE_FENCED`, `RECOVERY_FINAL_SNAPSHOT` | Audit restoration cannot reopen admission; activation requires both source fencing and a final archive. |
| `RECOVERY_ADMISSION_HISTORY`, `RECOVERY_CHARGES_RETAINED` | Active replacement preserves every admitted identity and its cumulative charge. |
| `RECOVERY_UNKNOWN_RETAINED`, `RECOVERY_NO_REDISPATCH` | Earlier execution grants survive as outstanding obligations; replacement cannot issue them again. |
| `RECOVERY_IDENTITY_UNIQUE` | Recovery and subsequent admission cannot recreate an admitted logical effect identity. |
| `RECOVERY_CORRELATED_EVIDENCE` | Retained output evidence refers to an actual modelled observation. |

Already granted external work may finish after the source is sealed or the
replacement activates. A witness requires such an observation after replacement
activation and its retention under the original grant. Other witnesses require
successful activation with retained spending and a stale snapshot remaining
closed after later spending. Seven mutations respectively reopen the sealed
source, omit external fencing, activate a non-final snapshot, open admission
during restore, discard an outstanding grant, duplicate an admission or refund
archived spending. Every mutation has its own named assertion oracle.

The model assumes the truth of the fencing evidence and the integrity and
completeness of the final archive. Signature validation, manifest/object hashes,
PostgreSQL durability and full-state backup/restore are separate implementation
and fault-qualification obligations. The model does not prove zero-loss
replication or a recovery-time objective.

## Executable implementation correspondence

`authority-traces.json` supplies seven selected traces with 59 action
observations. `rust-protocol/src/bin/verify/authority.rs` renders these through
the *same transition inlines* as the exhaustive command model. Every step checks
accepted/rejected status, the named rejection when relevant, controller and
epoch, drain state, cumulative charges, task states, retained evidence count
and the receipt's authoritative flag.

`host/tests/test_shared_authority_traces.py` runs those actions through actual
`SharedAuthority` and `SharedWorkerController` commands and the native PostgreSQL
repository. It does not substitute a controlled executor for the authority.
The fixed workload checks deployment, principal, root and child scope charges
in both call and reservation units. It does **not** establish concrete provider
quota correspondence; the provider dimension is checked in the finite model
and requires its separately supported implementation profile.

The corpus `admit` action prepares a run and invocation, performs native
`admit_invocation`, then queues the admitted worker job. The native admission
transaction commits all applicable host-scope charges, shared accounting and
Rust protocol admission together. Preparation and worker-job submission are
separate transactions. The selected corpus compares the completed macro's
observations; it does not explore a crash or handover between those subcommands.
An admitted invocation lacking a worker job remains charged. The existing
invocation/worker recovery tests supply evidence for that intermediate state;
this model does not silently turn the whole macro into one concrete transaction.

The traces cover competing claims, two independently occupied workers,
drain/resume, controller handover preserving dispatched obligations, worker
reboot with late evidence, exact dispatch/receipt replay and stale fresh dispatch
while the successor is undrained. The last case reaches the epoch guard before
any execution grant, so replay suppression cannot explain the rejection.
The native adapter requires the specific denial category; a transport failure,
malformed response or unrelated exception cannot satisfy a negative step.

`SharedWorkerController.dispatch` assigns a deterministic command identity.
Replaying it can return the retained grant receipt, which the facade converts
to `execute=false`, including after completion or worker reboot. A fresh native
command with an old boot/fence is a separate rejected operation. Corpus replay
steps test the former and do not conflate its reason with the latter.

The native claim operation selects eligible work using its scheduling policy;
the adapter asserts that it selected the exact task named in the corpus. The
model can select other eligible assignments, so these traces establish selected
observation correspondence, not equality of all model and implementation traces.
Admission replay, lease-clock expiry, arbitrary scope configurations, archive
and restore are not covered by this particular shared corpus. The exhaustive
models and dedicated implementation tests cover their separately stated scopes.

## Reproduction and evidence

The normal `verify` binary includes both models, all profiles and negative
controls, and the shared corpus. A focused check is:

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml --bin verify \
  shared_authority_models_mutations_and_traces -- --ignored --nocapture
```

The normal runner records full-state search logs, exact preprocessor profiles,
source and module hashes, commands, tool versions, counterexample trails and
their replays. The existing limits remain 30 seconds per command, 512 MiB and
100,000 search depth; exhaustion is a failure. It also checks the positive
models with reduction disabled. The corpus must be non-empty with unique safe
trace names, and every trace must contain action observations.

Changing a transaction boundary, controller/worker fence, quota hierarchy,
archive activation rule, invocation identity or receipt acceptance rule requires
updating the corresponding transition, mutation, trace or implementation test.
Passing retained evidence only qualifies the model and source hashes recorded
in that run. An absent PostgreSQL fixture is an unexecuted correspondence gate.
