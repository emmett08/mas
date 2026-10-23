# Native queue admission budgets

An optional `queue_admission` field in `mas-scheduler-policy/2` adds atomic
admission budgets to the existing native task authority. Existing policies and
count limits retain their meaning. Example field:

```json
{"format":"mas-queue-admission/1","max_accounting_records":4096,
 "global":{"max_retained_input_bytes":1048576,"max_pending_work_units":1000,
           "max_pending_estimated_cost_microusd":1000000},
 "principals":{"alice":{"max_retained_input_bytes":262144,
               "max_pending_work_units":100,
               "max_pending_estimated_cost_microusd":100000}}}
```

Every admitted workflow descriptor supplies immutable `admission_estimate` with
exact fields `work_units` (1..10^12), `estimated_cost_microusd` (0..10^12) and
`max_input_bytes` (1..65536). Callers cannot select a cheaper estimate. Actual
canonical UTF-8 input length must fit the registered bound. Both global and
principal byte/work/cost ceilings apply inside the original authority-locked
submission/update transaction. An unconfigured principal fails closed.

Retained input accounting counts application input copies in `service_tasks`
and every retained `service_executions` revision. It does not claim a physical
storage limit covering protocol messages, command receipts, artifacts, metadata
or backups; those retain their separate capacity/retention contracts. Completed
inputs keep consuming this budget while retained. Payload retirement releases
only its retired input copies; original request/effect identities remain intact.
Updating input accounts for the retained old execution input and both new copies.

Pending work/cost is an admission estimate, separate from native allocation and
actual cumulative cost accounting. An unresolved native invocation remains an
obligation even if a task envelope is reported completed or input-required.
Updates cannot hide such an obligation in an older revision. Multiple unresolved
legacy revisions have no single-task estimate in this first profile and prevent
activation/admission. Known settlement may release a pending estimate; it never
returns original native call or cost charges.

The bounded scalar scan visits at most `max_accounting_records+1` task records
and that many execution records, with a qualified combined cap of 1..16384. It
loads no application input/output into the controller, and transfers at most
1024 bytes per registered estimate. SQL still examines input length and existing
indexes; this is not a measured throughput improvement or constant-time admission
claim. Above-cap histories stop admission conservatively. Incremental accounting
and larger capacity claims require separate implementation and measurement.

The existing expected-digest/revision scheduler amendment changes these budgets.
Lowering them preserves existing tasks, retained inputs and unresolved charges;
new admissions block until they fit. The budget cannot be removed by relabelling
the scheduler policy. Initial activation refuses unknown legacy estimates and
unarchived composition work. New compositions are unsupported while this native
budget profile is active; their estimate/retention parity must be implemented
before activation. No new database table, scheduler or quota owner is added.

Provider deferral uses the same task execution. An exact blocked error
`{code:"native_provider_deferred",not_before_ms:<nonnegative i64>}` suppresses
`task.claim` and `task.claimable` until database time reaches the hint. The hint
is never execution permission: native admission rechecks the original deadline,
provider allowance and circuit. Long provider cooldowns remain retained; a
consumer may narrow only its scheduling wakeup to the original business expiry.
No revision, run or provider credit is created by becoming eligible.

Actual PostgreSQL tests independently count task/execution rows and input bytes,
race final global/principal credit, retain unknown obligations, reject misleading
task completion/update, preserve debt under policy lowering, account retained
revision inputs and reuse deferred execution identity. Ignored or merely compiled
tests are not backend acceptance. Sustained contention, cost-estimate adequacy,
provider bucket agreement and deployment capacity remain qualification work.
