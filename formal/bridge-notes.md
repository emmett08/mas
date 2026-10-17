# Durable bridge boundary model

`bridge-service.pml` models one accepted request key belonging to one already
authenticated principal, the same-payload duplicate, a conflicting payload,
one admitted remote operation, one restart, two status observations and one
cancellation request. `bridge-events.pml` separately models two admitted tasks,
three retained events, two reads and one reader restart. These are finite
scenario bounds. Three events is not the implementation's event capacity; the
implementation bounds tasks, revisions and workflow messages, and retains
events with their task receipts.

The service model combines the abstract ingress identity and one outbound
operation to check their composition. It does not identify inbound task IDs
with external task IDs. The full implementation binds those identities in
separate repositories. Prepared intent, network dispatch, in-memory reply and
persisted receipt are separate transitions. Atomic blocks represent short
SQLite transactions; remote work can progress independently. Stopping with
queued, running or unknown work is permitted. No fairness, eventual completion
or eventual cancellation is assumed.

| Property | Required behaviour | Implementation correspondence |
| --- | --- | --- |
| `BRIDGE_KEY_DEDUPLICATED` | A principal's key retains one receipt | `DurableTaskService._duplicate`, `submit`, unique `requests` key |
| `BRIDGE_KEY_CONTENT_IMMUTABLE` | Reusing a key with different content cannot replace its accepted payload | Submission and continuation digests and conflict rejection |
| `BRIDGE_ADMISSION_BEFORE_EFFECT` | External dispatch follows admission | `RemoteTaskService.submit`, `_operate`, `RemoteTaskStore.verify_admission`, `context.invoke` |
| `BRIDGE_UNKNOWN_NOT_REPLAYED` | Losing a receipt or restarting cannot cause another initial dispatch | Committed `unknown` intent and explicit remote reconciliation |
| `BRIDGE_CANCEL_NOT_CONFIRMATION` | Cancellation intent cannot establish remote cancellation | Separate `cancel_requested`, `pending_action` and remote snapshot state |
| `BRIDGE_TERMINAL_IMMUTABLE` | A stale response cannot reopen a terminal receipt | `RemoteTaskService._snapshot` terminal guard |
| `BRIDGE_EVENTS_ATOMIC`, `BRIDGE_EVENT_COMMIT` | A task transition and its event commit together | `TaskStore.transaction`, `event` and service transition methods |
| `BRIDGE_INBOX_BOUND` | Admission rejects a task before exceeding the configured capacity | `DurableTaskService._capacity` inside the submission transaction |
| `BRIDGE_EVENT_COMMITTED` | A response never exposes an event that has not committed | `DurableTaskService.events` reads durable SQLite rows |
| `BRIDGE_CURSOR_OBSERVED`, `BRIDGE_CURSOR_REPLAY` | The modelled caller advances only after receiving a page, and can reread after restart | Caller-owned event cursor contract; not enforced acknowledgement state on the server |

The event model uses one-event pages to expose read/restart boundaries. Real
pages contain at most 1,000 records. Lost responses may be reread, and a caller
that loses its saved cursor may receive duplicates. The API does not keep
subscriber acknowledgements or promise exactly-once event consumption. No
uncommitted data is exposed in the model even when a stale cursor is reused.

Ten deliberately faulty variants test duplicate insertion, replacement of
accepted content, dispatch without admission, replay of an unknown operation,
cancellation treated as confirmation, stale terminal overwrite, a lost event
commit, inbox overflow, exposure of an uncommitted event and advancing a cursor
after a lost, unobserved page. Only the delivered-page branch updates the
reader's observed-event maximum. Each mutation must produce its named assertion
counterexample in the replay, not merely any assertion failure. Positive models run
with and without partial-order reduction. Simulations use seeds 17, 29 and 53;
they supplement the exhaustive finite safety searches, not replace them.

Run the existing verifier from the repository root with SPIN and `cc` on PATH:

```sh
cargo run --locked --manifest-path rust-protocol/Cargo.toml --bin verify -- --out /tmp/mas-formal-evidence
```

The checker records model hashes, tool versions, commands, simulations, complete
search results, unreachable states and counterexample replays. Its standard
limits are 30 seconds per command, 512 MiB and depth 100,000. Exhaustion of a
resource bound fails acceptance rather than producing a positive result.

`test_bridge_operations.py` checks snapshot consistency, unknown-state retention,
all continuation run references, schema rejection and audit-only restoration.
The service and remote-adapter tests exercise identity conflicts, dispatch
recovery and lifecycle mapping against the implementation. These tests provide
correspondence evidence. The local-service model below adds executable shared
traces; there is still no formal refinement proof from Python, Rust, SQLite or
a remote implementation to the models.

Cross-principal authorisation, OAuth, HTTP framing, SDK behaviour, TLS, provider
availability, monetary limits and mathematical correctness of returned content
are outside these models. Local cancellation before any dispatch is also outside
the remote-operation model; the implementation can confirm that local case
without claiming that a dispatched remote effect has stopped.

## Local service concurrency and shared traces

`task-service.pml` separately models the local ingress lifecycle. Its exhaustive
schedule has two authenticated principals sharing one submission key, one task
per principal, two payload values, one scheduled task with at most two revision
identities, two competing update keys, one worker and one crash. The other task
exercises principal isolation and request-key separation; the exhaustive search
does not schedule both task lifecycles concurrently. Parameterised transitions
also execute the second principal's lifecycle in the shared trace corpus.

API transactions are indivisible, corresponding to `TaskStore.transaction` and
its `BEGIN IMMEDIATE`. Worker claim, executor reply and final service commit are
separate transitions so cancellation can intervene. A final `input_required` or
`auth_required` reply combined with persisted cancellation closes the local
continuation. `completed` and `failed` replies can win that race; `unknown`
remains unresolved. This distinction reproduced the implementation's previous
late-cancellation defect and is tested by `BUG_SERVICE_CANCEL_PAUSE`.
Grants remain fixed and each principal owns only one task in this model. Event
counts cover lifecycle events; extra protocol-event payloads appended in the
same final transaction are exercised by implementation tests, not represented
as individual records here.

| Requirement | Model assertion | Concrete boundary and observation |
| --- | --- | --- |
| An authenticated principal cannot observe another principal's receipt | `SERVICE_OWNER_ISOLATED` | `_owned`, principal-scoped request lookup, distinct task IDs and rejected foreign reads/cancel/update |
| A principal's accepted key and content remain stable | `SERVICE_KEY_UNIQUE`, `SERVICE_CONTENT_IMMUTABLE` | `requests` primary key, request digest, repeated and conflicting submission; corpus checks stored task and execution input |
| Continuation creates one execution identity per revision | `SERVICE_REVISION_UNIQUE` | `executions` uniqueness, `update`; corpus checks revision rows and assigned run IDs |
| An executing revision cannot be replaced before final commit | `SERVICE_COMMIT_CURRENT_REVISION` | Update only from paused state, one worker; competing updates and update-while-running rejection |
| Late cancellation cannot strand a paused local continuation | `SERVICE_CANCEL_CLOSES_PAUSE` | `run_once` rereads cancellation inside its final transaction; both paused outcomes and legacy paused cancellation tests |
| Service changes and durable events agree | `SERVICE_EVENT_ATOMIC` | Service mutations and `event` within the same SQLite transaction; per-step event counts and task identity |

`executions` counts registered execution identities, not physical calls to an
executor: recovery may invoke an executor again against the same run. The model
assumes the existing host checkpoint contract: an interrupted run either has a
retained reply or returns `unknown` without automatically repeating an uncertain
effect. The host-recovery model and real Rust-runner crash tests supply separate
evidence for that boundary. `SERVICE_REMOTE_INDEPENDENT` states environmental
separation: these local transitions do not modify remote evidence. It is not a
proof of remote cancellation or of the remote store implementation.

Four defect variants remove the paused-cancellation repair, scope a key without
its principal, permit a continuation during execution, or lose an event commit.
Each must fail its named assertion. Five separate reachability witnesses require
counterexamples to the *negation* of reaching both principal receipts, late
paused cancellation, revision one, unknown recovery after a crash, and a rejected
foreign lookup. They are evidence that these scenarios occur, not defects or
positive safety proofs. The ordinary model must complete with and without
partial-order reduction.

The canonical corpus is
`formal/task-service-traces.json`. The verifier generates deterministic
Promela executions using the same transition inlines; every action checks its
expected response category, task state, revision, cancellation intent and event
count. `test_task_service_conformance.py` consumes the exact same JSON against
the real `DurableTaskService` and SQLite repository, checking public errors and
receipts, stored inputs, event ordering/ownership and run identities. It uses a
controlled executor to expose reply/commit/crash schedules. These selected traces
provide executable finite trace correspondence. They do not establish that all
implementation traces refine the model, that every model trace is implemented,
or that an external SDK, process lock or authentication boundary is correct.

Transport authentication, OAuth, HTTP/stdio wire profiles, worker/guardian lock
ownership, grant revocation, retention and restoration require their own
implementation tests. No model here establishes arbitrary principal/task counts,
unbounded revision concurrency, provider progress, or automatic resolution of
unknown effects. Passing the suite is sufficient only for the explicit finite
properties and selected implementation traces above.
