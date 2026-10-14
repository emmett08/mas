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

Nine deliberately faulty variants test duplicate insertion, replacement of
accepted content, dispatch without admission, replay of an unknown operation,
cancellation treated as confirmation, stale terminal overwrite, a lost event
commit, inbox overflow and exposure of an uncommitted event. Each must produce
an assertion counterexample whose trail can be replayed. Positive models run
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
correspondence evidence; there is no formal refinement proof from Python, Rust,
SQLite or a remote implementation to the models.

Cross-principal authorisation, OAuth, HTTP framing, SDK behaviour, TLS, provider
availability, monetary limits and mathematical correctness of returned content
are outside these models. Local cancellation before any dispatch is also outside
the remote-operation model; the implementation can confirm that local case
without claiming that a dispatched remote effect has stopped.
