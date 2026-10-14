# Host architecture and behavioural contracts

The host schedules trusted adapters over the existing Rust admission boundary.
Its abstractions preserve actor identity, ordered workflow replay, call
reservation before dispatch, original deadlines and uncertainty after an
unconfirmed external effect. Rust remains the authority for wire validation,
message/task transactions and protocol admission. Python records workflow
checkpoints and deployment allocations.

## Design choice

Three options were considered for the readiness repairs:

| Option | Consequence |
| --- | --- |
| Patch the existing methods in place | Small textual changes, but bootstrap, subprocess transport and execution errors remain coupled and difficult to fault-test independently. |
| Extract the observed ownership and interface boundaries | Keeps the public runner façade and persistence semantics while making bootstrap, execution strategies and schema admission independently testable. Selected. |
| Replace the host with a new service framework or distributed scheduler | Introduces transport, deployment and migration obligations that do not resolve the observed single-host defects. |

The selected changes address specific failures. A file split or interface alone
does not establish SOLID compliance. The relevant acceptance criteria are
substitution across execution strategies, exclusive ownership during cleanup,
durable identity across bootstrap crashes and rejection of incompatible storage
before writes.

## Responsibilities

| Component | Owned responsibility |
| --- | --- |
| `contracts.py` | Adapter-facing values, typed public failures and narrow context/execution protocols. |
| `codec.py` | Bounded exact integer-JSON validation for host boundaries. |
| `Runner` | Ordered request orchestration and checkpoint replay through a stable façade. |
| `RunSession` | Exclusive run lifetime and staged, idempotent bootstrap. |
| `ProtocolClient` | Bounded Rust subprocess execution, credential-free child environments and sanitised diagnostics. |
| Execution strategies | Inline or supervised execution under the same adapter-facing contract. |
| Process-group guardian | Retain ownership until local adapter work and ordinary descendants have terminated. |
| Journal and deployment repositories | Own their distinct schema identities, validated migrations and durable records. |
| Reconciler monitor | Report a fresh, successful scan by a live watcher independently of admission headroom. |
| Provider binding | Require explicit effect identity and spending allocation before admission. |
| `provider_config.py` | Validate per-role provider settings and secrets, then construct only the selected implementations. |
| OpenAI and Cursor local providers | Adapt their respective SDKs to the narrow invoice-total contract. |
| `CursorCloudClient` | Bounded, non-retrying HTTP operations against the fixed Cursor Cloud API origin. |
| `JobStore` and `DurableJobService` | Persist remote-job intent, correlate exact identities, serialise observations and retain reconciliation evidence. |
| `CursorCloudJobAdapter` | Admit a repository-job submission through the existing runner and return its durable receipt. |

The façade, Strategy and Adapter patterns correspond to existing variations.
The bootstrap state machine records intent, deployment admission, Rust state,
journal readiness and gateway readiness. These stages span separate durable
stores; the design uses idempotent recovery rather than asserting a transaction
across all stores. Rust task transitions and message insertion retain their
single atomic transaction.

## Interface preservation

Both execution strategies provide only the adapter contract: actor identity,
invocation identity, remaining time, delegation and reserved invocation.
Recoverable public errors cross supervised IPC using fixed typed responses;
private exception strings and provider payloads do not become diagnostics.
An uncertain effect remains unresolved even when adapter code catches its
exception. The host cannot manufacture external cancellation or no-effect
evidence.

The new provider contract deliberately removes implicit defaults for custom
providers. Implementations must declare their descriptor and allocation.
Existing built-in descriptors retain their identity; custom providers missing
the newly mandatory members must be updated before they can run. A descriptor
is the adapter author's compatibility declaration, not proof of provider
determinism or truthful billing.

## Provider substitution and durable jobs

The provider extension compares three designs:

| Option | Consequence |
| --- | --- |
| Treat a Cursor token as an OpenAI credential | Unsupported: the documented APIs have different authentication and execution contracts. |
| Make every provider implement an asynchronous job service | Adds job lifecycle obligations to synchronous inference and obscures the existing result contract. |
| Use interchangeable local result providers and a distinct durable-job adapter | Preserves the existing workflow and gives remote jobs explicit submission, observation and cancellation semantics. Selected. |

`TotalProvider` is a Strategy interface: the planner and reviewer depend on
`total`, a descriptor and an explicit reservation. A Factory selects mock,
OpenAI or Cursor independently for each role. The SDK adapters share validation
of invoice inputs and exact integer-JSON outputs, while retaining provider-owned
authentication and response conversion. Existing OpenAI and mock descriptors
remain unchanged so their completed workflows retain recovery compatibility.

`CursorLocalProvider` creates a fresh private directory, disables tools, MCP,
subagents and ambient settings, and attempts to close its owned agent and bridge
on normal return or exception. Its bridge
is launched only after durable invocation admission, within the supervised
process group. These controls constrain the SDK configuration; they do not
isolate hostile code from the operating system. SDK-managed internal model
retries remain distinct from the host's one admitted submission.

The Cloud adapter implements the existing `Adapter` contract, not `TotalProvider`.
Its output says that a repository job was submitted. `JobStore` persists the
frozen specification, client-assigned agent ID and launch intent before dispatch.
The service records the exact returned run ID and polls only that identity.
Ambiguous dispatch retains `launch_unknown`; it never enables an automatic
second POST. Cancellation intent and remote cancellation are separate records.
The first correlated terminal observation remains immutable.

This division applies single responsibility to configuration, SDK conversion,
HTTP transport and persistence; interface segregation keeps repository lifecycle
methods out of synchronous providers. Injected clients permit contract tests
without credentials. Rust envelopes, route permissions and task transitions are
unchanged. The cloud job's completion has its own lifetime, so a completed MAS
submission task cannot imply that repository work finished or passed review.

The forked guardian and worker freeze inherited garbage-collected objects
before closing copied host descriptors. This prevents an inherited file or
SQLite finaliser from later closing a descriptor number reused by the worker.
New worker objects remain collectable; the parent collector is unchanged.
A regression forces that descriptor reuse and explicit collection.

## Verification and limits

Regressions exercise actual process death during bootstrap, a killed host with
ordinary adapter descendants, ownership retention during cleanup, incompatible
database versions, failed reconciliation, persistent live-acceptance state and
context error substitution. Container acceptance runs the installed package and
Rust binaries on native amd64 and arm64. OpenAI HTTP contract tests use the
real pinned SDK with a fake transport; Cursor contract tests use injected
clients and separately exercise the installed bridge. Live local-provider
acceptance is a two-submission check requiring credentials and persistent state.
The SDK may perform internal requests that the host does not count separately.

The finite Promela models preserve their documented safety and progress scope.
They do not model Python schemas, Linux process cleanup, actual storage flushes
or provider behaviour. The additional Cursor job model checks finite launch,
correlation, terminal-state and cancellation safety; it has no remote progress
or monetary-cap claim. Implementation regressions supply evidence for these
additional boundaries; no refinement proof is claimed.

The project retains Python's declared supported syntax, existing unittest/Cargo
tests, locked dependencies and SPIN verification. Rust 1.99.0 is the current
pinned toolchain. The runtime repair introduces no new formatter, linter or framework.
Private orchestration helpers remain with their owning workflow when separating
them would split a transaction or replay invariant; bootstrap and command
execution were extracted because they have independent failure and lifecycle
contracts.
