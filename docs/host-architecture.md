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

## Verification and limits

Regressions exercise actual process death during bootstrap, a killed host with
ordinary adapter descendants, ownership retention during cleanup, incompatible
database versions, failed reconciliation, persistent live-acceptance state and
context error substitution. Container acceptance runs the installed package and
Rust binaries on native amd64 and arm64. Provider HTTP contract tests use the
real pinned SDK with a fake transport; live acceptance is a separate two-call
check requiring credentials and persistent state.

The finite Promela models preserve their documented safety and progress scope.
They do not model Python schemas, Linux process cleanup, actual storage flushes
or provider behaviour. Implementation regressions supply evidence for these
additional boundaries; no refinement proof is claimed.

The project retains Python's declared supported syntax, existing unittest/Cargo
tests, locked dependencies and SPIN verification. Rust 1.99.0 is the current
pinned toolchain. The runtime repair introduces no new formatter, linter or framework.
Private orchestration helpers remain with their owning workflow when separating
them would split a transaction or replay invariant; bootstrap and command
execution were extracted because they have independent failure and lifecycle
contracts.
