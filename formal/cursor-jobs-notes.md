# Cursor durable-job safety contract

`cursor-jobs.pml` models one repository job submitted to the Cursor Cloud Agents
API. It applies Holzmann's separation of service, environment, state transitions
and observable outcomes. A submission receipt confirms local tracking; it does
not establish remote task completion or correctness of the returned work.

The host admits a reservation, persists its client-assigned agent identity and
launch intent, sends a POST, and separately records the returned run identity.
These are separate transitions. A crash can therefore occur after remote
acceptance and before local receipt persistence. Recovery leaves this case
unresolved. An exact agent/run lookup can establish a binding; neither a timeout
nor a missing GET result authorises another launch. Remote execution may
continue independently of the local process.

Host operations are serialised, corresponding to the implementation's exclusive
job-store lock. Atomic blocks represent durable SQLite transactions or one
local state update while holding that lock. They do not combine the remote POST
with the later durable receipt. A remote process can finish, fail, expire or
accept cancellation independently. Poll observations can be stale. Cancellation
intent, cancellation dispatch and a correlated cancelled observation are
different events. A local deadline supplies no remote terminal observation.

| Assertion | Required property |
| --- | --- |
| `CURSOR_ID_RETAINED` | The client-assigned agent identity survives restart and every operation. |
| `CURSOR_NO_RELAUNCH` | At most one launch is dispatched for the logical job, including unknown submissions. |
| `CURSOR_ADMITTED` | Admission precedes launch dispatch. |
| `CURSOR_RUN_CORRELATED` | A bound run belongs to the intended remote agent and matches the explicit run lookup. |
| `CURSOR_RUN_IMMUTABLE` | A recorded run identity is never replaced. |
| `CURSOR_TERMINAL_OBSERVED` | A terminal local status comes from a correlated remote observation. |
| `CURSOR_TERMINAL_IMMUTABLE` | A terminal receipt cannot be overwritten by a stale or conflicting poll. |

The finite domain has one logical job, one stable agent identity, two candidate
run identities (one correct and one mismatching), one crash, two poll
observations and one cancellation request. Admission is an abstract boolean:
the existing host-recovery model separately checks charging and reservation.
The model does not claim a numeric upper bound on provider charges.

The implementation correspondence is:

| Model transition | Implementation boundary |
| --- | --- |
| Admission | `AdapterContext.invoke` reserves and admits the call before the adapter callback. |
| Stable agent identity | `JobStore.prepare` persists the frozen specification and client-assigned ID. |
| Launch intent, POST, receipt | `DurableJobService.submit` commits `launch_unknown`, calls `CloudClient.create`, then commits the validated snapshot. The model's `Launching` is the transient live caller after that intent commit. |
| Restart after ambiguous dispatch | A new process reads the retained `launch_unknown` row; it cannot dispatch again. |
| Explicit run binding | `DurableJobService.attach` validates operator evidence, remote agent settings and the exact run identity before recording it. |
| Poll and terminal persistence | `DurableJobService.poll`, `run_snapshot` and `_snapshot` preserve the bound IDs and first terminal snapshot. |
| Cancellation intent and dispatch | `DurableJobService.cancel` commits the request flag before the API call and retains active status until a terminal poll. |
| Operation serialisation | `JobStore.open` holds the exclusive job lock across a complete operation, including its bounded HTTP request. |

The public service methods accept trusted application calls. The model abstracts
the adapter's admission contract; it does not make a caller-supplied invocation
identifier an authentication credential.

Seven isolated preprocessor mutations remove charging, replay an unknown launch,
discard the durable identity, accept a mismatching run, equate cancellation
intent with confirmation, equate a local deadline with remote cancellation, or
overwrite a terminal receipt. The verifier requires an assertion counterexample
and successful trail replay for each mutation. It also checks the unmodified
model without partial-order reduction and records simulations with seeds 17, 29
and 53. Simulations illustrate executions; they do not establish coverage.

Run the existing verifier to execute these checks alongside the protocol suite:

```sh
cargo build --locked --manifest-path rust-protocol/Cargo.toml --bin verify
./rust-protocol/target/debug/verify --out formal/evidence
```

The verifier records exact model hashes, SPIN/compiler versions, commands,
memory/time/depth limits, full-state-search results and replayable mutation
trails in `summary.json` and the per-case directories. The standard limits are
30 seconds per command, 512 MiB and depth 100,000. A resource limit or incomplete
search fails the acceptance gate; it cannot become a successful verification.

Stopping with an active or unresolved job is allowed. There is no progress,
eventual cancellation, availability, exactly-once remote effect, monetary-cap or
unrestricted-fault claim. SQLite durability, filesystem locking and provider
response interpretation remain implementation obligations. Credentials, HTTP,
Cursor SDK internals, native bridge processes, model outputs and repository
effects are outside this model. The restricted local SDK adapter continues to
use the separately modelled host checkpoint/recovery contract. No formal
refinement proof from Python, Rust or the external service to these models is
claimed.
