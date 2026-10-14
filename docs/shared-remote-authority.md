# Shared remote authority

The PostgreSQL service profile stores federation registrations, provider circuit
permits, remote submission intents, continuation intents and reconciliation
evidence in the same namespace and controller epoch as native MAS admission.
`shared_remote.rs` implements explicit domain commands; Python never translates
SQLite statements or copies a local database into PostgreSQL.

`SharedPeerRegistry(authority)` implements the versioned registry used by
`ManagedPeerConnector`. It preserves the existing descriptor identity, caller
grants, revision comparisons, administrative reasons and permanent revocation
tombstones. A controller whose epoch has been replaced cannot read or change the
registry using its old authority. Registration is blocked during drain;
revocation remains available to the current administrator.

`SharedCircuitBreaker(authority, policy)` uses the database clock for cooldowns
and half-open deadlines. The shared permit record prevents concurrent controllers
from choosing two half-open probes. An older generation's late success cannot
close a newly opened circuit. A missing probe response consumes another cooldown
without asserting that its effect stopped. Circuit state suppresses new calls;
it never retries one or returns budget. The runner obtains this implementation
from the shared authority's circuit factory.

`SharedRemoteTaskService(authority, client, namespace=...)` implements the
existing protocol-neutral remote lifecycle used by MCP, A2A and Temporal
adapters. The namespace and client descriptor come from trusted configuration.
Each outward operation runs inside `AdapterContext.invoke`. Its intent command
checks the native executing invocation, exact reservation, run owner and current
controller epoch inside the PostgreSQL transaction. The already admitted call
remains charged even when later transport work fails.

The command commits an unresolved intent before network I/O. Only a fresh
non-replayed begin acknowledgement permits dispatch. A lost acknowledgement,
unknown first submission or repeated mutation cannot cause a second outward
call. Completion compares the operation, invocation, remote identity and current
epoch before storing its snapshot. A late reply cannot overwrite an invocation
already resolved by an operator.

Unknown submissions require a separately admitted attachment with evidence and
an exact peer observation. Unknown continuations and cancellation requests retain
their pending mutation until an explicit `applied` or `not_applied` resolution.
Failed observations can be recorded as `observation_unavailable`. Authority
handover permits new observations while preserving unresolved mutation evidence;
it does not imply the original remote execution stopped. All original keys,
charges and tombstones survive. Remote intent count is capped at 100,000 per
namespace and operations at 10,000 per intent; exhaustion rejects admission.

Shared mode must construct these repositories from its `SharedAuthority` and
must not select `RemoteTaskStore` or a local peer/circuit database. Local runner
directories remain replaceable work caches. Remote provider effects still need
their own idempotency and reconciliation contracts; PostgreSQL transactions
cannot make an external provider call atomic with its response receipt.

`test_shared_remote.py` contains actual PostgreSQL acceptance tests for durable
lifecycles, lost begin and first receipts, pending continuation recovery,
principal namespaces, forged admission rejection, peer handover/revocation and
concurrent half-open probes. The backend workflow runs these against the pinned
PostgreSQL image. Tests skip explicitly when no PostgreSQL fixture is provided;
that skip is not backend acceptance evidence.
