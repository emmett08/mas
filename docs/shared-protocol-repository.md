# Shared Rust protocol repository

The optional `postgres-repository` feature implements the complete Rust
`ProtocolRepository` operation contract using PostgreSQL. The existing SQLite
`Store` implements the same contract. `Gateway` and `HostSession` accept either
repository through a typed transaction-level interface; callers cannot split a
message insertion from its task transition or quota charge.

This is the Rust protocol component of W16. Python lifecycle records,
invocation journals, deployment/ancestor allocations, bootstrap secrets,
scheduler ownership and artefact metadata remain separate local stores. The
MAS host does **not** acquire high availability by enabling this Rust feature.
The shared-host backend, object store, replicated promotion and measured RPO/RTO
remain required before M4 can be claimed.

## Storage and authority

Each deployment uses a dedicated PostgreSQL schema containing native tables for
runs, messages, task transitions, call reservations/evidence and gateway
admission attempts. A separate authority row contains the deployment identity,
controller identity and monotonically increasing epoch. Every operation locks
and validates that row inside the same transaction that reads or changes
protocol state. A previous controller's still-connected handle is rejected
following handover. The first implementation serialises protocol transactions
within a schema; it makes no throughput or multi-tenant scalability claim.

`initialise` creates a new schema atomically and rejects an existing namespace.
It cannot overwrite an earlier ledger or reset an epoch. `open` checks version
and retained schema-layout digest, including columns, defaults, constraints,
indices and user triggers. Database ownership, DDL and the retained digest are
trusted administrative authority; the application role and migration process
must prevent unauthorised changes to them. The module accepts an already
connected `postgres::Client`, so the deployment owns credentials and TLS
verification. It never logs connection strings or server error detail.

`handover` changes the controller and increments its epoch while holding the
shared authority row. This fences old handles in that same database. It does
not activate an old backup, promote a replica, prove an acknowledged commit
survived database loss, or fence another independently restored database.
Deployment-level recovery must still establish those facts before admitting
new work. An earlier granted provider call remains charged and can still
produce an external effect after handover. The current authority can retain
its evidence under the original call identity.

## Identity and transaction boundaries

Message identities remain idempotent: identical message content returns the
original sequence, while changed content conflicts. Call identities remain
conservative: a repeated reservation is refused and must be reconciled, rather
than interpreted as permission to dispatch again. Gateway ingress attempts
remain separately durable and charged even when their payload is rejected.
Receiving an unacknowledged message redelivers it; acknowledgement is idempotent.
A PostgreSQL sequence may contain gaps after rollback; delivery ordering depends
on ordering, not consecutive sequence numbers.

There are no automatic transaction retries. A database connection or commit
error returns a bounded failure and requires reconciliation using the retained
message/call identity. This avoids treating an uncertain commit as permission
for an external effect. The later whole-host command envelope must add stable
command receipts across Python/Rust allocation and invocation-intent boundaries;
this component does not claim that cross-store contract is complete.

Expiry observations are committed before inspection. Closing a task on expiry
does not settle an external call or refund allocations. Protocol clocks use the
PostgreSQL server; controller/worker local clocks cannot extend a run's bound.

## Verification

Compile and run the ordinary contracts with the optional backend enabled:

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml --features postgres-repository
```

The real PostgreSQL conformance suite is opt-in locally and mandatory in
`.github/workflows/postgres.yml`. The workflow pins the official PostgreSQL
16.15 image by manifest digest. Set `MAS_TEST_POSTGRES_DSN` to a disposable test
database whose account can create/drop schemas and terminate its test sessions:

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml \
  --features postgres-repository --test postgres_repository -- --ignored --nocapture
```

The tests compare a complete SQLite/PostgreSQL protocol trace, race independent
connections against quotas and terminal transitions, inject a task-insertion
failure, reject schema drift, fence stale controller handles, terminate a
connection after an acknowledged allocation and preserve rejected gateway
charges across reconnects. These tests qualify the stated protocol boundaries;
they do not substitute for multi-machine database replication, full-host
failover, object storage or production availability acceptance.

## Shared leaf-run authority

The version-two host extension adds native PostgreSQL tables for immutable run
metadata, deployment/principal/ancestor allowances, run ownership, deterministic
checkpoints, event outboxes, invocation state and stable command receipts.
`install_host_repository` performs an explicit additive transaction from protocol
schema 1 to schema 2. A version-one writer rejects the upgraded schema.

`admit_invocation` checks the current controller and run owner, charges every
frozen allocation scope, reserves the native Rust call and writes its executing
intent in **one database transaction**. A rejected native insert or exhausted
ancestor rolls back all these changes. The command receipt records whether this
is a replay; a replay supplies no permission for another dispatch. A lost reply
leaves the original invocation uncertain. Recording a completed observation
updates the host invocation and native Rust evidence together without releasing
any charge. Retry eligibility and checkpoint compare-and-set revisions remain
durable across a fresh controller cache.

The trusted Python API is `mas_host.shared_authority.SharedAuthority`. Its
`runner(...)` method uses the existing `Runner` through injected session and
protocol ports. `run_id` resumes an existing run from its shared metadata; a
replacement cache does not require copied SQLite files. The caller supplies the
original workflow version, principal and externally retained secret reference.
Gateway capabilities are derived from a caller-supplied high-entropy key; neither
that key nor its plaintext capabilities are persisted in public metadata.

Build the private host command executable with:

```sh
cargo build --locked --release --manifest-path rust-protocol/Cargo.toml \
  --features postgres-repository --bin mas-shared
```

The command executable accepts PostgreSQL Unix sockets or loopback endpoints
only. It does not silently use plaintext for a remote database. Direct remote
TLS connections can be supplied to the Rust library using a verified PostgreSQL
client; a separately qualified authenticated TLS tunnel can expose a local
endpoint to the command transport. The standard images do not yet advertise
this shared-host profile.

Local guardian locks still protect local process and cache lifetime. PostgreSQL
run-owner records and controller epochs decide whether shared mutations are
permitted. A crashed owner is retained until a controlled authority handover;
this implementation does not infer that a provider effect stopped from a lease
timeout. Automatic leader election, renewal and replica activation remain open.

The shared leaf-run profile is an operational component, not the completed MAS
service backend. Task/composition scheduling, definition registration, peer and
worker intent, provider circuits, shared artefact metadata and administrative
backup/restore still need their shared repositories and service integration.
Provider circuit settings are rejected by this profile instead of falling back
to local durable circuit state. The command ledger also needs a complete
retention/capacity policy before long-lived deployment acceptance.
