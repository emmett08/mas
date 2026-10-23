# Shared Rust protocol repository

The optional `postgres-repository` feature implements the complete Rust
`ProtocolRepository` operation contract using PostgreSQL. The existing SQLite
`Store` implements the same contract. `Gateway` and `HostSession` accept either
repository through a typed transaction-level interface; callers cannot split a
message insertion from its task transition or quota charge.

The shared host profile adds PostgreSQL repositories for lifecycle records,
invocation journals, deployment/ancestor allocations, definitions, scheduler
ownership, composition checkpoints, remote intents, workers, circuits and
artefact metadata. Local directories hold replaceable execution caches. The
profile still depends on externally retained secrets, an authenticated object
store, PostgreSQL durability and a qualified promotion/backup policy. Enabling
a feature flag alone cannot establish measured availability or RPO/RTO.

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
for an external effect. The host command envelope stores a stable command digest and receipt inside
the same transaction as its domain mutation. An exact replay returns the old
receipt with `replayed=true`; changed content for that identity conflicts.

Expiry observations are committed before inspection. Closing a task on expiry
does not settle an external call or refund allocations. Protocol clocks use the
PostgreSQL server; controller/worker local clocks cannot extend a run's bound.

## Verification

Compile and run the ordinary contracts with the optional backend enabled:

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml --features postgres-repository
```

The real PostgreSQL conformance suite is opt-in locally and mandatory in
the `native-admission` job in `.github/workflows/check.yml`. The shared-backend
qualification pins the official PostgreSQL
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

The command executable requires certificate and hostname verified TLS for
remote PostgreSQL endpoints. `sslmode=disable` is rejected remotely; default
remote negotiation is strengthened to `require`, without plaintext fallback.
`postgres_ca_file` supplies an optional trusted PEM CA. Local Unix/loopback
endpoints may use plaintext for a fixture or separately authenticated tunnel.
No option disables remote certificate or hostname validation. The Rust library
also accepts a caller-configured authenticated PostgreSQL client.

Local guardian locks protect local process and cache lifetime. PostgreSQL
run-owner records, controller epochs and leases decide whether shared mutations
are permitted. `run_id` resumes; `new_run_id` creates an explicitly retained ID
idempotently, so a lost creation response cannot allocate a second leaf run.

## Controller lifecycle and recovery

The explicit schema 2 to 3 migration installs a controller lease using the
PostgreSQL clock. Every read and mutation validates the epoch and live lease
under the same authority-row lock. `authority_status()` returns the observed
epoch, database time, lease deadline and drain state. Expired or stale controllers
cannot supply successful stale reads. `start_renewal()` maintains only the bound
epoch; it never elects itself or silently changes identity. `close()` stops that
renewal. A controller that loses its lease must reconcile or hand over explicitly.

`handover(next_controller)` transfers a live epoch. `takeover(next_controller)`
requires the exact preceding binding and an expired lease, increments the epoch
with compare-and-set semantics, and preserves all records. Competing successors
cannot both win. Run claims from earlier epochs may be reclaimed; their prepared
or executing effects keep their original call IDs and allocations. Lease expiry
never establishes that a provider stopped or that its outcome is absent.

Draining blocks new run, call, task, composition and worker admissions inside
the authority transaction. Existing observations, event delivery and audited
recovery remain available. `resolve_invocation` requires exclusive current run
ownership and an attributed decision with evidence. It records either established
completion or explicit permission to retry with a new metered call. It never
calls the provider, deletes the old reservation or refunds prior charges. Stable
resolution command identities make a repeated operator request replay-safe.
`SharedRecoveryService` supplies the authenticated inbox/query/resolve surface
using shared run IDs and receipts, without reading local SQLite or inferring
execution termination from a local lock.

Schema 3 to 4 installs the shared service domain tables in one explicit migration.
Older compiled schemas reject the upgraded version. Service components call
native domain transactions through `SharedAuthority`; there is no generic SQL
rewriter or local authoritative shadow database. The migration is explicit and
cannot reset deployment identity, task request epochs, frozen definitions,
checkpoints or uncertain effects.

These transitions fence controllers connected to the **same** writable database.
They do not fence a separately restored writable copy, certify replica promotion,
or prove that acknowledged WAL survived data loss. Backup/PITR activation must
establish source fencing and acknowledgement durability before new admissions.
The real PostgreSQL suite tests lease takeover, stale-owner rejection, drain,
atomic reservations, reconnect/replay and attributed resolution. Multi-machine
replication and measured RPO/RTO require their separate deployment evidence.
