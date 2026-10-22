# Nehemiah shared authority increment

This increment implements the first P1 observational host query set, a typed
P5b native admission boundary and the scheduler-policy amendment part of P6a.
The full enterprise, extensibility and PII plans retain their separate gates.

## Read observations

`scope`, `metadata`, `invocation`, `step`, `steps`, `invocations`,
`command_receipt` and `resolutions` now use the existing read-only repeatable-read
transaction. Deployment, controller, epoch, schema-version, backup-state and
lease checks remain active. These queries neither acquire the authority row's
write lock nor reconcile expiry. Every other host query retains its former
transaction until its reachable branches have been audited separately.

`observational_host_query_does_not_wait_for_authority_writer_or_expire_run`
holds the actual authority row lock and gives the observer a 500 ms statement
deadline. It checks that observing does not mark an expired run reconciled.
This supplies a targeted contention check. Workload capacity and latency claims
still require the declared deployment benchmark.

## Native dispatch permission

`shared_native::normalise_configuration` delegates run configuration to its
existing protocol owner. `PostgresRepository::admit_native_invocation` accepts
the original command, run, owner and call identities. It returns one of:

| Result | Consumer action |
|---|---|
| `FreshDispatch(permit)` | Consume the non-cloneable permit under its authoritative deadline |
| `Retained { receipt }` | Observe the original command/invocation without dispatch |
| `Rejected { code }` | Report the typed preflight refusal. No command was issued |
| `Uncertain { command_id }` | Reconcile that same identity before further action |

The native receipt and permit include `deadline_ms`, calculated in the admission transaction as the minimum of the original protocol deadline and any attached task business/remaining-active limits. A queued task with incomplete clock evidence refuses admission. The consumer must enforce that deadline and may narrow it further.

The successful transition remains the existing `admit_invocation` transaction,
including protocol reservation, scope charges and executing marker. This API
adds no alternative journal or admission authority. It reconstructs no permission
from a retained `admitted: true` JSON value. Database failures and authority or
domain changes between preflight and commit remain uncertain. Preflight refusals
are snapshots rather than durable terminal command receipts. A lost fresh reply
can therefore prevent execution while preserving conservative recovery.

The permit type prevents ordinary cloning/deserialisation in Rust. It supplies
no cryptographic isolation from trusted controller code, and it cannot recall a
permission after admission. The caller still owns worker lifetime fencing and
external-outcome reconciliation.

## Scheduler capacity amendments

The existing policy shape retains its 256-assignment ceiling. The explicit
`mas-scheduler-policy/2` shape admits configured worker, principal, workflow and
resource limits up to 16,384, with at most 256 named workflows/resources. This
bound expresses representational support rather than measured capacity.

`task.amend_scheduler` requires exactly `expected_policy_digest`, `expected_revision`, `policy`,
`reason` and `authoriser`. `expected_revision` and the next policy's monotonic `revision` also participate. The digest is SHA-256 of the current policy's
`serde_json::Value::to_string()` representation. The existing authority
transaction compares the full policy digest and monotonic revision before updating. Lower limits preserve occupied assignments and block new admissions until occupancy falls below the applicable allowance. It retains
the original command identity and an audit receipt containing both digests,
reason, authoriser assertion, occupied assignment count and new policy. Amendments consume the reserved administration receipt pool. Exact command replay returns that
receipt. It grants no new assignment.

`authoriser` is an assertion by the authenticated trusted controller, rather than
a new authentication mechanism. Existing assignments survive an amendment.
The existing `task.configure_scheduler` keeps its frozen-policy semantics.
Old clients that validate only the earlier limit reject larger version 2 values.

Queue byte/age allowances, provider quota adapters, complete queued-service
integration and deployment/capacity qualification remain distinct P6 work.

## Verification

Run the ordinary protocol tests, the PostgreSQL feature tests and the finite
model with:

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml
cargo test --locked --manifest-path rust-protocol/Cargo.toml --features postgres-repository --lib
cargo test --locked --manifest-path rust-protocol/Cargo.toml --features postgres-repository --test postgres_repository -- --ignored --test-threads=2
python3 formal/verify-native-dispatch.py
```

The database command requires `MAS_TEST_POSTGRES_DSN` pointing to an actual test
server. The dedicated CI job uses PostgreSQL 16.15. Compilation or an ignored
test supplies no database conformance result.

The model has two callers sharing one command, a potentially lost committed
reply, and two concurrent capacity amendments. Its safety search checks at most
one dispatch, compare-and-set revision admission and guarded new assignment admission after a lowered capacity.
Three isolated guard mutations produce replayed violations. Two deliberately
false reachability assertions witness dispatch and accepted amendment. The
model abstracts durable PostgreSQL transactions and successful worker ownership.
It proves no SQL/Rust refinement, unbounded availability, transport security,
worker fencing, performance or PII detector quality.

The implementation correspondence is the typed native admission test, actual
lost-reply/handover test and scheduler amendment/replay/revision/lowered-occupancy test in
`rust-protocol/tests/postgres_repository.rs`. The new database tests need
actual PostgreSQL execution before the shared profile can be qualified.

## Native source package custody

`definition.manifest` also accepts explicit `mas-native-implementation/1` packages.
Their exact keys are `format`, `bundle_digest` and nonempty `files`. Each file has
exactly a unique relative `path`, lowercase SHA-256 `sha256` and integer `bytes`
array. The authority verifies the retained file bytes and digest. This format
uses strict shared-command JSON parsing and its 1 MiB representation bound, with
at most 4,096 file entries. Existing Python package semantics remain unchanged.
The host consumer separately binds the package's bundle digest to its validated
native bundle. Retaining source and asserted binary identities establishes
custody, rather than a reproducible build, signature or runtime attestation.

`host_query("task.status", {principal,task})` reads one exact owned shared task
record in a repeatable-read, read-only transaction. It projects state, revision,
cancellation, update time, output presence and the small A2A service/agent
bindings. It does not load application input/result, event or execution history,
refresh timing, settle work or wait for the authority writer lock. It retains
current controller/epoch/lease checks and returns `execution_permission:false`.
