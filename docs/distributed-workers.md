# One controller, registered remote workers

The remote worker profile distributes leaf invocation execution while keeping
Rust admission, call allocation and host journals at one controller deployment.
It uses `RemoteWorkerAdapter` inside the ordinary Runner invocation path. Workers
consume an already issued call identity; they cannot submit work as a user,
choose a workflow, delegate host tasks or allocate another call.

This is an initial Linux worker profile with a SQLite controller. It does not
provide multiple active controllers, database replication, failover election or
an OS sandbox for hostile applications. Registered processes are trusted code;
the Rust guardian bounds their lifetime and resource use. Hostile code needs a
separate deployment isolation boundary. See [executable workers](executable-workers.md).

## Placement and authority

Each worker authenticates as its own principal with only `mas:worker`. A separate
controller grant binds that principal to operation names, exact implementation
SHA-256 identities, a trust zone and capacity. Neither token possession alone nor
a worker-supplied capability advertisement creates a grant. A worker registers
its runtime, architecture, boot identity and configured capabilities. Placement
requires an exact implementation and trust-zone match.

The capability identity includes the registered executable descriptor, local
execution deadline and allowed inner reservation. An inner provider invocation
uses the original controller call identity and cannot reserve more than the
controller already charged. The worker cannot retry a provider invocation. Known
`not_sent` and completed-but-invalid observations cross IPC, the receipt spool and
the controller without being converted to unknown; controller retry policy remains
the sole authority for a subsequent charged invocation.

Global and per-user outstanding caps include pending, claimed, dispatched and
unknown jobs. Unknown jobs also retain that worker principal's advertised capacity,
including after a new boot; restarting the worker does not free an uncertain effect. Unknown effects hold capacity until a trusted host-journal resolution is
explicitly imported by the local operator reconciliation command below. Saturation before enqueue records a
known `not_sent` failure; its existing call allocation remains charged.

Capacity, lease duration and grants are pinned in controller metadata. A second
process using different policy is rejected. There is currently no policy
amendment command: plan capability changes as a coordinated deployment upgrade,
and preserve existing state for audit. Drain stops new claims without cancelling
already granted effects.

## Durable boundaries

| Event | Durable behaviour |
| --- | --- |
| Claim expires before dispatch | Another worker may receive a newer fence. The old fence cannot dispatch. |
| Dispatch response is lost | The worker does not execute without a positive response. It retains an unknown observation; it never repeats dispatch to obtain a new grant. |
| Lease expires after dispatch | The job becomes unknown and is never assigned again. Heartbeats demonstrate contact, not that effects have stopped. |
| Worker restarts | A new boot fences its older claims. An old boot cannot register again. Interrupted dispatching/executing records become unknown receipts. |
| Receipt acknowledgement is lost | The worker resends the same durable receipt ID. It does not repeat application execution. |
| Receipt arrives after fencing | The controller retains the authenticated evidence but cannot use it to overwrite the current assignment or task. |
| Controller is restored for audit | Worker authority remains closed. Restore does not permit new dispatch. |

The pinned invocation and receipt explicitly declare `inline-json/1`. Successful
output is an integer-JSON object bounded to 65,536 bytes; evidence has the same
individual bound, with a separate whole-receipt bound. The controller commits
those inline bytes before acknowledging storage. URI strings and reference-shaped
dictionaries remain opaque application JSON: the acknowledgement does **not**
certify external files, artifact availability or content integrity. There is no
artifact-output worker profile. Use the separate explicit artifact upload/commit
API where needed; a future worker artifact contract must bind a trusted controller
store verifier before acknowledging that artifact as durable.

The controller commits receipt evidence before acknowledging storage. Workers
keep their private spool until that acknowledgement. Failed uploads stop new
work rather than discarding evidence. A spool defaults to a 16 MiB main SQLite
database and 100 unacknowledged attempts; each complete receipt is bounded below
128 KiB. SQLite's rollback journal needs additional temporary disk space.
The worker enforces the database page ceiling on each write connection, reserves
space for assignment/receipt row updates before claiming, and allocates its
receipt storage before requesting dispatch. Acknowledged rows release reusable
pages; small configured limits can close admission even when the database is
empty. The Rust guardian retains the worker ownership lock until its descendants
are reaped, including after worker-parent death. Oversized
or invalid application output produces a bounded unknown observation. Controller
retention protects live/unknown jobs and non-authoritative receipts. These records
are deliberately not automatically erased when a lease expires.

## Run the local example

Use the repository's pinned Rust and Python prerequisites. Build the release
binaries and install the host package, or set `PYTHONPATH` as below. Start in the
repository directory:

```sh
export PYTHONPATH="$PWD/host:$PWD/examples/host"
export MAS_BIN="$PWD/rust-protocol/target/release/mas"
export MAS_GATEWAY_BIN="$PWD/rust-protocol/target/release/mas-gateway"
python examples/host/remote_echo.py
```

The last command prints the echo capability digest. Compute it on each worker:
registered executable paths, versions and pinned files are part of its identity.
Use reproducible deployment paths and dependencies if several workers should
advertise the same capability. The example pins its own file and the worker
entry module; it does not claim to pin every transitive Python dependency.

Create private token files and a private verifier file using the authentication
setup described in [bridges](bridges.md). Give each worker a distinct principal
and token; the verifier entry's scopes must be `["mas:worker"]`. Keep the raw
bearer token only on its worker. Put the SHA-256 digest in the controller verifier.
A controller configuration is:

```json
{
  "version": 1,
  "auth": {"type": "static", "file": "/private/worker-verifier.json"},
  "public_url": "http://127.0.0.1:8766",
  "max_outstanding": 100,
  "per_principal": 10,
  "lease_ms": 30000,
  "worker_grants": {
    "worker-one": {
      "capabilities": {"echo": "REPLACE_WITH_PRINTED_64_HEX_DIGEST"},
      "trust_zone": "local-demo",
      "capacity": 1
    }
  }
}
```

The controller state directory must be the same deployment directory used by the
Runner executing `RemoteWorkerAdapter`. The controller endpoint alone never
admits a new call.

```sh
python -m mas_host workers controller --config controller.json \
  --state-dir ./controller-state --allow-loopback-http
```

A worker configuration is:

```json
{
  "version": 1,
  "endpoint": "http://127.0.0.1:8766",
  "token_file": "/private/worker-one.token",
  "factory": "remote_echo:operations",
  "trust_zone": "local-demo"
}
```

The factory is trusted operator configuration and returns registered leaf
operations. The registered operation name becomes the worker request operation,
including a contract version such as `solve/1`. The leaf context uses actor
`worker` and caller `controller`; configure structured-provider contracts for
those identities. The authenticated user principal remains in the controller
invocation binding, not a worker-selected caller field. Task data cannot supply import names or executable paths. Start each
worker with its own spool directory and token:

```sh
python -m mas_host workers run --config worker-one.json \
  --state-dir ./worker-one-state --allow-loopback-http
```

For different machines use HTTPS with the existing bounded HTTP transport policy
and an HTTPS reverse proxy at the controller; loopback HTTP is solely for this
example. See [HTTP transport policy](http-transport-policy.md) for private CA and
mTLS configuration. Preserve the worker spool across restarts. Do not share one
spool between worker identities or use multiple live processes for one token.

In the controller's application factory, construct the controller with the same
pinned grants and policy, then install a `RemoteWorkerAdapter` for the appropriate
actor. Bind its user principal from the authenticated task admission context:

```python
from mas_host.distributed import RemoteWorkerAdapter, WorkerController

controller = WorkerController(deployment_dir, worker_grants,
    max_outstanding=100, per_principal=10, lease_ms=30000)
adapter = RemoteWorkerAdapter(controller, principal=authenticated_principal,
    operation="echo", implementation=echo_digest, trust_zone="local-demo",
    reservation_microusd=0)
# Supply adapter in the ordinary Runner adapter map for the leaf actor.
```

The endpoint exposes only authenticated `POST /workers/register`, `claim`,
`dispatch`, `heartbeat`, `receipt` and `drain`. `WorkerClient.call` implements that
bounded RPC surface without automatic effect retries. Application/operator code
can inspect scoped jobs and retained receipts through `WorkerController.inspect`
and `evidence`. A worker token cannot adjudicate evidence or release capacity. First use the
existing operator recovery service or `mas_host admin resolve` to record an
evidence-backed host-journal resolution. Then release the exact remote obligation:

```sh
python -m mas_host workers reconcile --config controller.json \
  --state-dir ./controller-state --operator operator-name \
  --run-id ORIGINAL_RUN_ID --call-id ORIGINAL_CALL_ID
```

This local command requires the controller's private state and pinned configuration.
It accepts no new outcome. It verifies the original allocation, run identity, exact
invocation binding and an existing `resolved` or `retry_allowed` journal decision,
then records an immutable resolution reference/digest and marks only that job
`reconciled`. It releases its outstanding capacity without refunding the allocation
or resuming execution. A later worker receipt remains retained, non-authoritative
evidence and cannot reactivate the job. Normal service recovery is a separate
explicit action; confirmed no-effect permits a new charged call only through the
existing host recovery policy.

## Evidence and release limits

`test_distributed_workers.py` exercises real Runner allocations, authenticated
HTTP, lease expiry, stale boot/fence rejection, receipt replay after a lost
acknowledgement, queue quotas, restore admission closure, and two independent
worker processes executing through the Rust guardian. The provider outcome test
runs known failures through the same registered worker IPC, HTTP, spool and
controller boundaries. These tests use local processes. A real two-machine
network partition, machine reboot, durable-volume failure and certificate
rotation drill remains a release gate; no test here establishes high availability
or proves arbitrary provider effects are exactly once.
