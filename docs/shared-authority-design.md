# Shared authority and controller replacement

MAS has two explicit storage profiles. The local profile uses the Rust journal,
SQLite repositories and single-host process ownership. The shared profile uses
native Rust PostgreSQL transactions for all controller authority and S3-compatible
storage for shared artefact bytes. It does not translate SQLite statements or
upload local databases as a shared-state substitute.

`SharedAuthority` is the bound Python command port. `create_shared_service()`
constructs the shared task and composition service, verifies immutable workflow
implementations, configures allocation and scheduler policies, and starts the
controller's renewal subprocess. It accepts an already provisioned controller
binding; it never elects or silently replaces a controller. Credentials and key
material come from trusted deployment factories, not service requests.

## Authoritative state inventory

| State | Shared owner and behaviour |
| --- | --- |
| Protocol runs, messages, delivery, acknowledgements, native task transitions and call reservations | `PostgresRepository` preserves the Rust protocol transaction boundary, unique identities and rejected-but-charged gateway attempts. |
| Host run topology, deterministic step inputs/checkpoints, invocation intents, outputs and evidence | `shared_host.rs` records the invocation and all allocation charges before permission to invoke; `SharedAuthority.runner()` resumes the original run. |
| Deployment, principal and ancestor allocation | Shared host scopes and `shared_accounting.rs` retain cumulative allocations, period reservations, amendments and observed invoice evidence. Failure, deletion and controller replacement do not replenish reservations. |
| Leaf submissions, execution revisions, continuations, events, request epochs and retained request identity | `shared_tasks.rs` owns transactional lifecycle changes and cross-kind request arbitration. `SharedTaskService` is the Python lifecycle port. |
| Composition instances, graph nodes, attempts, scopes, joins and completed-prefix checkpoints | `shared_composition.rs` owns graph transitions, original run IDs, ancestor scopes, exact continuation receipts and persistent frontier statistics. `SharedCompositionService` runs the same evaluator through an explicit shared journal. |
| Definition descriptors, implementation manifests, versions, current aliases and retirement checks | The shared definition registry resolves pinned identities. Dispatch rechecks installed implementation bytes. Missing code blocks dispatch while retained history stays inspectable. |
| Queue ownership, principal rotation, resource capacity and assignment occupancy | Shared task scheduler transactions enforce global, principal, workflow and named-resource limits. Composition leaves consume the same assignment pool. Slot release does not establish the outcome of an uncertain external effect. |
| Business, queue, pause, active and summed-worker clocks; deadline amendments | `shared_timing.rs` uses database time and retained clock history. Extension commands preserve creation and policy identity, require an expected revision and append an idempotent audit receipt. Unknown or expired work cannot be revived by extension. |
| Cursor Cloud jobs, exact agent/run bindings, launch and cancellation intents, observations and audit | `shared_jobs.rs` and the shared job-store port retain the original logical job identity. A lost launch response becomes an unresolved launch; replacement does not launch another cloud agent. |
| Federated peer versions and remote task operations | `shared_remote.rs` retains peer identity, original remote IDs, continuation/cancellation intents and operation evidence. Native admission is checked before outward effects; replayed command receipts do not authorise another send. |
| Provider circuit state and half-open probe ownership | Shared remote transactions use database time and one retained probe permit; process-local cooldown state cannot admit a competing probe. |
| Worker registrations, boot generations, placement, claims, dispatch fences and authoritative/late receipts | `shared_control.rs` and `SharedWorkerController` own placement and receipt authority. Replayed dispatch receipts return no permission to execute again. |
| Worker-local spool | The worker durably transports already-issued assignment/effect evidence until acknowledgement. It cannot grant fresh admission. Missing unacknowledged evidence remains uncertain. |
| Artefact content reservations, manifests, grants, provenance and retention links | `shared_artifacts.rs` records reservations before upload and publishes authorised metadata only after complete byte verification. `SharedArtifactStore` retains the same reference across replacement controllers. |
| Transfer sessions, acknowledged chunks, offsets, expiry and closure | `shared_transfers.rs` retains upload bytes and session identities transactionally. A replacement resumes the same acknowledged offset; published references still use verified S3 content. |
| Artefact bytes | `S3ArtifactContent` uses immutable derived keys, conditional PUT and full size/SHA-256 verification. A lost reply leaves a charged reservation; the same content can be verified and published under its original identity. |
| Operator drain, shared recovery, schema version and immutable command receipts | Native epoch-checked commands persist state and audit together. Explicit migrations preserve existing identities and reject incompatible layouts. Recovery retains uncertainty and allocation. |
| Bootstrap, gateway and secret bindings | Shared run metadata pins run/topology/credential references. Replacement must provide the configured implementation and external secrets; it cannot substitute new authority because a local cache is absent. |

The shared service does not create authoritative `state.sqlite`, `host.sqlite`,
`deployment.sqlite`, task/composition, scheduler, remote or job databases in its
controller cache. Controller-local directories hold reconstructible workspaces,
verified download snapshots, supervised process resources and lossy telemetry.
Acknowledged upload transfer identities, bytes and offsets remain in PostgreSQL;
download snapshots are disposable and each read rechecks native authorisation.
Grants, content reservations and retention references also remain shared. Worker spool files
are a separate evidence-delivery responsibility, not a controller database.

## Transaction and effect boundary

The native command channel validates the deployment, controller, epoch, lease,
schema and recovery/drain state inside the repository transaction. A singleton
authority row serialises those checks with mutations. Each command identity is
bound to its operation and canonical parameters. Repeating the same command
returns its retained receipt; changing its content conflicts. No generic retry
loop repeats external provider work after a database exception or lost reply.

Effect admission checks the current task/composition state, cancellation,
enclosing deadlines and all relevant allocation scopes before retaining the
native reservation and invocation intent. The external call happens after that
transaction. Completion records evidence under the original invocation. Losing
the admission reply, completion reply or controller does not create permission
for a fresh invocation. Earlier admitted calls can still finish after fencing;
unknown outcomes remain charged and require evidence through their original
recovery path.

Shared services distinguish a read query from a mutating command. Idle expiry
polls first select actual candidates with bounded keyset queries; only a real
candidate creates an expiry command receipt. Read-only inspection does not
manufacture durable history. Admission rechecks the condition in its own
transaction, so read selection is an efficiency measure rather than authority.

## Composition and scheduling

Each attempt has a stable run ID, immutable input, workflow identity and ancestor
scope list. A replacement controller requeues the same unfinished attempt under
its new owner generation and resumes that original run. A completed provider
invocation whose composition publication was interrupted is read from its shared
checkpoint; it is not invoked again. Unknown native invocations prevent automatic
recovery.

Completed-prefix checkpoints advance only across a verified completed child
with the same output. Frontier queries use a latest-attempt index and persistent
node/completion counters, returning unfinished attempts without reloading every
completed output. Detailed graph and revision queries remain available for
inspection. Parallel leaves use supervised child processes and respect both
plan/nested-plan parallelism and the shared capacity pool. Active elapsed time
counts an overlapping interval once; summed worker time counts that interval for
each active worker.

Local scheduler process ownership protects descendants on a particular host.
It is not a distributed lease. PostgreSQL owner/epoch checks determine which
controller can mutate workflow state or grant new work. The private lease
renewal subprocess preserves the single-threaded process-supervision boundary
and exits when its parent lifetime pipe closes.

## Verified objects and replacement

Object publication has three durable stages: reserve the immutable content
identity and quota; upload and verify complete bytes; publish the manifest and
grants in a current-epoch transaction. No database transaction is held across
network I/O. A controller fenced after its upload cannot publish metadata. Its
successor finds the original charged reservation, verifies the same object and
publishes the original reference. Reads authorise before download and again
before exposing the verified snapshot, so revocation during transfer is checked.

Shared artifact metadata and controller failover are exercised together by:

```sh
PYTHONPATH=host MAS_SHARED_BIN=rust-protocol/target/debug/mas-shared \
  python scripts/check-object-storage.py --local-rustfs /path/to/rustfs \
  --shared-authority
```

The command also requires `MAS_TEST_POSTGRES_DSN`. It creates a random disposable
bucket and isolated database schema, and tests real S3 traffic with PostgreSQL
metadata: immutable publication, retained consumer references, discarded accepted
PUT replies, fencing between PUT and publication, replacement recovery, grant
revocation and object tampering. It makes no external model/provider calls.
The PostgreSQL workflow installs a hash-verified RustFS release and hash-locked
SDK dependencies before running this gate. The separate content/HTTP profile
also checks authenticated multi-chunk transfer against a real local S3 server.

## Handover, archives and qualification

A live controller can explicitly hand over to a successor; takeover after lease
expiry uses a compare-and-set against the exact prior binding. Neither path
recreates run/request/attempt identities or resets cumulative charges. Both old
reads and writes are fenced. The same-database replacement tests cover lost
admission/completion replies, unresolved effects, queued and paused work, exact
continuations, parallel compositions, controller renewal and shared worker and
remote receipts.

[Shared recovery](shared-recovery.md) documents final-source sealing, native
PostgreSQL archives, isolated audit restores, complete table/sequence inventory
verification, retained object verification and externally signed fencing proof.
A copied backup is not automatically writable authority. Controlled activation
starts drained after verifying the exact final source state and proof. Missing
later acknowledgements or unknown effects cannot be repaired by resetting IDs
or allocations.

The CI PostgreSQL and RustFS services qualify executable repository and transport
contracts. A production availability claim additionally needs evidence for the
chosen database/storage replication, failure domains, promotion manager, key
recovery and fencing integration. That deployment must specify acknowledgement
preservation, RPO/RTO and behaviour when required replicas are unavailable.
Automatic database promotion is not performed by the controller lease API.
Cross-machine partitions or an independently writable old database require
external fencing; a row lock in one database cannot fence another database.
