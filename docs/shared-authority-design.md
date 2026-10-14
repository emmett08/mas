# Shared authority and high-availability boundary

W16 is **not implemented**. The supported storage profile has one controller
host. Concurrent assignment processes and authenticated remote workers use
that controller's admission records, journals and SQLite databases. Moving
only the pending queue to PostgreSQL would leave essential authority on the
original host and would not support safe controller replacement.

This document records the implementation boundary and the ordered work needed
to cross it. It does not advertise a PostgreSQL backend, object-storage backend,
automatic controller promotion or a measured recovery objective.

## Complete state inventory

| Repository or resource | Current authoritative content | Shared-authority requirement |
| --- | --- | --- |
| Per-run `state.sqlite`, Rust `Store` | Frozen run topology and limits; message identities, delivery and acknowledgements; task transitions; call reservations/evidence; gateway admission attempts | Shared Rust-owned protocol transactions, stable IDs and epoch-checked writes, including rejected-but-charged gateway attempts |
| `deployment.sqlite` | Deployment identity/policy, admitted runs/calls, lifetime reserved allocations, purge tombstones and budget amendments | Shared admission ledger; no budget replenishment through deletion, restore or promotion |
| Per-run `host.sqlite` | Metadata, deterministic step inputs/checkpoints, invocation state/output/evidence and operator resolutions | Shared invocation intent and checkpoint repository with the same replay and uncertainty rules |
| `task-service.sqlite` | Leaf definitions, tasks, execution revisions, submission/continuation request identities, events and archive tombstones | Shared lifecycle and scheduling intents; atomic state/event transitions |
| `composition.sqlite` | Plan instances, nodes, attempts, ancestor scopes, allocations, continuations, events and retained archive identities | Shared graph/checkpoint/ancestor accounting and continuation deduplication |
| `definition-registry.sqlite` | Immutable descriptors and current aliases | Shared version resolution; unavailable code blocks execution while history remains readable |
| `scheduler-capacity.sqlite` plus capacity locks | Frozen concurrency policy, occupied assignments and principal rotation | Transactional claims and limits with explicit assignment/controller generations; elapsed expiry alone cannot establish effect completion |
| `cursor-jobs.sqlite` | Cloud job binding, peer/run identity, cancellation intent, observations and audit | Shared remote intent and reconciliation history |
| `remote-tasks.sqlite` | Federated peer identity, submissions, continuation/cancellation intent and observations | Shared exact peer mappings and unresolved operations |
| `remote-workers.sqlite` | Worker identities/boot generations, placement, claims, dispatch fences, authoritative and late receipts | Shared placement and receipt authority, bound to controller epochs and original invocations |
| Worker-local `worker-spool.sqlite` | Already-issued assignment state, effect observations and controller acknowledgements | Durable evidence transport until acknowledgement; never a source of new admission. Missing unacknowledged evidence leaves uncertainty |
| Per-run bootstrap and gateway files | Bootstrap stage, original identities, credential bindings and frozen gateway configuration | Shared bootstrap intent and secret references; replacement must not silently provision a different identity |
| Artefact manifests, grants, retention references and transfer records | Authorised reference identity, provenance, visibility, retained consumers and partially completed transfers | Transactional shared metadata and verified object publication, with coordinated retention/backup |
| Artefact bytes and workspace/context manifests | Content hashes, immutable inputs/context, configured executable dependencies and bounded returned outputs | Verified durable objects where required; reconstructible workspaces remain local caches |
| Deployment/run/task/capacity/dispatcher/workspace file locks | Single-machine ownership held through process and descendant cleanup | Explicit shared ownership/fencing. File descriptors and local `flock` do not become distributed locks by placing files on another host |

Readiness files, process IDs and open IPC channels are observations or local
resources. They must not be imported as replacement authority. The controller
backup currently covers its state directory; a remote worker's separate spool
has its own durability boundary.

## Implementation sequence

1. **Extract transaction-shaped repositories while retaining SQLite.**
   Rust `Store` currently owns a concrete `rusqlite::Connection`; `tasks.rs`
   consumes a concrete `rusqlite::Transaction`, and `Gateway`/`HostSession` own
   the concrete store. Extract operations that preserve whole invariants:
   send-plus-task-transition, receive, acknowledgement, call reservation,
   evidence recording, gateway attempt charge, expiry and inspection. Host
   repositories similarly need submit, continuation, claim, allocation,
   checkpoint, receipt and archive operations. A generic CRUD interface that
   exposes separate unchecked writes would weaken the existing boundary.
   Run the same identity, quota, rollback and lost-reply traces against the
   existing implementation before adding another backend.

2. **Specify the shared command and epoch contract.**
   Every mutating command carries deployment identity, controller epoch,
   original logical command/invocation identity and canonical content digest.
   The transaction checks the current epoch and the expected record revision;
   repeated identity/content returns the prior receipt and changed content
   conflicts. Leadership selection and renewal must use the same durable
   authority as these checks. A lease or advisory lock without a write-time
   epoch predicate is insufficient to reject a stale controller.

3. **Implement PostgreSQL admission and host repositories together.**
   Keep the Rust gateway authoritative for protocol acceptance. Prefer one
   Rust-owned admission command that transactionally checks the epoch,
   deployment/ancestor limits and unique allocation identity, then records
   the native call reservation and durable invocation intent before returning
   permission to dispatch. Where an operation necessarily spans authority
   transactions, record an idempotent intent first, then the downstream
   receipt, and specify recovery for every interruption between them. Never
   infer permission from an absent reply or release an allocation because a
   later step failed. Use transaction retries only for database operations
   known to have rolled back; external provider dispatch is outside that retry
   loop. A lost COMMIT reply requires lookup by the original command identity.

4. **Replace local ownership and bootstrap assumptions.**
   Convert run/task claims and shared capacity to bounded, generation-aware
   records. Reconstruct a replacement controller from retained bootstrap,
   definitions and secret references; do not copy live lock files. Revalidate
   an assignment's epoch before issuing any new grant. Earlier issued calls
   can still have external effects after fencing and remain charged and
   outstanding until reliable evidence resolves them. Late observations may
   be retained without overwriting a newer authoritative decision.

5. **Add verified object publication and recovery.**
   Upload immutable content and verify its size/hash before publishing the
   authorised metadata reference. Retain stable object/version identifiers,
   grants and consumer references transactionally; collect orphaned uploads
   without deleting live or unresolved references. Define backup manifests
   linking the database recovery point to required object versions and key
   material. Validate restored content before activation.

6. **Qualify replicated promotion and disaster recovery.**
   Test the actual PostgreSQL replication/promotion service and failure domains.
   Fence the previous database/controller authority before reopening admission.
   Automatic promotion with open admission requires preservation of every
   acknowledged request identity, allocation and effect record under the
   declared failure model. If a recovery point can omit these records, keep
   the replacement audit-only until complete recovery or conservative
   reconciliation establishes usable authority and remaining allocation.

PostgreSQL documents that streaming replication is asynchronous by default.
With configured synchronous standbys, `synchronous_commit=on` waits for durable
standby WAL; `remote_write` has a weaker durability guarantee and `remote_apply`
also waits for replay visibility. These settings do not choose a safe promotion
candidate or fence an old primary. The deployment must qualify that complete
procedure, including unavailable replicas and lost commit acknowledgements.
See [synchronous replication](https://www.postgresql.org/docs/current/warm-standby.html#SYNCHRONOUS-REPLICATION)
and [WAL configuration](https://www.postgresql.org/docs/current/runtime-config-wal.html).

PostgreSQL serializable transactions can require retrying the whole database
transaction. The retry boundary must preserve command identity and exclude
external effects; see [transaction isolation](https://www.postgresql.org/docs/current/transaction-iso.html).
Point-in-time recovery additionally requires a usable base backup and retained
WAL, with a separate activation policy; see
[continuous archiving](https://www.postgresql.org/docs/current/continuous-archiving.html).

## Deployment facts and acceptance still required

The implementation session has no configured PostgreSQL server/replication
cluster, object-storage account or accepted multi-host recovery profile. The
following facts remain unspecified:

- PostgreSQL version/service, promotion manager, database roles and TLS/secret
  delivery; permitted administrative and application actions.
- Failure domains, eligible promotion targets, synchronous acknowledgement
  policy and behavior when the required replicas are unavailable.
- Workload and bounds: run/message/call rates, concurrent principals and
  workers, transaction sizes, retained history and artefact sizes.
- Required availability and recovery-time objective, recovery-point objective
  for identity/admission/effects versus other data, and acceptable admission
  downtime during partitions or evidence reconciliation.
- Object-store conditional-write/versioning/consistency behavior, retention,
  encryption/key recovery and backup ownership.
- Upgrade compatibility, deployment drain, stale credential revocation and
  operator authority to fence/promote or perform conservative reconciliation.

Repository conformance, SQLite migrations, finite fault models and local
multi-process cleanup tests can run now. PostgreSQL isolation, serialization
retries, database connection loss and object publication require real backend
fixtures. Multi-machine partitions, old-controller reappearance, acknowledged
commit preservation, replica promotion and measured RPO/RTO require the actual
deployment profile. Passing the first group does not satisfy the latter gates.

The required fault matrix cuts each identity/admission/dispatch/checkpoint and
receipt/acknowledgement boundary, tests competing stale/current epochs and
partial object publication, and includes an old backup that omits later charges.
No failover scenario may turn an unknown effect into a fresh invocation or
recover historical allocation by forgetting it.
