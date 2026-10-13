# Operating a bounded single-host deployment

The supported operational boundary is one private state directory on a local filesystem, shared by trusted hosts and a Rust/SQLite broker. The host journal records workflow checkpoints; the deployment ledger admits work against lifetime quotas. Linux file locks exclude concurrent execution of the same run and coordinate backups and retention. Container CPU, memory and PID limits bound the supplied Compose services. Adapters remain trusted application code.

## Start and inspect

Run the existing demonstration command, then inspect the shared deployment:

```sh
docker compose up --build --abort-on-container-exit --exit-code-from mesh
docker compose run --rm mesh admin --state-dir /state health
docker compose run --rm mesh admin --state-dir /state runs
```

The `runs` command supplies retained directory names. Prefix one with `/state/` for inspection:

```sh
docker compose run --rm mesh admin inspect /state/RUN_DIRECTORY
```

The same commands work outside containers as `python -m mas_host admin ...` with `mas` and `mas-gateway` on `PATH`. `MAS_BIN` selects the Rust administrative binary. `MAS_STATE_DIR` supplies the default state directory. State and backup directories must have mode `0700`; copied files have mode `0600`. Reports and backups may contain application data and agent capabilities. Keep them private.

## Freeze a deployment policy

Before the first run, initialise a policy with a JSON file:

```json
{
  "max_runs": 1000,
  "max_calls": 10000,
  "max_reserved_microusd": 100000000,
  "min_free_bytes": 67108864,
  "max_state_bytes": 1073741824,
  "per_call_reservation_microusd": 100000
}
```

```sh
python -m mas_host admin --state-dir /absolute/private/state init --policy policy.json
```

These are also the default limits. Each new run and each provider invocation must be admitted by the same SQLite deployment ledger. Concurrent hosts share those transactions. Charges survive process restart and retention deletion. Reusing an existing identity is idempotent; changing its expenditure allocation is rejected. Mock/local calls may reserve zero expenditure while still consuming a call admission.

`max_reserved_microusd` measures **reserved allocation**, in millionths of a US dollar. A default invocation allocates 100,000 microUSD ($0.10). This is an operator allowance, not a verified price estimate or the provider's invoice. An unknown outcome retains its full allocation. Configure suitable per-call reservations and a provider-account spending limit before a live run. The runtime cannot prevent spending through an unrelated client, external account or another state directory.

Policy values are immutable after initialisation. Changing an environment variable or restarting a container cannot replenish the ledger. An exhausted deployment stops admitting work. A new deployment is an explicit new lifetime budget, so create one only after reconciling the old deployment and approving its replacement budget. Removing a ledger while retaining run databases fails closed. Deleting the entire deployment is an administrative destruction of its audit and cannot be detected by an independent replacement directory.

Disk thresholds are checked before admissions. They close admission when free space falls below `min_free_bytes` or retained state reaches `max_state_bytes`. Writes already admitted can cross the threshold; use a filesystem quota for a strict storage ceiling. Watch `free_bytes`, `state_bytes`, usage and admission state through the read-only `health` command. It never creates a deployment or migrates its database. Backups occupy separate storage and need their own retention policy.

## Reconcile expiry independently

A host crash must not leave task expiry dependent on the next adapter call. The watcher calls Rust reconciliation independently:

```sh
python -m mas_host admin --state-dir /absolute/private/state reconcile
python -m mas_host admin --state-dir /absolute/private/state watch --interval 5
```

A single run can be reconciled with `reconcile --run-dir /absolute/private/state/RUN_DIRECTORY`. The watcher prints one JSON record per scan, including disk/quota health, handles SIGINT/SIGTERM, and bounds each Rust command to ten seconds. `--iterations 3` performs a finite acceptance check and returns a nonzero exit code if any scan fails. Only one watcher may own a state directory at a time.

A separate readiness command checks that the continuous watcher has recently completed a successful scan and that the same process remains alive:

```sh
python -m mas_host admin --state-dir /absolute/private/state reconciler-health --max-age-seconds 30
```

Readiness fails for missing, failed, stale or stopped watchers. The status records the Linux boot identity, process start ticks and the state directory identity, so a reused PID or copied heartbeat cannot establish readiness. It uses monotonic elapsed time. Admission quotas remain a separate health dimension: an exhausted deployment can still have a healthy reconciler. A finite watcher stops after its checks and therefore is not subsequently ready as a continuous service. A scan of many slow run databases can exceed the freshness threshold; choose that threshold according to the required reconciliation latency. Reconciliation closes expired protocol tasks; it does not establish that a remote provider stopped or that an external effect was absent.

The production Compose overlay supplies a separate reconciler service. Its process can continue reconciling retained runs after a host exits. Its Docker health check uses `reconciler-health`, with a 30-second freshness threshold, rather than treating a running process as proof of successful reconciliation. Docker marks a failed service unhealthy; configure operational alerting for that status (the restart policy alone does not restart unhealthy but running processes). Stop this service as well as hosts for predictable operator maintenance; snapshot locking rejects a busy deployment rather than copying inconsistent databases.

## Recover an interrupted workflow

Inspect the run before replaying it. A `prepared` invocation has no recorded provider entry; `executing` or `unknown` requires reconciliation with the external system. A completed checkpoint supplies its saved result on replay. Workflow version and call order must match the frozen run.

For a custom host, reopen its original run with the same trusted adapters and workflow version:

```python
with Runner.resume(run_directory, adapters, workflow_version="invoice-v1") as runner:
    result = runner.request("checker", "planner", "invoice.plan", original_input)
```

Preserve the original top-level input. Changing it is rejected. Resume preserves the original protocol deadline and budgets; it cannot reopen an expired task. See [host-runner.md](host-runner.md) for the adapter contract and execution deadlines.

When external evidence establishes a result, record it explicitly:

```sh
python -m mas_host admin resolve /absolute/private/state/RUN_DIRECTORY CALL_ID \
  --decision completed --evidence provider-reconciliation.json --output verified-output.json
```

The evidence file must be a nonempty JSON object documenting the basis for the decision; the output file must contain the verified output object. Keep keys, credentials and sensitive raw exception text out of evidence. The command requires the run to be stopped and exclusively locked.

If the external system establishes that no effect occurred, the operator may instead authorise a new, independently charged attempt:

```sh
python -m mas_host admin resolve /absolute/private/state/RUN_DIRECTORY CALL_ID \
  --decision confirmed_no_effect --evidence provider-reconciliation.json
```

The decision is retained in an append-only resolution history. Original Rust invocation evidence remains unchanged. A timeout alone does not establish `confirmed_no_effect`. A provider response ID can help with reconciliation, but provider-specific retention, retrieval and side-effect semantics determine what it proves. The included OpenAI adapter disables automatic SDK retries; the operator must establish the outcome of uncertain requests before any fresh invocation.

## Back up and restore

Stop active runners. Backups take exclusive deployment and run locks, then use SQLite's backup API for each database, copy private configuration, and produce a SHA-256 manifest. A busy run causes the operation to fail. Use an absent or empty destination outside the state tree:

```sh
python -m mas_host admin --state-dir /absolute/private/state backup /absolute/private/backup-2026-10-13
python -m mas_host admin --state-dir /absolute/private/restored restore /absolute/private/backup-2026-10-13
```

Restoration verifies the complete file set, content hashes and SQLite integrity before writing into an empty destination. Symlinks and unsafe paths are rejected. Hashes detect corruption; authenticate the backup's origin separately. The snapshot preserves the deployment identity, quotas, private capabilities, workflow checkpoints and unresolved calls. Ephemeral reconciler status is excluded.

**Restored deployments are audit-only.** Their persistent ledger blocks every new call admission and every new run. Inspection, expiry reconciliation and replay of already completed root results remain available. This prevents an old snapshot from regaining budget consumed after the backup or becoming a second active copy of the same spending authority. This release deliberately has no command to reactivate a restored spending ledger. Reconcile external effects and account usage, stop the original deployment, and initialise a separately budgeted deployment for any authorised new work. Unknown work is never automatically replayed from a backup.

The original deployment remains unchanged by backup or restore. Treat backups as private operational evidence and periodically run the restoration acceptance check on separate storage.

## Repository formats and upgrades

The host-side repositories have independent application identities and versions:

| Repository | Application identity | Current version | Compatible previous format |
| --- | --- | --- | --- |
| Deployment ledger | `MASD` (`0x4d415344`) | 2 | Unidentified version 1 ledger from the preceding release |
| Workflow journal | `MASJ` (`0x4d41534a`) | 1 | Unidentified version 0 journal from the preceding release |

A writable open validates the exact declared tables, constraints and indices, and the saved metadata, before transactionally migrating a recognised previous format. Migration preserves deployment identity, policy, charges, checkpoints and resolutions. Unknown application identities, versions, extra objects or changed layouts are rejected before database mutation. Read-only health and inspection recognise supported previous formats without migrating them. Neither operation creates missing state. The Rust protocol repository retains its own format and migration rules.

Take a quiescent backup before an upgrade. A newer host-side format must never be opened for writing by an older release. Follow [the release procedure](releases.md); an audit-only restore cannot regain spending authority.

## Retain evidence and remove completed runs

Retention is explicit and defaults to a preview:

```sh
python -m mas_host admin --state-dir /absolute/private/state prune --older-than-seconds 2592000
python -m mas_host admin --state-dir /absolute/private/state prune --older-than-seconds 2592000 --apply
```

A run is eligible only when its retention age since the latest state-file change is reached, no host holds its lock, every protocol task is terminal, and the host journal establishes the outcome of every invocation. Unknown or prepared invocations block deletion. Deletion records a permanent deployment-ledger tombstone before removing files. Lifetime run, call and expenditure charges are never refunded. An interrupted deletion may leave files with a recorded tombstone; retain that ledger entry and remove remaining files only after operator inspection.

These controls support a bounded single-host service. They do not provide multi-host consensus, hostile-code isolation, automatic provider refund accounting, or a highly available distributed database. The acceptance suite exercises process interruption, quotas, disk thresholds, backup/restore, sustained local load and explicit live-provider acceptance separately.
