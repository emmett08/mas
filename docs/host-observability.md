# Host events and request diagnostics

The authenticated service offers `POST /service/events_page` with `task_id`,
optional `after` (default zero) and `limit` (default 100, maximum 1,000). The
response contains `events`, `next_after` and `has_more`. Persist `next_after`
only after processing the returned events. Pages are byte-bounded; `has_more`
remains true when the byte limit is reached before the requested event count.
An empty page preserves the supplied cursor, allowing a later poll to observe
new events. Cursors identify append-only positions, not historical snapshots.

The same operation is available as the MCP tool `mas.events_page` and
`TaskServiceClient.events_page`. MCP pages reserve space for their smaller
protocol envelope. A single event too large for that envelope produces
`page_item_too_large`; the HTTP service has a larger response allowance. The
existing `events` operation retains its list response for compatibility.

```python
page = client.events_page(principal, task_id, after=saved_cursor, limit=100)
for event in page["events"]:
    process(event)
saved_cursor = page["next_after"]
```

`TaskServiceClient.iter_events` follows pages until caught up, subject to an
explicit `max_pages` bound. It rejects non-advancing or malformed remote pages
and makes no automatic retry after a transport failure. Application consumers
choose their own durable processing and cursor-commit policy.

Every handled HTTP request emits one JSON record on stderr. The
`mas-request-log/1` record includes a generated request identifier, fixed route
name, response status, fixed diagnostic code, timestamp and duration in
milliseconds. The response exposes the same identifier as `X-Request-ID`.
Request bodies, request targets, bearer tokens, principal names and exception
messages are excluded. A custom `request_log` callable may send these records
to an existing collector. Logging failure does not change admission or task
state. Local readiness checks remain separate from provider-progress claims.

Services supporting composition also expose `task`, `discover` and `recover`
through the HTTP client and corresponding MCP tools. Recovery requires the
`mas:update` scope and only resumes previously reconciled work; it does not
authorise repeating an uncertain external effect. A service lacking an optional
capability returns `unsupported_capability` (HTTP 501).

The service persists accepted protocol progress while the runner is active.
A journal outbox republishes undelivered events after restart; service event IDs
deduplicate delivery by run and message identity. This reports protocol progress,
not token streaming or proof that an external effect has stopped.

`GET /console` serves a small operations page using the same authorised APIs for
discovery, submission, listing, inspection, leaf detail, human continuation,
cancellation, recovery after resolution, and resumable event polling. Configure
`allowed_origins` with the exact externally served origin, including a non-default
port, for example `https://mas.example.org`. The default empty origin allowlist
rejects browser POST requests, including same-origin requests. `allowed_hosts`
and `public_url` must also match the deployment. A reverse proxy must preserve
these checks and terminate HTTPS.

The static page contains no task data. Enter an existing service token to access
authorised records. The token is held only in memory and is cleared on
disconnect or navigation; disconnect aborts pending requests and clears displayed
data. A session generation check discards responses from an earlier connection.
The console rejects non-integer or unsafe JavaScript numeric values before
submission; use the Python client for integers outside the browser's exact
range and typed artefacts for scientific values. Evidence-backed resolution
uses the operator recovery service/CLI, followed by the console's Recover action.

`POST /service/events_wait` is a bounded long-poll subscription: it accepts
`task_id`, `after`, `limit` and `wait_ms` (0–25,000), returning the normal event
page as soon as progress arrives. `TaskServiceClient.subscribe_events` exposes
an explicitly bounded iterator. Keep the HTTP timeout longer than the wait.
The server reserves ordinary connection slots for management and rechecks task
visibility on every read. Expired cursors require resynchronisation; a transport
failure ends the iterator so its caller can resume from its last processed ID.

Optional webhooks use `mas-host events` with trusted target configuration.
The target is selected by a registered name; request bodies cannot choose URLs.
The delivery process stages one immutable page before sending it and advances
its cursor only after HTTP 200 with `{"ack": "<delivery_id>"}`. A lost response
repeats the same bytes and ID after durable backoff. The receiver must deduplicate
that ID. Pending pages and the last acknowledgement survive restart. Reads do
not wait for network delivery, and source grant revocation blocks future sends.
Already received bytes cannot be revoked. An expired source cursor stops the
subscription with `resync_required`, rather than silently skipping events.

The target file has format `mas-webhooks/1` and a `targets` mapping. Each entry
contains `endpoint` and `token_file`, with optional `timeout_seconds`,
`allow_loopback_http` (local testing only) and explicit HTTP `transport` policy.
Endpoint identity is immutable; credentials can rotate in the private file.

```sh
mas-host events --config bridge.json --state-dir /private/mas \
  --targets webhooks.json --principal alice --subscription review-progress \
  subscribe --task-id task-EXISTING --target review-system
mas-host events --config bridge.json --state-dir /private/mas \
  --targets webhooks.json --principal alice --subscription review-progress deliver
```

Run `deliver` from a supervised timer; it sends at most one retained page.
`inspect` and `stop` use the same arguments. The configured maximum is 1,000
subscriptions, one page per pending subscription, and 100 events per page by
default. Audit-only restored deployments cannot deliver or mutate subscriptions.
Distributed tracing remains separate from this event-delivery contract.

## Read-only preflight and effective configuration

```sh
mas-host admin --state-dir /private/mas doctor
mas-host admin --state-dir /private/mas preflight --config bridge.json
mas-host admin --state-dir /private/mas effective-config --config bridge.json
```

`doctor` reports the Python and SQLite versions, installed optional package
versions, executable presence and SHA-256 identities, recognised root SQLite
schemas, quick-check results, deployment budgets and storage observations.
`preflight` performs the same checks with a required service configuration.
Both return JSON and a nonzero exit status when a check fails. They are advisory
snapshots; they do not reserve capacity or guarantee that a later submission
will be admitted.

`effective-config` validates declared limits, scheduler settings, grants,
authentication and artefact transfer configuration. Its summary includes the
effective default limits, scheduler limits and rule counts, grant counts, the
authentication type and whether a workflow factory was declared. Static
credential files and OAuth client-secret files must be locally readable under
the existing private-file checks. No token is verified against a network server.
Names, endpoints, credential contents and factory targets are excluded from the
summary.

These commands never import or execute the declared workflow factory, run a
configured binary, call a provider or migrate a database. Binary versions are
therefore reported as unverified (`version_status: not_executed`); the digest is
the observed file identity. Factory registrations, provider connectivity and
provider-specific credentials remain runtime checks. Known legacy root schemas
are inspected in place and flagged `migration_needed`; an unsupported existing
schema or failed integrity check closes the diagnostic result. An absent store
is reported as absent and is not created. An uninitialised deployment has no
open admission budget.

WAL-mode databases and retained WAL/shared-memory sidecars are rejected before
opening SQLite: even SQLite's read-only mode can create or update shared memory.
Inspect a quiescent backup checkpointed into rollback-journal mode instead.
Diagnostics do not change journal mode, checkpoint the source or silently ignore
committed WAL records. Concurrent journal-mode changes or file replacement are
outside this advisory snapshot's guarantee.

## Bounded operational metrics

```sh
mas-host admin --state-dir /private/mas metrics
mas-host admin --state-dir /private/mas metrics --format prometheus
```

JSON is the default. Prometheus text contains a fixed set of metric names and
labels: task, composition, bridge and remote-worker state counts; oldest queued
and unknown record ages; lifetime call and reservation budgets; storage bytes;
retained scheduler slots; registered/draining worker counts; and provider circuit
counts. Principal, workflow, resource, call and task identities never become
metric labels. Request bodies, credentials and exception strings are excluded.

Age measures time since creation of the oldest record currently in that state,
not the exact time of its latest transition. Remote pending work is the remote
queue. Retained scheduler rows are observations, not a proof that their processes
are still alive. An expired worker lease does not resolve an external effect.
Circuit `not_open` means no active cooldown or probe is recorded; it does not
establish provider health.

Every recognised root store is checked before publishing metrics. Absent stores
produce zeros. Malformed existing state produces only a failed JSON diagnostic
or `mas_diagnostics_ok 0` in Prometheus, with a nonzero exit status; partial counts
are not presented as an empty queue. Storage inspection is bounded to 100,000
entries and rejects symlinks and special files. SQL observation has a five-second
progress deadline per opened store. Snapshots across stores are not globally
transactional. A collector can run the CLI under the deployment's existing local
operator account; this increment does not expose an unauthenticated metrics HTTP
endpoint.

These aggregates cover the deployment's root repositories, not an inventory or
integrity check of every per-run invocation journal.


## Correlated latency observations

The deployment-admin `telemetry` operation is available through the operator API,
MCP and portable client (`mas-client ENDPOINT operator --principal OPERATOR
--token-file PRIVATE_TOKEN --data REQUEST.json`, with request JSON
`{"operation":"telemetry","payload":{}}`). It returns `mas-telemetry/1`: fixed-stage latency histograms and the
last 20 retained spans. Stages distinguish HTTP handling, queue wait, dispatch,
provider calls, Rust protocol commands and reconciliation queries. The callback
outcomes `returned` and `raised` describe control flow, not effect certainty or
provider billing. The durable journal remains authoritative for those facts.

Request, task, run, assignment and invocation IDs correlate the observations when
available. A run ID determines its trace ID across processes. Identifiers never
become metric labels; labels are the fixed stage and outcome enums. No prompts,
outputs, credentials, principal names or exception messages enter these records.
HTTP response request IDs continue to link requests to safe structured logs.

Observation storage is a deliberately lossy, controller-local window, bounded to
two 1 MiB files. A shared-authority controller uses its disposable cache directory;
these files are neither PostgreSQL authority nor a distributed trace collector.
Local physical storage accounting includes the bounded observation files, so
operators must allow up to 2 MiB plus lock files within `max_state_bytes`.
Each controller's API reports its own retained window. Lock contention, full disks
or a failed recorder drop observations without changing admission or task results.
Audit-only copies are readable without recording new spans. Corrupt observations
produce `telemetry_unavailable`, never fabricated zero measurements.

Histograms describe retained samples rather than lifetime totals or an SLO. Queue
samples measure observed waits before local dispatch, and dispatch includes worker
execution and cleanup. Use durable task clocks for deadline enforcement, the
operational metrics for unknown-effect age and allocation headroom, and provider
charge observations for reconciliation. Exporters can poll the authenticated
operator snapshot; retained observations do not promise exactly-once delivery.
