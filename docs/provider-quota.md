# Shared provider request allowances

The optional `mas-provider-circuit/2` profile adds an authoritative fixed-window
request allowance to the existing PostgreSQL provider circuit. Version one is
unchanged. A version-two policy has exactly these fields:

```json
{"format":"mas-provider-circuit/2","resource_key":"github/installation/example",
 "failure_threshold":2,"open_seconds":10,"half_open_seconds":5,
 "window_ms":3600000,"max_requests":5000}
```

`window_ms` is 1..86400000 and `max_requests` is 1..10000. The database clock
selects an epoch-aligned window. This counts requests, not variable GraphQL cost,
GitHub's independently managed bucket or Kubernetes API server usage. The
operator must bind every replica and credential which shares an actual provider
allowance to the same `resource_key`. Distinct provider accounts need distinct
keys. A conflicting policy for a retained key is rejected; configuration cannot
reset its original permits or circuit obligations.

First install service repositories, then explicitly call
`PostgresRepository::install_provider_quota_index()`. The `mas-shared`
`initialise`/`install_host` operations include this upgrade. Installation compares
the stored layout with the current layout before DDL, validates the exact btree
index key structure, creates the index if absent and records the new layout in
one authority transaction. Ordinary admission performs no DDL. Version two
requires the qualified `(resource,created_ms)` index; version one does not.

The transaction for `circuit.admit` checks the current controller, lease, drain,
command capacity, exact policy, circuit state and remaining request credit. It
records one retained permit before the caller can issue HTTP. Unknown results,
local CPU release, an aborted invocation and circuit observations do not refund
this credit. It expires only when its database window ends. The original effect
identity remains retained afterwards. A backwards clock relative to a retained
permit stops new credit admission conservatively. These permits and receipts
are not deleted by this feature.

Rust consumers use `admit_provider_request(command_id,policy,call_id)` and match
`ProviderAdmission::FreshDispatch`. Its non-Clone, non-serialisable permit can be
consumed with `into_call_id()`. `Retained`, `Deferred` and `Uncertain` never grant
execution. A raw controller client must require **both** `replayed:false` on the
command envelope and `fresh:true` on the new receipt. An exact replay can contain
a historical `fresh:true` receipt and is never permission. A duplicate call under
a new command returns `fresh:false`. The provider permit supplements the
original native invocation permission; neither a provider receipt nor a status
query authorises another effect or retries an uncertain mutation.

For a safely parsed `Retry-After` or provider reset, the controller records
`circuit.defer` with `{policy,call_id,not_before_ms}`. The call must already have
an exact retained provider permit. The database keeps the maximum existing and
requested time. A later shorter hint, probe success or late observation cannot
shorten it. `circuit.status` reports a bounded safe snapshot and
`execution_permission:false`; an expired business deadline belongs to the
native/task owner and never becomes retry permission. Applications must release
local worker occupancy while deferring and reconcile uncertain mutations using
their original host and provider identities.

The actual PostgreSQL tests cover concurrent final-credit admission, retained
and lost replies, abort without credit return, immutable policy, monotone
deferral, controlled fixture ageing, conservative clock regression and explicit
layout-guarded installation. They run in the PostgreSQL workflow; compiling them
without an actual backend is not qualification. `formal/provider-quota.pml`
checks one bounded fixed-window abstraction, reply loss, duplicate identities
and monotone deferral, with four mechanism mutations and two positive witnesses.
It does not prove PostgreSQL refinement, clock synchronisation, provider bucket
agreement, network fairness or performance at the reference workload targets.
