# Bounded shared command receipts

The PostgreSQL profile retains each successful command identity, request digest
and receipt. Repeating that identity returns its exact receipt until an authorised
terminal lifecycle archive replaces its payload with an audited tombstone. A
different request under the identity is always rejected. Receipt identities are
never deleted to make space. Capacity exhaustion changes none of these rules.
See [shared payload archival](shared-payload-archival.md) for the explicit
`command_result_archived` response after authorised payload removal.

Schema 8 introduces three independently bounded receipt pools. Initial defaults
are 1,000,000 ordinary commands, 20,000 recovery commands and 1,024 administration
commands. A trusted deployment can supply all three positive integer limits to
`SharedAuthority.initialise(command_capacity=...)`. Migration accounts for every
existing receipt as an ordinary receipt and rejects a limit below the retained
count. Reopening the profile preserves its policy; supplying a different initial
policy is rejected.

Every new command reserves one slot in its transaction, before changing domain
state. A rejected or rolled-back command consumes no slot. Exact command replay
consumes no additional slot. The authority row serializes the check across
controllers and concurrent requests. Read queries, lease renewal and explicit
epoch handover do not create command receipts. Native protocol frame acceptance,
receive/ack and reservation retain their independent protocol admission, message,
byte and call limits. They do not create host command receipts and cannot dispatch
a provider effect without the atomic host invocation admission.

Ordinary capacity covers fresh task/run/effect admissions, definitions and
other workload mutations. The recovery pool covers retained-run claims and
release, effect observations and resolution, cancellation, completion,
checkpointing, expiration, archival, drain and accounting observations. These
commands still enforce their ordinary ownership, state, cancellation and epoch
checks. Exhausting ordinary capacity cannot consume the reserved recovery pool.
If recovery capacity also fills, further recovery mutations fail closed while
inspection and exact receipt replay remain available.

`command_capacity.status` reports each pool's limit, used count and remaining
count, the initial policy and the amendment revision. Idle scheduler loops read
bounded expiry/claim candidates before sending a mutation. Frozen scope
configuration uses a stable command identity. A mutation that happens to be a
no-op still retains its receipt: omitting that receipt could allow the same
identity to cause a different change later.

Trusted administration can use `command_capacity.amend` with `request_key`,
`expected_revision`, `normal_limit`, `recovery_limit`, `operator` and nonempty
`evidence`. It increases at least one workload/recovery limit and never reduces
either. The request has a permanent digest and receipt, with revision conflict
checks. Its receipt comes from the separate administration pool, so an exhausted
workload cannot prevent an authorised increase. The administration pool itself
is fixed and finite; exhausting it requires an explicitly reviewed future schema
migration. No automatic limit reset, identity recycling or receipt pruning occurs.

The actual PostgreSQL gate exercises concurrent admission at the limit,
observation and replay after workload exhaustion, amendment rollback and the
finite administration limit. These are bounded reference checks, not a claim
that the defaults are appropriate for every workload. Operators should monitor
remaining capacity and provision it from their measured command rate and
required retained history.
