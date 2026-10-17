# MAS: AI agent communication in Rust

The [assessment contract](docs/assessment-contract.md) binds criterion-level
findings to exact candidates, evaluators, rubrics, evidence and revision feedback
using existing message envelopes. Its [SPIN model](formal/assessment-revision-notes.md)
checks identity, authority, immutable history and bounded feedback handling.

The [bridge guide](docs/bridges.md) covers authenticated MCP and A2A endpoints,
durable remote tasks, Temporal/LangGraph connectors and mixed-provider deployments.

The [host service guide](docs/host-service-evolution.md) covers immutable workflow
versions, unified leaf/plan access, concurrent dispatch, human pauses and durable
recovery. [Operations diagnostics and the console](docs/host-observability.md)
provide progress polling, configuration checks and bounded metrics. The
[implementation record](docs/host-evolution-plan.md) distinguishes delivered
source profiles from outstanding roadmap work and deployment acceptance.

The [composable workflow guide](docs/composable-workflows.md) covers named
star/mesh/tree groups, reusable bounded plans, concurrent branches, arbitrary
model roles, versioned operation contracts and result acceptance. Two offline
applications demonstrate these features through the existing Rust gateway.

The [generic Cursor and OpenAI providers](docs/providers.md#reusable-model-adapters)
accept application-defined instructions and JSON schemas. Invoice calculation
is one example adapter; model transport, workflow execution and recovery are
shared across applications.

A Rust/nom protocol and durable SQLite mailbox for agent, tool and human-adapter
messages, with SPIN/Promela models and executable workflow examples. The protocol
runtime and CLI are native Rust. A reusable Python host connects model and tool
adapters to the Rust gateway. The formal verification runner is Rust; SPIN
generates the C model checker.

The design separates **who may communicate**, **delivery state**, **task state**
and **whether a result is correct**. A capability gateway binds agent requests to
registered identities and durably limits attempted operations. Runtime-owned
expiry closes still-open tasks even when their mailbox or message budget is full.
It supports star, mesh and tree graphs plus
review loops, peer handoffs, pipelines, routing and parallel work. See the
[design comparison and protocol specification](docs/design.md) and
[pattern examples](docs/examples.md).

## Example uses

- **Check a model-produced result.** Run the invoice mesh with model proposers
  and an exact Python arithmetic checker. The [runnable example](docs/host-runner.md)
  includes offline mocks and optional OpenAI/Cursor calls.
- **Recover a repository-job submission.** Submit a Cursor Cloud job, retain its
  identity across local restarts and reconcile an uncertain launch before
  continuing. The [offline recovery walkthrough](docs/providers.md#offline-failure-and-recovery-walkthrough)
  exercises the existing durable-job service without credentials.
- **Investigate a failed CI run.** Route diagnosis, repair and verification to
  separate participants, with a finite repair budget and explicit checks before
  dispatching another test run. This is an [integration recipe](docs/choosing-mas.md#organisational-use-cases)
  requiring CI and repository adapters.

The [use-case guide](docs/choosing-mas.md#organisational-use-cases) also covers
mathematical research, data analysis, infrastructure changes and service
remediation. It identifies existing demonstrations, required integrations,
acceptance criteria and simpler alternatives.

## Start with Docker Compose

Run the mixed-agent mesh locally, with no API key required:

```sh
docker compose up --build --abort-on-container-exit --exit-code-from mesh
```

The default uses mock model responses, real protocol exchanges and a Python
checker. To use two separate OpenAI GPT agents, supply `OPENAI_API_KEY` in your
host shell and run:

```sh
docker compose -f compose.yaml -f compose.openai.yaml up --build --abort-on-container-exit --exit-code-from mesh
```

Live mode makes paid API calls and checks model outputs deterministically. The
host supports custom adapters and graph configurations; this example is a logical
mesh over the central Rust/SQLite transport. See the [quickstart and adapter
guide](docs/host-runner.md) for credentials, model selection, persistence and a
complete custom adapter.

OpenAI and Cursor can also run together. The optional Cursor image uses the
official SDK in a restricted local mode: no tools, MCP servers, subagents or
ambient settings. Supply both API keys through the host shell, select a Cursor
model available to your account as `MAS_PLANNER_MODEL`, then run:

```sh
docker compose -f compose.yaml -f compose.mixed.yaml run --build --rm mesh
```

For two Cursor roles, set both model identifiers and use `compose.cursor.yaml`.
The separate durable-job adapter submits repository work to Cursor Cloud,
retains the exact remote agent/run identities and reconciles the result after
local execution ends. A submission receipt is not a completed remote task.
See [providers and durable jobs](docs/providers.md) for credentials, complete
commands, recovery and the different execution guarantees.

## Deployment scope

The supported host target is a bounded, single-host Linux deployment with trusted
Python adapters. It provides durable workflow checkpoints, supervised local
execution, shared call and spending reservations, independent reconciliation,
retention and audit-only backup restore. Uncertain provider outcomes require
explicit reconciliation; remote effects and actual billing remain outside the
protocol guarantee. See [operations](docs/operations.md),
[deployment acceptance](docs/deployment-acceptance.md) and
[release controls](docs/releases.md). Live-provider acceptance needs your own
credentials and is not implied by the offline tests.

An opt-in [remote worker profile](docs/distributed-workers.md) distributes
registered leaf execution while retaining one authoritative controller. It has
local multi-process failure tests; two-machine qualification remains a release
gate. [Trusted workspaces](docs/trusted-workspaces.md) are available for approved
tools, and [persistent Rust IPC](docs/gateway.md#persistent-host-transport) reduces
gateway process launches. These profiles do not add untrusted-code containment or
controller high availability; the [shared authority design](docs/shared-authority-design.md)
records that remaining work.

For a continuously served bridge, apply `compose.bridges.production.yaml` after
`compose.bridges.yaml`; it supervises ingress, worker and a reconciler on the
same durable volume. Follow the [bridge operations procedure](docs/operations.md)
for private credentials, readiness, alerts and recovery. Current verification
and its remaining deployment gates are recorded in [verification](docs/verification.md).

See [when to choose MAS](docs/choosing-mas.md) for organisational use cases,
alternatives and a measurable adoption pilot.

## Build and run

Requirements: Rust 1.99.0 (pinned), a C compiler for bundled SQLite and SPIN's
verifier, and SPIN for formal checks. The runtime does not need Python, an API
key or an MCP service. The verification harness requires Linux, `sh` and `sha256sum`; recorded
checks use Linux.

```sh
cargo build --release --locked --manifest-path rust-protocol/Cargo.toml
cargo test --locked --manifest-path rust-protocol/Cargo.toml
./rust-protocol/target/release/mas help
./rust-protocol/target/release/mas demo examples/star.json
./rust-protocol/target/release/mas demo examples/tree.json
./rust-protocol/target/release/mas demo examples/parallel-voting.json
```

`make examples` writes transcripts for all twelve workflow fixtures, a recovery
example that observes a real deadline after reopening SQLite, and an authenticated
gateway exchange that rejects spoofing and retains attempt limits across restart. They
exercise the codec and actual SQLite runtime, including redelivery after
restart. They execute no LLMs or humans. A host can replace the fixture functions
with independently executed participants without changing the envelope.

## An administrative persistent exchange

Run these commands from the repository root. The database path is real and
absolute; it is created automatically. Use a fresh database for this native
runtime rather than an older MAS Protocol Lab Python database. Native schema-1
databases are migrated transactionally to schema 2. These commands belong to the
trusted host: agents should use the [capability gateway](docs/gateway.md).
Keep a database backup if an older binary must remain usable: schema-1 binaries
cannot open a migrated schema-2 database.

```sh
MAS_BIN="$PWD/rust-protocol/target/release/mas"
MAS_DB="$PWD/state/demo.sqlite"
"$MAS_BIN" init "$MAS_DB" demo examples/star.json
cat > /tmp/mas-request.txt <<'MESSAGE'
message "q1" run "demo" from "hub" to "proposer" version "1" kind "Request" payload {"input":{"values":[2,3]},"operation":"sum","task_id":"t1"};
MESSAGE
"$MAS_BIN" send "$MAS_DB" hub /tmp/mas-request.txt
"$MAS_BIN" receive "$MAS_DB" demo proposer
"$MAS_BIN" ack "$MAS_DB" demo q1 proposer
cat > /tmp/mas-result.txt <<'MESSAGE'
message "r1" run "demo" from "proposer" to "hub" version "1" kind "Result" payload {"output":{"candidate":5},"reply_to":"q1","status":"completed","task_id":"t1"};
MESSAGE
"$MAS_BIN" send "$MAS_DB" proposer /tmp/mas-result.txt
"$MAS_BIN" receive "$MAS_DB" demo hub
"$MAS_BIN" ack "$MAS_DB" demo r1 hub
"$MAS_BIN" status "$MAS_DB" demo
```

Sending identical content with the same message ID is idempotent. Changed content
under that ID, disallowed routes, premature replies, incompatible versions and
contradictory terminal responses are rejected. Receive repeats unacknowledged
messages. A full mailbox rejects new messages for retry; acknowledgements release
capacity. Acknowledgement means receipt, not task success or exactly-once effects.

For agent integration, provision the gateway with a separate capability for each
participant. It derives the actor and run from that capability; agents cannot
select another identity. Keep its credential file, database and administrative
CLI outside agent access. The host privately routes each gateway response to its
caller. Reserve a call before execution, honour `actionable: false` on closed
requests, and record actual execution evidence with `finish`. Payloads never grant
tools or permissions. The gateway uses local standard input/output; it is not a
remote connector. The optional [Python host](docs/host-runner.md) supplies model
launching and application scheduling through this interface.

Run `mas reconcile DB RUN` to record expired open tasks as
`expired_outcome_unknown`. Ordinary run operations and status inspection also
reconcile. Expiry needs no worker reply or mailbox slot. It is observed when the
host executes; it cannot record itself while the host is stopped. Cancellation
and expiry close protocol work without proving that an external action stopped.

## Verification

Install SPIN (`apt-get install spin` on Debian/Ubuntu, or build from the official
SPIN distribution), then:

```sh
./rust-protocol/target/release/verify
```

The runner records simulations, independent safety and per-message LTL checks,
finite bounds, tool versions, commands, source hashes and counterexample replays.
Deliberate defects must be detected. A result that exceeds time, memory or depth
limits is incomplete. See [model contract](formal/README.md),
[verification results](docs/verification.md) and [independent review](docs/independent-review.md).

The bounded progress checks require no weak scheduler fairness. Separate safety
checks permit indefinitely repeated modelled faults. Retained counterexamples
show why endless faults still prevent eventual delivery, even with weak fairness.
Additional models cover transaction failures and expiry races. All models retain
finite identities and participants; none proves arbitrary-scale delivery, truth
of agent output, remote security or exactly-once external effects.

## Provenance and licence

This repository retains its GPL-3.0 licence. The nom codec and initial protocol
semantics derive from MAS Protocol Lab; the original MIT notice is retained in
[THIRD_PARTY_LICENSE.txt](THIRD_PARTY_LICENSE.txt). The native runtime and models
apply Holzmann's design method; sources and scope are recorded in the design
specification. There is one protocol implementation, in Rust; the Python host
uses its protocol state machine. Host workflow checkpoints and deployment
admissions are separate application records.
