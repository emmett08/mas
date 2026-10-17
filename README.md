# Nehemiah: bounded agent communication in Rust

Nehemiah defines bounded message, task and assessment exchanges for LLM agents,
deterministic agents, tools and human adapters. This `mas` repository provides
its Rust/nom codec, durable mailbox and formal models. **The name changes; the
wire format does not.**
Repository names, Rust crate names, the `mas` command, envelope version `1`,
`agent-task/1` and `assessment.v1` remain compatible.

Use the separate Rust [mas-host](https://github.com/emmett08/mas-host) for new
applications. It hosts native functions, executable tools, model adapters and
composed workflows. Maintained application examples live in
[mas-host-examples](https://github.com/emmett08/mas-host-examples).
The Python `host/` in this repository is transitional: its existing examples,
bridges and recovery tests remain available while those responsibilities move
to the Rust host. Python application handlers remain supported by the Rust host.

The design separates **who may communicate**, **delivery state**, **task state**
and **whether a result is correct**. A capability gateway binds agent requests to
registered identities and durably limits attempted operations. Observed expiry
closes still-open tasks even when their mailbox or message budget is full.
Star, mesh and tree describe logical permissions over a shared authority;
the same envelope serves mixed deterministic and LLM participants.
See the [design specification](docs/design.md) and
[Nehemiah/A2A responsibilities](docs/nehemiah-a2a.md).

A2A supplies an external interoperability interface. Its Agent Card describes a
service; the Nehemiah host still decides membership, routes, budgets and effect
permissions. The [bridge guide](docs/bridges.md) describes the existing
transitional Python integration. The optional
[native Rust A2A service](https://github.com/emmett08/mas-host/blob/main/docs/a2a.md)
publishes configured operations with scoped cards and durable request identities.
Local protocol runs need no network discovery service.

The [assessment contract](docs/assessment-contract.md) binds findings to exact
candidates, evaluators, rubrics, evidence and revision feedback. Its
[SPIN model](formal/assessment-revision-notes.md) checks identity, authority,
immutable history and bounded feedback handling. The
[paper](paper/mas-protocol.pdf) reports a pinned historical implementation and
scripted rehearsal; its retained results do not qualify later host or gateway
changes.

The remaining host guides in this repository describe the transitional Python
implementation: [workflow service](docs/host-service-evolution.md),
[composable workflows](docs/composable-workflows.md),
[providers](docs/providers.md#reusable-model-adapters),
[diagnostics](docs/host-observability.md) and
[implementation status](docs/host-evolution-plan.md).

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

## Run the transitional Python examples

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

## Transitional Python host deployment

The Python host in this repository targets a bounded, single-host Linux deployment
with trusted adapters. The separate Rust host documents its own supported profile.
The Python host provides durable workflow checkpoints, supervised local execution, shared call and spending reservations, independent reconciliation,
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
remote connector. The transitional [Python host](docs/host-runner.md) supplies model
launching and application scheduling through this interface. New applications use
the separate Rust [mas-host](https://github.com/emmett08/mas-host).

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

The bounded progress checks require no weak scheduler fairness because their
one-shot transitions exclude unrelated infinite computation and permanently
stopped service callers. A finite fault budget alone does not establish progress.
Separate safety checks permit indefinitely repeated modelled faults. Retained counterexamples
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
