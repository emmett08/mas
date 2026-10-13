# MAS: AI agent communication in Rust

A Rust/nom protocol and durable SQLite mailbox for agent, tool and human-adapter
messages, with SPIN/Promela models and executable workflow examples. The runtime,
CLI and examples are native Rust. The formal verification runner is Rust;
SPIN generates the C model checker.

The design separates **who may communicate**, **delivery state**, **task state**
and **whether a result is correct**. It supports star, mesh and tree graphs plus
review loops, peer handoffs, pipelines, routing and parallel work. See the
[design comparison and protocol specification](docs/design.md) and
[pattern examples](docs/examples.md).

## Build and run

Requirements: Rust 1.85.1 (pinned), a C compiler for bundled SQLite and SPIN's
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

`make examples` writes transcripts for all twelve deterministic fixtures. They
exercise the codec and actual SQLite runtime, including redelivery after
restart. They execute no LLMs or humans. A host can replace the fixture functions
with independently executed participants without changing the envelope.

## A persistent exchange

Run these commands from the repository root. The database path is real and
absolute; it is created automatically. Use a fresh database for this native
runtime rather than an older MAS Protocol Lab Python database.

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

For a live host integration, bind the `actor` argument to the participant the host
actually executed, reserve a call before execution, deliver only addressed messages,
then record the real execution identity and outcome with `finish`. Payloads never
grant tools or permissions. This executable is a local adapter; it is not itself
a remote ChatGPT connector, authenticated network service or model launcher.

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

The checked model has finite identities and participants, bounded faults and
explicit fairness assumptions. Its results do not prove arbitrary-scale delivery,
truth of agent output, remote security or exactly-once external effects. The
separate task model checks terminal-response safety, not eventual completion of
arbitrary tasks.

## Provenance and licence

This repository retains its GPL-3.0 licence. The nom codec and initial protocol
semantics derive from MAS Protocol Lab; the original MIT notice is retained in
[THIRD_PARTY_LICENSE.txt](THIRD_PARTY_LICENSE.txt). The native runtime and models
apply Holzmann's design method; sources and scope are recorded in the design
specification. There is one production implementation, in Rust.
