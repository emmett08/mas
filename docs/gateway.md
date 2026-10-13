# Private agent admission

The [reusable Python host runner](host-runner.md) provisions this gateway, keeps
capabilities private, supervises subprocess calls and dispatches model/tool
adapters. Its Compose mesh is the quickest complete integration example.

`mas-gateway` authenticates a capability before admitting an agent operation. Each
capability selects exactly one frozen run and actor. A worker can send only as
that actor, receive only its mailbox, acknowledge only messages addressed to it,
and finish only its own call reservations. The interface has no administrative
status, configuration or reconciliation command.

The executable is a Unix JSONL service on stdin/stdout. A trusted host broker
owns the process and routes each response privately to the corresponding agent.
It has no network listener. Sharing its raw stdout between mutually untrusted
readers would disclose their responses; the capability check cannot correct
that deployment. File permissions also do not isolate agents running with the
host's own OS privileges. Such agents need a process/container boundary that
denies access to the host database and other agents' credentials.

## Run the complete example

From the repository root:

```sh
cargo build --release --locked --manifest-path rust-protocol/Cargo.toml
cargo run --release --locked --manifest-path rust-protocol/Cargo.toml --example gateway
```

The Rust example creates a private temporary directory, provisions random
capabilities, launches the stdio executable, sends a valid message, rejects a
forged sender, restarts the executable, receives and acknowledges the message,
and checks that the admission limit remains exhausted after another restart.
It removes the private files and prints only the result assertions. It is a
deterministic local protocol exercise; no AI provider is called.

## Provision a deployment

Build the binaries as above. These commands create a private host directory and
initialise the existing star example:

```sh
private_dir="$PWD/.mas-private"
install -d -m 700 "$private_dir"
./rust-protocol/target/release/mas init "$private_dir/state.sqlite" demo examples/star.json
```

Save this nonsecret provisioning policy as `gateway-policy.json`:

```json
{
  "gateway_id": "star-broker",
  "max_attempts": 1000,
  "credentials": [
    {"credential_id": "hub", "run_id": "demo", "actor": "hub", "max_attempts": 400},
    {"credential_id": "proposer", "run_id": "demo", "actor": "proposer", "max_attempts": 300},
    {"credential_id": "checker", "run_id": "demo", "actor": "checker", "max_attempts": 300}
  ]
}
```

```sh
./rust-protocol/target/release/mas-gateway provision gateway-policy.json "$private_dir/credentials.json"
./rust-protocol/target/release/mas-gateway export "$private_dir/credentials.json" hub "$private_dir/hub-session.json"
./rust-protocol/target/release/mas-gateway export "$private_dir/credentials.json" proposer "$private_dir/proposer-session.json"
./rust-protocol/target/release/mas-gateway export "$private_dir/credentials.json" checker "$private_dir/checker-session.json"
```

Provisioning reads 32 bytes per capability from `/dev/urandom` and stores its
64-character lowercase hexadecimal representation. It creates new files with
mode `0600` and never overwrites existing files. Export writes only the selected
actor's token, run and identity to a private session file. Provisioning/export
responses contain no tokens. The host delivers each exported session through a
private channel; it retains the complete configuration itself. Tokens are bearer
credentials: possession grants the configured authority. Keep these files out of
source control and logs.

The host broker starts the service and retains its private pipes:

```sh
./rust-protocol/target/release/mas-gateway serve "$private_dir/state.sqlite" "$private_dir/credentials.json"
```

The executable checks that its credential input is a regular file with no group
or other permissions, and that the database's parent directory similarly denies
group/other access. The host controls the filesystem paths, links and ownership;
these checks do not establish protection against a compromised host. Windows
provisioning and permission enforcement are not implemented.

## Request frames

Send one JSON object per line. The host/client reads `token` from the selected
private session file and inserts it into each request without logging the frame.
The following token is an explanatory placeholder, not a working credential:

```json
{"token":"<private session token>","command":"send","wire":"message \"q1\" run \"demo\" from \"hub\" to \"proposer\" version \"1\" kind \"Request\" payload {\"task_id\":\"t1\",\"operation\":\"sum\",\"input\":{\"values\":[2,3]}};"}
```

With the proposer's capability, receive and acknowledge are:

```json
{"token":"<proposer session token>","command":"receive","limit":1}
{"token":"<proposer session token>","command":"ack","message_id":"q1"}
```

The acknowledgement records delivery; the worker returns a correlated Result or
Failure in a separate `send`. Requests cannot override `actor` or `run_id`:
extra fields are rejected, and the envelope's run and sender must match the
capability. A token cannot authorise itself by naming a different actor.

| Command | Additional fields | Authority |
| --- | --- | --- |
| `send` | `wire` | Matching run and sender; frozen route and task validation |
| `receive` | Optional `limit`, integer 1–100 | Bound actor's pending mailbox |
| `ack` | `message_id` | Bound actor's received message |
| `reserve` | `call_id` | Bound actor's call budget |
| `finish` | `call_id`, object `evidence` | Bound actor's existing reservation |

Responses are `{"ok":true,"result":...}` or
`{"ok":false,"error":{"code":"..."}}`. Rejected operations return a fixed
code; neither request contents nor credential details appear in diagnostics.
The complete Rust subprocess client is in
[`rust-protocol/examples/gateway.rs`](../rust-protocol/examples/gateway.rs).

## Admission and failure semantics

The global counter commits before frame parsing or authentication, including
invalid UTF-8, malformed JSON, unknown tokens and oversized input. Once a token
is recognised, its credential counter commits before command validation or the
store operation. A forbidden send, full mailbox, invalid command or exhausted
message budget therefore still consumes admission. A malformed frame whose token
cannot be parsed consumes only the global counter.

Counts use separate SQLite transactions and survive operation rollback and
process restart. They also serialize concurrent gateway processes. The global
counter caps all attempts through one configured gateway; a credential counter
caps authenticated attempts for its actor. A malicious caller can consume the
shared allowance and prevent useful requests. The bounds constrain admitted
work; they do not promise service availability.

The last globally admitted request is processed and then the process closes its
session. Reopening with the same configuration cannot reset the count. Exhausting
one credential leaves other credentials available until the global allowance is
exhausted. Restarting after an oversized frame can continue only with the
remaining durable allowance.

A SHA-256 fingerprint freezes the complete normalised configuration, including
credential bindings, limits and random tokens. Reordering credentials or JSON
fields preserves it. Changing a limit or token for the same gateway ID is
rejected. Plaintext tokens are not stored in the admission table. Configuration
must use generated, high-entropy capabilities; the library validates their
format and uniqueness, not their entropy. Gateway IDs and credential IDs use
1–32 ASCII letters, digits, underscores or hyphens; at most 32 credentials are
registered, with one binding per run/actor.

The host may retire a gateway and provision a new gateway ID as an explicit
administrative act. The old gateway's processes must be stopped and its private
access paths closed. There is no in-place revocation command: merely creating a
new gateway does not revoke existing capabilities or running processes. Database
replacement, new gateway registration and direct `mas` commands require trusted
host access; they are not operations exposed to agents.

A JSONL frame contains at most 65,536 bytes excluding the newline. JSON escaping
uses part of that allowance, so some maximum-sized wire messages need a smaller
payload to fit this adapter. The service retains at most the limit plus one
frame byte, closes oversized streams without draining them, and charges a
partial frame as soon as input is available. An idle or incomplete stdin read
can block: this implementation has no read timeout or scheduler guarantee. A
broker needing timeouts must supervise the child process.

Recovery stays available through the host path even after agent admission is
exhausted:

```sh
./rust-protocol/target/release/mas reconcile "$private_dir/state.sqlite" demo
./rust-protocol/target/release/mas status "$private_dir/state.sqlite" demo
```

Reconciliation observes the deadline and records an unresolved expiry where
appropriate. It cannot establish that an external effect did not occur. Direct
`Store` callers and the administrative `mas` CLI remain trusted; the capability
and admission guarantees apply to the `Gateway` interface.

## Evidence and remaining basis

Gateway tests exercise forged sender/run attempts, mailbox and call ownership,
unknown and duplicate fields, invalid encoding, durable rejected attempts,
configuration changes, stream limits, private provisioning, and concurrent
admission. SPIN models transport, transaction recovery and task expiry. Gateway
authentication and attempt limits are exercised by Rust tests; they are not
covered by those Promela models. SPIN does not verify the cryptographic libraries,
filesystem access controls or Rust implementation refinement.

Token equality uses fixed 32-byte arrays with
[`subtle` 2.6.1 `ConstantTimeEq`](https://docs.rs/subtle/2.6.1/subtle/trait.ConstantTimeEq.html),
checking all configured tokens without a token-prefix or credential-match early
exit. Policy fingerprints use
[`sha2` 0.10.9](https://docs.rs/sha2/0.10.9/sha2/). These are scoped implementation
choices, not a proof that the complete service has no timing side channels.
The remaining basis includes OS randomness, protection of bearer credentials,
the trusted broker/host, SQLite durability and the execution platform.
