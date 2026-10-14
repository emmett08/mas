# OpenAI, restricted Cursor and durable Cursor jobs

The host supports two distinct uses of Cursor. A restricted local SDK agent
returns a validated value through the same interface as OpenAI. A Cursor Cloud
agent performs repository work whose lifetime is independent of the local host.
Use the local provider for the invoice mesh; use the durable-job adapter for
remote repository tasks. A Cursor API key is not an OpenAI API credential.

| Implementation | Contract | Completion means |
| --- | --- | --- |
| `OpenAIProvider` | `TotalProvider` | A Responses API call returned a valid invoice total. |
| `CursorLocalProvider` | `TotalProvider` | A restricted SDK agent run returned a valid invoice total. |
| `CursorCloudJobAdapter` | `Adapter` for `cloud.submit` | A durable receipt identifies an accepted remote agent/run. |
| Deterministic checker | Existing Python adapter | Exact integer arithmetic agrees with the supplied totals. |

Providers can be selected independently of the graph. The same local provider
classes can serve roles in star, mesh or tree workflows. Changing a topology
does not create additional provider capabilities or a distributed transport.

## Credentials and model selection

Use a Cursor user or service-account API key. Team Admin keys are not supported
by the SDK. The key must have access to the selected model and, for Cloud jobs,
the repository and cloud-agent service. Select model identifiers from your
account's current catalogue; MAS does not assume that a model available through
OpenAI is also available through Cursor. The local adapter rejects `auto-smart`
because it has no configured Router optimisation policy.

Each provider reads its own `*_API_KEY_FILE`, or its `*_API_KEY` environment
variable when no file is configured. An unreadable or invalid secret file fails
without falling back to the environment. Credentials are excluded from provider
descriptors, protocol messages and reports. Keep secrets out of `.env`, source
control, prompts and job specifications.

For Bash, prompt without echoing a token or placing it in shell history:

```bash
read -rsp 'Cursor API key: ' CURSOR_API_KEY
export CURSOR_API_KEY
```

For mixed execution, also supply OpenAI's credential:

```bash
read -rsp 'OpenAI API key: ' OPENAI_API_KEY
export OPENAI_API_KEY
```

## Compose: two Cursor roles or a mixed mesh

Use Docker Compose 2.24.4 or newer for the Cursor overlays, which replace the
default temporary-filesystem allocation with a larger mount. Copy `.env.example`
to `.env` and set the model identifiers. For two Cursor
roles, both `MAS_PLANNER_MODEL` and `MAS_REVIEWER_MODEL` are required:

```sh
docker compose -f compose.yaml -f compose.cursor.yaml run --build --rm mesh
```

For a Cursor planner and OpenAI reviewer, set `MAS_PLANNER_MODEL` to a Cursor
model. `MAS_REVIEWER_MODEL` defaults to `gpt-4o-mini`; change it to an OpenAI model
that supports Responses API structured outputs if needed:

```sh
docker compose -f compose.yaml -f compose.mixed.yaml run --build --rm mesh
```

The mixed override mounts two separate secret files. It does not insert their
contents into the container environment. Both overrides retain the named state
volume and non-root execution. After the run, remove the shell variables:

```sh
unset CURSOR_API_KEY OPENAI_API_KEY
```

The optional `Dockerfile.cursor` uses a pinned Wolfi/glibc runtime for Linux
amd64 and arm64. Its complete Python runtime package set is SHA-256-locked and
verified against Wolfi's trusted package-signing keys during offline
installation. Debian is used only to build Python wheels. The SDK's native
bridge has no supported musl wheel, so this image is separate from the default
Alpine image. It contains both provider SDKs and is built locally by these
Compose commands. The image replaces the SDK's bundled Node executable with
SHA-256-locked Node 24.21.0 and applies the reviewed bridge dependency repair
described below. The SDK has separate licence terms; see
[third-party notices](../THIRD_PARTY_LICENSE.txt).

## Native and library use

The host's supervised execution requires Linux. For the locked Cursor runtime,
use CPython 3.14 on amd64 or arm64 with glibc, install the Rust binaries, then
run these commands from the repository checkout:

```sh
python -m pip install --require-hashes -r host/requirements-build.txt -r host/requirements-cursor.txt
python -m pip install --no-deps --no-build-isolation ./host
python scripts/install-cursor-runtime.py
export PATH="$PWD/rust-protocol/target/release:$PATH"
```

Run the installer with the same interpreter and environment that contains
`cursor-sdk==1.0.37`. It downloads the official Node 24.21.0 archive for the
current supported architecture, verifies the locked archive and executable
SHA-256 values, and replaces the SDK bridge's Node executable in place. It also
replaces the bridge dependency's obsolete Headers polyfill with Node's native
`Headers`, checks the exact source hashes, removes the unused `undici` and
`@fastify/busboy` package trees, and updates the installed wheel's `RECORD` to
describe the changes. This removes the dependency code as well as its metadata.

Repeat the installer after reinstalling the SDK. Local-provider preflight checks
the Node executable identity, repaired source files and dependency removals
before starting the bridge. The Compose build performs these steps automatically.
An interrupted dependency deletion or an unrecognised source change fails
integrity checks; reinstall `cursor-sdk==1.0.37` from the locked dependencies,
then rerun the installer. Do not delete more files to bypass that check.

For an offline installation, prepare the verified files on a connected machine
of the same architecture, then copy the directory to the deployment host:

```sh
python scripts/install-cursor-runtime.py --out /absolute/cursor-node-files
python scripts/install-cursor-runtime.py --source /absolute/cursor-node-files
```

The first command prepares the Node executable and licence; the second installs
them and applies the same bridge repair to the current environment. Both use
the repository's versioned runtime lock, so copying files does not bypass
executable verification. Add `--arch amd64` or `--arch arm64`
to `--out` when preparing files for a different target architecture. Installation
requires Linux with the matching architecture.

The combined lock contains both SDKs. OpenAI-only execution can continue using
`host/requirements-openai.txt` and the existing Alpine image. Cursor Cloud HTTP
jobs use the standard library and do not require the local SDK.

Configure a native mixed run using private key files and explicit role settings:

```sh
export CURSOR_API_KEY_FILE=/absolute/private/cursor-key
export OPENAI_API_KEY_FILE=/absolute/private/openai-key
export MAS_PLANNER_PROVIDER=cursor
export MAS_PLANNER_MODEL=YOUR_CURSOR_MODEL
export MAS_REVIEWER_PROVIDER=openai
export MAS_REVIEWER_MODEL=gpt-4o-mini
python -m mas_host --mode mixed --state-dir .mas-state
```

Replace `YOUR_CURSOR_MODEL` before running. `--mode cursor` defaults both roles
to Cursor; `--mode openai` defaults both to OpenAI. Explicit per-role provider
settings override those live defaults. `--mode mixed` requires both providers
to be specified. `--mode mock` always stays offline.

`MAS_PLANNER_RESERVATION_MICROUSD` and `MAS_REVIEWER_RESERVATION_MICROUSD`
override `MAS_CALL_RESERVATION_MICROUSD` for the corresponding role. Live
reservations must be positive; mock reservations are zero. The defaults are
100,000 micro-US-dollars per invocation. A reservation is a local admission
allowance, not a provider invoice or a monetary ceiling enforced by the provider.

Library callers can inject a `CursorLocalProvider` or `OpenAIProvider` into the
existing `PlannerAdapter`, `ReviewerAdapter` or `run_demo` parameters. The
factory in `provider_config.py` is the shared construction path for the CLI and
live acceptance. Provider selection requires no Rust wire-format change.

## Restricted local execution and recovery

The pinned local adapter creates one fresh private workspace per invocation,
sets `tools=[]`, disallows MCP and task delegation, supplies empty MCP/subagent
maps and loads no ambient setting sources. It rejects observed tool calls and
validates bounded, exact integer-JSON output. The checker independently computes
the result. Model-generated commands are never executed by this workflow.

The adapter launches the SDK's bundled bridge after durable MAS admission and
attempts to close its owned agent and client on normal return or exception.
The bridge runs in the supervised process group with a private home and a
restricted environment. Forced process termination cannot run Python cleanup;
the private temporary directory may remain until the host or container clears
it. Process-group termination is a separate guardian responsibility. These
controls form an SDK execution policy for trusted code, not an OS sandbox.

Bridge RPC retries are disabled. The pinned SDK may still retry underlying
model requests, so one MAS invocation counts one agent-run submission rather
than one guaranteed model request. Killing the local process or requesting
cancellation cannot establish that remote execution or billing stopped.

Resume with the original provider, model and effect-relevant settings. They form
part of workflow identity. The host reuses recorded successful invocation
outputs and blocks uncertain effects. It never switches providers automatically
after a timeout. See [host recovery](host-runner.md#persistence-and-explicit-recovery).

## Durable Cursor Cloud jobs

Copy [the example specification](../examples/host/cursor-cloud-job.json) to a
private `cursor-job.json`, replacing the repository, ref and model with values
available to your Cursor account:

```json
{
  "repository": "https://github.com/YOUR_ORGANISATION/YOUR_REPOSITORY",
  "ref": "main",
  "model": "YOUR_CURSOR_MODEL",
  "prompt": "Inspect the repository and propose a test plan. Do not change files.",
  "reservation_microusd": 100000,
  "mode": "plan",
  "auto_create_pr": false
}
```

Save it as `cursor-job.json`. Prefer an immutable commit ref when the job must
refer to a reproducible source version. `mode` may be `plan` or `agent`; plan
mode expresses agent behaviour, not an OS permission boundary. The adapter
always sends `workOnCurrentBranch=false`; automatic PR creation requires an
explicit `auto_create_pr=true`. Review the prompt and repository permissions
before submitting: Cloud agents can perform repository actions under the
selected account's authority.

The standalone Cloud Compose service uses the ordinary Alpine image and needs
only the Cursor secret. With `CURSOR_API_KEY` supplied in the shell, run:

```sh
docker compose -f compose.cursor-cloud.yaml run --build --rm \
  -v "$PWD/cursor-job.json:/job.json:ro" jobs submit review-001 --spec /job.json
docker compose -f compose.cursor-cloud.yaml run --rm jobs status review-001
docker compose -f compose.cursor-cloud.yaml run --rm jobs reconcile review-001
docker compose -f compose.cursor-cloud.yaml run --rm jobs status review-001 --result
```

The specification must be readable by container UID 10001. The named
`cursor-jobs-state` volume retains receipts and journals across invocations;
`docker compose -f compose.cursor-cloud.yaml down --volumes` deletes it. Do not
remove the volume while work is active or its outcome is unresolved. Running
the service without a command prints help and submits no job.

For native execution, use one stable local job ID for the intended operation:

```sh
python -m mas_host jobs --state-dir .mas-state submit review-001 --spec cursor-job.json
python -m mas_host jobs --state-dir .mas-state status review-001
python -m mas_host jobs --state-dir .mas-state reconcile review-001
python -m mas_host jobs --state-dir .mas-state status review-001 --result
```

`status` reads local state without a credential or network request. `reconcile`
polls the exact saved agent/run and updates the durable observation. Repeat it
when needed; the CLI does not run a background poller. `--result` includes
untrusted remote text for review. A remote `FINISHED` status records service
completion; it is not an independent correctness check or authorisation to
merge the resulting work.

The local job store persists specification and client-assigned agent identity
before dispatch. The normal runner reserves the submission before the HTTP
POST. A repeated job ID with a changed specification is rejected. A lost launch
response leaves `launch_unknown`; repeating submit never issues another POST.
Use a new job ID only for an intentionally separate, separately charged job.

To request cancellation, then observe its outcome:

```sh
python -m mas_host jobs --state-dir .mas-state cancel review-001
python -m mas_host jobs --state-dir .mas-state reconcile review-001
```

A cancellation acknowledgement means only that the request was accepted. The
job remains nonterminal until a correlated remote observation establishes a
terminal state. Local expiry and local process termination do not cancel the
remote job. After an uncertain cancellation request, first reconcile; an explicit
`cancel review-001 --retry` requests cancellation of the same nonterminal run again.
It never launches another job. Retain the private state directory until remote
and local outcomes have been reconciled.

Deployment health reports the last locally recorded job states; it does not
poll Cursor. Retention preserves the originating MAS run while its job is active
or unresolved. Backups include the cloud-job database. Restores remain audit
copies: local status inspection is allowed, while submissions, polling updates,
attachments and cancellation are blocked to prevent stale state authorising
remote operations.

For a lost launch response, obtain the exact remote run ID from independent
Cursor records. Record the source of that evidence in a private JSON object,
then attach and, if needed, resolve the uncertain MAS submission:

```sh
python -m mas_host jobs --state-dir .mas-state reconcile review-001 \
  --run-id run-EXACT_ID --evidence reconciliation.json --resolve-host
python -m mas_host jobs --state-dir .mas-state status review-001 --audit
```

Attachment verifies the frozen remote agent settings and exact run identity;
it never substitutes the agent's latest run. A missing lookup is not proof
that dispatch had no effect. `--resolve-host` records the submission receipt
as the invocation output. When the run identity is already saved,
`reconcile review-001 --resolve-host --evidence reconciliation.json` resolves
only the local receipt and needs no credential or network request. It does not
complete the remote repository task or reverse a local deadline. Resume the
original MAS run only after reconciliation:

```sh
python -m mas_host jobs --state-dir .mas-state submit review-001 \
  --spec cursor-job.json --resume /absolute/original/run-directory
```

## Verification and operating limits

Offline tests exercise provider contracts, output rejection, bridge cleanup,
frozen bindings and durable-job failure/recovery paths using injected clients.
The [Cursor Promela model](../formal/cursor-jobs-notes.md) separates launch,
receipt, polling and cancellation, with deliberate mutations that must fail.
These checks do not execute a real provider or prove the external service.

Run the [live acceptance commands](deployment-acceptance.md#explicit-live-provider-smoke-test)
with your own credentials and persistent state for the selected local-provider
combination. Cloud acceptance additionally requires a deliberately authorised
repository job, a correlated terminal observation and review of its result.
Missing credentials, account permissions or native runtime support remain failed
or blocked acceptance, not passing evidence.

Official API references consulted on 14 October 2026:

- [Cursor Python SDK](https://cursor.com/docs/sdk/python).
- [Cursor Cloud Agents API](https://cursor.com/docs/cloud-agent/api/endpoints).
- [OpenAI Responses API](https://developers.openai.com/api/reference/python/resources/responses).

Actual billing, service availability, model correctness and repository side
effects remain external obligations. Bounded formal checks do not establish
arbitrary-scale progress or exactly-once remote effects.
