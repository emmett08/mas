# Deployment acceptance

Acceptance is evidence about a particular release, host and workload. The
automated checks below exercise the Rust binaries, durable SQLite state and
supervised Python adapters. Mock runs make no provider requests. SPIN's bounded
protocol results remain separate from these implementation checks.

## Fault and recovery checks

Build the release binaries and install the host, then run the deployment tests:

```sh
cargo build --locked --release --manifest-path rust-protocol/Cargo.toml --bin mas --bin mas-gateway
python -m pip install ./host
export MAS_BIN="$PWD/rust-protocol/target/release/mas"
export MAS_GATEWAY_BIN="$PWD/rust-protocol/target/release/mas-gateway"
python -m unittest discover -s host/tests -p test_deployment.py -v
```

The Linux CI environment executes these checks:

| Injected condition | Required observation |
|---|---|
| `SIGKILL` of the host after recording provider output | Its direct supervised child terminates; explicit resumption returns the recorded output; the effect file contains one effect. |
| `SIGKILL` during a provider callback | Explicit resumption refuses to repeat the unknown invocation; the effect file still contains one effect. |
| Rust call completion commits but its response is lost | The journal retains the known output; resumption reconciles it with Rust without invoking the callback again. |
| Child-only file growth limit interrupts a real Rust database write | Message admission rolls back, SQLite integrity remains valid, and a subsequent bounded message succeeds. |
| SQLite returns `SQLITE_FULL` while writing the host checkpoint | No provider callback, call reservation or Rust message is admitted. SQLite integrity remains valid. |
| Back up and restore a completed run | Run identity, deployment identity and cumulative charges survive; replay returns the recorded output. |
| Independent host processes compete for one deployment call budget | Admission never exceeds the shared budget; the exercise reports rejected work. |
| Live mode has no usable credential | It returns `blocked_missing_credential` before creating or charging a deployment. |

Storage faults are bounded to a temporary database or child process. The tests
never fill the host disk. Process termination tests establish behaviour at the
observed checkpoint boundaries; they do not simulate physical power loss or
claim that an external provider cancelled work. A restored snapshot is for
inspection and completed-result replay; it cannot grant fresh effect admission.

## Repeatable load and bounded soak

Run twelve independent three-agent mesh jobs, with at most three active hosts:

```sh
python -m mas_host.acceptance --runs 12 --concurrency 3 \
  --state-dir .mas-acceptance --report evidence/deployment-acceptance.json
```

For a longer exercise, bound both admission time and job count:

```sh
python -m mas_host.acceptance --runs 500 --concurrency 4 --soak-seconds 300 \
  --state-dir .mas-acceptance-soak --report evidence/deployment-soak.json
```

The command stops submitting work at the first applicable limit and drains
already admitted jobs under their run deadlines. An existing deployment retains
its cumulative limits across repeated executions. Use `admin health` to inspect
those limits before a soak; budget exhaustion is a reported admission failure.
Omitting `--state-dir` uses a temporary deployment and removes its private state
after the public report has been produced.

Reports use schema `mas-deployment-acceptance/1` and contain:

- Workload, run count, concurrency, elapsed time and success/error counts.
- Integer millisecond latency summaries, including nearest-rank p50 and p95.
- Python, SQLite and platform versions, CPU count and SHA-256 identities of the Rust binaries.
- Deployment policy and cumulative run, call and micro-US-dollar reservations before and after execution.
- Individual run identifiers, outcomes and observed unknown-call counts.

The workload is deterministic integer invoice checking with simulated inference.
Measured latencies include process creation and durable protocol operations.
Use a representative application and target deployment to establish an
application-specific latency or throughput objective. A small passing run
establishes execution of the declared checks, not production capacity.

The checked-in [smoke report](../evidence/deployment-acceptance.json) and
[bounded soak report](../evidence/deployment-soak.json) record the executed
workloads. The [initial soak](../evidence/deployment-soak-initial.json) exposed
a race when SQLite removed a rollback journal during a state-size scan. The
scanner now takes one metadata snapshot per entry and tolerates an entry that
has disappeared; symlinks and special files still fail validation. A targeted
regression and the repeated soak cover that repair. Keep this failed evidence
distinct from the subsequent passing report.

## Explicit live-provider smoke test

Install the locked provider dependencies using the release instructions. Supply
`OPENAI_API_KEY_FILE` as a private, readable file, and select models available to
your account. The following command explicitly permits one mesh run with at most
two OpenAI requests:

```sh
python -m mas_host.acceptance --live --runs 1 --concurrency 1 \
  --state-dir .mas-live-acceptance --report evidence/deployment-live.json
```

Live mode rejects concurrency, multiple runs and soak settings. The host records
an expenditure reservation before each provider callback; the default OpenAI
adapter reserves 100,000 micro-US-dollars per call. These are conservative local
allocations, not a measurement of the provider's invoice. Provider account
controls and currently applicable pricing determine the actual financial limit.
`MAS_CALL_RESERVATION_MICROUSD` configures the positive allocation per call.

An unavailable credential, provider refusal, deadline, malformed output or
incorrect model answer prevents a passing report. A lost response can leave an
unknown outcome requiring inspection and reconciliation. Live acceptance is
complete only when a report records an actual successful provider execution;
mock tests and credential-mount checks do not supply that evidence.

Before unattended deployment, retain the passing fault report, a load report
from the target host, the bounded live report for the configured provider and
models, and the release's dependency/security check results. Re-run the affected
checks when execution, persistence, adapters or deployment settings change.
