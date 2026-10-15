# Bounded reference-load qualification

`scripts/check-reference-load.py` measures the local Linux execution profile
through the durable task service, real Rust binaries and supervised dispatcher
processes. It makes no provider requests and reserves zero micro-US dollars.
The profile is a regression fixture, not a production SLO or a capacity estimate.
HTTP latency, shared PostgreSQL/S3 operation, fault recovery and composition
shapes have separate acceptance gates.

## Fixed profile

The script emits its assumptions, ceilings, source revision and binary hashes
before starting work. A qualification requires a clean checkout and imports
that checkout's host package. It checks the revision and hashes again afterward.
Build the Rust binaries from that same revision; the hashes identify the tested
executables but do not independently prove their build provenance.

| Property | Value |
| --- | --- |
| Workload | Equal single-call leaves from Alice and Bob |
| Local simulated provider wait | 100 ms per callback |
| Admission pace | At most two submissions per second, alternating principals when both have room |
| Outstanding bound | Eight tasks per principal |
| Scheduler policy | Four workers total, at most two per principal |
| Required exposure | At least 60 seconds of ongoing load and 100 completed tasks |
| Stop limits | 180 seconds of load or 256 submissions; hitting either before required exposure fails |
| Drain / process cleanup | 60 seconds / 30 seconds, with a five-second forced-stop grace |
| Resource observations | Process tree and RSS every 100 ms; storage every second |
| Process / summed descendant RSS ceiling | 64 processes / 2 GiB |
| Observer retained RSS growth ceiling | 64 MiB |
| Retained state growth ceiling | 16 MiB plus 2 MiB per submitted run, and below the deployment's hard storage ceiling |
| p99 fixture ceilings | Two seconds for submit/inspect; 60 seconds from submission to completion observation |

These deliberately generous ceilings detect regression or failure under this
small profile. They are not promised performance. There is no implicit budget
increase: the script uses a fresh private ledger with the existing default
policy and verifies that the run/call cap can cover the fixture. It stops on
unexpected task states, resource excess or incomplete exposure; idle waiting
after exhausting the submission cap cannot count as soak time.

Before the workload, a deterministic check exercises the actual lock-backed
capacity owner: four held slots, global denial, per-principal denial despite a
free global slot, release/reuse, and round-robin FIFO ordering. During load the
script checks both principals complete work, verifies paired exactly-once
callback evidence, and measures observed callback concurrency against the same
caps. This is a bounded fairness check for equal leaves, not a guarantee for
arbitrary heterogeneous workloads.

## Run and interpret

Use the supported Python environment and installed host dependencies. For a
source checkout, run from the repository root after building the release Rust
executables:

```sh
export PYTHONPATH="$PWD/host"
export MAS_BIN="$PWD/rust-protocol/target/release/mas"
export MAS_GATEWAY_BIN="$PWD/rust-protocol/target/release/mas-gateway"
python scripts/check-reference-load.py \
  --expected-commit "$(git rev-parse HEAD)" \
  --report /tmp/mas-reference-load.json
```

The script exits nonzero when a check fails. `--smoke` runs a short development
probe, permits a dirty checkout and always records `qualification: false`.
It cannot replace the full profile. Neither mode edits deployment policy or
calls a paid-provider path. Temporary private state is removed after the run;
the JSON report contains aggregate observations and bounded sample series,
without request payloads, credentials or private file paths.

The report includes nearest-rank p50/p95/p99 with sample counts, per-principal
completion counts, throughput, process/RSS peaks, observed orphan counts,
storage bytes and file growth, cumulative run/call charges, and cleanup outcome.
Submission-to-callback-start includes queueing and execution setup; it is not
pure scheduler latency. Completion observation includes polling delay. Measured
callback wait is separated from the remaining completion interval; the latter
still contains queueing, host execution and observation overhead.

RSS is the sum of resident pages, so shared pages can count more than once.
Between-sample peaks can be missed. PID start ticks distinguish reused IDs;
previously observed descendants remain tracked after orphaning. This handles
an ancestor-mounted `/proc` by obtaining the observer's visible identity from
`/proc/self/stat`. The observer's own RSS is reported separately. These are
resource observations, not replacements for the scheduler and storage owners'
authoritative admission checks. Durable state grows with retained results, so
the report evaluates a declared per-run allowance rather than calling all
growth a leak.

The older `mas acceptance --soak-seconds` retains its documented maximum
admission-window meaning. This separate qualification provides the minimum
exposure requirement without changing that interface.
