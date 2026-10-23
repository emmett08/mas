# Workflow performance

Protocol CI qualifies native behaviour and pinned consumers. Its objectives are
earlier feedback, fewer repeated configuration changes and less unnecessary
setup. The Python reference host is scheduled for retirement, so its
qualification work is outside the maintained CI scope.

## Entry points and responsibilities

`.github/workflows/check.yml` owns pull requests, pushes to `main`, manual runs,
reusable invocation and weekly Rust dependency audits. Independent qualification
jobs run in parallel. `downstream.yml` groups native-host, application-example
and demo compatibility behind `workflow_call`.

| Configuration | Before consolidation at `76e999f` | Maintained native graph |
| --- | ---: | ---: |
| Workflow files | 8 | 2 |
| Workflow runs started by one PR update | 7 | 1 |
| Jobs in a full PR run | 35 | 10 |
| Workflow/action YAML lines | 489 | 377 |

Six former client, Compose, PostgreSQL, containment, machine and security wrappers
have been removed. The seven external reference-host workflow calls and their
legacy gates have also been retired. This removes out-of-scope Python product
qualification rather than accelerating equivalent work. The job-count reduction
must not be reported as a speedup with preserved coverage.

A full run retains protocol tests and SPIN/finite verification, executable Rust
examples, all 35 actual PostgreSQL admission tests, pinned native consumers and
a real Rust dependency audit. `check` requires protocol, downstream,
native-admission and dependency-audit success. Failure, cancellation or skipped
prerequisites fail the aggregate. The two required status names are `check` and
`dependency-audit`; adopting the graph requires the matching migration described
in [reference-host retirement](reference-host-migration.md#required-status-migration).

The Monday schedule retains cron `17 6 * * 1` and executes only the Rust
dependency audit. Other events execute the full graph. One entry-point
concurrency group cancels superseded PR runs across the graph. Non-PR invocations
use their run ID and remain independent.

## Shared setup and avoided work

`.github/actions/toolchains/action.yml` owns Go and Node versions and reads the
Rust version from the root `rust-toolchain.toml`. Each suite requests only the
runtimes and optional Rust components used by its commands.

| Installation requests in the existing local suites | Before consolidation | After consolidation |
| --- | ---: | ---: |
| Go setup | 5 | 4 |
| Node setup | 5 | 3 |
| Extra Rust components | 10 | 4 |

These counts compare the existing protocol, admission and downstream suites.
They exclude tooling introduced for the replacement dependency audit. An
installation request may reuse a runner's installed component. The counts
describe avoided requests rather than guaranteed downloads or seconds saved.

The incident rehearsal uses the Rust host and Python fixtures. Application
checks retain Go, Rust, `rustfmt` and Clippy. Mixed-agent interoperability retains
Go and Node. Native-host compatibility retains its formatter, linter and SDK
tools. The demo retains Go and Node for race tests and pinned Copilot CLI
compatibility.

A composite action centralises setup within each isolated job. The reusable
workflow owns job boundaries and runner selection. The example matrix preserves
separate generated-worker directories and the consumer Makefile's
`.NOTPARALLEL` requirement. Shared configuration does not itself share builds or
installed tools between runners.

The dependency audit downloads the official `cargo-audit` 0.22.2 Linux binary
and verifies its pinned SHA-256 before extraction. It checks the complete
protocol lock, including optional PostgreSQL dependencies, against a freshly
fetched RustSec advisory database. It needs no Rust installation or compilation.

## Timing evidence and comparison

[The original baseline](../evidence/workflow-performance/baseline.json)
identifies `e1257fb7125cdc971d94e38221c5185818bb577d` and every job in its seven
workflow runs. Its examples job ran applications for 410 seconds,
interoperability for 299 seconds and the incident rehearsal for 457 seconds,
sequentially.

The first optimisation, at `76e999f7c7e67670eda3b49ff9a5f886750f72f1`, moved
those groups into three isolated suites. All original native consumer commands
and all 108 incident fixture arms remained required.

| Examples measurement | Original | First optimisation |
| --- | ---: | ---: |
| Elapsed time through the examples gate | 1,211 s | 533 s |
| Summed suite/gate runner time | 1,211 runner-s | 1,333 runner-s |

That hosted comparison observed 678 seconds less waiting, a 56.0% reduction,
with 122 additional runner-seconds, or 10.1%. It met the pre-run targets of at
least 20% less waiting and at most 15% more examples runner time. This remains a
comparison of the same examples workload, independent of the later Python
product retirement.

[The pre-consolidation baseline](../evidence/workflow-performance/pre-consolidation.json)
records all 35 jobs at `76e999f`. Its seven runs took 963 seconds from their
common start to the last completion and consumed 8,932 summed runner-seconds.
That total includes the retired Python product. It is historical cost evidence,
not a same-coverage baseline for the native-only graph.

[The pre-retirement observation](../evidence/workflow-performance/pre-retirement.json)
records the consolidated graph at `2021477`. Its 35 jobs took 859 seconds from
run start to last completion and consumed 7,733 runner-seconds. All eight retained
native jobs passed and consumed 1,930 runner-seconds. The new graph also adds a
real local audit and a stricter aggregate, so those eight jobs alone are not its
complete runtime estimate.

Each recorded stage has one hosted observation on fresh runners. Timestamps
have one-second resolution. Further observations are needed to estimate the
typical timing effect. Compare unchanged native suites individually, including
setup and aggregate overhead, and record queue delays separately. Unchanged
SPIN and hosted-runner variability cannot be attributed to setup consolidation.
Runner-seconds differ from billed minutes, which depend on job rounding, runner
type, allowances and rates.

## Validation and rollback

Validate workflow and composite syntax with `actionlint`, parse YAML, check the
exact diff and verify the event graph. Confirm that every retained native
command, immutable consumer revision and candidate-protocol resolution check
remains present. The aggregate must fail when any prerequisite fails, is
cancelled or is skipped. Weekly runs must execute the dependency audit without
starting full qualification.

The toolchain composite's Bash script has been exercised with a recording
`rustup` substitute to check root-version resolution and component arguments
from a different working directory. The first optimisation passed protocol/SPIN
checks, all examples and all 35 actual PostgreSQL admission tests. The later
consolidated run also passed these native suites; its failures were in the
retired Python host and dependency/image qualification. Their failures remain
part of the historical record and have not been relabelled as successful audits.

Reverting setup consolidation restores per-job installation steps. Reverting
the example split restores sequential examples. Restoring the retired Python
qualification would require a new maintenance decision and the matching status
ruleset. Native application sources, formal models and consumer revisions are
unchanged by this CI work.
