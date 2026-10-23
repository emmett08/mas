# Workflow performance

The protocol's CI qualifies native behaviour and pinned consumers. Its objectives are earlier feedback, fewer repeated configuration changes and less unnecessary setup, with the existing test and security requirements preserved.

## Entry points and responsibilities

`.github/workflows/check.yml` owns pull requests, pushes to `main`, manual runs, reusable invocation and the weekly security schedule. Its independent qualification jobs run in parallel. `downstream.yml` groups native-host, application-example and demo compatibility behind `workflow_call`.

| Configuration | Before consolidation at `76e999f` | Consolidated |
| --- | ---: | ---: |
| Workflow files | 8 | 2 |
| Workflow runs started by one PR update | 7 | 1 |
| Jobs in a full PR run | 35 | 35 |
| Workflow/action YAML lines | 489 | 418 |

The client, Compose, PostgreSQL, containment, machine and security wrappers have been removed. Each former wrapper's external workflow call now appears in `check.yml`, with the same reviewed host revision and candidate protocol input. YAML aliases share those input bindings and the identical security status steps. Existing commit-pinned references to historical workflow paths remain available in git.

The five protected status names remain `check`, `dependency-audit`, `image-audit` and both architecture-specific `host-and-compose` checks. Their full-run dependencies and success conditions are preserved. Failed, cancelled, skipped or missing prerequisites fail the relevant gate.

The Monday security schedule retains cron `17 6 * * 1`. The schedule event executes the external security workflow and its two status gates. Other events execute the full graph. Full-suite and architecture gates are excluded specifically for the schedule event. PR, push, manual and reusable runs receive the complete existing coverage.

One entry-point concurrency group cancels superseded PR runs across the entire graph. Its prefix differs from every pinned reusable workflow's group. Non-PR invocations use their run ID. The separately maintained external workflows retain their own existing concurrency policies. Artefact names from the called workflows are distinct within the consolidated run.

## Shared setup and avoided work

`.github/actions/toolchains/action.yml` owns Go and Node versions and reads the Rust version from the root `rust-toolchain.toml`. Each suite requests only the runtimes and optional Rust components used by its commands.

| Local installation requests per full PR run | Before | Consolidated |
| --- | ---: | ---: |
| Go setup | 5 | 4 |
| Node setup | 5 | 3 |
| Extra Rust components | 10 | 4 |

These counts cover jobs configured in this repository. An installation request may reuse a runner's installed component. The counts describe avoided requests rather than guaranteed downloads or seconds saved.

The incident rehearsal uses the Rust host and Python fixtures. Application checks retain Go, Rust, `rustfmt` and Clippy. Mixed-agent interoperability retains Go and Node. Native-host compatibility retains its existing formatter, linter and SDK tools. The demo retains Go and Node for race tests and pinned Copilot CLI compatibility.

A composite action centralises setup within each isolated job. The reusable workflows continue to own job boundaries, services and runner selection. The example matrix preserves separate generated-worker directories and the consumer Makefile's `.NOTPARALLEL` requirement.

## Timing evidence and comparison

[The original baseline](../evidence/workflow-performance/baseline.json) identifies `e1257fb7125cdc971d94e38221c5185818bb577d` and every job in its seven workflow runs. The original examples job ran applications for 410 seconds, interoperability for 299 seconds and the incident rehearsal for 457 seconds, sequentially.

The first optimisation, at `76e999f7c7e67670eda3b49ff9a5f886750f72f1`, moved those groups into three isolated suites. All original commands and all 108 incident fixture arms remained required.

| Examples measurement | Original | First optimisation |
| --- | ---: | ---: |
| Elapsed time through the examples gate | 1,211 s | 533 s |
| Summed suite/gate runner time | 1,211 runner-s | 1,333 runner-s |

That hosted comparison observed 678 seconds less waiting, a 56.0% reduction, with 122 additional runner-seconds, or 10.1%. It met the pre-run targets of at least 20% less waiting and at most 15% more examples runner time.

[The pre-consolidation baseline](../evidence/workflow-performance/pre-consolidation.json) records all 35 jobs at `76e999f`. Its seven runs took 963 seconds from their common start to the last completion and consumed 8,932 summed runner-seconds. Each stage has one observation on fresh hosted runners. Timestamps have one-second resolution.

For the consolidation comparison, require the structural reductions above and complete existing full-run coverage. Compare the new run's total elapsed time and summed job durations with that baseline, and compare its examples gate with 533 seconds and 1,333 runner-seconds. Include setup and gate overhead. Record queue delays separately. Variation in unchanged VM and SPIN work belongs in the measurement limits.

Moving job definitions into one workflow preserves their execution. The runtime changes in this increment remove unnecessary tool setup. Further hosted observations are needed to estimate the typical timing effect. Runner-seconds differ from billed minutes, which depend on job rounding, runner type, allowances and rates.

## Remaining duplication and alternatives

Both pinned external `check.yml` and `security.yml` execute `python scripts/release-audit.py` against the same reviewed source. Their other obligations differ: the check workflow also validates dependency resolution, while security owns Cargo and image audits. Deduplicating the Python audit requires a supported input or shared result in `mas-reference-host`, revision-bound evidence and a new reviewed workflow pin. That owner change remains separate from this protocol-repository consolidation.

Host tests also run with different database, architecture and container configurations. Those executions retain their separate qualification purposes. Build reuse would require matching target, features, flags and source identities.

Keeping eight workflows preserves more independent entry points. Flattening every job into one file reduces the file count further. The selected design gives one triggered CI run and keeps native-consumer orchestration cohesive in one reusable file. Individual jobs remain available for targeted retries.

The [GitHub reuse documentation](https://docs.github.com/en/actions/how-tos/reuse-automations/reuse-workflows), [composite action documentation](https://docs.github.com/actions/creating-actions/creating-a-composite-action) and [YAML alias documentation](https://docs.github.com/en/actions/concepts/workflows-and-actions/reusing-workflow-configurations) describe the platform facilities used here.

## Validation and rollback

Local validation passes `actionlint` 1.7.12, YAML parsing and `git diff --check`. A differential check confirms all seven external calls retain their exact pins and inputs, all test commands remain present, and the PostgreSQL services, architecture matrix and protected status policies are preserved. The composite's Bash script has been executed with a recording `rustup` substitute to check root-version resolution and exact component arguments from a different working directory.

The first optimisation passed native protocol/SPIN checks, all examples, all 35 actual PostgreSQL admission tests, shared PostgreSQL shards, portable clients, containment, Host/Compose and machine qualification. External dependency/image audit failures remain recorded as failures. The earlier subscription-isolation failure passed on the first optimisation without changing the pinned host revision.

Reverting this consolidation restores the six wrappers and per-job setup. Reverting the earlier optimisation also restores sequential examples. Application sources, dependencies, formal models and consumer revisions are preserved.
