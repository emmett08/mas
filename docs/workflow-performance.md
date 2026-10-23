# Workflow performance

The protocol's pull-request checks qualify its native implementation and its pinned consumers. The performance objective is earlier feedback with the same test commands, candidate-protocol binding and required statuses.

## Baseline and acceptance

[The retained baseline](../evidence/workflow-performance/baseline.json) records the seven workflow runs for `e1257fb7125cdc971d94e38221c5185818bb577d`, including every job and steps lasting at least 12 seconds. Each workflow has one observation on fresh GitHub-hosted runners. Timestamps have one-second resolution. The record distinguishes failed checks from successful checks.

In [Protocol checks, run 39870744062](https://github.com/emmett08/mas/actions/runs/39870744062), the longest job was `downstream / examples` at 1,211 seconds. Its independent test groups ran sequentially:

| Group | Existing commands | Step duration |
| --- | --- | ---: |
| Applications | Unit and acceptance tests, documented example commands and A2A demo | 410 s |
| Interoperability | Official A2A SDK check and mixed-agent host conformance | 299 s |
| Incident rehearsal | Frozen experiment tests and all 108 fixture arms | 457 s |

The host build took 18 seconds. Protocol checks spent 595 seconds in their SPIN step. The separate VM qualification took 983 seconds in total. These timings favour isolating the examples groups before adding build caches or changing the formal verifier.

The comparison target, chosen before running the candidate, is at least 20% lower elapsed time for the examples gate with at most 15% more examples runner-seconds. Compare the baseline's 1,211 seconds with both the candidate's matrix-to-gate elapsed interval and its summed suite/gate job durations. Include duplicated setup in the latter. Report queue delays separately. A single candidate run provides an observation, with repeated runs needed to establish the typical effect.

## Changes

`downstream.yml` executes applications, interoperability and the incident rehearsal as three matrix suites. Every suite starts from a fresh checkout, preserves the examples repository's host pin and verifies that Cargo resolves the candidate protocol. The separate checkouts respect the consumer Makefile's shared generated worker directories and `.NOTPARALLEL` declaration. Each original test command still executes once. The incident suite retains the assertions for 108 complete fixture arms and 324 provider effects.

The matrix has `fail-fast: false`. The existing `examples` job now reports an aggregate result after every suite finishes and requires the matrix result to equal `success`. Failure, cancellation and skipped prerequisites therefore fail the aggregate. The existing top-level `check` and the security and architecture-specific host statuses retain their names and success conditions. The repository ruleset requires those statuses.

The wrapper workflows and directly callable downstream workflow each have a distinct concurrency group. A newer pull-request revision cancels the earlier invocation, including local jobs such as PostgreSQL native admission and wrapper status jobs. The group includes the calling workflow name, so separate entry points remain independent. Non-PR invocations use the run ID, preserving separate pushes, manual dispatches and scheduled runs. The wrapper prefixes differ from the pinned reusable workflows' prefixes to avoid caller/callee cancellation.

The [GitHub matrix documentation](https://docs.github.com/actions/writing-workflows/choosing-what-your-workflow-does/running-variations-of-jobs-in-a-workflow), [concurrency documentation](https://docs.github.com/actions/writing-workflows/choosing-what-your-workflow-does/control-the-concurrency-of-workflows-and-jobs) and [reusable-workflow constraints](https://docs.github.com/en/actions/reference/workflows-and-actions/reusing-workflow-configurations) describe the platform semantics used here.

## Measurement limits and rollback

Local validation passes `actionlint` 1.7.12 and `git diff --check`. A structural comparison with the baseline confirms that every original examples step retains its command, environment and pinned inputs, that each test step belongs to exactly one suite, and that all other jobs retain their success conditions. The ordinary locked Rust suite also passes, with the three existing SPIN-dependent ignored tests left to the unchanged full CI verifier.

Parallel suites add two checkouts and host builds. Hosted-runner queueing and test-duration variation can affect the elapsed saving. The VM and external reference-host suites remain potential limits on complete-PR feedback. Existing dependency/image audit failures and the reference-host amd64 subscription-isolation failure remain blocking outcomes.

Sum job durations as runner-seconds for comparisons. Runner-seconds differ from billed minutes, which depend on job rounding, runner type, account allowances and rates. This change makes no financial saving claim. Cancelled obsolete runs can avoid further execution, with the amount depending on push timing.

Reverting the workflow change restores sequential examples execution and the prior cancellation behaviour. Application sources, dependencies, formal models and consumer revisions are unchanged.
