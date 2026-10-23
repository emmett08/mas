# Python reference host retirement

The Python `host/` tree and its containers, release scripts and deployment
profiles were extracted into
[mas-reference-host](https://github.com/emmett08/mas-reference-host).
That repository is scheduled for retirement and is no longer a maintained
compatibility target of this repository. Protocol CI does not fetch it or call
its workflows. No changes to that repository are required for this work.

The maintained native Rust host is
[mas-host](https://github.com/emmett08/mas-host). Historical protocol commits and
tags remain available to existing pinned consumers.

## Native compatibility

The protocol repository owns Rust protocol tests, formal verification,
executable examples and real PostgreSQL authority/admission tests. The reusable
`downstream.yml` workflow qualifies these immutable consumer revisions:

| Consumer | Checked-out revision | Compatibility check |
| --- | --- | --- |
| `mas-host` | `71e40fe57521aa1048d001dff16c40379a3a3046` | Native/SDK tests, application acceptance, provider contract and SPIN checks |
| `mas-host-examples` | `6899cceb6314e0191070d6f5a0ae8d4100b4c6a0` | Application tests, documented commands, A2A SDK, mixed-agent conformance and credential-free incident rehearsal |
| `mas-demo` | `4e0d2f6ff8a9cd20c75f9ae18e0d0bbe4c89b96d` | Go race tests/vet/build, credential-free Copilot compatibility and four offline scenarios |

In disposable native-host checkouts, Cargo replaces only the protocol dependency
with the candidate source and verifies that resolution through Cargo metadata.
The examples retain their own immutable host revision. Consumer repositories and
their committed dependency pins are not edited. The demo runs the actual CLI
compiled from the candidate, checking each expected decision, 28 messages and
zero provider calls.

These checks cover credential-free integration. Live paid-provider execution,
production Kubernetes deployment, database replication and recovery qualification
require their own evidence.

## CI and security ownership

`.github/workflows/check.yml` is the entry point for pull requests, pushes to
`main`, manual runs, reusable invocation and weekly dependency audits. Full runs
execute protocol verification, native PostgreSQL admission, downstream
compatibility and a Rust dependency audit. The `check` aggregate requires all
four to succeed and rejects failed, cancelled or skipped prerequisites. Weekly
runs execute the dependency audit alone.

`dependency-audit` checks the protocol's locked Cargo dependency graph against
RustSec advisories. The Python dependency profiles, Python host tests, container
images, Compose deployments, portable Python clients and reference-host machine
and containment suites have been retired from protocol CI with that product.
Their removal is an explicit change of maintenance scope. It does not establish
that the old Python dependencies or images are secure.

Neither the protocol repository nor the checked native host revision publishes a
container product. The former `image-audit` and architecture-specific
`host-and-compose` statuses therefore have no product to qualify here. They are
removed rather than replaced with successful placeholders or unrelated fixture
image scans.

## Required status migration

The required status contexts for the maintained graph are `check` and
`dependency-audit`. The repository ruleset must replace the old five-context
list with these two names when adopting this graph. Preserve the remaining
pull-request, review-resolution, strict-status, deletion and force-push rules.
Until that ruleset migration is applied, GitHub will continue waiting for the
three retired image/Compose contexts.

Require the candidate's native checks and Rust dependency audit to pass before
merging. There is no reference-host PR or release prerequisite. Reverting the CI
retirement also requires restoring the matching required-status list.
