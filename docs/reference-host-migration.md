# Python reference host extraction

The Python `host/` tree and its supporting containers, release scripts, deployment
profiles, examples and documentation now belong to
[mas-reference-host](https://github.com/emmett08/mas-reference-host).
The separate native Rust host is still
[mas-host](https://github.com/emmett08/mas-host).

## Preserved contracts

This removal changes no protocol library code, wire format, native CLI, Cargo
package identity, Rust dependency lock, or downstream dependency pin. The only
Rust source change moves the verifier's task-service corpus lookup from
`host/tests/fixtures/task-service-traces.json` to
`formal/task-service-traces.json`; the corpus bytes are unchanged. Historical
commits and tags remain available to existing pinned consumers.

| Repository | Existing dependency | Compatibility check on the removal PR |
| --- | --- | --- |
| `mas-host` | Protocol crate pinned at `fe644bbf3acb3ccc2abaffd54bcc4b367053fe4b` | Native/SDK tests, real application acceptance, provider contract and SPIN checks |
| `mas-host-examples` | Native host pinned at `2e0ad88354f5fafb78725204648e31fb2c565cc4` | Application tests, documented commands, A2A SDK, mixed-agent conformance and credential-free incident rehearsal |
| `mas-demo` | Protocol CLI pinned at `9d9845990843f5e3464395da7bd60bb50ffb2dda` | Go race tests/vet/build, credential-free Copilot compatibility and four offline scenarios |

The downstream workflow checks immutable consumer revisions. In its disposable
checkout, Cargo replaces only the protocol dependency with the PR's source and
verifies that resolution through Cargo metadata. Consumer repositories and their
committed pins are not edited. The demo runs the actual CLI compiled from this
PR, checking each expected decision, 28 messages and zero provider calls. These
checks cover credential-free integration; they do not claim a live paid-provider
or production Kubernetes deployment test.

## Build and verification ownership

Use `make check` here for protocol and formal verification. Use `make protocol`
and `make build` in the reference host repository for host tooling. Its reviewed
protocol pin defaults to an immutable historical revision; compatibility CI
overrides it with the candidate protocol commit. Both source identities are
recorded in new runtime and release provenance. Historical runtime provenance
remains readable.

This repository calls the migrated host workflows at full commit SHAs. They
check out that host revision and this PR's protocol revision, so removal is
tested without the local Python tree. Existing required contexts `check`,
`dependency-audit`, `image-audit`, and both `host-and-compose` architecture checks
remain present. Their gates reject failed, cancelled and skipped prerequisites.
Container release candidates are now prepared in the reference host repository;
its workflow does not automatically publish releases or move tags.

## Merge order

1. Review and merge the [reference host completion PR](https://github.com/emmett08/mas-reference-host/pull/4).
2. Require all protocol, migrated host, infrastructure, security and downstream
   checks on the removal PR to pass. Preserve the required contexts on `mas/main`.
3. Merge the removal PR. A later protocol-pin update in the reference host should
   receive its own compatibility review; existing downstream pins need no update.

The migration preserves repository ownership boundaries and existing interfaces.
It does not assert certification against an external engineering standard. The
applicable acceptance criteria are the protocol contracts, locked build inputs,
existing qualification suites and repository review/protection policies.
