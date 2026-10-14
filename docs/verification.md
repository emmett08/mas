# Verification results

## Host service evolution — 14 October 2026

The integrated source suite passed **946 host tests with no skips**, using the
real Rust executables, pinned Python 3.14 SDK dependencies and the real Temporal
test server. It covers authenticated composed lifecycle through CLI/HTTP/MCP/A2A,
immutable versions, concurrent ownership, retention/deduplication, provider
certainty, supervised execution, remote worker fencing/spooling, recovery and
the console's request/session handling.

Three bridge regression tests were added afterwards and passed in a **24-test
bridge/orchestrator supplement**. No runtime code changed after the integrated
run. The final source therefore contains 949 host tests; the evidence above is a
946-test integrated run plus that supplement, not a claimed 949-test full run.

Rust passed **61 tests**; its two model-specific tests are excluded from ordinary
`cargo test` and covered by the separate verifier. Script tests passed **43** and
executable fixtures passed **14**. SPIN passed **110 cases**, including the new
host evolution model with and without reduction and seven named mutation
counterexamples. The model's bounds and correspondence limits are in
[host-evolution-notes.md](../formal/host-evolution-notes.md).

An isolated baseline host with only the persistent transport overlaid passed
**35 transport and existing Runner conformance tests**. Its broader baseline run
ended in a Cursor proxy fixture without a test-runner summary and is explicitly
excluded from passing evidence. The complete integrated suite above did pass.

The bounded [source receipt](verification/host-evolution-source.json) records
counts, toolchain, evidence hashes and a content binding. The
[IPC benchmark](verification/host-ipc-benchmark.json) measures 100 completed-run
status calls per strategy; it does not establish provider throughput or
production capacity. Raw transient logs are not required runtime dependencies.

These are source checks on Linux x86_64. Exact-commit hosted CI records installed
amd64/arm64 checks separately. No live paid provider, production OAuth/proxy,
two-machine qualification, shared authority, untrusted-code sandbox or accepted
published release is claimed. The [current implementation record](host-evolution-plan.md)
preserves every remaining work package and release gate.

## Production-readiness repairs — 14 October 2026

Review of main at `6a23507a183d4a35e188861756f89666100a6a4b` found
additional defects after the earlier bridge acceptance. The repairs reject the
A2A null-task scope bypass, allow ingress to reopen a current ledger during
active execution, and validate complete Rust/gateway request frames before
durable admission. Oversized results preserve known invocation evidence while
failing the application result. Regression tests exercise the actual HTTP,
SQLite, subprocess and Rust boundaries.

The supervised bridge profile now checks HTTP ingress, worker ownership and
bounded progress, and an independent reconciler on the same state volume. CI
exercises submission, completion and idempotent replay after restarting ingress
and worker on both supported architectures. Readiness does not establish
provider availability, and Docker health status alone does not restart a hung
container; the operations guide describes alerting and recovery.

The release workflow now accepts an existing owner-created tag, uses read-only
repository permissions, and packages both standard and Cursor variants on amd64
and arm64. Each archive is bound to the runtime image actually tested and scanned.
An owner-created release and durable retention remain separate operations.

The full formal rerun passed **90 cases and 118 checks**. All 32 deliberate
mutations now require their specific replayed assertion or selected liveness
property, including the original transport controls. There are **79 positive
checks, 32 defect counterexamples, two assumption counterexamples and five
reachability witnesses**. The model bounds are unchanged; this strengthens the
acceptance oracle without claiming whole-program refinement.

The [current repair record](../evidence/production-readiness.json) identifies
executed local checks and their limits. Its host suite has explicit optional SDK
skips. Prior optional-SDK and real Temporal results below are historical evidence;
the architecture workflow reruns its pinned SDK environments. Exact-revision PR
checks establish container and image-scan results. No live OpenAI, Cursor, Cloud,
organisation OAuth/TLS or representative production workload acceptance is
claimed. Code repairs and finite-model checks do not by themselves establish
production readiness.

## Earlier durable bridge extension — 14 October 2026

The whole-PR recheck found defects despite the earlier green CI. Repairs and
requirement coverage are recorded in [the independent review](independent-review.md#whole-pr-bridge-and-provider-recheck--14-october-2026).

The strengthened formal rerun passed **90 cases and 118 checks**: **79 complete
positive checks, 32 deliberate-defect counterexamples, two liveness-assumption
counterexamples and five reachability witnesses**. Witnesses demonstrate that
important scenarios are reachable; they are not additional safety proofs.
The local service model separates worker claim, executor reply and final commit,
so it includes the cancellation race omitted from the previous models. It
completes with and without partial-order reduction. Eleven shared traces check
77 action observations against both those model transitions and the real
TaskService/SQLite implementation, using an explicitly controlled executor.
Cursor, bridge and service mutations must fail their named assertions; an
unrelated counterexample no longer satisfies their gate. See the
[bridge model contract](../formal/bridge-notes.md) for bounds and correspondence.

An experimental expansion scheduling both task lifecycles exhausted its memory
budget and is excluded. The accepted model schedules one lifecycle alongside a
second principal's independent identity. The first stricter verifier run also
rejected two reachability results because SPIN remapped conditional source lines;
exact assertion-expression matching corrected the oracle, and the complete rerun
passed. Both unsuccessful checks remain in the raw evidence archive.

The local host suite reported **400 tests: 386 passed and 14 optional SDK tests skipped**;
the separately installed orchestrator environment passed **all 22 tests**,
including the real Temporal-server exercise. The Rust suite passed 50 tests and
the release-helper suite passed 32. Six dependency-lock groups, containing 120
package entries in total, were audited with no reported vulnerabilities. Those
package entries include dependencies repeated across groups; they are not 120
distinct packages. Dependency audits are separate from final image scanning.

The implementation suite now covers principal-scoped request conflicts,
exclusive service scheduling, execution factories with trusted identity,
crash/replay at the existing Rust boundary, paused local cancellation,
permission revocation after uncertain dispatch, schema rejection and retention
of unresolved remote effects. MCP and A2A tests validate checked-in official
schema fixtures and exercise real HTTP/stdio exchanges. A2A's generated JSON
Schema is non-normative; its protocol specification and protobuf definitions
remain authoritative. These checks provide evidence
for the declared profiles, not compatibility with every external client.
Temporal and LangGraph checks are separate optional SDK gates. A skipped SDK
or real-server test is not a passing interoperability result.

The current architecture workflow installs the bridge dependencies on amd64
and arm64, exercises the installed standard and Cursor images, and runs a
separate pinned orchestrator environment with the real Temporal-server test
enabled. Passing local checks establish only their executed environment; the
exact-revision Actions results establish those architecture gates. Provider
credentials, an organisation's OAuth infrastructure and production remote peers
remain separate acceptance requirements.

## Earlier Cursor provider extension — 14 October 2026

The local extension checks passed **208 host tests with no skips and 48 Rust
tests**. The combined SPIN suite passed **62 positive checks and 20
expected counterexamples**, across 54 cases. The Cursor job model includes a
search without partial-order reduction and seven defect mutations; see its
[contract and implementation mapping](../formal/cursor-jobs-notes.md).
The bounded deployment exercise completed 12/12 mock runs.

Tests exercise both pinned SDKs with offline transports, the actual Cursor
bridge after normal completion, timeout, adapter death and host death, mixed
provider replay, unknown effects, cloud launch process death, exact-run
reconciliation, cancellation, retention and audit-only restoration. The new
process finaliser regression fails when inherited-object freezing is removed.
No live provider acceptance passed: OpenAI, Cursor and mixed live checks were
blocked by missing credentials. Remote Cloud acceptance likewise requires an
authorised account and repository.

The existing architecture matrix now tests both the Alpine image and optional
Cursor/glibc image on native amd64 and arm64. Security gates scan both images.
Their exact-revision Actions results are the container evidence; local checks
are not a substitute for those runs. `cargo fmt --check` reports pre-existing
repository-wide formatting differences; this change retains the existing Rust
style and introduces no formatting gate or unrelated reformat.

The initial PR passed protocol and both architecture checks, but failed image
security acceptance. Review found Node 24.5.0 and obsolete bridge dependencies
inside Cursor SDK 1.0.37, and additional findings in the Debian runtime image.
The repaired optional image uses a pinned Wolfi/glibc base and signed,
SHA-256-locked packages. Debian remains only in the wheel-builder stage.

The unified runtime installer replaces Node with locked version 24.21.0,
applies a source-hash-verified native `Headers` repair, removes the actual
obsolete `undici` and `@fastify/busboy` trees and updates the installed wheel's
`RECORD`. Runtime preflight checks the repaired state before bridge launch.
Native users must rerun the installer after each SDK installation; Compose
applies it during the build. These checks distinguish source and executable
identity, dependency removal, shared-library resolution and bridge execution.
Release-helper, architecture and security gates must identify the repaired
revision. The earlier passing architecture jobs do not establish acceptance of
this image, and the repairs alone do not establish a passing security scan or
live-provider acceptance.

## Earlier protocol and host evidence

Recorded on 13 October 2026. **47 Rust tests passed; 60 positive SPIN checks
completed with zero errors; eleven deliberate defects and two assumption
counterexamples were detected and replayed; 29 sampled simulations completed.**
All twelve workflow fixtures, the deadline-recovery example and the authenticated
gateway example executed successfully.

These results establish the checked finite-model properties and tested runtime
behaviours. They do not establish unrestricted implementation correctness,
mathematical truth, live AI-agent performance or remote cryptographic security.

The optional Python host adds **132 passing offline tests**, including integration
with the real Rust binaries and an HTTP contract test using OpenAI SDK 3.29.0
with a simulated transport. The mock mesh exercised all six directed edges,
three completed tasks and two recorded mock calls; its exact Python checker
verified an invoice total of 5,497 cents. No live model calls were made.
Host tests include lost replies after durable commit, provider failure, refusal,
incomplete/malformed outputs, route rejection, depth limits and expiry. These are
implementation tests; the [host recovery model](../formal/host-recovery-notes.md) checks a finite
recovery abstraction, not the Python implementation.
The separate `Host and Compose checks` workflow runs the full installed-container
suite on native amd64 and arm64, checks reconciler readiness and tests live-mode
secret delivery with a dummy key. Seventeen offline release-helper tests check
immutable tag binding, OCI integrity, build provenance and imported-image identity.
Workflow success is recorded separately in Actions.

Both architecture gates passed for candidate `a84e190` (`v0.1.0`), with all 131
host tests passing natively and inside each installed container. Its tagged
packaging failed because Docker's classic exporter rejected the provenance-bearing
manifest list. The [initial CI report](../evidence/readiness-ci-initial.json)
retains that result. The repaired workflow exports one OCI archive and imports
that same image for testing. Candidate `v0.1.1` subsequently exposed a
[test synchronisation race](../evidence/readiness-ci-recovery-race.json) in one
arm64 container run: a terminated adapter did not imply that guardian cleanup
had released ownership. The helper now waits for actual lock availability;
the new deterministic regression proves premature recovery remains rejected.
Both final architecture image imports and container suites passed, but
[Trivy rejected the OCI tar input](../evidence/readiness-ci-scanner-input.json).
The workflow now scans a verified OCI directory with explicit manifest selection.
Candidate `v0.1.2` passed all architecture suites and prerequisites but
[failed Trivy's root-only reference resolution](../evidence/readiness-ci-scanner-reference.json).
The scanner layout now directly names the verified runtime manifest at its root,
preserving the original archive and every blob. Candidate `v0.1.3` must pass the
full release gates with this correction.

Deployment fault checks exercise actual process termination, response loss,
SQLite write failure and restored-state admission blocking. New regressions cover
bootstrap interruption, guardian ownership during descendant cleanup, schema
rejection without mutation, adapter error substitution and persistent live state.
The smoke workload completed 12/12 jobs; a 60-second soak completed 601/601
jobs across four concurrent hosts, with 1,202 mock calls and no errors. These are
measurements of this local workload, not a capacity promise. Concurrent load and
bounded-soak reports are in `evidence/deployment-*.json`; the initial failed soak
is retained separately from the corrected run. No live credential was available,
so `deployment-live.json` records a blocked check rather than a successful live
provider result. See [acceptance scope](deployment-acceptance.md).

## Evidence and reproduction

| Evidence | Contents |
|---|---|
| [`bridge-acceptance.json`](../evidence/bridge-acceptance.json) | Current bridge implementation and acceptance record, with executed checks distinguished from pending architecture and live-provider gates. |
| [`bridge-model-checks.json`](../evidence/bridge-model-checks.json) | Current full formal run: 90 cases, 118 checks; 79 positive checks, 32 defects, two assumption counterexamples and five reachability witnesses. |
| [`bridge-model-checks.tar.gz`](../evidence/bridge-model-checks.tar.gz) | Current generated models, raw logs, sampled traces and replayable counterexamples. |
| [`bridge-host-tests.log`](../evidence/bridge-host-tests.log) | Current local host suite, including its explicit optional-SDK skips. |
| [`bridge-orchestrator-tests.log`](../evidence/bridge-orchestrator-tests.log) | Separately installed SDK suite, including real Temporal-server interoperability. |
| [`model-checks.json`](../evidence/model-checks.json) | Formal report with commands, expected and observed results, hashes, bounds and tool versions; inspect the recorded case set and source identities. |
| [`model-checks.tar.gz`](../evidence/model-checks.tar.gz) | Generated model instances, raw logs, simulations and replayable counterexamples. |
| [`initial-incomplete-model.tar.gz`](../evidence/initial-incomplete-model.tar.gz) | Retained initial memory-limited searches; classified as incomplete. |
| [`rust-tests.log`](../evidence/rust-tests.log) | Parser, storage, workflow, CLI, gateway, independent and verifier regression results. |
| [`examples/`](../evidence/examples/) | Fourteen executed Rust/SQLite examples, including expiry and gateway restart. |
| [`readme-exchange.log`](../evidence/readme-exchange.log) | Execution of the administrative request/result sequence. |
| [`source-manifest.json`](../evidence/source-manifest.json) | SHA-256 identities for delivered source, configuration and documentation. |
| [`host-tests.log`](../evidence/host-tests.log) | Offline host, provider contract and Rust-backed integration tests. |
| [`host-mock.json`](../evidence/host-mock.json) | Six-edge mixed-adapter mesh report; explicitly mock mode. |

Reproduce the host checks after building the Rust release binaries:

```sh
python -m pip install --require-hashes -r host/requirements-build.txt -r host/requirements-openai.txt -r host/requirements-bridges.txt
python -m pip install --no-deps --no-build-isolation ./host
python -m unittest discover -s host/tests -v
docker compose up --build --abort-on-container-exit --exit-code-from mesh
```

These hash locks target the documented CPython 3.14 Linux wheel profiles.
For the optional Cursor bridge and its runtime repair, follow
[provider installation](providers.md). To execute the optional orchestrator
checks in a separate compatible environment:

```sh
python -m pip install --require-hashes -r host/requirements-build.txt -r host/requirements-orchestrators.txt
python -m pip install --no-deps --no-build-isolation ./host
MAS_TEMPORAL_INTEROP=1 python -m unittest discover -s host/tests -p 'test_orchestrator*.py' -v
```

The real-server test starts Temporal CLI 1.9.1 locally and uses no model
provider. `MAS_TEMPORAL_SERVER` can identify an existing binary; otherwise the
SDK downloads that pinned version. The test's explicit enablement and installed
optional SDKs must be recorded alongside its result.

```sh
cargo test --locked --manifest-path rust-protocol/Cargo.toml
cargo build --release --locked --manifest-path rust-protocol/Cargo.toml
./rust-protocol/target/release/verify
make examples
```

The verifier needs Linux, SPIN, a C compiler, `sh` and `sha256sum`. The current
toolchain is pinned to Rust/Cargo 1.99.0. The previously recorded formal evidence
used Rust/Cargo 1.85.1, SPIN 6.5.2 and GCC 13.3.0; its tool versions remain part
of that evidence. Dependencies are pinned in Cargo.lock. The native build uses rusqlite
0.40.2 and bundled SQLite 3.53.2; the supplied image selects maintained system
SQLite through the same Rust interface. Image tests check actual dynamic linkage
and run the host suite against the installed package.
Runtime, gateway, examples and verification orchestration are Rust; Promela and
SPIN-generated C provide the formal checks. Each report records the runner and
model hashes for its own revision. Historical reports must not be treated as
verification of later source without matching those identities.

## Checked scope

| Cases | Checks | Result |
|---|---:|---|
| Twelve bounded topology configurations | 36: safety and two per-message LTL properties per graph | Complete, zero errors; no fairness option. |
| Bounded no-reduction and capacity-one cases | 6 | Complete, zero errors; no fairness option. |
| Existing two-task lifecycle | 1 safety | Complete, zero errors. |
| Unlimited repeated faults: twelve graphs, capacity one and no reduction | 14 safety | Complete, zero errors. |
| Transaction rollback/crash/retry model | 1 safety | Complete, zero errors. |
| Task terminal/expiry race model | 1 safety | Complete, zero errors. |
| Host checkpoint recovery | 1 safety | Complete, zero errors; no liveness claim. |
| Cursor remote job, reduced and no reduction | 2 safety | Complete, zero errors; no remote liveness claim. |
| Remote bridge service, local task service and event history, each reduced and no reduction | 6 safety | Complete, zero errors within the declared scenario bounds. |
| Shared local-service action corpus | 11 deterministic trace checks | Complete; 77 selected observations also pass against TaskService/SQLite. |
| Local-service reachability | 5 witnesses | Named negated-reachability assertions fail and replay at the intended scenarios. |
| Early ack, forbidden route, missing ack, competing terminals, partial commit, duplicate insertion, expiry overwrite, uncharged host dispatch, unknown replay, stale owner, result overwrite | 11 deliberate defects | Expected violations detected and replayed. |
| Cursor uncharged dispatch, relaunch, lost identity, wrong run, premature cancellation, timeout cancellation and terminal overwrite | 7 deliberate defects | Expected assertion violations detected and replayed. |
| Bridge duplicate identity, conflicting content, unadmitted dispatch, unknown replay, premature cancellation, stale terminal, lost event commit, inbox overflow, uncommitted event exposure and cursor advancement before delivery | 10 deliberate defects | Named assertion violations detected and replayed. |
| Local service late cancellation, unscoped owner key, stale revision and lost event commit | 4 deliberate defects | Named assertion violations detected and replayed. |
| Unlimited faults, with and without weak process fairness | 2 assumption counterexamples | Eventual acknowledgement fails; cycles replayed. |

A counterexample expectation requires the appropriate violation type and a
successful trail replay. Resource exhaustion cannot satisfy it. No bitstate or
hash-compact approximate search was used. Every checker has a 30-second,
512-MiB command budget and depth limit 100,000; exceeding a limit prevents a
successful classification. Exact state counts, transitions and unreachable
statements are retained in the raw logs.

Transport models retain two message identities, two to four participants for the
supplied graphs, and the configured capacities. Routes range over every declared
edge. Positive bounded progress allows one restart, lost acknowledgement invocation
and redelivery per message. Every such action is bounded, so those models cannot
sustain a non-progress loop by selecting faults forever; weak process fairness
is unnecessary. This does not prove that a deployed worker eventually runs.

Separate safety variants remove the repetition limits on those faults. They
check routing, retained identity/content, receipt before acknowledgement and
capacity. Repeated non-destructive faults preserve these invariants in the finite
model. With unlimited faults, the liveness counterexample can keep redelivering
one message without acknowledging it, even under weak process fairness.

The transaction model admits rollback and crashes at pre-commit stages, atomic
commit, lost commit responses and identical retries. It verifies the protocol
against SQLite's atomic recovery contract; it does not prove that contract for
SQLite, the filesystem or hardware. Runtime tests inject insertion failures and
check rollback, reopen and deduplication. They do not simulate physical power loss.

The expiry model checks that runtime closure cannot replace an earlier terminal
outcome and cannot be undone by clock-condition rollback. It permits receipt and
acknowledgement of closed work, with a non-actionable delivery annotation. Expiry
is an explicit host observation, not a timed-automata clock: no deadline-in-seconds
or eventual-host-activity theorem is claimed. Earlier dispatched effects may
still occur. Message truth, exactly-once effects and eventual successful work
remain separate obligations.

Gateway authentication, frame limits and durable global/credential attempt caps
are exercised by implementation tests and the subprocess example. They are not
cryptographic or admission-protocol proofs from the Promela models.

Remote bridge service checking uses one already-authenticated principal, one request
key, two payload values, three submissions, one restart and two observations.
The event model uses two tasks, three events, two reads and one client restart.
The local service model uses two principals, one task each, one scheduled task
lifecycle, two revisions and one crash; the shared traces additionally execute
the second principal. It assumes the separately tested host checkpoint boundary.
Three events is a scenario bound, not the implementation's event-store capacity.
The caller owns its cursor; no subscriber acknowledgement or guaranteed push
delivery is claimed. Authentication, network transports, SDKs and external
effect truth require their separate implementation and deployment evidence.

## Implementation correspondence and review

The original 45-test baseline included 25 library checks, three verifier checks, three CLI checks,
seven gateway checks, four independent assumption regressions and three earlier
independent semantic regressions. They cover the actual codec/SQLite interfaces,
including concurrent admission, terminal races, expired-run latching, full
mailboxes, exhausted budgets, migration, rollback and forged identities.

Native schema-1 history migrates transactionally to schema 2. Expiry and admission
accounting commit separately from a proposed operation, so rejected work cannot
undo them. Closed task requests remain drainable and carry `actionable: false`;
that annotation is a snapshot, not an execution lease. The recovery example
observes an actual one-second deadline after database reopen; deterministic tests
inject the due condition directly to avoid scheduling-dependent assertions.

The independent reviewer identified and verified fixes for the stored-envelope
migration limit and admission timestamp, and checked final model counts, hashes
and counterexample replays. See [independent review](independent-review.md) and
[model contract](../formal/README.md). No formal refinement proof relates every
Rust execution to Promela.

The original model exhausted memory by distinguishing diagnostic markers for
state-preserving rejections. The current abstraction treats those observations
as stuttering while retaining relevant state changes; initial incomplete evidence
remains available. Earlier successful revisions remain in Git history rather
than being presented as current verification results.
