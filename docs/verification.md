# Verification results

## Consolidated implementation — 15 October 2026

The single review is [PR #45](https://github.com/emmett08/mas/pull/45), against
`main`; PRs #16–44 are superseded. Implementation and fault qualification continue.
The earlier records below remain historical evidence for their recorded source.

The subsequent combined revision `d8c04fab1adc2218beabfaaf2637b0728cbde311`
passed **all 104 shared Python tests without skips**, 65 native contract tests,
13 actual PostgreSQL cases and all six real PostgreSQL/RustFS boundaries. The
Python backend suite took 922.426 seconds. This includes both repaired fixtures,
the new peer-policy roundtrip, backup/restore, archival, native transfer IDs and
offsets across empty replacement caches, epoch fencing, revocation and tampering.
The same revision passed all five independent-VM fault cases, portable clients
on three operating systems, real containment, 110 SPIN cases and security gates.
Its tested merge `bcd0a51085f51fd42066b31a5d6f803d94840ded` has the exact
submitted tree `5b0c8cead1826e15e0f46b97fd162da11431d5cb`.

Its complete amd64 image workflow passed: 1,125 native tests (116 skips),
1,108 installed musl tests (126 skips), 1,120 Cursor-image tests (119 skips),
44 script tests, 26 real Temporal tests, 36 MCP tests and 35 A2A tests.
Reconciliation, private secret mounts and supervised bridge restart recovery
also passed. Skips identify profile-specific backend or dependency gates;
the actual PostgreSQL suite above ran without skips.

The arm64 native and musl suites passed. Its Cursor suite exposed a race only in
the new bootstrap-expiry test: the one-second fixture deadline could expire
before bootstrap, so the test's final manifest assertion failed despite correct
expiry and zero effects. Test-only repair `016b489` uses a shared controlled clock
that advances after the real bootstrap retains its run identity. All six scope
tests and independent review passed. A negative control still fails against the
original buggy executor, confirming that the test detects the production defect.
Production code is unchanged by this repair; final-head image qualification is
still required. The [receipt](verification/host-evolution-source.json) records
these revision-specific results; the current PR checks report the final run.

Revision `71e4d87dd661152d0202cbd76f23d8d524e64724` passed all five
independent-kernel VM fault cases, the native Linux/macOS/Windows clients,
real Linux containment, formal checks and dependency/image security gates.
The tested merge `fc9f0a2d5912f0fbf294f60079e2eacf96aa55cd` has the same
source tree, `6936ca2d2854426d834bdfcdbafdb5a7e7db33fc`. The VM run includes
the repaired SQLite durability and workspace dispatch. The lost-acknowledgement
case preserves the original receipt across controller restart and worker hard
reset, with guest disk flushes enabled. The three uncertain-effect cases retain
their original unknown state and allocations; their explicit second probes are
refused before dispatch. This qualifies the recorded QEMU profile.

Its PostgreSQL run passed all **13 native backend tests** and **100 of 102
Python shared tests** in 528.996 seconds. Two test fixtures failed: composition
children used a process-local object-store dictionary, and a one-second expiry
could elapse before inspection. Commits `18c4eb6` and `cd45eda` repair those
fixtures without changing production code. The first uses process-visible,
immutable object bytes and verifies retained workspace manifests and native
invocation records. The second expires only its isolated row using the actual
database clock and checks denied access, removed chunks and released quota.
The combined PostgreSQL/RustFS stage was skipped after these errors; the next
combined-head run must qualify both repairs and that real backend stage.
The [source receipt](verification/host-evolution-source.json) retains the failed
run as well as the passing evidence. Final-head results are linked from PR #45.

The same revision completed the full **arm64** workflow: native host, installed
musl and Cursor images, real Temporal, independent reconciler, private credential
injection and bridge restart. On **amd64**, the native suite and Temporal passed;
the installed musl suite reported one timeout error and one nested-deadline
failure among 1,223 tests (125 skips). Its later image/deployment stages were
skipped. These failures are retained in the receipt and require a repaired-head
rerun. The native suite ran 1,230 tests on each architecture, with 1,115 passed
and 115 fixture/optional-dependency skips; separate gates exercise those profiles.

The subsequent repair `980eb3e` classifies a stopped runner construction from
retained bootstrap/invocation evidence: unadmitted work can stop, while an
original unknown invocation remains unknown. Its 82-test source supplement
passed, including deterministic expiry during bootstrap and both previous musl
failures. The worker deadline fixture now arms after durable invocation admission;
a separate regression verifies that timeout before dispatch creates no call.
Repair `a3ef8f3` binds managed-peer public transport policy and timeout to an
immutable registration in both repositories. Legacy direct/default registrations
retain their exact identities, credential rotation remains separate, and changed
client configuration is rejected before credentials or effects. Its 22-test
source supplement and native validator test passed; the real PostgreSQL
roundtrip is included in the final combined-head gate. These focused checks
supplement the earlier full runs and do not replace the final installed-image run.

The hosted fixed load gate passed on that amd64 source before the later musl
failures: **101 completed tasks**, 51 for Alice and 50 for Bob, over **60.67
seconds** of active load. Submission p99 was 44.55 ms, inspection p99 8.46 ms,
and completion-observation p99 770.57 ms. It retained exactly 101 admissions and
calls, no paid reservations, and no observed orphan processes. The receipt binds
the clean tested source, binaries, bounds and CI artifact/report digests.

The fixed checkout `c623904660e17c5ce4c552246d67e4fa90bc8290` ran **1,221
host tests** in 273.931 seconds: **1,119 passed and 102 skipped**. The skips are
99 actual-PostgreSQL tests and three explicitly enabled real Temporal tests,
which have separate hosted gates. This run used Python 3.12.14, installed pinned
SDKs and freshly built Rust 1.99.0 binaries from the same native tree. Native
checks passed 64 tests, with 13 PostgreSQL and two formal-verifier cases reserved
for their actual fixtures; 44 script tests passed. The [source receipt](verification/host-evolution-source.json)
records source, binary and log digests and earlier integrated runs.
The subsequent local Cursor recovery and locking repairs at `eb4fdb8` passed
a 44-test supplement in 3.856 seconds, with independent concurrency review.
The complete published-head CI run covers the combined implementation.

The fixed [two-principal load profile](reference-load.md) passed at
`c623904660e17c5ce4c552246d67e4fa90bc8290`: 112 tasks completed, 56 per
principal, over 60.10 seconds of active load. Every callback had one paired
completion and every observed descendant was reaped. Submission p99 was 26.23 ms,
inspection p99 6.05 ms and completion-observation p99 450.38 ms. Peak observed
resource use was five descendant processes and 94.68 MiB summed RSS; retained
allocated state grew by 12.34 MiB. Deterministic capacity checks exercised all
four slots and principal/global denials. The paced workload's observed callback
concurrency was one, so this result does not establish saturated throughput.
The [aggregate receipt](verification/host-reference-load.json) binds source,
binaries, limits, raw-report digest and samples. The host regression suite ran
concurrently. These are fixture measurements, not production SLOs; CI repeats
the same bounded profile on the published source.

At remote revision `6c120aa487546e8d5bd4d63981978c4d168bbe44`, **59 actual
PostgreSQL Python tests passed**, together with 12 native PostgreSQL conformance
tests and the native accounting migration test. Five combined PostgreSQL and
RustFS boundaries passed: verified publication, lost PUT response, epoch
handover, grant revocation and corrupt bytes. This supersedes the earlier
50-of-51 PostgreSQL result at `7064567749466168721db184678f1baabcea4aa3` for
those tests; it does not qualify the later shared jobs, retention and transfers.

At `b7b84f2436a531fcd80ae6d81422c1b935c43867`, the complete installed-image
workflows passed on **amd64 and arm64**, including standard and Cursor runtimes,
real Temporal, bridge restart, reconciliation and private credential injection
without paid calls. Native Linux/macOS/Windows clients, real Linux containment,
formal checks and dependency/image scans passed. Its PostgreSQL phase ran 89
Python tests: 88 passed; one transfer-denial test errored because the native fixed
error code was absent from the public allowlist. The denial held. That repair and
the subsequent schema/workspace changes need the combined-head rerun; the S3
phase was skipped after this error.

The same revision passed all five independent-kernel VM cases: two workers,
lost acknowledgement plus controller restart and worker hard reset, network
partition, spool loss and artefact loss. The [machine-readable receipt](verification/host-evolution-source.json)
records the actual tested merge revision and retained CI artifact digest. These
are two QEMU guests, not physical-host or production-network acceptance. This
passing run predates the durability repair below and cannot qualify later source.

Independent VM hard resets exposed two real SQLite durability defects: a
read-only preflight could not recover a hot journal, and `synchronous=FULL`
with rollback journals could lose the worker's last committed receipt. Writable
preflight now permits SQLite recovery while prohibiting application writes;
all Python and Rust write owners use `synchronous=EXTRA`. A separate ledger
preflight regression verifies preserved identity, policy and allocations. The
fresh VM rerun at `71e4d87` passed with the original receipt-ID assertion and
verified that QEMU honours flushes. The canonical
[implementation record](host-evolution-plan.md) retains the acceptance conditions.

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

Hosted CI then exposed two tests that imported optional SDKs in the base
dependency phase. Those tests now skip only when their SDK is absent; the
installed-SDK workflow already discovers both files. The affected 22-test suite
passed with exactly two skips in an actual environment without those SDKs and
with no skips in the provisioned Python 3.14 environment. These test guards did
not change runtime code.

The installed one-CPU container also exposed a timing mismatch in the real-worker
success test: its one-second dispatch lease was shorter than the registered
executor's five-second budget. That test now allows ten seconds for execution and
receipt delivery. Five repetitions passed with inherited single-CPU affinity;
five unchanged deterministic expiry, fencing, capacity and lost-reply tests also
passed. Production lease and uncertainty rules are unchanged. Exact installed
image acceptance is recorded by the corresponding hosted CI revision.

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
