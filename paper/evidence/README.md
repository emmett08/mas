# Reproduction record

The checks in this directory were executed on 17 October 2026 against source
commit `a61b3da98ef3ef11996dd79b4a3a048ed6c98ce1`. The checked protocol, model
and example files matched that commit. This is a fresh, deliberately selected
run; it does not reclassify the repository's earlier evidence as newly executed.

| Evidence category | Completed checks | Interpretation |
| --- | ---: | --- |
| Default-feature Rust tests | 79 passed; 0 failed | Executed implementation and verification-runner tests |
| Ignored Rust entrypoints | 3 | Shared-authority, host-evolution and workflow-composition SPIN suites were not executed through these entrypoints |
| Offline executable examples | 3 completed | Star exchange, expiry/recovery and capability-gateway subprocess exercises |
| Core positive model searches | 13 | Complete full-state searches with zero reported errors |
| Core deliberate assertion defects | 6 | Expected assertion violations found and replayed |
| Core deliberate liveness defect | 1 | Expected acceptance cycle found and replayed |
| Unlimited-fault progress claim | 1 | Acceptance-cycle counterexample; eventual acknowledgement does not hold under this assumption |
| Assessment positive model searches | 4 | Complete safety and settlement searches, with and without partial-order reduction |
| Assessment deliberate assertion defects | 13 | Named assertion violations found and replayed |
| Assessment deliberate liveness defect | 1 | Expected settlement-cycle counterexample found and replayed |
| Assessment reachability witnesses | 2 | Deliberately negated target states reached and replayed |

The formal subset comprises 33 cases and 41 property checks: 17 complete
zero-error searches and 24 expected counterexamples or reachability witnesses.
These categories must not be combined into a count of established safety
properties. The assessment runner's `PASS` means that the result expected for
each category occurred and, for counterexamples, the corresponding trail was
replayed.

The star example accepted four messages, charged 748 bytes and reserved no
external calls; all messages were acknowledged and its deterministic checker
accepted the candidate. The recovery example charged one message, retained one
expired task with unknown outcome and returned non-actionable historical
delivery; an identical retry retained sequence one. The gateway example charged
four attempts including a rejected sender forgery, received and acknowledged a
message after restart, and preserved exhaustion across a further restart.
The raw archive retains each executable's exact JSON output. These observations
exercise real Rust/SQLite/stdio paths without a model provider.

The core subset uses the three-participant star topology, two message identities
and two abstract content values. It checks bounded-fault safety and progress at
mailbox capacities two and one, an additional unreduced run, unlimited-fault
safety, selected negative controls, and the separate task-lifecycle,
transaction-recovery and task-expiry models. The assessment subset has two
assessment identities, two candidates, two content values, two evaluators, three
deliveries, two revision reservations, two disclosure worlds and one restart.

The tools were Rust/Cargo 1.99.0, SPIN 6.5.2 and GCC 13.3.0 on Linux x86-64.
Model commands use a 30-second, 512-MiB limit, full-state storage and depth
100,000. Neither bitstate hashing nor a statistical sample substitutes for a
positive exhaustive search. Timings record this execution; they do not measure
comparative protocol performance.

## Reproduce

Install Rust/Cargo, SPIN and a C compiler on Linux, then run from the repository
root:

```sh
python3 paper/scripts/reproduce-evidence.py
```

Add `--offline` when the locked Cargo dependencies are already cached. Use
`--out /tmp/mas-paper-evidence` to retain a separate run. The script writes an
interrupted status before starting and records failed or incomplete runs.

`verification.json` records source hashes, tool versions, exact commands,
outcomes and the SHA-256 of `raw-checks.zip`. The archive contains logs, generated
models, replay trails and the existing assessment runner's detailed summary.
Generated C verifiers and executables are omitted because they can be recreated
from the models and commands.

The script generates `results.tex` from `verification.json` for inclusion in the
manuscript. It keeps native tests, executable fixtures, positive model searches,
deliberate defects, assumption counterexamples and reachability witnesses
separate. The command `make -C paper verify` runs the same reproduction script.

These implementation checks cover the default SQLite backend and the selected
finite abstractions. Implementation tests supply correspondence evidence for
the exercised behaviours. Deployment, PostgreSQL integration and external-effect
control require their own qualification. Application outcome measures assess
answer quality separately.

## Incident-analysis integration

The article also uses the retained scripted-provider rehearsal from
[`mas-host-examples` revision `f086e3e`](https://github.com/emmett08/mas-host-examples/tree/f086e3e51d4c18c2ff851a00b70422bdbfd8aa50/experiments/incident-revision).
Its execution was recorded on 16 October 2026. The article's analysis inspects
that archive and recomputes its counts. The original execution and this later
archive inspection remain separately identified.

`incident-rehearsal.json` records the immutable source, archive and receipt
identities and the output of `scripts/analyse-incident-rehearsal.py`.
The verifier reads archive members in memory. It checks all 1,084 manifest
entries, 14 frozen source hashes, the allocation of 108 jobs, invocation inputs
and outputs, 324 provider effects, 288 retained revision records and their
references, and the descriptive report calculations. All 36 revision draft
inputs match their paired one-pass inputs.

| Procedure | Completed runs | Generation calls | Assessment calls | Selected candidate |
|---|---:|---:|---:|---|
| One pass | 36 | 36 | 0 | First |
| Revision | 36 | 72 | 72 | Revised candidate |
| Resampling | 36 | 144 | 0 | Fourth |

Every procedure had the same four-call upper limit. The scripted generator
always requested further evidence and the critic returned inconclusive findings.
The quality scores therefore describe the authored cases and scripted responses.
The figure displays the observed procedures and their information dependencies.
The full outcome counts remain in the derived JSON for inspection.

The two archived reports are identical. The execution receipt records
completed-job reuse with zero additional provider effects. This analysis
verifies report equality against the retained final snapshot. The receipt
supplies the original replay observation.

The cited examples checkout pins host source revision
`bfcd27444f81e2323739e5abfbc30bfa3e672b0a`. That source pins protocol revision
`239e85c5ad1ab4f25df958365c50019c8b91fe15`. The original rehearsal identifies
its executed host by SHA-256
`2d9cb2a2716e3fdc30bd2562eca8e533055b59a4ca5cad67b0067c3cb8311479`.
The archive preserves executable hashes and invocation records. The external
source workflow rebuilds the omitted binaries and SQLite journals. These
identities preserve the distinction from the core checks at `a61b3da`.

The permitted feedback path uses wildcard readers in this fixture. The scorer
compares exact diagnosis codes, a case-specific list of hazardous actions and
the reference abstention decision after generation. Usage and prices remain
unavailable. Timing reflects four concurrent fixture workers. These observations
support the bounded integration example in the article.

The experiment's 13 unit tests passed during this review. Its schema-1.2
investigation record validates with zero errors and zero warnings at status
`specified`. The scientific review retains the six incident families as the
paired analysis units. A principal empirical comparison requires independently
adjudicated labels and an allocation justified for its quality and resource
margins. A quality-aware resampling selector would define a further comparator.
