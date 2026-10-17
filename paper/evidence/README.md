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

PostgreSQL integration, the separate host/examples repositories, deployment,
external effects and answer quality were not evaluated. Model checks establish
properties of the declared finite abstractions. Implementation tests support
correspondence; they do not prove that every implementation execution refines
those models. In particular, retained message identity does not establish
exactly-once external effects or improved agent answers.
