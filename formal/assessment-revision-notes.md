# Assessment and revision model

`assessment-revision.pml` checks finite control properties for the
[`assessment.v1` contract](../docs/assessment-contract.md) and its host use.
It does not establish the truth of findings, measured improvement, real-time
deadline guarantees or unrestricted implementation refinement.

## Domain and abstraction

The model has two assessment identities, two candidate identities, two content
values, two evaluator identities, three delivery attempts, two revision
reservations, one cancellation/expiry choice and one input replay. Candidate
identity is chosen independently of assessment identity, so retargeting is
reachable. Each submission may be a duplicate, conflicting content, foreign
evaluator, wrong candidate reference or an invalid passing error result.
The retained content symbol abstracts a complete record digest; explicit target,
evaluator, status and pass fields preserve the distinctions needed for the
identity, authority and error-handling questions.

Atomic blocks represent committed host transitions. Request-field selection
and consumed local variables are unobservable between commits, so the model
collapses their assembly and resets them after submission. This preserves all
request combinations and interleavings between durable operations. An earlier
unreduced representation retained those transient combinations and exceeded
512 MiB; it was inconclusive. The reduced representation has the same declared
identity, content, fault and reservation bounds. This is an explicit abstraction
choice, not evidence that the real implementation is atomic across execution.

Finite evaluator responses either complete or become unknown. A continuation
cannot redispatch an unknown reservation. Cancellation and expiry are explicit
host observations. No clock rate, guaranteed polling or network availability
is modelled. Liveness requires that the host continues the finite transitions;
the model contains no permanent external call. The deliberate stall variant
removes that condition and must exhibit an acceptance cycle.

Disclosure uses self-composition: two host input assemblies have identical
permitted input and policy but different withheld peer findings. Equality of
assembled input establishes the finite noninterference property. A restart
reuses the frozen input and policy even when live values change. This covers
host-managed channels only. Shared files, ambient credentials, provider memory,
timing side channels and malicious worker behaviour are outside the claim.

## Properties and correspondence

| Property | Concrete operation or implementation test |
| --- | --- |
| Assessment authority | `validate_assessment` compares frozen evaluator; envelope sender check; `public_validator_rejects_wrong_references_authority_metrics_and_criteria` |
| Exact candidate binding | Full-record `DigestRef` resolution; wrong and stale candidate tests |
| Conflict rejected | Exact identity/content retention; `history_deduplicates_exact_content_and_retains_conflicting_findings` |
| History immutable, duplicate not counted | First retained record preserved; identical retry returns no insertion |
| Error cannot pass | `Assessment::from_value` and public contextual validator; `evaluator_error_inconclusive_and_failed_criterion_remain_distinct` |
| Revision reservations bounded; unknown not replayed | `mas-host` durable revision reservation and recovery tests |
| Feedback cannot alter permissions or deadline | `mas-host` frozen revision plan and invocation policy tests |
| Remediation preconditions preserved | Revision executors provide analysis only; `mas-host-examples` keeps remediation admission and effect verification outside revision |
| Withheld peer input cannot affect assembly | `mas-host` invocation-specific assembly and peer-history tests |
| Replay preserves original context/policy | `mas-host` durable invocation snapshot and restart tests |
| `revision_settles`: `<> terminal` | Finite completion, cancellation, observed expiry, unknown or exhaustion |

The cross-repository entries are implementation obligations for the coordinated
host/examples changes. This repository alone cannot discharge them. The
mapping supports review and test correspondence, not a proof that arbitrary
Rust/SQLite/provider executions refine the Promela model.

The runner checks safety and `revision_settles` separately with and without
partial-order reduction, samples seeds 17, 29 and 53, and runs thirteen isolated
safety defects, one stall defect and two reachability witnesses. Each safety
defect must replay its named assertion; an unrelated failure cannot satisfy it.
The witnesses establish that accepted assessments and successful two-candidate
completion are reachable. A witness PASS means the deliberate assertion failed
on a reachable state; it is distinct from a positive safety result.

## Reproduce and interpret

```sh
cargo run --locked --manifest-path rust-protocol/Cargo.toml --bin verify -- \
  --assessments-only --out formal/evidence/assessments
```

The existing verifier enforces 30 seconds and 512 MiB per command, full-state
storage and depth 100,000. It retains tool versions, exact model hashes,
commands, raw logs and replay trails. Zero errors supports a claim only when
the search completed. Memory, depth, compilation and timeout failures are
inconclusive or failed execution. The ordinary full verifier and existing CI
include these cases, so the profile is not an optional unexecuted model.

Reference semantics were checked against the official
[SPIN command manual](https://spinroot.com/spin/Man/Spin.html),
[generated verifier manual](https://spinroot.com/spin/Man/Pan.html) and
[inline LTL manual](https://spinroot.com/spin/Man/ltl.html).
