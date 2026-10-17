# Hybrid-host admission, agreement and recovery contracts

`hybrid-contracts.pml` checks the four incremental contracts added after the
hybrid-topology review. It complements the existing admission, assessment,
workflow and native-host models. It does not prove that an agent's diagnosis is
correct, that two evaluators are statistically independent, or that revision
improves a measured outcome.

## Boundary, bounds and atomicity

Each execution nondeterministically selects one of four independent services:
reply-route admission, gateway projection/observation, candidate-bound quorum,
or durable assessment/revision recovery. This separation avoids a product of
unrelated states. It establishes no cross-service concurrency or end-to-end
refinement property. Implementation regressions exercise those boundaries
separately.

The finite domain contains two actor identities, two run identities, one retained
message identity, two values for each candidate/context/evidence component,
three quorum delivery attempts, a quorum threshold of two distinct actors, one
assessment identity with two proposed record contents, two revision reservations
and at most one process restart. The production revision ceiling can be up to
32; the model's smaller ceiling exposes allocation and recovery mistakes without
claiming arbitrary-scale coverage.

Atomic sections represent single repository/journal commits or construction of a
read-only response from a snapshot. Reservation commit, external dispatch and
result commit are separate transitions. A restart before a durable terminal
result conservatively blocks the unresolved reservation as unknown; a restart
after a result commit reuses the retained assessment and original allocation.
The result symbol abstracts any retained terminal assessment, including a
retained Error that must not be recomputed from a completed worker checkpoint.
SQLite/PostgreSQL atomic recovery, trusted host actor identity, exact JSON types
and collision-free full-record digests are assumptions. No network
cryptography, storage corruption, real-time clock or fairness claim is made.

## Named properties and implementation mapping

| Model property | Implementation correspondence and executable regressions |
| --- | --- |
| `REQUEST_REPLY_ROUTE_PREFLIGHT` | `tasks::response_route_permitted`, called by `Store::send_checked` and `PostgresRepository::send_checked` before allocation of a new task request; `one_way_task_request_refuses_before_message_task_or_call_allocation` |
| Routable requests, one-way notices and retained duplicates remain usable | `retained_one_way_task_request_remains_an_identical_resend_after_restart`; separate reachability witnesses for a new bidirectional Request and a one-way generic Notice |
| `GATEWAY_CLASSIFICATION_PRESERVED`, `GATEWAY_UNKNOWN_PRESERVED`, `GATEWAY_RETRY_PROVES_ABSENT` | Versioned `mas-admission/1` checked-send projection; `checked_gateway_distinguishes_capacity_identity_and_uncertain_storage_without_diagnostics` |
| `GATEWAY_DIAGNOSTIC_WITHHELD` | Checked gateway failures expose profile/code/disposition, without raw operational diagnostics; same regression and `persistent_gateway_preserves_versioned_admission_and_private_identity_observation` |
| `GATEWAY_OBSERVATION_SCOPED`, `GATEWAY_OBSERVATION_READ_ONLY` | `ProtocolRepository::reconcile_message`, `Store::reconcile_message`, `PostgresRepository::reconcile_message`: exact run/sender/message snapshot; `actor_scoped_reconciliation_discloses_only_own_receipt_and_absence_allows_later_commit` and `identity_observation_does_not_receive_acknowledge_or_expire_and_validates_actor` |
| `QUORUM_CANDIDATE_BOUND`, `QUORUM_CONTEXT_BOUND`, `QUORUM_EVIDENCE_BOUND` | `mas-candidate-quorum/1` compares exact candidate and complete frozen context; native `Quorum::Binding::finish` and Python `PlanEvaluator._parallel_join`; native `conflicting_candidates_and_stale_review_contexts_cannot_form_a_quorum` and `quorum_context_comparison_preserves_exact_integers_and_json_types` |
| `QUORUM_DISTINCT_EVALUATORS` | Native and Python plan binding reject direct branch target actor aliases before dispatch; this is stronger than the model's count-once abstraction. Native `candidate_quorum_retains_all_reviews_and_selects_distinct_actors_in_plan_order` and `quorum_binding_is_validated_before_dispatch_and_actor_aliases_cannot_add_votes` |
| `DURABLE_ASSESSMENT_IMMUTABLE` | Python `AssessmentJournal._record`, frozen binding and exact reference validation; `test_exact_duplicate_and_conflicting_identity_survive_reopen`, `test_error_assessment_is_retained_and_never_passes`; native `Inner::retained_assessment` and `restart_reuses_terminal_error_assessment_without_reclassifying_completed_worker_output` |
| `DURABLE_RESERVATION_RESTORED`, `DURABLE_REVISION_BOUND` | Transactional `AssessmentJournal._reserve_revision`, native durable revision plan/attempt reservation; `test_feedback_bound_is_checked_before_reservation_is_consumed`, `test_revision_replays_original_feedback_and_frozen_context_after_restart` |
| `DURABLE_UNKNOWN_NOT_REDISPATCHED`, `DURABLE_DISPATCH_WITHIN_RESERVATIONS` | Recovery preserves the exact pre-recovery dispatch count, with a separate aggregate reservation bound; Python `test_unknown_revision_execution_replays_original_context_without_new_call`; native recovery uses retained terminal assessment rather than another evaluator call |
| `DURABLE_AUTHORITY_FROZEN`, `ASSESSMENT_NOT_EXECUTION_AUTHORITY` | Host-owned registration policy/original deadline, feedback treated as data, remediation admission checked separately; Python `test_payload_cannot_choose_evaluator_rubric_or_stale_reference`; native `exact_evidence_and_frozen_authority_are_required_and_inconclusive_is_preserved` |
| `hybrid_contract_settles`: `<> settled` | Each selected finite service reaches its declared accepted, rejected, unknown or completed outcome if host execution continues. This is not eventual external success. |

The model separates `context` and `evidence` solely to vary two components of a
complete context independently. Implementations compare the entire frozen
application-supplied context object; they do not mandate particular rubric,
criterion or evidence field names. Evidence binding therefore covers references
included in that context. Applications must choose a context containing the
identities needed for their agreement decision. Neither exact equality nor a
passing Boolean proves substantive correctness. The distinct actors are trusted
configured execution identities, not untrusted evaluator names claimed in output.

The gateway starts from repository classifications. The model tests their
projection and handling, not the unmodelled internals of an arbitrary database
failure. Unknown may represent a durable commit with a lost reply. A scoped
observation is a single snapshot: absence cannot exclude a later commit and does
not authorise repetition of an uncertain external effect. Route preflight applies
to a new `agent-task/1` Request; exact retained retransmission remains a receipt
and generic messages can retain one-way routes.

The cross-repository native-host entries require that repository's tests and
its separate `formal/revision.pml` and `formal/disclosure.pml` checks. No result
from this repository alone establishes their implementation conformance. The
mapping is a review aid and finite regression correspondence, not a refinement
proof.

## Execution and controls

```sh
cargo run --locked --manifest-path rust-protocol/Cargo.toml --bin verify -- \
  --hybrid-contracts-only --out /tmp/mas-hybrid-contracts
```

The retained [verification receipt](../evidence/hybrid-contracts/verification.json)
and [compressed raw evidence](../evidence/hybrid-contracts/spin-checks.zip) record
26 cases and 28 checks: four complete positive safety/settlement searches,
seventeen expected safety counterexamples, one expected stall cycle and six
reachability witnesses. All expected assertions and the stall cycle were replayed.
The positive safety searches with and without reduction each stored 514 states;
the settlement searches each stored 422 states. These counts describe this exact
finite model and tool configuration.

The default protocol verifier includes this subset. The runner samples seeds 17,
29 and 53, then independently checks safety and settlement with and without
partial-order reduction. It uses full-state storage, a depth limit of 100,000,
512 MiB and 30 seconds per command; depth, memory or time exhaustion is
inconclusive. Raw logs preserve tool versions, exact command arguments, source
hashes, state/transition counts and unreached-state reports.

Seventeen safety mutations independently remove request preflight; collapse
retryable or unknown outcomes; disclose operational diagnostics; remove actor/run
scope; mutate observation state; weaken candidate/context/evidence binding or
distinct actor counting; overwrite or lose durable history; lose a reservation;
redispatch after recovery; change frozen authority; or let assessment acceptance
grant effect authority. One deliberate stall must exhibit a replayable acceptance
cycle. Six reachability controls establish routable requests, one-way notices,
reconciliation of a committed unknown send, a bound distinct-actor quorum,
successful bounded revision and replay of a retained terminal assessment under
its original allocation. Each safety defect must replay its named assertion;
an unrelated failure cannot satisfy the control. A witness PASS means the
intentionally false reachability assertion failed on a reachable state, distinct
from a positive safety result.

Commands and interpretation follow the official
[SPIN command manual](https://spinroot.com/spin/Man/Spin.html),
[verifier manual](https://spinroot.com/spin/Man/Pan.html) and
[inline LTL manual](https://spinroot.com/spin/Man/ltl.html).
