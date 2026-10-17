/* Incremental hybrid-host contracts. One nondeterministically selected service
 * per execution avoids an irrelevant product of independent service states.
 * Each service has finite requests and no environment fairness assumption.
 * Digests denote exact, collision-free, typed whole-record references.
 * Crash points separate committed reservation, dispatch and response commit.
 */
mtype = { Accepted, Retryable, Rejected, Unknown,
          Ack, Capacity, Conflict, Operational };
byte scenario;
bool settled;
bool accepted;
bool committed;
bool retained;
bool request;
bool reverse_route;
mtype source;
mtype disposition;
bool secret_returned;
byte query_actor;
byte query_run;
bool found;
byte revision=1;
byte before_revision;
byte actor;
byte candidate;
byte context;
byte evidence;
bool completed;
bool passing;
bool counted;
bool seen[2];
byte votes;
byte deliveries;
byte history=1;
byte frozen_history=1;
byte proposed_history;
byte reservations;
byte durable_reservations;
byte dispatches;
byte before_dispatches;
bool result_retained;
bool unknown;
bool recovered;
bool cancelled;
bool expired;
byte permission=1;
byte frozen_permission=1;
bool remediation_authorised;
bool remediation_dispatched;

inline choose(value) { if :: value=true :: value=false fi }

init {
    if :: scenario=1 :: scenario=2 :: scenario=3 :: scenario=4 fi;
    if
    :: scenario==1 ->
        choose(request); choose(retained); choose(reverse_route);
        atomic {
            if
            :: retained -> accepted=true /* receipt for exact retained identity */
#ifdef BUG_REPLY_PREFLIGHT
            :: !retained -> accepted=true
#else
            :: !retained && (!request || reverse_route) -> accepted=true
            :: !retained && request && !reverse_route -> accepted=false
#endif
            fi;
            assert(retained || !accepted || !request || reverse_route); /* REQUEST_REPLY_ROUTE_PREFLIGHT */
        };
#ifdef WITNESS_REPLY_REQUEST
        assert(!(request && !retained && reverse_route && accepted)); /* REACH_ROUTABLE_REQUEST */
#endif
#ifdef WITNESS_ONE_WAY_NOTICE
        assert(!(!request && !retained && !reverse_route && accepted)); /* REACH_ONE_WAY_NOTICE */
#endif
    :: scenario==2 ->
        /* Repository distinguishes accepted, proven nonadmission, and unknown
         * outcomes. An operational error can occur before or after commit. */
        atomic {
            if
            :: source=Ack; disposition=Accepted; committed=true
            :: source=Capacity; disposition=Retryable; committed=false
            :: source=Conflict; disposition=Rejected; committed=false
            :: source=Operational; disposition=Unknown; choose(committed)
            fi;
#ifdef BUG_GATEWAY_CLASSIFICATION
            if :: source==Capacity -> disposition=Rejected :: else -> skip fi;
#endif
#ifdef BUG_GATEWAY_UNKNOWN
            if :: source==Operational -> disposition=Retryable :: else -> skip fi;
#endif
#ifdef BUG_GATEWAY_DISCLOSURE
            secret_returned=true;
#endif
            assert((source!=Capacity || disposition==Retryable) && (source!=Conflict || disposition==Rejected)); /* GATEWAY_CLASSIFICATION_PRESERVED */
            assert(source!=Operational || disposition==Unknown); /* GATEWAY_UNKNOWN_PRESERVED */
            assert(disposition!=Retryable || !committed); /* GATEWAY_RETRY_PROVES_ABSENT */
            assert(!secret_returned); /* GATEWAY_DIAGNOSTIC_WITHHELD */
        };
        if :: query_actor=1 :: query_actor=2 fi;
        if :: query_run=1 :: query_run=2 fi;
        atomic {
            before_revision=revision;
            /* Durable identity belongs to run 1 and actor 1. */
#ifdef BUG_GATEWAY_ACTOR_SCOPE
            found=committed && query_run==1;
#else
#ifdef BUG_GATEWAY_RUN_SCOPE
            found=committed && query_actor==1;
#else
            found=committed && query_actor==1 && query_run==1;
#endif
#endif
#ifdef BUG_GATEWAY_OBSERVER_WRITE
            revision++;
#endif
            assert(!found || (query_actor==1 && query_run==1)); /* GATEWAY_OBSERVATION_SCOPED */
            assert(revision==before_revision); /* GATEWAY_OBSERVATION_READ_ONLY */
        };
#ifdef WITNESS_GATEWAY_COMMITTED_UNKNOWN
        assert(!(source==Operational && committed && disposition==Unknown && found)); /* REACH_COMMITTED_UNKNOWN_RECONCILED */
#endif
    :: scenario==3 ->
        do
        :: deliveries<3 -> atomic {
            if :: actor=0 :: actor=1 fi;
            if :: candidate=1 :: candidate=2 fi;
            if :: context=1 :: context=2 fi;
            if :: evidence=1 :: evidence=2 fi;
            choose(completed); choose(passing);
            counted=false;
            if
            :: completed && passing && votes<2 ->
                if
#ifdef BUG_QUORUM_CANDIDATE
                :: context==1 && evidence==1 ->
#else
#ifdef BUG_QUORUM_CONTEXT
                :: candidate==1 && evidence==1 ->
#else
#ifdef BUG_QUORUM_EVIDENCE
                :: candidate==1 && context==1 ->
#else
                :: candidate==1 && context==1 && evidence==1 ->
#endif
#endif
#endif
                    if
#ifdef BUG_QUORUM_DISTINCT
                    :: true ->
#else
                    :: !seen[actor] ->
#endif
                        votes++; seen[actor]=true; counted=true
                    :: else -> skip
                    fi
                :: else -> skip
                fi
            :: else -> skip
            fi;
            assert(!counted || candidate==1); /* QUORUM_CANDIDATE_BOUND */
            assert(!counted || context==1); /* QUORUM_CONTEXT_BOUND */
            assert(!counted || evidence==1); /* QUORUM_EVIDENCE_BOUND */
            assert(votes==(seen[0]+seen[1])); /* QUORUM_DISTINCT_EVALUATORS */
            deliveries++;
            actor=0;candidate=0;context=0;evidence=0;completed=false;passing=false;counted=false;
        }
        :: deliveries==3 -> break
        od;
#ifdef WITNESS_BOUND_QUORUM
        assert(votes!=2); /* REACH_BOUND_DISTINCT_QUORUM */
#endif
    :: scenario==4 ->
        /* A durable immutable finding survives process restart. A conflicting
         * retry is rejected instead of rewriting the retained record. */
        if :: proposed_history=1 :: proposed_history=2 fi;
        atomic {
#ifdef BUG_DURABLE_HISTORY
            history=proposed_history;
#endif
            assert(history==frozen_history); /* DURABLE_ASSESSMENT_IMMUTABLE */
        };
        do
        :: reservations<2 && !unknown && !cancelled && !expired ->
            atomic { reservations++; durable_reservations=reservations };
            /* Crash after reservation leaves a possibly dispatched operation.
             * Recovered running reservation is unknown and never relaunched. */
            if
            :: !recovered -> recovered=true; unknown=true
            :: dispatches++;
               if
               :: !recovered -> recovered=true; unknown=true
               :: result_retained=true;
                  if :: !recovered -> choose(recovered) :: recovered -> skip fi
               fi
            fi;
            before_dispatches=dispatches;
            if
            :: recovered ->
#ifdef BUG_DURABLE_HISTORY_LOSS
                history=0;
#endif
#ifdef BUG_DURABLE_RESERVATION
                reservations=0;
#endif
#ifdef BUG_DURABLE_UNKNOWN_REPLAY
                dispatches++;
#endif
                assert(reservations==durable_reservations); /* DURABLE_RESERVATION_RESTORED */
                assert(dispatches==before_dispatches); /* DURABLE_UNKNOWN_NOT_REDISPATCHED */
                assert(dispatches<=durable_reservations); /* DURABLE_DISPATCH_WITHIN_RESERVATIONS */
                assert(history==frozen_history); /* DURABLE_ASSESSMENT_IMMUTABLE */
            :: else -> skip
            fi;
            assert(reservations<=2); /* DURABLE_REVISION_BOUND */
            choose(cancelled); choose(expired)
        :: else -> break
        od;
        /* Peer feedback cannot alter frozen permissions. A passing assessment
         * is one evidence item, never a grant of remediation authority. */
        atomic {
#ifdef BUG_DURABLE_AUTHORITY
            permission=2;
#endif
            assert(permission==frozen_permission); /* DURABLE_AUTHORITY_FROZEN */
            choose(remediation_authorised);
#ifdef BUG_ASSESSMENT_AUTHORISES_EFFECT
            remediation_dispatched=result_retained;
#else
            remediation_dispatched=result_retained && remediation_authorised;
#endif
            assert(!remediation_dispatched || remediation_authorised); /* ASSESSMENT_NOT_EXECUTION_AUTHORITY */
        };
#ifdef WITNESS_DURABLE_ASSESSMENT_REPLAY
        assert(!(result_retained && recovered && !unknown && dispatches==durable_reservations)); /* REACH_DURABLE_ASSESSMENT_REPLAY */
#endif
#ifdef WITNESS_DURABLE_REVISION
        assert(!(reservations==2 && dispatches==2 && result_retained && !unknown)); /* REACH_DURABLE_REVISION_SUCCESS */
#endif
    fi;
#ifdef BUG_HYBRID_STALL
    do :: !settled -> revision=3-revision od;
#endif
    settled=true
}

ltl hybrid_contract_settles { <> settled }
