/* Assessment profile v1: two assessment identities, two exact candidate values,
 * two evaluator identities, three delivery attempts, two revision reservations.
 * Atomic blocks model committed host transitions, not evaluator execution.
 * Identity/digest comparisons abstract collision-free SHA256 and typed parsing.
 * Disclosure is two assembled contexts differing only in withheld peer input;
 * trusted workers and all unmediated channels remain outside this model.
 */
mtype = { Completed, Inconclusive, Error };
byte content[2];
byte first_content[2];
byte writes[2];
byte stored_actor[2];
byte stored_candidate[2];
byte stored_target[2];
mtype stored_status[2];
bool stored_passing[2];
byte candidate_content[2];
bool present[2];
byte reservations;
byte completions;
byte dispatches[2];
bool unknown;
bool cancelled;
bool expired;
bool terminal;
byte permission = 1;
byte deadline = 1;
byte remediation_preconditions = 1;
byte context_a;
byte context_b;
byte frozen_context;
byte context_permission;
bool input_recorded;

inline retain(ident, digest, actor, candidate, claimed, status, passing, accepted) {
    accepted=false;
    if
#ifdef BUG_ASSESSMENT_AUTHORITY
    :: true ->
#else
    :: actor == 1 ->
#endif
        if
#ifdef BUG_ASSESSMENT_BINDING
        :: true ->
#else
        :: claimed == candidate_content[candidate] ->
#endif
            if
#ifdef BUG_ASSESSMENT_ERROR_PASS
            :: true ->
#else
            :: status == Completed || !passing ->
#endif
                if
                :: !present[ident] ->
                    present[ident]=true; first_content[ident]=digest; content[ident]=digest;
                    writes[ident]++; stored_actor[ident]=actor; stored_candidate[ident]=claimed;
                    stored_target[ident]=candidate; stored_status[ident]=status;
                    stored_passing[ident]=passing; accepted=true
                :: present[ident] ->
#ifdef BUG_ASSESSMENT_HISTORY
                    content[ident]=digest;
#endif
#ifdef BUG_ASSESSMENT_DUPLICATE
                    writes[ident]++;
#endif
                    if
                    :: content[ident]==digest && stored_target[ident]==candidate && stored_status[ident]==status && stored_passing[ident]==passing -> accepted=true
                    :: else ->
#ifdef BUG_ASSESSMENT_CONFLICT
                        accepted=true;
#endif
                        skip
                    fi
                fi;
                assert(stored_actor[ident]==1); /* ASSESSMENT_AUTHORITY */
                assert(stored_candidate[ident]==candidate_content[stored_target[ident]]); /* ASSESSMENT_CANDIDATE_BINDING */
                assert(content[ident]==first_content[ident]); /* ASSESSMENT_HISTORY_IMMUTABLE */
                assert(writes[ident]==1); /* ASSESSMENT_IDENTITY_DEDUPLICATED */
                assert(stored_status[ident]==Completed || !stored_passing[ident]); /* ASSESSMENT_ERROR_NOT_PASS */
                assert(!accepted || (content[ident]==digest && stored_target[ident]==candidate && stored_status[ident]==status && stored_passing[ident]==passing)); /* ASSESSMENT_CONFLICT_REJECTED */
            :: else -> skip
            fi
        :: else -> skip
        fi
    :: else -> skip
    fi
}

proctype EvaluatorDelivery() {
    byte ident; byte digest; byte actor; byte candidate; byte claimed;
    byte attempt; mtype status; bool passing; bool accepted;
    do
    :: attempt < 3 -> atomic {
        if :: ident=0 :: ident=1 fi;
        if :: candidate=0 :: candidate=1 fi;
        if :: digest=1 :: digest=2 fi;
        if :: actor=1 :: actor=2 fi;
        if :: claimed=1 :: claimed=2 fi;
        if :: status=Completed :: status=Inconclusive :: status=Error fi;
        if :: passing=true :: passing=false fi;
        retain(ident,digest,actor,candidate,claimed,status,passing,accepted); attempt++;
        ident=0;digest=0;actor=0;candidate=0;claimed=0;status=Completed;passing=false;accepted=false;
        }
    :: else -> break
    od;
#ifdef WITNESS_ASSESSMENT_ACCEPTED
    assert(!present[0] && !present[1]); /* REACH_ASSESSMENT_ACCEPTED */
#endif
}

proctype Revision() {
    do
    :: !terminal -> atomic {
#ifdef BUG_REVISION_STALL
        if :: skip ::
#endif
        if
        :: cancelled || expired || unknown || reservations >= 2 -> terminal=true
        :: else ->
            reservations++;
            dispatches[reservations-1]++;
            if :: completions++ :: unknown=true fi;
#ifdef BUG_REVISION_BOUND
            reservations=3;
#endif
#ifdef BUG_REVISION_REPLAY_UNKNOWN
            if :: unknown -> dispatches[reservations-1]++ :: else -> skip fi;
#endif
            assert(reservations <= 2); /* REVISION_EXECUTION_BOUND */
            assert(dispatches[reservations-1] == 1); /* REVISION_UNKNOWN_NOT_REPLAYED */
            if :: completions==2 -> terminal=true :: else -> skip fi
        fi
#ifdef BUG_REVISION_STALL
        fi
#endif
    }
    :: terminal -> break
    od;
#ifdef WITNESS_REVISION_SUCCESS
    assert(completions != 2); /* REACH_REVISION_SUCCESS */
#endif
}

proctype HostControls() {
    if :: cancelled=true :: expired=true :: skip fi;
    /* Feedback includes requests for extra permission/deadline and remediation.
     * It is data: only the defect variants give it control authority. */
    atomic {
#ifdef BUG_FEEDBACK_PERMISSION
        permission=2;
#endif
#ifdef BUG_FEEDBACK_DEADLINE
        deadline=2;
#endif
#ifdef BUG_FEEDBACK_REMEDIATION
        remediation_preconditions=0;
#endif
        assert(permission==1); /* FEEDBACK_PERMISSION_FROZEN */
        assert(deadline==1); /* FEEDBACK_DEADLINE_FROZEN */
        assert(remediation_preconditions==1); /* REMEDIATION_PRECONDITIONS_PRESERVED */
    }
}

proctype InputAssembly() {
    byte withheld_a=1; byte withheld_b=2;
    atomic {
        context_a=1; context_b=1; context_permission=1;
#ifdef BUG_DISCLOSURE_PEER
        context_a=withheld_a; context_b=withheld_b;
#endif
        assert(context_a==context_b); /* WITHHELD_PEER_NONINTERFERENCE */
        frozen_context=context_a; input_recorded=true;
    }
    /* Restart uses the committed snapshot even after live inputs/policy change. */
    atomic {
        withheld_a=2;
#ifdef BUG_DISCLOSURE_REPLAY
        context_a=withheld_a; context_permission=2;
#endif
        assert(context_a==frozen_context && context_permission==1); /* CONTEXT_REPLAY_PRESERVED */
    }
}

init {
    atomic {
        candidate_content[0]=1; candidate_content[1]=2;
        run EvaluatorDelivery(); run Revision(); run HostControls(); run InputAssembly();
    }
}

ltl revision_settles { <> terminal }
