/* Generic guarded operation with an immutable attempt report and separately
 * appended observations. The adapter alone supplies admission, atomic guarded
 * execution and postcondition evidence; no remediation-specific wire profile.
 *
 * One operation/attempt, one durable reservation, one owner, recovery and two
 * reconcilers. Reservation and report/observation CAS writes are atomic. A
 * reserved original executor may act after recovery reports unknown: terminal
 * reporting is not external fencing. Response loss, crash before/after effect,
 * lost reservation ACK, competing observation CAS and mismatched bindings are
 * nondeterministic. No retry of the external effect is authorised.
 *
 * Two binding values abstract exact operation/attempt/digest/UID/template
 * comparison, not collision resistance. Live desired/original/changed state
 * can have another cause. No observed state rewrites the historical report.
 */
mtype = { NotStarted, Rejected, Applied, Unknown,
          NotRun, Passed, Failed, Inconclusive,
          Desired, Original, Changed, Unavailable,
          Added, Duplicate, Conflict, Invalid };
mtype effect_outcome=NotStarted;
mtype verification_outcome=NotRun;
mtype first_effect=NotStarted;
mtype first_verification=NotRun;
bool report_written=false;
bool reserved=false;
bool reservation_ack=false;
bool owner_done=false;
bool recovery_done=false;
bool actual_effect=false;
bool acknowledged_effect=false;
bool verified_evidence=false;
bool recovery_unknown=false;
bool late_effect=false;
byte dispatches=0;
byte report_revision=0;
byte record_count=0;
byte reconciler_done=0;
byte record_id[2];
mtype record_state[2];
byte record_binding[2];
byte original_record_id[2];
mtype original_record_state[2];
bool done=false;

inline choose(value) { if :: value=true :: value=false fi }

inline publish(effect, verification) {
    if
    :: !report_written ->
        effect_outcome=effect; verification_outcome=verification;
        first_effect=effect; first_verification=verification;
        report_written=true; report_revision++
    :: report_written -> skip /* first durable terminal report wins */
    fi
}

inline dispatch(origin) {
    assert(origin==1 && reserved && reservation_ack); /* OPERATION_ORIGINAL_RESERVATION */
    dispatches++;
    assert(dispatches==1); /* OPERATION_ONE_DISPATCH */
}

inline append_observation(candidate_id, candidate_state, binding, expected_revision, result) {
    if
    :: record_count>0 && record_id[0]==candidate_id ->
        if
        :: record_state[0]==candidate_state && record_binding[0]==binding -> result=Duplicate
        :: else ->
            result=Conflict;
#ifdef BUG_OPERATION_HISTORY
            record_state[0]=candidate_state;
#endif
            skip
        fi
    :: record_count>1 && record_id[1]==candidate_id ->
        if
        :: record_state[1]==candidate_state && record_binding[1]==binding -> result=Duplicate
        :: else -> result=Conflict
        fi
    :: (record_count==0 || record_id[0]!=candidate_id) && (record_count<2 || record_id[1]!=candidate_id) ->
        if
#ifdef BUG_OPERATION_BINDING
        :: report_revision==expected_revision && record_count<2 ->
#else
        :: report_revision==expected_revision && record_count<2 && binding==1 ->
#endif
            record_id[record_count]=candidate_id; record_state[record_count]=candidate_state;
            record_binding[record_count]=binding;
            original_record_id[record_count]=candidate_id; original_record_state[record_count]=candidate_state;
            assert(record_binding[record_count]==1); /* OPERATION_OBSERVATION_BOUND */
            record_count++; report_revision++; result=Added
        :: else -> result=Invalid
        fi
    fi;
    if
    :: record_count>0 -> assert(record_id[0]==original_record_id[0] && record_state[0]==original_record_state[0]); /* OPERATION_OBSERVATION_IMMUTABLE */
    :: else -> skip
    fi;
    if
    :: record_count>1 -> assert(record_id[1]==original_record_id[1] && record_state[1]==original_record_state[1]); /* OPERATION_OBSERVATION_IMMUTABLE */
    :: else -> skip
    fi
}

active proctype Owner() {
    bool admitted;
    bool crash;
    bool response;
    bool evidence_complete;
    bool postcondition;
    mtype local_effect=NotStarted;
    mtype local_verification=NotRun;
    choose(admitted);
    if
    :: !admitted -> atomic { publish(NotStarted,NotRun) }; goto stop
    :: admitted -> skip
    fi;
    atomic { reserved=true };
    choose(reservation_ack);
    if
    :: !reservation_ack -> goto stop
    :: reservation_ack -> skip
    fi;
    choose(crash);
    if :: crash -> goto stop :: !crash -> skip fi;
    /* Reservation is durable before dispatch. Recovery may already have
     * terminalised Unknown; this one original execution remains possible. */
    atomic { dispatch(1) };
#ifdef BUG_OPERATION_STALL
    do :: !actual_effect -> skip :: break od;
#endif
    atomic {
        choose(actual_effect);
        late_effect=(actual_effect && recovery_unknown)
    };
    choose(crash);
    if :: crash -> goto stop :: !crash -> skip fi;
    choose(response);
    if
    :: !response -> local_effect=Unknown
    :: response && !actual_effect -> local_effect=Rejected
    :: response && actual_effect ->
        acknowledged_effect=true; local_effect=Applied;
        choose(evidence_complete); choose(postcondition);
        verified_evidence=(evidence_complete && postcondition);
#ifdef BUG_OPERATION_EARLY_PASS
        local_verification=Passed;
#else
        if
        :: !evidence_complete -> local_verification=Inconclusive
        :: evidence_complete && !postcondition -> local_verification=Failed
        :: evidence_complete && postcondition -> local_verification=Passed
        fi;
#endif
        assert(local_verification!=Passed || verified_evidence); /* OPERATION_VERIFICATION_REQUIRES_EVIDENCE */
    fi;
    choose(crash);
    if :: crash -> goto stop :: !crash -> skip fi;
    atomic { publish(local_effect,local_verification) };
stop:
    owner_done=true
}

active proctype Recovery() {
    do
    :: reserved && !report_written ->
        atomic {
            if
            :: !report_written -> publish(Unknown,NotRun); recovery_unknown=true
            :: else -> skip
            fi
        };
        break
    :: report_written -> break
    :: owner_done && !reserved -> break
    od;
    recovery_done=true
}

proctype Reconciler(byte id) {
    byte seen_revision;
    byte binding;
    mtype observation;
    mtype result;
    report_written;
    atomic { seen_revision=report_revision };
    if :: binding=1 :: binding=2 fi;
    if :: observation=Desired :: observation=Original :: observation=Changed :: observation=Unavailable fi;
    /* Each value is a point-in-time read, not a historical effect receipt.
     * A different controller may have produced exactly the desired state. */
    atomic {
        append_observation(id,observation,binding,seen_revision,result)
    };
#ifdef BUG_OPERATION_CAUSALITY
    if
    :: observation==Desired && effect_outcome==Unknown -> effect_outcome=Applied
    :: else -> skip
    fi;
#endif
    assert(effect_outcome==first_effect && verification_outcome==first_verification); /* OPERATION_REPORT_IMMUTABLE */
    atomic { reconciler_done++ }
}

init {
    byte retained_count;
    mtype result;
    mtype changed_state;
    atomic { run Reconciler(1); run Reconciler(2) };
    owner_done && recovery_done && reconciler_done==2;
#ifdef BUG_OPERATION_REDISPATCH
    if
    :: effect_outcome==Unknown -> atomic { dispatch(2) }
    :: else -> skip
    fi;
#endif
    assert(dispatches<=1); /* OPERATION_ONE_DISPATCH */
    assert(effect_outcome!=Applied || (actual_effect && acknowledged_effect)); /* OPERATION_APPLIED_REQUIRES_RECEIPT */
    assert(verification_outcome!=Passed || (effect_outcome==Applied && verified_evidence)); /* OPERATION_VERIFICATION_REQUIRES_EVIDENCE */
    assert(effect_outcome==first_effect && verification_outcome==first_verification); /* OPERATION_REPORT_IMMUTABLE */
    /* Exercise replay and content conflict through the same append rule. */
    if
    :: record_count>0 ->
        retained_count=record_count;
        atomic { append_observation(record_id[0],original_record_state[0],1,report_revision,result) };
        assert(result==Duplicate && record_count==retained_count); /* OPERATION_OBSERVATION_REPLAY_IDEMPOTENT */
        if :: record_state[0]==Desired -> changed_state=Changed :: else -> changed_state=Desired fi;
        atomic { append_observation(record_id[0],changed_state,1,report_revision,result) };
        assert(result==Conflict && record_count==retained_count); /* OPERATION_OBSERVATION_CONFLICT_REJECTED */
    :: else -> skip
    fi;
    done=true;
#ifdef WITNESS_OPERATION_VERIFIED
    assert(!(effect_outcome==Applied && verification_outcome==Passed)); /* REACH_OPERATION_VERIFIED */
#endif
#ifdef WITNESS_OPERATION_LATE_EFFECT
    assert(!(late_effect && effect_outcome==Unknown)); /* REACH_OPERATION_LATE_EFFECT */
#endif
#ifdef WITNESS_OPERATION_RECONCILED_UNKNOWN
    assert(!(effect_outcome==Unknown && record_count>0 && record_state[0]==Desired)); /* REACH_OPERATION_RECONCILED_UNKNOWN */
#endif
}

ltl operation_settles { <> done }
