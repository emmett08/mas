/* Two independent leaf invocations and their all-completed join.
 * A transition selector represents every interleaving of their durable steps.
 * Bounds: two branches, allowance one or two, one crash, one cancellation,
 * one expiry, one response per dispatched branch. See companion notes.
 */
mtype = { Waiting, Intent, Admitted, Running, Completed, Failed,
          Cancelled, Expired, Unknown, Blocked };
mtype state[2];
mtype first_terminal[2];
bool intent[2];
bool admitted[2];
bool recorded[2];
byte assignment[2];
byte dispatches[2];
byte charges = 0;
byte allowance = 2;
bool crashed = false;
bool cancel = false;
bool expired = false;
bool joined = false;
bool accepted = false;

#define closed(i) (state[i] == Completed || state[i] == Failed || state[i] == Cancelled || state[i] == Expired || state[i] == Unknown || state[i] == Blocked)
#define stop_dispatch (cancel || expired || state[0] == Unknown || state[1] == Unknown)

inline invariants() {
    assert(charges <= allowance);                         /* WORKFLOW_SHARED_ALLOWANCE */
    assert(dispatches[0] <= 1 && dispatches[1] <= 1);      /* WORKFLOW_NO_REDISPATCH */
    assert(!recorded[0] || state[0] == first_terminal[0]); /* WORKFLOW_TERMINAL_IMMUTABLE */
    assert(!recorded[1] || state[1] == first_terminal[1]); /* WORKFLOW_TERMINAL_IMMUTABLE */
    assert(!intent[0] || assignment[0] == 1);             /* WORKFLOW_STABLE_ASSIGNMENT */
    assert(!intent[1] || assignment[1] == 2);             /* WORKFLOW_STABLE_ASSIGNMENT */
    assert(!accepted || (state[0] == Completed && state[1] == Completed)); /* WORKFLOW_JOIN_ALL_COMPLETED */
    assert(charges >= admitted[0] + admitted[1])          /* WORKFLOW_CHARGE_RETAINED */
}

inline finish(i, result) {
    state[i] = result;
    if
    :: !recorded[i] -> first_terminal[i] = result; recorded[i] = true
    :: else -> skip
    fi;
    invariants()
}

inline stop_unstarted(i) {
    if
    :: state[0] == Unknown || state[1] == Unknown -> finish(i, Blocked)
    :: !(state[0] == Unknown || state[1] == Unknown) && expired -> finish(i, Expired)
    :: !(state[0] == Unknown || state[1] == Unknown) && !expired && cancel -> finish(i, Cancelled)
    fi
}

inline dispatch(i) {
    assert(intent[i] && admitted[i]);                 /* WORKFLOW_INTENT_ADMISSION */
    assert(!stop_dispatch);                          /* WORKFLOW_STOP_BLOCKS_DISPATCH */
    dispatches[i]++;
    state[i] = Running;
    invariants()
}

inline advance(i) {
    if
    :: state[i] == Running ->
        /* A stop request is not an observation of remote cancellation. */
        if
        :: finish(i, Completed)
        :: finish(i, Failed)
        :: finish(i, Unknown)
#ifdef BUG_WORKFLOW_STALL
        :: skip /* Removing finite response progress permits an infinite wait. */
#endif
        fi
    :: state[i] != Running && stop_dispatch ->
#ifdef BUG_WORKFLOW_STOP_BYPASS
        if :: state[i] == Admitted -> dispatch(i) :: else -> stop_unstarted(i) fi
#else
        stop_unstarted(i)
#endif
    :: state[i] == Waiting && !stop_dispatch ->
        assignment[i] = i + 1; intent[i] = true; state[i] = Intent;
        invariants()
    :: state[i] == Intent && !stop_dispatch ->
        if
#ifdef BUG_WORKFLOW_OVERBUDGET
        :: true ->
#else
        :: charges < allowance ->
#endif
            charges++;
#ifndef BUG_WORKFLOW_UNADMITTED
            admitted[i] = true;
#endif
            state[i] = Admitted; invariants()
        :: else -> finish(i, Failed)
        fi
    :: state[i] == Admitted && !stop_dispatch -> dispatch(i)
    fi
}

active proctype Scheduler() {
    atomic {
        state[0] = Waiting; state[1] = Waiting;
        if :: allowance = 1 :: allowance = 2 fi
    }
    do
    :: !closed(0) -> atomic { advance(0) }
    :: !closed(1) -> atomic { advance(1) }
    :: !cancel -> atomic { cancel = true; invariants() }
    :: !expired -> atomic { expired = true; invariants() }
    :: !crashed -> atomic {
        crashed = true;
        /* Durable assignments survive restart. A previously dispatched branch
         * is unresolved; an intent/admission resumes the original identity. */
#ifdef BUG_WORKFLOW_NEW_ID
        if :: intent[0] -> assignment[0]++ :: else -> skip fi;
#endif
        if :: state[0] == Running -> finish(0, Unknown) :: else -> skip fi;
        if :: state[1] == Running -> finish(1, Unknown) :: else -> skip fi;
        invariants()
    }
#ifdef BUG_WORKFLOW_UNKNOWN_REPLAY
    :: state[0] == Unknown -> atomic { dispatches[0]++; invariants() }
    :: state[1] == Unknown -> atomic { dispatches[1]++; invariants() }
#endif
#ifdef BUG_WORKFLOW_TERMINAL_OVERWRITE
    :: state[0] == Completed -> atomic { finish(0, Failed) }
#endif
#ifdef BUG_WORKFLOW_EARLY_JOIN
    :: state[0] == Completed -> atomic { accepted = true; invariants() }
#endif
    :: closed(0) && closed(1) -> atomic {
        accepted = (state[0] == Completed && state[1] == Completed);
        joined = true; invariants();
#ifdef WITNESS_WORKFLOW_SUCCESS
        assert(!accepted); /* REACH_WORKFLOW_SUCCESS */
#endif
        break
    }
    od
}

ltl workflow_settles { <> joined }
