/* Two accepted requests; application terminal outcomes race with host expiry.
 * due is an abstract clock observation, not a timed-automata clock. A host call
 * observing due reconciles before fresh work. Expiry is an immutable run latch.
 * No liveness or deadline-in-seconds claim: host activity may stop indefinitely.
 */
mtype = { OPEN, RESULT, FAILURE, CANCEL, EXPIRED_OUTCOME_UNKNOWN };
mtype terminal[2];
byte terminal_count[2];
bool received[2];
bool acknowledged[2];
bool effect_started[2];
bool actionable[2];
bool due = false;
bool expired = false;

inline expire_one(task) {
    if
#ifdef BUG_EXPIRY_OVERWRITE
    :: true ->
#else
    :: terminal[task] == OPEN ->
#endif
         terminal[task] = EXPIRED_OUTCOME_UNKNOWN;
         terminal_count[task]++;
         assert(terminal_count[task] == 1);
         printf("TASK expired_outcome_unknown %d\n", task)
    :: else -> skip
    fi
}

inline reconcile() {
    if
    :: due || expired ->
         expired = true;
         expire_one(0); expire_one(1)
    :: else -> skip
    fi
}

inline task_invariant(task) {
    assert(terminal_count[task] <= 1);
    assert((terminal[task] == OPEN) == (terminal_count[task] == 0));
    assert(!expired || terminal[task] != OPEN);
    assert(!acknowledged[task] || received[task])
}

inline record_terminal(task, outcome) {
    assert(!expired && !due);
    assert(terminal[task] == OPEN);
    terminal[task] = outcome; terminal_count[task]++;
    task_invariant(task);
    printf("TASK terminal %d %e\n", task, outcome)
}

/* This environment may finish without ever exposing a due observation. It may
 * also roll a due observation back; a persisted expiry latch must survive that.
 */
proctype ClockObservation() {
    if
    :: due = true;
         if :: due = false :: skip fi
    :: skip
    fi
}

proctype Host() {
    do
    :: atomic { due && !expired -> reconcile(); task_invariant(0); task_invariant(1) }
    :: terminal[0] != OPEN && terminal[1] != OPEN -> break
    od
}

proctype Requester(bit task) {
    bool attempted = false;
    do
    :: atomic { !attempted ->
         attempted = true; reconcile();
         if :: terminal[task] == OPEN -> record_terminal(task, CANCEL)
            :: else -> printf("TASK reject_cancel_closed %d\n", task) fi;
         task_invariant(task) }
    :: attempted || terminal[task] != OPEN -> break
    od
}

proctype Worker(bit task) {
    bool attempted = false;
    do
    :: atomic { !received[task] ->
         reconcile(); received[task] = true;
         actionable[task] = (terminal[task] == OPEN);
         assert(!expired || !actionable[task]); task_invariant(task);
         printf("TASK receive %d actionable %d\n", task, actionable[task]) }
    :: atomic { received[task] && !acknowledged[task] ->
         reconcile(); acknowledged[task] = true; task_invariant(task) }
    :: atomic { received[task] && !effect_started[task] ->
         /* External work may already have been initiated from a previous
          * receipt. Expiry cannot assert that no such effect occurred. */
         effect_started[task] = true; task_invariant(task) }
    :: atomic { received[task] && !attempted ->
         attempted = true; reconcile();
         if
         :: terminal[task] == OPEN ->
              if :: record_terminal(task, RESULT) :: record_terminal(task, FAILURE) fi
         :: else -> printf("TASK reject_terminal_closed %d\n", task)
         fi;
         task_invariant(task) }
    :: acknowledged[task] && attempted -> break
    od
}

init {
    atomic {
        terminal[0] = OPEN; terminal[1] = OPEN;
        run ClockObservation(); run Host();
        run Requester(0); run Requester(1); run Worker(0); run Worker(1)
    }
}
