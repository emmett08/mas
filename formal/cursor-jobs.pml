/* One durable Cursor cloud job. SQLite intent/receipt commits are atomic;
 * the POST and its response persistence are deliberately separate transitions.
 * The host serialises operations with its job lock; the remote service can
 * progress independently. A client-assigned agent ID exists before dispatch.
 * One crash, two poll observations, and one cancellation request are modelled.
 * Stopping with an unresolved job is valid: no remote liveness is promised.
 */
mtype = { Prepared, Launching, LaunchUnknown, Active,
          Running, Finished, Failed, Cancelled, Expired };
mtype phase = Prepared;
mtype remote_phase = Prepared;
mtype confirmed = Prepared;
mtype first_terminal = Prepared;
bool admitted = false;
bool crashed = false;
bool reply_available = false;
bool remote_exists = false;
bool cancel_requested = false;
bool cancel_sent = false;
bool local_deadline = false;
bool terminal_written = false;
byte agent_id = 1;               /* Durable client-assigned ID, not a secret. */
byte run_id = 0;                 /* Zero means no run has been bound. */
byte first_run_id = 0;
byte dispatches = 0;
byte polls = 0;
byte candidate_run = 0;
mtype observation = Prepared;

#define TERMINAL(s) ((s) == Finished || (s) == Failed || (s) == Cancelled || (s) == Expired)

inline invariants() {
    assert(agent_id == 1);                         /* CURSOR_ID_RETAINED */
    assert(dispatches <= 1);                       /* CURSOR_NO_RELAUNCH */
    assert(dispatches == 0 || admitted);           /* CURSOR_ADMITTED */
    assert(run_id == 0 || (remote_exists && run_id == 1)); /* CURSOR_RUN_CORRELATED */
    assert(first_run_id == 0 || run_id == first_run_id);   /* CURSOR_RUN_IMMUTABLE */
    assert(!TERMINAL(phase) || phase == confirmed); /* CURSOR_TERMINAL_OBSERVED */
    assert(!terminal_written || phase == first_terminal)  /* CURSOR_TERMINAL_IMMUTABLE */
}

inline bind_run(candidate) {
    atomic {
#ifdef BUG_CURSOR_WRONG_RUN
        run_id = candidate;
        if :: first_run_id == 0 -> first_run_id = candidate :: else -> skip fi;
        phase = Active
#else
        if
        :: remote_exists && candidate == 1 && (run_id == 0 || run_id == candidate) ->
            run_id = candidate;
            if :: first_run_id == 0 -> first_run_id = candidate :: else -> skip fi;
            phase = Active
        :: else -> skip /* Wrong agent/run response or conflicting binding rejected. */
        fi
#endif
        invariants()
    }
}

inline record_poll() {
    atomic {
        if
#ifdef BUG_CURSOR_TERMINAL_OVERWRITE
        :: true ->
#else
        :: !terminal_written ->
#endif
            if
            :: TERMINAL(observation) ->
                confirmed = observation; phase = observation;
                if
                :: !terminal_written -> first_terminal = observation; terminal_written = true
                :: else -> skip
                fi
            :: else -> phase = Active
            fi
        :: else -> skip /* Terminal receipt is immutable, even after stale GET. */
        fi;
        invariants()
    }
}

active proctype Host() {
    do
    :: !admitted && phase == Prepared -> admitted = true
#ifdef BUG_CURSOR_UNCHARGED
    :: phase == Prepared ->
#else
    :: admitted && phase == Prepared ->
#endif
        atomic { phase = Launching; invariants() } /* Commit intent before POST. */
    :: phase == Launching && !reply_available ->
        atomic {
            dispatches++;
            invariants();
            if
            :: remote_exists = true; remote_phase = Running;
                if
                :: reply_available = true /* Response received; not yet committed. */
                :: phase = LaunchUnknown  /* Service accepted; response was lost. */
                fi
            :: phase = LaunchUnknown      /* Ambiguous failure, possibly no effect. */
            fi
        }
    :: phase == Launching && reply_available ->
        bind_run(1); reply_available = false
    :: phase == LaunchUnknown && remote_exists ->
        /* An operator supplies a run ID; GET validates that exact run and agent.
         * Finding no agent/run never resets phase to Prepared. */
        if :: candidate_run = 1 :: candidate_run = 2 fi;
        bind_run(candidate_run)
#ifdef BUG_CURSOR_REPLAY_UNKNOWN
    :: phase == LaunchUnknown -> phase = Launching
#endif
    :: !crashed ->
        atomic {
            crashed = true; reply_available = false;
            if :: phase == Launching -> phase = LaunchUnknown :: else -> skip fi;
#ifdef BUG_CURSOR_LOST_ID
            agent_id = 0;
#endif
            invariants()
        }
    :: run_id != 0 && polls < 2 ->
        polls++;
        if
        :: observation = remote_phase
        :: observation = Running /* A stale non-terminal GET must not undo completion. */
        fi;
        record_poll()
    :: phase == Active && !cancel_requested ->
        atomic {
            cancel_requested = true; /* Persist intent before the cancellation POST. */
#ifdef BUG_CURSOR_CANCEL_CONFIRMED
            phase = Cancelled;
#endif
            invariants()
        }
    :: cancel_requested && !cancel_sent ->
        atomic { cancel_sent = true; invariants() }
    :: !local_deadline ->
        atomic {
            local_deadline = true; /* Local deadline is not a remote observation. */
#ifdef BUG_CURSOR_TIMEOUT_CANCELLED
            phase = Cancelled;
#endif
            invariants()
        }
    :: break /* Unresolved/active jobs may remain for a later operator command. */
    od;
    invariants()
}

active proctype Remote() {
    do
    :: remote_exists && remote_phase == Running ->
        if
        :: remote_phase = Finished
        :: remote_phase = Failed
        :: remote_phase = Expired
        :: cancel_sent -> remote_phase = Cancelled
        fi
    :: break /* Availability, provider completion and cancellation are not assumed. */
    od
}
