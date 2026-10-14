/* Durable bridge ingress and remote effect boundary, deliberately separate.
 * A request key is scoped to an authenticated principal (one principal here).
 * Three submissions cover one payload, its duplicate, and a conflicting payload.
 * Local commits are atomic; network dispatch and receipt persistence are not.
 * One restart and two observations suffice to expose lost-receipt/stale races.
 */
mtype = { Absent, Queued, Launching, Unknown, Working, Succeeded, Cancelled };
mtype phase = Absent;
mtype remote = Absent;
mtype confirmed = Absent;
mtype first_terminal = Absent;
byte payload = 0;
byte receipt = 0;
byte admitted = 0;
byte dispatches = 0;
byte observations = 0;
byte events = 0;
byte transitions = 0;
bool duplicate_seen = false;
bool conflict_seen = false;
bool restarted = false;
bool response_pending = false;
bool cancel_requested = false;
bool cancel_sent = false;
bool stop_host = false;
mtype observation = Absent;

#define TERMINAL(s) ((s) == Succeeded || (s) == Cancelled)

inline invariants() {
    assert(receipt <= 1);                  /* BRIDGE_KEY_DEDUPLICATED */
    assert(receipt == 0 || payload == 1);   /* BRIDGE_KEY_CONTENT_IMMUTABLE */
    assert(dispatches <= 1);               /* BRIDGE_UNKNOWN_NOT_REPLAYED */
    assert(dispatches == 0 || admitted == 1); /* BRIDGE_ADMISSION_BEFORE_EFFECT */
    assert(!TERMINAL(phase) || phase == confirmed); /* BRIDGE_CANCEL_NOT_CONFIRMATION */
    assert(first_terminal == Absent || phase == first_terminal); /* BRIDGE_TERMINAL_IMMUTABLE */
    assert(events == transitions)          /* BRIDGE_EVENTS_ATOMIC */
}

inline change_phase(next) {
    phase = next;
    transitions++;
#ifndef BUG_BRIDGE_EVENT_LOST
    events++;
#endif
    invariants()
}

active proctype Client() {
    atomic {
        /* INSERT task, request identity and event in one transaction. */
        payload = 1; receipt = 1; change_phase(Queued)
    }
    if
    :: atomic {
        duplicate_seen = true;
#ifdef BUG_BRIDGE_DUPLICATE
        receipt++;
#endif
        invariants()
    }
    :: skip
    fi;
    atomic {
        conflict_seen = true;
#ifdef BUG_BRIDGE_CONFLICT
        payload = 2;
#endif
        invariants()
    }
}

active proctype Host() {
    do
    :: phase == Queued && admitted == 0 -> atomic { admitted = 1; invariants() }
#ifdef BUG_BRIDGE_UNADMITTED
    :: phase == Queued -> atomic { change_phase(Launching) }
#else
    :: phase == Queued && admitted == 1 -> atomic { change_phase(Launching) }
#endif
    :: phase == Launching && !response_pending ->
        atomic {
            dispatches++;
            invariants();
            if
            :: remote = Working;
                if
                :: response_pending = true
                :: change_phase(Unknown) /* Effect accepted, response lost. */
                fi
            :: change_phase(Unknown)     /* Dispatch may have had no effect. */
            fi
        }
    :: phase == Launching && response_pending ->
        atomic { response_pending = false; change_phase(Working) }
#ifdef BUG_BRIDGE_UNKNOWN_REPLAY
    :: phase == Unknown -> atomic { change_phase(Launching) }
#endif
    :: !restarted ->
        atomic {
            restarted = true; response_pending = false;
            if
            :: phase == Launching -> change_phase(Unknown)
            :: else -> invariants()
            fi
        }
    :: phase == Working && !cancel_requested ->
        atomic {
            cancel_requested = true;
#ifdef BUG_BRIDGE_CANCEL_CONFIRMED
            change_phase(Cancelled)
#else
            invariants()
#endif
        }
    :: cancel_requested && !cancel_sent -> atomic { cancel_sent = true; invariants() }
    :: (phase == Working || TERMINAL(phase)) && observations < 2 ->
        observations++;
        if :: observation = remote :: observation = Working fi;
        atomic {
            if
#ifdef BUG_BRIDGE_STALE_OVERWRITE
            :: true ->
#else
            :: !TERMINAL(phase) ->
#endif
                if
                :: TERMINAL(observation) ->
                    confirmed = observation;
                    if :: first_terminal == Absent -> first_terminal = observation :: else -> skip fi;
                    change_phase(observation)
                :: else -> change_phase(Working)
                fi
            :: else -> invariants()
            fi
        }
    :: break /* Active and unknown work may remain without a liveness promise. */
    od;
    stop_host = true;
    invariants()
}

active proctype Peer() {
    do
    :: remote == Working -> remote = Succeeded
    :: remote == Working && cancel_sent -> remote = Cancelled
    :: break
    od
}
