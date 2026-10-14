/* Local durable TaskService transition system. Two authenticated principals
 * share a request key; two tasks, one scheduled task with two revisions, one
 * worker and one crash. The second task exercises request-key separation;
 * shared deterministic traces separately exercise its lifecycle transitions.
 * API transactions are indivisible. The worker's claim, executor reply and
 * final service commit are separate transitions. Host checkpoint recovery is
 * assumed here and separately checked by host-recovery.pml / runner tests.
 * TRACE_ONLY reuses these same transitions for the shared JSON trace corpus.
 */
#define OWNERS 2
#define REVISIONS 2
#define SLOT(t,r) ((t)*REVISIONS+(r))
#define PAUSED(s) ((s)==Input || (s)==Auth)
#define TERMINAL(s) ((s)==Completed || (s)==Failed || (s)==Cancelled)
mtype = { Absent, Queued, Running, Input, Auth, Completed, Failed, Cancelled,
          Unknown, Accepted, Duplicate, Conflict, Forbidden, Invalid, Limit };
mtype state[OWNERS];
byte payload[OWNERS];
byte receipts[OWNERS];
byte revision[OWNERS];
byte executions[OWNERS*REVISIONS];
byte update_payload[OWNERS*2];
bool cancelled[OWNERS];
bool remote_unresolved[OWNERS]; /* Independent remote evidence: local cancel never clears it. */
bool remote_initial[OWNERS];
byte transitions[OWNERS];
byte events[OWNERS];
mtype checkpoint[OWNERS*REVISIONS];
bool busy = false;
bool ready = false;
byte worker_task = 0;
byte worker_revision = 0;
mtype reply = Absent;
mtype result = Invalid;
byte result_owner = 0;
byte last_actor = 0;
bool exposed = false;
bool crashed = false;
bool cancelled_pause = false;
bool reached_revision = false;
bool reached_unknown_restart = false;
bool foreign_rejected = false;

inline initialise() {
    state[0] = Absent; state[1] = Absent;
    checkpoint[0] = Absent; checkpoint[1] = Absent;
    checkpoint[2] = Absent; checkpoint[3] = Absent;
    remote_unresolved[0] = true; remote_initial[0] = true
}

inline invariants() {
    assert(!exposed || result_owner == last_actor); /* SERVICE_OWNER_ISOLATED */
    assert(receipts[0] <= 1 && receipts[1] <= 1); /* SERVICE_KEY_UNIQUE */
    assert(receipts[0] == 0 || payload[0] == 1); /* SERVICE_CONTENT_IMMUTABLE */
    assert(receipts[1] == 0 || payload[1] == 1); /* SERVICE_CONTENT_IMMUTABLE */
    assert(executions[0] <= 1 && executions[1] <= 1 && executions[2] <= 1 && executions[3] <= 1); /* SERVICE_REVISION_UNIQUE */
    assert(!busy || worker_revision == revision[worker_task]); /* SERVICE_COMMIT_CURRENT_REVISION */
    assert(!(cancelled[0] && PAUSED(state[0])) && !(cancelled[1] && PAUSED(state[1]))); /* SERVICE_CANCEL_CLOSES_PAUSE */
    /* Environmental separation assumption, not verification of remote code. */
    assert(remote_unresolved[0] == remote_initial[0] && remote_unresolved[1] == remote_initial[1]); /* SERVICE_REMOTE_INDEPENDENT */
    assert(events[0] == transitions[0] && events[1] == transitions[1]); /* SERVICE_EVENT_ATOMIC */
#ifdef WITNESS_SERVICE_TWO_OWNERS
    assert(!(receipts[0] == 1 && receipts[1] == 1)); /* REACH_SERVICE_TWO_OWNERS */
#endif
#ifdef WITNESS_SERVICE_CANCEL_PAUSE
    assert(!cancelled_pause); /* REACH_SERVICE_CANCEL_PAUSE */
#endif
#ifdef WITNESS_SERVICE_REVISION
    assert(!reached_revision); /* REACH_SERVICE_REVISION */
#endif
#ifdef WITNESS_SERVICE_UNKNOWN_RESTART
    assert(!reached_unknown_restart); /* REACH_SERVICE_UNKNOWN_RESTART */
#endif
#ifdef WITNESS_SERVICE_FOREIGN_REJECTED
    assert(!foreign_rejected); /* REACH_SERVICE_FOREIGN_REJECTED */
#endif
}

inline event(t) {
    transitions[t]++;
#ifndef BUG_SERVICE_EVENT_LOST
    events[t]++;
#endif
}

inline response(actor, target, status, visible) {
    last_actor = actor; result_owner = target; result = status; exposed = visible
}

inline submit(actor, value) {
    atomic {
        response(actor, actor, Accepted, true);
#ifdef BUG_SERVICE_UNSCOPED_KEY
        if
        :: actor == 1 && receipts[0] != 0 -> response(actor, 0, Duplicate, true)
        :: else ->
#endif
        if
        :: receipts[actor] != 0 ->
            if
            :: payload[actor] == value -> result = Duplicate
            :: else -> result = Conflict; exposed = false
            fi
        :: else ->
            receipts[actor]++; payload[actor] = value; state[actor] = Queued;
            executions[SLOT(actor,0)]++; event(actor)
        fi;
#ifdef BUG_SERVICE_UNSCOPED_KEY
        fi;
#endif
        invariants()
    }
}

inline inspect(actor, target) {
    atomic {
        if
        :: actor == target && receipts[target] != 0 -> response(actor, target, Accepted, true)
        :: else -> response(actor, target, Forbidden, false); foreign_rejected = true
        fi;
        invariants()
    }
}

inline cancel(actor, target) {
    atomic {
        if
        :: actor != target || receipts[target] == 0 -> response(actor, target, Forbidden, false); foreign_rejected = true
        :: else ->
            response(actor, target, Accepted, true);
            if
            :: TERMINAL(state[target]) -> skip
            :: state[target] == Queued || PAUSED(state[target]) ->
                cancelled[target] = true; state[target] = Cancelled; event(target)
            :: !TERMINAL(state[target]) && state[target] != Queued && !PAUSED(state[target]) && !cancelled[target] -> cancelled[target] = true; event(target)
            :: else -> skip
            fi
        fi;
        invariants()
    }
}

inline update(actor, target, key, value) {
    atomic {
        response(actor, target, Invalid, false);
        if
        :: actor != target || receipts[target] == 0 -> result = Forbidden; foreign_rejected = true
        :: actor == target && receipts[target] != 0 && update_payload[actor*2+key] != 0 ->
            if
            :: update_payload[actor*2+key] == value -> result = Duplicate; exposed = true
            :: else -> result = Conflict
            fi
#ifdef BUG_SERVICE_STALE_REVISION
        :: actor == target && update_payload[actor*2+key] == 0 && (PAUSED(state[target]) || state[target] == Running) && !cancelled[target] ->
#else
        :: actor == target && update_payload[actor*2+key] == 0 && PAUSED(state[target]) && !cancelled[target] ->
#endif
            if
            :: revision[target] + 1 < REVISIONS ->
                revision[target]++; executions[SLOT(target,revision[target])]++;
                update_payload[actor*2+key] = value; state[target] = Queued;
                result = Accepted; exposed = true; reached_revision = true; event(target)
            :: else -> result = Limit
            fi
        :: else -> skip
        fi;
        invariants()
    }
}

inline begin(t) {
    atomic {
        response(t, t, Invalid, false);
        if
        :: !busy && (state[t] == Queued || state[t] == Running) ->
            busy = true; worker_task = t; worker_revision = revision[t]; ready = false;
            if
            :: state[t] == Running ->
                ready = true;
                if
                :: checkpoint[SLOT(t,revision[t])] != Absent -> reply = checkpoint[SLOT(t,revision[t])]
                :: else -> reply = Unknown; reached_unknown_restart = true
                fi
            :: else -> event(t)
            fi;
            state[t] = Running; result = Accepted; exposed = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline answer(value) {
    atomic {
        assert(busy && !ready);
        reply = value; ready = true;
        checkpoint[SLOT(worker_task,worker_revision)] = value;
        invariants()
    }
}

inline commit() {
    atomic {
        assert(busy && ready);
        state[worker_task] = reply;
#ifndef BUG_SERVICE_CANCEL_PAUSE
        if
        :: cancelled[worker_task] && PAUSED(reply) -> state[worker_task] = Cancelled; cancelled_pause = true
        :: else -> skip
        fi;
#endif
        busy = false; ready = false; event(worker_task);
        response(worker_task, worker_task, Accepted, true);
        invariants()
    }
}

inline crash() {
    atomic { assert(busy && !crashed); crashed = true; busy = false; ready = false; invariants() }
}

#ifndef TRACE_ONLY
active proctype Schedules() {
    byte attempts[2];
    byte cancels[2];
    byte updates[2];
    bool foreign = false;
    initialise();
    do
    :: attempts[0] == 0 -> attempts[0]++; submit(0,1)
    :: attempts[1] == 0 -> attempts[1]++; submit(1,1)
    :: attempts[0] == 1 -> attempts[0]++; if :: submit(0,1) :: submit(0,2) fi
    :: attempts[1] == 1 -> attempts[1]++; if :: submit(1,1) :: submit(1,2) fi
    :: !foreign && receipts[0] != 0 -> foreign = true; inspect(1,0)
    :: receipts[0] != 0 && cancels[0] == 0 -> cancels[0]++; cancel(0,0)
    :: receipts[1] != 0 && cancels[1] == 0 -> cancels[1]++; cancel(1,1)
    :: receipts[0] != 0 && updates[0] < 2 -> updates[0]++; update(0,0,updates[0]-1,1)
    :: !busy && (state[0] == Queued || state[0] == Running) -> begin(0)
    :: busy && !ready -> if :: answer(Input) :: answer(Auth) :: answer(Completed) :: answer(Failed) :: answer(Unknown) fi
    :: busy && ready -> commit()
    :: busy && !crashed -> crash()
    :: break
    od
}
#endif
