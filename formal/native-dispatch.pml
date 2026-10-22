/* Two callers share one durable command identity. A lost committed reply
 * provides no fresh permission. Each capacity amendment compares its original
 * policy revision and preserves existing occupancy. Finite safety only.
 */
bool committed = false;
byte dispatched = 0;
byte revision = 0;
byte occupied = 2;
byte capacity = 2;

active [2] proctype Caller() {
    bool permit = false;
    atomic {
        if
        :: !committed ->
            committed = true;
            if
            :: permit = true /* successful fresh response */
            :: permit = false /* reply lost after commit */
            fi
        :: committed ->
#ifdef MUT_REPLAY_PERMISSION
            permit = true
#else
            permit = false
#endif
        fi
    }
    if
    :: permit -> atomic {
        dispatched++;
        permit = false;
        assert(dispatched <= 1);
#ifdef WITNESS_DISPATCH
        assert(dispatched == 0);
#endif
    }
    :: !permit -> skip
    fi
}

active [2] proctype Amendment() {
    byte expected = 0;
    byte proposed;
    byte previous_occupied;
    if :: proposed = 1 :: proposed = 3 fi;
    atomic {
        if
#ifdef MUT_POLICY_CAS
        :: true ->
#else
        :: expected == revision ->
#endif
            if
            :: true ->
                previous_occupied=occupied;
                revision++;
                capacity = proposed;
                assert(occupied == previous_occupied);
                assert(revision <= 1);
#ifdef WITNESS_AMENDMENT
                assert(revision == 0);
#endif
            :: else -> skip
            fi
        :: else -> skip
        fi
    }
}

active proctype NewAssignment() {
    atomic {
        if
#ifdef MUT_OCCUPANCY
        :: true ->
#else
        :: occupied < capacity ->
#endif
            assert(occupied < capacity);
            occupied++
        :: else -> skip
        fi
    }
}
