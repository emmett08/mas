/* One fixed authoritative provider window, two original effect identities and
 * two callers for identity zero. Replies may be lost. CPU release, replay,
 * duplicate commands and shorter hints cannot restore credit or permission. */
byte used = 0;
byte issued = 0;
bool retained[2];
byte dispatched[2];
byte not_before = 0;

proctype Caller(byte id) {
    bool fresh = false;
    atomic {
        if
        :: retained[id] ->
#ifdef MUT_RETAINED_PERMISSION
            fresh = true
#else
            fresh = false
#endif
        :: !retained[id] ->
            if
#ifdef MUT_ALLOWANCE
            :: true ->
#else
            :: used < 1 ->
#endif
                retained[id] = true;
                used++; issued++;
                assert(issued <= 1);
                if
                :: fresh = true
                :: fresh = false; /* committed admission reply lost */
#ifdef MUT_UNKNOWN_REFUND
                    used--
#else
                    skip
#endif
                fi
            :: else -> skip
            fi
        fi
    }
    if
    :: fresh -> atomic {
        dispatched[id]++;
        fresh = false;
        assert(dispatched[id] <= 1);
#ifdef WITNESS_FRESH
        assert(dispatched[id] == 0);
#endif
    }
    :: !fresh -> skip
    fi
}

proctype Defer() {
    atomic {
        not_before = 2;
        /* A second observation or successful response cannot shorten an
         * outstanding externally indicated cooldown. */
#ifdef MUT_SHORTER_HINT
        not_before = 1;
#else
        if :: not_before < 1 -> not_before = 1 :: else -> skip fi;
#endif
        assert(not_before >= 2);
#ifdef WITNESS_DEFER
        assert(not_before == 0);
#endif
    }
}
init {
    atomic {run Caller(0); run Caller(0); run Caller(1); run Defer()}
}
