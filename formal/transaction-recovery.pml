/* Safety abstraction of SQLite transaction/retry boundaries, not a proof of
 * SQLite's recovery implementation. Two durable identities, concurrent writers.
 * Rollback/crash at each pre-COMMIT stage discards private writes. COMMIT is
 * atomic. Losing its response retains committed state; a retry is idempotent.
 * Crashes and response losses may repeat indefinitely. No liveness claim.
 */
bool committed[2];
byte content[2];
byte stored = 0;
bool locked = false;

inline durable_invariant() {
    assert(stored == committed[0] + committed[1]);
    assert(stored <= 2);
    assert(!committed[0] || content[0] == 0);
    assert(!committed[1] || content[1] == 1)
}

proctype Writer(bit identity) {
    byte stage;
    bool staged_row;
    byte staged_content;
    byte staged_count;
    bool replied = false;
    do
    :: !replied ->
        atomic { !locked -> locked = true; stage = 0; staged_row = false };
        if
        :: committed[identity] ->
             /* The response to an earlier COMMIT may have been lost. */
             atomic {
                 assert(content[identity] == identity);
#ifdef BUG_DUPLICATE_INSERT
                 stored++;
#endif
                 durable_invariant(); locked = false;
                 printf("TX duplicate %d\n", identity)
             }
        :: !committed[identity] ->
             do
             :: stage == 0 -> staged_row = true; staged_content = identity; stage = 1
             :: stage == 1 -> staged_count = stored + 1; stage = 2
             :: stage < 3 ->
                  /* Rollback: private transaction data never becomes visible. */
                  atomic {
                      staged_row = false; stage = 3; locked = false;
                      durable_invariant(); printf("TX rollback %d\n", identity)
                  }; break
             :: stage < 3 ->
                  /* Process death before COMMIT, followed by SQLite recovery. */
                  atomic {
#ifdef BUG_PARTIAL_COMMIT
                      if :: staged_row -> committed[identity] = true; content[identity] = staged_content
                         :: else -> skip fi;
#endif
                      staged_row = false; stage = 3; locked = false;
                      durable_invariant(); printf("TX recovered_precommit_crash %d\n", identity)
                  }; break
             :: stage == 2 ->
                  atomic {
                      assert(staged_row && staged_content == identity);
                      committed[identity] = true; content[identity] = staged_content;
                      stored = staged_count; stage = 3; locked = false;
                      durable_invariant(); printf("TX commit %d\n", identity)
                  }; break
             od
        fi;
        if
        :: committed[identity] ->
             if
             :: replied = true; printf("TX response_received %d\n", identity)
             :: printf("TX lost_commit_response %d\n", identity)
             fi
        :: else -> skip
        fi
    :: replied -> break
    od
}

/* A reader may observe any committed snapshot while another transaction has
 * uncommitted local data. It never reads that private transaction data. */
active proctype Reader() {
    do
    :: atomic { durable_invariant() }
    :: committed[0] && committed[1] -> break
    od
}

init { atomic { run Writer(0); run Writer(1) } }
