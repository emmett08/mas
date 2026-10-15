/* Finite contract for two registered definitions, two slots, one logical key,
 * at most two invocation attempts and one worker-fence transition. Repository
 * commits are atomic; authenticated receipts and truthful not-sent evidence
 * are environmental assumptions. No OS/provider/network refinement claim. */
mtype = { Queued, Running, NotSent, Completed, Unknown, Archived };
mtype state = Queued;
byte receipts = 1;
byte attempts = 0;
byte charges = 0;
byte spent = 0;
byte ceiling = 2;
bool available = false;
byte pinned_version = 1;
byte alias_version = 2;
byte executed_version = 0;
bool settled = false;
bool archived = false;
byte slots = 0;
byte owners[2];
byte current_fence = 1;
byte assigned_fence = 1;
bool observation = false;
bool evidence_stored = false;
bool stale_committed = false;

inline invariants() {
    assert(receipts == 1); /* EVOLUTION_KEY_RETAINED */
    assert(charges == spent && charges <= ceiling); /* EVOLUTION_CHARGES_RETAINED */
    assert(executed_version == 0 || executed_version == pinned_version); /* EVOLUTION_VERSION_PINNED */
    assert(slots <= 2 && owners[0] <= 1 && owners[1] <= 1); /* EVOLUTION_SLOT_OWNERSHIP */
    assert(!archived || state != Unknown); /* EVOLUTION_UNKNOWN_RETAINED */
    assert(!observation || evidence_stored); /* EVOLUTION_LATE_EVIDENCE_RETAINED */
    assert(!stale_committed); /* EVOLUTION_STALE_WRITE_REJECTED */
}

proctype Execute() {
    available;
    atomic {
        state = Running;
        executed_version = pinned_version;
#ifdef BUG_EVOLUTION_ALIAS
        executed_version = alias_version;
#endif
        attempts++; charges++; spent++;
        invariants()
    }
    if
    :: state = Completed
    :: state = NotSent
    :: state = Unknown
    fi;
    if
    :: state == NotSent ->
        atomic { attempts++; charges++; spent++; state = Completed; invariants() }
    :: state != NotSent -> skip
    fi;
    atomic { settled = true; invariants() }
}

proctype Upgrade() {
    /* Availability is separate from the current new-submission alias. */
    available = true
}

proctype Maintain() {
    settled;
    atomic {
        if
        :: state == Completed ->
            archived = true; state = Archived;
#ifdef BUG_EVOLUTION_FORGET_KEY
            receipts++;
#endif
#ifdef BUG_EVOLUTION_REFUND
            charges = 0;
#endif
        :: else -> skip
        fi;
#ifdef BUG_EVOLUTION_ARCHIVE_UNKNOWN
        archived = true;
#endif
        /* An amendment changes the ceiling, never consumed allocations. */
        ceiling++;
        invariants()
    }
}

proctype Slot(byte id) {
    atomic {
#ifdef BUG_EVOLUTION_SLOT_DUPLICATE
        slots < 2;
#else
        owners[id] == 0 && slots < 2;
#endif
        owners[id]++; slots++;
        invariants()
    }
    skip;
    atomic { owners[id]--; slots--; invariants() }
}

proctype DelayedReceipt() {
    /* A new authority may reject a state write, but preserves old evidence. */
    atomic {
        current_fence++;
        observation = true;
#ifndef BUG_EVOLUTION_DROP_LATE
        evidence_stored = true;
#endif
        if
        :: assigned_fence == current_fence -> skip
        :: else ->
#ifdef BUG_EVOLUTION_STALE_COMMIT
            stale_committed = true;
#else
            skip;
#endif
        fi;
        invariants()
    }
}

init {
    atomic {
        run Execute(); run Upgrade(); run Maintain();
        run Slot(0); run Slot(0); run Slot(1); run DelayedReceipt()
    }
}
