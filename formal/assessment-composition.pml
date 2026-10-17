/* One uncertain request and one assessment-driven revision. Each apply step is
 * one repository/journal commit, one dispatch, or one scoped read. The scheduler
 * interleaves these boundaries; it does not make a whole workflow atomic.
 * Provider returns are finite; restart can instead leave an unknown terminal
 * outcome. SHA256 equality, trusted identity and atomic durable storage assumed.
 */
mtype = { Commit, Observe, ForeignObserve, Resume, Receive, Reserve,
          Dispatch, Assess, Reply, Revise, RevDispatch, RevComplete,
          Restart, Cancel, Expire };
bool pending=true;
bool stored;
byte identities;
bool received;
byte terminal; /* 0 absent/open, 1 completed, 2 cancelled, 3 expired */
byte first_terminal;
bool observed;
bool foreign_observed;
bool absent_seen;
bool absent_promised;
bool resumed;
bool reserved;
byte dispatches;
bool assessment;
byte assessment_content;
bool replied;
bool revision;
byte revision_dispatches;
bool revision_result;
bool restarted;
bool restart_decided;
bool unknown;
bool cancelled;
bool cancel_decided;
bool expired;
bool expiry_decided;
bool settled;
bool found;
bool before_received;
byte feedback;
bool blocked;

inline check() {
    assert(identities <= 1); /* COMPOSE_REQUEST_IDENTITY */
    assert(!absent_promised || !stored); /* COMPOSE_ABSENCE_IS_SNAPSHOT */
    assert(!received || stored); /* COMPOSE_RECEIPT_REQUIRES_ADMISSION */
    assert(dispatches == 0 || reserved); /* COMPOSE_DISPATCH_RESERVED */
    assert(dispatches <= 1 && revision_dispatches <= 1); /* COMPOSE_UNKNOWN_NOT_REDISPATCHED */
    assert(!assessment || assessment_content == 1); /* COMPOSE_RETAINED_ASSESSMENT */
    assert(first_terminal == 0 || terminal == first_terminal); /* COMPOSE_TERMINAL_IMMUTABLE */
    assert(!revision || (assessment && feedback == assessment_content)); /* COMPOSE_FEEDBACK_BOUND */
    assert(revision_dispatches == 0 || revision); /* COMPOSE_REVISION_RESERVED */
}

inline admit() {
    if
    :: stored -> skip
    :: !stored && !expired -> stored=true; identities++
    :: else -> skip
    fi
}

inline apply(op) {
    if
    :: op == Commit -> pending=false; admit()
    :: op == Observe ->
        observed=true; before_received=received; found=stored;
        if :: !found -> absent_seen=true :: else -> skip fi;
#ifdef BUG_COMPOSE_ABSENCE
        if :: !found -> absent_promised=true :: else -> skip fi;
#endif
#ifdef BUG_COMPOSE_OBSERVE_WRITE
        received=stored;
#endif
        assert(received == before_received); /* COMPOSE_OBSERVATION_READ_ONLY */
    :: op == ForeignObserve ->
        foreign_observed=true; found=false;
#ifdef BUG_COMPOSE_SCOPE
        found=stored;
#endif
        assert(!found); /* COMPOSE_OBSERVATION_SCOPED */
    :: op == Resume ->
        resumed=true;
#ifdef BUG_COMPOSE_IDENTITY
        if :: stored -> identities++ :: else -> skip fi;
#endif
        admit()
    :: op == Receive -> received=true
    :: op == Reserve -> reserved=true
    :: op == Dispatch ->
#ifdef BUG_COMPOSE_UNRESERVED
        reserved=false;
#endif
        dispatches++
    :: op == Assess -> assessment=true; assessment_content=1
    :: op == Reply ->
        replied=true;
#ifdef BUG_COMPOSE_TERMINAL
        terminal=1;
#else
        if :: terminal == 0 && !expired -> terminal=1 :: else -> skip fi;
#endif
        if :: first_terminal == 0 -> first_terminal=terminal :: else -> skip fi
    :: op == Revise ->
        blocked=cancelled || expired;
        if
#ifdef BUG_COMPOSE_STOP
        :: true ->
#else
        :: !blocked ->
#endif
            revision=true; feedback=assessment_content;
#ifdef BUG_COMPOSE_FEEDBACK
            feedback=2;
#endif
            assert(!blocked); /* COMPOSE_STOP_PREVENTS_REVISION */
        :: else -> skip
        fi
    :: op == RevDispatch -> revision_dispatches++
    :: op == RevComplete -> revision_result=true
    :: op == Restart ->
        restarted=true; restart_decided=true;
        if
        :: (reserved && !assessment) || (revision && !revision_result) -> unknown=true
        :: else -> skip
        fi;
#ifdef BUG_COMPOSE_REPLAY
        if :: unknown && dispatches == 1 -> dispatches++ :: else -> skip fi;
#endif
#ifdef BUG_COMPOSE_HISTORY
        if :: assessment -> assessment_content=2 :: else -> skip fi;
#endif
    :: op == Cancel ->
        cancelled=true; cancel_decided=true;
        if :: stored && terminal == 0 -> terminal=2; first_terminal=2 :: else -> skip fi
    :: op == Expire ->
        expired=true; expiry_decided=true;
        if :: stored && terminal == 0 -> terminal=3; first_terminal=3 :: else -> skip fi
    fi;
    check()
}

#ifndef SHARED_TRACE
init {
    do
    :: atomic { pending -> apply(Commit) }
    :: atomic { !observed -> apply(Observe) }
    :: atomic { !foreign_observed -> apply(ForeignObserve) }
    :: atomic { !resumed -> apply(Resume) }
    :: atomic { stored && !received -> apply(Receive) }
    :: atomic { !settled && received && terminal == 0 && !reserved && !expired && !cancelled && !unknown -> apply(Reserve) }
    :: atomic { !settled && reserved && dispatches == 0 && !expired && !cancelled && !unknown -> apply(Dispatch) }
    :: atomic { dispatches == 1 && !assessment && !unknown -> apply(Assess) }
    :: atomic { assessment && !replied -> apply(Reply) }
    :: atomic { !settled && assessment && replied && !revision && !unknown ->
        apply(Revise);
        if :: blocked -> settled=true :: else -> skip fi }
    :: atomic { !settled && revision && revision_dispatches == 0 && !expired && !cancelled && !unknown -> apply(RevDispatch) }
    :: atomic { revision_dispatches == 1 && !revision_result && !unknown -> apply(RevComplete) }
    :: atomic { !restart_decided -> if :: apply(Restart) :: restart_decided=true fi }
    :: atomic { !cancel_decided -> if :: apply(Cancel) :: cancel_decided=true fi }
    :: atomic { !expiry_decided -> if :: apply(Expire) :: expiry_decided=true fi }
    :: atomic { !settled && (cancelled || expired || unknown || revision_result) -> settled=true }
    :: settled && !pending -> break
#ifdef BUG_COMPOSE_STALL
    :: !settled -> skip
#endif
    od;
#ifdef WITNESS_COMPOSE_SUCCESS
    assert(!revision_result); /* REACH_COMPOSE_REVISION */
#endif
#ifdef WITNESS_COMPOSE_LATE_COMMIT
    assert(!(absent_seen && stored)); /* REACH_COMPOSE_LATE_COMMIT */
#endif
#ifdef WITNESS_COMPOSE_LATE_EVIDENCE
    assert(!(assessment && terminal == 2)); /* REACH_COMPOSE_LATE_EVIDENCE */
#endif
#ifdef WITNESS_COMPOSE_RETAINED_RESTART
    assert(!(restarted && assessment && !unknown)); /* REACH_COMPOSE_RETAINED_RESTART */
#endif
}
ltl assessment_composition_settles { <> settled }
#endif
