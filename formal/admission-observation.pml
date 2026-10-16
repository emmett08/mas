/* Local native host admission and protocol read-only observation.
 * One logical request, two possible identifiers (the second is a mutation),
 * one durable request slot and one independent mailbox occupant. A call makes
 * at most two attempts; one explicit resume retains identity and deadline.
 * Production uses eight attempts. No aggregate bound over arbitrary resumes
 * or restarts follows. Due/cancel are observations, not physical clock models.
 *
 * A send transaction may reconcile expiry even when admission is rejected.
 * Rejected therefore means the new request was not admitted, not that every
 * protocol row stuttered. Observer transactions alone leave all state unchanged.
 * SQLite atomicity, durable recovery and truthful error classification are
 * assumptions. Loss of a commit reply and crash before reply handling are
 * explicit, separate from the durable commit. Unknown blocks this invocation.
 * An explicit later resume can obtain the original duplicate admission before
 * its first worker launch; that is not replay of an uncertain external effect.
 */
mtype = { Prepared, Running, Completed, Blocked, Refused,
          NoReply, Accepted, Retryable, Rejected, Unknown };
mtype step = Prepared;
mtype disposition = NoReply;
byte identity = 1;
byte durable_identity = 0;
byte durable_revision = 0;
byte attempts = 0;
byte invocation_actual_attempts = 0;
byte total_attempts = 0;
byte resumes = 0;
byte launches = 0;
bool occupied = false;
bool occupancy_released = false;
bool committed = false;
bool protocol_expired = false;
bool due_observed = false;
bool cancelled = false;
bool unknown_seen = false;
bool retry_seen = false;
bool lost_commit_reply = false;
bool resumed_unknown = false;
bool unknown_current = false;
bool submitted = false;
bool settled = false;
bool host_done = false;
bool observed = false;

inline choose(value) { if :: value=true :: value=false fi }

active proctype Host() {
    bool crash;
    bool commit_reply;
    bool expire_at_send;
    submitted=true;
    choose(occupied);
again:
    do
    :: attempts<2 ->
        choose(due_observed);
        choose(cancelled);
        if
#ifdef BUG_ADMISSION_DEADLINE
        :: true -> skip
#else
        :: due_observed || cancelled -> goto finish
        :: !due_observed && !cancelled -> skip
#endif
        fi;
        assert(!due_observed && !cancelled); /* ADMISSION_LIVE_AT_RETRY_CHECK */
        assert(!unknown_current); /* ADMISSION_UNKNOWN_NOT_RETRIED */
        assert(identity==1); /* ADMISSION_IDENTITY_RETAINED */
        attempts++; invocation_actual_attempts++; total_attempts++;
        assert(invocation_actual_attempts<=2); /* ADMISSION_ATTEMPT_BOUND */
        choose(expire_at_send);
        atomic {
            /* Expiry can become observable after the host's prior check. */
            if
            :: expire_at_send && !protocol_expired -> protocol_expired=true; durable_revision++
            :: else -> skip
            fi;
            if
            :: committed -> disposition=Accepted /* identical resend */
            :: !committed && protocol_expired -> disposition=Rejected
            :: !committed && !protocol_expired && occupied -> disposition=Retryable
            :: !committed && !protocol_expired && !occupied ->
                if
                :: disposition=Rejected /* validation failure, no insertion */
                :: disposition=Unknown /* operational failure before commit */
                :: committed=true; durable_identity=identity; durable_revision++;
                   disposition=Accepted
                fi
            fi
        };
        /* Commit and response are intentionally not atomic. */
        if
        :: disposition==Accepted ->
            choose(commit_reply);
            if
            :: !commit_reply -> disposition=Unknown; lost_commit_reply=true
            :: commit_reply -> skip
            fi
        :: else -> skip
        fi;
        choose(crash);
        if
        :: crash -> step=Blocked; goto finish
        :: !crash -> skip
        fi;
        if
        :: disposition==Retryable ->
            assert(!committed); /* ADMISSION_RETRY_PROVES_ABSENT */
            retry_seen=true;
#ifdef BUG_ADMISSION_IDENTITY
            identity=2;
#endif
#ifdef BUG_ADMISSION_BOUND
            attempts=0;
#endif
#ifdef BUG_ADMISSION_STALL
            do :: occupied -> skip :: !occupied -> break od;
#endif
            skip
        :: disposition==Unknown ->
            unknown_seen=true; unknown_current=true;
#ifdef BUG_ADMISSION_UNKNOWN_RETRY
            attempts=0
#else
            step=Blocked; goto invocation_end
#endif
        :: disposition==Rejected -> step=Refused; goto finish
        :: disposition==Accepted ->
            /* Duplicate admission is still a receipt after expiry, but the
             * receive transaction marks its task non-actionable. */
            if
#ifdef BUG_ADMISSION_EXPIRED_LAUNCH
            :: true -> skip
#else
            :: protocol_expired -> step=Blocked; goto finish
            :: !protocol_expired -> skip
#endif
            fi;
            assert(!protocol_expired); /* ADMISSION_LAUNCH_REQUIRES_ACTIONABLE */
            assert(committed && durable_identity==1); /* ADMISSION_LAUNCH_REQUIRES_COMMIT */
            step=Running; launches++;
            assert(launches==1); /* ADMISSION_SINGLE_LAUNCH */
            step=Completed; goto finish
        fi
    :: attempts==2 -> break
    od;
    /* Retry exhaustion is a known nonadmission. One explicit later call may
     * resume, subject to the same identity and a fresh original-deadline check.
     */
    if
    :: resumes==0 -> resumes++; attempts=0; invocation_actual_attempts=0; goto again
    :: else -> skip
    fi;
    goto finish;
invocation_end:
    assert(launches==0); /* ADMISSION_UNKNOWN_BLOCKS_LAUNCH */
    /* This is a distinct explicit host invocation, never an automatic retry.
     * It keeps the old durable step, Request identity and original deadline.
     */
    if
    :: resumes==0 ->
        resumes++; resumed_unknown=true; unknown_current=false;
        attempts=0; invocation_actual_attempts=0; step=Prepared; goto again
    :: else -> skip
    fi;
finish:
    assert(!unknown_current || launches==0); /* ADMISSION_UNKNOWN_BLOCKS_LAUNCH */
    assert(!committed || durable_identity==1); /* ADMISSION_IDENTITY_RETAINED */
    settled=true;
    host_done=true;
#ifdef WITNESS_ADMISSION_RETRY_SUCCESS
    assert(!(retry_seen && step==Completed)); /* REACH_ADMISSION_RETRY_SUCCESS */
#endif
#ifdef WITNESS_ADMISSION_LOST_COMMIT
    assert(!(lost_commit_reply && committed && step==Blocked)); /* REACH_ADMISSION_LOST_COMMIT */
#endif
#ifdef WITNESS_ADMISSION_RESUME_DUPLICATE
    assert(!(lost_commit_reply && resumed_unknown && step==Completed)); /* REACH_ADMISSION_RESUME_DUPLICATE */
#endif
}

active proctype Recipient() {
    /* Acknowledging an unrelated message frees capacity independently. */
    if
    :: occupied -> atomic { occupied=false; occupancy_released=true; durable_revision++ }
    :: !occupied -> skip
    fi
}

active proctype Observer() {
    byte before_revision;
    byte before_identity;
    bool before_committed;
    bool before_expired;
    byte reads=0;
    do
    :: reads<2 ->
        atomic {
            before_revision=durable_revision;
            before_identity=durable_identity;
            before_committed=committed;
            before_expired=protocol_expired;
#ifdef BUG_OBSERVER_MUTATION
            durable_revision++;
#endif
            observed=true;
            assert(durable_revision==before_revision && durable_identity==before_identity && committed==before_committed && protocol_expired==before_expired); /* OBSERVER_PRESERVES_PROTOCOL_STATE */
            reads++
        }
    :: reads==2 -> break
    od
}

ltl admission_settles { [] (submitted -> <> settled) }
