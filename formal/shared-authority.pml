/* Two controller bindings, two workers and two logical calls. Each call costs
 * one call/reservation unit in its deployment, principal, provider, root and
 * leaf scopes. Atomic transitions represent committed authority commands.
 * Admission precedes worker placement; controller handover does not revoke a
 * worker claim. Worker boot fencing and controller epochs are distinct. */
#ifndef DEPLOYMENT_LIMIT
#define DEPLOYMENT_LIMIT 2
#endif
#ifndef PRINCIPAL_LIMIT
#define PRINCIPAL_LIMIT 2
#endif
#ifndef PROVIDER_LIMIT
#define PROVIDER_LIMIT 2
#endif
#ifndef ANCESTOR_LIMIT
#define ANCESTOR_LIMIT 2
#endif
#ifndef LEAF_LIMIT
#define LEAF_LIMIT 2
#endif
#define LIVE(c) ((c) == controller)
#define HELD(t) (state[t] == Claimed || state[t] == Dispatched || state[t] == Unknown)
mtype = { Absent, Admitted, Claimed, Dispatched, Completed, Unknown, Archived };
mtype = { NoError, StaleAuthority, AdmissionDraining, NoAssignment, DispatchAlreadyGranted, Rejected };
mtype rejection = NoError;
mtype state[2];
byte controller = 0;
byte epoch = 1;
bool draining = false;
bool handed_over = false;
byte boot[2];
byte owner[2];
byte assigned_boot[2];
byte assignment_fence[2];
byte claimed_controller[2];
byte principal[2];
byte provider[2];
byte leaf_scope[2];
byte charges = 0;
byte principal_charges[2];
byte provider_charges[2];
byte root_charges = 0;
byte leaf_charges[2];
bool admitted[2];
byte claims[2];
byte dispatches[2];
byte stored_evidence[2];
bool authoritative[2];
bool archived[2];
bool accepted = false;
bool rejected_stale_admission = false;
bool rejected_stale_dispatch = false;
bool late_evidence = false;
bool completed_after_handover = false;
byte request_boot = 1;
byte request_fence = 1;
bool expired[2];
bool expired_evidence = false;

inline invariants() {
    assert(charges == admitted[0] + admitted[1]); /* AUTH_CHARGES_RETAINED */
    assert(charges <= DEPLOYMENT_LIMIT); /* AUTH_DEPLOYMENT_LIMIT */
    assert(principal_charges[0] <= PRINCIPAL_LIMIT && principal_charges[1] <= PRINCIPAL_LIMIT); /* AUTH_PRINCIPAL_LIMIT */
    assert(provider_charges[0] <= PROVIDER_LIMIT && provider_charges[1] <= PROVIDER_LIMIT); /* AUTH_PROVIDER_LIMIT */
    assert(root_charges <= ANCESTOR_LIMIT); /* AUTH_ANCESTOR_LIMIT */
    assert(leaf_charges[0] <= LEAF_LIMIT && leaf_charges[1] <= LEAF_LIMIT); /* AUTH_CHILD_ANCESTOR_LIMIT */
    assert(principal_charges[0] + principal_charges[1] == charges && provider_charges[0] + provider_charges[1] == charges && root_charges == charges && leaf_charges[0] + leaf_charges[1] == charges); /* AUTH_SCOPE_ACCOUNTING */
    assert(claims[0] <= 1 && claims[1] <= 1); /* AUTH_ONE_CLAIM */
    assert(!(HELD(0) && HELD(1)) || owner[0] != owner[1]); /* AUTH_WORKER_CAPACITY */
    assert(dispatches[0] <= 1 && dispatches[1] <= 1); /* AUTH_NO_REDISPATCH */
    assert(!archived[0] || state[0] != Unknown); /* AUTH_UNKNOWN_RETAINED */
    assert(!archived[1] || state[1] != Unknown); /* AUTH_UNKNOWN_RETAINED */
    assert(!authoritative[0] || stored_evidence[0] == 1); /* AUTH_COMPLETION_EVIDENCE */
    assert(!authoritative[1] || stored_evidence[1] == 1); /* AUTH_COMPLETION_EVIDENCE */
#ifdef WITNESS_AUTH_TWO_WORKERS
    assert(!(state[0] == Dispatched && state[1] == Dispatched)); /* REACH_AUTH_TWO_WORKERS */
#endif
#ifdef WITNESS_AUTH_STALE_REJECTED
    assert(!(rejected_stale_admission && rejected_stale_dispatch)); /* REACH_AUTH_STALE_REJECTED */
#endif
#ifdef WITNESS_AUTH_LATE_EVIDENCE
    assert(!late_evidence); /* REACH_AUTH_LATE_EVIDENCE */
#endif
#ifdef WITNESS_AUTH_HANDOVER_COMPLETION
    assert(!completed_after_handover); /* REACH_AUTH_HANDOVER_COMPLETION */
#endif
#ifdef WITNESS_AUTH_EXPIRED_EVIDENCE
    assert(!expired_evidence); /* REACH_AUTH_EXPIRED_EVIDENCE */
#endif
}

inline initialise() {
    state[0] = Absent; state[1] = Absent;
    boot[0] = 1; boot[1] = 1;
#ifdef SPLIT_SCOPES
    principal[1] = 1; provider[1] = 1; leaf_scope[1] = 1;
#endif
}

inline admit(c, t) {
    atomic {
        accepted = false;
        if
        :: !LIVE(c) -> rejection = StaleAuthority
        :: LIVE(c) && draining -> rejection = AdmissionDraining
        :: else -> rejection = Rejected
        fi;
        if
        :: handed_over && c == 0 -> rejected_stale_admission = true
        :: else -> skip
        fi;
        if
        ::
#ifndef BUG_AUTH_STALE_ADMISSION
           LIVE(c) &&
#endif
           !draining && !admitted[t] &&
#ifndef BUG_AUTH_DEPLOYMENT
           charges < DEPLOYMENT_LIMIT &&
#endif
#ifndef BUG_AUTH_PRINCIPAL
           principal_charges[principal[t]] < PRINCIPAL_LIMIT &&
#endif
#ifndef BUG_AUTH_PROVIDER
           provider_charges[provider[t]] < PROVIDER_LIMIT &&
#endif
#ifndef BUG_AUTH_ANCESTOR
           root_charges < ANCESTOR_LIMIT &&
#endif
#ifndef BUG_AUTH_CHILD_ANCESTOR
           leaf_charges[leaf_scope[t]] < LEAF_LIMIT &&
#endif
           true ->
            assert(LIVE(c)); /* AUTH_CURRENT_ADMISSION */
            admitted[t] = true; charges++;
            principal_charges[principal[t]]++; provider_charges[provider[t]]++;
            root_charges++; leaf_charges[leaf_scope[t]]++; state[t] = Admitted; accepted = true
        :: LIVE(c) && admitted[t] -> accepted = true /* Existing receipt, no new charge. */
        :: else -> skip
        fi;
        invariants()
    }
}

inline claim(c, w, t) {
    atomic {
        accepted = false; rejection = NoAssignment;
        if
        :: LIVE(c) && !draining && admitted[t] &&
#ifndef BUG_AUTH_DOUBLE_CLAIM
           state[t] == Admitted &&
#else
           (state[t] == Admitted || state[t] == Claimed) &&
#endif
           (!HELD(1-t) || owner[1-t] != w) ->
            claims[t]++; owner[t] = w; assigned_boot[t] = boot[w];
            assignment_fence[t]++; claimed_controller[t] = c;
            state[t] = Claimed; accepted = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline dispatch_token(c, w, t, token, fence) {
    atomic {
        accepted = false;
        if
        :: !LIVE(c) -> rejection = StaleAuthority
        :: LIVE(c) && draining -> rejection = AdmissionDraining
        :: LIVE(c) && !draining && dispatches[t] != 0 -> rejection = DispatchAlreadyGranted
        :: else -> rejection = Rejected
        fi;
        if
        :: handed_over && c == 0 -> rejected_stale_dispatch = true
        :: else -> skip
        fi;
        if
        ::
#ifndef BUG_AUTH_STALE_DISPATCH
           LIVE(c) &&
#endif
           !draining && admitted[t] && owner[t] == w &&
#ifndef BUG_AUTH_WORKER_FENCE
           token == assigned_boot[t] && token == boot[w] && fence == assignment_fence[t] &&
#endif
#ifndef BUG_AUTH_REDISPATCH
           dispatches[t] == 0 &&
#endif
           (state[t] == Claimed || state[t] == Dispatched) ->
            assert(LIVE(c)); /* AUTH_CURRENT_DISPATCH */
            assert(token == assigned_boot[t] && token == boot[w] && fence == assignment_fence[t]); /* AUTH_WORKER_FENCED */
            assert(admitted[t] && leaf_charges[leaf_scope[t]] != 0); /* AUTH_ADMITTED_DISPATCH */
            dispatches[t]++; state[t] = Dispatched; accepted = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline dispatch(c, w, t) { dispatch_token(c,w,t,assigned_boot[t],assignment_fence[t]) }

inline receipt(c, w, t) {
    atomic {
        accepted = false;
        if
        :: !LIVE(c) -> rejection = StaleAuthority
        :: else -> rejection = Rejected
        fi;
        if
        :: LIVE(c) && dispatches[t] != 0 && owner[t] == w ->
            accepted = true;
            if
            :: stored_evidence[t] != 0 -> skip /* Exact receipt replay retains the original ACK. */
            :: else ->
#ifdef BUG_AUTH_DROP_LATE
                if
                :: assigned_boot[t] == boot[w] -> stored_evidence[t] = 1
                :: else -> skip
                fi;
#else
                stored_evidence[t] = 1;
#endif
                assert(stored_evidence[t] == 1); /* AUTH_RECEIPT_RETAINED */
                if
#ifdef BUG_AUTH_STALE_COMPLETION
                :: true ->
#else
                :: assigned_boot[t] == boot[w] &&
#ifndef BUG_AUTH_EXPIRED_COMPLETION
                   state[t] == Dispatched ->
#else
                   true ->
#endif
#endif
                    assert(assigned_boot[t] == boot[w] && state[t] == Dispatched); /* AUTH_CURRENT_COMPLETION */
                    authoritative[t] = true; state[t] = Completed;
                    if
                    :: handed_over && claimed_controller[t] == 0 -> completed_after_handover = true
                    :: else -> skip
                    fi
                :: else ->
                    late_evidence = true;
                    if
                    :: assigned_boot[t] == boot[w] && expired[t] -> expired_evidence = true
                    :: else -> skip
                    fi
                fi
            fi
        :: else -> skip
        fi;
        invariants()
    }
}

inline expire(t) {
    atomic {
        accepted = false;
        if
        :: !expired[t] && state[t] == Claimed ->
            expired[t] = true; state[t] = Admitted; claims[t] = 0
        :: !expired[t] && state[t] == Dispatched ->
            expired[t] = true; state[t] = Unknown
        :: else -> skip
        fi;
        invariants()
    }
}

inline drain(c, value) {
    atomic {
        accepted = false;
        if
        :: LIVE(c) -> draining = value; accepted = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline handover(next) {
    atomic {
        accepted = false;
        if
        :: !handed_over && draining && next == 1 ->
            handed_over = true; controller = 1; epoch = 2; accepted = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline reboot(w) {
    atomic {
        accepted = false;
        if
        :: boot[w] == 1 ->
            boot[w] = 2; accepted = true;
            if
            :: owner[0] == w && state[0] == Claimed -> state[0] = Admitted; claims[0] = 0
            :: owner[0] == w && state[0] == Dispatched -> state[0] = Unknown
            :: else -> skip
            fi;
            if
            :: owner[1] == w && state[1] == Claimed -> state[1] = Admitted; claims[1] = 0
            :: owner[1] == w && state[1] == Dispatched -> state[1] = Unknown
            :: else -> skip
            fi
        :: else -> skip
        fi;
        invariants()
    }
}

inline archive(t) {
    atomic {
        accepted = false;
        if
        :: !archived[t] &&
#ifdef BUG_AUTH_ARCHIVE_UNKNOWN
           (state[t] == Completed || state[t] == Unknown) ->
#else
           state[t] == Completed ->
#endif
            archived[t] = true; accepted = true;
#ifdef BUG_AUTH_REFUND
            charges--;
#endif
            invariants();
            state[t] = Archived
        :: else -> skip
        fi;
        invariants()
    }
}

#ifndef TRACE_ONLY
init {
    initialise();
    do
    :: admit(0,0) :: admit(0,1) :: admit(1,0) :: admit(1,1)
    :: claim(0,0,0) :: claim(0,1,0) :: claim(0,0,1) :: claim(0,1,1)
    :: claim(1,0,0) :: claim(1,1,0) :: claim(1,0,1) :: claim(1,1,1)
    :: request_boot = 1 :: request_boot = 2
    :: request_fence = 1 :: request_fence = 2 :: request_fence = 3 :: request_fence = 4
    :: dispatch_token(0,0,0,request_boot,request_fence) :: dispatch_token(0,1,0,request_boot,request_fence)
    :: dispatch_token(0,0,1,request_boot,request_fence) :: dispatch_token(0,1,1,request_boot,request_fence)
    :: dispatch_token(1,0,0,request_boot,request_fence) :: dispatch_token(1,1,0,request_boot,request_fence)
    :: dispatch_token(1,0,1,request_boot,request_fence) :: dispatch_token(1,1,1,request_boot,request_fence)
    :: receipt(0,0,0) :: receipt(0,1,0) :: receipt(0,0,1) :: receipt(0,1,1)
    :: receipt(1,0,0) :: receipt(1,1,0) :: receipt(1,0,1) :: receipt(1,1,1)
    :: drain(0,true) :: drain(0,false) :: drain(1,true) :: drain(1,false)
    :: handover(1) :: reboot(0) :: reboot(1) :: archive(0) :: archive(1)
    :: expire(0) :: expire(1)
    :: break /* Safety only; caller/provider progress is not assumed. */
    od
}
#endif
