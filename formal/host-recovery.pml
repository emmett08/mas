/* Bounded safety abstraction: one logical checkpoint, two charged attempts,
 * at most one crash and two responses per dispatched attempt. Durable commits
 * are separate transitions. Pending old-owner responses survive a new dispatch.
 * No-effect resolutions require trusted external evidence; remote effects are
 * opaque. This is not a proof of liveness, billing or implementation refinement.
 */
mtype = { Intent, Charged, Reserved, Dispatched, Recorded, Unknown };
mtype phase = Intent;
byte attempt = 0;
byte owner = 1;
byte ticket_owner[2];
byte dispatches[2];
byte responses[2];
bool charged[2];
bool reserved[2];
bool crashed = false;
bool retry_authorised = false;
bool result_written = false;
byte result_owner = 0;
byte result_value = 0;
byte first_result = 0;
byte charges = 0;

inline check_result() {
    assert(!result_written || result_owner == owner);
    assert(!result_written || result_value == first_result)
}

inline complete(ticket) {
    atomic {
        responses[ticket]++;
        if
#ifdef BUG_HOST_STALE_OWNER
        :: !result_written ->
#else
        :: !result_written && ticket_owner[ticket] == owner && ticket == attempt ->
#endif
            result_owner = ticket_owner[ticket];
            result_value = responses[ticket]; first_result = result_value;
            result_written = true; phase = Recorded
#ifdef BUG_HOST_MUTABLE_RESULT
        :: result_written -> result_value = 3
#endif
        :: else -> skip /* Reject stale/conflicting completion; first result wins. */
        fi;
        check_result()
    }
}

active proctype Host() {
    do
    :: phase == Intent ->
#ifdef BUG_HOST_UNCHARGED
        reserved[attempt] = true; phase = Reserved
#else
        charged[attempt] = true; charges++; phase = Charged
#endif
    :: phase == Charged -> reserved[attempt] = true; phase = Reserved
    :: phase == Reserved ->
        atomic {
            assert(charged[attempt] && reserved[attempt]);
            assert(attempt == 0 || retry_authorised);
            dispatches[attempt]++;
            assert(dispatches[attempt] == 1);
            ticket_owner[attempt] = owner; phase = Dispatched
        }
    :: dispatches[0] > 0 && responses[0] < 2 -> complete(0)
    :: dispatches[1] > 0 && responses[1] < 2 -> complete(1)
    :: !crashed && phase != Recorded ->
        atomic { crashed = true; owner++; phase = Unknown; check_result() }
    :: phase == Unknown ->
        if
        :: skip /* Leave ambiguous work blocked. */
        :: atomic { /* Trusted evidence establishes a completed output. */
            phase = Recorded; result_written = true;
            result_owner = owner; result_value = 1; first_result = 1;
            check_result()
        }
        :: attempt == 0 ->
            /* Evidence-backed no-effect decision permits a NEW charged attempt. */
            atomic { retry_authorised = true; attempt = 1; phase = Intent }
#ifdef BUG_HOST_REPLAY_UNKNOWN
        :: phase = Reserved /* Defect: dispatch the uncertain attempt again. */
#endif
        fi
    :: break /* Stopping is permitted; no eventual progress claim. */
    od;
    check_result();
    assert(charges <= 2);
    assert(dispatches[0] <= 1 && dispatches[1] <= 1)
}
