/* Source authority, one snapshot, one audit restore and one replacement. Two
 * logical call IDs, unit charges, original execution grants and observations.
 * A final archive is captured after sealing; external fencing evidence is a
 * separate trusted input. Old/non-final snapshots cannot become active. */
bool source_sealed = false;
bool source_fenced = false;
bool snapshot_exists = false;
bool snapshot_final = false;
bool restored = false;
bool target_open = false;
bool activated = false;
bool source_admitted[2];
bool source_granted[2];
bool snapshot_admitted[2];
bool snapshot_granted[2];
bool target_admitted[2];
bool target_granted[2];
bool target_archived[2];
bool observed[2];
bool target_evidence[2];
bool observed_after_replacement[2];
byte source_charges = 0;
byte snapshot_charges = 0;
byte target_charges = 0;
byte historical_admissions[2];
byte historical_grants[2];
bool rejected_stale_snapshot = false;
bool accepted = false;

inline invariants() {
    assert(historical_admissions[0] <= 1 && historical_admissions[1] <= 1); /* RECOVERY_IDENTITY_UNIQUE */
    assert(historical_grants[0] <= 1 && historical_grants[1] <= 1); /* RECOVERY_NO_REDISPATCH */
    assert(!target_open || activated); /* RECOVERY_ACTIVATION_GATE */
    assert(!target_open || source_fenced); /* RECOVERY_SOURCE_FENCED */
    assert(!target_open || snapshot_final); /* RECOVERY_FINAL_SNAPSHOT */
    assert(!target_open || (target_admitted[0] == (historical_admissions[0] != 0) && target_admitted[1] == (historical_admissions[1] != 0))); /* RECOVERY_ADMISSION_HISTORY */
    assert(!target_open || target_charges == historical_admissions[0] + historical_admissions[1]); /* RECOVERY_CHARGES_RETAINED */
    assert(!target_open || (target_granted[0] == (historical_grants[0] != 0) && target_granted[1] == (historical_grants[1] != 0))); /* RECOVERY_UNKNOWN_RETAINED */
    assert(!target_evidence[0] || observed[0]); /* RECOVERY_CORRELATED_EVIDENCE */
    assert(!target_evidence[1] || observed[1]); /* RECOVERY_CORRELATED_EVIDENCE */
#ifdef WITNESS_RECOVERY_STALE_AUDIT
    assert(!(rejected_stale_snapshot && !target_open && snapshot_charges < source_charges)); /* REACH_RECOVERY_STALE_AUDIT */
#endif
#ifdef WITNESS_RECOVERY_ACTIVE
    assert(!(target_open && snapshot_charges != 0 && target_charges >= snapshot_charges)); /* REACH_RECOVERY_ACTIVE */
#endif
#ifdef WITNESS_RECOVERY_LATE_RESULT
    assert(!(target_open && snapshot_granted[0] && target_evidence[0] && observed_after_replacement[0])); /* REACH_RECOVERY_LATE_RESULT */
#endif
}

inline source_admit(t) {
    atomic {
        accepted = false;
        if
        ::
#ifndef BUG_RECOVERY_SEALED_SOURCE
           !source_sealed &&
#endif
           !source_admitted[t] ->
            assert(!source_sealed); /* RECOVERY_SEALED_SOURCE */
            source_admitted[t] = true; source_charges++; historical_admissions[t]++
        :: else -> skip
        fi;
        invariants()
    }
}

inline source_dispatch(t) {
    atomic {
        accepted = false;
        if
        :: !source_sealed && source_admitted[t] && !source_granted[t] ->
            source_granted[t] = true; historical_grants[t]++
        :: else -> skip
        fi;
        invariants()
    }
}

inline seal_source() { atomic { source_sealed = true; invariants() } }
inline fence_source() {
    atomic {
        accepted = false;
        if
        :: source_sealed -> source_fenced = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline snapshot() {
    atomic {
        accepted = false;
        if
        :: !snapshot_exists ->
            snapshot_exists = true; snapshot_final = source_sealed;
            snapshot_charges = source_charges;
            snapshot_admitted[0] = source_admitted[0]; snapshot_admitted[1] = source_admitted[1];
            snapshot_granted[0] = source_granted[0]; snapshot_granted[1] = source_granted[1]
        :: else -> skip
        fi;
        invariants()
    }
}

inline restore_audit() {
    atomic {
        accepted = false;
        if
        :: snapshot_exists && !restored ->
            restored = true; target_open = false;
            target_charges = snapshot_charges;
            target_admitted[0] = snapshot_admitted[0]; target_admitted[1] = snapshot_admitted[1];
            target_granted[0] = snapshot_granted[0]; target_granted[1] = snapshot_granted[1];
#ifdef BUG_RECOVERY_DROP_UNKNOWN
            target_granted[0] = false;
#endif
#ifdef BUG_RECOVERY_OPEN_RESTORE
            target_open = true;
#endif
        :: else -> skip
        fi;
        invariants()
    }
}

inline activate() {
    atomic {
        accepted = false;
        if
        :: restored && !target_open &&
#ifndef BUG_RECOVERY_UNFENCED
           source_fenced &&
#endif
#ifndef BUG_RECOVERY_STALE_SNAPSHOT
           snapshot_final &&
#endif
           true -> activated = true; target_open = true
        :: restored && !snapshot_final -> rejected_stale_snapshot = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline target_admit(t) {
    atomic {
        accepted = false;
        if
        :: target_open &&
#ifndef BUG_RECOVERY_DUPLICATE_ADMISSION
           !target_admitted[t] &&
#endif
           true ->
            target_admitted[t] = true; target_charges++; historical_admissions[t]++
        :: else -> skip
        fi;
        invariants()
    }
}

inline target_dispatch(t) {
    atomic {
        accepted = false;
        if
        :: target_open && target_admitted[t] && !target_granted[t] ->
            target_granted[t] = true; historical_grants[t]++
        :: else -> skip
        fi;
        invariants()
    }
}

inline observe_effect(t) {
    atomic {
        accepted = false;
        if
        :: historical_grants[t] != 0 && !observed[t] ->
            observed[t] = true;
            if
            :: target_open -> observed_after_replacement[t] = true
            :: else -> skip
            fi
        :: else -> skip
        fi;
        invariants()
    }
}

inline retain_evidence(t) {
    atomic {
        accepted = false;
        if
        :: target_open && target_granted[t] && observed[t] -> target_evidence[t] = true
        :: else -> skip
        fi;
        invariants()
    }
}

inline archive(t) {
    atomic {
        accepted = false;
        if
        :: target_open && target_evidence[t] && !target_archived[t] ->
            target_archived[t] = true;
#ifdef BUG_RECOVERY_ARCHIVE_REFUND
            target_charges--;
#endif
        :: else -> skip
        fi;
        invariants()
    }
}

init {
    do
    :: source_admit(0) :: source_admit(1)
    :: source_dispatch(0) :: source_dispatch(1)
    :: seal_source() :: fence_source() :: snapshot() :: restore_audit() :: activate()
    :: target_admit(0) :: target_admit(1)
    :: target_dispatch(0) :: target_dispatch(1)
    :: observe_effect(0) :: observe_effect(1)
    :: retain_evidence(0) :: retain_evidence(1)
    :: archive(0) :: archive(1)
    :: break
    od
}
