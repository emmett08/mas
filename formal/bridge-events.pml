/* Durable event history with bounded task admission and paginated reads.
 * There is no server acknowledgement or guaranteed push delivery. A client
 * supplies its own cursor; a lost response or cursor is safely reread.
 * Two admitted tasks, three event slots, two reads and one client restart.
 */
byte tasks = 0;
byte event_count = 0;
byte transition_count = 0;
byte cursor = 0;
byte page = 0;
byte requests = 0;
byte reads = 0;
byte received_max = 0;
byte saved_cursor = 0;
bool restarted = false;
bool page_pending = false;

inline invariants() {
    assert(tasks <= 2);                       /* BRIDGE_INBOX_BOUND */
    assert(event_count <= 3);                 /* BRIDGE_EVENT_BOUND */
    assert(event_count == transition_count);  /* BRIDGE_EVENT_COMMIT */
    assert(cursor <= received_max);           /* BRIDGE_CURSOR_OBSERVED */
    assert(received_max <= event_count);      /* BRIDGE_EVENT_COMMITTED */
    assert(saved_cursor <= cursor)           /* BRIDGE_CURSOR_REPLAY */
}

active proctype Service() {
    do
    :: requests < 3 ->
        atomic {
            requests++;
            if
#ifdef BUG_BRIDGE_INBOX_OVERFLOW
            :: true ->
#else
            :: tasks < 2 ->
#endif
                tasks++; event_count++; transition_count++
            :: else -> skip /* Bounded admission rejects before insertion. */
            fi;
            invariants()
        }
    :: tasks == 2 && event_count < 3 ->
        atomic { event_count++; transition_count++; invariants() }
    :: break
    od
}

active proctype Reader() {
    do
    :: reads < 2 && !page_pending ->
        atomic {
            reads++;
            if
            :: cursor < event_count -> page = cursor + 1
            :: else -> page = cursor
            fi;
#ifdef BUG_BRIDGE_UNCOMMITTED_EVENT
            page = event_count + 1;
#endif
            page_pending = true;
            invariants()
        }
    :: page_pending ->
        atomic {
            if
            :: received_max < page -> received_max = page
            :: else -> skip
            fi;
            if
            :: cursor = page /* Caller consumes the returned page, then advances. */
            :: skip          /* Lost reply: caller retries its previous cursor. */
            fi;
            page_pending = false;
            invariants()
        }
    :: !restarted ->
        atomic {
            restarted = true;
            if :: saved_cursor = cursor :: saved_cursor = 0 fi;
            cursor = saved_cursor; page_pending = false;
            invariants()
        }
    :: break
    od;
    invariants()
}
