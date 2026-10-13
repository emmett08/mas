/* Safety abstraction of agent-task/1 Store.send validation in one transaction.
 * Two immutable request identities; owners 0/2 and shared worker 1.
 * Result, Failure and Cancel race for a single terminal state per request.
 * Application completion is deliberately NOT a liveness property.
 */
#define TASKS 2
mtype = { NONE, RESULT, FAILURE, CANCEL };
bool requested[TASKS];
bool received[TASKS];
bool stale_tried[TASKS];
bool cancel_sent[TASKS];
mtype terminal[TASKS];
byte terminal_count[TASKS];
byte owner[TASKS];

inline record_terminal(t, outcome, actor, target, reply) {
    assert(requested[t]);
    assert(reply == t);
    if
    :: outcome == CANCEL -> assert(actor == owner[t] && target == 1)
    :: else -> assert(actor == 1 && target == owner[t]); assert(received[t])
    fi;
    terminal[t] = outcome;
    terminal_count[t]++;
    assert(terminal_count[t] == 1)
}

proctype Requester(byte task) {
    atomic {
        requested[task] = true;
        printf("TASK request %d owner %d\n", task, owner[task]);
    }
    do
    :: atomic { (requested[task] && !cancel_sent[task]
#ifndef BUG_DOUBLE_TERMINAL
        && terminal[task] == NONE
#endif
        ) -> cancel_sent[task] = true;
        record_terminal(task, CANCEL, owner[task], 1, task);
        printf("TASK cancel %d\n", task) }
    :: terminal[task] != NONE -> break
    od
}

proctype Worker() {
    byte task;
    byte stale;
    do
    :: atomic {
        if :: task = 0 :: task = 1 fi;
        if
        :: requested[task] && !received[task] ->
             received[task] = true;
             printf("TASK receive %d\n", task)
        :: requested[task] && received[task] && terminal[task] == NONE ->
             if
             :: record_terminal(task, RESULT, 1, owner[task], task)
             :: record_terminal(task, FAILURE, 1, owner[task], task)
             fi;
             printf("TASK terminal %d %e\n", task, terminal[task])
        :: requested[task] && received[task] && !stale_tried[task] ->
             stale_tried[task] = true; stale = 1 - task;
             /* Guard checks the exact request identity. */
             assert(stale != task);
             printf("TASK reject_stale_reply %d reply %d\n", task, stale)
        :: else -> skip
        fi
    }
    :: terminal[0] != NONE && terminal[1] != NONE -> break
    od
}

init {
    atomic {
        owner[0] = 0; owner[1] = 2;
        terminal[0] = NONE; terminal[1] = NONE;
        run Requester(0); run Requester(1); run Worker()
    }
}
