"""Barrier workloads proving real assignment overlap without timing inference."""
import json
import os
from pathlib import Path
import time

from mas_host import Invocation, Outcome
from mas_host.task_service import Workflow


class Barrier:
    def __init__(self):
        # Capture the assignment owner before the adapter guardian forks. Avoid
        # reading /proc numeric paths across nested PID namespace mappings.
        self.service_pid = os.getpid()

    def handle(self, context, request):
        def call():
            guardian = os.getppid()
            Path(request.input["started"]).write_text(json.dumps(
                {"worker": os.getpid(), "guardian": guardian, "service": self.service_pid}))
            limit = time.monotonic() + 15
            while not Path(request.input["release"]).exists() and time.monotonic() < limit:
                time.sleep(0.01)
            return Invocation({"value": request.input["value"]}, {"provider": "barrier-fixture"})
        return Outcome(context.invoke(call, input=request.input, reservation_microusd=0))


def workflows():
    topology = {"agents": ["client", "worker"], "edges": [["client", "worker"], ["worker", "client"]],
                "application": "agent-task/1", "mailbox_capacity": 10,
                "max_messages": 20, "max_bytes": 100_000, "max_calls": 4, "wall_seconds": 20}
    return {"barrier": Workflow("barrier/1", topology,
        {"client": Barrier(), "worker": Barrier()}, "client", "worker", "barrier")}
