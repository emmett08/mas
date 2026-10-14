"""No-network callback used only by the private reference qualification."""
import fcntl
import json
import os
from pathlib import Path
import time

from .contracts import Invocation, Outcome
from .task_service import Workflow


class ReferenceAdapter:
    def __init__(self):
        # Trusted fixture configuration. Application input cannot select paths.
        self.evidence = Path(os.environ['MAS_REFERENCE_EVIDENCE'])

    def record(self, kind, value):
        with self.evidence.open('a', encoding='utf-8') as stream:
            fcntl.flock(stream, fcntl.LOCK_EX)
            stream.write(json.dumps({'kind': kind, 'time_ns': time.monotonic_ns(),
                                     'principal': value['principal'], 'key': value['key']}) + '\n')
            stream.flush()

    def handle(self, context, request):
        def call():
            self.record('start', request.input)
            time.sleep(0.1)
            self.record('end', request.input)
            return Invocation({'key': request.input['key']}, {'provider': 'reference-no-network'})
        return Outcome(context.invoke(call, input=request.input, reservation_microusd=0))


def workflows():
    topology = {'agents': ['client', 'worker'], 'edges': [['client', 'worker'], ['worker', 'client']],
                'application': 'agent-task/1', 'mailbox_capacity': 10,
                'max_messages': 20, 'max_bytes': 100000, 'max_calls': 1, 'wall_seconds': 20}
    return {'reference': Workflow('reference-load/1', topology,
        {'client': ReferenceAdapter(), 'worker': ReferenceAdapter()}, 'client', 'worker', 'reference')}
