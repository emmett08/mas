"""Replay the bounded authority corpus against native PostgreSQL transitions.

Expected observations come from the reviewed formal corpus, not a Python copy of
the transition function. This is selected implementation correspondence, not a
proof that every concrete implementation history refines the model.
"""
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from mas_host import RunnerError
from mas_host.distributed import remote_binding, WORKER_PROTOCOL
from mas_host.service_contracts import ServiceError
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_control import SharedWorkerController
import test_shared_service as fixtures


ROOT = Path(__file__).resolve().parents[2]


class NativeAuthorityTrace:
    def __init__(self, testcase, root):
        self.testcase, self.root = testcase, root
        self.settings = {'binary': os.environ.get('MAS_SHARED_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-shared'),
            'dsn': os.environ['MAS_TEST_POSTGRES_DSN'], 'namespace': 'mas_trace_' + uuid.uuid4().hex,
            'deployment': 'trace-deployment', 'secret_key': b'authority-trace-fixture' * 2,
            'secret_reference': 'trace-key/1'}
        self.current = 0
        self.authorities = {0: self.authority(0, 1)}
        self.authorities[0].initialise()
        self.authorities[0].renew(lease_seconds=300)
        for scope in ('deployment', 'principal/alice', 'ancestor/root', 'provider/fixture'):
            self.authorities[0].configure_scope(scope, max_runs=10, max_calls=10, max_microusd=10)
        digest = hashlib.sha256(b'formal-authority-fixture').hexdigest()
        self.grants = {f'worker-{worker}': {'trust_zone': 'trusted', 'capacity': 1,
            'capabilities': {'work': digest}} for worker in range(2)}
        self.binding = remote_binding('alice', 'work', digest, 'trusted', {}, 1)
        self.controllers = {0: SharedWorkerController(self.authorities[0], self.grants, lease_ms=300000)}
        self.boots = [0, 0]
        for worker in range(2):
            self.register(worker)
        self.tasks = [None, None]
        self.assignments = [None, None]
        self.authoritative = [False, False]
        self.runners = []

    def authority(self, controller, epoch):
        authority = SharedAuthority(**self.settings, controller=f'controller-{controller}', epoch=epoch)
        self.testcase.addCleanup(authority.close)
        return authority

    def close(self):
        for runner in self.runners:
            try:
                runner.close()
            except RunnerError:
                pass  # A fenced handle cannot release ownership.

    def register(self, worker):
        return self.controllers[self.current].register(f'worker-{worker}',
            boot=f'boot-{worker}-{self.boots[worker]}', capabilities=self.grants[f'worker-{worker}']['capabilities'],
            architecture='x86_64', runtime='python-3.14.0', trust_zone='trusted', protocol=WORKER_PROTOCOL)

    def act(self, operation, args):
        try:
            if operation == 'admit':
                controller, task = args
                authority = self.authorities[controller]
                runner = authority.runner({'agents': ['a', 'b'], 'application': 'agent-task/1',
                    'max_calls': 2, 'wall_seconds': 300},
                    {'a': fixtures.SharedEcho(), 'b': fixtures.SharedEcho()}, self.root / f'task-{task}',
                    principal='alice', ancestors=('ancestor/root', 'provider/fixture'),
                    workflow_version='authority-trace/1', supervised=False)
                self.runners.append(runner)
                runner._journal.step('effect', 'invoke', {'input': self.binding}, {})
                call = runner._journal.create_invocation('effect', 'b', 1)
                runner._session.admit_invocation(call['call_id'])
                self.controllers[controller].submit_admitted(call['call_id'], self.binding)
                self.tasks[task] = call['call_id']
                return True
            if operation == 'claim':
                controller, worker, task = args
                assignment = self.controllers[controller].claim(f'worker-{worker}',
                    boot=f'boot-{worker}-{self.boots[worker]}')['assignment']
                if assignment is None:
                    return False
                self.testcase.assertEqual(assignment['call_id'], self.tasks[task],
                                          'native scheduling selected a different logical task')
                self.assignments[task] = assignment
                return True
            if operation in ('dispatch', 'replay_dispatch'):
                controller, worker, task = args
                assignment = self.assignments[task]
                return self.controllers[controller].dispatch(f'worker-{worker}',
                    **{key: assignment[key] for key in ('boot', 'call_id', 'fence')})['execute']
            if operation == 'receipt':
                controller, worker, task = args
                assignment = self.assignments[task]
                receipt = {'id': f'receipt-{task}', 'call_id': self.tasks[task],
                    'boot': assignment['boot'], 'fence': assignment['fence'], 'status': 'completed',
                    'output': {'task': task}, 'evidence': {'fixture': True}, 'output_profile': 'inline-json/1'}
                ack = self.controllers[controller].receipt(f'worker-{worker}', receipt=receipt)
                self.authoritative[task] = ack['authoritative']
                return ack['stored']
            if operation == 'drain':
                controller, draining = args
                self.authorities[controller].set_draining(draining, operator='fixture', evidence={'trace': True})
                return True
            if operation == 'handover':
                controller, = args
                binding = self.authorities[self.current].handover(f'controller-{controller}')
                self.authorities[controller] = self.authority(controller, binding['epoch'])
                self.controllers[controller] = SharedWorkerController(self.authorities[controller],
                    self.grants, lease_ms=300000)
                self.current = controller
                return True
            if operation == 'reboot':
                worker, = args
                self.boots[worker] += 1
                self.register(worker)
                return True
            raise AssertionError(f'unsupported formal corpus operation: {operation}')
        except (RunnerError, ServiceError):
            return False

    def observe(self):
        authority = self.authorities[self.current]
        status = authority.authority_status()
        states, evidence = [], []
        for call in self.tasks:
            if call is None:
                states.append('absent')
                evidence.append(0)
            else:
                state = self.controllers[self.current].inspect('alice', call)['state']
                states.append('admitted' if state == 'pending' else state)
                evidence.append(len(self.controllers[self.current].evidence('alice', call)))
        allocations = [authority.query('scope', {'scope': scope})
                       for scope in ('deployment', 'principal/alice', 'ancestor/root', 'provider/fixture')]
        self.testcase.assertEqual({value['calls'] for value in allocations}, {allocations[0]['calls']})
        self.testcase.assertEqual({value['microusd'] for value in allocations}, {allocations[0]['calls']})
        return {'epoch': status['epoch'], 'controller': int(status['controller'].removeprefix('controller-')),
                'draining': status['draining'],
                'charges': allocations[0]['calls'], 'states': states,
                'authoritative': list(self.authoritative), 'stored_evidence': evidence}


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedAuthorityTraceTests(unittest.TestCase):
    def test_formal_corpus_observations_match_native_shared_authority(self):
        corpus = json.loads((ROOT / 'formal/authority-traces.json').read_text())
        self.assertEqual(corpus['version'], 1)
        self.assertGreaterEqual(len(corpus['traces']), 6)
        for trace in corpus['traces']:
            with self.subTest(trace=trace['name']), tempfile.TemporaryDirectory() as root:
                native = NativeAuthorityTrace(self, Path(root))
                try:
                    for ordinal, action in enumerate(trace['actions']):
                        accepted = native.act(action['op'], action['args'])
                        actual = {'accepted': accepted, **native.observe()}
                        self.assertEqual(actual, action['expect'],
                            f"trace {trace['name']} step {ordinal}: {action['op']} {action['args']}")
                finally:
                    native.close()
