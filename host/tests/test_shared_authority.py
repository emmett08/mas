"""Real PostgreSQL and Rust shared-authority integration, run by backend CI."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid

from mas_host import Invocation, Outcome, RecoveryRequired, RunnerError
from mas_host.shared_authority import SharedAuthority


class Adapter:
    def __init__(self, function):
        self.function = function

    def handle(self, context, request):
        return self.function(context, request)


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedAuthorityIntegrationTests(unittest.TestCase):
    def setUp(self):
        binary = os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared')
        self.authority = SharedAuthority(binary=binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace='mas_py_' + uuid.uuid4().hex, deployment='deployment', controller='controller', epoch=1,
            secret_key=b'fixture-secret' * 4, secret_reference='test-key/1', default_reservation_microusd=10)
        self.authority.initialise()
        for scope in ('deployment', 'principal/alice', 'ancestor/parent'):
            self.authority.configure_scope(scope, max_runs=10, max_calls=10, max_microusd=100)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.topology = {'agents': ['a', 'b'], 'application': 'agent-task/1', 'max_calls': 5, 'wall_seconds': 120}
        self.effects = []
        def effect(context, request):
            value = context.invoke(lambda: self.invoke(request.input), input=request.input)
            return Outcome(value)
        self.adapters = {'a': Adapter(lambda _c, r: Outcome(r.input)), 'b': Adapter(effect)}

    def invoke(self, value):
        self.effects.append(value)
        return Invocation({'answer': value['x'] + 1}, {'certainty': 'completed'})

    def runner(self, *, run_id=None, cache='cache', principal='alice'):
        runner = self.authority.runner(self.topology, self.adapters, Path(self.temp.name) / cache,
            principal=principal, run_id=run_id, ancestors=('ancestor/parent',),
            workflow_version='shared-test/1', supervised=False)
        self.addCleanup(runner.close)
        return runner

    def test_completed_effect_replays_from_different_cache_without_reexecution(self):
        runner = self.runner()
        self.assertEqual(runner.request('a', 'b', 'work', {'x': 7}).output, {'answer': 8})
        run_id = runner.run_id
        runner.close()
        resumed = self.runner(run_id=run_id, cache='replacement-cache')
        self.assertEqual(resumed.request('a', 'b', 'work', {'x': 7}).output, {'answer': 8})
        self.assertEqual(self.effects, [{'x': 7}])
        self.assertFalse(list(Path(self.temp.name).rglob('*.sqlite')))
        for scope in ('deployment', 'principal/alice', 'ancestor/parent'):
            self.assertEqual(self.authority.query('scope', {'scope': scope})['calls'], 1)
        self.assertEqual(resumed.status()['calls'][0]['state'], 'recorded')

    def test_shared_run_ownership_and_principal_binding_survive_cache_separation(self):
        runner = self.runner()
        with self.assertRaises(RunnerError):
            self.runner(run_id=runner.run_id, cache='other-controller-cache')
        runner.close()
        with self.assertRaises(RunnerError):
            self.runner(run_id=runner.run_id, cache='wrong-principal', principal='mallory')
        self.runner(run_id=runner.run_id, cache='replacement')

    def test_lost_admission_reply_never_redispatches_and_retains_all_charges(self):
        runner = self.runner()
        original = self.authority._request
        def lost(operation, **kwargs):
            result = original(operation, **kwargs)
            if operation == 'command' and kwargs.get('command') == 'admit_invocation':
                raise RunnerError('injected lost admission reply')
            return result
        with mock.patch.object(self.authority, '_request', side_effect=lost):
            with self.assertRaises(RecoveryRequired):
                runner.request('a', 'b', 'work', {'x': 7})
        run_id = runner.run_id
        runner.close()
        resumed = self.runner(run_id=run_id, cache='replacement')
        with self.assertRaises(RecoveryRequired):
            resumed.request('a', 'b', 'work', {'x': 7})
        self.assertEqual(self.effects, [])
        self.assertEqual(resumed.status()['calls'][0]['state'], 'reserved')
        self.assertEqual(self.authority.query('scope', {'scope': 'deployment'})['calls'], 1)
