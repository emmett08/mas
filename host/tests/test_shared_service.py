"""Native task receipts, ownership and cache-loss recovery on actual PostgreSQL."""
from dataclasses import replace
import os
from pathlib import Path
import tempfile
import time
import unittest
from unittest import mock
import uuid

from mas_host import Invocation, Outcome, RunnerError
from mas_host.service_contracts import ServiceError
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_service import create_shared_service
from mas_host.task_service import Workflow


class SharedEcho:
    def handle(self, context, request):
        return Outcome(request.input)


class SharedEffect:
    def __init__(self, effects):
        self.effects = effects

    def handle(self, context, request):
        def effect():
            self.effects.append(request.input)
            return Invocation({'answer': request.input['x'] + 1}, {'certainty': 'completed'})
        return Outcome(context.invoke(effect, input=request.input))


def paused(_outcome):
    return 'input_required'


def continued(previous, supplied):
    return supplied


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedServiceTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.authority = SharedAuthority(
            binary=os.environ.get('MAS_SHARED_BIN') or str(Path(__file__).resolve().parents[2] / 'rust-protocol/target/debug/mas-shared'),
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace='mas_task_' + uuid.uuid4().hex,
            deployment='deployment', controller='controller', epoch=1,
            secret_key=b'test-secret-only' * 3, secret_reference='key/1', default_reservation_microusd=1)
        self.authority.initialise()
        self.addCleanup(self.authority.close)
        self.effects = []
        self.workflow = Workflow('shared-task/1', {'agents': ['a', 'b'], 'application': 'agent-task/1',
            'max_calls': 5, 'wall_seconds': 30}, {'a': SharedEcho(), 'b': SharedEffect(self.effects)}, 'a', 'b', 'work')
        self.limits = {name: {'max_runs': 100, 'max_calls': 100, 'max_microusd': 1000}
                       for name in ('deployment', 'principal/alice', 'principal/bob')}
        self.service = self.build('cache-one')

    def build(self, cache, *, workflow=None, versions=None, authority=None):
        return create_shared_service(authority or self.authority, Path(self.temp.name) / cache,
            {'work': workflow or self.workflow}, {'alice': {'work'}, 'bob': {'work'}},
            workflow_versions=versions, allocation_limits=self.limits, supervised=False)

    def test_receipts_and_result_survive_replaced_cache_without_effect_repeat(self):
        submitted = self.service.submit('alice', 'key', 'work', {'x': 7})
        task = submitted['task_id']
        self.assertEqual(self.service.run_once(task)['result'], {'answer': 8})
        replacement = self.build('cache-two')
        self.assertEqual(replacement.submit('alice', 'key', 'work', {'x': 7})['task_id'], task)
        self.assertEqual(replacement.result('alice', task)['result'], {'answer': 8})
        self.assertIsNone(replacement.run_once())
        self.assertEqual(self.effects, [{'x': 7}])
        self.assertFalse(list(Path(self.temp.name).rglob('*.sqlite')))
        with self.assertRaises(ServiceError) as denied:
            replacement.inspect('bob', task)
        self.assertEqual(denied.exception.code, 'task_not_found')

    def test_lost_submission_and_completion_replies_reconcile_original_receipt(self):
        original = self.authority.command
        def lost_submit(operation, parameters, **kwargs):
            reply = original(operation, parameters, **kwargs)
            if operation == 'task.submit':
                raise RunnerError('injected lost committed response')
            return reply
        with mock.patch.object(self.authority, 'command', side_effect=lost_submit):
            with self.assertRaises(RunnerError):
                self.service.submit('alice', 'key', 'work', {'x': 1})
        task = self.service.submit('alice', 'key', 'work', {'x': 1})['task_id']
        def lost_complete(operation, parameters, **kwargs):
            reply = original(operation, parameters, **kwargs)
            if operation == 'task.complete':
                raise RunnerError('injected lost committed response')
            return reply
        with mock.patch.object(self.authority, 'command', side_effect=lost_complete):
            with self.assertRaises(RunnerError):
                self.service.run_once(task)
        self.assertEqual(self.service.result('alice', task)['state'], 'completed')
        self.assertIsNone(self.build('cache-two').run_once(task))
        self.assertEqual(self.effects, [{'x': 1}])

    def test_queue_claim_fences_second_worker_and_stale_epoch_completion(self):
        task = self.service.submit('alice', 'key', 'work', {'x': 2})['task_id']
        first = self.authority.command('task.claim', {'owner': 'worker-one', 'task': task, 'workflows': ['work']})['receipt']
        self.assertIsNotNone(first)
        self.assertIsNone(self.authority.command('task.claim', {'owner': 'worker-two', 'task': task, 'workflows': ['work']})['receipt'])
        binding = self.authority.handover('successor')
        successor = SharedAuthority(binary=self.authority._binary, dsn=os.environ['MAS_TEST_POSTGRES_DSN'],
            namespace=self.authority.namespace, deployment='deployment', controller='successor', epoch=binding['epoch'],
            secret_key=b'test-secret-only' * 3, secret_reference='key/1', default_reservation_microusd=1)
        self.addCleanup(successor.close)
        with self.assertRaises(RunnerError):
            self.authority.command('task.complete', {'principal': 'alice', 'task': task,
                'owner': 'worker-one', 'state': 'completed', 'result': {}, 'error': None})
        replacement = self.build('cache-two', authority=successor)
        self.assertEqual(replacement.run_once(task)['result'], {'answer': 3})
        self.assertEqual(replacement.recovery_runs('alice', task)[0]['run_id'], first['run_id'])
        self.assertEqual(self.effects, [{'x': 2}])

    def test_versioned_epoch_retirement_is_shared_and_owner_scoped(self):
        task = self.service.submit('alice', 'same', 'work', {'x': 3}, request_epoch='spring')['task_id']
        with self.assertRaises(ServiceError) as active:
            self.service.retire_request_epoch('alice', 'spring', reason='done')
        self.assertEqual(active.exception.code, 'request_epoch_in_use')
        self.service.run_once(task)
        self.service.retire_request_epoch('alice', 'spring', reason='done')
        replacement = self.build('cache-two')
        for key in ('same', 'new'):
            with self.assertRaises(ServiceError) as closed:
                replacement.submit('alice', key, 'work', {'x': 3}, request_epoch='spring')
            self.assertEqual(closed.exception.code, 'request_epoch_retired')
        self.assertEqual(replacement.inspect('alice', task)['state'], 'completed')
        self.assertNotEqual(replacement.submit('bob', 'same', 'work', {'x': 3}, request_epoch='spring')['task_id'], task)

    def test_missing_historical_implementation_blocks_execution_but_keeps_receipt(self):
        task = self.service.submit('alice', 'old', 'work', {'x': 4})['task_id']
        version2 = replace(self.workflow, version='shared-task/2')
        replacement = self.build('cache-two', workflow=version2)
        self.assertEqual(replacement.run_once(task)['state'], 'blocked')
        restored = self.build('cache-three', workflow=version2, versions={'work': (self.workflow,)})
        self.assertEqual(restored.run_once(task)['result'], {'answer': 5})
        self.assertEqual(self.effects, [{'x': 4}])

    def test_queue_clock_expires_without_dispatch_after_cache_replacement(self):
        timed = replace(self.workflow, timing_policy={'format': 'mas-timing-policy/1',
            'business_seconds': 20, 'active_seconds': 5, 'queue_seconds': 1, 'pause_seconds': 5})
        timed_service = self.build('clock-one', workflow=timed)
        task = timed_service.submit('alice', 'timed', 'work', {'x': 5})['task_id']
        time.sleep(1.05)
        replacement = self.build('clock-two', workflow=timed)
        self.assertEqual(replacement.run_once(task)['error']['code'], 'queue_deadline')
        self.assertEqual(self.effects, [])

    def test_paused_clock_and_continuation_share_original_task(self):
        workflow = replace(self.workflow, classify=paused, continue_input=continued,
            timing_policy={'format': 'mas-timing-policy/1', 'business_seconds': 30,
                'active_seconds': 10, 'queue_seconds': 10, 'pause_seconds': 10})
        service = self.build('pause-one', workflow=workflow)
        task = service.submit('alice', 'start', 'work', {'x': 8})['task_id']
        first = service.run_once(task)
        self.assertEqual(first['state'], 'input_required')
        active = first['timing']['active_ms']
        time.sleep(.05)
        replacement = self.build('pause-two', workflow=workflow)
        self.assertEqual(replacement.inspect('alice', task)['timing']['active_ms'], active)
        updated = replacement.update('alice', task, 'continue', {'x': 9})
        self.assertEqual((updated['task_id'], updated['revision']), (task, 1))
        self.assertEqual(replacement.run_once(task)['state'], 'input_required')
        self.assertEqual(len(replacement.recovery_runs('alice', task)), 2)
