"""Native shared workspace lifecycle with real exec and fault-controlled S3 bytes."""
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import uuid

from mas_host.artifacts import ArtifactRef
from mas_host.executable_worker import ProfileExecutor
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig
from mas_host.shared_artifacts import SharedArtifactStore
from mas_host.shared_authority import SharedAuthority
from mas_host.shared_service import create_shared_service
from mas_host.workspaces import WorkspaceAdapter
from test_s3_artifacts import ObjectClient
from test_service_workspaces import ROOT, LostServiceReceipt, WorkspaceRegistration


GATEWAY = os.environ.get('MAS_GATEWAY_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-gateway')


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedWorkspaceServiceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.config = {'binary': os.environ.get('MAS_SHARED_BIN') or str(ROOT / 'rust-protocol/target/debug/mas-shared'),
            'dsn': os.environ['MAS_TEST_POSTGRES_DSN'], 'namespace': 'mas_workspace_' + uuid.uuid4().hex,
            'deployment': 'deployment', 'controller': 'controller', 'epoch': 1,
            'secret_key': b'workspace-fixture-secret' * 2, 'secret_reference': 'fixture/1'}
        self.authority = self.authority_for()
        self.authority.initialise()
        self.backend = S3ArtifactContent(S3ContentConfig('https://objects.example', 'test-bucket',
            'mas', 'us-east-1', self.directory / 'unused-credentials'), client=ObjectClient())

    def authority_for(self, **overrides):
        authority = SharedAuthority(**(self.config | overrides))
        self.addCleanup(authority.close)
        return authority

    def service(self, cache, authority=None):
        authority = authority or self.authority
        artifacts = SharedArtifactStore(authority, namespace='project', principal='alice', content_backend=self.backend)
        fixture = WorkspaceRegistration(self.directory / cache, artifacts=artifacts)
        selected = fixture.registration()
        sequence = {'format': 'mas-workflow/1', 'name': 'sequence', 'version': '1',
            'limits': {'max_steps': 10, 'max_parallel': 1, 'wall_seconds': 120,
                       'max_calls': 10, 'max_reserved_microusd': 20},
            'root': {'kind': 'sequence', 'steps': [{'kind': 'task', 'workflow': 'uppercase'}] * 2}}
        service = create_shared_service(authority, self.directory / cache, {'uppercase': selected},
            {'alice': {'uppercase', 'sequence'}}, plans={'sequence': sequence}, gateway_bin=GATEWAY,
            allocation_limits={scope: {'max_runs': 20, 'max_calls': 20, 'max_microusd': 100}
                               for scope in ('deployment', 'principal/alice')})
        return service, fixture, selected

    def test_replacement_replays_completed_workspace_from_native_journal_and_shared_artefacts(self):
        service, fixture, selected = self.service('first-cache')
        queued = service.submit('alice', 'lost-workspace', 'uppercase', {})
        execute = ProfileExecutor.execute

        def lose_receipt(executor, adapter, *args, **kwargs):
            outcome = execute(executor, adapter, *args, **kwargs)
            if isinstance(adapter, WorkspaceAdapter):
                # Re-enter WorkspaceExecutor during recovery: only the native
                # invocation checkpoint exists, not the root terminal reply.
                raise LostServiceReceipt()
            return outcome

        with mock.patch.object(ProfileExecutor, 'execute', new=lose_receipt):
            with self.assertRaises(LostServiceReceipt):
                service.run_once(queued['task_id'])
        original_run = service.recovery_runs('alice', queued['task_id'])[0]['run_id']
        self.assertEqual(service.inspect('alice', queued['task_id'])['state'], 'running')
        output_file = fixture.output_path(fixture.references[-1])
        self.assertEqual(output_file.read_bytes(), b'APPROVED SOURCE')
        output_file.unlink()
        before = self.authority.query('scope', {'scope': 'deployment'})
        steps = self.authority.query('steps', {'run': original_run})
        self.assertFalse(any(row['data'].get('terminal') for row in steps if row['kind'] == 'request'))
        handover = self.authority.handover('replacement')
        successor = self.authority_for(controller='replacement', epoch=handover['epoch'])
        restarted, replacement_fixture, rebound = self.service('empty-replacement-cache', successor)
        self.assertEqual(rebound.identity, selected.identity)
        done = restarted.run_once(queued['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        result = restarted.result('alice', queued['task_id'])
        reference = ArtifactRef.from_dict(result['artifacts'][0])
        self.assertEqual(replacement_fixture.artifacts.read_bytes(reference), b'APPROVED SOURCE')
        self.assertEqual(reference.input_sha256s, (fixture.source.sha256,))
        self.assertFalse(output_file.exists())
        self.assertFalse(replacement_fixture.output_path(replacement_fixture.references[-1]).exists(),
                         'replacement executed the workspace tool instead of replaying its native checkpoint')
        self.assertEqual(restarted.recovery_runs('alice', queued['task_id'])[0]['run_id'], original_run)
        self.assertEqual(successor.query('scope', {'scope': 'deployment'}), before)
        calls = successor.query('invocations', {'run': original_run})
        self.assertEqual([(call['state'], call['reservation_microusd']) for call in calls], [('completed', 1)])
        self.assertFalse(list(self.directory.rglob('*.sqlite')), 'shared ownership leaked into local SQLite')

    def test_shared_composition_dispatches_separate_registered_workspace_attempts(self):
        service, fixture, _selected = self.service('composition-cache')
        queued = service.submit('alice', 'workspace-sequence', 'sequence', {})
        done = service.run_once(queued['task_id'])
        self.assertEqual(done['state'], 'completed', done)
        result = service.result('alice', queued['task_id'])
        self.assertEqual(fixture.artifacts.read_bytes(ArtifactRef.from_dict(result['artifacts'][0])), b'APPROVED SOURCE')
        self.assertEqual(len(fixture.references), 2)
        self.assertNotEqual(fixture.references[0].identity, fixture.references[1].identity)
        runs = service.recovery_runs('alice', queued['task_id'])
        self.assertEqual(len(runs), 2)
        for run in runs:
            calls = self.authority.query('invocations', {'run': run['run_id']})
            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]['state'], 'completed')
        self.assertFalse(list(self.directory.rglob('*.sqlite')))


if __name__ == '__main__':
    unittest.main()
