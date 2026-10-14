"""Real PostgreSQL metadata with fault-controlled S3 transport boundaries."""
import concurrent.futures
from dataclasses import replace
import hashlib
import os
from pathlib import Path
import tempfile
import unittest
import uuid

from mas_host.artifacts import ArtifactError, ArtifactPolicy, ArtifactRef, _manifest_identity
from mas_host.shared_artifacts import SharedArtifactStore
from mas_host.shared_authority import SharedAuthority
from mas_host.s3_artifacts import S3ArtifactContent
from mas_host.s3_transport import S3ContentConfig
import test_s3_artifacts as fixtures


@unittest.skipUnless(os.environ.get('MAS_TEST_POSTGRES_DSN'), 'requires actual PostgreSQL backend fixture')
class SharedArtifactTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.root = Path(self.directory.name)
        self.schema = 'mas_artifact_' + uuid.uuid4().hex
        self.authority = self.authority_for('controller', 1)
        self.authority.initialise()
        self.client = fixtures.ObjectClient()
        self.config = S3ContentConfig('https://objects.example', 'test-bucket', 'mas', 'us-east-1',
                                      self.root / 'credentials-not-used-by-transport-double')
        self.backend = S3ArtifactContent(self.config, client=self.client)
        self.policy = ArtifactPolicy(max_artifact_bytes=1024, max_total_bytes=2048, max_artifacts=2, max_links=4)
        self.writer = self.store('writer')

    def authority_for(self, controller, epoch):
        authority = SharedAuthority(binary=os.environ['MAS_SHARED_BIN'],
            dsn=os.environ['MAS_TEST_POSTGRES_DSN'], namespace=self.schema,
            deployment='deployment', controller=controller, epoch=epoch,
            secret_key=b'fixture-secret' * 4, secret_reference='test-key/1')
        self.addCleanup(authority.close)
        return authority

    def store(self, principal, *, authority=None, policy=None):
        return SharedArtifactStore(authority or self.authority, namespace='results', principal=principal,
            content_backend=self.backend, policy=policy or self.policy)

    def test_replacement_cache_grants_hashes_revocation_and_retention(self):
        ref = self.writer.put_bytes(b'precise bytes', readers=('reader',))
        reader = self.store('reader')
        self.assertEqual(reader.read_bytes(ref), b'precise bytes')
        self.assertFalse(list(self.root.rglob('*.sqlite')))
        with self.assertRaises(ArtifactError):
            self.store('intruder').read_bytes(ref)
        with self.assertRaises(ArtifactError):
            self.store('intruder').link('fake-release', [ref], state='released')
        reader.ensure_link('consumer', [ref])
        reader.link('consumer', [ref], state='released')
        self.assertEqual(reader.ensure_link('consumer', [ref])['state'], 'released')
        self.writer.revoke(ref, 'reader')
        with self.assertRaises(ArtifactError):
            reader.read_bytes(ref)
        self.assertEqual(self.writer.put_bytes(b'precise bytes', readers=('reader',)), ref)
        with self.assertRaises(ArtifactError):
            reader.read_bytes(ref)  # re-publication cannot undo a revocation
        self.client.corrupt = True
        with self.assertRaises(ArtifactError):
            self.writer.verify(ref)

    def test_lost_put_retains_charge_without_publishing_then_recovers(self):
        self.client.lose_reply = True
        with self.assertRaises(ArtifactError):
            self.writer.put_bytes(b'known immutable input')
        status = self.writer.status()
        self.assertEqual(status['artifacts'], [])
        self.assertEqual(status['content'], {'objects': 1, 'reserved_bytes': 21, 'unverified': 1})
        self.client.lose_reply = False
        replacement = self.store('writer')
        ref = replacement.put_bytes(b'known immutable input')
        self.assertEqual(replacement.read_bytes(ref), b'known immutable input')
        self.assertEqual(replacement.status()['content']['objects'], 1)

    def test_epoch_change_during_upload_cannot_publish_from_stale_controller(self):
        put = self.client.put_object
        binding = []
        def fenced_put(**request):
            result = put(**request)
            binding.append(self.authority.handover('replacement'))
            return result
        self.client.put_object = fenced_put
        with self.assertRaises(ArtifactError):
            self.writer.put_bytes(b'escaped original upload')
        self.client.put_object = put
        successor = self.authority_for('replacement', binding[0]['epoch'])
        replacement = self.store('writer', authority=successor)
        self.assertEqual(replacement.status()['artifacts'], [])
        self.assertEqual(replacement.status()['content']['unverified'], 1)
        ref = replacement.put_bytes(b'escaped original upload')
        self.assertEqual(replacement.read_bytes(ref), b'escaped original upload')
        self.assertEqual(replacement.status()['content']['objects'], 1)
        with self.assertRaises(ArtifactError):
            self.writer.put_bytes(b'new stale bytes')

    def test_concurrent_namespace_reservations_hold_total_byte_limit(self):
        narrow = ArtifactPolicy(max_artifact_bytes=1024, max_total_bytes=1024, max_artifacts=4, max_links=4)
        store = SharedArtifactStore(self.authority, namespace='bounded', principal='writer',
                                    content_backend=self.backend, policy=narrow)
        def reserve(index):
            data = bytes([index]) * 700
            provisional = ArtifactRef('bounded', '0'*64, hashlib.sha256(data).hexdigest(), len(data),
                                       'application/octet-stream', 'writer')
            ref = replace(provisional, artifact_id=_manifest_identity(provisional, []))
            try:
                store._command('reserve', {'reference': ref.to_dict(), 'readers': []})
                return True
            except ArtifactError:
                return False
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            results = list(pool.map(reserve, range(4)))
        self.assertEqual(sum(results), 1)
        self.assertEqual(store.status()['content'], {'objects': 1, 'reserved_bytes': 700, 'unverified': 1})
        self.assertEqual(self.client.calls, [])

    def test_forged_reference_or_changed_backend_cannot_reuse_namespace(self):
        ref = self.writer.put_bytes(b'valid')
        forged = replace(ref, artifact_id='f'*64)
        with self.assertRaises(ArtifactError):
            self.writer._command('reserve', {'reference': forged.to_dict(), 'readers': []})
        with self.assertRaises(ArtifactError):
            SharedArtifactStore(self.authority, namespace='results', principal='writer', policy=self.policy,
                content_backend=S3ArtifactContent(replace(self.config, prefix='changed'), client=self.client))
        self.assertEqual(self.writer.status()['content']['objects'], 1)


if __name__ == '__main__':
    unittest.main()
