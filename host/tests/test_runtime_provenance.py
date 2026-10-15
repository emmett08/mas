"""Version evidence stays content-bound, bounded and read-only."""
import hashlib
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from mas_host.definitions import DefinitionRegistry, canonical
from mas_host.diagnostics import doctor
from mas_host.operations import DeploymentLedger
from mas_host.runtime_provenance import build_record, package_digest, retained_versions


class ProvenanceTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.package = self.root / 'package'
        self.package.mkdir()
        (self.package / '__init__.py').write_text('raise AssertionError("must not import")\n')
        self.binaries = {name: {'sha256': hashlib.sha256(name.encode()).hexdigest()}
                         for name in ('mas', 'mas-gateway')}
        self.record = {'format': 'mas-runtime-build/1', 'source_commit': 'a' * 40,
            'release_tag': 'v0.3.0', 'host_version': '0.1.0', 'package_sha256': package_digest(self.package),
            'binaries': {name: {**value, 'version': '0.1.0'} for name, value in self.binaries.items()}}

    def write(self, value=None):
        (self.package / 'runtime_build.json').write_text(json.dumps(self.record if value is None else value))

    def test_absent_and_matching_build_evidence(self):
        self.assertEqual(build_record(self.package, self.binaries)['status'], 'not_recorded')
        self.write()
        with patch('subprocess.Popen', side_effect=AssertionError('execution forbidden')):
            result = build_record(self.package, self.binaries)
        self.assertEqual(result['status'], 'content_bound')
        self.assertEqual(result['source_commit'], 'a' * 40)
        self.assertTrue(all(result['binary_matches'].values()))
        self.assertNotIn(str(self.root), json.dumps(result))

    def test_changed_sources_or_binaries_cannot_inherit_build_version(self):
        self.write()
        different = {**self.binaries, 'mas': {'sha256': 'b' * 64}}
        result = build_record(self.package, different)
        self.assertEqual(result['status'], 'binary_mismatch')
        self.assertFalse(result['binary_matches']['mas'])
        (self.package / '__init__.py').write_text('changed')
        result = build_record(self.package, self.binaries)
        self.assertEqual(result['status'], 'package_mismatch')
        self.assertIsNone(result['source_commit'])
        self.assertNotIn('binaries', result)

    def test_malformed_symlinked_and_duplicate_records_fail_without_details(self):
        for value in ({**self.record, 'source_commit': 'secret'},
                      {**self.record, 'binaries': {}}, {**self.record, 'extra': 'secret'}):
            self.write(value)
            result = build_record(self.package, self.binaries)
            self.assertEqual(result['status'], 'unavailable')
            self.assertNotIn('secret', json.dumps(result))
        path = self.package / 'runtime_build.json'
        path.write_text('{"format":1,"format":2}')
        self.assertEqual(build_record(self.package, self.binaries)['status'], 'unavailable')
        path.unlink()
        path.symlink_to(self.package / '__init__.py')
        self.assertEqual(build_record(self.package, self.binaries)['status'], 'unavailable')

    def test_registry_reports_only_version_bindings_without_loading_retired_code(self):
        state = self.root / 'state'
        DeploymentLedger(state)
        registry = DefinitionRegistry(state)
        worker = {'format': 'mas-executable-worker/1', 'name': 'private-worker', 'version': 'worker-v2',
                  'command': ['/private/executable'], 'configuration': {'secret': 'sk-private'}}
        provider = {'provider': 'private-provider', 'implementation': 'adapter-v3',
                    'contract': 'structured-inference/1', 'instructions': 'private prompt'}
        descriptor = {'version': 'workflow-v1', 'worker_bindings': {'private-actor': worker},
                      'contract_descriptor': {'agent': {'provider': provider}}}
        identity = hashlib.sha256(canonical(descriptor).encode()).hexdigest()
        registry.remember('workflow', 'private-name', identity, descriptor=descriptor)
        registry.retire('workflow', 'private-name', identity)
        before = (state / 'definition-registry.sqlite').read_bytes()
        result = retained_versions(state)
        self.assertEqual(result['status'], 'retained')
        item = result['items'][0]
        self.assertEqual(item['workers'][0]['version'], 'worker-v2')
        self.assertEqual(item['providers'][0]['implementation'], 'adapter-v3')
        self.assertTrue(item['retired'])
        self.assertEqual(item['execution_availability'], 'not_checked')
        for private in ('private-name', 'private-worker', 'private-actor', 'sk-private',
                        'private prompt', '/private/executable', 'private-provider'):
            self.assertNotIn(private, json.dumps(result))
        self.assertEqual(before, (state / 'definition-registry.sqlite').read_bytes())
        with registry.transaction() as db:
            db.execute("UPDATE definitions SET descriptor='{}'")
        self.assertEqual(retained_versions(state)['status'], 'unavailable')

    def test_retained_version_bound_is_explicit(self):
        state = self.root / 'state'
        DeploymentLedger(state)
        registry = DefinitionRegistry(state)
        for version in ('1', '2'):
            descriptor = {'version': version}
            identity = hashlib.sha256(canonical(descriptor).encode()).hexdigest()
            registry.remember('workflow', 'private-name', identity, descriptor=descriptor)
        with patch('mas_host.runtime_provenance.MAX_DEFINITIONS', 1):
            report = retained_versions(state)
        self.assertEqual(len(report['items']), 1)
        self.assertTrue(report['has_more'])
        self.assertEqual(report['limit'], 1)

    def test_doctor_versions_require_the_exact_observed_binary(self):
        path = self.root / 'executable'
        path.write_text('#!/bin/sh\nexit 99\n'); path.chmod(0o700)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        evidence = {**self.record, 'status': 'content_bound',
            'binary_matches': {'mas': True, 'mas-gateway': False}}
        with patch('mas_host.runtime_provenance.build_record', return_value=evidence), \
             patch('subprocess.Popen', side_effect=AssertionError('no execution')):
            result = doctor(self.root / 'absent', environ={'MAS_BIN': str(path), 'MAS_GATEWAY_BIN': str(path)})
        self.assertEqual(result['runtime']['binaries']['mas']['version'], '0.1.0')
        self.assertEqual(result['runtime']['binaries']['mas']['sha256'], digest)
        self.assertIsNone(result['runtime']['binaries']['mas-gateway']['version'])
        self.assertFalse((self.root / 'absent').exists())

    def test_build_writer_binds_generated_package_and_does_not_run_binary(self):
        import shutil
        repository = Path(__file__).resolve().parents[2]
        host = self.root / 'host'; host.mkdir()
        shutil.copytree(repository / 'host' / 'mas_host', host / 'mas_host', ignore=shutil.ignore_patterns('__pycache__'))
        shutil.copyfile(repository / 'host' / 'pyproject.toml', host / 'pyproject.toml')
        binary_dir = self.root / 'bin'; binary_dir.mkdir()
        for name in self.binaries:
            (binary_dir / name).write_bytes(name.encode())
        subprocess.run([sys.executable, str(repository / 'scripts' / 'write-runtime-provenance.py'),
            '--host', str(host), '--rust-manifest', str(repository / 'rust-protocol' / 'Cargo.toml'),
            '--binaries', str(binary_dir), '--source-commit', 'a' * 40, '--release-tag', 'v0.3.0'], check=True)
        result = build_record(host / 'mas_host', self.binaries)
        self.assertEqual(result['status'], 'content_bound')
        self.assertEqual(result['release_tag'], 'v0.3.0')


if __name__ == '__main__':
    unittest.main()
