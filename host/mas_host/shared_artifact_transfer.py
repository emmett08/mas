"""Disposable bounded transfer caches over shared artefact authority.

Transfer IDs belong to one controller cache and expire. Committed references,
content, grants and revocations survive that cache; every read reauthorises the
shared reference before a cached byte can leave this process.
"""
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
import stat

from .artifacts import ArtifactRef, _open_dir
from .bridges.artifact_transfer import ArtifactScope, ArtifactTransferService
from .service_contracts import ServiceError
from .shared_artifacts import SharedArtifactStore


class SharedArtifactScope(ArtifactScope):
    def __init__(self, authority, root, namespace, principals, policy, content_backend):
        super().__init__(root, namespace, principals, policy, content_backend)
        if content_backend is None:
            raise ServiceError('shared_artifacts_require_s3')
        object.__setattr__(self, 'authority', authority)

    def binding(self):
        return hashlib.sha256(json.dumps({'deployment': self.authority._binding['deployment'],
            'database_namespace': self.authority.namespace, 'namespace': self.namespace,
            'principals': self.principals, 'policy': self.policy.__dict__,
            'content_backend': self.content_backend.descriptor}, sort_keys=True).encode()).hexdigest()

    def repository(self, principal, *, readonly=False):
        actor = self.principals.get(principal)
        if actor is None:
            raise ServiceError('artifact_unavailable')
        return SharedArtifactStore(self.authority, namespace=self.namespace, principal=actor,
            policy=self.policy, readonly=readonly, content_backend=self.content_backend)


class SharedArtifactTransferService(ArtifactTransferService):
    @contextmanager
    def _locked(self):
        directory = _open_dir(self.root, create=True)
        try:
            if stat.S_IMODE(os.fstat(directory).st_mode) & 0o077:
                raise ServiceError('artifact_storage_unsafe')
            fcntl.flock(directory, fcntl.LOCK_EX)
            self._cleanup(directory)
            yield directory
        finally:
            os.close(directory)

    def _capacity(self, directory, size):
        sessions, reserved = 0, 0
        for name in os.listdir(directory):
            if name.endswith('.json'):
                value = self._metadata(directory, name[:-5])
                sessions += 1
                reserved += value['size']
            elif name.endswith('.part'):
                info = os.stat(name, dir_fd=directory, follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise ServiceError('artifact_storage_unsafe')
                if not os.path.exists(self.root / (name[:-5] + '.json')):
                    reserved += info.st_size
            else:
                raise ServiceError('artifact_storage_unsafe')
        if sessions >= self.max_sessions or reserved + size > self.max_staging_bytes:
            raise ServiceError('artifact_staging_full')

    def _check_grant(self, directory, scope_name, scope, principal, reference):
        scope.repository(principal, readonly=True).inspect(reference)

    def revoke(self, principal, scope, reference, readers):
        selected = self._scope(principal, scope)
        ref = ArtifactRef.from_dict(reference)
        if (type(readers) is not list or not 1 <= len(readers) <= 32
                or any(reader not in selected.principals for reader in readers)
                or ref.producer != selected.principals[principal]):
            raise ServiceError('artifact_unavailable')
        repository = selected.repository(principal)
        repository.inspect(ref)
        for reader in readers:
            repository.revoke(ref, selected.principals[reader])
        return {'revoked': True}
