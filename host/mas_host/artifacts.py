"""Bounded immutable local artefacts with explicit provenance and read grants.

The host supplies authenticated principals; this module does not authenticate
strings supplied by remote callers. Store below a run directory as ``artifacts``
to include content in deployment backups and run retention. References are data,
not bearer capabilities or filesystem paths. No remote URLs are dereferenced.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import asdict, dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid
from typing import BinaryIO, ContextManager, Iterator, Protocol

from .codec import is_actor_identity
from .operations import OperationsError, _file_lock


class ArtifactError(RuntimeError):
    """Fixed diagnostic suitable for an application boundary."""


_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}\Z")
_DIGEST = re.compile(r"[0-9a-f]{64}\Z")
_MEDIA = re.compile(r"[a-z0-9!#$&^_.+-]+/[a-z0-9!#$&^_.+-]+\Z")
_FORMAT = "mas-artifact/1"
_INDEX_FORMAT = "mas-artifact-store/1"
_MAX_INDEX = 16 * 1024 * 1024
_MAX_REF = 8192
_CHUNK = 1024 * 1024


def _identity(value: str) -> str:
    if type(value) is not str or not _ID.fullmatch(value):
        raise ArtifactError("invalid artefact identity")
    return value


def _principal(value: str) -> str:
    """Rust actor identity; opaque metadata, never a filesystem component."""
    if not is_actor_identity(value):
        raise ArtifactError("invalid artefact principal")
    return value


def _digest(value: str) -> str:
    if type(value) is not str or not _DIGEST.fullmatch(value):
        raise ArtifactError("invalid artefact digest")
    return value


def _json(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _load_json(data: bytes) -> object:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ArtifactError("duplicate artefact metadata key")
            result[key] = value
        return result
    try:
        return json.loads(data, object_pairs_hook=pairs)
    except (ValueError, UnicodeError):
        raise ArtifactError("invalid artefact metadata") from None


@dataclass(frozen=True)
class ArtifactPolicy:
    max_artifact_bytes: int = 16 * 1024 * 1024
    max_total_bytes: int = 64 * 1024 * 1024
    max_artifacts: int = 1024
    max_links: int = 4096

    def __post_init__(self):
        if any(type(v) is not int or v <= 0 or v > 2**63 - 1 for v in asdict(self).values()):
            raise ArtifactError("artefact limits must be positive bounded integers")
        if self.max_artifact_bytes > self.max_total_bytes or self.max_artifacts > 4096 or self.max_links > 16384:
            raise ArtifactError("invalid artefact policy bounds")


@dataclass(frozen=True)
class ArtifactRef:
    namespace: str
    artifact_id: str
    sha256: str
    size: int
    media_type: str
    producer: str
    input_sha256s: tuple[str, ...] = ()
    format: str = _FORMAT

    def __post_init__(self):
        if self.format != _FORMAT:
            raise ArtifactError("unsupported artefact reference version")
        _identity(self.namespace)
        _principal(self.producer)
        _digest(self.artifact_id)
        _digest(self.sha256)
        if type(self.size) is not int or self.size < 0 or self.size > 2**63 - 1:
            raise ArtifactError("invalid artefact size")
        if type(self.media_type) is not str or len(self.media_type) > 128 or not _MEDIA.fullmatch(self.media_type):
            raise ArtifactError("invalid artefact media type")
        if type(self.input_sha256s) is not tuple or len(self.input_sha256s) > 32:
            raise ArtifactError("invalid artefact input digests")
        for item in self.input_sha256s:
            _digest(item)
        if tuple(sorted(set(self.input_sha256s))) != self.input_sha256s:
            raise ArtifactError("artefact input digests must be unique and sorted")
        if len(_json(self.to_dict())) > _MAX_REF:
            raise ArtifactError("artefact reference exceeds message bound")

    def to_dict(self) -> dict:
        return {**asdict(self), "input_sha256s": list(self.input_sha256s)}

    @classmethod
    def from_dict(cls, value: dict) -> "ArtifactRef":
        if type(value) is not dict or set(value) != set(cls.__dataclass_fields__):
            raise ArtifactError("invalid artefact reference fields")
        if type(value["input_sha256s"]) is not list:
            raise ArtifactError("invalid artefact input digests")
        return cls(**{**value, "input_sha256s": tuple(value["input_sha256s"])})


class ArtifactRepository(Protocol):
    """Backend boundary; implementations must preserve authorisation and hashes."""

    def put_bytes(self, data: bytes, *, media_type: str = "application/octet-stream",
                  input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef: ...
    def put_file(self, path: Path, *, media_type: str = "application/octet-stream",
                 input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef: ...
    def read_bytes(self, reference: ArtifactRef | dict) -> bytes: ...
    def open(self, reference: ArtifactRef | dict) -> ContextManager[BinaryIO]: ...
    def verify(self, reference: ArtifactRef | dict) -> bool: ...
    def inspect(self, reference: ArtifactRef | dict) -> dict: ...
    def link(self, link_id: str, references: list[ArtifactRef | dict], *, state: str = "live") -> dict: ...
    def status(self) -> dict: ...


def _open_dir(path: Path, *, create: bool = False) -> int:
    """Walk with directory descriptors: no symlink component is followed."""
    path = Path(path).absolute()
    fd = os.open("/", os.O_RDONLY | os.O_DIRECTORY)
    try:
        for part in path.parts[1:]:
            if part in ("", ".", ".."):
                raise ArtifactError("unsafe artefact path")
            if create:
                try:
                    os.mkdir(part, 0o700, dir_fd=fd)
                    os.fsync(fd)
                except FileExistsError:
                    pass
            next_fd = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = next_fd
        return fd
    except BaseException:
        os.close(fd)
        raise


def _open_file(directory: int, name: str, flags: int = os.O_RDONLY) -> int:
    fd = os.open(name, flags | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600, dir_fd=directory)
    info = os.fstat(fd)
    if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
        os.close(fd)
        raise ArtifactError("artefact storage requires private regular files")
    return fd


def _read_file(directory: int, name: str, limit: int) -> bytes:
    with os.fdopen(_open_file(directory, name), "rb") as stream:
        if os.fstat(stream.fileno()).st_size > limit:
            raise ArtifactError("artefact metadata exceeds size bound")
        data = stream.read(limit + 1)
        if len(data) > limit:
            raise ArtifactError("artefact metadata exceeds size bound")
        return data


def _hash_source(source: BinaryIO, limit: int, *, destination: BinaryIO | None = None) -> tuple[int, str]:
    """Read bounded content, optionally copying the exact bytes being hashed."""
    digest, size = hashlib.sha256(), 0
    while chunk := source.read(min(_CHUNK, limit - size + 1)):
        size += len(chunk)
        if size > limit:
            raise ArtifactError("artefact exceeds per-object quota")
        if destination is not None:
            destination.write(chunk)
        digest.update(chunk)
    return size, digest.hexdigest()


def _atomic_json(directory: int, name: str, data: dict) -> None:
    encoded = _json(data)
    if len(encoded) > _MAX_INDEX:
        raise ArtifactError("artefact metadata exceeds size bound")
    pending = ".pending-" + uuid.uuid4().hex
    try:
        with os.fdopen(_open_file(directory, pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(pending, name, src_dir_fd=directory, dst_dir_fd=directory)
        os.fsync(directory)
    finally:
        try:
            os.unlink(pending, dir_fd=directory)
        except FileNotFoundError:
            pass


def _validate_index(index: object, namespace: str) -> ArtifactPolicy:
    if type(index) is not dict or set(index) != {"format", "namespace", "policy", "artifacts", "links"}:
        raise ArtifactError("invalid artefact store metadata")
    if index["format"] != _INDEX_FORMAT or index["namespace"] != namespace:
        raise ArtifactError("unsupported artefact store version or namespace")
    if type(index["policy"]) is not dict or set(index["policy"]) != set(ArtifactPolicy.__dataclass_fields__):
        raise ArtifactError("invalid artefact policy metadata")
    policy = ArtifactPolicy(**index["policy"])
    artifacts, links = index["artifacts"], index["links"]
    if type(artifacts) is not dict or len(artifacts) > policy.max_artifacts or type(links) is not dict or len(links) > policy.max_links:
        raise ArtifactError("artefact store metadata exceeds policy")
    for key, manifest in artifacts.items():
        if type(manifest) is not dict or set(manifest) != {"reference", "readers", "artifact_id"}:
            raise ArtifactError("invalid artefact manifest")
        ref = ArtifactRef.from_dict(manifest["reference"])
        readers = manifest["readers"]
        if (type(readers) is not list or len(readers) > 32 or any(type(item) is not str for item in readers)
                or readers != sorted(set(readers))):
            raise ArtifactError("invalid artefact grants")
        for reader in readers:
            _principal(reader)
        # IDs include grants/provenance, but omit the self-referential ID.
        identity = hashlib.sha256(_json({"reference": {k: v for k, v in ref.to_dict().items() if k != "artifact_id"}, "readers": readers})).hexdigest()
        if key != ref.artifact_id or key != manifest["artifact_id"] or key != identity or ref.namespace != namespace or ref.size > policy.max_artifact_bytes:
            raise ArtifactError("artefact manifest integrity mismatch")
    for key, link in links.items():
        _identity(key)
        if type(link) is not dict or set(link) != {"owner", "state", "artifacts"} or link["state"] not in ("live", "unresolved", "released"):
            raise ArtifactError("invalid artefact retention manifest")
        _principal(link["owner"])
        refs = link["artifacts"]
        if (type(refs) is not list or not refs or len(refs) > 64 or any(type(item) is not str for item in refs)
                or refs != sorted(set(refs)) or any(item not in artifacts for item in refs)):
            raise ArtifactError("invalid artefact retention references")
    return policy


class ArtifactStore:
    """Local per-namespace store. All publications are immutable and idempotent.

    Quotas count physical blobs, including crash-orphaned blobs. There is no
    automatic object deletion. Explicit live/unresolved links protect the whole
    containing run from operational retention; released links preserve history.
    Read grants and producer/input provenance are frozen at publication.
    """

    def __init__(self, root: Path, *, namespace: str, principal: str,
                 policy: ArtifactPolicy | None = None, readonly: bool = False):
        self.root = Path(root).absolute()
        self.namespace = _identity(namespace)
        self.principal = _principal(principal)
        self.readonly = readonly
        self.directory = self.root / self.namespace
        if policy is not None and not isinstance(policy, ArtifactPolicy):
            raise ArtifactError("invalid artefact policy")
        self.policy = policy
        with self._locked(write=not readonly, initialise=True) as (directory, index):
            stored = _validate_index(index, self.namespace)
            if policy is not None and policy != stored:
                raise ArtifactError("artefact policy is immutable")
            self.policy = stored

    def _deployment(self) -> Path | None:
        for parent in self.root.parents:
            if (parent / ".restored-audit-only").exists() or (parent / ".restored-audit-only").is_symlink() or (parent / "deployment.sqlite").exists():
                return parent
        return None

    def _writable(self) -> None:
        if self.readonly:
            raise ArtifactError("artefact store is read-only")
        for parent in (self.root, *self.root.parents):
            marker = parent / ".restored-audit-only"
            if marker.exists() or marker.is_symlink():
                raise ArtifactError("restored artefacts are audit-only")
        deployment = self._deployment()
        if deployment is not None:
            from .operations import DeploymentLedger
            if not DeploymentLedger(deployment, readonly=True).admission_enabled:
                raise ArtifactError("restored artefacts are audit-only")

    def _publication_capacity(self) -> None:
        deployment = self._deployment()
        if deployment is not None:
            from .operations import DeploymentLedger
            try:
                DeploymentLedger(deployment, readonly=True).check_storage_admission()
            except OperationsError:
                raise ArtifactError("artefact publication admission closed") from None

    @contextmanager
    def _locked(self, *, write: bool = False, initialise: bool = False):
        try:
            if write:
                self._writable()
            deployment = self._deployment()
            # Cooperate with backup/prune, which hold the deployment lock exclusively.
            lock = _file_lock(deployment / ".deployment.lock", False) if deployment is not None else _nothing()
            with lock:
                if write:
                    self._writable()
                    if initialise and not (self.directory / "index.json").exists():
                        self._publication_capacity()
                root_fd = _open_dir(self.root, create=write and initialise)
                try:
                    if stat.S_IMODE(os.fstat(root_fd).st_mode) & 0o077:
                        raise ArtifactError("artefact root must be private (mode 0700)")
                    if write and initialise:
                        try:
                            os.mkdir(self.namespace, 0o700, dir_fd=root_fd)
                            os.fsync(root_fd)
                        except FileExistsError:
                            pass
                    directory = os.open(self.namespace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=root_fd)
                finally:
                    os.close(root_fd)
                try:
                    if stat.S_IMODE(os.fstat(directory).st_mode) & 0o077:
                        raise ArtifactError("artefact namespace must be private (mode 0700)")
                    # Directory-inode locking needs no writable lock file, so
                    # audit-only inspection leaves restored evidence unchanged.
                    try:
                        fcntl.flock(directory, fcntl.LOCK_EX if write else fcntl.LOCK_SH)
                        try:
                            index = _load_json(_read_file(directory, "index.json", _MAX_INDEX))
                        except FileNotFoundError:
                            if not (write and initialise):
                                raise ArtifactError("artefact store metadata is missing") from None
                            # A missing index with existing data is corruption,
                            # not permission to reset quotas and authorisation.
                            if os.listdir(directory):
                                raise ArtifactError("artefact store metadata is missing") from None
                            index = {"format": _INDEX_FORMAT, "namespace": self.namespace,
                                     "policy": asdict(self.policy or ArtifactPolicy()), "artifacts": {}, "links": {}}
                            _atomic_json(directory, "index.json", index)
                        stored_policy = _validate_index(index, self.namespace)
                        if self.policy is not None and self.policy != stored_policy:
                            raise ArtifactError("artefact policy is immutable")
                        yield directory, index
                    finally:
                        fcntl.flock(directory, fcntl.LOCK_UN)
                finally:
                    os.close(directory)
        except (OSError, OperationsError):
            raise ArtifactError("artefact store is unavailable or unsafe") from None

    def _authorised(self, reference: ArtifactRef | dict, index: dict) -> ArtifactRef:
        ref = reference if isinstance(reference, ArtifactRef) else ArtifactRef.from_dict(reference)
        manifest = index["artifacts"].get(ref.artifact_id)
        if (ref.namespace != self.namespace or manifest is None or manifest["reference"] != ref.to_dict()
                or self.principal not in (ref.producer, *manifest["readers"])):
            raise ArtifactError("artefact is unavailable or access is denied")
        return ref

    def put_bytes(self, data: bytes, *, media_type: str = "application/octet-stream",
                  input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef:
        if type(data) is not bytes:
            raise ArtifactError("artefact content must be bytes")
        if len(data) > self.policy.max_artifact_bytes:
            raise ArtifactError("artefact exceeds per-object quota")
        from io import BytesIO
        return self._put(BytesIO(data), media_type, input_sha256s, readers)

    def put_file(self, path: Path, *, media_type: str = "application/octet-stream",
                 input_sha256s: tuple[str, ...] = (), readers: tuple[str, ...] = ()) -> ArtifactRef:
        path = Path(path).absolute()
        try:
            directory = _open_dir(path.parent)
            try:
                descriptor = _open_file(directory, path.name)
            finally:
                os.close(directory)
            with os.fdopen(descriptor, "rb") as source:
                if os.fstat(source.fileno()).st_size > self.policy.max_artifact_bytes:
                    raise ArtifactError("artefact exceeds per-object quota")
                return self._put(source, media_type, input_sha256s, readers)
        except OSError:
            raise ArtifactError("artefact source is unavailable or unsafe") from None

    def _put(self, source: BinaryIO, media_type: str, inputs: tuple[str, ...], readers: tuple[str, ...]) -> ArtifactRef:
        if type(inputs) is not tuple or type(readers) is not tuple or len(inputs) > 32 or len(readers) > 32:
            raise ArtifactError("invalid artefact provenance or grants")
        for reader in readers:
            _principal(reader)
        inputs = tuple(sorted(set(_digest(item) for item in inputs)))
        grants = sorted(set(readers) - {self.principal})
        # Validate metadata before consuming any source bytes.
        ArtifactRef(self.namespace, "0" * 64, "0" * 64, 0, media_type, self.principal, inputs)
        with self._locked(write=True) as (directory, index):
            # Recognise a duplicate without allocating a temporary blob. New
            # publications obey deployment admission before writing any bytes.
            offset = source.tell()
            size, digest = _hash_source(source, self.policy.max_artifact_bytes)
            source.seek(offset)
            provisional = ArtifactRef(self.namespace, "0" * 64, digest, size, media_type, self.principal, inputs)
            identity = hashlib.sha256(_json({"reference": {key: value for key, value in provisional.to_dict().items() if key != "artifact_id"}, "readers": grants})).hexdigest()
            ref = ArtifactRef(self.namespace, identity, digest, size, media_type, self.principal, inputs)
            if identity in index["artifacts"]:
                self._verify_file(directory, ref)
                return ref
            self._publication_capacity()
            pending = ".pending-" + uuid.uuid4().hex
            try:
                if len(index["artifacts"]) >= self.policy.max_artifacts:
                    raise ArtifactError("artefact count quota exhausted")
                blob = ref.sha256 + ".blob"
                names = os.listdir(directory)
                used = 0
                for name in names:
                    if name.endswith(".blob"):
                        if not _DIGEST.fullmatch(name[:-5]):
                            raise ArtifactError("invalid artefact storage entry")
                        with os.fdopen(_open_file(directory, name), "rb") as stored:
                            used += os.fstat(stored.fileno()).st_size
                    elif name.startswith(".pending-") and name != pending:
                        with os.fdopen(_open_file(directory, name), "rb") as orphan:
                            used += os.fstat(orphan.fileno()).st_size
                    elif name not in ("index.json", pending):
                        raise ArtifactError("invalid artefact storage entry")
                if used + (0 if blob in names else size) > self.policy.max_total_bytes:
                    raise ArtifactError("artefact total-byte quota exhausted")
                if blob in names:
                    self._verify_file(directory, ref)
                else:
                    with os.fdopen(_open_file(directory, pending, os.O_WRONLY | os.O_CREAT | os.O_EXCL), "wb") as output:
                        copied = _hash_source(source, self.policy.max_artifact_bytes, destination=output)
                        if copied != (size, digest):
                            raise ArtifactError("artefact source changed during publication")
                        output.flush()
                        os.fsync(output.fileno())
                    os.replace(pending, blob, src_dir_fd=directory, dst_dir_fd=directory)
                    os.fsync(directory)
                index["artifacts"][identity] = {"artifact_id": identity, "reference": ref.to_dict(), "readers": grants}
                _atomic_json(directory, "index.json", index)
                return ref
            finally:
                try:
                    os.unlink(pending, dir_fd=directory)
                except FileNotFoundError:
                    pass

    @staticmethod
    def _verify_file(directory: int, reference: ArtifactRef, *, collect: bool = False) -> bytes:
        digest = hashlib.sha256()
        result = []
        with os.fdopen(_open_file(directory, reference.sha256 + ".blob"), "rb") as source:
            if os.fstat(source.fileno()).st_size != reference.size:
                raise ArtifactError("artefact content integrity mismatch")
            size = 0
            while chunk := source.read(min(_CHUNK, reference.size - size + 1)):
                size += len(chunk)
                if size > reference.size:
                    raise ArtifactError("artefact content integrity mismatch")
                digest.update(chunk)
                if collect:
                    result.append(chunk)
            if size != reference.size or digest.hexdigest() != reference.sha256:
                raise ArtifactError("artefact content integrity mismatch")
        return b"".join(result)

    def read_bytes(self, reference: ArtifactRef | dict) -> bytes:
        with self._locked() as (directory, index):
            ref = self._authorised(reference, index)
            return self._verify_file(directory, ref, collect=True)

    @contextmanager
    def open(self, reference: ArtifactRef | dict) -> Iterator[BinaryIO]:
        """Return a verified bounded snapshot, never an unverified filesystem handle."""
        from io import BytesIO
        with BytesIO(self.read_bytes(reference)) as stream:
            yield stream

    def verify(self, reference: ArtifactRef | dict) -> bool:
        with self._locked() as (directory, index):
            self._verify_file(directory, self._authorised(reference, index))
            return True

    def inspect(self, reference: ArtifactRef | dict) -> dict:
        with self._locked() as (_, index):
            ref = self._authorised(reference, index)
            manifest = index["artifacts"][ref.artifact_id]
            return {"reference": ref.to_dict(), "readers": list(manifest["readers"]),
                    "links": sorted(key for key, link in index["links"].items() if ref.artifact_id in link["artifacts"])}

    def link(self, link_id: str, references: list[ArtifactRef | dict], *, state: str = "live") -> dict:
        """Publish or transition an owned retention manifest with a frozen ref set.

        Release only after its consumer has reached a resolved terminal state.
        No run/task state is inferred from model output or from an elapsed age.
        """
        _identity(link_id)
        if state not in ("live", "unresolved", "released") or type(references) is not list or not 1 <= len(references) <= 64:
            raise ArtifactError("invalid artefact retention manifest")
        with self._locked(write=True) as (directory, index):
            ids = sorted(set(self._authorised(ref, index).artifact_id for ref in references))
            current = index["links"].get(link_id)
            link = {"owner": self.principal, "state": state, "artifacts": ids}
            if current is not None and (current["owner"] != self.principal or current["artifacts"] != ids):
                raise ArtifactError("artefact retention identity is immutable")
            if current is None and len(index["links"]) >= self.policy.max_links:
                raise ArtifactError("artefact retention quota exhausted")
            index["links"][link_id] = link
            _atomic_json(directory, "index.json", index)
            return {"link_id": link_id, **link}

    def status(self) -> dict:
        """Only authorised references are listed; scope quotas are not capabilities."""
        with self._locked() as (_, index):
            visible = [entry["reference"] for entry in index["artifacts"].values()
                       if self.principal in (entry["reference"]["producer"], *entry["readers"])]
            return {"namespace": self.namespace, "policy": asdict(self.policy),
                    "artifacts": sorted(visible, key=lambda item: item["artifact_id"]),
                    "links": {key: value for key, value in index["links"].items() if value["owner"] == self.principal}}


@contextmanager
def _nothing():
    yield


def has_retained_artifacts(run_dir: Path) -> bool:
    """Read-only retention hook. Malformed evidence blocks run deletion."""
    root = Path(run_dir) / "artifacts"
    if not root.exists() and not root.is_symlink():
        return False
    try:
        directory = _open_dir(root)
        try:
            retained = False
            for namespace in os.listdir(directory):
                _identity(namespace)
                child = os.open(namespace, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory)
                try:
                    index = _load_json(_read_file(child, "index.json", _MAX_INDEX))
                    _validate_index(index, namespace)
                    retained = retained or any(link["state"] != "released" for link in index["links"].values())
                finally:
                    os.close(child)
            return retained
        finally:
            os.close(directory)
    except (OSError, ArtifactError):
        raise OperationsError("artefact retention state cannot be verified") from None
