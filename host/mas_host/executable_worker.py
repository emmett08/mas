"""Registered exec workers with Rust-owned Linux process lifetime.

Workers are trusted applications with explicit executable, environment and
resource limits. They are not OS sandboxes. Request data cannot select code.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import sys
import time
from types import MappingProxyType
from typing import Mapping

from .codec import canonical_json, integer_json
from .contracts import AdapterError, AdapterTimeout, RunnerError
from .supervision import AdapterProtocolSession, _Channel, MAX_FRAME


@dataclass(frozen=True)
class WorkerLimits:
    memory_bytes: int = 512 * 1024 * 1024
    cpu_seconds: int = 120
    file_bytes: int = 64 * 1024 * 1024

    def __post_init__(self):
        for name, lower, upper in (("memory_bytes", 64*1024*1024, 1024**4),
                                   ("cpu_seconds", 1, 86_400), ("file_bytes", 1024, 1024**4)):
            value = getattr(self, name)
            if type(value) is not int or not lower <= value <= upper:
                raise RunnerError("invalid registered worker resource limit")


def _file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True, init=False)
class WorkerSpec:
    """Immutable registration selected by host code before any request.

    The executable is content-checked before each dispatch. Scripts, packages
    and libraries used by an interpreter remain deployment-owned dependencies;
    their changes require a new implementation version or image identity.
    Environment values can contain secrets and never enter the descriptor.
    """
    _source: str
    _environment: object = field(repr=False)

    def __init__(self, name: str, version: str, command: tuple[str, ...], *,
                 factory: str, configuration: dict, cwd: Path,
                 environment: Mapping[str, str] | None = None,
                 limits: WorkerLimits = WorkerLimits(),
                 pinned_files: Mapping[str, str] | None = None):
        valid = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
        if any(type(value) is not str or valid.fullmatch(value) is None for value in (name, version, factory)):
            raise RunnerError("invalid registered worker identity")
        if (type(command) is not tuple or not 1 <= len(command) <= 32
                or any(type(value) is not str or not value or "\0" in value or len(value) > 4096 for value in command)
                or type(configuration) is not dict or not isinstance(limits, WorkerLimits)):
            raise RunnerError("invalid registered worker command")
        try:
            executable = Path(command[0]).resolve(strict=True)
            directory = Path(cwd).resolve(strict=True)
            if not Path(command[0]).is_absolute() or not executable.is_file() or not os.access(executable, os.X_OK) or not directory.is_dir():
                raise ValueError
            digest = _file_digest(executable)
            supplied = {} if environment is None else dict(environment)
            if (len(supplied) > 64 or any(type(key) is not str or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", key) is None
                    or type(value) is not str or "\0" in value or len(value) > 65_536
                    for key, value in supplied.items()) or "MAS_WORKER_CHANNEL_FD" in supplied):
                raise ValueError
            # Credentials are supplied through this explicit process environment,
            # never IPC configuration, logs or descriptor text.
            descriptor = {"format": "mas-executable-worker/1", "name": name, "version": version,
                          # Preserve the registered invocation path: resolving
                          # a venv Python symlink changes its package environment.
                          "command": [str(Path(command[0])), *command[1:]], "executable_sha256": digest,
                          "factory": factory, "configuration": configuration, "cwd": str(directory),
                          "environment_names": sorted(supplied), "limits": asdict(limits),
                          "trust_profile": "trusted-linux-process"}
            pinned = {} if pinned_files is None else dict(pinned_files)
            if len(pinned) > 128:
                raise ValueError
            for path, expected in pinned.items():
                if (type(path) is not str or not Path(path).is_absolute()
                        or type(expected) is not str or re.fullmatch(r"[0-9a-f]{64}", expected) is None
                        or _file_digest(path) != expected):
                    raise ValueError
            if pinned:
                descriptor["pinned_files"] = pinned
            source = integer_json(descriptor, 32_768)
        except (OSError, ValueError, TypeError):
            raise RunnerError("invalid registered worker configuration") from None
        object.__setattr__(self, "_source", source)
        object.__setattr__(self, "_environment", MappingProxyType(supplied))

    @property
    def descriptor(self):
        return json.loads(self._source)

    @property
    def identity(self):
        return hashlib.sha256(canonical_json(self.descriptor).encode()).hexdigest()

    def verify(self):
        try:
            actual = _file_digest(self.descriptor["command"][0])
            if actual != self.descriptor["executable_sha256"]:
                raise ValueError
            for path, expected in self.descriptor.get("pinned_files", {}).items():
                if _file_digest(path) != expected:
                    raise ValueError
        except (OSError, ValueError):
            raise RunnerError("registered worker executable changed") from None

    def with_workspace(self, cwd: Path, configuration: dict, *, pinned_files=None):
        """Copy a registration into a host-approved attempt workspace.

        The environment remains private and inherited pins cannot be replaced.
        The caller owns workspace approval and context/effect identity binding.
        """
        descriptor = self.descriptor
        pins = dict(descriptor.get("pinned_files", {}))
        for path, digest in ({} if pinned_files is None else dict(pinned_files)).items():
            if path in pins and pins[path] != digest:
                raise RunnerError("workspace cannot replace registered dependency identity")
            pins[path] = digest
        self.verify()
        return WorkerSpec(descriptor["name"], descriptor["version"], tuple(descriptor["command"]),
            factory=descriptor["factory"], configuration=configuration, cwd=cwd,
            environment=self._environment, limits=WorkerLimits(**descriptor["limits"]), pinned_files=pins)


class WorkerRegistry:
    def __init__(self, registrations):
        selected = {}
        for spec in registrations:
            if not isinstance(spec, WorkerSpec):
                raise RunnerError("invalid worker registration")
            name = spec.descriptor["name"]
            if name in selected or len(selected) >= 128:
                raise RunnerError("duplicate or excessive worker registrations")
            selected[name] = spec
        self._registrations = MappingProxyType(selected)

    def adapter(self, name: str):
        try:
            return ExecutableAdapter(self._registrations[name])
        except (KeyError, TypeError):
            raise RunnerError("worker is not registered") from None


@dataclass(frozen=True)
class ExecutableAdapter:
    spec: WorkerSpec

    def descriptor(self):
        return self.spec.descriptor

    def handle(self, _context, _request):
        raise RunnerError("executable adapter requires RegisteredProcessExecutor")


class WorkerCleanupError(RunnerError):
    """The guardian has not confirmed that its owned descendants were reaped."""


class RegisteredProcessExecutor:
    """No fork or signal-handler mutation in Python; usable from host threads."""
    def __init__(self, gateway_bin="mas-gateway"):
        self.gateway = shutil.which(str(gateway_bin))
        if self.gateway is None or not sys.platform.startswith("linux"):
            raise RunnerError("registered process execution requires Linux and mas-gateway")

    def execute(self, adapter, context, request, *, deadline: float, tick=None,
                ownership_descriptors: tuple[int, ...] = ()):
        if not isinstance(adapter, ExecutableAdapter) or not isinstance(adapter.spec, WorkerSpec):
            raise RunnerError("executor requires an explicitly registered worker")
        if type(deadline) not in (int, float) or not math.isfinite(deadline):
            raise RunnerError("invalid worker deadline")
        remaining = deadline - time.monotonic()
        if not 0 < remaining <= 86_400:
            raise AdapterTimeout("worker deadline is expired or exceeds one day")
        spec = adapter.spec
        spec.verify()
        descriptor = spec.descriptor
        frame = {"version": "mas-worker/1", "factory": descriptor["factory"],
                 "actor": context.actor, "configuration": descriptor["configuration"],
                 "request": asdict(request), "remaining_ms": max(1, int(remaining * 1000))}
        # Reject the complete frame before launching any worker.
        integer_json(frame, MAX_FRAME - 1)
        parent_sock, child_sock = socket.socketpair()
        lifetime, owner = os.pipe()
        process = None
        session = AdapterProtocolSession(context)
        try:
            if type(ownership_descriptors) is not tuple:
                raise RunnerError("invalid worker ownership descriptors")
            locks = tuple(getattr(context, "ownership_descriptors", ())) + ownership_descriptors
            if (len(locks) > 32 or any(type(fd) is not int or fd <= 2 for fd in locks)
                    or len(set(locks)) != len(locks)):
                raise RunnerError("invalid worker ownership descriptors")
            limits = descriptor["limits"]
            command = [self.gateway, "supervise", str(lifetime), str(child_sock.fileno()),
                       str(int((time.time() + remaining) * 1000)), str(limits["memory_bytes"]),
                       str(limits["cpu_seconds"]), str(limits["file_bytes"]),
                       ",".join(map(str, locks)) or "-", "--", *descriptor["command"]]
            process = subprocess.Popen(command, env=dict(spec._environment), cwd=descriptor["cwd"],
                stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                pass_fds=(lifetime, child_sock.fileno(), *locks), close_fds=True,
                start_new_session=True)
            child_sock.close()
            os.close(lifetime)
            lifetime = None
            channel = _Channel(parent_sock, deadline)
            frame["remaining_ms"] = max(1, int((deadline - time.monotonic()) * 1000))
            channel.send(frame)
            return session.serve(channel, tick=tick)
        finally:
            # Closing the lifetime endpoint asks Rust to kill and reap the whole
            # owned worker group before releasing retained ownership locks.
            os.close(owner)
            if lifetime is not None:
                os.close(lifetime)
            child_sock.close()
            parent_sock.close()
            if process is not None:
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    # Do not kill the guardian and release its ownership locks
                    # while descendants may still be alive. It retains cleanup
                    # authority and the caller must investigate the stuck host.
                    raise WorkerCleanupError("worker guardian cleanup has not completed") from None
                if process.returncode != 0:
                    raise WorkerCleanupError("worker guardian failed; inspect retained run")
            session.finish_pending()
