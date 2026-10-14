#!/usr/bin/env python3
"""Exercise installed release binaries/package using read-only test fixtures.

Run inside the release container with host/tests at /mounted-tests and examples
at /mounted-examples. All writable fixtures live under /tmp. Provider credentials
are removed before running tests; the container should also use --network none.
"""
from __future__ import annotations

import argparse
import json
import importlib.util
import ctypes
import os
from pathlib import Path
import platform
import shutil
import sqlite3
import subprocess
import sys
import tempfile


def runtime_package_identity() -> dict:
    """The deployed runtime must not retain installer code or bootstrap wheels."""
    present = [name for name in ("pip", "ensurepip")
               if importlib.util.find_spec(name) is not None]
    if present:
        raise RuntimeError("Runtime image retains package installers: " + ", ".join(present))
    return {"package_installers": "absent"}


def runtime_identity(binaries: dict[str, Path]) -> dict:
    """Require the actual Alpine release to use its patched shared SQLite."""
    package_identity = runtime_package_identity()
    linkage = {}
    for name, binary in binaries.items():
        result = subprocess.run(["ldd", str(binary)], text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=10, check=False)
        listing = result.stdout
        if (result.returncode or "libsqlite3.so.0" not in listing or "musl" not in listing.lower()
                or "not found" in listing.lower() or "error loading" in listing.lower()):
            raise RuntimeError(f"{name} must link successfully to musl and libsqlite3.so.0")
        linkage[name] = listing.strip().splitlines()
    library = ctypes.CDLL("libsqlite3.so.0")
    library.sqlite3_libversion.argtypes = []
    library.sqlite3_libversion.restype = ctypes.c_char_p
    native_version = library.sqlite3_libversion().decode("ascii")
    if tuple(int(part) for part in native_version.split(".")) < (3, 53, 2):
        raise RuntimeError("the container SQLite runtime must be at least 3.53.2")
    if sqlite3.sqlite_version != native_version:
        raise RuntimeError("Python and the shared native SQLite runtime must have identical versions")
    return {"native_sqlite": native_version, "python_sqlite": sqlite3.sqlite_version,
            "rust_dynamic_linkage": linkage, **package_identity}


def cursor_runtime_identity(binaries: dict[str, Path]) -> dict:
    """Check the optional glibc image and exercise its actual native SDK bridge."""
    package_identity = runtime_package_identity()
    if platform.libc_ver()[0] != "glibc":
        raise RuntimeError("Cursor SDK runtime requires glibc")
    for name, binary in binaries.items():
        result = subprocess.run(["ldd", str(binary)], text=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, timeout=10, check=False)
        listing = result.stdout.lower()
        if not any(marker in listing for marker in ("not a dynamic executable", "statically linked")):
            raise RuntimeError(f"{name} must be a static musl executable in the Cursor image")
    from mas_host.cursor_local import _bridge_files
    node, _ = _bridge_files()
    result = subprocess.run(["ldd", str(node)], text=True, stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, timeout=10, check=False)
    if result.returncode or "not found" in result.stdout.lower() or "libstdc++.so.6" not in result.stdout:
        raise RuntimeError("Cursor's bundled Node runtime has unresolved shared libraries")
    return {"runtime": "cursor-glibc", "rust_linkage": "static-musl-bundled-sqlite",
            "python_sqlite": sqlite3.sqlite_version, **package_identity,
            **cursor_bridge_smoke()}


def cursor_bridge_smoke() -> dict:
    """Launch and close the pinned bridge without creating an agent or API call."""
    from mas_host.cursor_local import CURSOR_SDK_VERSION, _launch_client
    with tempfile.TemporaryDirectory(prefix="mas-cursor-smoke-", dir="/tmp") as directory:
        client = _launch_client(directory)
        bridge = client._owned_bridge
        process = bridge.process
        try:
            if process is None or process.poll() is not None:
                raise RuntimeError("Cursor bridge did not remain running during startup")
            if bridge.endpoint.pid != process.pid:
                raise RuntimeError("Cursor bridge supervision must own Node directly")
            if os.getpgid(process.pid) != os.getpgrp():
                raise RuntimeError("Cursor bridge must remain in the guardian's process group")
            version = bridge.endpoint.server_version
        finally:
            client.close()
        if process.poll() is None:
            raise RuntimeError("Cursor bridge survived client.close()")
        return {"cursor_sdk": CURSOR_SDK_VERSION, "bridge_version": version,
                "bridge_started_and_reaped": True, "provider_calls": 0}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cursor", action="store_true", help="check the optional glibc Cursor image")
    args = parser.parse_args()
    # Smoke checks must not inherit credentials or a user's provider selection.
    for name in list(os.environ):
        if (name.startswith(("OPENAI_", "CURSOR_"))
                or name in ("MAS_PLANNER_PROVIDER", "MAS_REVIEWER_PROVIDER", "MAS_PLANNER_MODEL", "MAS_REVIEWER_MODEL")):
            os.environ.pop(name, None)
    tests = Path("/mounted-tests")
    examples = Path("/mounted-examples")
    if not tests.is_dir() or not examples.is_dir():
        print("Mount host/tests at /mounted-tests and examples at /mounted-examples.", file=sys.stderr)
        return 2
    binaries = {name: Path("/usr/local/bin") / name for name in ("mas", "mas-gateway")}
    if not all(path.is_file() and os.access(path, os.X_OK) for path in binaries.values()):
        print("The installed release binaries are unavailable.", file=sys.stderr)
        return 2
    try:
        identities = cursor_runtime_identity(binaries) if args.cursor else runtime_identity(binaries)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"Release runtime identity check failed: {error}", file=sys.stderr)
        return 2
    # An inherited source path would test the checkout instead of the installed
    # wheel, so remove it from the test subprocess environment.
    environment = dict(os.environ)
    for name in ("PYTHONPATH", "OPENAI_API_KEY", "OPENAI_API_KEY_FILE", "CURSOR_API_KEY", "CURSOR_API_KEY_FILE"):
        environment.pop(name, None)
    environment.update(MAS_BIN=str(binaries["mas"]),
                       MAS_GATEWAY_BIN=str(binaries["mas-gateway"]),
                       MAS_MODE="mock", PYTHONDONTWRITEBYTECODE="1")
    with tempfile.TemporaryDirectory(prefix="mas-release-check-", dir="/tmp") as directory:
        root = Path(directory)
        copied_tests = root / "host" / "tests"
        shutil.copytree(tests, copied_tests, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        shutil.copytree(examples, root / "examples")
        release = root / "rust-protocol" / "target" / "release"
        release.mkdir(parents=True)
        for name, binary in binaries.items():
            (release / name).symlink_to(binary)
        print(json.dumps({"check": "installed-release-container", "uid": os.getuid(),
                          "python": platform.python_version(), "sqlite": sqlite3.sqlite_version,
                          "platform": platform.platform(), "network_required": False,
                          **identities}), flush=True)
        # Isolated mode also ignores Python's user site and the current working
        # directory; unittest explicitly adds only the copied tests directory.
        return subprocess.run([sys.executable, "-I", "-m", "unittest", "discover",
                               "-s", str(copied_tests), "-v"], cwd=root,
                              env=environment, check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
