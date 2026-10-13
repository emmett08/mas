#!/usr/bin/env python3
"""Refresh wheel-only dependency locks for the supported Linux container targets.

Run with CPython 3.12. Resolution is deliberately an explicit maintenance action;
normal builds only consume the committed, hash-checked result.
"""
from __future__ import annotations

import argparse
import email
import hashlib
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[1]
PLATFORMS = ("manylinux2014_x86_64", "manylinux2014_aarch64", "musllinux_1_1_x86_64", "musllinux_1_1_aarch64")


def normalise(name: str) -> str:
    return re.sub(r"[-_.]+", "-", name).lower()


def collect(directory: Path) -> dict[str, tuple[str, set[str]]]:
    packages: dict[str, tuple[str, set[str]]] = {}
    for path in sorted(directory.rglob("*.whl")):
        with zipfile.ZipFile(path) as wheel:
            metadata = email.message_from_bytes(wheel.read(next(
                name for name in wheel.namelist() if name.endswith(".dist-info/METADATA"))))
        name, version = normalise(metadata["Name"]), metadata["Version"]
        previous, hashes = packages.setdefault(name, (version, set()))
        if previous != version:
            raise ValueError(f"Platform versions disagree for {name}: {previous}, {version}")
        hashes.add(hashlib.sha256(path.read_bytes()).hexdigest())
    return packages


def refresh(filename: str, requirements: list[str], platforms: tuple[str, ...]) -> None:
    with tempfile.TemporaryDirectory(prefix="mas-lock-") as temporary:
        directory = Path(temporary)
        for platform in platforms:
            subprocess.run([
                sys.executable, "-m", "pip", "download", "--only-binary=:all:",
                "--platform", platform, "--python-version", "3.12", "--implementation", "cp",
                "--abi", "cp312", "--dest", str(directory / platform), *requirements,
            ], check=True)
        packages = collect(directory)
    scope = "CPython 3.12 Linux amd64/arm64, glibc/musl" if len(platforms) > 1 else "CPython 3.12 Linux amd64 (CI tooling only)"
    lines = [f"# Generated from downloaded wheels; {scope}.",
             "# Refresh: python scripts/release-locks.py; review changes before committing.",
             "--only-binary=:all:", ""]
    for name, (version, hashes) in sorted(packages.items()):
        lines.append(f"{name}=={version} \\\n" + " \\\n".join(f"    --hash=sha256:{digest}" for digest in sorted(hashes)))
    (ROOT / "host" / filename).write_text("\n".join(lines) + "\n")


def check() -> None:
    metadata = tomllib.loads((ROOT / "host/pyproject.toml").read_text())
    for filename in ("requirements-openai.txt", "requirements-build.txt", "requirements-security.txt"):
        contents = (ROOT / "host" / filename).read_text()
        if "--only-binary=:all:" not in contents:
            raise ValueError(f"{filename}: source distributions are forbidden")
        for line in contents.replace("\\\n", " ").splitlines():
            line = line.strip()
            if not line or line.startswith(("#", "--only-binary")):
                continue
            if not re.fullmatch(r"[a-z0-9-]+==[a-zA-Z0-9.+]+(?:\s+--hash=sha256:[0-9a-f]{64})+", line):
                raise ValueError(f"Invalid locked requirement in {filename}: {line}")
    runtime = (ROOT / "host/requirements-openai.txt").read_text()
    build = (ROOT / "host/requirements-build.txt").read_text()
    for dependency in metadata["project"]["optional-dependencies"]["openai"]:
        if dependency + " " not in runtime:
            raise ValueError(f"SDK pin mismatch: {dependency}")
    for dependency in metadata["build-system"]["requires"]:
        if dependency + " " not in build:
            raise ValueError(f"Build pin mismatch: {dependency}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="check lock structure and direct pins without network")
    args = parser.parse_args()
    if not args.check:
        if sys.version_info[:2] != (3, 12):
            parser.error("refresh requires CPython 3.12")
        metadata = tomllib.loads((ROOT / "host/pyproject.toml").read_text())
        refresh("requirements-openai.txt", metadata["project"]["optional-dependencies"]["openai"], PLATFORMS)
        refresh("requirements-build.txt", [*metadata["build-system"]["requires"], "pip==26.2.1", "wheel==0.48.0"], PLATFORMS)
        refresh("requirements-security.txt", ["pip-audit==2.10.1"], PLATFORMS[:1])
    check()
    print("Dependency lock structure and direct pins verified.")


if __name__ == "__main__":
    main()
