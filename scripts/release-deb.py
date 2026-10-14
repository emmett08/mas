#!/usr/bin/env python3
"""Fetch the hash-locked C++ libraries needed by Cursor's bundled Node runtime.

Refresh explicitly from Debian's trixie package index; image builds consume the
committed identities and install offline. No apt resolver runs during a build.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import lzma
from pathlib import Path
import urllib.request

ARCHES = ("amd64", "arm64")
PACKAGES = ("gcc-14-base", "libgcc-s1", "libstdc++6")
ORIGIN = "https://deb.debian.org/debian/"


def download(url: str) -> bytes:
    if not url.startswith(ORIGIN):
        raise ValueError("Only official Debian package URLs are accepted")
    with urllib.request.urlopen(url, timeout=60) as response:
        return response.read()


def refresh(arch: str) -> list[dict]:
    index = lzma.decompress(download(ORIGIN + f"dists/trixie/main/binary-{arch}/Packages.xz")).decode()
    rows = {}
    for block in index.split("\n\n"):
        row = dict(line.split(": ", 1) for line in block.splitlines()
                   if ": " in line and not line.startswith(" "))
        if row.get("Package") in PACKAGES:
            rows[row["Package"]] = row
    return [{"name": name, "version": rows[name]["Version"],
             "filename": Path(rows[name]["Filename"]).name,
             "url": ORIGIN + rows[name]["Filename"],
             "sha256": rows[name]["SHA256"], "dependencies": rows[name].get("Depends", "")}
            for name in PACKAGES]


def fetch(lock: dict, arch: str, directory: Path) -> None:
    if lock.get("format") != "mas-deb-lock/1":
        raise ValueError("Unsupported package lock")
    packages = lock["runtime"][arch]
    if {package["name"] for package in packages} != set(PACKAGES) or len(packages) != len(PACKAGES):
        raise ValueError("Unexpected Cursor runtime package set")
    directory.mkdir(parents=True, exist_ok=True)
    for package in packages:
        filename = package["filename"]
        if Path(filename).name != filename or not filename.endswith(f"_{arch}.deb"):
            raise ValueError("Invalid package filename")
        data = download(package["url"])
        if hashlib.sha256(data).hexdigest() != package["sha256"]:
            raise ValueError(f"Package hash mismatch: {filename}")
        (directory / filename).write_bytes(data)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lock", type=Path, required=True)
    parser.add_argument("--arch", choices=ARCHES)
    parser.add_argument("--out", type=Path)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()
    if args.refresh:
        lock = {"format": "mas-deb-lock/1", "runtime": {arch: refresh(arch) for arch in ARCHES}}
        args.lock.parent.mkdir(parents=True, exist_ok=True)
        args.lock.write_text(json.dumps(lock, indent=2, sort_keys=True) + "\n")
        return
    if not args.arch or not args.out:
        parser.error("download requires --arch and --out")
    fetch(json.loads(args.lock.read_text()), args.arch, args.out)


if __name__ == "__main__":
    main()
