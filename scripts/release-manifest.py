#!/usr/bin/env python3
"""Create checksums and a source identity record for a CI release candidate."""
import hashlib
import json
import os
from pathlib import Path
import re

sha = os.environ["SOURCE_SHA"]
tag = os.environ["RELEASE_TAG"]
if not re.fullmatch(r"[0-9a-f]{40}", sha) or not re.fullmatch(r"v0\.[0-9]+\.[0-9]+(?:-[A-Za-z0-9.-]+)?", tag):
    raise SystemExit("Expected a full commit SHA and a v0.MINOR.PATCH release tag")
files = sorted([*Path(".").glob("mas-*.oci.tar"), Path("release-audit.json"), Path("release-sbom.cdx.json")])
if not any(path.name.endswith(".oci.tar") for path in files):
    raise SystemExit("The OCI archive is missing")
def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
manifest = {
    "format": "mas-release/1", "source_commit": sha, "tag": tag,
    "platform": os.environ["RELEASE_PLATFORM"],
    "files": {str(path): digest(path) for path in files},
    "inputs": {str(path): digest(path) for path in [Path("Dockerfile"), Path("docker/alpine-packages.json"), Path("rust-protocol/Cargo.lock"), *sorted(Path("host").glob("requirements-*.txt"))]},
    "provenance": "BuildKit provenance is embedded in the OCI archive; this JSON is an unsigned source and checksum record.",
}
Path("release-manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
files.append(Path("release-manifest.json"))
Path("SHA256SUMS").write_text("".join(f"{digest(path)}  {path}\n" for path in files))
