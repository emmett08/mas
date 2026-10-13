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
files = sorted([*Path(".").glob("mas-*.oci.tar"), Path("release-audit.json"),
                Path("release-sbom.cdx.json"), Path("release-image-identity.json"),
                Path("release-image-binding.json"), Path("release-import-tool.txt"),
                Path("release-container-tests.log")])
if not any(path.name.endswith(".oci.tar") for path in files):
    raise SystemExit("The OCI archive is missing")
def digest(path):
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()
binding = json.loads(Path("release-image-binding.json").read_text())
if (binding.get("format") != "mas-release-image-binding/1"
        or binding.get("docker_identity_verified") is not True
        or binding.get("archive_sha256") != "sha256:" + digest(Path(binding["archive"]))
        or f"{binding.get('os')}/{binding.get('architecture')}" != os.environ["RELEASE_PLATFORM"]):
    raise SystemExit("Release archive and tested image do not have a verified identity binding")
layout = Path(binding.get("scanner_directory", ""))
if (binding.get("scanner_layout_verified") is not True
        or binding.get("scanner_input") != str(layout) + "@" + binding["manifest_digest"]
        or not binding.get("scanner_layout_files")
        or any("sha256:" + digest(layout / name) != identity
               for name, identity in binding["scanner_layout_files"].items())):
    raise SystemExit("The scanned OCI layout does not match the verified release archive")
manifest = {
    "format": "mas-release/1", "source_commit": sha, "tag": tag,
    "platform": os.environ["RELEASE_PLATFORM"],
    "files": {str(path): digest(path) for path in files},
    "inputs": {str(path): digest(path) for path in [Path("Dockerfile"), Path("docker/alpine-packages.json"), Path("rust-protocol/Cargo.lock"), *sorted(Path("host").glob("requirements-*.txt"))]},
    "provenance": "BuildKit provenance is embedded in the OCI archive; this JSON is an unsigned source and checksum record.",
    "tested_image_binding": binding,
}
Path("release-manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
files.append(Path("release-manifest.json"))
Path("SHA256SUMS").write_text("".join(f"{digest(path)}  {path}\n" for path in files))
