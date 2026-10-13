#!/usr/bin/env python3
"""Create an immutable candidate tag after CI, or verify an existing tag."""
from __future__ import annotations

import json
import os
from pathlib import Path
import re
import urllib.error
import urllib.request


class ApiError(RuntimeError):
    def __init__(self, status: int):
        super().__init__(f"GitHub API request failed with HTTP {status}")
        self.status = status


class GitHub:
    def __init__(self, repository: str, token: str):
        if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repository):
            raise ValueError("invalid repository identity")
        self.base = f"https://api.github.com/repos/{repository}"
        self.token = token

    def request(self, method: str, path: str, data: dict | None = None) -> dict:
        request = urllib.request.Request(
            self.base + path,
            data=json.dumps(data).encode("utf-8") if data is not None else None,
            method=method,
            headers={"Authorization": f"Bearer {self.token}",
                     "Accept": "application/vnd.github+json",
                     "Content-Type": "application/json",
                     "X-GitHub-Api-Version": "2022-11-28"},
        )
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            # API bodies and request headers never enter logs.
            raise ApiError(error.code) from None


def candidate_version(ref: str) -> tuple[str, bool]:
    """Return the version and whether this is an authorised creation branch."""
    branch_prefix, tag_prefix = "refs/heads/release/", "refs/tags/"
    if ref.startswith(branch_prefix):
        version, may_create = ref[len(branch_prefix):], True
    elif ref.startswith(tag_prefix):
        version, may_create = ref[len(tag_prefix):], False
    else:
        raise ValueError("select a release/v0.MINOR.PATCH branch or an existing version tag")
    if not re.fullmatch(r"v0\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", version):
        raise ValueError("candidate versions must have the form v0.MINOR.PATCH")
    return version, may_create


def tag_commit(api: GitHub, version: str) -> str | None:
    try:
        reference = api.request("GET", f"/git/ref/tags/{version}")
    except ApiError as error:
        if error.status == 404:
            return None
        raise
    target = reference["object"]
    # Support annotated tags without trusting an unbounded reference chain.
    for _ in range(8):
        if target["type"] == "commit":
            return target["sha"]
        if target["type"] != "tag" or not re.fullmatch(r"[0-9a-f]{40}", target["sha"]):
            raise ValueError("candidate tag does not resolve to a commit")
        target = api.request("GET", f"/git/tags/{target['sha']}")["object"]
    raise ValueError("candidate annotated-tag chain is too deep")


def prepare_candidate(api: GitHub, ref: str, source: str) -> tuple[str, str]:
    version, may_create = candidate_version(ref)
    if not re.fullmatch(r"[0-9a-f]{40}", source):
        raise ValueError("candidate source must be a full commit SHA")
    if api.request("GET", f"/git/commits/{source}").get("sha") != source:
        raise ValueError("candidate source commit identity does not match")
    existing = tag_commit(api, version)
    if existing is None:
        if not may_create:
            raise ValueError("manual or tag execution requires an existing candidate tag")
        try:
            api.request("POST", "/git/refs", {"ref": f"refs/tags/{version}", "sha": source})
        except ApiError as error:
            if error.status != 422:
                raise
            # Another gated run may have created it concurrently. Resolve and
            # compare below; never update or delete an existing tag.
        existing = tag_commit(api, version)
    if existing != source:
        raise ValueError("candidate tag already names a different commit, or creation did not complete")
    return version, source


def main() -> None:
    try:
        api = GitHub(os.environ["GITHUB_REPOSITORY"], os.environ["GH_TOKEN"])
        version, source = prepare_candidate(api, os.environ["RELEASE_REF"], os.environ["SOURCE_SHA"])
        with Path(os.environ["GITHUB_OUTPUT"]).open("a", encoding="utf-8") as output:
            output.write(f"version={version}\nsource={source}\n")
        print(f"Candidate {version} identifies source commit {source}.")
    except (ValueError, ApiError) as error:
        raise SystemExit(f"Candidate tag refused: {error}. No existing tag was changed.") from None
    except (KeyError, RuntimeError, OSError):
        raise SystemExit("Candidate tag validation or creation failed; no existing tag was changed.") from None


if __name__ == "__main__":
    main()
