# Releases and upgrades

The container build fixes Rust 1.85.1, Cargo.lock, both base-image manifests, the
Python build tools and the complete OpenAI SDK dependency set. Python downloads
accept only the committed wheel hashes. The application wheel is built without
network access; the final image installs from the local wheel directory with
hash verification and no network. Build inputs are repeatable. Bit-for-bit image
reproducibility has not been established: build timestamps and the CI/build
infrastructure can still differ.

## Supported targets and lock maintenance

The container targets are CPython 3.12 on Linux amd64 and arm64. The runtime lock
contains hashes for both architectures. The audit-tool lock targets the amd64 CI
runner. The dependency-free embedded Python interface may work elsewhere, but
those platforms are outside these release checks.

To change dependencies, use a temporary CPython 3.12 virtual environment:

```sh
python3.12 -m venv .venv-release
. .venv-release/bin/activate
python -m pip install --require-hashes -r host/requirements-build.txt
# Update the explicit SDK/build versions in host/pyproject.toml first if needed.
python scripts/release-locks.py
python scripts/release-locks.py --check
```

The refresh command resolves wheel-only dependencies separately for both target
architectures and refuses conflicting versions. Review every version/hash
change, run all CI gates, and commit the lock updates together. Security-tool
versions are explicit in the refresh script and security workflow. Do not merge
a dependency-update PR until its regenerated locks pass the same checks.

Base-image tags identify the upstream release series; their digests identify the
actual downloaded manifests. Refresh them from Docker Hub using
`docker buildx imagetools inspect rust:1.85.1-bookworm` and
`docker buildx imagetools inspect python:3.12-slim-bookworm`, then review both
architecture manifests and rerun image scanning. A fixed digest deliberately
does not receive security updates until changed. GitHub actions are pinned to
commits and must also be updated through review.

## Candidate artefacts

Creating a `v0.MINOR.PATCH` tag triggers the release workflow. Manual dispatch
must select an existing tag. Protocol/SPIN, host/Compose/acceptance and security
jobs all gate packaging. Native amd64 and arm64 jobs produce an OCI archive,
embedded BuildKit provenance, a CycloneDX SBOM, audit results, a source manifest
and `SHA256SUMS`. The manifest is an unsigned identity record; it is not an
independent attestation of correctness. Action artefacts expire after 30 days,
so the maintainer must retain approved releases and checksums in durable release
storage. The workflow does not publish a package or push an image registry tag.

Before distribution, download the intended architecture's artefacts and run
`sha256sum -c SHA256SUMS`. Import or copy the OCI archive with a tool supporting
the OCI layout, retaining its digest and provenance. Record the tested image
digest in the deployment configuration. A release tag alone cannot establish
that an application's live-provider acceptance has passed.

## Upgrade and rollback

1. Stop admission of new jobs and inspect open tasks and unknown provider
   outcomes. Quiesce host runners before making a backup with the supported
   administration command. Keep the old image digest and configuration.
2. Restore the backup into a separate staging state directory. Never let staging
   invoke a provider for an unresolved call merely to test recovery. Run the
   mocked acceptance harness and inspection/reconciliation commands against the
   candidate. Review schema/version errors before upgrading production.
3. Run bounded live acceptance with a dedicated provider project, spending
   limits and an operator. Record actual provider/model identity and usage.
   The automated workflows never invoke the paid API.
4. Start the candidate with the production state volume and independent
   reconciliation watcher. Inspect budgets, disk headroom, expiries and errors.
   Keep the pre-upgrade backup until the acceptance window has closed.
5. If rollback is needed, stop the candidate first. Restore the pre-upgrade
   backup into a fresh directory and run the previous image against it. Do not
   run an older binary against a database that a newer schema has modified.
   Restoring old state does not undo remote effects: reconcile calls made since
   the backup before admitting retries. Preserve both state copies for review.

The supplied Compose resource caps limit each container's CPU, memory, PID count
and log rotation. The named volume still requires host disk monitoring, backup
storage and operator-controlled retention. `compose.production.yaml` runs the
watcher independently; use `up -d reconciler`, then `run --rm mesh` for jobs.
Avoid `--abort-on-container-exit` for the watcher deployment because jobs finish
normally while reconciliation must continue.

These controls follow [pip's secure-install guidance](https://pip.pypa.io/en/stable/topics/secure-installs/)
and [Docker's Compose service controls](https://docs.docker.com/reference/compose-file/services/).
