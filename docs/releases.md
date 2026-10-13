# Releases and upgrades

The container build fixes Rust 1.85.1, Cargo.lock, both base-image manifests,
Alpine package archives, Python build tools and the complete OpenAI SDK dependency set. Python downloads
accept only the committed wheel hashes. The application wheel is built without
network access; the final image installs from the local wheel directory with
hash verification and no network. Build inputs are repeatable. Bit-for-bit image
reproducibility has not been established: build timestamps and the CI/build
infrastructure can still differ.

## Supported targets and lock maintenance

The container targets are CPython 3.12 on Alpine Linux amd64 and arm64. The runtime lock
contains musllinux hashes for both architectures, alongside glibc wheels for the
native Ubuntu test job. The audit-tool lock targets the amd64 CI
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
`docker buildx imagetools inspect rust:1.85.1-alpine` and
`docker buildx imagetools inspect python:3.12-alpine`, then review both
architecture manifests and rerun image scanning. A fixed digest deliberately
does not receive security updates until changed. GitHub actions are pinned to
commits and must also be updated through review.

The production image uses Alpine 3.24, musl 1.2.6 and packaged SQLite 3.53.4.
Rust links both libc and SQLite dynamically, so the runtime package inventory
includes those libraries and receives their security advisories. The container
sets `RUSTFLAGS=-C target-feature=-crt-static` and
`LIBSQLITE3_SYS_USE_PKG_CONFIG=1`; the native Cargo build uses its bundled SQLite
dependency. The Rust builder retains its pinned compiler and matching Alpine
3.21 development libraries; the deployed library is the newer runtime package.
CI checks actual dynamic linkage, SQLite version and the complete host test suite
inside the installed container with networking disabled.

`docker/alpine-packages.json` fixes archive versions and SHA256 hashes separately
for builder/runtime and amd64/arm64. Downloads verify hashes; offline `apk`
installation also verifies package signatures. Refresh explicitly with
`python scripts/release-apk.py --lock docker/alpine-packages.json --refresh`.
Review package versions and dependencies, retain the corresponding base-image
series, and rerun all image/runtime gates. The package list is explicit: a new
dependency causes the offline build to fail until its archive is reviewed and
added. An archive disappearing upstream requires a reviewed refresh or an
independently retained copy with the same hash.

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
2. Restore the backup into a separate staging state directory. Restored state
   remains audit-only: use candidate inspection, reconciliation and cached
   completed-result replay to check it. Run the mocked acceptance harness in a
   separate, fresh state directory. Never invoke a provider for an unresolved
   call merely to test recovery. Review schema/version errors before upgrading
   production.
3. Run bounded live acceptance with a dedicated provider project, spending
   limits and an operator. Record actual provider/model identity and usage.
   The automated workflows never invoke the paid API.
4. Start the candidate with the production state volume and independent
   reconciliation watcher. Inspect budgets, disk headroom, expiries and errors.
   Keep the pre-upgrade backup until the acceptance window has closed.
5. If rollback is needed, stop the candidate first. Preserve its current state
   and restore the pre-upgrade backup into a separate directory for audit.
   Restore does not enable new admissions or undo remote effects. Reconcile
   calls made since the backup before making any retry decision, and retain
   both state copies. An older binary must never modify a database upgraded to
   a newer schema or reactivate a restored snapshot. A cutover that needs new
   work must use a separately authorised and budgeted fresh deployment, with
   the previous deployment's reserved expenditure and unresolved effects
   included in the operator's account-level accounting. Restoring a backup
   never replenishes the deployment's spending allowance.

The supplied Compose resource caps limit each container's CPU, memory, PID count
and log rotation. The named volume still requires host disk monitoring, backup
storage and operator-controlled retention. `compose.production.yaml` runs the
watcher independently; use `up -d reconciler`, then `run --rm mesh` for jobs.
Avoid `--abort-on-container-exit` for the watcher deployment because jobs finish
normally while reconciliation must continue.

These controls follow [pip's secure-install guidance](https://pip.pypa.io/en/stable/topics/secure-installs/)
and [Docker's Compose service controls](https://docs.docker.com/reference/compose-file/services/).
