# Security and maintenance

The supported deployment is a single Linux host with trusted Python adapters,
the Rust capability gateway and a private SQLite state volume. Python adapters
execute application code with the host account's authority. Process supervision
controls duration; it does not isolate hostile code. Use separate operating-system
sandboxes before admitting adapters from another trust domain.

Supported container targets are CPython 3.12 on Alpine Linux amd64 and arm64. The
current development line receives fixes. A tagged candidate becomes a supported
release only after its CI, deployment acceptance and application-specific live
acceptance have passed. A successful scan describes the advisory databases and
artefact scanned at that time.

## Reporting a vulnerability

Use the repository's **Security → Report a vulnerability** facility when private
reporting is enabled. If it is unavailable, open an issue requesting a private
contact channel without including exploit details, credentials or private run
records. Include affected commit/version, deployment configuration and a minimal
reproduction through the private channel when available. No response-time SLA is
currently offered.

## Release gates

- Committed Cargo and Python locks identify dependency versions; Python installs
  require wheel hashes. Container base images use manifest digests. Alpine
  package archives use fixed versions/hashes and signed offline installation.
  Container libc and SQLite are dynamically linked, inventoried runtime packages.
- CI audits all locked Python runtime/build/scanner dependencies and Cargo.lock.
  Image scanning fails on HIGH or CRITICAL advisories, including unfixed ones.
- Scheduled weekly scanning checks for advisories discovered after a commit.
  Dependabot proposes dependency, container and action updates for review.
- No vulnerability ignore list or fail-open audit step is supplied. When a gate
  fails, update the dependency/base or record a reviewed, narrowly scoped,
  time-limited exception before proposing a release. Never make a gate pass by
  silently discarding findings.

Run state includes private messages, gateway capabilities and provider evidence.
Restrict access to state and backups. Provider API keys remain outside protocol
messages. Do not upload state directories as CI evidence or issue attachments.
See [release and upgrade procedures](https://github.com/emmett08/mas-reference-host/blob/main/docs/releases.md) and the operations guide
for backup, restore, retention and uncertain external outcomes.
