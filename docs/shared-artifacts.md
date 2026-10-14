# Shared artefact authority

`SharedArtifactStore` implements the principal-bound repository contract using
PostgreSQL metadata and the explicit `S3ArtifactContent` strategy. Controller
directories contain no authoritative index, grants, reservation or retention
links. The shared service migration installs these tables with the other host
repositories; a schema version mismatch fails closed.

The namespace freezes its content endpoint, bucket, prefix, transport policy,
storage identity and quota policy. Credential values and paths are excluded.
Its object key is derived from retained deployment/namespace identity and the
content hash. The producer, exact input digests and initial readers contribute
to the same reference identity as the local repository.

Publication has explicit ordering:

1. An epoch-validated transaction reserves object count and bytes, including
   reservations whose prior upload result is unknown.
2. The trusted content port conditionally writes the immutable object and
   verifies its complete length and SHA-256 through GET. No database lock is
   held during network I/O.
3. A second epoch-validated transaction publishes the manifest. A lost result
   or replacement controller repeats verification against the original key.

A controller fenced during step 2 can leave an orphaned object and charged
reservation, but cannot publish metadata. Concurrent reservations obey the
namespace's aggregate bounds. A digest never grants read authority. Reads
verify a private snapshot and recheck grants before returning it; revocation
does not recall bytes already supplied to an authorised consumer.

Retention links have immutable owner and reference sets. `ensure_link` is for
worker receipt replay: it cannot reactivate an already released link. Explicit
`link` transitions retain the local repository contract. A producer can revoke
an original reader permanently; duplicate publication cannot undo that change.
Unknown upload reservations remain charged. There is no automatic deletion or
quota refund based on elapsed time.

```python
from mas_host.shared_artifacts import SharedArtifactStore

repository = SharedArtifactStore(
    authority, namespace='results', principal=authenticated_principal,
    content_backend=configured_s3_backend, policy=configured_artifact_policy,
)
reference = repository.put_bytes(report, readers=('approved-consumer',))
```

Only the trusted service constructs this binding. Network payloads cannot choose
the database connection, content backend or principal. Backup/replacement must
retain both the shared database and all referenced object versions. Restoring
metadata alone never proves content availability or permission to reactivate
admission. The real-backend suite exercises publication loss, concurrent quotas,
fencing during upload, replacement-cache reads, revocation, tampering and replay
retention. Its S3 transport fault double is separate from the real S3 acceptance
in `scripts/check-object-storage.py`.
