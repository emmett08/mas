# Artefact exchange

`mas_host.artifacts` supplies immutable, bounded local file exchange for any
workflow. Messages carry `ArtifactRef.to_dict()` values; bulk content remains in
the run's private state directory. Each reference contains its namespace,
content hash, byte count, media type, producer and input hashes. A second hash
binds those fields and the publication's read grants into an immutable identity.
References are limited to 8,192 JSON bytes, below the Rust protocol's complete
message limit. Applications must still budget for the surrounding message.

```python
from pathlib import Path
from mas_host.artifacts import ArtifactPolicy, ArtifactStore

# run_dir is the existing admitted Rust run directory. The host obtains actor
# identities from its authenticated configuration, not from message payloads.
store = ArtifactStore(
    Path(run_dir) / "artifacts",
    namespace="analysis-v1",
    principal="analyst",
    policy=ArtifactPolicy(
        max_artifact_bytes=8 * 1024 * 1024,
        max_total_bytes=64 * 1024 * 1024,
        max_artifacts=1024,
        max_links=4096,
    ),
)
reference = store.put_bytes(
    b"derived result\n",
    media_type="text/plain",
    input_sha256s=(input_digest,),
    readers=("checker",),
)
store.link("check-task-17", [reference], state="live")
message_payload = {"result": reference.to_dict()}
```

The recipient opens the **same local root and namespace**, under its own trusted
principal, then calls `read_bytes(reference)` or `open(reference)`. Both verify
the complete bounded content before returning any bytes. `open` returns an
in-memory snapshot; it does not stream unverified bytes to a consumer.
`verify(reference)` checks integrity without retaining the content in memory.
`put_file(path)` streams a regular source file under the per-object limit;
symlinks, hard links and special files are rejected. No method fetches a URL.

`inspect(reference)` returns authorised metadata and associated link identities.
`status()` lists references readable by the principal, its own retention links
and the namespace's fixed policy. Publication of identical content and metadata
is idempotent, including after a restart. Different media types, input hashes or
read grants produce different reference identities. Identical bytes are stored
once within a namespace. There is no cross-namespace content lookup.

## Trust and scope

`principal` is an identity supplied by the trusted host. Qualified actor names
such as `coordinate/worker` and `review/checker` are preserved exactly in
producer identities, grants and retention ownership. They are opaque metadata;
namespace and retention-link identifiers remain path-safe components. The constructor is not
an authentication mechanism for remote callers, and the module does not isolate
malicious Python code running as the same operating-system user. Remote bridges
must resolve authenticated callers into configured principals before giving
access to the repository. Knowing a content hash or possessing a reference does
not grant access.

The producer always equals the publishing principal. Producers and explicitly
listed readers may read the object. Read grants are frozen; publishing another
reference with additional readers leaves earlier references unchanged. There is
no grant-revocation API in this version. Sensitive information therefore needs
an appropriate retention policy and protection of the private run directory.
Input hashes identify declared dependencies. The store verifies stored content;
it does not independently attest that the producer actually used those inputs.

A namespace is a locally configured scope with immutable quotas. Create its
configuration through trusted application code; allocating arbitrary new
namespaces is not a quota mechanism. Limits apply to object size, total physical
content bytes, manifest count and retention-link count. Crash-orphaned blobs and
pending files remain charged against the byte quota. One bounded pending object
may temporarily exist during publication. Metadata has a separate 16 MiB bound.
Publication is serialised across processes and atomically installs durable
content before its manifest. Unknown versions, inconsistent metadata and
corrupt content fail closed.

Within a deployment, new namespaces and publications also require its storage
admission thresholds to remain open. An identical retained publication is
verified and returned without allocating temporary content, even when admission
is closed. Reads and retention-state updates remain available so existing work
can be resolved. These are admission thresholds, not a physical byte quota;
existing writes still require free space. A file is fingerprinted before admission
and checked again while copying, so a changed source cannot publish mismatched
content.

## Retention and recovery

Place the root at `<run_dir>/artifacts`. Existing deployment backup includes the
objects and manifests. Repository operations cooperate with the deployment
lock, so backup and run pruning cannot overlap publication or reads. Restored
stores require `readonly=True`; publication and retention changes are rejected
in audit-only restored deployments.

Explicit retention links protect results used beyond the producing run's
lifetime. Create a link before handing a reference to a downstream consumer.
Keep it `live` while the consumer is running or `unresolved` when its effects
cannot be determined. Only its creating principal can update it, and its set of
references is fixed. Release it when the owning application has established the
consumer's resolved terminal state:

```python
store.link("check-task-17", [reference], state="released")
```

Ordinary `prune_runs` retains a run containing any live or unresolved link and
blocks deletion when artefact retention metadata cannot be verified. Releasing
a link does not delete an object. Objects disappear only when the containing
run becomes eligible for explicit operational retention. Unlinked objects do
not extend a terminal run's lifetime. Applications remain responsible for
establishing and releasing links; the store does not infer lifecycle decisions
from model responses or elapsed time. Composition history also retains its
referenced run directories through the composition-store retention guard.

Standalone roots can be useful in embedding applications, but they do not
receive deployment backup or run-retention management automatically. The
`ArtifactRepository` protocol permits alternative storage implementations;
this release provides only the local backend. Distributed consumers require a
separately authenticated transfer or an appropriate future backend, with the
same hash, provenance, authorisation and retention guarantees.
