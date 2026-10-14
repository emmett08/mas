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
the complete bounded content before returning any bytes. `open` returns a private, disk-backed verified snapshot; it does not buffer the
whole object in memory or expose unverified bytes to a consumer.
`verify(reference)` checks integrity without retaining the content in memory.
`put_stream(stream)` accepts a trusted seekable binary source with two-pass
hashing. `put_file(path)` streams a regular source file under the per-object limit;
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
the committed backend remains local. Distributed consumers can use the
authenticated bounded transfer below. This is not an S3-compatible object-store
backend or a replicated metadata authority.


## Authenticated cross-host transfer

`ArtifactTransferService` exposes opt-in chunk operations through authenticated
`POST /service/artifact/<operation>` and MCP `mas.artifact.<operation>` tools.
Operations are `upload_begin`, `upload_chunk`, `upload_commit`, `download_begin`,
`download_chunk`, `status`, `close` and `revoke`. Each binary chunk is at most
32 KiB, base64 encoded inside the existing bounded integer-JSON envelope. The
service never accepts a filesystem path or fetches a message-supplied URL.

Enable fixed repositories in the trusted bridge configuration:

```json
{
  "artifacts": {
    "ttl_seconds": 3600,
    "max_sessions": 16,
    "max_staging_bytes": 67108864,
    "scopes": {
      "analysis-results": {
        "namespace": "analysis-v1",
        "principals": {"alice": "analyst", "bob": "checker"},
        "policy": {
          "max_artifact_bytes": 16777216,
          "max_total_bytes": 67108864,
          "max_artifacts": 1024,
          "max_links": 4096
        }
      }
    }
  }
}
```

This is the `artifacts` field of the existing version-1 bridge configuration.
Each scope maps authenticated service principals to distinct local actor
identities. Aliasing two principals to one actor is rejected. The optional
scope `path` is relative to the deployment state directory; its default is
`shared-artifacts/<scope>`. The scope name, actor map, local quotas and paths are
operator configuration, never request authority. A scope rooted in a run's
`artifacts` directory participates in that run's retention; the default shared
root is included in deployment backup and storage admission but needs explicit
operator lifetime management. Upload publication does not infer a downstream
consumer's lifecycle or create a run-retention link on that consumer's behalf.

Upload and revocation operations require `mas:submit`; download, status and
close require `mas:read`. The authenticated principal must also appear in the
selected scope. Upload producers come from that mapping. The request's readers
are names in the same configured mapping, not self-asserted actor identities.
Download checks the exact stored reference and its existing grants. A content
hash, copied reference or guessed transfer identifier grants no access.

```python
from mas_host.bridges.artifact_client import ArtifactTransferClient
from mas_host.bridges.service_client import TaskServiceClient

peer = TaskServiceClient(
    "https://peer.example", principal="bob", token_file=private_token_file)
transfer = ArtifactTransferClient.from_service(peer)
# local_repository is the consumer's configured ArtifactStore.
receipt = transfer.materialize("analysis-results", remote_reference, local_repository)
local_reference = receipt["reference"]
```

`from_mcp(configured_mcp_client)` provides the same transfer operations. A2A
application data parts, MCP structured results, Temporal payloads and LangGraph
state can carry the unchanged `ArtifactRef.to_dict()` value. Their configured
consumer calls `materialize` explicitly through the selected HTTP/MCP peer.
There is no additional A2A RPC extension or automatic file-URL download.

Materialisation streams bounded chunks into a private temporary file and checks
the complete size and SHA-256 before publication. The local producer is the
local repository principal; the source content hash is an input dependency.
A second durable artefact records the exact source reference and configured
peer descriptor. A live retention link protects both local objects. Release
`receipt["link_id"]` with both `receipt["reference"]` and `receipt["receipt"]`
only after the consuming work is resolved terminal. Peer credentials and proxy
credentials never enter this provenance receipt.

`publish(scope, repository, reference, readers=(...))` uploads a verified local
object and returns the remote reference. Upload bytes remain unusable until an
exact-size/hash commit. Repeated identical chunks at the same offset and exact
commits are idempotent. Changed bytes conflict. The client does not retry failed
requests. If a chunk or commit reply is lost, `ArtifactTransferIncomplete`
contains `transfer_id`; retain it and call `inspect_transfer(transfer_id)`.
The owner can then repeat the exact chunk/commit rather than allocate another
upload. A lost initial begin reply leaves a bounded expiring reservation.

Sessions persist across service restarts and are bound to their original
principal and scope configuration. Changes to the scope invalidate old
sessions. Download sessions contain verified private snapshots. TTL expires
access; the next transfer operation cleans expired bytes. Staging reserves the
whole expected size before accepting content, with a separate session-count
bound. A private verification snapshot and local publication temporary can
add bounded transient disk use beyond the staging reservation; these limits are
not a filesystem quota. The client checks an overall transfer deadline between
requests, each of which also has its transport's hard deadline. Automatic
cross-session download resume is not implemented; explicit same-session offset
reads are available until expiry.

Only the mapped producer can `revoke` future remote downloads for selected
readers. Revocation persists across restarts and applies to existing download
sessions as well as new ones. It does not rewrite immutable local manifests,
recall bytes already delivered or revoke a recipient's local copy. Revocation
records are bounded to 4,096 distinct grants and are included in backup.
Transfers, publication and revocation are disabled in audit-only restored
state; authorised local read-only inspection remains available.

## Scientific values without wire JSON floats

`mas_host.numeric_artifacts.put_float64` stores finite IEEE 754 binary64 arrays
as `application/vnd.mas.ndarray` artefacts. The `MASARRAY1` encoding contains a
bounded versioned header with dtype, little-endian byte order and integer shape,
followed by exactly the declared number of 8-byte values. The hash binds the
header and values together; mismatched dimensions and nonfinite values fail.
The protocol message still contains only the ordinary integer-JSON reference.

```python
from mas_host.numeric_artifacts import put_float64, open_float64

reference = put_float64(repository, [0.125, 3.141592653589793], shape=(2,))
with open_float64(repository, reference) as (shape, values):
    for value in values:
        consume(value)
```

This encoding preserves binary64 values; it does not claim arbitrary decimal
precision, units or coordinate semantics. Applications needing another encoding
can use a separately versioned media type under the same integrity and access
rules. Inside a MAS workflow, transfers belong in an admitted adapter invocation
with a frozen implementation and bounded transfer policy.
