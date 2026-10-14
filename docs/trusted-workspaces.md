# Trusted workspaces, tools and context snapshots

`mas_host.workspaces` integrates approved input artefacts and explicitly
registered tools with the Rust executable-worker supervisor. The supported
profiles are **trusted-code-execution** and **trusted-repository-analysis**.
Existing restricted text-only inference remains available separately.

These profiles run trusted application code as the host's operating-system
user. They are **not sandboxes**. They do not confine filesystem syscalls or
network access, and cannot safely execute arbitrary model-generated code.
`isolated-untrusted-code` and other unqualified profile names are rejected.

The current qualification environment denied both user/network namespace
creation and the corresponding bubblewrap setup. An isolated profile requires
a separately implemented and tested deployment backend. It must enforce
filesystem, network, device and process access as well as resource limits before
being advertised for untrusted workloads.

## Authority and resource boundaries

| Boundary | Current enforcement |
|---|---|
| Tool selection | Trusted startup registers `WorkspaceTool` objects. `WorkspaceTools.resolve()` checks the immutable policy allow-list. Request data cannot select a command, import or executable. |
| External effects | A tool declaring external effects requires explicit `allow_external_effects=True`. Each complete tool process is admitted through `context.invoke`; child requests cannot create nested calls or delegation inside that invocation. |
| Inputs | A principal-bound `ArtifactStore` authenticates every reference before materialisation. Exact immutable manifests, contents and file sets are checked before and after execution. |
| Workspace ownership | Principal, logical run and attempt determine a hashed private directory. Host file operations use directory descriptors and reject traversal, symlinks, hardlinked files and non-regular objects. |
| Read-only snapshots | Input files are created with mode `0400`. Trusted workers must respect this contract; the shared OS user can bypass permissions. Mutation is detected and publication fails. |
| Process resources | The Rust supervisor enforces the registered memory, CPU and per-file limits and reaps owned descendants. |
| Aggregate storage | Input file/byte limits are enforced before materialisation. Total workspace file/byte limits and output limits are checked after execution and before publication. These are not hard filesystem quotas during execution. |
| Environment/network | WorkerSpec passes only its explicit environment. Network restriction remains a trusted-tool contract, not an OS containment claim. |
| Cleanup | An explicit resolution artefact and an exclusive workspace lease are required. The Rust guardian retains that lease until descendants are reaped. |

A principal supplied to `WorkspaceManager` must come from the host's
authenticated session. Workspace references are identifiers, not bearer
capabilities. All managers and registrations are trusted host configuration.

## Prepare and execute an approved tool

An application worker follows `mas_host.worker_entry.run_worker`, with a fixed
factory allow-list. Its factory receives trusted configuration and uses the
workspace's `inputs`, `work` and `outputs` directories. The executable-worker
protocol is required; an arbitrary CLI cannot be substituted directly.

```python
from hashlib import sha256
from pathlib import Path
import sys

from mas_host.artifacts import ArtifactStore
from mas_host.executable_worker import RegisteredProcessExecutor, WorkerLimits, WorkerSpec
from mas_host.workspaces import (
    WorkspaceAdapter, WorkspaceExecutor, WorkspaceInput, WorkspaceManager,
    WorkspaceOutput, WorkspacePolicy, WorkspaceTool, WorkspaceTools,
)

store = ArtifactStore(Path("/srv/mas/artefacts"), namespace="project", principal=principal)
manager = WorkspaceManager(Path("/srv/mas/workspaces"), principal=principal, artifacts=store)
policy = WorkspacePolicy("approved-tests", "1", ("test-runner",))

script = Path("/srv/mas/application/test_worker.py")
worker = WorkerSpec(
    "test-runner", "1", (sys.executable, str(script)),
    factory="approved-tests", configuration={}, cwd=script.parent,
    environment={"PYTHONPATH": "/srv/mas/application:/srv/mas/host",
                 "PYTHONDONTWRITEBYTECODE": "1"},
    limits=WorkerLimits(memory_bytes=512 * 1024 * 1024, cpu_seconds=120),
    pinned_files={str(script): sha256(script.read_bytes()).hexdigest()},
)
tools = WorkspaceTools((WorkspaceTool("test-runner", worker, reservation_microusd=1000),))
reference = manager.prepare(
    logical_run_id, attempt_id, policy,
    (WorkspaceInput("source.py", approved_source_reference),),
)
adapter = WorkspaceAdapter(
    manager, reference, policy, tools.resolve("test-runner", policy),
    (WorkspaceOutput("test-report.json", "application/json"),
     WorkspaceOutput("changes.patch", "text/plain")),
)
executor = WorkspaceExecutor(RegisteredProcessExecutor("/srv/mas/bin/mas-gateway"))
```

Register `adapter` with `Runner` or a trusted workflow adapter factory and pass
`executor` to that runner. The workspace is bound to one attempt and one admitted
tool execution. Use a new attempt identity for a distinct execution; completed
calls resume through the existing journal checkpoints. Reusing a non-pristine
workspace cannot authorise another execution.

`WorkerSpec.with_workspace()` preserves the original environment and pinned
dependencies while binding the private working directory. The executable,
explicit script/dependency pins and each materialised input are checked before
dispatch. Interpreted code and dependencies must be included in the deployment's
pins or immutable image identity; pinning an interpreter alone does not freeze
all imported packages.

The worker's trusted configuration receives a `workspace` object containing its
identity and fixed relative directory names. Input directories must stay
unchanged; put caches, temporary files and other writable state in `work`.
Publishable results must be written beneath `outputs` at the registered output
paths. Task input does not extend that path allow-list.

The adapter returns `result`, immutable `artifacts`, and the workspace identity.
Each published artefact includes source/context SHA-256 provenance. Its admitted
invocation evidence also records worker and tool identity. Publication remains
subject to the artefact store's own quotas. A crash during multi-output
publication may retain an immutable partial set; the invocation remains unknown
until reconciled, and automatic execution is not repeated.

## Exact durable context

`mas_host.context_snapshots` selects an ordered prefix of complete UTF-8 text
artefacts under item, byte and token limits:

```python
from mas_host.context_snapshots import ContextPolicy, TokenCounter, build_context

counter = TokenCounter("approved-model-tokeniser", "pinned-v1", count_model_tokens)
context = build_context(
    store, ordered_context_references,
    policy=ContextPolicy(max_items=12, max_bytes=24000, max_tokens=6000),
    tokeniser=counter,
)
reference = manager.prepare(logical_run_id, attempt_id, policy, inputs, context=context)
```

The token-counting callable is explicitly supplied by trusted startup code. It
must match the selected model and be versioned when its behaviour changes. Its
budget covers selected context text, including separators; reserve additional
space for instructions, tool schemas and generated output separately.

Selection stops at the first item that exceeds a bound. It neither silently
summarises nor partially truncates a document. The published `mas-context/1`
snapshot contains exact text, selected references, omitted reference identities,
the omission reason, measured byte/token counts and the selection/tokeniser
versions. Recovery reads that snapshot, preserving the original input. Changing
the selection policy or tokeniser identity produces a different artefact.

Context references and workspace inputs/outputs receive live retention links.
Release a shared context's retention link only when its other consumers have
also finished; workspace cleanup does not infer that relationship.

## Resolution and retention

After the application has reconciled every associated effect, retain a
resolution receipt as an artefact and call:

```python
manager.release(reference, resolution=resolution_reference)
manager.cleanup(reference)
```

The application owns the interpretation and authorisation of that receipt.
Worker exit, apparent success and elapsed time alone do not prove that a remote
effect resolved. Release closes the workspace's artefact-retention links;
cleanup takes an exclusive lease before removing local files. A small retired
identity record prevents the same principal/run/attempt from being recreated
after deletion. Cleanup never follows links to their targets.

Current tests cover admitted execution, durable replay without another process,
content and script drift, authorised context reads, principal separation,
path traversal, symlinks and hardlinks, mutation of source snapshots, allow-list
and effect-policy rejection, and retention/quiescence rules. A deployment still
needs its W17 live-provider, capacity and recovery qualification. Untrusted code
is unsupported until a containment backend passes its adversarial tests.
