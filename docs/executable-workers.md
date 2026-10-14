# Registered executable workers

The opt-in `RegisteredProcessExecutor` launches each adapter in a fresh executable
process. A Rust guardian owns its deadline, process group and descendant cleanup.
Python clients may call it from multiple threads because the host neither forks
its interpreter nor changes signal handlers. The existing `LinuxProcessExecutor`
remains available for trusted adapters that depend on inherited Python objects.

Both strategies use the same parent-owned context protocol for delegation,
invocation admission, checkpointing and uncertain effects. Application code calls
`context.invoke` before external effects. Model output and task input never choose
an executable, import path, factory registration or resource allowance.

## Configure a worker

Build the Rust binaries and install `mas-host`. This example uses the installed
Python environment and the built-in echo factory:

```python
from pathlib import Path
import sys
from mas_host import Runner
from mas_host.executable_worker import (
    RegisteredProcessExecutor, WorkerLimits, WorkerRegistry, WorkerSpec,
)

spec = WorkerSpec(
    "echo", "1", (sys.executable, "-m", "mas_host.worker_entry"),
    factory="echo", configuration={}, cwd=Path.cwd(), environment={},
    limits=WorkerLimits(memory_bytes=512 * 1024 * 1024,
                        cpu_seconds=60, file_bytes=16 * 1024 * 1024),
)
registry = WorkerRegistry([spec])
topology = {
    "agents": ["client", "worker"],
    "edges": [["client", "worker"], ["worker", "client"]],
    "application": "agent-task/1", "mailbox_capacity": 10,
    "max_messages": 20, "max_bytes": 65536, "max_calls": 4,
    "wall_seconds": 60,
}
with Runner(topology, {actor: registry.adapter("echo")
                      for actor in topology["agents"]}, Path("private-state"),
            executor=RegisteredProcessExecutor(),
            protocol_strategy="persistent") as runner:
    print(runner.request("client", "worker", "echo", {"text": "hello"}).output)
```

For a source checkout, register `PYTHONPATH` explicitly in `environment`.
Environment variables are not inherited automatically. Credentials required by
an approved provider must be passed explicitly and remain absent from worker
descriptors, IPC frames and diagnostics. Command arguments and `configuration`
are public configuration.

The built-in `structured` factory accepts `actor`, `agent`, `contract` and
`mock_output`. Its contract contains `name`, `version`, `input_schema`,
`output_schema` and `allowed_callers`; `agent` uses the ordinary agent-configuration
format. Set `mock_output` to null for a live provider. Supply that provider's
credentials and dependencies to the worker environment. Live-provider acceptance
is separate from deterministic mock acceptance.

## Register an application executable

A custom Python executable registers its factories explicitly:

```python
from mas_host import Invocation, Outcome
from mas_host.worker_entry import run_worker

class ApplicationAdapter:
    def __init__(self, configuration):
        self.configuration = configuration

    def handle(self, context, request):
        return Outcome(context.invoke(
            lambda: Invocation({"received": request.input}, {"provider": "example"}),
            input=request.input, reservation_microusd=0))

if __name__ == "__main__":
    raise SystemExit(run_worker({"application": ApplicationAdapter}))
```

Register its absolute interpreter/script command, `factory="application"`, public
configuration and explicit implementation version in `WorkerSpec`. Other
languages can implement the same bounded context protocol. The startup frame is
integer JSON with version `mas-worker/1`, registered factory, actor, configuration,
complete task request and remaining milliseconds. Subsequent frames use the
shared adapter context protocol. Frames are capped at 65,536 bytes including the
newline; oversized combined configuration/request input fails before launch.

The executable's SHA-256 is checked at registration and before dispatch. Use
`pinned_files={absolute_path: sha256}` to pin scripts and other dependencies.
Unlisted interpreter packages remain deployment dependencies: freeze the package
environment or container image and change the implementation version with its
behaviour. Virtual-environment interpreter paths retain their invocation path.

`WorkerSpec.with_workspace(...)` copies an approved registration into a workspace
while retaining its private environment and existing dependency pins. A wrapper
can pass extra ownership-lock descriptors to the executor; the guardian retains
them until its worker descendants have stopped.

## Lifetime and trust limits

The guardian runs through `mas-gateway supervise`. It retains host run/workspace
locks, closes them in the worker at exec, and passes only the private context
socket. Host termination closes a separate lifetime pipe. The guardian then kills
and reaps its process group before releasing ownership. Cleanup also runs after
successful results, worker crashes, deadline expiry and SIGTERM/SIGINT/SIGHUP.
Terminating local work does not cancel or disprove a remote provider effect.

Linux `RLIMIT_AS`, `RLIMIT_CPU` and `RLIMIT_FSIZE` enforce address-space, CPU-time
and file-size limits; core dumps are disabled. These are per-process limits,
not aggregate cgroup limits. Workers retain the host account's filesystem and
network permissions and must remain in the guardian-owned process group.
Malicious code escaping that group, a compromised guardian, or guardian SIGKILL
is outside this trusted-process profile. Untrusted code requires a separately
tested container or operating-system sandbox; this strategy advertises no such
isolation.

The executor confirms cleanup before recording a pending invocation as unknown.
If cleanup cannot be confirmed, it reports a failure and retains the guardian's
ownership authority instead of killing it and releasing locks while descendants
may remain alive.

Tests cover built-in/application factories, admitted calls, threaded clients,
environment and descriptor restrictions, dependency changes, enforced resource
limits, successful-exit descendant cleanup, deadline expiry, guardian termination
and host death. They do not establish live SDK acceptance, cross-platform support
or untrusted-code containment.
