"""Dependency-neutral host interfaces and values shared by execution strategies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Protocol, runtime_checkable


class RunnerError(RuntimeError):
    """A fixed diagnostic safe to show without disclosing provider details."""


class AdapterError(RunnerError):
    """An adapter failed; inspect the durable task and invocation records."""


class RecoveryRequired(AdapterError):
    """An invocation may have affected the outside world; operator action required."""


class AdapterTimeout(AdapterError):
    """Local execution exceeded its inherited deadline."""


class AdapterInterrupted(AdapterError):
    """Host shutdown interrupted local execution."""


@dataclass(frozen=True)
class InvocationTicket:
    call_id: str
    cached_output: dict | None
    execute: bool
    step_key: str


@dataclass(frozen=True)
class TaskRequest:
    sender: str
    recipient: str
    operation: str
    input: dict
    task_id: str
    message_id: str


@dataclass(frozen=True)
class Outcome:
    output: dict
    status: str = "completed"


@dataclass(frozen=True)
class Invocation:
    output: dict
    evidence: dict


@runtime_checkable
class AdapterContext(Protocol):
    """Capabilities available to adapters under every execution strategy."""

    @property
    def actor(self) -> str: ...

    @property
    def invocation_id(self) -> str | None: ...

    @property
    def remaining_seconds(self) -> float: ...

    def request(self, recipient: str, operation: str, input: dict) -> Outcome: ...

    def invoke(self, call: Callable[[], Invocation], *, input: dict | None = None,
               reservation_microusd: int | None = None) -> dict: ...


class Adapter(Protocol):
    def handle(self, context: AdapterContext, request: TaskRequest) -> Outcome: ...


class HostContext(AdapterContext, Protocol):
    """Additional admission/checkpoint operations owned by the host process."""

    @property
    def ownership_descriptors(self) -> tuple[int, ...]:
        """Locks retained by the guardian until all adapter execution stops."""
        ...

    def prepare_invocation(self, *, input: dict | None = None,
                           reservation_microusd: int | None = None) -> InvocationTicket: ...

    def complete_invocation(self, ticket: InvocationTicket, invocation: Invocation) -> dict: ...

    def fail_invocation(self, ticket: InvocationTicket) -> None: ...


class AdapterExecutor(Protocol):
    """Execute an adapter without changing its request or invocation contract."""

    def execute(self, adapter: Adapter, context: HostContext, request: TaskRequest, *,
                deadline: float, tick: Callable[[], None] | None = None) -> Outcome: ...
