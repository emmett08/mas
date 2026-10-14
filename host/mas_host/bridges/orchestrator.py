"""Framework-neutral orchestration over the durable task-service port.

The outer orchestrator owns sequencing. MAS owns admission, execution and
reconciliation. A polling deadline does not cancel work or change its state.
"""
from __future__ import annotations

import hashlib
import math
import time
from typing import Callable

from ..codec import integer_json
from ..service_contracts import ServiceError, TaskLifecycle

STATES = frozenset({"queued", "running", "blocked", "input_required", "auth_required",
                    "completed", "failed", "cancelled", "unknown"})
WAIT_STATES = frozenset({"queued", "running"})


def stable_request_key(*parts: str) -> str:
    """Include execution and logical operation identity, never retry attempt."""
    if not parts or any(type(part) is not str or not 1 <= len(part) <= 512 for part in parts):
        raise ServiceError("invalid_orchestration_identity")
    return "orch-" + hashlib.sha256(integer_json(list(parts)).encode()).hexdigest()


def receipt(value: dict, task_id: str | None = None) -> dict:
    if (type(value) is not dict or value.get("state") not in STATES
            or type(value.get("task_id")) is not str or not value["task_id"]
            or (task_id is not None and value["task_id"] != task_id)):
        raise ServiceError("invalid_task_receipt")
    integer_json(value, 131072)
    return value


def bounded_seconds(value: float, maximum: int = 3600) -> float:
    if (type(value) not in (int, float) or not math.isfinite(value)
            or value < 0 or value > maximum):
        raise ServiceError("invalid_poll_deadline")
    return float(value)


class OrchestratorConnector:
    """One authenticated principal with narrow, injectable service capabilities."""
    def __init__(self, service: TaskLifecycle, principal: str, *,
                 clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        if type(principal) is not str or not principal:
            raise ServiceError("invalid_principal")
        self.service, self.principal = service, principal
        self._clock, self._sleep = clock, sleep

    def submit(self, request_key: str, workflow: str, input: dict) -> dict:
        return receipt(self.service.submit(self.principal, request_key, workflow, input))

    def inspect(self, task_id: str) -> dict:
        return receipt(self.service.inspect(self.principal, task_id), task_id)

    def result(self, task_id: str) -> dict:
        """Retrieve only a completed payload; inspect retains failure receipts."""
        current = self.inspect(task_id)
        if current["state"] != "completed":
            raise ServiceError("result_unavailable")
        result = self.service.result(self.principal, task_id)
        if type(result) is not dict:
            raise ServiceError("invalid_task_result")
        integer_json(result, 131072)
        return result

    def cancel(self, task_id: str) -> dict:
        """Return observed state: cancellation intent is not stopped execution."""
        return receipt(self.service.cancel(self.principal, task_id), task_id)

    def update(self, task_id: str, request_key: str, input: dict) -> dict:
        return receipt(self.service.update(self.principal, task_id, request_key, input), task_id)

    def wait(self, task_id: str, *, timeout_seconds: float = 30,
             poll_seconds: float = 1, heartbeat: Callable[[dict], None] | None = None,
             cancelled: Callable[[], bool] | None = None) -> dict:
        timeout = bounded_seconds(timeout_seconds)
        interval = bounded_seconds(poll_seconds, 60)
        if interval <= 0:
            raise ServiceError("invalid_poll_interval")
        deadline = self._clock() + timeout
        while True:
            current = self.inspect(task_id)
            if heartbeat is not None:
                heartbeat({"task_id": task_id, "state": current["state"]})
            if cancelled is not None and cancelled():
                return self.cancel(task_id)
            if current["state"] not in WAIT_STATES or self._clock() >= deadline:
                return current
            self._sleep(min(interval, max(0, deadline - self._clock())))
