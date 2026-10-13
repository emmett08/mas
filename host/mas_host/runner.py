"""Synchronous, host-owned adapters over the Rust MAS admission boundary.

Adapters are trusted Python code, not an isolation boundary. Model text is data.
The runner never resumes a previous run or automatically retries an invocation.
An interrupted reservation needs external reconciliation before any replay.
"""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
from typing import Callable, Mapping, Protocol
import uuid


class RunnerError(RuntimeError):
    """A fixed diagnostic safe to show without disclosing provider details."""


class AdapterError(RunnerError):
    """An adapter failed; inspect the durable task and invocation records."""


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


class Adapter(Protocol):
    def handle(self, context: Context, request: TaskRequest) -> Outcome:
        """Handle one request, optionally delegating through its context."""
        ...


def _json(value: object, limit: int = 65_536) -> str:
    """Reject Python coercions before handing JSON to the authoritative codec."""
    def check(item: object, depth: int) -> None:
        if depth > 32:
            raise RunnerError("JSON nesting limit exceeded")
        if item is None or type(item) in (str, bool):
            return
        if type(item) is int:
            if len(str(abs(item))) > 1024:
                raise RunnerError("JSON integer limit exceeded")
            return
        if type(item) is list:
            for child in item:
                check(child, depth + 1)
            return
        if type(item) is dict:
            for key, child in item.items():
                if type(key) is not str:
                    raise RunnerError("JSON object keys must be strings")
                check(child, depth + 1)
            return
        raise RunnerError("protocol data must use integer-only JSON")

    try:
        check(value, 0)
        source = json.dumps(value, ensure_ascii=False, allow_nan=False,
                            separators=(",", ":"))
        if len(source.encode("utf-8")) > limit:
            raise RunnerError("JSON byte limit exceeded")
        return source
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise RunnerError("invalid protocol JSON") from None


class Context:
    """Actor-scoped operations valid only while this request is executing.

    ``invoke`` reserves a call before entering a provider. The callback must set
    its own execution timeout. Evidence is caller-supplied public metadata:
    never put API keys, credentials, sensitive prompts or raw exceptions there.
    """

    def __init__(self, runner: Runner, actor: str, request_id: str):
        self._runner = runner
        self._actor = actor
        self._request_id = request_id
        self._thread = threading.get_ident()

    @property
    def actor(self) -> str:
        return self._actor

    def _check(self) -> None:
        runner = self._runner
        if (runner._closed or runner._invoking
                or self._thread != threading.get_ident()
                or not runner._active
                or runner._active[-1] != self._request_id):
            raise RunnerError("adapter context is not active")

    def request(self, recipient: str, operation: str, input: dict) -> Outcome:
        self._check()
        return self._runner._exchange(self.actor, recipient, operation, input)

    def invoke(self, call: Callable[[], Invocation]) -> dict:
        self._check()
        runner = self._runner
        call_id = "call-" + uuid.uuid4().hex
        runner._gateway(self.actor, command="reserve", call_id=call_id)
        try:
            runner._invoking = True
            try:
                invocation = call()
            finally:
                runner._invoking = False
            if not isinstance(invocation, Invocation):
                raise AdapterError("adapter invocation returned an invalid result")
            if type(invocation.output) is not dict or type(invocation.evidence) is not dict:
                raise AdapterError("adapter invocation requires object output and evidence")
            # The same Rust codec validates provider data before it can be
            # exposed to another adapter or recorded as successful evidence.
            runner._wire(self.actor, self.actor, "Notice", {
                "output": invocation.output, "evidence": invocation.evidence,
            }, "validation-" + uuid.uuid4().hex)
            runner._gateway(self.actor, command="finish", call_id=call_id,
                            evidence=invocation.evidence)
            return json.loads(_json(invocation.output))
        except Exception:
            # Failure may follow an actual remote effect. Do not claim that the
            # provider did nothing and do not replay this reservation.
            try:
                runner._gateway(self.actor, command="finish", call_id=call_id,
                                evidence={"outcome": "unknown", "error": "adapter_invocation_failed"})
            except RunnerError:
                pass  # A durable uncompleted reservation is also honest evidence.
            raise AdapterError("adapter invocation failed; outcome may be unknown") from None


class Runner:
    """Run any frozen directed topology using synchronous Python adapters.

    Each instance owns a fresh private run directory. Every actor operation uses
    a provisioned capability and the Rust gateway. The CLI is used only for
    administrative initialisation/status and authoritative wire encoding.

    Execution is serial: nested delegation can revisit an actor with a new task,
    but the active outer request is never dispatched a second time. ``max_depth``
    bounds delegation cycles. A Result means an adapter's claim, not consensus.
    """

    def __init__(self, topology: dict, adapters: Mapping[str, Adapter],
                 state_dir: Path, mas_bin: str = "mas",
                 gateway_bin: str = "mas-gateway", max_depth: int = 8,
                 command_timeout: float = 10):
        if type(max_depth) is not int or not 1 <= max_depth <= 64:
            raise RunnerError("max_depth must be an integer from 1 to 64")
        if not isinstance(command_timeout, (int, float)) or not 0 < command_timeout <= 120:
            raise RunnerError("command timeout must be positive and at most 120 seconds")
        self._mas = shutil.which(str(mas_bin))
        self._gateway_bin = shutil.which(str(gateway_bin))
        if not self._mas or not self._gateway_bin:
            raise RunnerError("mas and mas-gateway binaries must be installed")
        self._timeout = command_timeout
        self._max_depth = max_depth
        self._adapters = dict(adapters)
        self._active: list[str] = []
        self._invoking = False
        self._closed = False
        self._request_lock = threading.Lock()
        self.events: list[dict] = []
        self.run_id = "run-" + uuid.uuid4().hex
        try:
            state_dir = Path(state_dir).resolve()
            state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            self.run_dir = Path(tempfile.mkdtemp(prefix=self.run_id + "-", dir=state_dir))
            self.run_dir.chmod(0o700)
            self._db = self.run_dir / "state.sqlite"
            self._config = self.run_dir / "gateway.json"
            spec = self.run_dir / "topology.json"
            spec.write_text(_json(topology), encoding="utf-8")
            snapshot = self._command([self._mas, "init", str(self._db), self.run_id, str(spec)])
            config = snapshot["config"]
            if set(config["agents"]) != set(self._adapters):
                raise RunnerError("register exactly one adapter for every topology agent")
            if any(not callable(getattr(adapter, "handle", None)) for adapter in self._adapters.values()):
                raise RunnerError("every adapter must implement handle")
            # Finite metering survives the process-per-command gateway. This
            # overhead allowance is derived from frozen message/call budgets.
            attempts = min(2**63 - 1, 100 + 6 * config["max_messages"] + 3 * config["max_calls"])
            policy = {
                "gateway_id": "host-" + uuid.uuid4().hex[:27],
                "max_attempts": attempts,
                "credentials": [
                    {"credential_id": f"agent-{index}", "run_id": self.run_id,
                     "actor": actor, "max_attempts": attempts}
                    for index, actor in enumerate(config["agents"])
                ],
            }
            policy_path = self.run_dir / "policy.json"
            policy_path.write_text(_json(policy), encoding="utf-8")
            self._command([self._gateway_bin, "provision", str(policy_path), str(self._config)])
            private = json.loads(self._config.read_text(encoding="utf-8"))
            self._tokens = {item["actor"]: item["token"] for item in private["credentials"]}
        except RunnerError:
            raise
        except (OSError, ValueError, TypeError, KeyError):
            raise RunnerError("could not initialise private host run") from None

    def _command(self, args: list[str], source: str | None = None):
        try:
            # Provider credentials belong in this host, not in Rust children.
            result = subprocess.run(args, input=source, encoding="utf-8",
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    timeout=self._timeout, check=False,
                                    env={"PATH": os.environ.get("PATH", os.defpath)})
            response = json.loads(result.stdout)
            if result.returncode or response.get("ok") is not True:
                raise RunnerError("protocol operation rejected; inspect run status")
            return response["result"]
        except (OSError, subprocess.TimeoutExpired, UnicodeError, ValueError, KeyError, AttributeError):
            # Never propagate subprocess diagnostics (or an input-bearing
            # TimeoutExpired object) to an adapter/report.
            raise RunnerError("protocol command unavailable or interrupted") from None

    def _gateway(self, actor: str, **command):
        if actor not in self._tokens:
            raise RunnerError("unknown topology actor")
        frame = {"token": self._tokens[actor], **command}
        return self._command([self._gateway_bin, "serve", str(self._db), str(self._config)],
                             _json(frame) + "\n")

    def _wire(self, sender: str, recipient: str, kind: str, payload: dict, message_id: str) -> str:
        envelope = {"message_id": message_id, "run_id": self.run_id, "sender": sender,
                    "recipient": recipient, "version": "1", "kind": kind, "payload": payload}
        return self._command([self._mas, "encode", "-"], _json(envelope, 65_792))["text"]

    def _send(self, sender: str, recipient: str, kind: str, payload: dict) -> str:
        message_id = "message-" + uuid.uuid4().hex
        wire = self._wire(sender, recipient, kind, payload, message_id)
        self._gateway(sender, command="send", wire=wire)
        self.events.append({"sender": sender, "recipient": recipient, "kind": kind,
                            "message_id": message_id, "task_id": payload["task_id"]})
        return message_id

    def request(self, sender: str, recipient: str, operation: str, input: dict) -> Outcome:
        """Start one exchange. Adapters use Context.request for delegation."""
        if self._closed or not self._request_lock.acquire(blocking=False):
            raise RunnerError("runner is closed or already executing a request")
        try:
            return self._exchange(sender, recipient, operation, input)
        finally:
            self._request_lock.release()

    def _exchange(self, sender: str, recipient: str, operation: str, input: dict) -> Outcome:
        if len(self._active) >= self._max_depth:
            raise RunnerError("delegation depth limit reached")
        if sender not in self._adapters or recipient not in self._adapters:
            raise RunnerError("unknown topology actor")
        if type(input) is not dict:
            raise RunnerError("request input must be an object")
        task_id = "task-" + uuid.uuid4().hex
        request_id = self._send(sender, recipient, "Request", {
            "task_id": task_id, "operation": operation, "input": input,
        })
        self._service(recipient, request_id)
        deliveries = self._gateway(sender, command="receive", limit=100)
        replies = [item["message"] for item in deliveries
                   if item["message"]["kind"] in ("Result", "Failure")
                   and item["message"]["payload"].get("reply_to") == request_id]
        if len(replies) != 1:
            raise RunnerError("expected terminal response unavailable; inspect run status")
        reply = replies[0]
        if (reply["sender"] != recipient or reply["recipient"] != sender
                or reply["run_id"] != self.run_id or reply["payload"]["task_id"] != task_id):
            raise RunnerError("terminal response correlation failed")
        self._gateway(sender, command="ack", message_id=reply["message_id"])
        if reply["kind"] == "Failure":
            raise AdapterError("adapter execution failed; inspect run status")
        return Outcome(reply["payload"]["output"], reply["payload"]["status"])

    def _service(self, actor: str, request_id: str) -> None:
        deliveries = self._gateway(actor, command="receive", limit=100)
        # A nested mesh request may encounter this actor's unacknowledged outer
        # Request first. Select the exact new request, never redispatch the outer.
        selected = [item for item in deliveries if item["message"]["message_id"] == request_id
                    and item["message"]["kind"] == "Request"]
        if len(selected) != 1 or request_id in self._active:
            raise RunnerError("new request unavailable or already executing")
        delivery = selected[0]
        if delivery.get("actionable") is not True:
            raise RunnerError("task is no longer actionable; inspect run status")
        message = delivery["message"]
        payload = message["payload"]
        request = TaskRequest(message["sender"], actor, payload["operation"], payload["input"],
                              payload["task_id"], request_id)
        context = Context(self, actor, request_id)
        self._active.append(request_id)
        try:
            try:
                outcome = self._adapters[actor].handle(context, request)
                if (not isinstance(outcome, Outcome) or type(outcome.output) is not dict
                        or outcome.status not in ("completed", "unresolved")):
                    raise AdapterError("adapter returned an invalid outcome")
                self._send(actor, request.sender, "Result", {
                    "task_id": request.task_id, "reply_to": request_id,
                    "status": outcome.status, "output": outcome.output,
                })
            except Exception:
                try:
                    self._send(actor, request.sender, "Failure", {
                        "task_id": request.task_id, "reply_to": request_id,
                        "reason": "adapter_execution_failed",
                    })
                except RunnerError:
                    raise RunnerError("terminal response could not be confirmed; inspect run status") from None
            # Receipt is acknowledged only after a terminal record commits.
            self._gateway(actor, command="ack", message_id=request_id)
        finally:
            self._active.pop()

    def status(self) -> dict:
        """Return durable status, reconciling expiry even when quotas are spent."""
        return self._command([self._mas, "status", str(self._db), self.run_id])

    def close(self) -> None:
        """Stop new exchanges; retain the private run and evidence for inspection."""
        if self._active:
            raise RunnerError("cannot close a runner during adapter execution")
        self._closed = True

    def __enter__(self) -> Runner:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
