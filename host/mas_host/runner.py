"""Synchronous, host-owned adapters over the Rust MAS admission boundary.

Adapters are trusted Python code, not an isolation boundary. Model text is data.
Durable checkpoints permit compatible workflow replay. Unknown external effects
require an evidenced operator decision before any invocation can be repeated.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
import threading
import time
from typing import Callable, Mapping
import uuid

from .bootstrap import RunSession
from .codec import ENCODE_ENVELOPE_BYTES, integer_json as _json
from .contracts import (
    Adapter, AdapterContext, AdapterError, AdapterExecutor, AdapterInterrupted, AdapterTimeout, Invocation,
    InvocationTicket, Outcome, RecoveryRequired, RunnerError, TaskRequest,
    invocation_failure_diagnostic, validate_invocation_diagnostic,
)
from .journal import JournalError
from .protocol_client import ProtocolClient, ProtocolTransport


def _encode_wire(command, mas_bin: str, run_id: str, sender: str, recipient: str,
                 kind: str, payload: dict, message_id: str) -> str:
    """Use the authoritative Rust codec, including its complete wire bound."""
    envelope = {"message_id": message_id, "run_id": run_id, "sender": sender,
                "recipient": recipient, "version": "1", "kind": kind, "payload": payload}
    return command([mas_bin, "encode", "-"], _json(envelope, ENCODE_ENVELOPE_BYTES))["text"]


def _gateway_frame(token: str, **command) -> str:
    """The gateway bounds its escaped JSON frame independently of the wire."""
    return _json({"token": token, **command})


def validate_request_input(client: ProtocolTransport, run_id: str, sender: str,
                           recipient: str, operation: str, input: dict) -> None:
    """Read-only preflight of the exact generated-identity request format.

    Generated task/message identifiers and capabilities have fixed ASCII
    widths. Their values do not affect encoding size. No database, gateway
    session, reservation or adapter is opened by this validation.
    """
    wire = _encode_wire(client.command, client.mas, run_id, sender, recipient,
                        "Request", {"task_id": "task-" + "0" * 32,
                                    "operation": operation, "input": input},
                        "message-" + "0" * 32)
    _gateway_frame("0" * 64, command="send", wire=wire)


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
        self._ordinal = 0
        self._invocation_id = None
        self._recovery_required = False
        self._pending_invocation_id = None

    @property
    def actor(self) -> str:
        return self._actor

    @property
    def invocation_id(self) -> str | None:
        return self._invocation_id

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self._runner._deadline - time.monotonic())

    @property
    def ownership_descriptors(self) -> tuple[int, ...]:
        """Host-only lock retained by the guardian until local work is reaped."""
        return (self._runner._session.lock.fileno(),)

    def _check(self, *, allow_pending: bool = False) -> None:
        runner = self._runner
        if self._recovery_required:
            raise RecoveryRequired("invocation outcome unknown; inspect recovery journal")
        if (runner._closed or runner._invoking
                or self._thread != threading.get_ident()
                or not runner._active
                or runner._active[-1] != self._request_id):
            raise RunnerError("adapter context is not active")
        if self._pending_invocation_id is not None and not allow_pending:
            # A caller may catch a prepare/IPC failure. That cannot replace the
            # outstanding attempt with another call or delegation.
            self._recovery_required = True
            raise RecoveryRequired("pending invocation requires reconciliation before more work")

    def _step_key(self) -> str:
        key = f"{self._request_id}:{self._ordinal}"
        self._ordinal += 1
        return key

    def request(self, recipient: str, operation: str, input: dict) -> Outcome:
        self._check()
        try:
            return self._runner._exchange(self.actor, recipient, operation, input, self._step_key())
        except RecoveryRequired:
            self._recovery_required = True
            raise

    def prepare_invocation(self, *, input: dict | None = None,
                           reservation_microusd: int | None = None) -> InvocationTicket:
        """Parent-only durable admission. Providers run only after this returns execute=True."""
        self._check()
        if input is not None and type(input) is not dict:
            raise RunnerError("invocation identity input must be an object")
        if reservation_microusd is not None and (type(reservation_microusd) is not int or reservation_microusd < 0):
            raise RunnerError("invocation spending reservation must be a nonnegative integer")
        runner = self._runner
        key = self._step_key()
        data = runner._journal.step(key, "invoke", json.loads(_json({"input": input,
            "reservation_microusd": reservation_microusd})), {})
        call = runner._journal.invocation(data["call_id"]) if data else None
        if call is None or call["state"] == "retry_allowed":
            call = runner._journal.create_invocation(key, self.actor, reservation_microusd)
        call_id = call["call_id"]
        ticket = InvocationTicket(call_id, call["output"], False, key)
        if call["state"] in ("completed", "resolved"):
            self._pending_invocation_id = call_id
            runner._finish_recorded(self.actor, call)
            self._pending_invocation_id = None
            return ticket
        if call["state"] == "not_executed":
            raise RunnerError("invocation was not admitted")
        if call["state"] in ("executing", "unknown"):
            self._recovery_required = True
            raise RecoveryRequired("invocation outcome unknown; inspect and resolve before resuming")
        if runner.status()["expired"]:
            raise RunnerError("run deadline expired")
        try:
            runner._operations.admit_call(runner.run_id, call_id, reservation_microusd)
        except Exception:
            runner._journal.update_invocation(call_id, "not_executed", evidence={
                "outcome": "not_executed", "reason": "deployment_admission_rejected"})
            raise RunnerError("deployment invocation admission rejected") from None
        # Prepared means callback was never entered. A lost reserve reply can
        # therefore be reconciled without repeating an external effect.
        calls = runner.status()["calls"]
        if not any(item["id"] == call_id for item in calls):
            try:
                runner._gateway(self.actor, command="reserve", call_id=call_id)
            except RunnerError:
                try:
                    reserved = any(item["id"] == call_id for item in runner.status()["calls"])
                except RunnerError:
                    self._recovery_required = True
                    raise RecoveryRequired("reservation could not be reconciled; resume this run") from None
                if not reserved:
                    runner._journal.update_invocation(call_id, "not_executed", evidence={
                        "outcome": "not_executed", "reason": "protocol_admission_rejected"})
                    raise
        # This parent-owned marker precedes the durable write. An interrupt
        # between its commit and the supervisor receiving the returned ticket
        # must keep the workflow recoverable, not turn it into a Failure.
        self._pending_invocation_id = call_id
        runner._journal.update_invocation(call_id, "executing")
        return InvocationTicket(call_id, None, True, key)

    def complete_invocation(self, ticket: InvocationTicket, invocation: Invocation) -> dict:
        self._check(allow_pending=True)
        if (not ticket.execute or not isinstance(invocation, Invocation)
                or type(invocation.output) is not dict or type(invocation.evidence) is not dict):
            raise AdapterError("adapter invocation requires object output and evidence")
        runner = self._runner
        call = runner._journal.invocation(ticket.call_id)
        if (call is None or call["state"] != "executing" or call["actor"] != self.actor
                or ticket.call_id != self._pending_invocation_id):
            raise RunnerError("invocation ticket is no longer active")
        runner._wire(self.actor, self.actor, "Notice", {
            "output": invocation.output, "evidence": invocation.evidence,
        }, "validation-" + uuid.uuid4().hex)
        output = json.loads(_json(invocation.output))
        evidence = json.loads(_json(invocation.evidence))
        runner._journal.update_invocation(ticket.call_id, "completed", output=output, evidence=evidence)
        runner._finish_recorded(self.actor, runner._journal.invocation(ticket.call_id))
        self._pending_invocation_id = None
        return output

    def fail_invocation(self, ticket: InvocationTicket, *, diagnostic: dict | None = None) -> None:
        runner = self._runner
        call = runner._journal.invocation(ticket.call_id)
        # A failure to finish after the full result was checkpointed is recoverable
        # from that checkpoint and must not erase it.
        if call and call["state"] == "executing":
            self._recovery_required = True
            evidence = {"outcome": "unknown", "error": "adapter_invocation_failed"}
            if diagnostic is not None:
                evidence["diagnostic"] = validate_invocation_diagnostic(diagnostic)
            runner._journal.update_invocation(ticket.call_id, "unknown", evidence=evidence)
            try:
                runner._gateway(self.actor, command="finish", call_id=ticket.call_id, evidence=evidence)
            except RunnerError:
                pass

    def invoke(self, call: Callable[[], Invocation], *, input: dict | None = None,
               reservation_microusd: int | None = None) -> dict:
        ticket = self.prepare_invocation(input=input, reservation_microusd=reservation_microusd)
        if not ticket.execute:
            return json.loads(_json(ticket.cached_output))
        runner = self._runner
        try:
            runner._invoking = True
            self._invocation_id = ticket.call_id
            try:
                invocation = call()
            finally:
                runner._invoking = False
                self._invocation_id = None
            return self.complete_invocation(ticket, invocation)
        except Exception as error:
            self._recovery_required = True
            try:
                self.fail_invocation(ticket, diagnostic=invocation_failure_diagnostic(error))
            except Exception:
                pass  # The durable executing record still denotes uncertainty.
            raise RecoveryRequired("adapter invocation interrupted; inspect recovery journal") from None


class Runner:
    """Run any frozen directed topology using synchronous Python adapters.

    Each instance exclusively owns a private run directory. Resume reuses its
    frozen identities, deadline and credentials. Every actor operation uses a
    provisioned capability and the Rust gateway; the journal only checkpoints
    host orchestration, never implements protocol admission or task transitions.

    Execution is serial: nested delegation can revisit an actor with a new task,
    but the active outer request is never dispatched a second time. ``max_depth``
    bounds delegation cycles. A Result means an adapter's claim, not consensus.
    """

    def __init__(self, topology: dict, adapters: Mapping[str, Adapter],
                 state_dir: Path, mas_bin: str = "mas",
                 gateway_bin: str = "mas-gateway", max_depth: int = 8,
                 command_timeout: float = 10, *, workflow_version: str = "unversioned",
                 supervised: bool = True, operations=None, executor: AdapterExecutor | None = None,
                 protocol_client: ProtocolTransport | None = None, _resume_dir: Path | None = None,
                 execution_deadline: float | None = None):
        from .operations import DeploymentLedger
        if type(max_depth) is not int or not 1 <= max_depth <= 64:
            raise RunnerError("max_depth must be an integer from 1 to 64")
        if not isinstance(command_timeout, (int, float)) or not 0 < command_timeout <= 120:
            raise RunnerError("command timeout must be positive and at most 120 seconds")
        if type(workflow_version) is not str or not workflow_version or len(workflow_version) > 256:
            raise RunnerError("workflow_version must be a nonempty string of at most 256 characters")
        if execution_deadline is not None and (type(execution_deadline) not in (int, float)
                or (type(execution_deadline) is int and abs(execution_deadline) > 10**18)
                or not math.isfinite(execution_deadline)):
            raise RunnerError("execution deadline must be a finite monotonic timestamp")
        self._client = protocol_client if protocol_client is not None else ProtocolClient(mas_bin, gateway_bin)
        self._mas = self._client.mas
        self._gateway_bin = self._client.gateway
        self._timeout = command_timeout
        self._max_depth = max_depth
        self._adapters = dict(adapters)
        self._active: list[str] = []
        self._invoking = False
        self._closed = False
        self._request_lock = threading.Lock()
        self._root_ordinal = 0
        from .supervision import InlineExecutor, LinuxProcessExecutor
        self._executor = executor if executor is not None else (LinuxProcessExecutor() if supervised else InlineExecutor())
        self._deadline = float("inf")
        self._session = None
        self._journal = None
        self.events: list[dict] = []
        try:
            state_dir = Path(state_dir).absolute()
            state_dir.mkdir(mode=0o700, parents=True, exist_ok=True)
            self._operations = operations if operations is not None else DeploymentLedger(state_dir)
            if any(not callable(getattr(adapter, "handle", None)) for adapter in self._adapters.values()):
                raise RunnerError("every adapter must implement handle")
            self._session = RunSession(state_dir, operations=self._operations,
                client=self._client, command=self._command, topology=topology,
                workflow_version=workflow_version, max_depth=max_depth,
                actors=set(self._adapters), resume_dir=_resume_dir,
                command_timeout=self._timeout)
            self.run_id = self._session.run_id
            self.run_dir = self._session.run_dir
            self._db = self._session.database
            self._config = self._session.configuration
            self._journal = self._session.journal
            self._tokens = self._session.tokens
            snapshot = self._session.snapshot
            # Rust's persisted wall deadline is never renewed by restart. A
            # monotonic deadline additionally bounds this host process.
            self._deadline = time.monotonic() + max(0, snapshot["deadline"] - time.time())
            if execution_deadline is not None:
                # An enclosing workflow may tighten, but never extend, Rust's
                # persisted deadline. Adapter timeouts use this same bound.
                self._deadline = min(self._deadline, execution_deadline)
        except Exception as error:
            if self._session is not None:
                self._session.close()
            self._closed = True
            if isinstance(error, RunnerError):
                raise
            raise RunnerError("could not initialise or resume private host run") from None

    @classmethod
    def resume(cls, run_dir: Path, adapters: Mapping[str, Adapter], *, workflow_version: str,
               **options) -> Runner:
        """Replay the original workflow calls using frozen identities and checkpoints.

        Adapters must be deterministic except for effects made through invoke.
        Matching workflow_version is the caller's compatibility declaration.
        """
        run_dir = Path(run_dir).absolute()
        return cls({}, adapters, run_dir.parent, workflow_version=workflow_version,
                   _resume_dir=run_dir, **options)

    def _finish_recorded(self, actor: str, call: dict):
        existing = next((item for item in self.status()["calls"] if item["id"] == call["call_id"]), None)
        if existing is None:
            raise RunnerError("checkpoint has no matching Rust call reservation")
        if existing["state"] == "recorded":
            if call["state"] != "resolved" and existing["evidence"] != call["evidence"]:
                raise RunnerError("invocation evidence conflicts with Rust record")
            return
        self._gateway(actor, command="finish", call_id=call["call_id"], evidence=call["evidence"])

    def _command(self, args: list[str], source: str | None = None):
        timeout = (self._timeout if not self._active else
                   max(0.01, min(self._timeout, self._deadline - time.monotonic())))
        return self._client.command(args, source, timeout=timeout)

    def _gateway(self, actor: str, **command):
        if actor not in self._tokens:
            raise RunnerError("unknown topology actor")
        return self._command([self._gateway_bin, "serve", str(self._db), str(self._config)],
                             _gateway_frame(self._tokens[actor], **command) + "\n")

    def _wire(self, sender: str, recipient: str, kind: str, payload: dict, message_id: str) -> str:
        return _encode_wire(self._command, self._mas, self.run_id, sender, recipient,
                            kind, payload, message_id)

    def _send_recorded(self, envelope: dict) -> str:
        wire = self._wire(envelope["sender"], envelope["recipient"], envelope["kind"],
                          envelope["payload"], envelope["message_id"])
        try:
            self._gateway(envelope["sender"], command="send", wire=wire)
        except RunnerError:
            # Exact-envelope retry is safe under Rust's durable identity rule.
            self._gateway(envelope["sender"], command="send", wire=wire)
        if not any(event["message_id"] == envelope["message_id"] for event in self.events):
            self.events.append({key: envelope[key] for key in ("sender", "recipient", "kind", "message_id")}
                               | {"task_id": envelope["payload"]["task_id"]})
        return envelope["message_id"]

    def _ack(self, actor, message_id):
        try:
            self._gateway(actor, command="ack", message_id=message_id)
        except RunnerError:
            self._gateway(actor, command="ack", message_id=message_id)

    def request(self, sender: str, recipient: str, operation: str, input: dict) -> Outcome:
        """Start/replay one exchange; repeat original calls in original order on resume."""
        if self._closed or not self._request_lock.acquire(blocking=False):
            raise RunnerError("runner is closed or already executing a request")
        key = f"root:{self._root_ordinal}"
        self._root_ordinal += 1
        try:
            return self._exchange(sender, recipient, operation, input, key)
        except JournalError as error:
            raise RunnerError(str(error)) from None
        finally:
            self._request_lock.release()

    def _exchange(self, sender: str, recipient: str, operation: str, input: dict, key: str) -> Outcome:
        if len(self._active) >= self._max_depth:
            raise RunnerError("delegation depth limit reached")
        if sender not in self._adapters or recipient not in self._adapters:
            raise RunnerError("unknown topology actor")
        if type(input) is not dict:
            raise RunnerError("request input must be an object")
        inputs = json.loads(_json({"sender": sender, "recipient": recipient,
                                  "operation": operation, "input": input}))
        initial = {"request": {"sender": sender, "recipient": recipient, "kind": "Request",
            "message_id": "message-" + uuid.uuid4().hex,
            "payload": {"task_id": "task-" + uuid.uuid4().hex, "operation": operation, "input": input}},
            "terminal": None, "done": False}
        if not self._operations.admission_enabled:
            saved = self._journal.get_step(key)
            terminal = saved.get("terminal") if saved else None
            committed = terminal and any(task.get("terminal_message_id") == terminal["message_id"]
                                         for task in self.status()["tasks"])
            if not committed:
                raise RunnerError("restored deployment is audit-only; incomplete work cannot resume")
        step = self._journal.step(key, "request", inputs, initial)
        request = step["request"]
        request_id = request["message_id"]
        self._send_recorded(request)
        if step["terminal"] is None:
            self._service(recipient, request_id, key, step)
            step = self._journal.get_step(key)
        else:
            self._send_recorded(step["terminal"])
            self._ack(recipient, request_id)
        # Drain even when a lost ACK response already removed the terminal from
        # the mailbox; the persisted envelope and Rust commit identify it.
        self._gateway(sender, command="receive", limit=100)
        reply = step["terminal"]
        if reply is None:
            raise RunnerError("terminal response unavailable; inspect recovery journal")
        self._ack(sender, reply["message_id"])
        step["done"] = True
        self._journal.set_step(key, step)
        if reply["kind"] == "Failure":
            raise AdapterError("adapter execution failed; inspect run status")
        return Outcome(reply["payload"]["output"], reply["payload"]["status"])

    def _service(self, actor: str, request_id: str, key: str, step: dict) -> None:
        deliveries = self._gateway(actor, command="receive", limit=100)
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
            terminal_id = "message-" + uuid.uuid4().hex
            try:
                outcome = self._executor.execute(self._adapters[actor], context, request,
                                                 deadline=self._deadline, tick=self.status)
                if context._recovery_required or context._pending_invocation_id is not None:
                    raise RecoveryRequired("invocation outcome unknown; inspect recovery journal")
                if (not isinstance(outcome, Outcome) or type(outcome.output) is not dict
                        or outcome.status not in ("completed", "unresolved")):
                    raise AdapterError("adapter returned an invalid outcome")
                if context._ordinal != self._journal.child_count(request_id):
                    raise JournalError("workflow replay omitted recorded steps")
                terminal_payload = {"task_id": request.task_id, "reply_to": request_id,
                                    "status": outcome.status, "output": outcome.output}
                _json(terminal_payload)
                # An unencodable result must not become a durable terminal
                # intent that can never be sent. Completed invocation records
                # remain known; only this application result is rejected.
                wire = self._wire(actor, request.sender, "Result", terminal_payload, terminal_id)
                _gateway_frame(self._tokens[actor], command="send", wire=wire)
                kind = "Result"
            except (RecoveryRequired, JournalError):
                raise
            except (AdapterInterrupted, AdapterTimeout):
                if context._recovery_required or context._pending_invocation_id is not None:
                    raise RecoveryRequired("adapter invocation interrupted; inspect recovery journal") from None
                raise
            except Exception:
                if context._recovery_required or context._pending_invocation_id is not None:
                    raise RecoveryRequired("adapter invocation interrupted; inspect recovery journal") from None
                terminal_payload = {"task_id": request.task_id, "reply_to": request_id,
                                    "reason": "adapter_execution_failed"}
                kind = "Failure"
            # Persist the entire terminal before submission; response loss may
            # then retry the identical envelope instead of a conflicting Failure.
            step["terminal"] = {"sender": actor, "recipient": request.sender, "kind": kind,
                                "payload": terminal_payload, "message_id": terminal_id}
            self._journal.set_step(key, step)
            self._send_recorded(step["terminal"])
            self._ack(actor, request_id)
        finally:
            self._active.pop()

    def status(self) -> dict:
        """Return durable Rust status, reconciling expiry independently of adapters."""
        return self._command([self._mas, "status", str(self._db), self.run_id])

    def close(self) -> None:
        """Release ownership; retain the private run for inspection/recovery."""
        if self._active:
            raise RunnerError("cannot close a runner during adapter execution")
        if not self._closed:
            self._closed = True
            self._session.close()

    def __enter__(self) -> Runner:
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
