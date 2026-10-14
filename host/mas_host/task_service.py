"""Durable principal-scoped ingress above the unchanged Rust MAS boundary.

One submitted revision owns one deterministic run and one root request. A
single Linux worker replays that root after interruption; transports never
execute Runner on their request threads. Unknown effects require an evidenced
journal resolution before explicit recovery, never a fresh submission.
"""
from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
import fcntl
import hashlib
import json
import os
from pathlib import Path
from types import MappingProxyType
from typing import Callable, Mapping, Protocol
import uuid

from .codec import JsonSizeError, integer_json
from .contracts import Adapter, AdapterInterrupted, AdapterTimeout, Outcome, RecoveryRequired, RunnerError
from .service_contracts import ServiceError
from .task_store import TaskStore, canonical, decoded, event, now


TERMINAL = frozenset({"completed", "failed", "cancelled"})
INTERRUPTED = frozenset({"input_required", "auth_required"})
STATES = TERMINAL | INTERRUPTED | {"queued", "running", "unknown"}
PAGE_BYTES = 512 * 1024


def _bounded_items(values, *, limit=1000, overhead=2):
    """Consume a bounded JSON prefix, leaving room for its enclosing object."""
    selected, size = [], overhead
    for value in values:
        item_bytes = len(canonical(value).encode("utf-8")) + bool(selected)
        if len(selected) >= limit or size + item_bytes > PAGE_BYTES:
            if not selected:
                raise ServiceError("page_item_too_large")
            return selected, True
        selected.append(value)
        size += item_bytes
    return selected, False


def _text(value, name: str, maximum: int = 256):
    if type(value) is not str or not value or any(ord(c) < 32 for c in value):
        raise ServiceError("invalid_" + name)
    try:
        if len(value.encode("utf-8")) > maximum:
            raise ServiceError("invalid_" + name)
    except UnicodeError:
        raise ServiceError("invalid_" + name) from None
    return value


def _object(value):
    if type(value) is not dict:
        raise ServiceError("invalid_input")
    try:
        return json.loads(integer_json(value))
    except JsonSizeError:
        raise ServiceError("input_too_large", "application JSON exceeds its byte limit") from None
    except RunnerError:
        raise ServiceError("invalid_input", "input must be bounded integer-only JSON") from None


@dataclass(frozen=True)
class ExecutionBinding:
    """Trusted immutable identity supplied only by the durable scheduler.

    Factory code can bind provider adapters to the assigned run directory and
    scope remote receipts to an authenticated owner. These values never come
    from application input or a peer's claimed identity.
    """
    principal: str
    workflow: str
    workflow_identity: str
    task_id: str
    revision: int
    run_dir: Path
    state_dir: Path

    def remote_namespace(self, actor: str) -> str:
        _text(actor, "actor")
        return "owner-" + hashlib.sha256(canonical({
            "principal": self.principal, "workflow": self.workflow,
            "workflow_identity": self.workflow_identity, "actor": actor}).encode()).hexdigest()


@dataclass(frozen=True)
class Workflow:
    """Explicit trusted workflow registration, frozen for recovery.

    ``version`` covers adapter implementation, provider identity, classifiers
    and continuation logic. Change the registered name/version when changing
    those behaviours. ``continue_input`` is pure: it combines the previous
    output with supplied input for a fresh, separately admitted revision.
    """
    version: str
    topology: dict
    adapters: Mapping[str, Adapter]
    sender: str
    recipient: str
    operation: str
    max_depth: int = 8
    classify: Callable[[Outcome], str] | None = None
    continue_input: Callable[[dict, dict], dict] | None = None
    adapter_factory: Callable[[ExecutionBinding], Mapping[str, Adapter]] | None = None

    def __post_init__(self):
        _text(self.version, "workflow_version")
        if self.version == "unversioned":
            raise ServiceError("invalid_workflow_version")
        topology = _object(self.topology)
        if (type(self.max_depth) is not int or not 1 <= self.max_depth <= 64
                or set(topology.get("agents", [])) != set(self.adapters)
                or self.sender not in self.adapters or self.recipient not in self.adapters
                or any(not callable(getattr(adapter, "handle", None)) for adapter in self.adapters.values())):
            raise ServiceError("invalid_workflow")
        _text(self.operation, "operation")
        object.__setattr__(self, "topology", topology)
        object.__setattr__(self, "adapters", MappingProxyType(dict(self.adapters)))

    @property
    def identity(self) -> str:
        return hashlib.sha256(canonical({"version": self.version, "topology": self.topology,
            "sender": self.sender, "recipient": self.recipient, "operation": self.operation,
            "max_depth": self.max_depth, "continuation": self.continue_input is not None,
            "classifier": self.classify is not None, "adapter_factory": self.adapter_factory is not None}).encode()).hexdigest()


@dataclass(frozen=True)
class ExecutionResult:
    state: str
    result: dict | None = None
    error: dict | None = None
    events: tuple[dict, ...] = ()


class WorkflowExecutor(Protocol):
    def execute(self, workflow: Workflow, execution: dict, *, cancelled: Callable[[], bool],
                ownership_descriptor: int) -> ExecutionResult: ...


class WorkflowInputValidator(Protocol):
    """Optional executor-specific preflight, without effects or durable writes.

    Executors with additional representation limits implement this capability.
    The service's common integer-JSON bound still applies to every executor.
    """
    def validate_input(self, workflow: Workflow, run_id: str, input: dict) -> None: ...


class _OwnedContext:
    def __init__(self, context, descriptor):
        self._context, self._descriptor = context, descriptor

    @property
    def ownership_descriptors(self):
        return (*self._context.ownership_descriptors, self._descriptor)

    def __getattr__(self, name):
        return getattr(self._context, name)


class _CancellableExecutor:
    def __init__(self, executor, cancelled, descriptor):
        self.executor, self.cancelled, self.descriptor = executor, cancelled, descriptor

    def execute(self, adapter, context, request, *, deadline, tick=None):
        def poll():
            if self.cancelled():
                raise AdapterInterrupted("task cancellation requested")
            if tick:
                tick()
        poll()
        return self.executor.execute(adapter, _OwnedContext(context, self.descriptor), request,
                                     deadline=deadline, tick=poll)


class RunnerExecutor:
    """Execute registrations through real Runner admission and durable replay."""
    def __init__(self, state_dir: Path, *, operations, mas_bin: str = "mas",
                 gateway_bin: str = "mas-gateway", supervised: bool = True,
                 command_timeout: float = 10):
        self.state_dir = Path(state_dir).absolute()
        self.operations = operations
        self.options = {"mas_bin": os.environ.get("MAS_BIN", mas_bin) if mas_bin == "mas" else mas_bin,
                        "gateway_bin": os.environ.get("MAS_GATEWAY_BIN", gateway_bin) if gateway_bin == "mas-gateway" else gateway_bin,
                        "command_timeout": command_timeout, "supervised": supervised}

    def validate_input(self, workflow: Workflow, run_id: str, input: dict) -> None:
        from .protocol_client import ProtocolClient
        from .runner import validate_request_input
        try:
            client = ProtocolClient(self.options["mas_bin"], self.options["gateway_bin"])
            validate_request_input(client, run_id, workflow.sender, workflow.recipient,
                                   workflow.operation, input)
        except JsonSizeError:
            raise ServiceError("input_too_large", "input does not fit the complete protocol frame") from None
        except RunnerError:
            raise ServiceError("input_not_encodable", "input could not be encoded for this workflow") from None

    def _bootstrap(self, workflow, execution):
        from .bootstrap import _write_json, _sync_directory, _read_json
        path = self.state_dir / execution["run_directory"]
        if path.parent != self.state_dir or path.is_symlink():
            raise ServiceError("invalid_run_directory")
        path.mkdir(mode=0o700, exist_ok=True)
        manifest = path / "bootstrap.json"
        if not manifest.exists():
            # The service's committed execution row authorises this exact
            # identity. Before this manifest exists, no admission/provider
            # invocation can have happened for this revision.
            if any(item.name != ".bootstrap.json.pending" for item in path.iterdir()):
                raise ServiceError("incomplete_bootstrap")
            _write_json(manifest, {"format": "mas-host-bootstrap/1", "stage": "intent",
                "run_id": execution["run_id"], "workflow_version": workflow.version,
                "max_depth": workflow.max_depth, "topology": workflow.topology,
                "gateway_id": "service-" + hashlib.sha256(execution["run_id"].encode()).hexdigest()[:24]})
            _sync_directory(self.state_dir)
        intent = _read_json(manifest)
        if (intent.get("run_id") != execution["run_id"]
                or intent.get("workflow_version") != workflow.version
                or intent.get("max_depth") != workflow.max_depth
                or intent.get("topology") != workflow.topology):
            raise ServiceError("run_assignment_conflict")
        return path

    def execute(self, workflow, execution, *, cancelled, ownership_descriptor):
        from .runner import Runner
        from .supervision import InlineExecutor, LinuxProcessExecutor
        path = self._bootstrap(workflow, execution)
        adapters = workflow.adapters
        if workflow.adapter_factory is not None:
            binding = ExecutionBinding(execution["principal"], execution["workflow"], workflow.identity,
                execution["task_id"], execution["revision"], path, self.state_dir)
            try:
                adapters = dict(workflow.adapter_factory(binding))
            except Exception:
                raise ServiceError("invalid_adapter_factory") from None
            if (set(adapters) != set(workflow.adapters)
                    or any(not callable(getattr(adapter, "handle", None)) for adapter in adapters.values())):
                raise ServiceError("invalid_adapter_factory")
        executor = LinuxProcessExecutor() if self.options["supervised"] else InlineExecutor()
        with Runner.resume(path, adapters, workflow_version=workflow.version,
                           max_depth=workflow.max_depth, operations=self.operations,
                           executor=_CancellableExecutor(executor, cancelled, ownership_descriptor),
                           **self.options) as runner:
            try:
                outcome = runner.request(workflow.sender, workflow.recipient,
                                         workflow.operation, execution["input"])
                state = workflow.classify(outcome) if workflow.classify else (
                    "completed" if outcome.status == "completed" else "failed")
                if state not in TERMINAL | INTERRUPTED:
                    raise ServiceError("invalid_workflow_state")
                return ExecutionResult(state, _object(outcome.output),
                    error={"code": "application_unresolved"} if not workflow.classify and state == "failed" else None,
                    events=tuple(runner.events))
            except RecoveryRequired:
                return ExecutionResult("unknown", error={"code": "reconciliation_required"},
                                       events=tuple(runner.events))
            except AdapterInterrupted:
                from .journal import inspect_run
                if inspect_run(path)["recovery_required"]:
                    return ExecutionResult("unknown", error={"code": "reconciliation_required"}, events=tuple(runner.events))
                # The supervised executor reports RecoveryRequired instead if
                # an external invocation may have escaped before cancellation.
                if cancelled():
                    return ExecutionResult("cancelled", events=tuple(runner.events))
                raise
            except AdapterTimeout:
                from .journal import inspect_run
                if inspect_run(path)["recovery_required"]:
                    return ExecutionResult("unknown", error={"code": "reconciliation_required"}, events=tuple(runner.events))
                return ExecutionResult("failed", error={"code": "deadline_exceeded"},
                                       events=tuple(runner.events))
            except RunnerError:
                from .journal import inspect_run
                if inspect_run(path)["recovery_required"]:
                    return ExecutionResult("unknown", error={"code": "reconciliation_required"}, events=tuple(runner.events))
                return ExecutionResult("failed", error={"code": "workflow_failed"},
                                       events=tuple(runner.events))


class DurableTaskService:
    def __init__(self, state_dir: Path, workflows: Mapping[str, Workflow],
                 grants: Mapping[str, set[str] | frozenset[str]], *, operations=None,
                 executor: WorkflowExecutor | None = None, max_pending: int = 1000,
                 max_tasks: int = 10000, max_revisions: int = 32,
                 mas_bin: str = "mas", gateway_bin: str = "mas-gateway",
                 supervised: bool = True):
        from .operations import DeploymentLedger, _file_lock
        self.state_dir = Path(state_dir).absolute()
        self.operations = operations or DeploymentLedger(self.state_dir)
        if Path(self.operations.state_dir).absolute() != self.state_dir:
            raise ServiceError("deployment_mismatch")
        if any(type(x) is not int or not 1 <= x <= 1_000_000 for x in (max_pending, max_tasks, max_revisions)):
            raise ServiceError("invalid_service_limits")
        self.max_pending, self.max_tasks, self.max_revisions = max_pending, max_tasks, max_revisions
        self.workflows = MappingProxyType(dict(workflows))
        self.grants = MappingProxyType({_text(p, "principal"): frozenset(names) for p, names in grants.items()})
        for name, workflow in self.workflows.items():
            _text(name, "workflow")
            if not isinstance(workflow, Workflow):
                raise ServiceError("invalid_workflow")
        if any(not names <= self.workflows.keys() for names in self.grants.values()):
            raise ServiceError("invalid_grant")
        self.identities = {name: workflow.identity for name, workflow in self.workflows.items()}
        active = self.operations.admission_enabled
        with _file_lock(self.state_dir / ".deployment.lock", False):
            self.store = TaskStore(self.state_dir, deployment_id=self.operations.deployment_id,
                                   readonly=not active)
            with self.store.transaction(readonly=not active) as db:
                for name, identity in self.identities.items():
                    row = db.execute("SELECT identity FROM workflows WHERE name=?", (name,)).fetchone()
                    if row and row[0] != identity:
                        raise ServiceError("workflow_changed")
                    if not row:
                        if not active:
                            raise ServiceError("audit_only")
                        db.execute("INSERT INTO workflows VALUES(?,?)", (name, identity))
        self.executor = executor or RunnerExecutor(self.state_dir, operations=self.operations,
            mas_bin=mas_bin, gateway_bin=gateway_bin, supervised=supervised)

    @contextmanager
    def _mutation(self):
        from .operations import _file_lock
        with _file_lock(self.state_dir / ".deployment.lock", False):
            if not self.operations.admission_enabled:
                raise ServiceError("audit_only")
            self.operations._disk()
            with self.store.transaction() as db:
                yield db

    def _authorise(self, principal, workflow=None):
        _text(principal, "principal")
        if principal not in self.grants or workflow is not None and workflow not in self.grants[principal]:
            raise ServiceError("forbidden")
        if workflow is not None and self.workflows[workflow].identity != self.identities[workflow]:
            raise ServiceError("workflow_changed")

    def _owned(self, db, principal, task_id):
        self._authorise(principal)
        _text(task_id, "task_id")
        row = db.execute("SELECT * FROM tasks WHERE id=? AND principal=?", (task_id, principal)).fetchone()
        if row is None:
            raise ServiceError("task_not_found")
        task = decoded(row)
        self._authorise(principal, task["workflow"])
        return task

    def _receipt(self, task):
        return {"task_id": task["id"], **{key: task[key] for key in (
            "state", "workflow", "request_key", "created_at", "updated_at",
            "cancel_requested", "result", "error", "revision", "metadata")},
            "capabilities": {"cancel": True,
                "update": self.workflows[task["workflow"]].continue_input is not None,
                "events": True}}

    def _capacity(self, db, *, new=False):
        if new and db.execute("SELECT count(*) FROM tasks").fetchone()[0] >= self.max_tasks:
            raise ServiceError("task_limit")
        if db.execute("SELECT count(*) FROM tasks WHERE state NOT IN ('completed','failed','cancelled')").fetchone()[0] >= self.max_pending:
            raise ServiceError("queue_full")

    @staticmethod
    def _digest(kind, task_id, workflow, input, metadata=None):
        return hashlib.sha256(canonical({"kind": kind, "task_id": task_id,
            "workflow": workflow, "input": input, "metadata": metadata or {}}).encode()).hexdigest()

    def _duplicate(self, db, principal, key, digest):
        row = db.execute("SELECT digest,task_id FROM requests WHERE principal=? AND request_key=?", (principal, key)).fetchone()
        if row:
            if row["digest"] != digest:
                raise ServiceError("request_conflict")
            return self._receipt(self._owned(db, principal, row["task_id"]))
        return None

    @staticmethod
    def _run_id(task_id, revision):
        stem = task_id.removeprefix("task-")
        return f"run-service-{stem}-{revision}"

    def _validate_execution_input(self, workflow, task_id, revision, input):
        validate = getattr(self.executor, "validate_input", None)
        if validate is not None:
            if not callable(validate):
                raise ServiceError("invalid_executor_validator")
            validate(workflow, self._run_id(task_id, revision), input)

    @staticmethod
    def _execution(db, task_id, revision, input):
        run_id = DurableTaskService._run_id(task_id, revision)
        db.execute("INSERT INTO executions(task_id,revision,run_id,run_directory,input,state) VALUES(?,?,?,?,?,'queued')",
                   (task_id, revision, run_id, run_id, canonical(input)))

    def submit(self, principal, request_key, workflow, input, *, metadata=None):
        _text(workflow, "workflow")
        self._authorise(principal, workflow)
        _text(request_key, "request_key")
        value = _object(input)
        metadata = _object(metadata) if metadata is not None else {}
        digest = self._digest("submit", None, workflow, value, metadata)
        with self._mutation() as db:
            duplicate = self._duplicate(db, principal, request_key, digest)
            if duplicate:
                return duplicate
            self._capacity(db, new=True)
            task_id, timestamp = "task-" + uuid.uuid4().hex, now()
            self._validate_execution_input(self.workflows[workflow], task_id, 0, value)
            db.execute("INSERT INTO tasks(id,principal,request_key,workflow,workflow_identity,state,input,metadata,created_at,updated_at) VALUES(?,?,?,?,?,'queued',?,?,?,?)",
                (task_id, principal, request_key, workflow, self.identities[workflow], canonical(value), canonical(metadata), timestamp, timestamp))
            db.execute("INSERT INTO requests VALUES(?,?,?,?,?)", (principal, request_key, digest, task_id, "submit"))
            self._execution(db, task_id, 0, value)
            event(db, task_id, "submitted", {"state": "queued", "revision": 0})
            return self._receipt(self._owned(db, principal, task_id))

    def inspect(self, principal, task_id):
        with self.store.transaction(readonly=True) as db:
            return self._receipt(self._owned(db, principal, task_id))

    def result(self, principal, task_id):
        receipt = self.inspect(principal, task_id)
        if receipt["state"] != "completed":
            raise ServiceError("result_unavailable")
        return receipt["result"]

    def cancel(self, principal, task_id):
        """Close local queued/paused work; record intent for executing work.

        A paused revision has already stopped executing. Closing its future
        continuation does not cancel any separately tracked remote task, whose
        durable receipt and reconciliation obligations remain intact.
        """
        with self._mutation() as db:
            task = self._owned(db, principal, task_id)
            can_close = task["state"] in {"queued", *INTERRUPTED}
            if task["state"] in TERMINAL or (task["cancel_requested"] and not can_close):
                return self._receipt(task)
            # Repeated cancellation also repairs a paused record written by an
            # older worker after cancellation intent was already recorded.
            state = "cancelled" if can_close else task["state"]
            db.execute("UPDATE tasks SET cancel_requested=1,state=?,updated_at=? WHERE id=?", (state, now(), task_id))
            if state == "cancelled":
                db.execute("UPDATE executions SET state='cancelled' WHERE task_id=? AND revision=?", (task_id, task["revision"]))
            event(db, task_id, "cancel_requested", {"state": state, "confirmed": state == "cancelled",
                                                    "scope": "local_workflow"})
            return self._receipt(self._owned(db, principal, task_id))

    def update(self, principal, task_id, request_key, input, *, metadata=None):
        _text(request_key, "request_key")
        value = _object(input)
        metadata = _object(metadata) if metadata is not None else {}
        with self._mutation() as db:
            task = self._owned(db, principal, task_id)
            digest = self._digest("update", task_id, task["workflow"], value, metadata)
            duplicate = self._duplicate(db, principal, request_key, digest)
            if duplicate:
                return duplicate
            workflow = self.workflows[task["workflow"]]
            if workflow.continue_input is None:
                raise ServiceError("unsupported_capability")
            if task["state"] not in INTERRUPTED or task["cancel_requested"]:
                raise ServiceError("invalid_task_state")
            if task["revision"] + 1 >= self.max_revisions:
                raise ServiceError("revision_limit")
            # Pure application mapping, never a network effect or model call.
            try:
                combined = _object(workflow.continue_input(task["result"] or {}, value))
            except Exception:
                raise ServiceError("invalid_continuation") from None
            revision = task["revision"] + 1
            self._validate_execution_input(workflow, task_id, revision, combined)
            db.execute("INSERT INTO requests VALUES(?,?,?,?,?)", (principal, request_key, digest, task_id, "update"))
            self._execution(db, task_id, revision, combined)
            db.execute("UPDATE tasks SET state='queued',revision=?,error=NULL,updated_at=? WHERE id=?", (revision, now(), task_id))
            event(db, task_id, "continued", {"state": "queued", "revision": revision})
            return self._receipt(self._owned(db, principal, task_id))

    def events(self, principal, task_id, after=0):
        """Return a byte-bounded prefix; resume after the last returned event ID."""
        if type(after) is not int or not 0 <= after <= 2**63 - 1:
            raise ServiceError("invalid_cursor")
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, task_id)
            values = (decoded(row) for row in db.execute(
                "SELECT * FROM events WHERE task_id=? AND id>? ORDER BY id LIMIT 1000", (task_id, after)))
            return _bounded_items(values)[0]

    def list_tasks(self, principal, limit=100, offset=0):
        """Return a byte-bounded prefix; advance offset by the returned count."""
        self._authorise(principal)
        if type(limit) is not int or not 1 <= limit <= 1000 or type(offset) is not int or not 0 <= offset <= 2**63 - 1:
            raise ServiceError("invalid_pagination")
        with self.store.transaction(readonly=True) as db:
            # Filter before pagination so revoking a workflow cannot reveal its
            # tasks, nor change another workflow's page boundaries by accident.
            allowed = sorted(self.grants[principal])
            if not allowed:
                return []
            values = (self._receipt(decoded(row)) for row in db.execute(
                "SELECT * FROM tasks WHERE principal=? AND workflow IN (" + ",".join("?" * len(allowed)) + ") ORDER BY created_at,id LIMIT ? OFFSET ?",
                (principal, *allowed, limit, offset)))
            return _bounded_items(values)[0]

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        """Keyset page ordered by last update, scoped before counts and paging.

        Cursors identify an observed position, not a historical snapshot: a
        task updated during traversal can move to an earlier page. The
        ``updated_after`` lower timestamp bound is inclusive. Pages are bounded
        by both count and encoded bytes; ``next`` covers either truncation.
        At most 64 metadata predicates may be supplied in one query.
        """
        self._authorise(principal)
        if type(page_size) is not int or not 1 <= page_size <= 1000:
            raise ServiceError("invalid_pagination")
        if state is not None and (type(state) is not str or state not in STATES):
            raise ServiceError("invalid_task_state")
        if updated_after is not None and (type(updated_after) is not int or not 0 <= updated_after <= 2**63 - 1):
            raise ServiceError("invalid_timestamp")
        if after is not None and (type(after) not in (tuple, list) or len(after) != 2
                or type(after[0]) is not int or not 0 <= after[0] <= 2**63 - 1
                or type(after[1]) is not str or not after[1]):
            raise ServiceError("invalid_cursor")
        filters = _object(metadata) if metadata is not None else {}
        if len(filters) > 64 or any(not key or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_" for c in key)
                                   or type(value) is not str for key, value in filters.items()):
            raise ServiceError("invalid_metadata_filter")
        allowed = sorted(self.grants[principal])
        if not allowed:
            return {"tasks": [], "total_size": 0, "next": None}
        clauses = ["principal=?", "workflow IN (" + ",".join("?" * len(allowed)) + ")"]
        values = [principal, *allowed]
        if state is not None:
            clauses.append("state=?")
            values.append(state)
        if updated_after is not None:
            clauses.append("updated_at>=?")
            values.append(updated_after)
        for key, value in sorted(filters.items()):
            clauses.append("json_extract(metadata,?)=?")
            values.extend(("$." + key, value))
        with self.store.transaction(readonly=True) as db:
            total = db.execute("SELECT count(*) FROM tasks WHERE " + " AND ".join(clauses), values).fetchone()[0]
            if after is not None:
                clauses.append("(updated_at<? OR (updated_at=? AND id<?))")
                values.extend((after[0], after[0], after[1]))
            rows = db.execute("SELECT * FROM tasks WHERE " + " AND ".join(clauses) + " ORDER BY updated_at DESC,id DESC LIMIT ?",
                              [*values, page_size + 1])
            # Reserve ample space for total_size and the bounded task cursor.
            selected, more = _bounded_items((self._receipt(decoded(row)) for row in rows),
                                           limit=page_size, overhead=1024)
            cursor = [selected[-1]["updated_at"], selected[-1]["task_id"]] if more else None
            return {"tasks": selected, "total_size": total, "next": cursor}

    def recover(self, principal, task_id):
        """Queue a previously unknown revision after evidenced journal resolution.

        The existing admin resolve-invocation operation owns that evidence. This
        method cannot authorise a retry and never changes invocation records.
        """
        from .journal import inspect_run
        with self._mutation() as db:
            task = self._owned(db, principal, task_id)
            if task["state"] != "unknown":
                raise ServiceError("invalid_task_state")
            execution = db.execute("SELECT * FROM executions WHERE task_id=? AND revision=?", (task_id, task["revision"])).fetchone()
            try:
                report = inspect_run(self.state_dir / execution["run_directory"])
            except Exception:
                raise ServiceError("reconciliation_required") from None
            if report["recovery_required"]:
                raise ServiceError("reconciliation_required")
            db.execute("UPDATE tasks SET state='queued',error=NULL,updated_at=? WHERE id=?", (now(), task_id))
            db.execute("UPDATE executions SET state='queued' WHERE task_id=? AND revision=?", (task_id, task["revision"]))
            event(db, task_id, "recovery_queued", {"state": "queued", "revision": task["revision"]})
            return self._receipt(self._owned(db, principal, task_id))

    def run_once(self):
        from .operations import _file_lock
        lock = self.state_dir / ".task-service-worker.lock"
        descriptor = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError("worker_busy") from None
            with _file_lock(self.state_dir / ".deployment.lock", False):
                if not self.operations.admission_enabled:
                    raise ServiceError("audit_only")
                with self.store.transaction() as db:
                    row = db.execute("SELECT e.* FROM executions e JOIN tasks t ON t.id=e.task_id AND t.revision=e.revision WHERE e.state IN ('queued','running') ORDER BY e.sequence LIMIT 1").fetchone()
                    if row is None:
                        return None
                    execution = decoded(row)
                    task = decoded(db.execute("SELECT * FROM tasks WHERE id=?", (execution["task_id"],)).fetchone())
                    workflow = self.workflows.get(task["workflow"])
                    if workflow is None or workflow.identity != task["workflow_identity"]:
                        raise ServiceError("workflow_changed")
                    if task["workflow"] not in self.grants.get(task["principal"], ()):
                        # Revocation prevents further dispatch; it says nothing
                        # about effects that escaped before an interrupted
                        # worker. Keep those outcomes unresolved, including a
                        # previously recovered revision queued on its old run.
                        started = execution["state"] == "running" or (self.state_dir / execution["run_directory"]).exists()
                        state = "unknown" if started else "failed"
                        code = "permission_revoked_reconciliation_required" if started else "permission_revoked"
                        db.execute("UPDATE tasks SET state=?,error=?,updated_at=? WHERE id=?",
                                   (state, canonical({"code": code}), now(), task["id"]))
                        db.execute("UPDATE executions SET state=? WHERE sequence=?", (state, execution["sequence"]))
                        event(db, task["id"], "state_changed", {"state": state, "reason": code})
                        return self._receipt(decoded(db.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()))
                    db.execute("UPDATE tasks SET state='running',updated_at=? WHERE id=?", (now(), task["id"]))
                    db.execute("UPDATE executions SET state='running' WHERE sequence=?", (execution["sequence"],))
                    if task["state"] != "running":
                        event(db, task["id"], "running", {"state": "running", "revision": task["revision"], "run_id": execution["run_id"]})

                def cancelled():
                    with self.store.transaction(readonly=True) as db:
                        return bool(db.execute("SELECT cancel_requested FROM tasks WHERE id=?", (task["id"],)).fetchone()[0])
                # BaseException and infrastructure failures leave running intent
                # for replay. They never create a new execution or run identity.
                outcome = self.executor.execute(workflow, execution | {
                    "principal": task["principal"], "workflow": task["workflow"]},
                    cancelled=cancelled, ownership_descriptor=descriptor)
                if not isinstance(outcome, ExecutionResult) or outcome.state not in STATES - {"queued", "running"}:
                    raise ServiceError("invalid_executor_result")
                output = _object(outcome.result) if outcome.result is not None else None
                error = _object(outcome.error) if outcome.error is not None else None
                with self.store.transaction() as db:
                    # Cancellation can arrive after the executor returns. A
                    # paused revision has stopped and can safely lose its future
                    # continuation, but an uncertain effect remains unknown.
                    requested = db.execute("SELECT cancel_requested FROM tasks WHERE id=?", (task["id"],)).fetchone()[0]
                    state = "cancelled" if requested and outcome.state in INTERRUPTED else outcome.state
                    for item in outcome.events:
                        event(db, task["id"], "protocol", _object(item))
                    db.execute("UPDATE tasks SET state=?,result=?,error=?,updated_at=? WHERE id=?", (
                        state, canonical(output) if output is not None else None,
                        canonical(error) if error is not None else None, now(), task["id"]))
                    db.execute("UPDATE executions SET state=? WHERE sequence=?", (state, execution["sequence"]))
                    event(db, task["id"], "state_changed", {"state": state, "revision": task["revision"]})
                    return self._receipt(decoded(db.execute("SELECT * FROM tasks WHERE id=?", (task["id"],)).fetchone()))
        finally:
            # close (rather than LOCK_UN) preserves the inherited lock until a
            # surviving guardian releases its copy after stopping descendants.
            os.close(descriptor)
