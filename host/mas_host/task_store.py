"""Version-aware repository for protocol-neutral submissions and task events.

Every transaction is short. Execution ownership is a separate process lock;
SQLite connections are never retained across a provider call or process fork.
Run directories are relative to the deployment, so audit restores cannot refer
back to their source deployment.
"""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import time

from .schema import Schema, SchemaError, connect
from .service_contracts import ServiceError


SERVICE = Schema("task service", 0x4D415354, 1, (), (
    "CREATE TABLE metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)",
    "CREATE TABLE workflows(name TEXT PRIMARY KEY,identity TEXT NOT NULL)",
    "CREATE TABLE tasks(id TEXT PRIMARY KEY,principal TEXT NOT NULL,request_key TEXT NOT NULL,workflow TEXT NOT NULL,workflow_identity TEXT NOT NULL,state TEXT NOT NULL,input TEXT NOT NULL,metadata TEXT NOT NULL,result TEXT,error TEXT,cancel_requested INTEGER NOT NULL DEFAULT 0,revision INTEGER NOT NULL DEFAULT 0,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL)",
    "CREATE TABLE executions(sequence INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,revision INTEGER NOT NULL,run_id TEXT UNIQUE NOT NULL,run_directory TEXT UNIQUE NOT NULL,input TEXT NOT NULL,state TEXT NOT NULL,UNIQUE(task_id,revision))",
    "CREATE TABLE requests(principal TEXT NOT NULL,request_key TEXT NOT NULL,digest TEXT NOT NULL,task_id TEXT NOT NULL,kind TEXT NOT NULL,PRIMARY KEY(principal,request_key))",
    "CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,task_id TEXT NOT NULL,event TEXT NOT NULL,data TEXT NOT NULL,created_at INTEGER NOT NULL)",
    "CREATE INDEX task_owner ON tasks(principal,created_at,id)",
    "CREATE INDEX execution_queue ON executions(state,sequence)",
    "CREATE INDEX task_events ON events(task_id,id)",
))


def canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def now() -> int:
    return int(time.time() * 1000)


def event(db, task_id: str, kind: str, data: dict) -> None:
    db.execute("INSERT INTO events(task_id,event,data,created_at) VALUES(?,?,?,?)",
               (task_id, kind, canonical(data), now()))


def decoded(row) -> dict:
    result = dict(row)
    for field in ("input", "result", "error", "data", "metadata"):
        if field in result and result[field] is not None:
            result[field] = json.loads(result[field])
    if "cancel_requested" in result:
        result["cancel_requested"] = bool(result["cancel_requested"])
    return result


class TaskStore:
    def __init__(self, state_dir: Path, *, deployment_id: str, readonly: bool = False):
        from .operations import _file_lock
        self.path = Path(state_dir) / "task-service.sqlite"
        self.deployment_id = deployment_id

        def initialise(db):
            db.execute("INSERT INTO metadata VALUES('deployment_id',?)", (deployment_id,))

        try:
            with _file_lock(Path(state_dir) / ".task-service-init.lock", True):
                db = connect(self.path, SERVICE, readonly=readonly,
                             initialise=None if readonly else initialise,
                             validate_content=self._validate)
                db.close()
        except SchemaError as error:
            raise ServiceError("unsupported_schema", str(error)) from None

    def _validate(self, db):
        values = dict(db.execute("SELECT key,value FROM metadata"))
        if values != {"deployment_id": self.deployment_id}:
            raise ServiceError("deployment_mismatch")

    @contextmanager
    def transaction(self, *, readonly: bool = False):
        try:
            db = connect(self.path, SERVICE, readonly=readonly, validate_content=self._validate)
            try:
                if not readonly:
                    db.execute("BEGIN IMMEDIATE")
                yield db
                if not readonly:
                    db.commit()
            except BaseException:
                if not readonly:
                    db.rollback()
                raise
            finally:
                db.close()
        except SchemaError as error:
            raise ServiceError("unsupported_schema", str(error)) from None
