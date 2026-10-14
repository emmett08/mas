"""Versioned durable composition records, distinct from Rust task state."""
from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
import time

from .codec import integer_json
from .schema import Schema, SchemaError, connect


COMPOSITION = Schema('workflow composition', 0x4D415357, 1, (), (
    'CREATE TABLE instances(id TEXT PRIMARY KEY,principal TEXT NOT NULL,request_key TEXT NOT NULL,plan TEXT NOT NULL,identity TEXT NOT NULL,input TEXT NOT NULL,state TEXT NOT NULL,output TEXT,error TEXT,created INTEGER NOT NULL,deadline INTEGER NOT NULL,cancel_requested INTEGER NOT NULL DEFAULT 0,UNIQUE(principal,request_key))',
    'CREATE TABLE nodes(instance TEXT NOT NULL,path TEXT NOT NULL,kind TEXT NOT NULL,input TEXT NOT NULL,state TEXT NOT NULL,output TEXT,error TEXT,PRIMARY KEY(instance,path))',
    'CREATE TABLE attempts(instance TEXT NOT NULL,path TEXT NOT NULL,revision INTEGER NOT NULL,workflow TEXT NOT NULL,identity TEXT NOT NULL,run_directory TEXT UNIQUE NOT NULL,input TEXT NOT NULL,wall_seconds INTEGER NOT NULL,state TEXT NOT NULL,output TEXT,error TEXT,PRIMARY KEY(instance,path,revision))',
    'CREATE TABLE continuations(instance TEXT NOT NULL,request_key TEXT NOT NULL,path TEXT NOT NULL,input TEXT NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(instance,request_key))',
    'CREATE TABLE allocations(instance TEXT NOT NULL,run_id TEXT NOT NULL,call_id TEXT NOT NULL,amount INTEGER NOT NULL,PRIMARY KEY(instance,run_id,call_id))',
    'CREATE TABLE scopes(instance TEXT NOT NULL,path TEXT NOT NULL,limits TEXT NOT NULL,deadline INTEGER NOT NULL,PRIMARY KEY(instance,path))',
    'CREATE TABLE events(id INTEGER PRIMARY KEY AUTOINCREMENT,instance TEXT NOT NULL,path TEXT,event TEXT NOT NULL,state TEXT NOT NULL,created INTEGER NOT NULL)',
))


def encode(value):
    integer_json(value)
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False)


def decode(row):
    if row is None:
        return None
    result = dict(row)
    for key in ('input', 'output', 'error'):
        if key in result and result[key] is not None:
            result[key] = json.loads(result[key])
    return result


def event(db, instance, path, name, state):
    db.execute('INSERT INTO events(instance,path,event,state,created) VALUES(?,?,?,?,?)',
               (instance, path, name, state, int(time.time())))


class WorkflowStore:
    """One connection per transaction; no SQLite owner crosses a process fork."""
    def __init__(self, state_dir: Path, *, readonly=False):
        self.path = Path(state_dir) / 'composition.sqlite'
        self.readonly = readonly
        if readonly:
            db = connect(self.path, COMPOSITION, readonly=True)
            db.close()
            return
        from .operations import _file_lock
        with _file_lock(Path(state_dir) / '.composition-init.lock', True):
            db = connect(self.path, COMPOSITION, initialise=lambda _db: None)
            db.close()

    @contextmanager
    def transaction(self, *, readonly=False):
        if self.readonly and not readonly:
            raise SchemaError('workflow composition was opened read-only')
        db = connect(self.path, COMPOSITION, readonly=readonly)
        try:
            if not readonly:
                db.execute('BEGIN IMMEDIATE')
            yield db
            if not readonly:
                db.commit()
        except BaseException:
            if not readonly:
                db.rollback()
            raise
        finally:
            db.close()


def holds_run(state_dir: Path, run_directory: str) -> bool:
    """Conservative retention: composition history retains all referenced runs.

    A future explicit workflow-history pruning operation may remove these
    references. Ordinary run pruning must not invalidate resumable or inspectable
    composition evidence, including completed child checkpoints.
    """
    path = Path(state_dir) / 'composition.sqlite'
    if not path.exists():
        return False
    db = connect(path, COMPOSITION, readonly=True)
    try:
        return db.execute('SELECT 1 FROM attempts WHERE run_directory=? LIMIT 1',
                          (run_directory,)).fetchone() is not None
    finally:
        db.close()
