"""Host workflow checkpoints; Rust remains the task and transport authority.

The journal records intent before effects and results before protocol completion.
It never interprets a missing reply as proof that an external effect did not occur.
"""
from __future__ import annotations

import fcntl
import json
import os
from pathlib import Path
import sqlite3
import time
import uuid


class JournalError(RuntimeError):
    pass


class RunLock:
    """Exclusive process lifetime lock shared by runners and operator mutations."""
    def __init__(self, run_dir: Path):
        descriptor = os.open(Path(run_dir) / ".host.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        self._file = os.fdopen(descriptor, "a+", encoding="utf-8")
        try:
            fcntl.flock(self._file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self._file.close()
            raise JournalError("run is already in use") from None

    def close(self):
        if not self._file.closed:
            fcntl.flock(self._file, fcntl.LOCK_UN)
            self._file.close()

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        self.close()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class Journal:
    def __init__(self, run_dir: Path, metadata: dict | None = None):
        path = Path(run_dir) / "host.sqlite"
        if metadata is None and not path.is_file():
            raise JournalError("run has no recovery journal")
        self.db = sqlite3.connect(path)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA synchronous=FULL")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS steps(key TEXT PRIMARY KEY,kind TEXT NOT NULL,
                input TEXT NOT NULL,data TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS invocations(call_id TEXT PRIMARY KEY,step_key TEXT NOT NULL,
                actor TEXT NOT NULL,state TEXT NOT NULL,output TEXT,evidence TEXT,
                reservation_microusd INTEGER);
            CREATE TABLE IF NOT EXISTS resolutions(id INTEGER PRIMARY KEY,call_id TEXT NOT NULL,
                decision TEXT NOT NULL,evidence TEXT NOT NULL,created INTEGER NOT NULL);
        """)
        if metadata is not None:
            with self.db:
                for key, value in metadata.items():
                    self.db.execute("INSERT INTO metadata VALUES(?,?)", (key, canonical(value)))
        path.chmod(0o600)

    def metadata(self):
        return {row["key"]: json.loads(row["value"]) for row in self.db.execute("SELECT * FROM metadata")}

    def step(self, key: str, kind: str, inputs: dict, initial: dict):
        with self.db:
            row = self.db.execute("SELECT * FROM steps WHERE key=?", (key,)).fetchone()
            if row is not None:
                if row["kind"] != kind or row["input"] != canonical(inputs):
                    raise JournalError("workflow replay differs from durable checkpoint")
                return json.loads(row["data"])
            self.db.execute("INSERT INTO steps VALUES(?,?,?,?)", (key, kind, canonical(inputs), canonical(initial)))
            return initial

    def set_step(self, key, data):
        with self.db:
            self.db.execute("UPDATE steps SET data=? WHERE key=?", (canonical(data), key))

    def get_step(self, key):
        row = self.db.execute("SELECT data FROM steps WHERE key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def child_count(self, request_id):
        prefix = request_id + ":"
        return self.db.execute("SELECT COUNT(*) FROM steps WHERE substr(key,1,?)=?", (len(prefix), prefix)).fetchone()[0]

    def invocation(self, call_id):
        row = self.db.execute("SELECT * FROM invocations WHERE call_id=?", (call_id,)).fetchone()
        if row is None:
            return None
        value = dict(row)
        for key in ("output", "evidence"):
            if value[key] is not None:
                value[key] = json.loads(value[key])
        return value

    def create_invocation(self, step_key, actor, reservation_microusd):
        call_id = "call-" + uuid.uuid4().hex
        with self.db:
            self.db.execute("INSERT INTO invocations VALUES(?,?,?,'prepared',NULL,NULL,?)",
                            (call_id, step_key, actor, reservation_microusd))
            self.db.execute("UPDATE steps SET data=? WHERE key=?", (canonical({"call_id": call_id}), step_key))
        return self.invocation(call_id)

    def update_invocation(self, call_id, state, *, output=None, evidence=None):
        with self.db:
            self.db.execute("UPDATE invocations SET state=?,output=?,evidence=? WHERE call_id=?",
                            (state, canonical(output) if output is not None else None,
                             canonical(evidence) if evidence is not None else None, call_id))

    def close(self):
        self.db.close()


def inspect_run(run_dir: Path) -> dict:
    """Read checkpoint metadata and invocation audit without returning task inputs."""
    path = Path(run_dir).resolve() / "host.sqlite"
    if not path.is_file():
        raise JournalError("run has no recovery journal")
    with sqlite3.connect(path.as_uri() + "?mode=ro", uri=True) as db:
        db.row_factory = sqlite3.Row
        metadata = {row["key"]: json.loads(row["value"]) for row in db.execute("SELECT * FROM metadata")}
        calls = []
        for row in db.execute("SELECT * FROM invocations ORDER BY rowid"):
            item = dict(row)
            for key in ("output", "evidence"):
                if item[key] is not None:
                    item[key] = json.loads(item[key])
            calls.append(item)
        resolutions = [dict(row) for row in db.execute("SELECT * FROM resolutions ORDER BY id")]
        for item in resolutions:
            item["evidence"] = json.loads(item["evidence"])
        return {"run_id": metadata["run_id"], "workflow_version": metadata["workflow_version"],
                "invocations": calls, "resolutions": resolutions,
                "steps": db.execute("SELECT COUNT(*) FROM steps").fetchone()[0],
                "recovery_required": any(item["state"] in ("executing", "unknown") for item in calls)}


def resolve_invocation(run_dir: Path, call_id: str, *, decision: str, evidence: dict,
                       output: dict | None = None) -> dict:
    """Record a trusted operator decision. This never contacts/repeats a provider.

    completed requires independently established output; confirmed_no_effect is
    explicit permission to try this invocation again, with a fresh metered call.
    Neither decision clears or edits the original Rust call record.
    """
    from .runner import _json
    if decision not in ("completed", "confirmed_no_effect") or type(evidence) is not dict or not evidence:
        raise JournalError("resolution requires a supported decision and nonempty evidence")
    if decision == "completed" and type(output) is not dict:
        raise JournalError("completed resolution requires object output")
    if decision == "confirmed_no_effect" and output is not None:
        raise JournalError("confirmed_no_effect cannot supply output")
    _json({"evidence": evidence, "output": output})
    with RunLock(run_dir):
        journal = Journal(run_dir)
        try:
            call = journal.invocation(call_id)
            if call is None or call["state"] not in ("executing", "unknown"):
                raise JournalError("only an uncertain invocation can be resolved")
            with journal.db:
                journal.db.execute("INSERT INTO resolutions(call_id,decision,evidence,created) VALUES(?,?,?,?)",
                                   (call_id, decision, canonical(evidence), int(time.time())))
                journal.db.execute("UPDATE invocations SET state=?,output=?,evidence=? WHERE call_id=?",
                                   ("resolved" if decision == "completed" else "retry_allowed",
                                    canonical(output) if output is not None else None, canonical(evidence), call_id))
        finally:
            journal.close()
    return inspect_run(run_dir)
