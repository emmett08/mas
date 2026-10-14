"""Immutable definition metadata and submission aliases, without executable code.

Legacy identities are retained even when their source is unavailable. A missing
descriptor is explicitly unknown; an exact, hash-matching descriptor can later
complete that record. Registration precedes submission, so an interrupted
registration can leave unused metadata but never an unrecorded new identity.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import time

from .schema import Schema, SchemaError, connect
from .service_contracts import ServiceError


DEFINITIONS = Schema("definition registry", 0x4D415356, 1, (), (
    "CREATE TABLE definitions(kind TEXT NOT NULL,name TEXT NOT NULL,identity TEXT NOT NULL,version TEXT,descriptor TEXT,implementation_digest TEXT,created INTEGER NOT NULL,retired INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(kind,name,identity))",
    "CREATE TABLE aliases(kind TEXT NOT NULL,name TEXT NOT NULL,identity TEXT NOT NULL,PRIMARY KEY(kind,name))",
))


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


class DefinitionRegistry:
    """Durable metadata. Loading a record never imports or invokes an adapter."""
    def __init__(self, state_dir, *, readonly=False):
        from .operations import _file_lock
        self.path = Path(state_dir) / "definition-registry.sqlite"
        self.readonly = readonly
        # Pre-registry backups remain inspectable without writing a migration.
        self.absent = readonly and not self.path.exists()
        if self.absent:
            return
        if readonly:
            with self.transaction(readonly=True):
                pass
            return
        with _file_lock(Path(state_dir) / ".definition-registry-init.lock", True):
            with self.transaction(readonly=readonly, initialise=not readonly):
                pass

    @contextmanager
    def transaction(self, *, readonly=False, initialise=False):
        if self.readonly and not readonly:
            raise ServiceError("audit_only")
        try:
            db = connect(self.path, DEFINITIONS, readonly=readonly,
                         initialise=(lambda _db: None) if initialise else None)
            try:
                db.execute("BEGIN" if readonly else "BEGIN IMMEDIATE")
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

    def remember(self, kind, name, identity, *, descriptor=None, implementation_digest=None,
                 current=False):
        if kind not in {"workflow", "plan", "plan_source"} or not re.fullmatch(r"[0-9a-f]{64}", identity):
            raise ServiceError("invalid_definition")
        source = canonical(descriptor) if descriptor is not None else None
        if source is not None and hashlib.sha256(source.encode()).hexdigest() != identity:
            raise ServiceError("definition_identity_conflict")
        if implementation_digest is not None and (descriptor is None or descriptor.get("implementation_digest") != implementation_digest):
            raise ServiceError("definition_identity_conflict")
        version = descriptor.get("version") if descriptor else None
        if kind == "plan" and descriptor:
            version = descriptor["spec"]["version"]
        with self.transaction() as db:
            old = db.execute("SELECT * FROM definitions WHERE kind=? AND name=? AND identity=?",
                             (kind, name, identity)).fetchone()
            if old is not None:
                if old["retired"] and current:
                    raise ServiceError("definition_retired")
                if source is not None and old["descriptor"] is not None and old["descriptor"] != source:
                    raise ServiceError("definition_identity_conflict")
                if source is not None and old["descriptor"] is None:
                    db.execute("UPDATE definitions SET version=?,descriptor=?,implementation_digest=? "
                               "WHERE kind=? AND name=? AND identity=?",
                               (version, source, implementation_digest, kind, name, identity))
            else:
                db.execute("INSERT INTO definitions(kind,name,identity,version,descriptor,implementation_digest,created) "
                           "VALUES(?,?,?,?,?,?,?)", (kind, name, identity, version, source,
                                                     implementation_digest, int(time.time())))
            if current:
                db.execute("INSERT INTO aliases VALUES(?,?,?) ON CONFLICT(kind,name) DO UPDATE SET identity=excluded.identity",
                           (kind, name, identity))

    def inspect(self, kind, name, identity):
        if self.absent:
            return None
        with self.transaction(readonly=True) as db:
            row = db.execute("SELECT * FROM definitions WHERE kind=? AND name=? AND identity=?",
                             (kind, name, identity)).fetchone()
            if row is None:
                return None
            result = dict(row)
            result["descriptor"] = json.loads(result["descriptor"]) if result["descriptor"] else None
            result["implementation_provenance"] = ("deployment_asserted_digest" if result["implementation_digest"]
                                                    else "not_recorded")
            return result

    def names(self, kind):
        if self.absent:
            return set()
        with self.transaction(readonly=True) as db:
            return {row[0] for row in db.execute("SELECT DISTINCT name FROM definitions WHERE kind=?", (kind,))}

    def current(self, kind, name):
        if self.absent:
            return None
        with self.transaction(readonly=True) as db:
            row = db.execute("SELECT identity FROM aliases WHERE kind=? AND name=?", (kind, name)).fetchone()
            return row[0] if row else None

    def retire(self, kind, name, identity):
        """Called by an owning service after checking all live references."""
        with self.transaction() as db:
            if db.execute("SELECT 1 FROM aliases WHERE kind=? AND name=? AND identity=?",
                          (kind, name, identity)).fetchone():
                raise ServiceError("definition_is_current")
            changed = db.execute("UPDATE definitions SET retired=1 WHERE kind=? AND name=? AND identity=?",
                                 (kind, name, identity)).rowcount
            if not changed:
                raise ServiceError("definition_not_found")
