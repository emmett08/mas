"""Versioned durable composition records, distinct from Rust task state."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import math
from pathlib import Path
import time
import uuid

from .codec import canonical_json
from .contracts import RunnerError
from .schema import Schema, SchemaError, connect
from .service_contracts import ServiceError
from .workflow_evaluator import FINISHED_STATES


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
    return canonical_json(value)


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
    """Composition journal and atomic transitions, without execution authority.

    Callers authorise owners and resolve workflow bindings before writing. Each
    transaction owns its connection, so no SQLite owner crosses a process fork.
    Read transactions retain one snapshot across related observations.
    """
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
            db.execute('BEGIN' if readonly else 'BEGIN IMMEDIATE')
            yield db
            if not readonly:
                db.commit()
        except BaseException:
            if not readonly:
                db.rollback()
            raise
        finally:
            db.close()

    @staticmethod
    def instance(db, instance, principal=None, *, include_input=False):
        columns = 'id,principal,request_key,plan,identity,state,output,error,created,deadline,cancel_requested'
        if include_input:
            columns += ',input'
        query, values = f'SELECT {columns} FROM instances WHERE id=?', (instance,)
        if principal is not None:
            query += ' AND principal=?'
            values += (principal,)
        return decode(db.execute(query, values).fetchone())

    def read_instance(self, instance):
        with self.transaction(readonly=True) as db:
            return self.instance(db, instance, include_input=True)

    @staticmethod
    def submitted(db, principal, request_key):
        return decode(db.execute('SELECT * FROM instances WHERE principal=? AND request_key=?',
                                 (principal, request_key)).fetchone())

    @staticmethod
    def create_instance(db, principal, request_key, plan, identity, input, *, wall_seconds, maximum):
        if db.execute('SELECT count(*) FROM instances').fetchone()[0] >= maximum:
            raise ServiceError('workflow_limit')
        instance, now = 'flow-' + uuid.uuid4().hex, int(time.time())
        db.execute("INSERT INTO instances(id,principal,request_key,plan,identity,input,state,created,deadline) "
                   "VALUES(?,?,?,?,?,?,'queued',?,?)",
                   (instance, principal, request_key, plan, identity, encode(input), now, now + wall_seconds))
        event(db, instance, None, 'submitted', 'queued')
        return instance

    @staticmethod
    def graph(db, item):
        instance = item['id']
        # Project before decoding: retained payloads can be much larger than
        # this graph, especially across many continuation revisions.
        nodes = [decode(row) for row in db.execute(
            'SELECT path,kind,state,error FROM nodes WHERE instance=? ORDER BY path', (instance,))]
        attempts = [decode(row) for row in db.execute(
            'SELECT path,revision,run_directory,state,error FROM attempts WHERE instance=? ORDER BY path,revision',
            (instance,))]
        usage = db.execute('SELECT count(*),coalesce(sum(amount),0) FROM allocations WHERE instance=?',
                           (instance,)).fetchone()
        return {'instance_id': instance, 'plan': item['plan'], 'identity': item['identity'],
                'state': item['state'], 'output': item['output'], 'error': item['error'],
                'deadline': item['deadline'], 'cancel_requested': bool(item['cancel_requested']),
                'usage': {'steps': len(nodes), 'calls': usage[0], 'reserved_microusd': usage[1]},
                'nodes': nodes, 'attempts': attempts}

    @staticmethod
    def events(db, instance, after, limit):
        return [dict(row) for row in db.execute(
            'SELECT * FROM events WHERE instance=? AND id>? ORDER BY id LIMIT ?', (instance, after, limit))]

    @staticmethod
    def task_detail(db, instance, path, revision=None):
        columns = 'path,revision,workflow,identity,run_directory,state,output,error'
        query, values = f'SELECT {columns} FROM attempts WHERE instance=? AND path=?', (instance, path)
        if revision is not None:
            query += ' AND revision=?'
            values += (revision,)
        return decode(db.execute(query + ' ORDER BY revision DESC LIMIT 1', values).fetchone())

    @staticmethod
    def latest_attempt(db, instance, path):
        return decode(db.execute('SELECT * FROM attempts WHERE instance=? AND path=? '
                                 'ORDER BY revision DESC LIMIT 1', (instance, path)).fetchone())

    @classmethod
    def request_cancellation(cls, db, item):
        if item['state'] in FINISHED_STATES:
            return
        instance, state = item['id'], item['state']
        db.execute('UPDATE instances SET cancel_requested=1 WHERE id=?', (instance,))
        # A running owner must classify and drain its workers first. Unknown
        # effects survive cancellation, even though future local work closes.
        if state in {'queued', 'input_required', 'auth_required', 'unknown'}:
            cls.close_stopped_work(db, instance)
            if state != 'unknown':
                state = 'cancelled'
                db.execute('UPDATE instances SET state=?,output=NULL,error=? WHERE id=?',
                           (state, encode({'code': state}), instance))
        if not item['cancel_requested']:
            event(db, instance, None, 'cancellation_requested', state)
        elif state != item['state']:
            event(db, instance, None, 'state_changed', state)

    def node(self, instance, path, kind, input, limits):
        with self.transaction() as db:
            row = decode(db.execute('SELECT * FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone())
            if row is not None:
                if row['kind'] != kind or encode(row['input']) != encode(input):
                    raise ServiceError('workflow_replay_conflict')
                return row
            if db.execute('SELECT count(*) FROM nodes WHERE instance=?', (instance,)).fetchone()[0] >= limits['max_steps']:
                raise ServiceError('workflow_step_limit')
            db.execute("INSERT INTO nodes(instance,path,kind,input,state) VALUES(?,?,?,?,'waiting')",
                       (instance, path, kind, encode(input)))
            event(db, instance, path, 'node_created', 'waiting')
            return {'state': 'waiting', 'output': None, 'error': None}

    def finish_node(self, instance, path, state, output=None, error=None):
        try:
            source = encode(output) if output is not None else None
        except RunnerError:
            state, source, output = 'failed', None, None
            error = {'code': 'composed_output_not_encodable'}
        with self.transaction() as db:
            old = db.execute('SELECT state FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone()
            db.execute('UPDATE nodes SET state=?,output=?,error=? WHERE instance=? AND path=?',
                       (state, source, encode(error) if error else None, instance, path))
            if old['state'] != state:
                event(db, instance, path, 'node_state', state)
        return state, output

    def scopes(self, instance, path):
        with self.transaction(readonly=True) as db:
            return [{'path': row['path'], 'limits': json.loads(row['limits']), 'deadline': row['deadline']}
                    for row in db.execute('SELECT * FROM scopes WHERE instance=? ORDER BY path', (instance,))
                    if path == row['path'] or path.startswith(row['path'] + '/')]

    def scope(self, instance, path, limits, deadline):
        with self.transaction() as db:
            row = db.execute('SELECT * FROM scopes WHERE instance=? AND path=?', (instance, path)).fetchone()
            if row is None:
                bound = min(deadline, int(time.time()) + limits['wall_seconds'])
                db.execute('INSERT INTO scopes VALUES(?,?,?,?)', (instance, path, encode(limits), bound))
                return bound
            if row['limits'] != encode(limits):
                raise ServiceError('workflow_scope_conflict')
            return row['deadline']

    @classmethod
    def assign_attempt(cls, db, instance, path, workflow, identity, revision, input, *, deadline, wall_seconds):
        """Persist either an initial assignment or an explicit continuation."""
        directory = 'run-flow-' + hashlib.sha256(f'{instance}:{path}:{revision}'.encode()).hexdigest()[:40]
        seconds = min(wall_seconds, max(1, math.ceil(deadline - time.time())))
        db.execute('INSERT INTO attempts(instance,path,revision,workflow,identity,run_directory,input,wall_seconds,state) '
                   "VALUES(?,?,?,?,?,?,?,?,'queued')",
                   (instance, path, revision, workflow, identity, directory, encode(input), seconds))
        return cls.latest_attempt(db, instance, path)

    def attempt(self, instance, path, workflow, input, deadline, *, identity, wall_seconds):
        with self.transaction() as db:
            attempt = self.latest_attempt(db, instance, path)
            if attempt is None:
                if deadline <= time.time():
                    return None
                attempt = self.assign_attempt(db, instance, path, workflow, identity, 0, input,
                                              deadline=deadline, wall_seconds=wall_seconds)
            if attempt['workflow'] != workflow or attempt['identity'] != identity:
                raise ServiceError('workflow_changed')
            return attempt

    def save_attempt(self, attempt, state, output=None, error=None):
        with self.transaction() as db:
            db.execute('UPDATE attempts SET state=?,output=?,error=? WHERE instance=? AND path=? AND revision=? AND identity=?',
                       (state, encode(output) if output is not None else None, encode(error) if error else None,
                        attempt['instance'], attempt['path'], attempt['revision'], attempt['identity']))
            event(db, attempt['instance'], attempt['path'], 'task_state', state)

    def attempt_state(self, attempt):
        with self.transaction(readonly=True) as db:
            return db.execute('SELECT state FROM attempts WHERE instance=? AND path=? AND revision=?',
                              (attempt['instance'], attempt['path'], attempt['revision'])).fetchone()[0]

    def pending_attempts(self, instance):
        """Assignments that need evidence checks before a new owner may run."""
        with self.transaction(readonly=True) as db:
            return [dict(row) for row in db.execute(
                "SELECT instance,path,revision,identity,run_directory,state FROM attempts "
                "WHERE instance=? AND state IN ('running','queued')", (instance,))]

    def start_instance(self, instance, reconciled):
        """Apply the owner's evidence decisions before exposing running state.

        ``reconciled`` contains exact assignments and whether they remain
        uncertain. This journal does not inspect external-effect evidence.
        """
        with self.transaction() as db:
            for attempt, uncertain in reconciled:
                state = 'unknown' if uncertain else 'queued'
                db.execute("UPDATE attempts SET state=?,error=? WHERE instance=? AND path=? AND revision=? "
                           "AND identity=? AND state IN ('running','queued')",
                           (state, encode({'code': 'reconciliation_required'}) if uncertain else None,
                            instance, attempt['path'], attempt['revision'], attempt['identity']))
                if state != attempt['state']:
                    event(db, instance, attempt['path'], 'task_state', state)
            db.execute("UPDATE instances SET state='running' WHERE id=?", (instance,))
            event(db, instance, None, 'running', 'running')

    def finish_instance(self, instance, state, output, *, expired):
        with self.transaction() as db:
            # Cancellation may commit after the scheduler's last observation.
            cancelled = db.execute('SELECT cancel_requested FROM instances WHERE id=?', (instance,)).fetchone()[0]
            uncertain = db.execute("SELECT 1 FROM attempts WHERE instance=? AND state='unknown' LIMIT 1",
                                   (instance,)).fetchone()
            final = 'unknown' if uncertain else 'expired' if expired else 'cancelled' if cancelled else state
            db.execute('UPDATE instances SET state=?,output=?,error=? WHERE id=?',
                       (final, encode(output) if final == 'completed' else None,
                        encode({'code': final}) if final != 'completed' else None, instance))
            if cancelled or final in {'failed', 'cancelled', 'expired'}:
                self.close_stopped_work(db, instance)
            event(db, instance, None, 'state_changed', final)

    @staticmethod
    def continuation(db, instance, request_key):
        return db.execute('SELECT * FROM continuations WHERE instance=? AND request_key=?',
                          (instance, request_key)).fetchone()

    @classmethod
    def continue_attempt(cls, db, item, attempt, request_key, supplied, input, *, wall_seconds):
        revision = attempt['revision'] + 1
        cls.assign_attempt(db, item['id'], attempt['path'], attempt['workflow'], attempt['identity'],
                           revision, input, deadline=item['deadline'], wall_seconds=wall_seconds)
        db.execute('INSERT INTO continuations VALUES(?,?,?,?,?)',
                   (item['id'], request_key, attempt['path'], encode(supplied), revision))
        db.execute("UPDATE instances SET state='queued',error=NULL WHERE id=?", (item['id'],))
        event(db, item['id'], attempt['path'], 'continued', 'queued')

    @staticmethod
    def unknown_attempts(db, instance):
        return [dict(row) for row in db.execute(
            "SELECT path,revision,run_directory FROM attempts WHERE instance=? AND state='unknown'", (instance,))]

    @staticmethod
    def requeue_unknown(db, instance):
        db.execute("UPDATE attempts SET state='queued',error=NULL WHERE instance=? AND state='unknown'", (instance,))
        db.execute("UPDATE instances SET state='queued',error=NULL WHERE id=?", (instance,))
        event(db, instance, None, 'recovery_queued', 'queued')

    @staticmethod
    def close_stopped_work(db, instance):
        """Close future local work only after the instance's workers have drained.

        Historical revisions and completed or uncertain effects retain their
        evidence. A continuation's latest revision is the only actionable one.
        """
        latest = ('SELECT max(other.revision) FROM attempts other '
                  'WHERE other.instance=attempts.instance AND other.path=attempts.path')
        for row in db.execute('SELECT path,revision FROM attempts WHERE instance=? '
                              "AND state IN ('queued','input_required','auth_required') "
                              f'AND revision=({latest})', (instance,)).fetchall():
            db.execute("UPDATE attempts SET state='cancelled' WHERE instance=? AND path=? AND revision=?",
                       (instance, row['path'], row['revision']))
            event(db, instance, row['path'], 'task_state', 'cancelled')
        # Reconcile the visible graph with closed attempts. Do not conceal a
        # running or unknown descendant beneath a cancelled control node.
        for row in db.execute("SELECT path,kind FROM nodes WHERE instance=? AND state IN "
                              "('waiting','queued','running','input_required','auth_required','unknown')", (instance,)).fetchall():
            path = row['path']
            if row['kind'] == 'task':
                attempt = db.execute('SELECT state FROM attempts WHERE instance=? AND path=? '
                                     'ORDER BY revision DESC LIMIT 1', (instance, path)).fetchone()
                if attempt is None or attempt['state'] != 'cancelled':
                    continue
            elif db.execute("SELECT 1 FROM attempts WHERE instance=? AND state IN ('running','unknown') "
                            'AND (path=? OR substr(path,1,?)=?) LIMIT 1',
                            (instance, path, len(path) + 1, path + '/')).fetchone():
                continue
            db.execute("UPDATE nodes SET state='cancelled',error=? WHERE instance=? AND path=?",
                       (encode({'code': 'cancelled'}), instance, path))
            event(db, instance, path, 'node_state', 'cancelled')


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
