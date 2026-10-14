"""Durable bounded composition of independently owned Rust-backed workflows."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import replace
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import select
import time
import uuid

from .operations import DeploymentLedger, _file_lock
from .contracts import RunnerError
from .service_contracts import ServiceError
from .service_mutation import service_mutation
from .task_service import RunnerExecutor, _object, _text
from .workflow_allocation import WorkflowAllocation
from .workflow_plan import WorkflowPlan, bind_plan, validate_registry
from .workflow_evaluator import PlanEvaluator, FINISHED_STATES as _FINISHED, PAUSED_STATES as _PAUSED
from .workflow_inspection import encode_task_inspection, validate_task_selector
from .workflow_store import WorkflowStore, decode, encode, event


class WorkflowEngine:
    """Trusted Linux scheduler; callbacks/providers remain in supervised adapters.

    Each task node gets one stable Rust run per explicit continuation revision.
    Plans sequence and join results; they never bypass Rust message admission.
    A run owns a process lock inherited by all child guardians. A parent lifetime
    pipe stops admission after scheduler death without relying on PID reuse.
    """
    def __init__(self, state_dir, plans, workflows, grants, *, operations=None,
                 max_instances=1000, mas_bin='mas', gateway_bin='mas-gateway'):
        self.state_dir = Path(state_dir).absolute()
        self.operations = operations or DeploymentLedger(self.state_dir)
        if Path(self.operations.state_dir).absolute() != self.state_dir:
            raise ServiceError('deployment_mismatch')
        if type(max_instances) is not int or not 1 <= max_instances <= 100000:
            raise ServiceError('invalid_instance_limit')
        self.max_instances = max_instances
        self.plans = {name: plan if isinstance(plan, WorkflowPlan) else WorkflowPlan(plan)
                      for name, plan in plans.items()}
        # Copy mutable topology mappings; adapter implementations are trusted
        # registrations covered by their explicit implementation version.
        self.workflows = {name: replace(workflow, topology=json.loads(encode(workflow.topology)))
                          for name, workflow in workflows.items()}
        validate_registry(self.plans, self.workflows)
        self.bindings = {name: bind_plan(name, self.plans, self.workflows) for name in self.plans}
        self.evaluator = PlanEvaluator(self.plans, self)
        self.grants = {}
        for principal, names in grants.items():
            _text(principal, 'principal')
            if isinstance(names, str) or not set(names) <= set(self.plans):
                raise ServiceError('invalid_grants')
            self.grants[principal] = frozenset(names)
        self.options = {'mas_bin': mas_bin, 'gateway_bin': gateway_bin}
        ownership = (nullcontext() if self.operations.readonly else
                     _file_lock(self.state_dir / '.deployment.lock', False))
        with ownership:
            active = not self.operations.readonly and self.operations.admission_enabled
            if not active and not (self.state_dir / 'composition.sqlite').exists():
                raise ServiceError('audit_only')
            self.store = WorkflowStore(self.state_dir, readonly=not active)

    def _mutation(self):
        return service_mutation(self.operations, self.store)

    def _owned(self, db, principal, instance):
        row = db.execute('SELECT * FROM instances WHERE id=? AND principal=?', (instance, principal)).fetchone()
        if row is None:
            raise ServiceError('workflow_not_found')
        item = decode(row)
        if item['plan'] not in self.grants.get(principal, ()):
            raise ServiceError('forbidden')
        if bind_plan(item['plan'], self.plans, self.workflows)['identity'] != item['identity']:
            raise ServiceError('workflow_changed')
        return item

    def submit(self, principal, request_key, plan, input):
        _text(request_key, 'request_key')
        if plan not in self.grants.get(principal, ()):
            raise ServiceError('forbidden')
        if bind_plan(plan, self.plans, self.workflows)['identity'] != self.bindings[plan]['identity']:
            raise ServiceError('workflow_changed')
        value = _object(input)
        with self._mutation() as db:
            old = db.execute('SELECT * FROM instances WHERE principal=? AND request_key=?',
                             (principal, request_key)).fetchone()
            if old is not None:
                row = decode(old)
                if (row['plan'] != plan or encode(row['input']) != encode(value)
                        or row['identity'] != self.bindings[plan]['identity']):
                    raise ServiceError('request_conflict')
                instance = row['id']
            else:
                self.operations.check_storage_admission()
                if db.execute('SELECT count(*) FROM instances').fetchone()[0] >= self.max_instances:
                    raise ServiceError('workflow_limit')
                instance, now = 'flow-' + uuid.uuid4().hex, int(time.time())
                deadline = now + self.plans[plan].spec['limits']['wall_seconds']
                db.execute('INSERT INTO instances(id,principal,request_key,plan,identity,input,state,created,deadline) VALUES(?,?,?,?,?,?,\'queued\',?,?)',
                           (instance, principal, request_key, plan, self.bindings[plan]['identity'], encode(value), now, deadline))
                event(db, instance, None, 'submitted', 'queued')
        return self.inspect(principal, instance)

    def inspect(self, principal, instance):
        with self.store.transaction(readonly=True) as db:
            item = self._owned(db, principal, instance)
            nodes = [decode(row) for row in db.execute('SELECT * FROM nodes WHERE instance=? ORDER BY path', (instance,))]
            attempts = [decode(row) for row in db.execute('SELECT * FROM attempts WHERE instance=? ORDER BY path,revision', (instance,))]
            usage = db.execute('SELECT count(*),coalesce(sum(amount),0) FROM allocations WHERE instance=?', (instance,)).fetchone()
            # Do not expose inputs, outputs or credentials in the graph view.
            return {'instance_id': instance, 'plan': item['plan'], 'identity': item['identity'],
                    'state': item['state'], 'output': item['output'], 'error': item['error'],
                    'deadline': item['deadline'], 'cancel_requested': bool(item['cancel_requested']),
                    'usage': {'steps': len(nodes), 'calls': usage[0], 'reserved_microusd': usage[1]},
                    'nodes': [{k: row[k] for k in ('path', 'kind', 'state', 'error')} for row in nodes],
                    'attempts': [{k: row[k] for k in ('path', 'revision', 'run_directory', 'state', 'error')} for row in attempts]}

    def events(self, principal, instance, after=0, limit=100):
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ServiceError('invalid_cursor')
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
            return [dict(row) for row in db.execute('SELECT * FROM events WHERE instance=? AND id>? ORDER BY id LIMIT ?',
                                                   (instance, after, limit))]

    def inspect_task(self, principal, instance, path, revision=None):
        """Return one owned leaf revision; omitted revision selects the latest."""
        validate_task_selector(path, revision)
        with self.store.transaction(readonly=True) as db:
            # SELECT alone does not start a SQLite snapshot transaction. Keep
            # ownership, parent state and the selected revision in one snapshot.
            db.execute('BEGIN')
            item = self._owned(db, principal, instance)
            columns = 'path,revision,workflow,identity,run_directory,state,output,error'
            query = f'SELECT {columns} FROM attempts WHERE instance=? AND path=?'
            arguments = (instance, path)
            if revision is not None:
                query += ' AND revision=?'
                arguments += (revision,)
            attempt = decode(db.execute(query + ' ORDER BY revision DESC LIMIT 1', arguments).fetchone())
            if attempt is None:
                raise ServiceError('workflow_task_not_found')
            workflow = self.workflows.get(attempt['workflow'])
            if workflow is None or workflow.identity != attempt['identity']:
                raise ServiceError('workflow_changed')
            detail = {'instance_id': instance, 'plan': item['plan'],
                      'instance_state': item['state'], 'deadline': item['deadline'], **attempt}
            encode_task_inspection(detail)
            return detail

    def cancel(self, principal, instance):
        with self._mutation() as db:
            item = self._owned(db, principal, instance)
            if item['state'] not in _FINISHED:
                db.execute('UPDATE instances SET cancel_requested=1 WHERE id=?', (instance,))
                # Unknown effects survive cancellation; the scheduler observes
                # running workers and records the final local state.
                state = item['state']
                if state in {'queued', 'input_required', 'auth_required', 'unknown'}:
                    self.store.close_stopped_work(db, instance)
                    if state != 'unknown':
                        state = 'cancelled'
                        db.execute('UPDATE instances SET state=?,output=NULL,error=? WHERE id=?',
                                   (state, encode({'code': state}), instance))
                if not item['cancel_requested']:
                    event(db, instance, None, 'cancellation_requested', state)
                elif state != item['state']:
                    event(db, instance, None, 'state_changed', state)
        return self.inspect(principal, instance)

    @contextmanager
    def _ownership(self, instance):
        if type(instance) is not str or re.fullmatch(r'flow-[0-9a-f]{32}', instance) is None:
            raise ServiceError('invalid_workflow_identity')
        # Close-only is deliberate: LOCK_UN would release a surviving guardian's
        # inherited ownership after an interrupted scheduler exits.
        fd = os.open(self.state_dir / ('.' + instance + '.lock'), os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise ServiceError('workflow_busy') from None
            yield fd
        finally:
            os.close(fd)

    def _instance(self, instance):
        with self.store.transaction(readonly=True) as db:
            return decode(db.execute('SELECT * FROM instances WHERE id=?', (instance,)).fetchone())

    def _node(self, instance, path, kind, input, limits):
        with self.store.transaction() as db:
            row = decode(db.execute('SELECT * FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone())
            if row is not None:
                if row['kind'] != kind or encode(row['input']) != encode(input):
                    raise ServiceError('workflow_replay_conflict')
                return row
            if db.execute('SELECT count(*) FROM nodes WHERE instance=?', (instance,)).fetchone()[0] >= limits['max_steps']:
                raise ServiceError('workflow_step_limit')
            db.execute('INSERT INTO nodes(instance,path,kind,input,state) VALUES(?,?,?,?,\'waiting\')',
                       (instance, path, kind, encode(input)))
            event(db, instance, path, 'node_created', 'waiting')
            return {'state': 'waiting', 'output': None, 'error': None}

    def _finish_node(self, instance, path, state, output=None, error=None):
        try:
            source = encode(output) if output is not None else None
        except RunnerError:
            state, source, output = 'failed', None, None
            error = {'code': 'composed_output_not_encodable'}
        with self.store.transaction() as db:
            old = db.execute('SELECT state FROM nodes WHERE instance=? AND path=?', (instance, path)).fetchone()
            db.execute('UPDATE nodes SET state=?,output=?,error=? WHERE instance=? AND path=?',
                       (state, source, encode(error) if error else None, instance, path))
            if old['state'] != state:
                event(db, instance, path, 'node_state', state)
        return state, output

    def _scopes(self, instance, path):
        with self.store.transaction(readonly=True) as db:
            return [{'path': row['path'], 'limits': json.loads(row['limits']), 'deadline': row['deadline']}
                    for row in db.execute('SELECT * FROM scopes WHERE instance=? ORDER BY path', (instance,))
                    if path == row['path'] or path.startswith(row['path']+'/')]

    def _scope(self, instance, path, limits, deadline):
        with self.store.transaction() as db:
            scope = db.execute('SELECT * FROM scopes WHERE instance=? AND path=?', (instance, path)).fetchone()
            if scope is None:
                bound = min(deadline, int(time.time()) + limits['wall_seconds'])
                db.execute('INSERT INTO scopes VALUES(?,?,?,?)', (instance, path, encode(limits), bound))
                return bound
            if scope['limits'] != encode(limits):
                raise ServiceError('workflow_scope_conflict')
            return scope['deadline']

    def _attempt(self, instance, path, workflow, input, deadline):
        with self.store.transaction() as db:
            attempt = decode(db.execute('SELECT * FROM attempts WHERE instance=? AND path=? ORDER BY revision DESC LIMIT 1',
                                        (instance, path)).fetchone())
            if attempt is None:
                if deadline <= time.time():
                    return None
                leaf = self.workflows[workflow]
                directory = 'run-flow-' + hashlib.sha256((instance + ':' + path + ':0').encode()).hexdigest()[:40]
                seconds = min(leaf.topology.get('wall_seconds', 3600), max(1, math.ceil(deadline - time.time())))
                db.execute('INSERT INTO attempts(instance,path,revision,workflow,identity,run_directory,input,wall_seconds,state) VALUES(?,?,0,?,?,?,?,?,\'queued\')',
                           (instance, path, workflow, leaf.identity, directory, encode(input), seconds))
                attempt = decode(db.execute('SELECT * FROM attempts WHERE instance=? AND path=?', (instance, path)).fetchone())
            if attempt['workflow'] != workflow or attempt['identity'] != self.workflows[workflow].identity:
                raise ServiceError('workflow_changed')
            return attempt

    def _advance(self, instance, path, node, input, limits, deadline, ready):
        return self.evaluator.advance(instance, path, node, input, limits, deadline, ready)

    def _save_attempt(self, attempt, state, output=None, error=None):
        with self.store.transaction() as db:
            db.execute('UPDATE attempts SET state=?,output=?,error=? WHERE instance=? AND path=? AND revision=? AND identity=?',
                       (state, encode(output) if output is not None else None, encode(error) if error else None,
                        attempt['instance'], attempt['path'], attempt['revision'], attempt['identity']))
            event(db, attempt['instance'], attempt['path'], 'task_state', state)

    def _worker(self, attempt, item, limits, lock_fd, lifetime_fd, monotonic_deadline):
        def stopped():
            if select.select([lifetime_fd], [], [], 0)[0]:
                return True  # EOF: scheduler lifetime ended; nobody writes bytes.
            current = self._instance(item['id'])
            return (current['cancel_requested'] or time.monotonic() >= monotonic_deadline
                    or current['plan'] not in self.grants.get(current['principal'], ()))
        try:
            scopes = self._scopes(item['id'], attempt['path'])
            monotonic_deadline = min([monotonic_deadline, *[
                time.monotonic() + max(0, scope['deadline'] - time.time()) for scope in scopes]])
            allocation = WorkflowAllocation(self.operations, self.store, item['id'], limits, stopped,
                                            monotonic_deadline, scopes=scopes)
            executor = RunnerExecutor(self.state_dir, operations=allocation, **self.options)
            original = self.workflows[attempt['workflow']]
            workflow = replace(original, topology={**original.topology, 'wall_seconds': attempt['wall_seconds']})
            result = executor.execute(workflow, {'run_id': attempt['run_directory'],
                'run_directory': attempt['run_directory'], 'input': attempt['input'],
                'principal': item['principal'], 'workflow': attempt['workflow'],
                'task_id': 'task-flow-' + hashlib.sha256((item['id'] + ':' + attempt['path']).encode()).hexdigest(),
                'revision': attempt['revision'], 'execution_deadline': monotonic_deadline},
                cancelled=stopped, ownership_descriptor=lock_fd)
            self._save_attempt(attempt, result.state, result.result, result.error)
        except BaseException:
            # Unknown includes infrastructure uncertainty. Recovery reuses the
            # assigned run; it never creates a fresh external-effect identity.
            self._save_attempt(attempt, 'unknown', error={'code': 'worker_interrupted'})

    def run(self, principal, instance):
        from .workflow_scheduler import SchedulerOwnership
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
        with SchedulerOwnership(self.state_dir, instance) as ownership:
            if self.operations.readonly or not self.operations.admission_enabled:
                raise ServiceError('audit_only')
            item = self._instance(instance)
            if item['state'] in _FINISHED | _PAUSED:
                return self.inspect(principal, instance)
            plan = self.plans[item['plan']].spec
            limits = plan['limits']
            deadline = time.monotonic() + max(0, item['deadline'] - time.time())
            live = {}
            # A stale running intent means a predecessor died. Its retained
            # lock has now drained; replay the exact run, not a replacement.
            with self.store.transaction() as db:
                db.execute("UPDATE attempts SET state='queued' WHERE instance=? AND state='running'", (instance,))
                db.execute("UPDATE instances SET state='running' WHERE id=?", (instance,))
                event(db, instance, None, 'running', 'running')
            while True:
                for key in ownership.poll():
                    attempt = live.pop(key)
                    with self.store.transaction(readonly=True) as db:
                        state = db.execute('SELECT state FROM attempts WHERE instance=? AND path=? AND revision=?',
                                           (instance, attempt['path'], attempt['revision'])).fetchone()[0]
                    if state == 'running':
                        self._save_attempt(attempt, 'unknown', error={'code': 'worker_lost'})
                current = self._instance(instance)
                ready = []
                state, output = self._advance(instance, 'root', plan['root'], item['input'], limits, item['deadline'], ready)
                expired = time.monotonic() >= deadline
                stopped = current['cancel_requested'] or expired
                if not live and (stopped or state in _PAUSED | {'expired', 'cancelled'}
                                 or state in _FINISHED and not ready):
                    with self.store.transaction() as db:
                        # Cancellation may have committed after the iteration's
                        # snapshot. Final publication must honour that intent.
                        cancelled = db.execute('SELECT cancel_requested FROM instances WHERE id=?',
                                               (instance,)).fetchone()[0]
                        uncertain = db.execute("SELECT 1 FROM attempts WHERE instance=? AND state='unknown' LIMIT 1", (instance,)).fetchone()
                        final = ('unknown' if uncertain else 'expired' if expired else 'cancelled' if cancelled else state)
                        db.execute('UPDATE instances SET state=?,output=?,error=? WHERE id=?',
                                   (final, encode(output) if final == 'completed' else None,
                                    encode({'code': final}) if final != 'completed' else None, instance))
                        # A stopped instance never leaves an unstarted task
                        # looking actionable. Running/unknown records survive.
                        if cancelled or final in {'failed', 'cancelled', 'expired'}:
                            self.store.close_stopped_work(db, instance)
                        event(db, instance, None, 'state_changed', final)
                    return self.inspect(principal, instance)
                if not stopped and state not in _PAUSED | {'expired', 'cancelled'}:
                    for attempt in ready:
                        if len(live) >= limits['max_parallel']:
                            break
                        scopes = self._scopes(instance, attempt['path'])
                        if any(sum(other['path'] == scope['path'] or other['path'].startswith(scope['path']+'/')
                                   for other in live.values()) >= scope['limits']['max_parallel'] for scope in scopes):
                            continue
                        self._save_attempt(attempt, 'running')
                        key = (attempt['path'], attempt['revision'])
                        def execute(reader, descriptors, attempt=attempt):
                            self._worker(attempt, item, limits, descriptors, reader, deadline)
                        ownership.spawn(key, execute)
                        live[key] = attempt
                if not live:
                    raise ServiceError('workflow_no_progress')
                time.sleep(0.02)

    def continue_task(self, principal, instance, path, request_key, input):
        _text(request_key, 'request_key')
        supplied = _object(input)
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
        with self._ownership(instance), self._mutation() as db:
            item = self._owned(db, principal, instance)
            old = db.execute('SELECT * FROM continuations WHERE instance=? AND request_key=?', (instance, request_key)).fetchone()
            if old:
                if old['path'] != path or old['input'] != encode(supplied):
                    raise ServiceError('request_conflict')
            else:
                if item['cancel_requested'] or item['state'] not in {'input_required', 'auth_required'} or item['deadline'] <= time.time():
                    raise ServiceError('invalid_workflow_state')
                attempt = decode(db.execute('SELECT * FROM attempts WHERE instance=? AND path=? ORDER BY revision DESC LIMIT 1', (instance, path)).fetchone())
                if not attempt or attempt['state'] not in {'input_required', 'auth_required'}:
                    raise ServiceError('invalid_continuation')
                workflow = self.workflows[attempt['workflow']]
                if workflow.continue_input is None:
                    raise ServiceError('unsupported_capability')
                self.operations.check_storage_admission()
                value = _object(workflow.continue_input(attempt['output'] or {}, supplied))
                revision = attempt['revision'] + 1
                if revision >= 32:
                    raise ServiceError('revision_limit')
                directory = 'run-flow-' + hashlib.sha256((instance + ':' + path + ':' + str(revision)).encode()).hexdigest()[:40]
                seconds = min(workflow.topology.get('wall_seconds', 3600), max(1, math.ceil(item['deadline'] - time.time())))
                db.execute('INSERT INTO attempts(instance,path,revision,workflow,identity,run_directory,input,wall_seconds,state) VALUES(?,?,?,?,?,?,?,?,\'queued\')',
                           (instance, path, revision, attempt['workflow'], attempt['identity'], directory, encode(value), seconds))
                db.execute('INSERT INTO continuations VALUES(?,?,?,?,?)', (instance, request_key, path, encode(supplied), revision))
                db.execute('UPDATE instances SET state=\'queued\',error=NULL WHERE id=?', (instance,))
                event(db, instance, path, 'continued', 'queued')
        return self.inspect(principal, instance)

    def recover(self, principal, instance):
        from .journal import inspect_run
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
        with self._ownership(instance), self._mutation() as db:
            item = self._owned(db, principal, instance)
            if item['state'] != 'unknown':
                raise ServiceError('invalid_workflow_state')
            for row in db.execute('SELECT * FROM attempts WHERE instance=? AND state=\'unknown\'', (instance,)).fetchall():
                directory = self.state_dir / row['run_directory']
                if (directory / 'host.sqlite').exists() and inspect_run(directory)['recovery_required']:
                    raise ServiceError('reconciliation_required')
                db.execute('UPDATE attempts SET state=\'queued\',error=NULL WHERE instance=? AND path=? AND revision=?',
                           (instance, row['path'], row['revision']))
            db.execute('UPDATE instances SET state=\'queued\',error=NULL WHERE id=?', (instance,))
            event(db, instance, None, 'recovery_queued', 'queued')
        return self.inspect(principal, instance)
