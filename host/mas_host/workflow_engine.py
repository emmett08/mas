"""Durable bounded composition of independently owned Rust-backed workflows."""
from __future__ import annotations

from contextlib import contextmanager, nullcontext
from dataclasses import replace
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import select
import time

from .operations import DeploymentLedger, _file_lock
from .service_contracts import ServiceError
from .service_mutation import service_mutation
from .task_service import RunnerExecutor, _object, _text
from .workflow_allocation import WorkflowAllocation
from .workflow_plan import WorkflowPlan, bind_plan, validate_registry
from .workflow_evaluator import PlanEvaluator, FINISHED_STATES as _FINISHED, PAUSED_STATES as _PAUSED
from .workflow_inspection import encode_task_inspection, validate_task_selector
from .workflow_store import WorkflowStore, encode


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
        item = self.store.instance(db, instance, principal)
        if item is None:
            raise ServiceError('workflow_not_found')
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
            row = self.store.submitted(db, principal, request_key)
            if row is not None:
                if (row['plan'] != plan or encode(row['input']) != encode(value)
                        or row['identity'] != self.bindings[plan]['identity']):
                    raise ServiceError('request_conflict')
                instance = row['id']
            else:
                self.operations.check_storage_admission()
                instance = self.store.create_instance(
                    db, principal, request_key, plan, self.bindings[plan]['identity'], value,
                    wall_seconds=self.plans[plan].spec['limits']['wall_seconds'], maximum=self.max_instances)
        return self.inspect(principal, instance)

    def inspect(self, principal, instance):
        with self.store.transaction(readonly=True) as db:
            item = self._owned(db, principal, instance)
            return self.store.graph(db, item)

    def events(self, principal, instance, after=0, limit=100):
        if type(after) is not int or after < 0 or type(limit) is not int or not 1 <= limit <= 1000:
            raise ServiceError('invalid_cursor')
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
            return self.store.events(db, instance, after, limit)

    def inspect_task(self, principal, instance, path, revision=None):
        """Return one owned leaf revision; omitted revision selects the latest."""
        validate_task_selector(path, revision)
        with self.store.transaction(readonly=True) as db:
            item = self._owned(db, principal, instance)
            attempt = self.store.task_detail(db, instance, path, revision)
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
            self.store.request_cancellation(db, item)
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
        return self.store.read_instance(instance)

    # The evaluator's public journal contract resolves trusted registration
    # metadata here; the store never receives adapters or execution authority.
    def node(self, instance, path, kind, input, limits):
        return self.store.node(instance, path, kind, input, limits)

    def finish_node(self, instance, path, state, output=None, error=None):
        return self.store.finish_node(instance, path, state, output, error)

    def scope(self, instance, path, limits, deadline):
        return self.store.scope(instance, path, limits, deadline)

    def attempt(self, instance, path, workflow, input, deadline):
        leaf = self.workflows[workflow]
        return self.store.attempt(instance, path, workflow, input, deadline,
                                  identity=leaf.identity, wall_seconds=leaf.topology.get('wall_seconds', 3600))

    def _advance(self, instance, path, node, input, limits, deadline, ready):
        return self.evaluator.advance(instance, path, node, input, limits, deadline, ready)

    def _save_attempt(self, attempt, state, output=None, error=None):
        # Keep this execution receipt seam for existing interruption fixtures.
        self.store.save_attempt(attempt, state, output, error)

    def _requires_reconciliation(self, run_directory):
        from .bootstrap import requires_reconciliation
        return requires_reconciliation(self.state_dir / run_directory, run_directory, operations=self.operations)

    def _restart(self, instance):
        # Ownership has drained all predecessors before these evidence reads.
        # Recheck queued assignments too: a predecessor may have died after
        # requeuing an old run but before publishing its observed result.
        reconciled = [(attempt, self._requires_reconciliation(attempt['run_directory']))
                      for attempt in self.store.pending_attempts(instance)]
        self.store.start_instance(instance, reconciled)

    def _worker(self, attempt, item, limits, lock_fd, lifetime_fd, monotonic_deadline):
        def stopped():
            if select.select([lifetime_fd], [], [], 0)[0]:
                return True  # EOF: scheduler lifetime ended; nobody writes bytes.
            current = self._instance(item['id'])
            return (current['cancel_requested'] or time.monotonic() >= monotonic_deadline
                    or current['plan'] not in self.grants.get(current['principal'], ()))
        try:
            scopes = self.store.scopes(item['id'], attempt['path'])
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
            # Classify retained effects even when cancellation or expiry has
            # already closed admission. Stopping is not reconciliation.
            self._restart(instance)
            while True:
                for key in ownership.poll():
                    attempt = live.pop(key)
                    state = self.store.attempt_state(attempt)
                    if state == 'running':
                        self._save_attempt(attempt, 'unknown', error={'code': 'worker_lost'})
                current = self._instance(instance)
                ready = []
                state, output = self._advance(instance, 'root', plan['root'], item['input'], limits, item['deadline'], ready)
                expired = time.monotonic() >= deadline
                stopped = current['cancel_requested'] or expired
                if not live and (stopped or state in _PAUSED | {'expired', 'cancelled'}
                                 or state in _FINISHED and not ready):
                    self.store.finish_instance(instance, state, output, expired=expired)
                    return self.inspect(principal, instance)
                if not stopped and state not in _PAUSED | {'expired', 'cancelled'}:
                    for attempt in ready:
                        if len(live) >= limits['max_parallel']:
                            break
                        scopes = self.store.scopes(instance, attempt['path'])
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
            old = self.store.continuation(db, instance, request_key)
            if old:
                if old['path'] != path or old['input'] != encode(supplied):
                    raise ServiceError('request_conflict')
            else:
                if item['cancel_requested'] or item['state'] not in {'input_required', 'auth_required'} or item['deadline'] <= time.time():
                    raise ServiceError('invalid_workflow_state')
                attempt = self.store.latest_attempt(db, instance, path)
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
                self.store.continue_attempt(db, item, attempt, request_key, supplied, value,
                                            wall_seconds=workflow.topology.get('wall_seconds', 3600))
        return self.inspect(principal, instance)

    def recover(self, principal, instance):
        with self.store.transaction(readonly=True) as db:
            self._owned(db, principal, instance)
        with self._ownership(instance), self._mutation() as db:
            item = self._owned(db, principal, instance)
            if item['state'] != 'unknown':
                raise ServiceError('invalid_workflow_state')
            for row in self.store.unknown_attempts(db, instance):
                if self._requires_reconciliation(row['run_directory']):
                    raise ServiceError('reconciliation_required')
            self.store.requeue_unknown(db, instance)
        return self.inspect(principal, instance)
