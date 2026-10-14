"""One authenticated lifecycle for leaf submissions and composed plans.

Each underlying submission transaction is its own durable scheduling intent.
The dispatcher reads those records directly; there is no non-atomic second
enqueue operation. Existing task and composition identities remain unchanged.
"""
from __future__ import annotations

from types import SimpleNamespace

from .service_contracts import ServiceError
from .task_service import DurableTaskService, ExecutionResult, _bounded_items, _object, _text
from .workflow_engine import WorkflowEngine


class _ExecutionRouter:
    def __init__(self, service, leaf_executor):
        self.service, self.leaf_executor = service, leaf_executor

    @property
    def validate_input(self):
        return getattr(self.leaf_executor, 'validate_input', None)

    def execute(self, workflow, execution, *, cancelled, ownership_descriptor):
        if execution.get('composition_id'):
            result = self.service.compositions.run(execution['principal'], execution['composition_id'])
            return ExecutionResult(result['state'], result)
        return self.leaf_executor.execute(workflow, execution, cancelled=cancelled,
                                          ownership_descriptor=ownership_descriptor)


class UnifiedTaskService:
    """Transport-neutral facade; trusted workers use ``run_once`` separately."""
    def __init__(self, state_dir, workflows, plans, grants, *, operations=None,
                 workflow_versions=None, max_pending=1000, max_tasks=10000,
                 max_instances=1000, mas_bin='mas', gateway_bin='mas-gateway',
                 executor=None, supervised=True, scheduler_policy=None):
        from .operations import DeploymentLedger
        from .task_store import TaskStore
        from .workflow_store import WorkflowStore
        self.operations = operations or DeploymentLedger(state_dir)
        self.state_dir = self.operations.state_dir
        leaf_names, plan_names = set(workflows), set(plans)
        if (self.state_dir / 'task-service.sqlite').exists():
            store = TaskStore(self.state_dir, deployment_id=self.operations.deployment_id, readonly=True)
            with store.transaction(readonly=True) as db:
                leaf_names.update(row[0] for row in db.execute('SELECT DISTINCT workflow FROM tasks'))
                leaf_names.update(row[0] for row in db.execute('SELECT name FROM workflows'))
        if (self.state_dir / 'composition.sqlite').exists():
            with WorkflowStore(self.state_dir, readonly=True).transaction(readonly=True) as db:
                plan_names.update(row[0] for row in db.execute('SELECT DISTINCT plan FROM instances'))
                if db.execute("SELECT 1 FROM sqlite_master WHERE name='archives'").fetchone():
                    plan_names.update(row[0] for row in db.execute('SELECT DISTINCT plan FROM archives'))
        if leaf_names & plan_names:
            raise ServiceError('ambiguous_workflow_name')
        self.grants = {principal: frozenset(names) for principal, names in grants.items()}
        if any(not names <= leaf_names | plan_names for names in self.grants.values()):
            raise ServiceError('invalid_grant')
        leaf_grants = {principal: names & leaf_names for principal, names in self.grants.items()}
        plan_grants = {principal: names & plan_names for principal, names in self.grants.items()}
        self.leaves = DurableTaskService(self.state_dir, workflows, leaf_grants, operations=self.operations,
            workflow_versions=workflow_versions, max_pending=max_pending, max_tasks=max_tasks,
            mas_bin=mas_bin, gateway_bin=gateway_bin, executor=executor, supervised=supervised,
            scheduler_policy=scheduler_policy)
        self.compositions = WorkflowEngine(self.state_dir, plans, workflows, plan_grants,
            operations=self.operations, workflow_versions=workflow_versions, max_instances=max_instances,
            mas_bin=mas_bin, gateway_bin=gateway_bin, scheduler_policy=scheduler_policy)
        self.capacity = self.leaves.capacity
        self.plan_names = plan_names
        self.executor = _ExecutionRouter(self, self.leaves.executor)

    @property
    def executor(self):
        return self.leaves.executor

    @executor.setter
    def executor(self, value):
        self.leaves.executor = value

    def _authorise(self, principal, name=None):
        _text(principal, 'principal')
        if principal not in self.grants or name is not None and name not in self.grants[principal]:
            raise ServiceError('forbidden')

    @staticmethod
    def _composition(task_id):
        _text(task_id, 'task_id')
        return task_id.startswith('flow-')

    def _receipt(self, principal, graph):
        from .service_timing import receipt
        instance = graph['instance_id']
        with self.compositions.store.transaction(readonly=True) as db:
            item = self.compositions._owned(db, principal, instance)
            resumable = False
            for row in db.execute("SELECT workflow,identity FROM attempts WHERE instance=? "
                                  "AND state IN ('input_required','auth_required')", (instance,)):
                try:
                    leaf = self.compositions._workflow(row['workflow'], row['identity'])
                    resumable |= leaf.continue_input is not None
                except ServiceError:
                    pass
        state = graph['state']
        # The established TaskService terminal-failure representation includes
        # explicit expiry evidence without changing existing bridge state enums.
        public_state = 'failed' if state == 'expired' else state
        latest = {}
        for attempt in graph.get('attempts', ()):
            if attempt['path'] not in latest or attempt['revision'] > latest[attempt['path']]['revision']:
                latest[attempt['path']] = attempt
        paused = [{'path': row['path'], 'revision': row['revision'], 'state': row['state']}
                  for row in latest.values() if row['state'] in {'input_required', 'auth_required'}]
        result = graph['output']
        if state in {'input_required', 'auth_required'}:
            result = {'pending_tasks': paused}
        return {'task_id': instance, 'kind': 'plan', 'workflow': item['plan'],
                'workflow_identity': item['identity'],
                'request_key': item['request_key'], 'state': public_state,
                'created_at': graph.get('created', item['created']) * 1000,
                'updated_at': graph.get('updated', item['created']) * 1000,
                'cancel_requested': graph['cancel_requested'], 'result': result,
                'error': graph['error'], 'metadata': item.get('metadata', {}),
                'revision': max((row['revision'] for row in graph.get('attempts', ())), default=0),
                'capabilities': {'cancel': True, 'update': bool(paused) and resumable, 'events': True, 'task_detail': True},
                'progress': {'nodes': graph.get('usage', {}).get('steps', 0),
                             'completed_nodes': sum(row['state'] == 'completed' for row in graph.get('nodes', ()))},
                **({'archived': True, 'result_available': False, 'archived_at': graph['archived_at'] * 1000,
                    'result_digest': graph.get('output_digest')} if graph.get('archived') else {}), **receipt(item)}

    def submit(self, principal, request_key, workflow, input, *, metadata=None, expected_identity=None, expected_deployment_id=None):
        self._authorise(principal, workflow)
        from .operations import _file_lock
        # The durable target record is the enqueue intent. This lock serialises
        # cross-store key arbitration without a vulnerable second queue write.
        with _file_lock(self.state_dir / '.unified-submission.lock', True):
            with self.leaves.store.transaction(readonly=True) as db:
                leaf = db.execute('SELECT task_id FROM requests WHERE principal=? AND request_key=?',
                                  (principal, request_key)).fetchone()
            with self.compositions.store.transaction(readonly=True) as db:
                plan = self.compositions.store.submitted(db, principal, request_key)
            if leaf is not None and (plan is not None or workflow in self.plan_names) or plan is not None and workflow not in self.plan_names:
                raise ServiceError('request_conflict')
            if workflow in self.plan_names:
                return self._receipt(principal, self.compositions.submit(principal, request_key, workflow, input,
                                                                        metadata=metadata, expected_identity=expected_identity,
                                                                        expected_deployment_id=expected_deployment_id))
            return self.leaves.submit(principal, request_key, workflow, input, metadata=metadata, expected_identity=expected_identity,
                                      expected_deployment_id=expected_deployment_id)

    def inspect(self, principal, task_id):
        if self._composition(task_id):
            return self._receipt(principal, self.compositions.inspect(principal, task_id))
        return self.leaves.inspect(principal, task_id)

    def result(self, principal, task_id):
        current = self.inspect(principal, task_id)
        if current['state'] != 'completed':
            raise ServiceError('result_unavailable')
        if current.get('archived'):
            raise ServiceError('result_unavailable')
        return current['result']

    def inspect_task(self, principal, task_id, path, revision=None):
        if not self._composition(task_id):
            raise ServiceError('unsupported_capability')
        return self.compositions.inspect_task(principal, task_id, path, revision)

    def cancel(self, principal, task_id):
        if self._composition(task_id):
            return self._receipt(principal, self.compositions.cancel(principal, task_id))
        return self.leaves.cancel(principal, task_id)

    def update(self, principal, task_id, request_key, input, *, metadata=None, expected_identity=None, expected_deployment_id=None):
        if not self._composition(task_id):
            return self.leaves.update(principal, task_id, request_key, input, metadata=metadata,
                                      expected_identity=expected_identity, expected_deployment_id=expected_deployment_id)
        current = self.inspect(principal, task_id)
        supplied = _object(input)
        if set(supplied) != {'path', 'input'}:
            raise ServiceError('composition_continuation_requires_path_and_input')
        metadata = _object(metadata) if metadata is not None else {}
        return self._receipt(principal, self.compositions.continue_task(principal, task_id, supplied['path'],
                                                                       request_key, supplied['input'], expected_identity=expected_identity,
                                                                       expected_deployment_id=expected_deployment_id, metadata=metadata))

    def recover(self, principal, task_id):
        if self._composition(task_id):
            return self._receipt(principal, self.compositions.recover(principal, task_id))
        return self.leaves.recover(principal, task_id)

    def recovery_runs(self, principal, task_id):
        if not self._composition(task_id):
            return self.leaves.recovery_runs(principal, task_id)
        with self.compositions.store.transaction(readonly=True) as db:
            self.compositions._owned(db, principal, task_id)
            return [dict(row) for row in db.execute('SELECT path,revision,run_directory,state FROM attempts '
                'WHERE instance=? ORDER BY path,revision', (task_id,))]

    def events(self, principal, task_id, after=0):
        if self._composition(task_id):
            return self.compositions.events(principal, task_id, after, 1000)
        return self.leaves.events(principal, task_id, after)

    def events_page(self, principal, task_id, after=0, limit=100):
        if not self._composition(task_id):
            return self.leaves.events_page(principal, task_id, after, limit)
        if type(limit) is not int or not 1 <= limit <= 1000:
            raise ServiceError('invalid_cursor')
        values = self.compositions.events(principal, task_id, after, min(limit + 1, 1000))
        selected, more = _bounded_items(values, limit=limit, overhead=128)
        if len(selected) == 1000:
            more = bool(self.compositions.events(principal, task_id, selected[-1]['id'], 1))
        return {'events': selected, 'next_after': selected[-1]['id'] if selected else after, 'has_more': more}

    def discover(self, principal):
        self._authorise(principal)
        registrations = []
        for name in sorted(self.grants[principal]):
            if name in self.plan_names:
                binding = self.compositions.bindings.get(name)
                registrations.append({'name': name, 'kind': 'plan', 'available': binding is not None,
                                      'identity': binding['identity'] if binding else None})
            else:
                workflow = self.leaves.workflows.get(name)
                registrations.append({'name': name, 'kind': 'workflow', 'available': workflow is not None,
                    'identity': workflow.identity if workflow else None,
                    'contract': workflow.contract_descriptor if workflow else None})
        return {'format': 'mas-task-service/1', 'deployment_id': self.operations.deployment_id,
                'capabilities': ['submit', 'inspect', 'result', 'cancel', 'update', 'events_page', 'task_detail', 'recover', 'pinned_identity'],
                'registrations': registrations}

    def _page_ids(self, principal, *, limit, offset=0, after=None, state=None, metadata=None,
                  updated_after=None, newest=False):
        """Select bounded identifiers in SQL; never materialise retained payloads."""
        with self.leaves.store.transaction(readonly=True) as db:
            db.execute('ATTACH DATABASE ? AS composition_store',
                       (self.compositions.store.path.resolve().as_uri() + '?mode=ro',))
            columns = {row[1] for row in db.execute('PRAGMA composition_store.table_info(instances)')}
            plan_metadata = 'i.metadata' if 'metadata' in columns else "'{}'"
            projection = ("SELECT id,principal,workflow AS name,state,created_at,updated_at,metadata FROM tasks UNION ALL "
                "SELECT i.id,i.principal,i.plan,CASE WHEN i.state='expired' THEN 'failed' ELSE i.state END,"
                "i.created*1000,(SELECT coalesce(max(e.created),i.created)*1000 FROM composition_store.events e WHERE e.instance=i.id),"
                + plan_metadata + ' FROM composition_store.instances i')
            if db.execute("SELECT 1 FROM composition_store.sqlite_master WHERE name='archives'").fetchone():
                projection += (" UNION ALL SELECT id,principal,plan,CASE WHEN state='expired' THEN 'failed' ELSE state END,"
                               "created*1000,archived_at*1000,metadata FROM composition_store.archives")
            allowed = sorted(self.grants[principal])
            clauses = ['principal=?', 'name IN (' + ','.join('?' * len(allowed)) + ')']
            values = [principal, *allowed]
            if state is not None:
                clauses.append('state=?'); values.append(state)
            if updated_after is not None:
                clauses.append('updated_at>=?'); values.append(updated_after)
            for key, value in sorted((metadata or {}).items()):
                clauses.append('json_extract(metadata,?)=?'); values.extend(('$.'+key, value))
            source = 'WITH records AS (' + projection + ') '
            total = db.execute(source + 'SELECT count(*) FROM records WHERE ' + ' AND '.join(clauses), values).fetchone()[0]
            if after is not None:
                clauses.append('(updated_at<? OR (updated_at=? AND id<?))')
                values.extend((after[0], after[0], after[1]))
            order = 'updated_at DESC,id DESC' if newest else 'created_at,id'
            rows = db.execute(source + 'SELECT id FROM records WHERE ' + ' AND '.join(clauses) +
                              ' ORDER BY ' + order + ' LIMIT ? OFFSET ?', [*values, limit, offset]).fetchall()
            return [row['id'] for row in rows], total

    def list_tasks(self, principal, limit=100, offset=0):
        self._authorise(principal)
        if type(limit) is not int or not 1 <= limit <= 1000 or type(offset) is not int or offset < 0:
            raise ServiceError('invalid_pagination')
        ids, _total = self._page_ids(principal, limit=limit, offset=offset)
        return _bounded_items((self.inspect(principal, task_id) for task_id in ids), limit=limit)[0]

    def list_task_page(self, principal, *, page_size=100, after=None, state=None, metadata=None, updated_after=None):
        self._authorise(principal)
        # Reuse the established input/cursor validation before collecting the
        # two bounded local repositories into one deterministic page.
        self.leaves.list_task_page(principal, page_size=page_size, after=after, state=state,
                                   metadata=metadata, updated_after=updated_after)
        ids, total = self._page_ids(principal, limit=page_size+1, after=after, state=state,
                                    metadata=metadata, updated_after=updated_after, newest=True)
        selected, more = _bounded_items((self.inspect(principal, task_id) for task_id in ids), limit=page_size, overhead=1024)
        return {'tasks': selected, 'total_size': total,
                'next': [selected[-1]['updated_at'], selected[-1]['task_id']] if more else None}

    def pending(self):
        rows = self.leaves.pending()
        with self.compositions.store.transaction(readonly=True) as db:
            rows.extend(dict(row) for row in db.execute("SELECT id AS task_id,principal,created*1000 AS created_at "
                "FROM instances WHERE state IN ('queued','running','blocked') ORDER BY created,id"))
            from .service_timing import status
            from .workflow_store import decode
            for row in db.execute("SELECT id,principal,created,clock FROM instances i WHERE state IN ('input_required','auth_required') "
                                  "AND (clock IS NOT NULL OR EXISTS(SELECT 1 FROM scopes s WHERE s.instance=i.id AND s.clock IS NOT NULL))"):
                item = decode(row)
                observed = status(item)
                if observed and observed['expired'] or self.compositions.store.expired_scopes(item['id']):
                    rows.append({'task_id': item['id'], 'principal': item['principal'], 'created_at': item['created']*1000})
        return self.capacity.fair_order(sorted(rows, key=lambda row: (row['created_at'], row['task_id'])))

    def run_once(self, task_id=None):
        rows = self.pending() if task_id is None else [{'task_id': task_id}]
        for row in rows:
            try:
                if not self._composition(row['task_id']):
                    result = self.leaves.run_once(row['task_id'])
                    if result is not None and result['state'] != 'blocked':
                        return result
                    continue
                with self.compositions.store.transaction(readonly=True) as db:
                    item = self.compositions.store.instance(db, row['task_id'])
                if item is None:
                    continue
                import time
                workflow = SimpleNamespace(topology={'wall_seconds': max(1, item['deadline'] - int(time.time()))})
                result = self.executor.execute(workflow, {'composition_id': item['id'], 'principal': item['principal']},
                                               cancelled=lambda: False, ownership_descriptor=0)
                receipt = self._receipt(item['principal'], result.result)
                if receipt['state'] != 'blocked':
                    return receipt
            except ServiceError as error:
                if error.code not in {'workflow_busy', 'worker_busy', 'forbidden'}:
                    raise
        return None
