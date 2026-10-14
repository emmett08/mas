"""Construct the shared service from a trusted, already bound controller epoch."""
from __future__ import annotations

from .service_contracts import ServiceError
from .shared_task_service import SharedTaskService
from .task_service import _bounded_items


class SharedTaskFacade:
    def __init__(self, leaves, compositions=None):
        self.leaves, self.compositions = leaves, compositions
        self.authority = self.operations = leaves.authority
        self.state_dir = leaves.state_dir
        self.grants = leaves.grants
        self.definitions = leaves.definitions
        from .shared_remote import SharedPeerRegistry
        self.peer_registry = SharedPeerRegistry(self.authority)

    def _owner(self, task_id):
        if type(task_id) is str and task_id.startswith('task-'):
            return self.leaves
        if type(task_id) is str and task_id.startswith('flow-') and self.compositions is not None:
            return self.compositions
        raise ServiceError('task_not_found')

    def submit(self, principal, request_key, workflow, input, **options):
        owner = self.compositions if self.compositions is not None and workflow in self.compositions.plans else self.leaves
        return owner.submit(principal, request_key, workflow, input, **options)

    def inspect(self, principal, task_id):
        return self._owner(task_id).inspect(principal, task_id)

    def result(self, principal, task_id):
        return self._owner(task_id).result(principal, task_id)

    def cancel(self, principal, task_id):
        return self._owner(task_id).cancel(principal, task_id)

    def update(self, principal, task_id, request_key, input, **options):
        return self._owner(task_id).update(principal, task_id, request_key, input, **options)

    def recover(self, principal, task_id):
        return self._owner(task_id).recover(principal, task_id)

    def inspect_task(self, principal, task_id, path, revision=None):
        owner = self._owner(task_id)
        if owner is self.leaves:
            raise ServiceError('unsupported_capability')
        return owner.inspect_task(principal, task_id, path, revision)

    def events(self, principal, task_id, after=0):
        return self._owner(task_id).events(principal, task_id, after)

    def events_page(self, principal, task_id, after=0, limit=100):
        return self._owner(task_id).events_page(principal, task_id, after, limit)

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        options = dict(page_size=page_size, after=after, state=state,
                       metadata=metadata, updated_after=updated_after)
        pages = [self.leaves.list_task_page(principal, **options)]
        if self.compositions is not None:
            pages.append(self.compositions.list_task_page(principal, **options))
        values = sorted((item for page in pages for item in page['tasks']),
                        key=lambda x: (x['updated_at'], x['task_id']), reverse=True)
        selected, more = _bounded_items(values, limit=page_size, overhead=1024)
        more = more or any(page['next'] is not None for page in pages)
        return {'tasks': selected, 'total_size': sum(page['total_size'] for page in pages),
            'next': [selected[-1]['updated_at'], selected[-1]['task_id']] if more and selected else None}

    def list_tasks(self, principal, limit=100, offset=0):
        if type(limit) is not int or not 1 <= limit <= 1000 or type(offset) is not int or offset < 0:
            raise ServiceError('invalid_pagination')
        after, remaining = None, offset
        while True:
            page = self.list_task_page(principal, page_size=min(1000, remaining + limit), after=after)
            if remaining < len(page['tasks']):
                return page['tasks'][remaining:remaining + limit]
            remaining -= len(page['tasks'])
            if page['next'] is None:
                return []
            after = page['next']

    def discover(self, principal):
        discovery = self.leaves.discover(principal)
        if self.compositions is not None:
            plans = self.compositions.discover(principal)
            names = set(self.compositions.plans)
            discovery['registrations'] = [r for r in discovery['registrations'] if r['name'] not in names] + plans
        return discovery

    def pending(self):
        pending = self.leaves.pending()
        if self.compositions is not None:
            pending += self.compositions.pending()
        return sorted(pending, key=lambda x: (x.get('created_at', 0), x['task_id']))

    def run_once(self, task_id=None):
        if task_id is not None:
            return self._owner(task_id).run_once(task_id)
        for item in self.pending():
            result = self._owner(item['task_id']).run_once(item['task_id'])
            if result is not None:
                return result
        return None

    def recovery_runs(self, principal, task_id):
        owner = self._owner(task_id)
        if owner is self.leaves:
            return owner.recovery_runs(principal, task_id)
        return [{key: row[key] for key in ('revision', 'run_id', 'state', 'path')}
                for row in owner._graph(principal, task_id)['attempts']]

    def request_epoch_status(self, principal, epoch):
        return self.leaves.request_epoch_status(principal, epoch)

    def retire_request_epoch(self, principal, epoch, **options):
        return self.leaves.retire_request_epoch(principal, epoch, **options)

    def recovery_service(self, *, operators, queries=None):
        from .shared_recovery import SharedRecoveryService
        return SharedRecoveryService(self, operators=operators, queries=queries)

    def deadline_service(self, *, operators):
        from .shared_deadlines import SharedDeadlineService
        return SharedDeadlineService(self, operators=operators)

    def close(self):
        self.authority.close()


def create_shared_service(authority, cache_dir, workflows, grants, *, plans=None,
                          workflow_versions=None, allocation_limits=None,
                          max_pending=1000, max_tasks=10000, max_revisions=32,
                          supervised=True):
    """Install additive repositories and renew this explicitly supplied epoch.

    This factory never initialises or takes over a deployment. Provision and
    handover are separate operator actions against the shared authority.
    """
    if type(allocation_limits) is not dict or not {'deployment', *(f'principal/{p}' for p in grants)} <= set(allocation_limits):
        raise ServiceError('invalid_shared_allocations')
    authority.install_host()
    authority.start_renewal()
    try:
        for scope, limits in (allocation_limits or {}).items():
            authority.configure_scope(scope, **limits)
        leaves = SharedTaskService(authority, cache_dir, workflows, grants,
            workflow_versions=workflow_versions, max_pending=max_pending,
            max_tasks=max_tasks, max_revisions=max_revisions, supervised=supervised)
        compositions = None
        if plans:
            from .shared_composition_service import SharedCompositionService
            if set(workflows) & set(plans):
                raise ServiceError('ambiguous_registration')
            compositions = SharedCompositionService(authority, cache_dir, plans, workflows,
                grants, workflow_versions=workflow_versions, supervised=supervised)
        return SharedTaskFacade(leaves, compositions)
    except BaseException:
        authority.close()
        raise
