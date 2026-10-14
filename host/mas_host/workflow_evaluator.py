"""Plan decision rules, independent of process scheduling and SQLite storage.

The evaluator selects paths and joins retained results through four journal
operations. It never dispatches adapters, owns processes or grants admission.
"""
from __future__ import annotations

from collections.abc import Mapping
import time
from typing import Protocol

from .service_contracts import ServiceError
from .workflow_plan import WorkflowPlan, evaluate_condition


FINISHED_STATES = frozenset({'completed', 'failed', 'cancelled', 'expired'})
PAUSED_STATES = frozenset({'unknown', 'input_required', 'auth_required'})


class PlanJournal(Protocol):
    """Durable operations required by the plan decision algorithm."""
    def _node(self, instance: str, path: str, kind: str, input: dict, limits: dict) -> dict: ...
    def _attempt(self, instance: str, path: str, workflow: str, input: dict,
                 deadline: float) -> dict | None: ...
    def _finish_node(self, instance: str, path: str, state: str,
                     output: dict | None = None, error: dict | None = None) -> tuple[str, dict | None]: ...
    def _scope(self, instance: str, path: str, limits: dict, deadline: float) -> float: ...


class PlanEvaluator:
    """Advance registered control nodes and collect ready leaf assignments."""
    def __init__(self, plans: Mapping[str, WorkflowPlan], journal: PlanJournal):
        self.plans = plans
        self.journal = journal

    def advance(self, instance, path, node, input, limits, deadline, ready):
        kind = node['kind']
        saved = self.journal._node(instance, path, kind, input, limits)
        if saved['state'] == 'completed' or kind == 'task' and saved['state'] in FINISHED_STATES:
            return saved['state'], saved['output']
        if kind == 'task':
            attempt = self.journal._attempt(instance, path, node['workflow'], input, deadline)
            if attempt is None:
                return self.journal._finish_node(instance, path, 'expired')
            state = attempt['state']
            if state == 'queued':
                ready.append(attempt)
            if state in FINISHED_STATES | PAUSED_STATES:
                return self.journal._finish_node(instance, path, state, attempt['output'], attempt['error'])
            return state, None
        if kind == 'sequence':
            output = input
            for index, child in enumerate(node['steps']):
                state, output = self.advance(instance, f'{path}/s{index}', child, output, limits, deadline, ready)
                if state != 'completed':
                    return self.journal._finish_node(instance, path, state)
            return self.journal._finish_node(instance, path, 'completed', output)
        if kind == 'parallel':
            results = {name: self.advance(instance, f'{path}/p-{name}', child, input, limits, deadline, ready)
                       for name, child in sorted(node['branches'].items())}
            states = [value[0] for value in results.values()]
            if all(state == 'completed' for state in states):
                return self.journal._finish_node(instance, path, 'completed', {'branches': {name: value[1] for name, value in results.items()}})
            # Every active branch is drained before finalisation. Unknown takes
            # precedence over failure/cancellation and blocks fresh dispatch.
            state = next((s for s in ('unknown', 'input_required', 'auth_required', 'failed', 'expired', 'cancelled') if s in states), 'waiting')
            return self.journal._finish_node(instance, path, state)
        if kind == 'choose':
            branch = 'then' if evaluate_condition(node['condition'], input) else 'else'
            state, output = self.advance(instance, f'{path}/{branch}', node[branch], input, limits, deadline, ready)
        elif kind == 'loop':
            output = input
            for index in range(node['max_iterations']):
                state, output = self.advance(instance, f'{path}/i{index}', node['body'], output, limits, deadline, ready)
                if state != 'completed' or evaluate_condition(node['until'], output):
                    break
            else:
                return self.journal._finish_node(instance, path, 'failed', error={'code': 'iteration_limit'})
        elif kind == 'subworkflow':
            child = self.plans[node['workflow']].spec
            child_path = f'{path}/sub-{node["workflow"]}'
            child_deadline = self.journal._scope(instance, child_path, child['limits'], deadline)
            if child_deadline <= time.time():
                return self.journal._finish_node(instance, path, 'expired')
            state, output = self.advance(instance, child_path, child['root'], input, limits, child_deadline, ready)
        else:
            raise ServiceError('invalid_node')
        return self.journal._finish_node(instance, path, state, output)
