"""Immutable declarative plans over explicitly registered MAS workflows.

Plans contain bounded integer JSON, never Python expressions or import paths.
Topology admission remains the leaf workflow's responsibility. This module
validates composition and conditions without executing adapters or tools.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import hashlib
import json
import re
from typing import TYPE_CHECKING

from .codec import integer_json
from .contracts import RunnerError
from .service_contracts import ServiceError

if TYPE_CHECKING:
    from .task_service import Workflow


PLAN_FORMAT = "mas-workflow/1"
MAX_DEPTH = 16
MAX_REGISTRY = 256
_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
_LIMITS = {
    "max_steps": (1, 10_000),
    "max_parallel": (1, 32),
    "wall_seconds": (1, 86_400),
    "max_calls": (1, 100_000),
    "max_reserved_microusd": (0, 1_000_000_000_000),
}


def _canonical(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False)


def _copy(value: object, code: str) -> object:
    try:
        return json.loads(integer_json(value))
    except RunnerError:
        raise ServiceError(code, "expected bounded integer-only JSON") from None


def _fields(value: object, required: set[str], code: str = "invalid_workflow_plan") -> None:
    if type(value) is not dict or set(value) != required:
        raise ServiceError(code, "object fields do not match the declared contract")


def _name(value: object) -> str:
    if type(value) is not str or _NAME.fullmatch(value) is None:
        raise ServiceError("invalid_workflow_plan_name")
    return value


def _integer(value: object, minimum: int, maximum: int) -> None:
    if type(value) is not int or not minimum <= value <= maximum:
        raise ServiceError("invalid_workflow_plan_limit")


def _condition(value: object) -> None:
    _fields(value, {"path", "equals"}, "invalid_workflow_condition")
    path = value["path"]
    if type(path) is not list or len(path) > 32:
        raise ServiceError("invalid_workflow_condition")
    for part in path:
        if type(part) is str:
            continue
        if type(part) is not int or not 0 <= part <= 2_147_483_647:
            raise ServiceError("invalid_workflow_condition")


def evaluate_condition(condition: dict, value: object) -> bool:
    """Match an exact JSON value at a path; absent or incompatible paths fail.

    An empty path selects the entire value. Integer path components select array
    positions, strings select object properties. Booleans differ from integers,
    including inside arrays and objects; no truthiness or coercion is applied.
    """
    checked = _copy(condition, "invalid_workflow_condition")
    _condition(checked)
    selected = _copy(value, "invalid_workflow_value")
    for part in checked["path"]:
        if type(selected) is dict and type(part) is str and part in selected:
            selected = selected[part]
        elif type(selected) is list and type(part) is int and part < len(selected):
            selected = selected[part]
        else:
            return False
    return _canonical(selected) == _canonical(checked["equals"])


def _validate_node(node: object, depth: int = 1) -> int:
    """Return a local upper bound, charging unresolved subworkflows one step."""
    if depth > MAX_DEPTH or type(node) is not dict:
        raise ServiceError("invalid_workflow_plan_depth")
    kind = node.get("kind")
    if kind in ("task", "subworkflow"):
        _fields(node, {"kind", "workflow"})
        _name(node["workflow"])
        return 1
    if kind == "sequence":
        _fields(node, {"kind", "steps"})
        if type(node["steps"]) is not list or not node["steps"]:
            raise ServiceError("invalid_workflow_plan")
        return 1 + sum(_validate_node(child, depth + 1) for child in node["steps"])
    if kind == "parallel":
        _fields(node, {"kind", "branches"})
        branches = node["branches"]
        if type(branches) is not dict or not branches:
            raise ServiceError("invalid_workflow_plan")
        for name in branches:
            _name(name)
        return 1 + sum(_validate_node(child, depth + 1) for child in branches.values())
    if kind == "choose":
        _fields(node, {"kind", "condition", "then", "else"})
        _condition(node["condition"])
        return 1 + max(_validate_node(node["then"], depth + 1),
                       _validate_node(node["else"], depth + 1))
    if kind == "loop":
        _fields(node, {"kind", "body", "until", "max_iterations"})
        _condition(node["until"])
        _integer(node["max_iterations"], 1, 1000)
        return 1 + node["max_iterations"] * _validate_node(node["body"], depth + 1)
    raise ServiceError("invalid_workflow_plan_kind")


@dataclass(frozen=True, init=False, slots=True)
class WorkflowPlan:
    """A source plan whose identity is independent of mutable caller objects.

    ``identity`` covers this source only. Use ``bind_plan`` to bind all leaf and
    subworkflow registrations before durable execution; a source plan cannot
    establish the identity of a referenced adapter by itself.
    """
    _source: str

    def __init__(self, spec: dict):
        checked = _copy(spec, "invalid_workflow_plan")
        _fields(checked, {"format", "name", "version", "limits", "root"})
        if checked["format"] != PLAN_FORMAT:
            raise ServiceError("invalid_workflow_plan_format")
        _name(checked["name"])
        version = checked["version"]
        if (type(version) is not str or not version or version == "unversioned"
                or len(version.encode("utf-8")) > 128
                or any(ord(char) < 32 for char in version)):
            raise ServiceError("invalid_workflow_plan_version")
        _fields(checked["limits"], set(_LIMITS))
        for name, (minimum, maximum) in _LIMITS.items():
            _integer(checked["limits"][name], minimum, maximum)
        if _validate_node(checked["root"]) > checked["limits"]["max_steps"]:
            raise ServiceError("workflow_plan_step_limit")
        object.__setattr__(self, "_source", _canonical(checked))

    @property
    def spec(self) -> dict:
        """A fresh application-owned copy of the validated source."""
        return json.loads(self._source)

    @property
    def identity(self) -> str:
        return hashlib.sha256(self._source.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class _Expansion:
    cost: int
    depth: int
    plans: frozenset[str]
    workflows: frozenset[str]


def _registry(plans: Mapping[str, WorkflowPlan], workflows: Mapping[str, Workflow]):
    # A local import keeps Workflow independent from plan validation.
    from .task_service import Workflow

    if (not isinstance(plans, Mapping) or not isinstance(workflows, Mapping)
            or len(plans) > MAX_REGISTRY or len(workflows) > MAX_REGISTRY):
        raise ServiceError("invalid_workflow_plan_registry")
    specs = {}
    for name, plan in plans.items():
        _name(name)
        if not isinstance(plan, WorkflowPlan):
            raise ServiceError("invalid_workflow_plan_registry")
        specs[name] = plan.spec
        if specs[name]["name"] != name:
            raise ServiceError("workflow_plan_name_mismatch")
    identities = {}
    for name, workflow in workflows.items():
        _name(name)
        if not isinstance(workflow, Workflow):
            raise ServiceError("invalid_workflow_plan_registry")
        identities[name] = workflow.identity

    visiting: set[str] = set()
    expansions: dict[str, _Expansion] = {}

    def plan_expansion(name: str) -> _Expansion:
        if name in visiting:
            raise ServiceError("workflow_plan_cycle")
        if name not in specs:
            raise ServiceError("unknown_subworkflow")
        if name in expansions:
            return expansions[name]
        if len(visiting) >= MAX_DEPTH:
            raise ServiceError("invalid_workflow_plan_depth")
        visiting.add(name)
        expansion = node_expansion(specs[name]["root"])
        visiting.remove(name)
        if expansion.cost > specs[name]["limits"]["max_steps"]:
            raise ServiceError("workflow_plan_step_limit")
        if expansion.depth > MAX_DEPTH:
            raise ServiceError("invalid_workflow_plan_depth")
        expansions[name] = expansion
        return expansion

    def node_expansion(node: dict) -> _Expansion:
        kind = node["kind"]
        if kind == "task":
            if node["workflow"] not in identities:
                raise ServiceError("unknown_leaf_workflow")
            return _Expansion(1, 1, frozenset(), frozenset({node["workflow"]}))
        if kind == "subworkflow":
            name = node["workflow"]
            child = plan_expansion(name)
            return _Expansion(1 + child.cost, 1 + child.depth,
                              child.plans | {name}, child.workflows)
        if kind == "sequence":
            children = [node_expansion(child) for child in node["steps"]]
        elif kind == "parallel":
            children = [node_expansion(child) for child in node["branches"].values()]
        elif kind == "choose":
            children = [node_expansion(node["then"]), node_expansion(node["else"])]
        else:  # WorkflowPlan has already validated the complete node ontology.
            children = [node_expansion(node["body"])]
        if kind == "choose":
            cost = 1 + max(child.cost for child in children)
        elif kind == "loop":
            cost = 1 + node["max_iterations"] * children[0].cost
        else:
            cost = 1 + sum(child.cost for child in children)
        return _Expansion(cost, 1 + max(child.depth for child in children),
                          frozenset().union(*(child.plans for child in children)),
                          frozenset().union(*(child.workflows for child in children)))

    for name in specs:
        plan_expansion(name)
    return specs, identities, expansions


def validate_registry(plans: Mapping[str, WorkflowPlan], workflows: Mapping[str, Workflow]) -> None:
    """Reject missing references, recursive plans and excessive expanded work.

    All registrations are checked, including currently unselected conditional
    branches. Limits are conservative: a loop is charged its declared maximum
    iterations, and a choice the more expensive branch. Parallelism is bounded
    globally by the scheduler rather than the number of declared branches.
    """
    _registry(plans, workflows)


def bind_plan(name: str, plans: Mapping[str, WorkflowPlan],
              workflows: Mapping[str, Workflow]) -> dict:
    """Return an inspectable binding and its transitive SHA-256 identity.

    Unrelated registry entries do not affect the identity. Referenced leaf
    identities include their registered version, route configuration and hooks;
    callers must version adapter implementations as required by ``Workflow``.
    """
    specs, identities, expansions = _registry(plans, workflows)
    _name(name)
    if name not in specs:
        raise ServiceError("unknown_workflow_plan")
    expansion = expansions[name]
    descriptor = {
        "format": "mas-workflow-binding/1",
        "name": name,
        "spec": specs[name],
        "plans": {key: plans[key].identity for key in sorted(expansion.plans)},
        "workflows": {key: identities[key] for key in sorted(expansion.workflows)},
        "max_steps": expansion.cost,
    }
    return {**descriptor, "identity": hashlib.sha256(
        _canonical(descriptor).encode("utf-8")).hexdigest()}
