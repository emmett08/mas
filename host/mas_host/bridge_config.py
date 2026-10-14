"""Load trusted deployment configuration and compose the shared task service."""
from __future__ import annotations

import importlib
import json
import os
from pathlib import Path
import re

from .codec import integer_json
from .service_contracts import ServiceError


def _pairs(items):
    value = {}
    for key, item in items:
        if key in value:
            raise ServiceError("duplicate_field")
        value[key] = item
    return value


def load_config(path: Path) -> dict:
    try:
        with Path(path).open(encoding="utf-8") as stream:
            raw = stream.read(65537)
        value = json.loads(raw, object_pairs_hook=_pairs)
        integer_json(value)
        if (type(value) is not dict or type(value.get("version")) is not int or value.get("version") != 1
                or set(value) - {"version", "mode", "factory", "grants", "auth", "public_url",
                                 "allowed_hosts", "allowed_origins", "max_pending", "max_tasks", "max_instances", "scheduler", "artifacts"}
                or type(value.get("grants")) is not dict):
            raise ServiceError("invalid_configuration")
        for principal, workflows in value["grants"].items():
            if (not principal or type(workflows) is not list or not workflows
                    or any(type(name) is not str or not name for name in workflows)):
                raise ServiceError("invalid_grants")
        return value
    except (OSError, UnicodeError, ValueError):
        raise ServiceError("invalid_configuration") from None


def build_service(config: dict, state_dir: Path):
    from .task_service import DurableTaskService
    from .bridge_workflows import invoice_workflows
    from .scheduler_capacity import SchedulerPolicy
    try:
        scheduler_policy = SchedulerPolicy(**config['scheduler']) if 'scheduler' in config else None
    except (TypeError, ValueError):
        raise ServiceError('invalid_scheduler_policy') from None
    if "factory" in config:
        target = config["factory"]
        if type(target) is not str or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", target):
            raise ServiceError("invalid_factory")
        module, name = target.split(":")
        # This is local operator configuration, never a remotely supplied name.
        workflows = getattr(importlib.import_module(module), name)()
        if (type(workflows) is dict and 'plans' in workflows and 'workflows' in workflows
                and set(workflows) <= {'plans', 'workflows', 'grants', 'workflow_versions'}):
            from .unified_service import UnifiedTaskService
            return UnifiedTaskService(state_dir, workflows['workflows'], workflows['plans'],
                {principal: set(names) for principal, names in config['grants'].items()},
                workflow_versions=workflows.get('workflow_versions'),
                max_pending=config.get('max_pending', 1000), max_tasks=config.get('max_tasks', 10000),
                max_instances=config.get('max_instances', 1000), scheduler_policy=scheduler_policy,
                mas_bin=os.environ.get('MAS_BIN', 'mas'), gateway_bin=os.environ.get('MAS_GATEWAY_BIN', 'mas-gateway'))
    else:
        workflows = invoice_workflows(mode=config.get("mode", "mock"))
    return DurableTaskService(state_dir, workflows,
        {principal: set(names) for principal, names in config["grants"].items()},
        max_pending=config.get("max_pending", 1000), max_tasks=config.get("max_tasks", 10000),
        scheduler_policy=scheduler_policy,
        mas_bin=os.environ.get("MAS_BIN", "mas"), gateway_bin=os.environ.get("MAS_GATEWAY_BIN", "mas-gateway"))


def build_verifier(config: dict):
    from .bridges.authentication import StaticTokenVerifier, OAuthIntrospectionVerifier
    auth = config.get("auth", {})
    if type(auth) is not dict:
        raise ServiceError("invalid_auth_configuration")
    if auth.get("type") == "static" and set(auth) == {"type", "file"}:
        return StaticTokenVerifier.from_file(Path(auth["file"]))
    required = {"type", "issuer", "resource", "introspection_url", "client_id", "client_secret_file"}
    if auth.get("type") == "oauth" and required <= set(auth) <= required | {'transport'}:
        options = {key: value for key, value in auth.items() if key not in {'type', 'transport'}}
        if 'transport' in auth:
            from .bridges.http_policy import HTTPTransportPolicy
            options['transport_policy'] = HTTPTransportPolicy.from_config(auth['transport'])
        return OAuthIntrospectionVerifier(**options)
    raise ServiceError("invalid_auth_configuration")


def build_artifact_transfer(config: dict, state_dir: Path):
    """Build only fixed operator-selected repositories; request bodies contain no paths."""
    if 'artifacts' not in config:
        return None
    from .artifacts import ArtifactPolicy
    from .bridges.artifact_transfer import ArtifactScope, ArtifactTransferService
    value = config['artifacts']
    if (type(value) is not dict or 'scopes' not in value
            or set(value) - {'scopes', 'ttl_seconds', 'max_sessions', 'max_staging_bytes'}
            or type(value['scopes']) is not dict):
        raise ServiceError('invalid_artifact_configuration')
    scopes = {}
    for name, item in value['scopes'].items():
        if (type(item) is not dict or not {'namespace', 'principals'} <= set(item)
                or set(item) - {'namespace', 'principals', 'path', 'policy'}):
            raise ServiceError('invalid_artifact_configuration')
        relative = item.get('path', 'shared-artifacts/' + name)
        if (type(relative) is not str or not relative or Path(relative).is_absolute()
                or any(part in {'', '.', '..'} for part in relative.split('/'))
                or relative.split('/')[0] == 'artifact-transfers'):
            raise ServiceError('invalid_artifact_configuration')
        policy = item.get('policy', {})
        if type(policy) is not dict or set(policy) - set(ArtifactPolicy.__dataclass_fields__):
            raise ServiceError('invalid_artifact_configuration')
        scopes[name] = ArtifactScope(Path(state_dir) / relative, item['namespace'],
                                    item['principals'], ArtifactPolicy(**policy))
    options = {key: item for key, item in value.items() if key != 'scopes'}
    return ArtifactTransferService(Path(state_dir) / 'artifact-transfers', scopes, **options)
