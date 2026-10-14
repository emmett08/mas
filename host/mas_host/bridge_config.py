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
                                 "allowed_hosts", "allowed_origins", "max_pending", "max_tasks"}
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
    if "factory" in config:
        target = config["factory"]
        if type(target) is not str or not re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", target):
            raise ServiceError("invalid_factory")
        module, name = target.split(":")
        # This is local operator configuration, never a remotely supplied name.
        workflows = getattr(importlib.import_module(module), name)()
    else:
        workflows = invoice_workflows(mode=config.get("mode", "mock"))
    return DurableTaskService(state_dir, workflows,
        {principal: set(names) for principal, names in config["grants"].items()},
        max_pending=config.get("max_pending", 1000), max_tasks=config.get("max_tasks", 10000),
        mas_bin=os.environ.get("MAS_BIN", "mas"), gateway_bin=os.environ.get("MAS_GATEWAY_BIN", "mas-gateway"))


def build_verifier(config: dict):
    from .bridges.authentication import StaticTokenVerifier, OAuthIntrospectionVerifier
    auth = config.get("auth", {})
    if type(auth) is not dict:
        raise ServiceError("invalid_auth_configuration")
    if auth.get("type") == "static" and set(auth) == {"type", "file"}:
        return StaticTokenVerifier.from_file(Path(auth["file"]))
    if auth.get("type") == "oauth" and set(auth) == {
            "type", "issuer", "resource", "introspection_url", "client_id", "client_secret_file"}:
        return OAuthIntrospectionVerifier(**{key: value for key, value in auth.items() if key != "type"})
    raise ServiceError("invalid_auth_configuration")
