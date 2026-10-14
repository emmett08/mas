"""Authenticated service client and bounded HTTP transport.

No automatic POST retries occur. Callers retain their original request key to
recover the same durable receipt after an interrupted submission.
"""
from __future__ import annotations

import base64
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import sys
from urllib.parse import urlsplit

from ..codec import integer_json
from ..contracts import RunnerError
from ..service_contracts import ServiceError
from .authentication import _principal, _unique, read_token_file

MAX_WIRE_BYTES = 1_048_576

def validate_endpoint(endpoint, *, allow_loopback_http=False):
    try:
        parsed = urlsplit(endpoint)
        loopback = False
        try:
            loopback = ipaddress.ip_address(parsed.hostname or "").is_loopback
        except ValueError:
            pass
        if (type(endpoint) is not str or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment
                or any(ord(c) <= 32 or ord(c) >= 127 for c in endpoint)
                or parsed.scheme != "https" and not (
                    allow_loopback_http and parsed.scheme == "http" and loopback)):
            raise ValueError()
        parsed.port
        return parsed
    except (ValueError, TypeError):
        raise ServiceError("invalid_endpoint") from None


def http_exchange(endpoint, *, method="POST", body=None, headers=None,
                  timeout_seconds=5, max_response_bytes=65536, allow_loopback_http=False,
                  sse_response_id=None):
    """One explicit HTTP request with an absolute, thread-compatible deadline.

    A small disposable subprocess prevents socket/DNS implementation details
    from defeating the deadline. Private pipes carry authentication. The child
    uses stdlib HTTP directly, with no environment proxies and no redirects.
    """
    validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
    if (type(timeout_seconds) not in (int, float) or not 0 < timeout_seconds <= 300
            or type(max_response_bytes) is not int or not 1 <= max_response_bytes <= 1024 * 1024
            or method not in {"GET", "POST", "DELETE", "PUT"}
            or body is not None and (type(body) is not bytes or len(body) > 1024 * 1024)
            or sse_response_id is not None and type(sse_response_id) not in (int, str)):
        raise ServiceError("invalid_transport_configuration")
    header_map = dict(headers or {})
    for key, value in header_map.items():
        if (type(key) is not str or type(value) is not str or not key
                or any(ord(c) < 33 or ord(c) > 126 or c == ":" for c in key)
                or any(ord(c) < 32 or ord(c) > 126 for c in value)
                or len(value) > 16384):
            raise ServiceError("invalid_transport_configuration")
    wire = json.dumps({"endpoint": endpoint, "method": method, "headers": header_map,
                       "body": base64.b64encode(body).decode() if body is not None else None,
                       "timeout_seconds": timeout_seconds, "max_response_bytes": max_response_bytes,
                       "sse_response_id": sse_response_id},
                      separators=(",", ":")).encode()
    process = None
    try:
        # -I excludes Python path/user-site injection; the worker is a fixed
        # sibling file and only imports standard-library modules.
        process = subprocess.Popen([sys.executable, "-I", str(Path(__file__).with_name("_http_worker.py"))],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                                   close_fds=True, env={"PATH": os.defpath, "LANG": "C.UTF-8"})
        output, _unused = process.communicate(wire, timeout=timeout_seconds)
        if len(output) > 2 * max_response_bytes + 65536:
            raise ServiceError("invalid_remote_response")
        response = json.loads(output, object_pairs_hook=_unique)
        if process.returncode or type(response) is not dict or "error" in response:
            raise ServiceError("transport_failure")
        status = response["status"]
        if type(status) is not int or not 100 <= status <= 599:
            raise ServiceError("invalid_remote_response")
        data = base64.b64decode(response["body"], validate=True)
        if len(data) > max_response_bytes:
            raise ServiceError("invalid_remote_response")
        # Following any redirect could disclose the inbound bearer to a peer.
        if 300 <= status <= 399:
            raise ServiceError("redirect_rejected")
        return status, response["headers"], data
    except subprocess.TimeoutExpired:
        raise ServiceError("transport_timeout") from None
    except ServiceError:
        raise
    except Exception:
        raise ServiceError("transport_failure") from None
    finally:
        if process is not None:
            if process.poll() is None:
                process.kill()
            process.communicate()


def decode_integer_json(payload, *, max_bytes=65536):
    if len(payload) > max_bytes:
        raise ServiceError("request_too_large")
    def bad(_value):
        raise ValueError()
    try:
        value = json.loads(payload, object_pairs_hook=_unique, parse_float=bad, parse_constant=bad)
        integer_json(value, max_bytes)
        return value
    except (ValueError, TypeError, UnicodeError, RunnerError, RecursionError):
        raise ServiceError("invalid_json") from None


class TaskServiceClient:
    """TaskService port over authenticated HTTP, bound to one local principal."""
    def __init__(self, endpoint, *, token_file, principal, timeout_seconds=10,
                 allow_loopback_http=False, transport=None):
        parsed = validate_endpoint(endpoint, allow_loopback_http=allow_loopback_http)
        if parsed.path not in ("", "/", "/service"):
            raise ServiceError("invalid_endpoint")
        self.endpoint = endpoint.rstrip("/").removesuffix("/service")
        self.principal = _principal(principal)
        self.token_file = token_file
        self.timeout_seconds = timeout_seconds
        self.allow_loopback_http = allow_loopback_http
        self._transport = transport or http_exchange

    def _request(self, principal, operation, payload):
        if principal != self.principal:
            raise ServiceError("principal_mismatch")
        body = integer_json(payload, MAX_WIRE_BYTES).encode()
        status, _headers, raw = self._transport(
            self.endpoint + "/service/" + operation, method="POST", body=body,
            headers={"Authorization": "Bearer " + read_token_file(self.token_file),
                     "Content-Type": "application/json", "Accept": "application/json"},
            timeout_seconds=self.timeout_seconds, max_response_bytes=MAX_WIRE_BYTES,
            allow_loopback_http=self.allow_loopback_http)
        if _headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json":
            raise ServiceError("invalid_remote_content_type")
        response = decode_integer_json(raw, max_bytes=MAX_WIRE_BYTES)
        if type(response) is not dict:
            raise ServiceError("invalid_remote_response")
        if status >= 400:
            error = response.get("error")
            if type(error) is dict and type(error.get("code")) is str:
                code = error["code"]
                if len(code) <= 128 and all(c.islower() or c.isdigit() or c == "_" for c in code):
                    raise ServiceError(code)
            raise ServiceError("remote_service_error")
        if not 200 <= status < 300 or set(response) != {"result"}:
            raise ServiceError("invalid_remote_response")
        result = response["result"]
        if operation in {"events", "list"}:
            if type(result) is not list or any(type(item) is not dict for item in result):
                raise ServiceError("invalid_remote_response")
        elif type(result) is not dict:
            raise ServiceError("invalid_remote_response")
        return result

    def submit(self, principal, request_key, workflow, input, *, metadata=None):
        payload = {"request_key": request_key, "workflow": workflow, "input": input}
        if metadata is not None:
            payload["metadata"] = metadata
        return self._request(principal, "submit", payload)

    def inspect(self, principal, task_id):
        return self._request(principal, "inspect", {"task_id": task_id})

    def result(self, principal, task_id):
        return self._request(principal, "result", {"task_id": task_id})

    def cancel(self, principal, task_id):
        return self._request(principal, "cancel", {"task_id": task_id})

    def update(self, principal, task_id, request_key, input, *, metadata=None):
        payload = {"task_id": task_id, "request_key": request_key, "input": input}
        if metadata is not None:
            payload["metadata"] = metadata
        return self._request(principal, "update", payload)

    def events(self, principal, task_id, after=0):
        return self._request(principal, "events", {"task_id": task_id, "after": after})

    def list_tasks(self, principal, limit=100, offset=0):
        return self._request(principal, "list", {"limit": limit, "offset": offset})

    def list_task_page(self, principal, *, page_size=100, after=None, state=None,
                       metadata=None, updated_after=None):
        cursor = list(after) if type(after) is tuple else after
        return self._request(principal, "page", {"page_size": page_size, "after": cursor,
            "state": state, "metadata": metadata, "updated_after": updated_after})
