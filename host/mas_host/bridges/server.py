"""Bounded authenticated HTTP and trusted-local stdio bridge entry points.

HTTP handlers only persist/inspect TaskService requests. A separate single-
threaded worker owns Runner execution and its process/fork lifecycle.
"""
from __future__ import annotations

import io
import json
import socket
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
import sys
import threading
import time
from urllib.parse import urlsplit

from ..codec import integer_json
from ..contracts import RunnerError
from ..service_contracts import ServiceError
from .authentication import AuthenticationError, SCOPES, _principal
from .service_client import MAX_WIRE_BYTES, decode_integer_json, validate_endpoint

_SERVICE_SCOPES = {"submit": "mas:submit", "inspect": "mas:read", "result": "mas:read",
                   "cancel": "mas:cancel", "update": "mas:update", "events": "mas:read",
                   "list": "mas:read", "page": "mas:read"}


class _DeadlineReader:
    """Socket reader whose header/body deadline is absolute, not inactivity."""
    def __init__(self, connection, deadline, header_limit):
        self.connection, self.deadline = connection, deadline
        self.header_limit, self.header_bytes = header_limit, 0
        self.buffer = bytearray()
        self.closed = False

    def _receive(self, maximum):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError()
        self.connection.settimeout(remaining)
        return self.connection.recv(maximum)

    def readline(self, limit=-1):
        maximum = min(65537, limit) if limit >= 0 else 65537
        while True:
            newline = self.buffer.find(b"\n", 0, maximum)
            if newline >= 0 or len(self.buffer) >= maximum:
                size = newline + 1 if newline >= 0 else maximum
                break
            chunk = self._receive(min(4096, maximum - len(self.buffer)))
            if not chunk:
                size = len(self.buffer)
                break
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        self.header_bytes += len(result)
        if self.header_bytes > self.header_limit:
            raise ServiceError("headers_too_large")
        return result

    def read(self, size=-1):
        if size < 0:
            raise ServiceError("invalid_content_length")
        while len(self.buffer) < size:
            chunk = self._receive(min(8192, size - len(self.buffer)))
            if not chunk:
                break
            self.buffer.extend(chunk)
        result = bytes(self.buffer[:size])
        del self.buffer[:size]
        return result

    def close(self):
        self.closed = True


class BridgeHTTPServer(ThreadingMixIn, HTTPServer):
    """At most max_connections concurrent bounded I/O handlers; no task workers."""
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, server_address, *, service, verifier, public_url,
                 mcp_bridge=None, a2a_bridge=None, allowed_hosts=None, allowed_origins=(),
                 request_timeout=30, max_body_bytes=MAX_WIRE_BYTES, max_response_bytes=MAX_WIRE_BYTES,
                 max_header_bytes=16384,
                 max_connections=8, allow_loopback_http=False, public_agent_card=True,
                 ssl_context=None):
        parsed = validate_endpoint(public_url, allow_loopback_http=allow_loopback_http)
        if parsed.path not in ("", "/", "/mcp"):
            raise ServiceError("invalid_public_url")
        if (type(request_timeout) not in (int, float) or not 0 < request_timeout <= 300
                or type(max_body_bytes) is not int or not 1024 <= max_body_bytes <= 1048576
                or type(max_response_bytes) is not int or not 1024 <= max_response_bytes <= MAX_WIRE_BYTES
                or type(max_header_bytes) is not int or not 1024 <= max_header_bytes <= 65536
                or type(max_connections) is not int or not 1 <= max_connections <= 64):
            raise ServiceError("invalid_server_configuration")
        self.service, self.verifier = service, verifier
        self.public_url = public_url.rstrip("/").removesuffix("/mcp")
        self.mcp_bridge, self.a2a_bridge = mcp_bridge, a2a_bridge
        self.allowed_hosts = frozenset(allowed_hosts or [parsed.netloc.lower()])
        if any(type(host) is not str or not host or any(ord(c) <= 32 for c in host)
               or "/" in host or "@" in host for host in self.allowed_hosts):
            raise ServiceError("invalid_server_configuration")
        self.allowed_origins = frozenset(allowed_origins)
        for origin in self.allowed_origins:
            origin_parts = validate_endpoint(origin, allow_loopback_http=allow_loopback_http)
            if origin_parts.path not in ("", "/"):
                raise ServiceError("invalid_server_configuration")
        self.request_timeout = request_timeout
        self.max_body_bytes, self.max_header_bytes = max_body_bytes, max_header_bytes
        self.max_response_bytes = max_response_bytes
        self.public_agent_card = bool(public_agent_card)
        self._slots = threading.BoundedSemaphore(max_connections)
        super().__init__(server_address, _Handler)
        if ssl_context is not None:
            # Handshake is deferred to the bounded handler, so it cannot block
            # the accept loop while other clients need status/cancellation.
            self.socket = ssl_context.wrap_socket(self.socket, server_side=True, do_handshake_on_connect=False)

    def process_request(self, request, client_address):
        if not self._slots.acquire(blocking=False):
            try:
                request.settimeout(0.1)
                request.sendall(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
            except OSError:
                pass
            self.shutdown_request(request)
            return
        try:
            super().process_request(request, client_address)
        except BaseException:
            self._slots.release()
            raise

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._slots.release()

    def handle_error(self, request, client_address):
        # Request headers/body and authentication errors never reach a log.
        pass


class _Handler(BaseHTTPRequestHandler):
    server_version = "MAS"
    sys_version = ""
    protocol_version = "HTTP/1.1"

    def setup(self):
        self.request.settimeout(self.server.request_timeout)
        super().setup()
        self.rfile.close()
        self.deadline = time.monotonic() + self.server.request_timeout
        self.rfile = _DeadlineReader(self.connection, self.deadline, self.server.max_header_bytes)

    def log_message(self, *_args):
        pass

    def handle_one_request(self):
        try:
            super().handle_one_request()
        except ServiceError as exc:
            self._error(431 if exc.code == "headers_too_large" else 400, exc.code)
        except (OSError, TimeoutError):
            self.close_connection = True

    def _send(self, status, body, *, headers=None):
        self.close_connection = True
        try:
            raw = integer_json(body, self.server.max_response_bytes).encode("utf-8") if body is not None else b""
        except RunnerError:
            status, raw = 500, b'{"error":{"code":"response_too_large"}}'
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Connection", "close")
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(raw)

    def _error(self, status, code, *, headers=None):
        # Initialise minimal state if failure occurred before header parsing.
        if not hasattr(self, "request_version"):
            self.request_version = "HTTP/1.1"
            self.command = ""
        self._send(status, {"error": {"code": code}}, headers=headers)

    def send_error(self, code, message=None, explain=None):
        self._error(code, "invalid_http_request")

    def _validate_headers(self):
        # Singletons are enforced before routing or authentication. Duplicate
        # values, transfer encodings and request smuggling are never tolerated.
        for key in ("Host", "Authorization", "Origin", "Content-Length", "Content-Type",
                    "MCP-Protocol-Version", "MCP-Method", "MCP-Name", "A2A-Version"):
            if len(self.headers.get_all(key, [])) > 1:
                raise ServiceError("duplicate_header")
        if (self.headers.get("Host", "").lower() not in self.server.allowed_hosts
                or "Transfer-Encoding" in self.headers or "Content-Encoding" in self.headers
                or "Expect" in self.headers):
            raise ServiceError("invalid_http_headers")
        origin = self.headers.get("Origin")
        if origin is not None and origin not in self.server.allowed_origins:
            raise ServiceError("origin_denied")
        if urlsplit(self.path).scheme or "?" in self.path or "#" in self.path:
            raise ServiceError("invalid_request_target")

    def _auth(self, scopes):
        authorization = self.headers.get("Authorization", "")
        if not authorization.startswith("Bearer ") or authorization.count(" ") != 1:
            raise AuthenticationError(required_scopes=scopes)
        return self.server.verifier.verify(authorization[7:], scopes)

    def _challenge(self, exc):
        status = 503 if exc.code == "authentication_unavailable" else (403 if exc.code == "insufficient_scope" else 401)
        challenge = 'Bearer error="' + ("insufficient_scope" if status == 403 else "invalid_token") + '"'
        if callable(getattr(self.server.verifier, "metadata", None)):
            challenge += ', resource_metadata="' + self.server.public_url + '/.well-known/oauth-protected-resource"'
        if exc.required_scopes:
            challenge += ', scope="' + " ".join(sorted(exc.required_scopes)) + '"'
        self._error(status, exc.code, headers={"WWW-Authenticate": challenge})

    def do_GET(self):
        try:
            self._validate_headers()
            if self.path in ("/.well-known/oauth-protected-resource", "/.well-known/oauth-protected-resource/mcp"):
                metadata = getattr(self.server.verifier, "metadata", None)
                if not callable(metadata):
                    raise ServiceError("not_found")
                return self._send(200, metadata())
            if self.path == "/.well-known/agent-card.json" and self.server.a2a_bridge is not None:
                if not self.server.public_agent_card:
                    self._auth({"mas:read"})
                return self._send(200, self.server.a2a_bridge.agent_card())
            self._auth({"mas:read"})
            self._error(405, "method_not_allowed", headers={"Allow": "POST"})
        except AuthenticationError as exc:
            self._challenge(exc)
        except ServiceError as exc:
            self._error(404 if exc.code == "not_found" else 400, exc.code)
        except Exception:
            self._error(500, "internal_error")

    def do_POST(self):
        try:
            self._validate_headers()
            # Authenticate before parsing task input or revealing any state.
            principal = self._auth(set())
            content_type = self.headers.get("Content-Type", "").lower().replace(" ", "")
            if content_type not in {"application/json", "application/json;charset=utf-8"}:
                raise ServiceError("unsupported_media_type")
            length = self.headers.get("Content-Length", "")
            if not length.isascii() or not length.isdecimal() or len(length) > 10:
                raise ServiceError("invalid_content_length")
            size = int(length)
            if size > self.server.max_body_bytes:
                raise ServiceError("request_too_large")
            raw = self.rfile.read(size)
            if len(raw) != size:
                raise ServiceError("invalid_content_length")
            try:
                payload = decode_integer_json(raw, max_bytes=self.server.max_body_bytes)
            except ServiceError:
                if self.path in {"/mcp", "/a2a"}:
                    return self._send(400, {"jsonrpc": "2.0", "id": None,
                        "error": {"code": -32700, "message": "Invalid JSON"}})
                raise
            if type(payload) is not dict:
                raise ServiceError("invalid_json")
            if time.monotonic() >= self.deadline:
                raise ServiceError("request_timeout")
            if self.path.startswith("/service/"):
                operation = self.path.removeprefix("/service/")
                scope = _SERVICE_SCOPES.get(operation)
                if scope is None:
                    raise ServiceError("not_found")
                principal.require({scope})
                value = _service_call(self.server.service, principal.name, operation, payload)
                return self._send(200, {"result": value})
            if self.path == "/mcp" and self.server.mcp_bridge is not None:
                principal.require({_mcp_scope(payload)})
                from .mcp import http_status
                value = self.server.mcp_bridge.handle(principal.name, payload, dict(self.headers.items()))
                return self._send(202 if value is None else http_status(value), value)
            if self.path == "/a2a" and self.server.a2a_bridge is not None:
                principal.require({_a2a_scope(payload)})
                value = self.server.a2a_bridge.handle(principal.name, payload,
                    {"a2a-version": self.headers.get("A2A-Version", "")})
                return self._send(200, value)
            raise ServiceError("not_found")
        except AuthenticationError as exc:
            self._challenge(exc)
        except ServiceError as exc:
            status = _service_status(exc.code)
            self._error(status, exc.code)
        except (OSError, TimeoutError):
            self.close_connection = True
        except Exception:
            self._error(500, "internal_error")

    def handle_expect_100(self):
        self._error(417, "expectation_not_supported")
        return False

    def do_OPTIONS(self):
        self._error(405, "method_not_allowed")


def _service_status(code):
    if code in {"not_found", "task_not_found", "workflow_not_found"}:
        return 404
    if code in {"forbidden", "principal_mismatch"}:
        return 403
    if code in {"idempotency_conflict", "request_key_conflict", "request_conflict", "task_not_ready", "invalid_state", "result_not_ready", "result_unavailable", "invalid_task_state"}:
        return 409
    if code in {"queue_full", "service_unavailable", "authentication_unavailable"}:
        return 503
    if code == "request_too_large":
        return 413
    if code == "unsupported_media_type":
        return 415
    if code == "request_timeout":
        return 408
    return 400


def _service_call(service, principal, operation, payload):
    required = {"submit": {"request_key", "workflow", "input"}, "inspect": {"task_id"},
                "result": {"task_id"}, "cancel": {"task_id"},
                "update": {"task_id", "request_key", "input"}, "events": {"task_id"},
                "list": set(), "page": set()}[operation]
    optional = {"submit": {"metadata"}, "update": {"metadata"}, "events": {"after"},
                "list": {"limit", "offset"},
                "page": {"page_size", "after", "state", "metadata", "updated_after"}}.get(operation, set())
    if not required.issubset(payload) or set(payload) - required - optional:
        raise ServiceError("invalid_parameters")
    method = getattr(service, {"list": "list_tasks", "page": "list_task_page"}.get(operation, operation))
    return method(principal, **payload)


def _mcp_scope(payload):
    method = payload.get("method")
    if method in {"tasks/cancel"}:
        return "mas:cancel"
    if method in {"tasks/update"}:
        return "mas:update"
    if method == "tools/call":
        params = payload.get("params")
        name = params.get("name") if type(params) is dict else None
        return {"mas.submit": "mas:submit", "mas.cancel": "mas:cancel", "mas.update": "mas:update"}.get(name, "mas:read")
    return "mas:read"


def _a2a_scope(payload):
    method = payload.get("method")
    if method in {"CancelTask", "tasks/cancel"}:
        return "mas:cancel"
    if method in {"SendMessage", "SendStreamingMessage", "message/send", "message/stream"}:
        params = payload.get("params")
        message = params.get("message") if type(params) is dict else None
        return "mas:update" if type(message) is dict and "taskId" in message else "mas:submit"
    return "mas:read"


def serve_stdio(bridge, principal, *, input_stream=None, output_stream=None, max_bytes=65536):
    """Serve bounded JSON lines as one trusted OS identity, never body identity."""
    _principal(principal)
    incoming = input_stream if input_stream is not None else sys.stdin.buffer
    outgoing = output_stream if output_stream is not None else sys.stdout.buffer
    while True:
        raw = incoming.readline(max_bytes + 1)
        if not raw:
            return
        if len(raw) > max_bytes:
            # Terminate on framing loss; never treat the tail as another request.
            raise ServiceError("request_too_large")
        try:
            message = decode_integer_json(raw, max_bytes=max_bytes)
            if type(message) is not dict:
                raise ServiceError("invalid_json")
            response = bridge.handle(principal, message)
        except ServiceError:
            response = {"jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Invalid JSON"}}
        if response is not None:
            outgoing.write(integer_json(response, max_bytes).encode() + b"\n")
            outgoing.flush()
