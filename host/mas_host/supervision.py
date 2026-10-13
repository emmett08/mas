"""Killable Linux adapter execution with parent-owned protocol operations.

Adapters are trusted code, and a fork is not a security sandbox: inherited
memory includes host state. Only bounded integer JSON crosses the local IPC
channel. The single-threaded parent retains admission and checkpoint authority.
Terminating local execution cannot establish cancellation of a remote effect.
"""

from __future__ import annotations

import ctypes
import json
import math
import os
import select
import signal
import socket
import sys
import threading
import time
from typing import Callable

from .runner import AdapterError, Invocation, Outcome, RecoveryRequired, RunnerError, _json


MAX_FRAME = 65_536
_CHANNEL_FDS: set[int] = set()


class AdapterTimeout(AdapterError):
    """The local handler exceeded its inherited deadline."""


class AdapterInterrupted(AdapterError):
    """The host received a shutdown signal during adapter execution."""


def _invalid() -> None:
    raise AdapterError("invalid adapter IPC frame")


def _decode(data: bytes) -> dict:
    def pairs(items):
        value = {}
        for key, item in items:
            if key in value:
                _invalid()
            value[key] = item
        return value

    def reject(_value):
        _invalid()

    try:
        value = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                           parse_float=reject, parse_constant=reject)
        if type(value) is not dict:
            _invalid()
        _json(value, MAX_FRAME - 1)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise AdapterError("invalid adapter IPC frame") from None


def _fields(frame: dict, names: set[str]) -> None:
    if set(frame) != names:
        _invalid()


class _Channel:
    def __init__(self, sock: socket.socket, deadline: float):
        self.sock = sock
        self.deadline = deadline
        self.buffer = bytearray()

    def remaining(self) -> float:
        value = self.deadline - time.monotonic()
        if value <= 0:
            raise AdapterTimeout("adapter deadline exceeded; outcome may be unknown")
        return value

    def send(self, value: dict) -> None:
        source = (_json(value, MAX_FRAME - 1) + "\n").encode("utf-8")
        try:
            self.sock.settimeout(self.remaining())
            self.sock.sendall(source)
        except (OSError, TimeoutError):
            raise AdapterError("adapter IPC interrupted; outcome may be unknown") from None

    def receive(self, tick: Callable[[], None] | None = None,
                poll_interval: float = 0.05) -> dict:
        while True:
            remaining = self.remaining()
            newline = self.buffer.find(b"\n")
            if newline >= 0:
                source = bytes(self.buffer[:newline])
                del self.buffer[:newline + 1]
                return _decode(source)
            if len(self.buffer) >= MAX_FRAME:
                _invalid()
            if tick is not None:
                tick()
                remaining = self.remaining()
            try:
                readable, _, _ = select.select([self.sock], [], [],
                                                min(remaining, poll_interval))
                if not readable:
                    continue
                data = self.sock.recv(min(4096, MAX_FRAME - len(self.buffer)))
            except (OSError, TimeoutError):
                raise AdapterError("adapter IPC interrupted; outcome may be unknown") from None
            if not data:
                raise AdapterError("adapter exited without a result; outcome may be unknown")
            self.buffer.extend(data)


class _ContextProxy:
    """A child-side context; callables remain in the child, never in IPC."""

    def __init__(self, channel: _Channel, actor: str):
        self._channel = channel
        self._actor = actor
        self._thread = threading.get_ident()
        self._invoking = False
        self._invocation_id: str | None = None

    @property
    def actor(self) -> str:
        return self._actor

    @property
    def remaining_seconds(self) -> float:
        return max(0.0, self._channel.deadline - time.monotonic())

    @property
    def invocation_id(self) -> str | None:
        """Durable attempt identifier; diagnostic correlation, not idempotency."""
        return self._invocation_id

    def _check(self) -> None:
        if self._invoking or threading.get_ident() != self._thread:
            raise RunnerError("adapter context is not active")
        self._channel.remaining()

    def request(self, recipient: str, operation: str, input: dict) -> Outcome:
        self._check()
        self._channel.send({"op": "request", "recipient": recipient,
                            "operation": operation, "input": input})
        reply = self._channel.receive()
        _fields(reply, {"output", "status"})
        return Outcome(reply["output"], reply["status"])

    def invoke(self, call, *, input: dict | None = None,
               reservation_microusd: int | None = None) -> dict:
        self._check()
        self._channel.send({"op": "prepare", "input": input,
                            "reservation_microusd": reservation_microusd})
        reply = self._channel.receive()
        _fields(reply, {"execute", "output", "call_id"})
        if reply["execute"] is False:
            if type(reply["output"]) is not dict:
                _invalid()
            return reply["output"]
        if (reply["execute"] is not True or reply["output"] is not None
                or type(reply["call_id"]) is not str or not reply["call_id"]):
            _invalid()
        try:
            self._invoking = True
            self._invocation_id = reply["call_id"]
            invocation = call()
            if (not isinstance(invocation, Invocation)
                    or type(invocation.output) is not dict
                    or type(invocation.evidence) is not dict):
                raise AdapterError("adapter invocation returned an invalid result")
            self._channel.send({"op": "complete", "output": invocation.output,
                                "evidence": invocation.evidence})
            result = self._channel.receive()
            _fields(result, {"output"})
            if type(result["output"]) is not dict:
                _invalid()
            return result["output"]
        except Exception:
            # No exception text, keys, or provider response bodies become
            # diagnostics. If this frame cannot be sent the parent still holds
            # the outstanding ticket and records the same uncertainty.
            try:
                self._channel.send({"op": "failed"})
            except Exception:
                pass
            raise AdapterError("adapter invocation failed; outcome may be unknown") from None
        finally:
            self._invoking = False
            self._invocation_id = None


def _child(channel: _Channel, adapter, actor: str, request, parent_pid: int) -> None:
    try:
        os.setsid()
        signal.signal(signal.SIGTERM, signal.SIG_DFL)
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        # Linux clears PDEATHSIG in a forked child; set it before any adapter
        # code and close the race where the parent died before this call.
        libc = ctypes.CDLL(None, use_errno=True)
        if libc.prctl(1, signal.SIGKILL, 0, 0, 0) != 0:
            raise AdapterError("could not supervise adapter lifetime")
        if os.getppid() != parent_pid:
            os.kill(os.getpid(), signal.SIGKILL)
        outcome = adapter.handle(_ContextProxy(channel, actor), request)
        if (not isinstance(outcome, Outcome) or type(outcome.output) is not dict
                or outcome.status not in ("completed", "unresolved")):
            raise AdapterError("adapter returned an invalid outcome")
        channel.send({"op": "result", "output": outcome.output, "status": outcome.status})
    except BaseException:
        try:
            channel.send({"op": "error"})
        except BaseException:
            pass
    finally:
        channel.sock.close()
        # Do not flush or run copied parent atexit handlers after fork.
        os._exit(0)


def _stop(pid: int) -> None:
    # Kill the process group even after a normal result: an adapter must not
    # leave background work behind. Descendants which deliberately create a
    # new session are outside this trusted-adapter contract.
    try:
        os.killpg(pid, signal.SIGKILL)
    except ProcessLookupError:
        try:
            os.kill(pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    while True:
        try:
            os.waitpid(pid, 0)
            return
        except InterruptedError:
            continue
        except ChildProcessError:
            return


def execute(adapter, context, request, *, deadline: float,
            tick: Callable[[], None] | None = None, poll_interval: float = 0.05) -> Outcome:
    """Supervise one handler until an absolute monotonic run deadline.

    ``context`` stays in the parent. Its request/prepare/complete/fail methods
    are authoritative and their exceptions propagate without being rewritten.
    Nested execution must receive the same or an earlier absolute deadline.
    The caller must bound administrative commands and its reconciliation tick.
    SIGTERM/SIGINT stop local work and leave pending effects uncertain.
    """
    if (not sys.platform.startswith("linux") or threading.active_count() != 1
            or threading.current_thread() is not threading.main_thread()):
        raise RunnerError("supervised adapters require a single-threaded Linux host")
    if (type(deadline) not in (int, float) or not math.isfinite(deadline)
            or type(poll_interval) not in (int, float)
            or not math.isfinite(poll_interval) or not 0 < poll_interval <= 1):
        raise RunnerError("invalid adapter supervision deadline or poll interval")
    if deadline <= time.monotonic():
        raise AdapterTimeout("adapter deadline exceeded; outcome may be unknown")
    parent_sock, child_sock = socket.socketpair()
    inherited_channels = _CHANNEL_FDS.copy()
    parent_fd = parent_sock.fileno()
    _CHANNEL_FDS.add(parent_fd)
    channel = _Channel(parent_sock, deadline)
    pid = None
    ticket = None
    previous = {}

    def interrupt(_number, _frame):
        raise AdapterInterrupted("host shutdown interrupted adapter; outcome may be unknown")

    try:
        for number in (signal.SIGTERM, signal.SIGINT):
            previous[number] = signal.signal(number, interrupt)
        parent_pid = os.getpid()
        pid = os.fork()
        if pid == 0:
            parent_sock.close()
            for fd in inherited_channels:
                os.close(fd)
            _child(_Channel(child_sock, deadline), adapter, context.actor, request, parent_pid)
        child_sock.close()
        while True:
            frame = channel.receive(tick, poll_interval)
            operation = frame.get("op")
            if operation == "complete":
                _fields(frame, {"op", "output", "evidence"})
                if ticket is None or type(frame["output"]) is not dict or type(frame["evidence"]) is not dict:
                    _invalid()
                output = context.complete_invocation(ticket, Invocation(frame["output"], frame["evidence"]))
                ticket = None
                channel.send({"output": output})
            elif operation == "failed":
                _fields(frame, {"op"})
                if ticket is None:
                    _invalid()
                raise AdapterError("adapter invocation failed; outcome may be unknown")
            elif ticket is not None:
                _invalid()
            elif operation == "prepare":
                _fields(frame, {"op", "input", "reservation_microusd"})
                amount = frame["reservation_microusd"]
                if (frame["input"] is not None and type(frame["input"]) is not dict
                        or amount is not None and (type(amount) is not int or amount < 0)):
                    _invalid()
                prepared = context.prepare_invocation(input=frame["input"],
                                                     reservation_microusd=frame["reservation_microusd"])
                if prepared.execute:
                    ticket = prepared
                    channel.send({"execute": True, "output": None, "call_id": prepared.call_id})
                else:
                    channel.send({"execute": False, "output": prepared.cached_output,
                                  "call_id": prepared.call_id})
            elif operation == "request":
                _fields(frame, {"op", "recipient", "operation", "input"})
                if (type(frame["recipient"]) is not str or type(frame["operation"]) is not str
                        or type(frame["input"]) is not dict):
                    _invalid()
                outcome = context.request(frame["recipient"], frame["operation"], frame["input"])
                channel.send({"output": outcome.output, "status": outcome.status})
            elif operation == "result":
                _fields(frame, {"op", "output", "status"})
                if type(frame["output"]) is not dict or frame["status"] not in ("completed", "unresolved"):
                    _invalid()
                return Outcome(frame["output"], frame["status"])
            elif operation == "error":
                _fields(frame, {"op"})
                raise AdapterError("adapter execution failed; inspect run status")
            else:
                _invalid()
    except Exception:
        if ticket is not None:
            # A terminal Failure would prevent an operator from resolving and
            # resuming this invocation. Keep its workflow open; the finally
            # block stops local work and marks the ticket unknown first.
            raise RecoveryRequired("adapter invocation interrupted; inspect recovery journal") from None
        raise
    finally:
        # Repeated shutdown signals must not interrupt child cleanup. Restore
        # the caller's handlers afterwards, including nested supervision.
        for number in previous:
            signal.signal(number, signal.SIG_IGN)
        try:
            if pid is not None and pid > 0:
                _stop(pid)
            if ticket is not None:
                try:
                    context.fail_invocation(ticket)
                except Exception:
                    pass  # A durable outstanding reservation is still unknown.
        finally:
            _CHANNEL_FDS.discard(parent_fd)
            parent_sock.close()
            child_sock.close()
            for number, handler in previous.items():
                signal.signal(number, handler)
