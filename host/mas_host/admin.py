"""Operator commands for one bounded, private MAS deployment."""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import signal
import sqlite3
import threading
import time

from .operations import (DeploymentLedger, DeploymentPolicy, OperationsError,
                         backup_state, prune_runs, reconcile_run, reconcile_state,
                         restore_state, retained_runs)


def _object_file(path: str) -> dict:
    source = Path(path).read_text(encoding="utf-8")
    if len(source.encode("utf-8")) > 65_536:
        raise OperationsError("operator JSON file exceeds input limit")
    result = json.loads(source)
    if type(result) is not dict:
        raise OperationsError("operator JSON input must be an object")
    return result


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    root.add_argument("--state-dir", default=os.environ.get("MAS_STATE_DIR", ".mas-state"))
    commands = root.add_subparsers(dest="command", required=True)
    init = commands.add_parser("init", help="create an immutable deployment policy before the first run")
    init.add_argument("--policy", help="JSON object of DeploymentPolicy integer limits")
    commands.add_parser("health", help="show lifetime quotas and current disk thresholds")
    commands.add_parser("runs", help="list retained run identities and directories")
    inspect = commands.add_parser("inspect", help="inspect durable workflow and invocation checkpoints")
    inspect.add_argument("run_dir")
    reconcile = commands.add_parser("reconcile", help="reconcile protocol expiry for one run or all retained runs")
    reconcile.add_argument("--run-dir")
    watch = commands.add_parser("watch", help="independently reconcile expiry until SIGTERM/SIGINT")
    watch.add_argument("--interval", type=float, default=5, help="seconds between scans (0.1 to 60)")
    watch.add_argument("--iterations", type=int, default=0, help="stop after this many scans; zero means until signalled")
    resolution = commands.add_parser("resolve", help="record an operator-supported external invocation outcome")
    resolution.add_argument("run_dir")
    resolution.add_argument("call_id")
    resolution.add_argument("--decision", choices=("completed", "confirmed_no_effect"), required=True)
    resolution.add_argument("--evidence", required=True, help="private JSON object documenting the basis for the decision")
    resolution.add_argument("--output", help="verified JSON object; required for a completed invocation")
    backup = commands.add_parser("backup", help="snapshot a quiescent deployment into a private empty directory")
    backup.add_argument("destination")
    restore = commands.add_parser("restore", help="verify and restore a snapshot into an empty state directory")
    restore.add_argument("bundle")
    prune = commands.add_parser("prune", help="list terminal runs eligible for retention deletion")
    prune.add_argument("--older-than-seconds", type=int, required=True)
    prune.add_argument("--apply", action="store_true", help="delete eligible files while preserving charged admissions")
    return root


def _watch(state_dir: Path, interval: float, iterations: int) -> dict:
    if not 0.1 <= interval <= 60 or iterations < 0:
        raise OperationsError("watch interval must be 0.1 to 60 seconds; iterations must be nonnegative")
    ledger = DeploymentLedger(state_dir)
    stopping = threading.Event()
    previous = {}
    if threading.current_thread() is threading.main_thread():
        for sig in (signal.SIGTERM, signal.SIGINT):
            previous[sig] = signal.signal(sig, lambda *_: stopping.set())
    count = 0
    try:
        while not stopping.is_set():
            report = reconcile_state(state_dir, os.environ.get("MAS_BIN", "mas"))
            print(json.dumps({"event": "reconciliation", "timestamp": int(time.time()), "health": ledger.health(), **report}, sort_keys=True), flush=True)
            count += 1
            if iterations and count >= iterations:
                break
            stopping.wait(interval)
    finally:
        for sig, handler in previous.items():
            signal.signal(sig, handler)
    return {"ok": True, "scans": count, "stopped": stopping.is_set()}


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    state_dir = Path(args.state_dir)
    try:
        if args.command == "init":
            policy = DeploymentPolicy(**_object_file(args.policy)) if args.policy else None
            result = {"ok": True, "health": DeploymentLedger(state_dir, policy).health()}
        elif args.command == "health":
            result = {"ok": True, "health": DeploymentLedger(state_dir).health()}
        elif args.command == "runs":
            result = {"ok": True, "runs": retained_runs(state_dir)}
        elif args.command == "inspect":
            from .journal import inspect_run
            result = {"ok": True, "run": inspect_run(Path(args.run_dir))}
        elif args.command == "reconcile":
            result = ({"ok": True, **reconcile_run(Path(args.run_dir), os.environ.get("MAS_BIN", "mas"))}
                      if args.run_dir else reconcile_state(state_dir, os.environ.get("MAS_BIN", "mas")))
        elif args.command == "watch":
            result = _watch(state_dir, args.interval, args.iterations)
        elif args.command == "resolve":
            from .journal import resolve_invocation
            output = _object_file(args.output) if args.output else None
            result = {"ok": True, "resolution": resolve_invocation(Path(args.run_dir), args.call_id,
                      decision=args.decision, evidence=_object_file(args.evidence), output=output)}
        elif args.command == "backup":
            result = backup_state(state_dir, Path(args.destination))
        elif args.command == "restore":
            result = restore_state(Path(args.bundle), state_dir)
        elif args.command == "prune":
            result = prune_runs(state_dir, args.older_than_seconds, args.apply)
        else:
            raise OperationsError("unsupported operator command")
    except OperationsError as error:
        result = {"ok": False, "error": str(error)}
    except Exception:
        # Journal/provider data and filesystem names must not leak through an
        # arbitrary exception traceback on a shared operator console.
        result = {"ok": False, "error": "operator action failed; inspect private state and command arguments"}
    print(json.dumps(result, sort_keys=True, separators=(",", ":")), flush=True)
    return 0 if result.get("ok") is True else 1
