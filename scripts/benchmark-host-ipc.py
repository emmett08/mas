#!/usr/bin/env python3
"""Bounded local status-path comparison; reports measurements, asserts no speedup.

Run after cargo build --release --locked --manifest-path rust-protocol/Cargo.toml.
The workload uses no provider, network, persistent evidence or credentials.
"""
import argparse
import json
from pathlib import Path
import resource
import statistics
import sys
import tempfile
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "host"))
from mas_host import Outcome, Runner
from mas_host.protocol_client import PersistentProtocolClient, ProtocolClient
import subprocess


class Echo:
    def handle(self, _context, request):
        return Outcome(request.input)


def measure(strategy, samples):
    binaries = ROOT / "rust-protocol" / "target" / "release"
    client = strategy(str(binaries / "mas"), str(binaries / "mas-gateway"))
    spec = {"agents": ["a", "b"], "edges": [["a", "b"], ["b", "a"]],
            "application": "agent-task/1", "mailbox_capacity": 10,
            "max_messages": 100, "max_bytes": 100_000, "max_calls": 10, "wall_seconds": 120}
    with tempfile.TemporaryDirectory() as directory:
        try:
            with Runner(spec, {"a": Echo(), "b": Echo()}, Path(directory),
                        protocol_client=client, supervised=False) as runner:
                runner.request("a", "b", "echo", {"value": 1})
                launches = []
                original = subprocess.Popen

                def start(*args, **kwargs):
                    launches.append(True)
                    return original(*args, **kwargs)

                cpu_before = resource.getrusage(resource.RUSAGE_SELF)
                child_before = resource.getrusage(resource.RUSAGE_CHILDREN)
                elapsed = []
                with patch("mas_host.protocol_client.subprocess.Popen", side_effect=start):
                    for _ in range(samples):
                        started = time.perf_counter_ns()
                        snapshot = runner.status()
                        elapsed.append((time.perf_counter_ns() - started) / 1e6)
                cpu_after = resource.getrusage(resource.RUSAGE_SELF)
                child_after = resource.getrusage(resource.RUSAGE_CHILDREN)
                return {"strategy": strategy.__name__, "samples": samples,
                        "status_process_launches": len(launches),
                        "median_ms": statistics.median(elapsed),
                        "p95_ms": sorted(elapsed)[int((samples - 1) * 0.95)],
                        "total_ms": sum(elapsed), "response_bytes": len(json.dumps(snapshot)),
                        "host_cpu_seconds": (cpu_after.ru_utime + cpu_after.ru_stime
                                             - cpu_before.ru_utime - cpu_before.ru_stime),
                        "reaped_child_cpu_seconds": (child_after.ru_utime + child_after.ru_stime
                                                     - child_before.ru_utime - child_before.ru_stime),
                        "cpu_note": "Persistent child CPU is not reaped within measurement; these CPU totals are not comparable."}
        finally:
            if hasattr(client, "close"):
                client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples", type=int, default=100)
    args = parser.parse_args()
    if not 5 <= args.samples <= 1000:
        parser.error("samples must be from 5 to 1000")
    print(json.dumps({"workload": "completed-run status; one two-actor echo exchange",
        "results": [measure(strategy, args.samples) for strategy in (ProtocolClient, PersistentProtocolClient)]}, indent=2))


if __name__ == "__main__":
    main()
