#!/usr/bin/env python3
"""Check finite dispatch safety, mechanism mutations and reachable positives."""
import hashlib
import json
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parent
MODEL = ROOT / "native-dispatch.pml"


def run(args, cwd):
    result = subprocess.run(args, cwd=cwd, text=True, capture_output=True, timeout=30)
    if result.returncode:
        raise RuntimeError(f"command failed: {args!r}\n{result.stdout}\n{result.stderr}")
    return result.stdout + result.stderr


def main():
    cases = []
    for flag in [None, "MUT_REPLAY_PERMISSION", "MUT_POLICY_CAS", "MUT_OCCUPANCY", "WITNESS_DISPATCH", "WITNESS_AMENDMENT"]:
        with tempfile.TemporaryDirectory(prefix="mas-native-dispatch-") as directory:
            path = Path(directory)
            (path / MODEL.name).write_bytes(MODEL.read_bytes())
            define = [f"-D{flag}"] if flag else []
            generation = run(["spin", *define, "-a", MODEL.name], path)
            compilation = run(["cc", "-O2", "-DSAFETY", "-DNOCLAIM", "-o", "pan", "pan.c"], path)
            output = run(["./pan", "-b", "-m100000"], path)
            match = re.search(r"errors:\s*(\d+)", output)
            if not match or (not flag and "Search not completed" in output) or "max search depth too small" in output:
                raise RuntimeError(f"incomplete search: {flag}\n{output}")
            errors = int(match.group(1))
            if (errors > 0) != bool(flag):
                raise RuntimeError(f"unexpected check result: {flag}\n{output}")
            replay = run(["spin", *define, "-t", "-p", "-g", MODEL.name], path) if errors else ""
            if errors and "assertion violated" not in replay:
                raise RuntimeError(f"counterexample did not replay: {flag}\n{replay}")
            cases.append({"case": flag or "safety", "errors": errors, "generation": generation,
                          "compilation": compilation, "search": output, "replay": replay,
                          "trail": (path / (MODEL.name + ".trail")).read_text() if errors else ""})
    print(json.dumps({"model_sha256": hashlib.sha256(MODEL.read_bytes()).hexdigest(),
                      "spin": run(["spin", "-V"], ROOT).strip(),
                      "compiler": run(["cc", "--version"], ROOT).splitlines()[0],
                      "cases": cases}, indent=2))


if __name__ == "__main__":
    main()
