#!/usr/bin/env python3
"""Reproduce the paper's bounded checks without running deployment suites.

Requires Python 3.10+, Rust/Cargo (locked dependencies), SPIN, a C compiler and
Linux. Retains exact commands, logs, generated models and replayed trails; never
interprets a deliberate counterexample as a positive safety result.
"""
import argparse
import collections
import datetime
import hashlib
import json
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time
import zipfile

ROOT = Path(__file__).resolve().parents[2]


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def fixture_observations(name, output):
    if name == "star":
        result = output["result"]
        return {**result["usage"], "all_acknowledged": result["all_acknowledged"],
                "candidate_accepted": result["result"]["accepted"], "retained_events": len(result["events"])}
    if name == "recovery":
        return {"messages_charged": output["messages_charged"], "tasks": len(output["tasks"]),
                "states": [task["state"] for task in output["tasks"]],
                "closed_delivery_actionable": output["closed_delivery_actionable"],
                "retry_duplicate": output["identical_retry"]["duplicate"],
                "retained_sequence": output["identical_retry"]["sequence"]}
    return {key: output[key] for key in ("attempts_including_rejection", "authenticated_send", "forged_sender_rejected",
                                         "received_after_restart", "acknowledged", "restart_preserves_exhaustion")}


def render_results(report, path):
    """Render only recorded outcomes; expected failures remain separate."""
    counts = collections.Counter()
    for case in report["core_cases"]:
        if case["expected_counterexample"] is None:
            category = "positive"
        elif case["expected_counterexample"] != "acceptance_cycle":
            category = "assertion_defect"
        elif "UNBOUNDED_FAULTS" in case["definitions"]:
            category = "assumption_counterexample"
        else:
            category = "liveness_defect"
        counts[category] += len(case["checks"])
    counts.update(report["assessment"]["checks_by_category"])
    native = report["native_tests"]
    rows = [
        ("Executed Rust tests", f"{native['passed']} passed, {native['failed']} failed"),
        ("Ignored Rust model-suite entrypoints", str(native["ignored"])),
        ("Offline executable fixtures", f"{len(report['runtime_examples'])} completed"),
        ("Complete model searches with zero errors", str(counts["positive"])),
        ("Replayed assertion-defect counterexamples", str(counts["assertion_defect"])),
        ("Replayed liveness-defect counterexamples", str(counts["liveness_defect"])),
        ("Unlimited-fault progress counterexample", str(counts["assumption_counterexample"])),
        ("Replayed reachability witnesses", str(counts["reachability_witness"]))]
    limits = report["limits"]
    rust = report["versions"]["rustc"].split()[1]
    spin = report["versions"]["spin"].split()[2]
    compiler = report["versions"]["cc"].splitlines()[0].split()[-1]
    observed_date = datetime.date.fromisoformat(report["completed_utc"][:10])
    date = f"{observed_date.day} {observed_date.strftime('%B %Y')}"
    content = ["% Generated from verification.json by reproduce-evidence.py; do not edit.",
               r"\begin{table}[htbp]", r"\centering\small",
               r"\caption{Recorded verification outcomes. Counterexamples and witnesses are distinct from positive searches.}",
               r"\label{tab:verification-results}", r"\begin{tabularx}{\textwidth}{@{}Xr@{}}", r"\toprule",
               r"Evidence category & Result \\", r"\midrule"]
    content.extend(label + " & " + value + r" \\" for label, value in rows)
    content.extend([r"\bottomrule", r"\end{tabularx}", r"\end{table}"])
    star, recovery, gateway = (report["runtime_examples"][name]["observations"] for name in ("star", "recovery", "gateway"))
    content.extend([r"\begin{table}[htbp]", r"\centering\small",
                    r"\caption{Observed offline fixture outcomes with deterministic local participants.}",
                    r"\label{tab:runtime-examples}", r"\begin{tabularx}{\textwidth}{@{}lX@{}}", r"\toprule",
                    r"Fixture & Recorded observation \\", r"\midrule",
                    "Star & " + f"{star['messages']} accepted messages, {star['bytes']} charged bytes and "
                    f"{star['calls']} call reservations. All messages acknowledged. Checker accepted the proposed sum." + r" \\",
                    "Recovery & " + f"{recovery['messages_charged']} charged message and {recovery['tasks']} task expired with unknown outcome. "
                    f"Historical delivery marked non-actionable. Identical retry retained sequence {recovery['retained_sequence']}." + r" \\",
                    "Gateway & " + f"{gateway['attempts_including_rejection']} charged attempts, including a rejected sender forgery. "
                    "Message received and acknowledged after restart. Restart preserved the exhausted attempt budget." + r" \\",
                    r"\bottomrule", r"\end{tabularx}", r"\end{table}",
                    f"The {date} run used Rust/Cargo {rust}, SPIN {spin} and GCC {compiler}. "
                    f"Each model command had a {limits['seconds_per_core_command']}-second, "
                    f"{limits['memory_mib']}-MiB limit and depth {limits['search_depth']:,}. "
                    "Successful verification searches exhaustively explored the specified finite abstractions. "
                    "Defect counterexamples demonstrate detection of the introduced faults. "
                    "Under indefinitely recurring faults, the transport model contains an execution in which an accepted message remains pending indefinitely. "
                    "Witnesses establish reachability. These results support the declared communication and assessment controls, "
                    "with selected implementation tests supplying correspondence evidence."])
    path.write_text("\n".join(content) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=ROOT / "paper/evidence")
    parser.add_argument("--offline", action="store_true", help="Use cached Cargo dependencies only")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    manifest = out / "verification.json"
    report = {"schema": "mas-paper-verification/1", "status": "RUNNING_OR_INTERRUPTED",
              "started_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
              "source_commit": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
              "scope": "Default-feature Rust tests; offline star, recovery and gateway fixtures; selected transport, task, transaction, expiry models; assessment profile subset",
              "limits": {"seconds_per_core_command": 30, "memory_mib": 512, "search_depth": 100000},
              "commands": [], "core_cases": []}
    source_paths = subprocess.check_output(
        ["git", "ls-files", "rust-protocol", "formal", "examples"], cwd=ROOT, text=True).splitlines()
    report["source_sha256"] = {p: sha(ROOT / p) for p in source_paths}
    tracked_diff = subprocess.check_output(["git", "diff", "HEAD", "--", "rust-protocol", "formal", "examples"], cwd=ROOT)
    report["checked_sources_match_commit"] = not bool(tracked_diff)
    manifest.write_text(json.dumps(report, indent=2) + "\n")

    with tempfile.TemporaryDirectory(prefix="mas-paper-") as temporary:
        work = Path(temporary)

        def run(argv, cwd, name, seconds=30, limited=False):
            log = cwd / (name + ".log")
            effective = (["sh", "-c", 'ulimit -v "$1" || exit 125; shift; exec "$@"',
                          "paper-limit", str(512 * 1024), *argv] if limited else argv)
            started = time.monotonic()
            with log.open("w") as stream:
                try:
                    result = subprocess.run(effective, cwd=cwd, stdout=stream, stderr=subprocess.STDOUT,
                                            timeout=seconds, check=False)
                    code, timeout = result.returncode, False
                except subprocess.TimeoutExpired:
                    code, timeout = None, True
            text = log.read_text()
            record = {"argv": argv, "cwd": str(cwd), "returncode": code, "timeout": timeout,
                      "elapsed_seconds": time.monotonic() - started, "log": str(log.relative_to(work)),
                      "log_sha256": sha(log), "memory_limited": limited}
            report["commands"].append(record)
            if code != 0 or timeout:
                raise RuntimeError(f"Command failed or timed out: {argv}; see {log.name}")
            return text

        def core_case(name, source, definitions=(), properties=("safety",), no_reduce=False, expected=None):
            directory = work / "core" / name
            directory.mkdir(parents=True)
            model = directory / "model.pml"
            shutil.copyfile(source, model)
            defs = ["-D" + x for x in definitions]
            run(["spin", *defs, "-a", "model.pml"], directory, "generate", limited=True)
            checks = []
            for prop in properties:
                flags = ["-DSAFETY", "-DNOCLAIM"] if prop == "safety" else []
                run(["cc", "-O2", "-DMEMLIM=480", *( ["-DNOREDUCE"] if no_reduce else []),
                     *flags, "-o", "pan-" + prop, "pan.c"], directory, "compile-" + prop, limited=True)
                check = run(["./pan-" + prop, "-b", "-m100000", "-w20",
                             *([] if prop == "safety" else ["-a", "-N", prop])],
                            directory, "check-" + prop, limited=True)
                errors = re.search(r"errors:\s*(\d+)", check)
                incomplete = any(s in check.lower() for s in
                                 ("max search depth", "out of memory", "not enough memory", "vectorsz", "reached -dmemlim"))
                if errors is None or incomplete or "Full statespace search" not in check:
                    raise RuntimeError(f"Incomplete or unrecognised search: {name}/{prop}")
                count = int(errors.group(1))
                if expected is None:
                    if count or "Warning: Search not completed" in check:
                        raise RuntimeError(f"Positive search failed or incomplete: {name}/{prop}")
                    status = "COMPLETE_NO_ERRORS"
                else:
                    if count == 0:
                        raise RuntimeError(f"Expected counterexample absent: {name}/{prop}")
                    replay = run(["spin", *defs, "-t", "-p", "-g", "-l", "model.pml"],
                                 directory, "replay-" + prop, limited=True)
                    if expected == "acceptance_cycle":
                        if "acceptance cycle" not in check or "<<<<<START OF CYCLE>>>>>" not in replay or prop not in replay:
                            raise RuntimeError(f"Wrong or unreplayed liveness counterexample: {name}/{prop}")
                    elif "assertion violated" not in replay or expected not in replay:
                        raise RuntimeError(f"Wrong or unreplayed assertion counterexample: {name}/{prop}")
                    status = "EXPECTED_COUNTEREXAMPLE_REPLAYED"
                states = re.search(r"([\d.e+]+) states, stored", check)
                checks.append({"property": prop, "status": status,
                               "stored_states": float(states.group(1)) if states else None})
            result = {"name": name, "model_sha256": sha(model), "definitions": list(definitions),
                      "partial_order_reduction": not no_reduce, "expected_counterexample": expected,
                      "checks": checks}
            report["core_cases"].append(result)
            print(name + ": " + ", ".join(c["status"] for c in checks), flush=True)

        try:
            report["versions"] = {name: run(cmd, work, name).strip() for name, cmd in
                                  [("rustc", ["rustc", "--version"]), ("cargo", ["cargo", "--version"]),
                                   ("spin", ["spin", "-V"]), ("cc", ["cc", "--version"]),
                                   ("os", ["uname", "-srm"])]}
            cargo = ["cargo", "test", "--locked", *( ["--offline"] if args.offline else []),
                     "--manifest-path", str(ROOT / "rust-protocol/Cargo.toml")]
            native = run(cargo, work, "rust-tests", seconds=600)
            results = re.findall(r"test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored", native)
            if not results:
                raise RuntimeError("No Rust test summaries found")
            report["native_tests"] = dict(zip(("passed", "failed", "ignored"),
                                              map(sum, zip(*(map(int, r) for r in results)))))
            cargo_options = ["--locked", *( ["--offline"] if args.offline else []),
                             "--manifest-path", str(ROOT / "rust-protocol/Cargo.toml")]
            run(["cargo", "build", *cargo_options, "--bins", "--examples"], work, "build-examples", seconds=600)
            commands = {
                "star": ["cargo", "run", "--quiet", *cargo_options, "--bin", "mas", "--", "demo", str(ROOT / "examples/star.json")],
                "recovery": ["cargo", "run", "--quiet", *cargo_options, "--example", "recovery"],
                "gateway": ["cargo", "run", "--quiet", *cargo_options, "--example", "gateway"]}
            report["runtime_examples"] = {}
            for name, command in commands.items():
                output = json.loads(run(command, work, "example-" + name, seconds=120))
                (work / ("example-" + name + ".json")).write_text(json.dumps(output, indent=2) + "\n")
                if name == "star":
                    result = output["result"]
                    valid = output["ok"] and result["all_acknowledged"] and result["result"]["accepted"] and all(
                        event["identical_resend"] and event["restart_redelivery"] and event["acknowledged"]
                        for event in result["events"])
                elif name == "recovery":
                    valid = (output["deadline_observed_after_restart"] and output["closed_delivery_actionable"] is False
                             and output["messages_charged"] == 1 and output["identical_retry"]["duplicate"]
                             and all(task["state"] == "expired_outcome_unknown" for task in output["tasks"]))
                else:
                    valid = all(output[field] is True for field in
                                ("authenticated_send", "forged_sender_rejected", "received_after_restart", "acknowledged", "restart_preserves_exhaustion"))
                if not valid:
                    raise RuntimeError(f"Runtime example result failed acceptance: {name}")
                report["runtime_examples"][name] = {"status": "PASS", "record": "example-" + name + ".json",
                                                   "sha256": sha(work / ("example-" + name + ".json")),
                                                   "observations": fixture_observations(name, output)}
            base = ["cargo", "run", "--locked", *( ["--offline"] if args.offline else []),
                    "--manifest-path", str(ROOT / "rust-protocol/Cargo.toml"), "--bin", "verify", "--"]
            run([*base, "--assessments-only", "--out", str(work / "assessments")], work, "assessment-run", seconds=1200)
            assessment = json.loads((work / "assessments/summary.json").read_text())
            if assessment["status"] != "PASS":
                raise RuntimeError("Assessment checks did not pass")
            categories = collections.Counter()
            for case in assessment["cases"]:
                category = ("reachability_witness" if case["purpose"] == "reachability_witness" else
                            "positive" if case["defect"] is None else
                            "liveness_defect" if case["defect"] == "BUG_REVISION_STALL" else "assertion_defect")
                categories[category] += len(case["checks"])
            report["assessment"] = {"status": assessment["status"], "cases": len(assessment["cases"]),
                                    "checks_by_category": dict(categories),
                                    "checks_by_status": dict(collections.Counter(c["status"] for case in assessment["cases"] for c in case["checks"])),
                                    "cases_by_purpose": dict(collections.Counter(c["purpose"] for c in assessment["cases"])),
                                    "contract": assessment["contract"]}
            generated = work / "generated"
            run([*base, "--generate-only", "--only", "star", "--out", str(generated)], work, "generate-star", seconds=120)
            star = generated / "star.pml"
            progress = ("safety", "message_0_progress", "message_1_progress")
            core_case("star", star, properties=progress)
            core_case("star-no-reduction", star, properties=progress, no_reduce=True)
            core_case("star-capacity-one", star, ("CAPACITY=1",), properties=progress)
            core_case("star-unlimited-faults", star, ("UNBOUNDED_FAULTS",))
            core_case("star-early-ack-defect", star, ("BUG_EARLY_ACK",), expected="assert((!(acknowledged[0])||received[0]))")
            core_case("star-forbidden-route-defect", star, ("BUG_ALLOW_FORBIDDEN",), expected="assert(allowed[((actor*3)+recipient[0])])")
            core_case("star-no-ack-defect", star, ("BUG_NO_ACK",), properties=("message_0_progress",), expected="acceptance_cycle")
            core_case("star-unlimited-fault-progress-counterexample", star, ("UNBOUNDED_FAULTS",),
                      properties=("message_0_progress",), expected="acceptance_cycle")
            for filename, defects in [
                ("task-lifecycle", [("BUG_DOUBLE_TERMINAL", "assert((terminal_count[task]==1))")]),
                ("transaction-recovery", [("BUG_PARTIAL_COMMIT", "assert((stored==(committed[0]+committed[1])))"),
                                           ("BUG_DUPLICATE_INSERT", "assert((stored==(committed[0]+committed[1])))")]),
                ("task-expiry", [("BUG_EXPIRY_OVERWRITE", "assert((terminal_count[0]==1))")])]:
                source = ROOT / "formal" / (filename + ".pml")
                core_case(filename, source)
                for defect, expression in defects:
                    core_case(filename + "-" + defect.lower(), source, (defect,), expected=expression)
            report["core_checks_by_status"] = dict(collections.Counter(c["status"] for case in report["core_cases"] for c in case["checks"]))
            report["limitations"] = [
                "Finite abstractions are checked, with no proof of unrestricted implementation refinement.",
                "Default-feature tests exclude PostgreSQL integration and ignored formal entrypoints.",
                "Host and example repositories, deployment, external effects and model answer quality were not evaluated.",
                "Elapsed times document this execution only and are not comparative performance measurements."]
            report["status"] = "PASS"
        except Exception as error:
            report["status"] = "FAIL_OR_INCONCLUSIVE"
            report["error"] = str(error)
            raise
        finally:
            report["completed_utc"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
            archive = out / "raw-checks.zip"
            with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as zipped:
                for path in sorted(work.rglob("*")):
                    if path.is_file() and path.suffix in (".log", ".pml", ".trail", ".json"):
                        zipped.write(path, path.relative_to(work))
            report["raw_evidence"] = {"path": archive.name, "sha256": sha(archive)}
            manifest.write_text(json.dumps(report, indent=2) + "\n")
            if report["status"] == "PASS":
                render_results(report, out / "results.tex")
            print(json.dumps({k: report.get(k) for k in ("status", "native_tests", "core_checks_by_status", "error")}), flush=True)


if __name__ == "__main__":
    main()
