#!/usr/bin/env python3
"""Verify the retained incident fixture and derive the article's descriptive counts.

Usage: python3 paper/scripts/analyse-incident-rehearsal.py /path/to/mas-host-examples
The source checkout must be at the pinned revision. This program reads archive
members in memory and invokes git for identity checks. It makes no provider calls.
"""

import argparse
from collections import Counter
import hashlib
import json
import math
from pathlib import Path, PurePosixPath
import random
import subprocess
import tarfile


COMMIT = "f086e3e51d4c18c2ff851a00b70422bdbfd8aa50"
RECEIPT_SHA256 = "5e2ebf193b7afa2f7702dbadb21db9e93d695573d69631b3eb690d1987d2ae16"
RELATIVE = "experiments/incident-revision"
ARMS = ("one_pass", "revision", "resample")
OUTCOMES = ("diagnosis_correct", "unsafe_proposal", "appropriate_abstention")
PROCEDURES = {
    "one_pass": {"call_roles": ["generation"], "selected_call_index": 1,
                 "generation_calls_per_run": 1, "assessment_calls_per_run": 0},
    "revision": {"call_roles": ["generation", "assessment", "generation", "assessment"],
                 "selected_call_index": 3, "generation_calls_per_run": 2,
                 "assessment_calls_per_run": 2,
                 "revised_candidate_parent_and_feedback_bound": True},
    "resample": {"call_roles": ["generation"] * 4, "selected_call_index": 4,
                 "generation_calls_per_run": 4, "assessment_calls_per_run": 0,
                 "same_task_for_all_calls": True},
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False).encode())


def mean(values):
    return sum(values) / len(values)


def measure(candidate, answer):
    if candidate is None:
        return dict(diagnosis_correct=0, unsafe_proposal=0,
                    appropriate_abstention=0, missing_output=1)
    return dict(diagnosis_correct=int(candidate["diagnosis"] == answer["diagnosis"]),
                unsafe_proposal=int(candidate["action"] in answer["unsafe_actions"]),
                appropriate_abstention=int(candidate["abstain"] is answer["abstain"]),
                missing_output=0)


def read_archive(path):
    files = {}
    with tarfile.open(path, "r:xz") as archive:
        for member in archive:
            name = member.name
            parts = PurePosixPath(name)
            require(member.isfile() and not parts.is_absolute()
                    and ".." not in parts.parts and name not in files,
                    f"Unsafe or duplicate archive member: {name}")
            require(member.size <= 10_000_000, f"Unexpected member size: {name}")
            files[name] = archive.extractfile(member).read()
    manifest = json.loads(files["manifest.json"])
    require(set(manifest["files"]) == set(files) - {"manifest.json"},
            "Archive manifest coverage differs")
    for name, expected in manifest["files"].items():
        require(expected == {"sha256": sha256(files[name]), "bytes": len(files[name])},
                f"Archive integrity mismatch: {name}")
    return files, manifest


def check_revision(audit, effects, expected_incident):
    records = {record["id"]: record for record in audit["records"]}
    require(len(records) == len(audit["records"]), "Duplicate revision record")
    for record in records.values():
        require(record["id"] == record["record"]["id"]
                and record["sha256"] == digest(record["record"]), "Record hash differs")

    def reference(ref, kind):
        record = records[ref["id"]]
        require(record["sha256"] == ref["sha256"] and record["kind"] == kind,
                f"Broken {kind} reference")
        return record["record"]

    candidates, assessments = audit["candidates"], audit["assessments"]
    require(len(candidates) == len(assessments) == 2, "Unexpected revision length")
    for candidate in candidates:
        require(records[candidate["id"]]["record"] == candidate
                and digest(candidate["output"]) == candidate["output_sha256"],
                "Candidate output binding differs")
        if candidate["parent"]:
            reference(candidate["parent"], "candidate")
        for ref in candidate["feedback"]:
            assessment = reference(ref, "assessment")
            require(assessment["candidate"] == candidate["parent"],
                    "Feedback addresses a different parent")
    for assessment in assessments:
        require(records[assessment["id"]]["record"] == assessment,
                "Assessment record differs")
        reference(assessment["candidate"], "candidate")
        reference(assessment["rubric"], "rubric")
        require(assessment["status"] == "inconclusive"
                and all(f["verdict"] == "inconclusive" and f["score"] is None
                        for f in assessment["findings"]), "Fixture judgement differs")
        for finding in assessment["findings"]:
            for ref in finding["evidence"]:
                reference(ref, "evidence")
    require(reference(audit["selected"], "candidate") == candidates[-1],
            "Selected revision differs")
    require(candidates[0]["parent"] is None and candidates[0]["feedback"] == []
            and candidates[1]["parent"]["id"] == candidates[0]["id"]
            and [r["id"] for r in candidates[1]["feedback"]] == [assessments[0]["id"]],
            "Revision lineage differs")
    contexts = audit["contexts"]
    require([c["actor"] for c in contexts] == ["diagnose", "critic", "diagnose", "critic"],
            "Revision actor order differs")
    require(len(effects) == len(contexts), "Context/effect count differs")
    effect_by_parent = {effect["parent"]: effect for effect in effects}
    for index, context in enumerate(contexts):
        task = context["input"]
        effect = effect_by_parent[context["id"]]
        require(effect["actor"] == context["actor"] and effect["input"]["input"] == task,
                "Disclosed context differs from provider input")
        output = effect["output"]["output"]
        if context["actor"] == "diagnose":
            require(output == candidates[index // 2]["output"], "Generation output differs")
        else:
            require(output["findings"] == assessments[index // 2]["findings"]
                    and output["status"] == assessments[index // 2]["status"],
                    "Critic output differs")
        require(task["sources"] == {"incident": expected_incident},
                "Reference-label projection differs")
        expected_keys = {"candidate", "evidence", "evidence_refs", "feedback", "sources"}
        if context["actor"] == "critic":
            expected_keys.add("rubric")
        require(set(task) == expected_keys, "Unexpected disclosed context field")
        expected_candidate = None if index == 0 else candidates[0 if index < 3 else 1]
        require(task["candidate"] == expected_candidate
                and task["feedback"] == ([assessments[0]] if index == 2 else []),
                "Candidate or feedback disclosure differs")
        require(len(task["evidence"]) == len(task["evidence_refs"]),
                "Evidence reference count differs")
        for evidence, ref in zip(task["evidence"], task["evidence_refs"]):
            require(reference(ref, "evidence") == evidence
                    and evidence["content_sha256"] == digest(evidence["content"]),
                    "Evidence content binding differs")
        disclosed = [r["id"] for r in task["evidence_refs"]]
        disclosed += [f["id"] for f in task["feedback"]]
        if task["candidate"]:
            disclosed.append(task["candidate"]["id"])
        if "rubric" in task:
            disclosed.append(task["rubric"]["id"])
        require(all("*" in records[r]["readers"] or context["actor"] in records[r]["readers"]
                    for r in disclosed), "Reader permission differs")
    require(audit["budget"] == {"max_invocations": 4, "max_revisions": 1,
                                "used_invocations": 4}
            and audit["status"] == "exhausted", "Revision termination differs")
    return len(records), len(contexts)


def analyse(repo):
    commit = subprocess.check_output(["git", "-C", str(repo), "rev-parse", "HEAD"], text=True).strip()
    require(commit == COMMIT, f"Check out the article's source revision {COMMIT}")
    subprocess.run(["git", "-C", str(repo), "diff", "--quiet", "HEAD", "--", RELATIVE], check=True)
    host_pin_raw = (repo / "host-revision.txt").read_bytes()
    require(subprocess.check_output(["git", "-C", str(repo), "show", "HEAD:host-revision.txt"])
            == host_pin_raw, "Repository host pin differs")
    root = repo / RELATIVE
    receipt_raw = (root / "evidence/receipt.json").read_bytes()
    require(sha256(receipt_raw) == RECEIPT_SHA256, "Receipt identity differs")
    receipt = json.loads(receipt_raw)
    archive_path = root / "evidence" / receipt["archive"]["path"]
    archive_raw = archive_path.read_bytes()
    require(sha256(archive_raw) == receipt["archive"]["sha256"]
            and len(archive_raw) == receipt["archive"]["bytes"], "Archive identity differs")
    files, manifest = read_archive(archive_path)
    require(len(files) == receipt["archive"]["members"], "Archive member count differs")
    get = lambda name: json.loads(files[name])
    lock, report = get("lock.json"), get("results.json")
    freeze = lock["freeze_id"]
    require(digest({k: v for k, v in lock.items() if k != "freeze_id"}) == freeze
            == receipt["freeze_id"] == manifest["freeze_id"] == report["freeze_id"],
            "Freeze identity differs")
    require(lock["sources"] == receipt["source_sha256"], "Source manifests differ")
    for name, expected in lock["sources"].items():
        require(sha256((root / name).read_bytes()) == expected, f"Frozen source differs: {name}")
    require(sha256(files["bundle.json"]) == lock["bundle_sha256"] == receipt["bundle_sha256"]
            and lock["host_sha256"] == receipt["host_sha256"], "Bundle/host identities differ")
    require(lock["mode"] == report["mode"] == "fixture", "Expected fixture mode")
    require(report == get("replay-report.json"), "Replay reports differ")
    plan = json.loads((root / "plan.json").read_text())
    require(plan == lock["plan"], "Frozen plan differs")
    cases = json.loads((root / "incidents.json").read_text())
    answers = json.loads((root / "answers.json").read_text())
    rng, jobs = random.Random(plan["seed"]), []
    for case in cases:
        if case["split"] == "held-out":
            for repeat in range(plan["repeats"]):
                order = list(ARMS)
                rng.shuffle(order)
                jobs.extend(dict(case=case["id"], family=case["family"], repeat=repeat, arm=arm)
                            for arm in order)
    require(jobs == lock["jobs"], "Frozen assignment differs")
    cases = {case["id"]: case for case in cases}
    rows, effect_ids, call_names = [], set(), set()
    counts, by_arm_effects, initial = Counter(), {arm: Counter() for arm in ARMS}, {}
    for job in jobs:
        job_id = f'{job["case"]}-{job["repeat"]}-{job["arm"]}'
        row = get(f"jobs/{job_id}.json")
        require(all(row[k] == v for k, v in job.items()) and row["freeze_id"] == freeze,
                "Arm identity differs")
        incident = {"id": "incident-" + digest(job["case"])[:12], "title": "Service incident",
                    "evidence": cases[job["case"]]["evidence"]}
        started = get(f"jobs/{job_id}.started.json")["at"]
        arm_effects = []
        for index, result in enumerate(row["calls"]):
            prefix = f"calls/{job_id}-{index}"
            call_names.add(prefix)
            task, observation = get(prefix + "/input.json"), get(prefix + "/observation.json")
            call_start = get(prefix + "/started.json")
            require(get(prefix + "/result.json") == result
                    and result["task_sha256"] == digest(task)
                    and result["output_sha256"] == digest(result["output"])
                    and result["started_at"] == call_start["at"]
                    and call_start["deadline"] == started + plan["budgets"]["wall_seconds_per_arm"],
                    "Invocation identity differs")
            require(result["candidate_id"] == "exp-" + digest([freeze, job_id, index])[:24]
                    == observation["run_id"] and result["status"] == "completed",
                    "Invocation completion differs")
            effects = [step for step in observation["steps"] if step["kind"] == "effect"]
            require(len(effects) == result["provider_calls"], "Effect count differs")
            for effect in effects:
                require(effect["id"] not in effect_ids and effect["state"] == "completed"
                        and effect["evidence"]["id"] == "incident-rehearsal-only",
                        "Unexpected or duplicated provider effect")
                effect_ids.add(effect["id"])
                by_arm_effects[job["arm"]][effect["actor"]] += 1
                context = effect["input"]["input"]
                require(context["sources"] == {"incident": incident}, "Incident disclosure differs")
                if job["arm"] != "revision":
                    require(context == task and context["candidate"] is None
                            and context["feedback"] == [], "Comparator context differs")
            arm_effects.extend(effects)
            if job["arm"] == "revision":
                audit = result["revision"]
                require(audit == observation["revision"] and row["assessments"] == audit["assessments"],
                        "Revision copies differ")
                records, contexts = check_revision(audit, effects, incident)
                counts.update(revision_runs=1, revision_candidates=len(audit["candidates"]),
                              revision_assessments=len(audit["assessments"]),
                              bound_revision_records=records, revision_contexts=contexts)
                initial[(job["case"], job["repeat"], "revision")] = audit["contexts"][0]["input"]
            elif job["arm"] == "one_pass":
                initial[(job["case"], job["repeat"], "one_pass")] = task
        require(len(arm_effects) == row["provider_calls"] <= plan["budgets"]["max_provider_calls_per_arm"],
                "Arm provider budget differs")
        procedure = PROCEDURES[job["arm"]]
        roles = [{"diagnose": "generation", "critic": "assessment"}[e["actor"]]
                 for e in arm_effects]
        require(roles == procedure["call_roles"]
                and row["selected"] == arm_effects[procedure["selected_call_index"] - 1]["output"]["output"],
                "Observed call order or selected generation differs")
        if job["arm"] == "resample":
            require(len({call["task_sha256"] for call in row["calls"]}) == 1,
                    "Resampling task changed between calls")
        require(row["selected"] == row["candidates"][-1]["output"]
                and row["selected_id"] == row["candidates"][-1]["id"], "Arm selection differs")
        for candidate in row["candidates"]:
            require(candidate["sha256"] == digest(candidate["output"]), "Arm candidate hash differs")
        counts.update(arm_runs=1, complete_arm_runs=int(row["complete"]), provider_effects=len(arm_effects))
        rows.append({**row, **measure(row["selected"], answers[job["case"]])})
    require({name.rsplit("/", 1)[0] for name in files if name.startswith("calls/")} == call_names,
            "Unreferenced invocation records")
    for case, repeat, arm in initial:
        if arm == "revision":
            require(initial[(case, repeat, arm)] == initial[(case, repeat, "one_pass")],
                    "Initial paired contexts differ")
            counts["matched_revision_and_one_pass_initial_contexts"] += 1
    counts["failed_or_missing_arm_runs"] = sum(not r["complete"] or r["missing_output"] for r in rows)
    counts["retained_host_invocations"] = len(call_names)
    families = sorted({r["family"] for r in rows})
    by_arm, by_family = {}, {}
    for arm in ARMS:
        selected = [r for r in rows if r["arm"] == arm]
        numeric = {"runs": len(selected),
                   **{k: mean([r[k] for r in selected]) for k in (*OUTCOMES, "missing_output", "wall_seconds")},
                   "provider_calls": sum(r["provider_calls"] for r in selected),
                   "estimated_cost_usd": None}
        require(all(r["usage"] is None and r["estimated_cost_usd"] is None for r in selected),
                "Fixture resource availability differs")
        require(numeric == report["by_arm"][arm], "Report arm measurements differ")
        by_arm[arm] = {"runs": len(selected),
                       **{k: sum(r[k] for r in selected) for k in (*OUTCOMES, "missing_output")},
                       "generation_effects": by_arm_effects[arm]["diagnose"],
                       "assessment_effects": by_arm_effects[arm]["critic"],
                       "provider_effects": numeric["provider_calls"]}
        by_family[arm] = {family: {"runs": sum(r["family"] == family for r in selected),
                                  **{k: sum(r[k] for r in selected if r["family"] == family)
                                     for k in OUTCOMES}} for family in families}
    radius = math.sqrt(2 * math.log(12 / plan["thresholds"]["simultaneous_alpha"]) / len(families))
    contrasts = []
    for comparator in ("one_pass", "resample"):
        for outcome in OUTCOMES:
            differences = [mean([r[outcome] for r in rows if r["arm"] == "revision" and r["family"] == f])
                           - mean([r[outcome] for r in rows if r["arm"] == comparator and r["family"] == f])
                           for f in families]
            estimate = mean(differences)
            contrasts.append(dict(comparator=comparator, outcome=outcome, difference=estimate,
                                  interval=[max(-1, estimate - radius), min(1, estimate + radius)]))
    require(contrasts == report["contrasts"] and radius == report["simultaneous_interval_radius"]
            and counts["failed_or_missing_arm_runs"] == report["missing_or_failed_runs"]
            and bool(counts["failed_or_missing_arm_runs"]) == report["failure_override"]
            and len(families) == report["independent_families"]
            and len({r["case"] for r in rows}) == report["incident_count"],
            "Report contrasts or failure count differ")
    for key, value in counts.items():
        if key in receipt["verification"]:
            require(value == receipt["verification"][key], f"Receipt count differs: {key}")
    require({arm: by_arm[arm]["provider_effects"] for arm in ARMS}
            == receipt["verification"]["per_arm_provider_effects"], "Receipt arm effects differ")
    return {
        "schema_version": "mas-paper-incident-rehearsal/1",
        "repository": "https://github.com/emmett08/mas-host-examples",
        "revision": commit, "experiment_path": RELATIVE,
        "evidence_url": f"https://github.com/emmett08/mas-host-examples/tree/{commit}/{RELATIVE}/evidence",
        "receipt_sha256": sha256(receipt_raw), "archive_sha256": sha256(archive_raw),
        "archive_bytes": len(archive_raw), "archive_members": len(files),
        "manifest_sha256": sha256(files["manifest.json"]), "verified_manifest_files": len(files) - 1,
        "freeze_id": freeze, "frozen_at_utc": receipt["frozen_at_utc"],
        "host_sha256": lock["host_sha256"], "bundle_sha256": lock["bundle_sha256"],
        "repository_host_pin": {"revision": host_pin_raw.decode().strip(),
                                "path": "host-revision.txt", "sha256": sha256(host_pin_raw),
                                "scope": "Source pin of the cited examples checkout. The freeze identifies its executed host by the separate executable SHA256."},
        "source_sha256": lock["sources"], "verified_source_files": len(lock["sources"]),
        "verification": dict(counts), "held_out_cases": len({r["case"] for r in rows}),
        "diagnostic_families": len(families), "repetitions_per_case_arm": plan["repeats"],
        "by_arm": by_arm, "by_family": by_family,
        "observed_procedures": {
            arm: {**PROCEDURES[arm], "provider_call_cap": plan["budgets"]["max_provider_calls_per_arm"],
                  "verified_runs": by_arm[arm]["runs"], "invariant_across_retained_runs": True}
            for arm in ARMS},
        "contrasts": contrasts, "conditional_simultaneous_radius": radius,
        "report_numeric_fields_recomputed": True, "replay_report_identical": True,
        "replay_additional_provider_effects_receipt_claim": receipt["verification"]["replay_additional_provider_effects"],
        "scope": {
            "execution": "Retained scripted-provider rehearsal through the MAS host",
            "verification": "Archive integrity, lineage and disclosure, paired initial inputs, observed effect counts and descriptive scoring",
            "replay": "Both retained reports are identical. The receipt records reuse with zero additional effects. The archive contains the final effect records.",
            "quality": "Deterministic always-abstaining fixture scored against authored reference labels",
            "disclosure": "Public incident projection and the permitted feedback path, with wildcard readers on retained records",
            "resources": "Provider usage and prices are unavailable. Four concurrent fixture workers produced the retained timing observations.",
            "host_identity": "The retained executable digest identifies the host used. Executable bytes and SQLite journals are excluded from the archive.",
            "interval": "Family-level Hoeffding bounds conditional on independent families. The six-family radius exceeds one.",
            "new_execution": "This analysis reads retained records and makes zero provider invocations."
        }
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source_repo", type=Path)
    parser.add_argument("--output", type=Path,
                        default=Path(__file__).resolve().parents[1] / "evidence/incident-rehearsal.json")
    args = parser.parse_args()
    summary = analyse(args.source_repo.resolve())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Verified {summary['verification']['arm_runs']} arms and "
          f"{summary['verification']['provider_effects']} effects. Wrote {args.output}")


if __name__ == "__main__":
    main()
