#!/usr/bin/env python3
"""Run the VRAM lab pipeline end to end.

By default this validates kube access, refits/evaluates the existing dataset,
and predicts the example manifest. Probe execution is opt-in.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = ROOT / "scripts"
DEFAULT_MANIFEST = ROOT / "examples" / "annotated-training-manifests.yaml"
REPORT = ROOT / "data" / "models" / "scheduler_report.json"


def run(cmd: list[str], *, env: dict[str, str] | None = None, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, text=True, capture_output=True, check=False, env=env, timeout=timeout)


def run_checked(cmd: list[str], *, env: dict[str, str] | None = None, timeout: int | None = None) -> subprocess.CompletedProcess[str]:
    got = run(cmd, env=env, timeout=timeout)
    if got.returncode != 0:
        raise RuntimeError(
            "command failed: {cmd}\nstdout:\n{stdout}\nstderr:\n{stderr}".format(
                cmd=" ".join(cmd),
                stdout=got.stdout,
                stderr=got.stderr,
            )
        )
    return got


def kube_env(kubeconfig: str) -> dict[str, str]:
    env = dict(os.environ)
    env["KUBECONFIG"] = os.path.expanduser(kubeconfig)
    return env


def kube_status(env: dict[str, str]) -> dict[str, Any]:
    context = run_checked(["kubectl", "config", "current-context"], env=env, timeout=30).stdout.strip()
    nodes = json.loads(run_checked(["kubectl", "get", "nodes", "-o", "json"], env=env, timeout=60).stdout)
    probe_resources = run_checked(
        ["kubectl", "get", "jobs,pods", "-l", "app.kubernetes.io/name=ksolver-vram-probe", "-o", "json"],
        env=env,
        timeout=60,
    )
    resources = json.loads(probe_resources.stdout)
    return {
        "context": context,
        "node_count": len(nodes.get("items", [])),
        "nodes": [
            {
                "name": node["metadata"]["name"],
                "ready": any(
                    condition.get("type") == "Ready" and condition.get("status") == "True"
                    for condition in node.get("status", {}).get("conditions", [])
                ),
                "capacity": node.get("status", {}).get("capacity", {}),
                "allocatable": node.get("status", {}).get("allocatable", {}),
                "runtime": node.get("status", {}).get("nodeInfo", {}).get("containerRuntimeVersion"),
            }
            for node in nodes.get("items", [])
        ],
        "leftover_probe_resources": len(resources.get("items", [])),
    }


def maybe_run_probes(args: argparse.Namespace, env: dict[str, str]) -> list[str]:
    commands = []
    if args.run_smoke:
        cmd = [
            "python3",
            str(SCRIPTS / "run_k8s_probe.py"),
            "--scenario",
            "smoke-mlp",
            "--skip-existing",
            "--wait-timeout",
            str(args.wait_timeout),
        ]
        run_checked(cmd, env=env, timeout=args.wait_timeout + 120)
        commands.append(" ".join(cmd))
    if args.run_grid:
        run_checked(["python3", str(SCRIPTS / "generate_scenario_grid.py")], timeout=60)
        cmd = [
            "python3",
            str(SCRIPTS / "run_k8s_probe.py"),
            "--all",
            "--scenarios-file",
            str(ROOT / "generated" / "scenario_grid.yaml"),
            "--skip-existing",
            "--wait-timeout",
            str(args.wait_timeout),
        ]
        run_checked(cmd, env=env, timeout=args.wait_timeout * 30)
        commands.append(" ".join(cmd))
    return commands


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--kubeconfig", default="~/.kube/wsl")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--run-smoke", action="store_true")
    parser.add_argument("--run-grid", action="store_true")
    parser.add_argument("--wait-timeout", type=int, default=1800)
    parser.add_argument("--out", type=Path, default=REPORT)
    args = parser.parse_args()

    env = kube_env(args.kubeconfig)
    started = int(time.time())
    before = kube_status(env)
    probe_commands = maybe_run_probes(args, env)
    fit = run_checked(["python3", str(SCRIPTS / "fit_peak_vram_model.py")], timeout=60)
    run_checked(["python3", str(SCRIPTS / "summarize_results.py")], timeout=60)
    csv_export = run_checked(["python3", str(SCRIPTS / "export_training_csv.py")], timeout=60)
    timeseries_export = run_checked(["python3", str(SCRIPTS / "export_timeseries_csv.py")], timeout=60)
    oom_classifier = run_checked(["python3", str(SCRIPTS / "fit_oom_classifier.py")], timeout=60)
    evidence_gate = run_checked(["python3", str(SCRIPTS / "verify_evidence_gate_manifest.py")], timeout=60)
    evaluation = json.loads(run_checked(["python3", str(SCRIPTS / "evaluate_model.py")], timeout=60).stdout)
    manifest_predictions = json.loads(
        run_checked(["python3", str(SCRIPTS / "predict_manifest_vram.py"), str(args.manifest)], timeout=60).stdout
    )
    after = kube_status(env)
    report = {
        "schema_version": 1,
        "started_at_unix": started,
        "finished_at_unix": int(time.time()),
        "kube": after,
        "kube_before": before,
        "probe_commands": probe_commands,
        "fit_stdout": fit.stdout.strip(),
        "csv_export_stdout": csv_export.stdout.strip(),
        "timeseries_export_stdout": timeseries_export.stdout.strip(),
        "oom_classifier": json.loads(oom_classifier.stdout),
        "evidence_gate_verifier_stdout": evidence_gate.stdout.strip(),
        "evidence_gate_verifier_ok": evidence_gate.returncode == 0,
        "evaluation": evaluation,
        "manifest": str(args.manifest),
        "manifest_predictions": manifest_predictions,
        "ready_for_scheduler_demo": (
            evaluation.get("ready_for_scheduler_demo") is True
            and evidence_gate.returncode == 0
            and all(row.get("status") == "predicted" for row in manifest_predictions)
            and after.get("leftover_probe_resources") == 0
        ),
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "report": str(args.out),
        "ready_for_scheduler_demo": report["ready_for_scheduler_demo"],
        "manifest_predictions": len(manifest_predictions),
        "leftover_probe_resources": after.get("leftover_probe_resources"),
        "evidence_gate_verifier_ok": report["evidence_gate_verifier_ok"],
        "usable_families": sorted((evaluation.get("usable_family_models") or {}).keys()),
    }, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
