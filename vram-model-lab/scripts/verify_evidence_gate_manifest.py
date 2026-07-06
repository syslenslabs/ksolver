#!/usr/bin/env python3
"""Verify evidence-gate scenario flags survive manifest generation."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from typing import Any

import yaml


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCENARIOS = ROOT / "examples" / "evidence-gate-scenarios.yaml"
RUNNER = ROOT / "scripts" / "run_k8s_probe.py"


def run_checked(cmd: list[str]) -> subprocess.CompletedProcess[str]:
    got = subprocess.run(cmd, text=True, capture_output=True, check=False)
    if got.returncode != 0:
        raise RuntimeError(
            "command failed: {cmd}\nstdout:\n{stdout}\nstderr:\n{stderr}".format(
                cmd=" ".join(cmd),
                stdout=got.stdout,
                stderr=got.stderr,
            )
        )
    return got


def boolish(value: Any) -> str:
    return "true" if bool(value) else "false"


def container_env(manifest: dict[str, Any]) -> dict[str, str]:
    containers = (
        (((manifest.get("spec") or {}).get("template") or {}).get("spec") or {}).get("containers")
        or []
    )
    if not containers:
        raise AssertionError("rendered manifest has no containers")
    return {
        row.get("name"): str(row.get("value"))
        for row in containers[0].get("env") or []
        if row.get("name")
    }


def verify_scenario(scenarios_file: Path, scenario: dict[str, Any]) -> dict[str, str]:
    name = scenario["name"]
    rendered = run_checked(
        [
            sys.executable,
            str(RUNNER),
            "--print-manifest",
            "--scenarios-file",
            str(scenarios_file),
            "--scenario",
            name,
        ]
    )
    docs = [doc for doc in yaml.safe_load_all(rendered.stdout) if doc]
    if len(docs) != 1:
        raise AssertionError(f"expected one rendered manifest for {name}, got {len(docs)}")
    env = container_env(docs[0])
    expected_verified = boolish(scenario.get("verified_real_framework", False))
    expected_customer = boolish(scenario.get("customer_workload_fingerprint", False))
    got_verified = env.get("KSOLVER_VERIFIED_REAL_FRAMEWORK")
    got_customer = env.get("KSOLVER_CUSTOMER_WORKLOAD_FINGERPRINT")
    if got_verified != expected_verified:
        raise AssertionError(
            f"{name}: KSOLVER_VERIFIED_REAL_FRAMEWORK={got_verified!r}, expected {expected_verified!r}"
        )
    if got_customer != expected_customer:
        raise AssertionError(
            f"{name}: KSOLVER_CUSTOMER_WORKLOAD_FINGERPRINT={got_customer!r}, expected {expected_customer!r}"
        )
    return {
        "scenario": name,
        "verified_real_framework": got_verified,
        "customer_workload_fingerprint": got_customer,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--scenarios-file", type=Path, default=DEFAULT_SCENARIOS)
    args = parser.parse_args()

    doc = yaml.safe_load(args.scenarios_file.read_text())
    scenarios = list(doc.get("scenarios") or [])
    if not scenarios:
        raise SystemExit(f"no scenarios found in {args.scenarios_file}")
    rows = [verify_scenario(args.scenarios_file, scenario) for scenario in scenarios]
    for row in rows:
        print(
            "{scenario}: verified_real_framework={verified_real_framework} "
            "customer_workload_fingerprint={customer_workload_fingerprint}".format(**row)
        )
    print(f"verified {len(rows)} evidence-gate scenario manifest(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
