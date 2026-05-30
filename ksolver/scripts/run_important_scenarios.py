#!/usr/bin/env python3

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
import yaml

DEFAULT_CONFIG = {
    "pollSeconds": 5,
    "maxParallel": 1,
    "topPoolScenarios": 3,
    "baseScenario": {
        "Solver": "cp-sat-rust",
        "IgnoreUnschedulableWorkloads": True,
        "VerificationBackend": "local-precheck",
        "NodePoolLabelKeys": [
            "eks.amazonaws.com/nodegroup",
            "cloud.google.com/gke-nodepool",
            "agentpool",
        ],
    },
    "scenarios": [
        {
            "name": "strict",
            "overrides": {
                "IgnoreTaints": False,
                "RelaxPreferredAffinity": False,
                "RelaxRequiredAntiAffinity": False,
            },
        },
        {
            "name": "no-taints",
            "overrides": {
                "IgnoreTaints": True,
                "RelaxPreferredAffinity": False,
                "RelaxRequiredAntiAffinity": False,
            },
        },
        {
            "name": "relaxed-affinity",
            "overrides": {
                "IgnoreTaints": False,
                "RelaxPreferredAffinity": True,
                "RelaxRequiredAntiAffinity": False,
            },
        },
        {
            "name": "relaxed-anti-affinity",
            "overrides": {
                "IgnoreTaints": False,
                "RelaxPreferredAffinity": False,
                "RelaxRequiredAntiAffinity": True,
            },
        },
        {
            "name": "theoretical-max",
            "overrides": {
                "IgnoreTaints": True,
                "RelaxPreferredAffinity": True,
                "RelaxRequiredAntiAffinity": True,
            },
        },
    ],
}


def scenario_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in "-._" else "-" for ch in value).strip("-") or "pool"


def request_json(url: str, payload=None, timeout: int = 30):
    if payload is None:
        req = urllib.request.Request(url)
    else:
        req = urllib.request.Request(
            url,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"},
        )
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.load(response)


def deep_merge(base: dict, overlay: dict) -> dict:
    merged = json.loads(json.dumps(base))
    for key, value in (overlay or {}).items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = deep_merge(merged[key], value)
        else:
            merged[key] = value
    return merged


def load_config(path: Path | None) -> dict:
    config = json.loads(json.dumps(DEFAULT_CONFIG))
    if path is None or not path.exists():
        return config
    with path.open() as f:
        loaded = yaml.safe_load(f) or {}
    if not isinstance(loaded, dict):
        raise SystemExit(f"scenario config must be a YAML object: {path}")
    return deep_merge(config, loaded)


def infer_cluster_name(state_root: Path, cluster_name: str) -> str:
    if cluster_name:
        return cluster_name
    return state_root.parent.name


def load_base_request(base_url: str, state_root: Path, cluster_name: str) -> dict:
    latest = request_json(f"{base_url}/api/latest")
    request = latest.get("request") or {}
    request["SnapshotFile"] = str(state_root / "snapshot.json")
    request["ClusterName"] = infer_cluster_name(state_root, cluster_name)
    scenario = request.setdefault("Scenario", {})
    scenario["VerificationBackend"] = scenario.get("VerificationBackend") or "local-precheck"
    scenario["VerificationURL"] = scenario.get("VerificationURL") or scenario.get("VerificationUrl") or ""
    scenario["IgnoreUnschedulableWorkloads"] = scenario.get("IgnoreUnschedulableWorkloads", True)
    return request


def build_payload(base_request: dict, scenario_name: str, overrides: dict, base_scenario: dict) -> dict:
    payload = json.loads(json.dumps(base_request))
    payload["ScenarioName"] = scenario_name
    scenario = payload.setdefault("Scenario", {})
    scenario.update(base_scenario or {})
    scenario.update(overrides)
    return payload


def wait_for_job(base_url: str, job_id: str, poll_seconds: int) -> dict:
    while True:
        status = request_json(f"{base_url}/api/jobs/{job_id}/status")
        progress = status.get("progress") or {}
        print(
            f"[{job_id}] stage={progress.get('stage')} percent={progress.get('percent')} done={status.get('done')} error={status.get('error') or '<none>'}",
            flush=True,
        )
        if status.get("done"):
            return status
        time.sleep(poll_seconds)


def run_scenarios(base_url: str, scenarios: list[tuple[str, dict]], base_request: dict, base_scenario: dict, scenarios_root: Path, poll_seconds: int, force: bool, max_parallel: int) -> None:
    pending = []
    for scenario_name, overrides in scenarios:
        report_path = scenarios_root / scenario_name / "report.json"
        if report_path.exists() and not force:
            print(f"[skip] {scenario_name} already cached at {report_path}", flush=True)
            continue
        pending.append((scenario_name, overrides))

    if not pending:
        return

    max_parallel = max(1, int(max_parallel))
    inflight: dict[str, str] = {}

    while pending or inflight:
        while pending and len(inflight) < max_parallel:
            scenario_name, overrides = pending.pop(0)
            payload = build_payload(base_request, scenario_name, overrides, base_scenario)
            started = request_json(f"{base_url}/api/solve/start", payload=payload)
            job_id = started["jobId"]
            inflight[job_id] = scenario_name
            print(f"[start] {scenario_name} -> {job_id}", flush=True)

        if not inflight:
            continue

        completed = []
        for job_id, scenario_name in list(inflight.items()):
            status = request_json(f"{base_url}/api/jobs/{job_id}/status")
            progress = status.get("progress") or {}
            print(
                f"[{job_id}] stage={progress.get('stage')} percent={progress.get('percent')} done={status.get('done')} error={status.get('error') or '<none>'}",
                flush=True,
            )
            if not status.get("done"):
                continue
            if status.get("error"):
                raise SystemExit(f"[error] {scenario_name}: {status['error']}")
            print(f"[done] {scenario_name}", flush=True)
            completed.append(job_id)

        for job_id in completed:
            inflight.pop(job_id, None)

        if inflight:
            time.sleep(poll_seconds)


def load_scenario_reports(state_root: Path) -> dict:
    reports = {}
    scenarios_root = state_root / "scenarios"
    if not scenarios_root.exists():
        return reports
    for child in scenarios_root.iterdir():
        if not child.is_dir():
            continue
        report_path = child / "report.json"
        request_path = child / "request.json"
        if not report_path.exists():
            continue
        with report_path.open() as f:
            report = json.load(f)
        request = None
        if request_path.exists():
            with request_path.open() as f:
                request = json.load(f)
        reports[child.name] = {"request": request, "report": report}
    return reports


def optimization_value(report: dict, path: list, default=0):
    node = report
    for key in path:
        if not isinstance(node, dict):
            return default
        node = node.get(key)
    return default if node is None else node


def build_summary(cluster_name: str, reports: dict) -> dict:
    strict_report = reports.get("strict", {}).get("report")
    strict_savings = optimization_value(
        strict_report or {}, ["Optimization", "SavingsMonthly", "Monthly"], 0
    )
    strict_active = len((strict_report or {}).get("Optimization", {}).get("ActiveNodes") or [])
    strict_moves = len((strict_report or {}).get("Optimization", {}).get("RecommendedMoves") or [])
    strict_pools = {
        pool.get("Pool"): pool
        for pool in ((strict_report or {}).get("Normalized", {}).get("PoolSummaries") or [])
        if (pool.get("Pool") or "").strip()
    }

    scenario_rows = []
    pool_impacts = []
    for name, payload in reports.items():
        report = payload["report"]
        request = payload.get("request") or {}
        savings = optimization_value(report, ["Optimization", "SavingsMonthly", "Monthly"], 0)
        optimized_cost = optimization_value(
            report, ["Optimization", "OptimizedMonthlyCost", "Monthly"], 0
        )
        active_nodes = len(report.get("Optimization", {}).get("ActiveNodes") or [])
        moves = len(report.get("Optimization", {}).get("RecommendedMoves") or [])
        scenario_rows.append(
            {
                "scenarioName": name,
                "optimizedMonthly": optimized_cost,
                "savingsMonthly": savings,
                "activeNodes": active_nodes,
                "moves": moves,
                "deltaVsStrictSavings": savings - strict_savings,
                "deltaVsStrictActiveNodes": strict_active - active_nodes if strict_report else None,
                "deltaVsStrictMoves": moves - strict_moves if strict_report else None,
            }
        )

        disallowed_pools = (
            ((request.get("Scenario") or {}).get("DisallowedPools"))
            or []
        )
        if len(disallowed_pools) == 1 and strict_report:
            pool_name = disallowed_pools[0]
            strict_pool = strict_pools.get(pool_name) or {}
            pool_impacts.append(
                {
                    "pool": pool_name,
                    "scenarioName": name,
                    "currentMonthlyCost": (strict_pool.get("CurrentMonthlyCost") or {}).get("Monthly", 0),
                    "currentNodeCount": strict_pool.get("NodeCount", 0),
                    "removable": True,
                    "optimizedMonthly": optimized_cost,
                    "savingsMonthly": savings,
                    "deltaVsStrictSavings": savings - strict_savings,
                    "deltaVsStrictActiveNodes": strict_active - active_nodes if strict_report else 0,
                    "moves": moves,
                }
            )

    ranked = sorted(
        [
            row
            for row in scenario_rows
            if row["scenarioName"] != "strict"
        ],
        key=lambda row: (row["deltaVsStrictSavings"], row["deltaVsStrictActiveNodes"] or 0),
        reverse=True,
    )
    pool_impacts.sort(
        key=lambda row: (row["deltaVsStrictSavings"], row["currentMonthlyCost"]),
        reverse=True,
    )

    return {
        "clusterName": cluster_name,
        "generatedAtEpochMs": int(time.time() * 1000),
        "baselineScenario": "strict" if strict_report else None,
        "rankedLevers": ranked,
        "poolImpacts": pool_impacts,
        "scenarios": sorted(scenario_rows, key=lambda row: row["scenarioName"]),
    }


def pool_scenarios_from_report(report: dict, top_n: int) -> list[tuple[str, dict]]:
    pools = (report.get("Normalized") or {}).get("PoolSummaries") or []
    ranked_pools = sorted(
        [pool for pool in pools if (pool.get("Pool") or "").strip()],
        key=lambda pool: pool.get("CurrentMonthlyCost", {}).get("Monthly", 0),
        reverse=True,
    )[: int(top_n)]
    return [
        (
            f"drop-pool-{scenario_slug(pool['Pool'])}",
            {
                "IgnoreTaints": False,
                "RelaxPreferredAffinity": False,
                "RelaxRequiredAntiAffinity": False,
                "DisallowedPools": [pool["Pool"]],
            },
        )
        for pool in ranked_pools
    ]


def main() -> int:
    parser = argparse.ArgumentParser(description="Run important solver scenarios serially.")
    parser.add_argument("--base-url", default="http://127.0.0.1:18080")
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--cluster-name", default="")
    parser.add_argument("--poll-seconds", type=int, default=None)
    parser.add_argument("--max-parallel", type=int, default=None)
    parser.add_argument("--config", default="syslens-solver/scenario-runner.yaml")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    state_root = Path(args.state_root)
    cluster_name = infer_cluster_name(state_root, args.cluster_name)
    config = load_config(Path(args.config) if args.config else None)
    poll_seconds = args.poll_seconds if args.poll_seconds is not None else int(config.get("pollSeconds", 5))
    max_parallel = args.max_parallel if args.max_parallel is not None else int(config.get("maxParallel", 1))
    top_pool_scenarios = int(config.get("topPoolScenarios", 3))
    base_request = load_base_request(args.base_url, state_root, cluster_name)
    base_request.setdefault("Scenario", {}).update(config.get("baseScenario") or {})
    scenarios_root = state_root / "scenarios"
    scenarios_root.mkdir(parents=True, exist_ok=True)

    configured_scenarios = [
        (scenario["name"], scenario.get("overrides") or {})
        for scenario in (config.get("scenarios") or [])
    ]
    run_scenarios(
        args.base_url,
        configured_scenarios,
        base_request,
        config.get("baseScenario") or {},
        scenarios_root,
        poll_seconds,
        args.force,
        max_parallel,
    )

    reports = load_scenario_reports(state_root)
    strict_report = reports.get("strict", {}).get("report") or {}
    pool_scenarios = pool_scenarios_from_report(strict_report, top_pool_scenarios)
    run_scenarios(
        args.base_url,
        pool_scenarios,
        base_request,
        config.get("baseScenario") or {},
        scenarios_root,
        poll_seconds,
        args.force,
        max_parallel,
    )

    reports = load_scenario_reports(state_root)
    summary = build_summary(cluster_name, reports)
    summary_path = state_root / "scenario-summary.json"
    with summary_path.open("w") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    print(f"[summary] wrote {summary_path}", flush=True)

    if summary["rankedLevers"]:
        print("[ranking]", flush=True)
        for row in summary["rankedLevers"]:
            print(
                f"  {row['scenarioName']}: deltaSavings={row['deltaVsStrictSavings']:.2f} deltaNodes={row['deltaVsStrictActiveNodes']} deltaMoves={row['deltaVsStrictMoves']}",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        print(f"HTTP error {exc.code}: {body}", file=sys.stderr)
        raise SystemExit(1)
