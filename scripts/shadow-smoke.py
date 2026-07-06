#!/usr/bin/env python3
"""Smoke-check the ksolver shadow dashboard and simulator refresh contract."""

from __future__ import annotations

import argparse
import collections
import html.parser
import json
import pathlib
import re
import shutil
import subprocess
import sys
import urllib.error
import urllib.request
from typing import Any

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from evidence_helpers import (  # noqa: E402
    display_vram_driver_labels,
    missing_artifact_category_counts,
    missing_artifact_category_rows,
    operator_runbook_command_rows,
    synthetic_headroom_driver_enabled,
)


class DashboardMarkupParser(html.parser.HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.tabs: list[dict[str, str]] = []
        self.panels: list[dict[str, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr = {key: value or "" for key, value in attrs}
        classes = set(attr.get("class", "").split())
        if tag == "button" and "tab" in classes:
            self.tabs.append(attr)
        if tag == "section" and attr.get("role") == "tabpanel":
            self.panels.append(attr)


def fetch(url: str, method: str = "GET") -> tuple[int, bytes]:
    req = urllib.request.Request(url, method=method)
    try:
        with urllib.request.urlopen(req, timeout=75) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def count_label(value: Any) -> str:
    return "unknown" if value is None else str(value)


def classify_readiness_blocker(probe: dict[str, Any]) -> str:
    production = probe.get("production_readiness") or {}
    if production.get("blocker"):
        blocker = str(production.get("blocker") or "").lower()
        if "watch" in blocker or "kubernetes" in blocker:
            return "kubernetes_watch"
        if "solver" in blocker:
            return "solver"
        return "production_readiness"
    readyz = probe.get("readyz") or {}
    if readyz.get("ok") is False:
        detail = str(readyz.get("body") or readyz.get("error") or "").lower()
        if "watch" in detail:
            return "kubernetes_watch"
        if "solver" in detail:
            return "solver"
        return "readyz"
    evidence = probe.get("evidence_summary") or {}
    evidence_class = evidence.get("production_readiness_blocker_class")
    if evidence_class and evidence_class != "none":
        return str(evidence_class)
    simulator = probe.get("simulator_readiness") or {}
    simulator_readiness = evidence.get("simulator_readiness") or simulator.get("readiness")
    if simulator_readiness and simulator_readiness != "ready":
        if simulator_readiness == "not_configured":
            return "simulator_not_configured"
        return "simulator"
    if evidence.get("review_ready") is False:
        return "review_claims"
    return "unknown"


def fetch_probe(url: str) -> dict[str, Any]:
    try:
        status, body = fetch(url)
        text = body.decode("utf-8", errors="replace").strip()
        return {"ok": 200 <= status < 300, "status": status, "body": text}
    except Exception as exc:  # noqa: BLE001 - diagnostic probe should not mask smoke failure.
        return {"ok": False, "status": None, "error": str(exc)}


def evidence_summary_from_bundle(payload: dict[str, Any]) -> dict[str, Any]:
    summary = payload.get("summary") or {}
    return {
        "review_ready": summary.get("review_ready"),
        "demo_gate_status": summary.get("demo_gate_status"),
        "demo_gate_strict_exit_code": summary.get("demo_gate_strict_exit_code"),
        "primary_claim_blocker": summary.get("primary_claim_blocker"),
        "primary_claim_blocker_next_action": summary.get("primary_claim_blocker_next_action"),
        "claim_blockers": summary.get("claim_blockers") or [],
        "vram_admission_mode": summary.get("vram_admission_mode"),
        "vram_scheduler_use": summary.get("vram_scheduler_use"),
        "vram_hard_blocker_count": summary.get("vram_hard_blocker_count"),
        "vram_next_evidence_target": summary.get("vram_next_evidence_target"),
        "production_readiness_blocker_class": summary.get("production_readiness_blocker_class"),
        "simulator_endpoint_count": summary.get("simulator_endpoint_count"),
        "simulator_probe_checked_count": summary.get("simulator_probe_checked_count"),
        "simulator_probe_ready_count": summary.get("simulator_probe_ready_count"),
        "simulator_probe_timeout_millis": summary.get("simulator_probe_timeout_millis"),
        "simulator_readiness": summary.get("simulator_readiness"),
        "simulator_readiness_note": summary.get("simulator_readiness_note"),
    }


def readiness_probe(base_url: str) -> dict[str, Any]:
    base = base_url.rstrip("/")
    probe: dict[str, Any] = {
        "healthz": fetch_probe(f"{base}/healthz"),
        "readyz": fetch_probe(f"{base}/readyz"),
    }
    production_probe = fetch_probe(f"{base}/api/scheduler/production-safety")
    if production_probe.get("body"):
        try:
            payload = json.loads(str(production_probe.get("body") or "{}"))
            readiness = payload.get("readiness") or {}
            simulator = payload.get("simulator") or {}
            production_probe = {
                "ok": production_probe.get("ok"),
                "status": production_probe.get("status"),
            }
            if readiness:
                probe["production_readiness"] = readiness
            if simulator:
                probe["simulator_readiness"] = {
                    "endpoint_count": simulator.get("endpoint_count"),
                    "live_dashboard_baseline_configured": simulator.get(
                        "live_dashboard_baseline_configured"
                    ),
                    "readiness": simulator.get("readiness"),
                    "readiness_note": simulator.get("readiness_note"),
                    "readiness_probe": simulator.get("readiness_probe"),
                    "claim_guard": simulator.get("claim_guard"),
                }
        except json.JSONDecodeError:
            pass
    probe["production_safety"] = production_probe
    bundle_probe = fetch_probe(f"{base}/api/scheduler/evidence-bundle")
    if bundle_probe.get("body"):
        try:
            payload = json.loads(str(bundle_probe.get("body") or "{}"))
            summary = evidence_summary_from_bundle(payload)
            bundle_probe = {
                "ok": bundle_probe.get("ok"),
                "status": bundle_probe.get("status"),
            }
            if summary:
                probe["evidence_summary"] = summary
        except json.JSONDecodeError:
            pass
    probe["evidence_bundle"] = bundle_probe
    operator_probe = fetch_probe(f"{base}/api/scheduler/operator-status")
    if operator_probe.get("body"):
        try:
            payload = json.loads(str(operator_probe.get("body") or "{}"))
            operator_probe = {
                "ok": operator_probe.get("ok"),
                "status": operator_probe.get("status"),
            }
            probe["operator_status"] = payload
        except json.JSONDecodeError:
            pass
    probe["operator_status_probe"] = operator_probe
    return probe


def base_url_from_argv(argv: list[str]) -> str:
    for idx, arg in enumerate(argv):
        if arg == "--base-url" and idx + 1 < len(argv):
            return argv[idx + 1].rstrip("/")
        if arg.startswith("--base-url="):
            return arg.split("=", 1)[1].rstrip("/")
    return "http://127.0.0.1:8090"


def validate_cache_coverage(
    coverage: dict, *, label: str, allow_incomplete_cache: bool
) -> tuple[int, int, int]:
    expect(coverage.get("ok") is True, f"{label} simulator-cache-coverage ok=false")
    total = int(coverage.get("simulator_cache_total_baselines") or 0)
    cached = int(coverage.get("simulator_cache_cached_baselines") or 0)
    missing = int(coverage.get("simulator_cache_missing_baselines") or 0)
    expect(total > 0, f"{label} simulator cache has no expected baselines")
    expect(0 <= cached <= total, f"{label} simulator cache cached count is invalid")
    expect(missing == total - cached, f"{label} simulator cache missing count is inconsistent")
    if not allow_incomplete_cache:
        expect(
            missing == 0,
            "simulator cache is incomplete; pass --allow-incomplete-cache for development checks",
        )
    return total, cached, missing


def validate_refresh_payload(refresh_payload: dict, expected_total: int) -> None:
    refresh = refresh_payload.get("demo_refresh") or {}
    expect(refresh.get("ok") is True, "demo refresh ok=false")
    expect("report" not in refresh, "demo refresh unexpectedly included heavy report")
    expect(
        refresh.get("simulator_cache_total_baselines") == expected_total,
        "refresh total baseline count differs from cache coverage endpoint",
    )
    expect(
        refresh.get("simulator_cache_cached_baselines") is not None,
        "refresh missing cached baseline count",
    )
    expect(
        refresh.get("simulator_cache_coverage_milli") is not None,
        "refresh missing cache coverage percent",
    )


def scenario_useful_gpu(scenario: dict, engine: str) -> int:
    return int(((scenario.get(engine) or {}).get("metrics") or {}).get("useful_gpu") or 0)


def validate_scenario_engine_provenance(scenario: dict, idx: int) -> None:
    name = str(scenario.get("name") or idx)
    for engine in ["kube", "kube_binpack", "ksolver"]:
        row = scenario.get(engine) or {}
        expect(isinstance(row, dict), f"demo-report scenario {name} missing {engine} engine")
        expect(row.get("source"), f"demo-report scenario {name} {engine} missing source")
        expect(isinstance(row.get("metrics") or {}, dict), f"demo-report scenario {name} {engine} missing metrics")
        expect(
            row.get("placements") is not None,
            f"demo-report scenario {name} {engine} missing placements",
        )
    for engine, variant in [("kube", "spread"), ("kube_binpack", "binpack")]:
        simulator = (scenario.get(engine) or {}).get("simulator") or {}
        expect(
            isinstance(simulator, dict) and simulator,
            f"demo-report scenario {name} {engine} missing simulator provenance",
        )
        mode = str(simulator.get("mode") or "")
        expect(mode, f"demo-report scenario {name} {engine} missing simulator mode")
        expect(
            "fallback" not in mode.lower(),
            f"demo-report scenario {name} {engine} has invalid fallback simulator provenance",
        )
        expect(
            simulator.get("variant") == variant,
            f"demo-report scenario {name} {engine} simulator variant mismatch",
        )
        expect(
            simulator.get("timed_out") is not True,
            f"demo-report scenario {name} {engine} simulator baseline timed out",
        )
        expect(
            simulator.get("cache_key") or simulator.get("url"),
            f"demo-report scenario {name} {engine} simulator provenance missing cache key or URL",
        )


def validate_vram_investment_demo_summary(summary: dict[str, Any]) -> dict[str, int]:
    expect(isinstance(summary, dict) and summary, "demo-report missing VRAM investment demo summary")
    rows = summary.get("rows") or []
    claims = summary.get("operator_claims") or []
    evidence = summary.get("required_real_predictor_evidence") or []
    notice = str(summary.get("synthetic_prediction_notice") or "")

    expect(summary.get("passed") is True, "VRAM investment demo summary is not passing")
    expect(isinstance(rows, list) and rows, "VRAM investment demo has no decision rows")
    expect(
        int(summary.get("scenario_count") or 0) == len(rows),
        "VRAM investment demo scenario count is inconsistent",
    )
    expect(len(rows) >= 6, "VRAM investment demo needs at least six decision rows")
    expect(
        "deterministic fake values" in notice and "not as production accuracy claims" in notice,
        "VRAM investment demo missing fake-predictor claim boundary",
    )
    baseline_risk = int(summary.get("baseline_cuda_oom_risk_pods") or 0)
    ksolver_risk = int(summary.get("ksolver_cuda_oom_risk_pods") or 0)
    expect(
        baseline_risk > ksolver_risk,
        "VRAM investment demo does not reduce likely CUDA OOM risk",
    )
    expect(
        int(summary.get("cuda_oom_risk_reduction_pods") or 0) == baseline_risk - ksolver_risk,
        "VRAM investment demo OOM risk reduction count is inconsistent",
    )
    expect(
        int(summary.get("high_vram_nodes_preserved") or 0) > 0,
        "VRAM investment demo has no high-VRAM preservation case",
    )
    expect(
        int(summary.get("unknown_or_advisory_rows") or 0) > 0,
        "VRAM investment demo has no advisory inventory/confidence boundary row",
    )
    expect(
        int(summary.get("average_baseline_oom_risk_percent") or 0)
        > int(summary.get("average_ksolver_oom_risk_percent") or 0),
        "VRAM investment demo average risk does not improve",
    )

    avoided = False
    preserved = False
    advisory = False
    for idx, row in enumerate(rows):
        expect(isinstance(row, dict), f"VRAM investment demo row {idx} is not an object")
        expect(row.get("workload"), f"VRAM investment demo row {idx} missing workload")
        expect(row.get("scenario"), f"VRAM investment demo row {idx} missing scenario")
        expect(row.get("predictor_source"), f"VRAM investment demo row {idx} missing predictor source")
        expect(row.get("decision_reason"), f"VRAM investment demo row {idx} missing decision reason")
        expect(row.get("caveat"), f"VRAM investment demo row {idx} missing caveat")
        expect(
            int(row.get("predicted_lower_vram_gib") or 0)
            <= int(row.get("predicted_peak_vram_gib") or 0)
            <= int(row.get("predicted_upper_vram_gib") or 0),
            f"VRAM investment demo row {idx} has invalid confidence band",
        )
        expect(
            0 <= int(row.get("confidence") or 0) <= 100,
            f"VRAM investment demo row {idx} confidence is out of range",
        )
        expect(
            int(row.get("gpu_request") or 0) > 0,
            f"VRAM investment demo row {idx} missing GPU request",
        )
        if row.get("avoided_failure"):
            avoided = True
            expect(
                int(row.get("risk_delta_percent") or 0) > 0,
                f"VRAM investment demo avoided-failure row {idx} does not reduce risk",
            )
            expect(
                int(row.get("ksolver_upper_band_headroom_gib") or 0)
                > int(row.get("kube_upper_band_headroom_gib") or 0),
                f"VRAM investment demo avoided-failure row {idx} does not improve upper-band headroom",
            )
        preserved = preserved or bool(row.get("preserves_high_vram_capacity"))
        advisory = advisory or bool(row.get("advisory_only"))
    expect(avoided, "VRAM investment demo has no avoided CUDA OOM row")
    expect(preserved, "VRAM investment demo has no high-VRAM preservation row")
    expect(advisory, "VRAM investment demo has no advisory-only row")
    expect(isinstance(claims, list) and len(claims) >= 3, "VRAM investment demo needs operator claims")
    expect(
        any("upper confidence" in str(claim).lower() for claim in claims),
        "VRAM investment demo claims missing upper confidence band claim",
    )
    evidence_text = "\n".join(str(item) for item in evidence)
    expect(
        isinstance(evidence, list) and len(evidence) >= 4,
        "VRAM investment demo missing required real predictor evidence",
    )
    expect(
        "DCGM" in evidence_text or "NVML" in evidence_text,
        "VRAM investment demo evidence missing DCGM/NVML peak-memory source",
    )
    expect(
        "confidence" in evidence_text.lower() or "upper-band" in evidence_text.lower(),
        "VRAM investment demo evidence missing confidence-band validation",
    )
    return {
        "rows": len(rows),
        "baseline_cuda_oom_risk_pods": baseline_risk,
        "ksolver_cuda_oom_risk_pods": ksolver_risk,
        "cuda_oom_risk_reduction_pods": baseline_risk - ksolver_risk,
        "high_vram_nodes_preserved": int(summary.get("high_vram_nodes_preserved") or 0),
        "unknown_or_advisory_rows": int(summary.get("unknown_or_advisory_rows") or 0),
        "average_baseline_oom_risk_percent": int(
            summary.get("average_baseline_oom_risk_percent") or 0
        ),
        "average_ksolver_oom_risk_percent": int(
            summary.get("average_ksolver_oom_risk_percent") or 0
        ),
    }


def validate_demo_report_payload(
    demo_report_payload: dict, *, min_scenarios: int
) -> tuple[int, int, int, str, str]:
    expect(demo_report_payload.get("ok") is True, "demo-report ok=false")
    expect(
        not demo_report_payload.get("demo_report_error"),
        "demo-report returned demo_report_error",
    )
    report = demo_report_payload.get("report") or {}
    scenarios = report.get("scenarios") or []
    pages = report.get("scenario_pages") or []
    readiness = report.get("demo_readiness_summary") or {}
    expect(isinstance(scenarios, list), "demo-report scenarios is not a list")
    expect(
        len(scenarios) >= min_scenarios,
        f"demo-report has {len(scenarios)} scenarios; expected at least {min_scenarios}",
    )
    expect(isinstance(pages, list) and pages, "demo-report has no scenario pages")
    page_keys = {
        (str(page.get("slug") or "") + " " + str(page.get("title") or "")).lower()
        for page in pages
        if isinstance(page, dict)
    }
    expect(
        any("vram" in page for page in page_keys),
        "demo-report missing VRAM scenario page",
    )
    expect(
        any("gang" in page for page in page_keys),
        "demo-report missing gang scenario page",
    )
    expect(
        any("preemption" in page or "migration" in page or "repair" in page for page in page_keys),
        "demo-report missing preemption/migration scenario page",
    )
    vram_investment = validate_vram_investment_demo_summary(
        report.get("vram_investment_demo_summary") or {}
    )
    for idx, scenario in enumerate(scenarios):
        expect(isinstance(scenario, dict), f"demo-report scenario {idx} is not an object")
        validate_scenario_engine_provenance(scenario, idx)
    wins = [
        scenario
        for scenario in scenarios
        if isinstance(scenario, dict)
        and scenario_useful_gpu(scenario, "ksolver")
        > max(scenario_useful_gpu(scenario, "kube"), scenario_useful_gpu(scenario, "kube_binpack"))
    ]
    expect(wins, "demo-report has no scenario where ksolver beats the kube baselines")
    expect(readiness.get("passed") is True, "demo-readiness summary is not passing")
    expect(readiness.get("primary_story"), "demo-readiness summary missing primary story")
    expect(readiness.get("kube_baseline_mode"), "demo-readiness summary missing kube baseline mode")
    live_rows = readiness.get("live_validation_rows") or []
    expect(isinstance(live_rows, list) and live_rows, "demo-readiness summary has no live validation rows")
    for idx, row in enumerate(live_rows):
        expect(isinstance(row, dict), f"demo-readiness live validation row {idx} is not an object")
        expect(row.get("gate"), f"demo-readiness live validation row {idx} missing gate")
        expect(row.get("live_endpoint"), f"demo-readiness live validation row {idx} missing endpoint")
    first_row = live_rows[0]
    expect(first_row.get("gate"), "demo-readiness first live validation row missing gate")
    expect(
        first_row.get("live_endpoint"),
        "demo-readiness first live validation row missing endpoint",
    )
    return (
        len(scenarios),
        len(wins),
        len(live_rows),
        str(first_row.get("gate")),
        str(first_row.get("live_endpoint")),
        vram_investment,
    )


def validate_vram_calibration_payload(calibration_payload: dict) -> tuple[int, int, int, int, int, bool, int, bool]:
    expect(calibration_payload.get("available") is True, "vram-calibration is not available")
    dataset = calibration_payload.get("dataset") or {}
    schema = dataset.get("schema") or {}
    headroom = dataset.get("synthetic_headroom") or {}
    reserve = dataset.get("reserve_pressure") or {}
    readiness = calibration_payload.get("scheduler_readiness") or {}
    decision = readiness.get("admission_decision") or {}
    drivers = calibration_payload.get("model_drivers") or {}
    top_drivers = drivers.get("top_drivers") or []
    real_top_drivers = drivers.get("real_top_drivers") or [
        driver
        for driver in top_drivers
        if isinstance(driver, dict) and driver.get("class") != "synthetic-pressure"
    ]
    synthetic_pressure_drivers = drivers.get("synthetic_pressure_drivers") or [
        driver
        for driver in top_drivers
        if isinstance(driver, dict) and driver.get("class") == "synthetic-pressure"
    ]

    rows = int(dataset.get("rows") or 0)
    samples = int(dataset.get("time_series_samples") or 0)
    expect(headroom, "vram-calibration missing synthetic_headroom block")
    expect(reserve, "vram-calibration missing reserve_pressure compatibility block")
    for field in [
        "definition",
        "pressure_rows",
        "max_synthetic_reserve_extra_mib",
        "torch_allocator_reserve_gap_avg_mib",
        "torch_allocator_reserve_gap_max_mib",
        "torch_allocator_reserve_gap_rows",
    ]:
        if field in headroom or field in reserve:
            expect(
                headroom.get(field) == reserve.get(field),
                f"vram-calibration synthetic_headroom/reserve_pressure mismatch for {field}",
            )
    reserve_rows = int(headroom.get("pressure_rows") or 0)
    evidence_present = int(schema.get("evidence_columns_present") or 0)
    evidence_total = int(schema.get("evidence_columns_total") or 0)

    expect(rows > 0, "vram-calibration has no training rows")
    expect(samples > 0, "vram-calibration has no time-series samples")
    expect(reserve_rows > 0, "vram-calibration has no synthetic headroom probe rows")
    expect(evidence_total > 0, "vram-calibration has no evidence column contract")
    expect(
        evidence_present == evidence_total,
        "vram-calibration evidence columns are incomplete",
    )
    reserve_definition = str(headroom.get("definition") or "")
    expect(
        "reserve_extra_mib" in reserve_definition,
        "vram-calibration synthetic headroom definition missing reserve_extra_mib",
    )
    expect(
        "not organic model demand" in reserve_definition,
        "vram-calibration synthetic headroom definition missing organic-demand caveat",
    )
    expect(drivers.get("available") is True, "vram-calibration model drivers are not available")
    expect(isinstance(top_drivers, list) and top_drivers, "vram-calibration model drivers are empty")
    expect(
        len(top_drivers) >= 3,
        "vram-calibration model drivers must include at least three top contributors",
    )
    for idx, driver in enumerate(top_drivers):
        expect(isinstance(driver, dict), f"vram-calibration model driver {idx} is not an object")
        expect(driver.get("feature"), f"vram-calibration model driver {idx} missing feature")
        expect(driver.get("label"), f"vram-calibration model driver {idx} missing label")
        expect(driver.get("class"), f"vram-calibration model driver {idx} missing class")
        expect(
            float(driver.get("mean_abs_contribution_mib") or 0) > 0,
            f"vram-calibration model driver {idx} missing contribution",
        )
    has_synthetic_reserve_driver = any(
        str(driver.get("feature") or "").startswith("reserve")
        and driver.get("class") == "synthetic-pressure"
        and "organic model memory" in str(driver.get("interpretation") or "")
        for driver in top_drivers
    )
    expect(
        has_synthetic_reserve_driver,
        "vram-calibration model drivers missing synthetic headroom caveat",
    )
    expect(
        isinstance(real_top_drivers, list) and real_top_drivers,
        "vram-calibration missing real_top_drivers for model-memory claims",
    )
    expect(
        all(
            isinstance(driver, dict) and driver.get("class") != "synthetic-pressure"
            for driver in real_top_drivers
        ),
        "vram-calibration real_top_drivers include synthetic pressure",
    )
    expect(
        isinstance(synthetic_pressure_drivers, list) and synthetic_pressure_drivers,
        "vram-calibration missing synthetic_pressure_drivers",
    )
    expect(
        "organic workload predictors" in str(drivers.get("claim_boundary") or ""),
        "vram-calibration model drivers missing claim boundary",
    )
    has_shape_driver = any(
        driver.get("class") in ("activation", "model-size", "precision")
        for driver in top_drivers
    )
    expect(has_shape_driver, "vram-calibration model drivers missing model-shape contributor")
    expect(
        readiness.get("ready_for_shadow_demo") is True,
        "vram-calibration is not ready for shadow demo",
    )
    expect(
        readiness.get("advisory_ready") is True,
        "vram-calibration advisory gate is not ready",
    )
    expect(
        isinstance(readiness.get("hard_admission_blockers") or [], list),
        "vram-calibration hard-admission blockers missing",
    )
    expect(
        isinstance(readiness.get("evidence_collection_plan") or [], list),
        "vram-calibration evidence collection plan missing",
    )
    expect(
        isinstance(decision, dict) and decision.get("mode"),
        "vram-calibration admission decision missing mode",
    )
    expect(
        decision.get("scheduler_use"),
        "vram-calibration admission decision missing scheduler use",
    )
    expect(
        isinstance(decision.get("blocker_count"), int),
        "vram-calibration admission decision missing blocker count",
    )
    expect(
        decision.get("next_evidence_target"),
        "vram-calibration admission decision missing next evidence target",
    )
    hard_ready = readiness.get("hard_admission_ready") is True
    if not hard_ready:
        expect(
            decision.get("mode") == "Shadow advisory only",
            "vram-calibration admission decision should be shadow advisory when hard admission is blocked",
        )
        expect(
            decision.get("scheduler_use") == "Score and warn; do not reject pods",
            "vram-calibration admission decision should not reject pods",
        )
        expect(
            readiness.get("hard_admission_blockers"),
            "vram-calibration hard admission is false without blockers",
        )
        expect(
            decision.get("blocker_count") == len(readiness.get("hard_admission_blockers") or []),
            "vram-calibration admission decision blocker count does not match blockers",
        )
        expect(
            readiness.get("evidence_collection_plan"),
            "vram-calibration hard admission is false without next evidence plan",
        )
    return (
        rows,
        samples,
        reserve_rows,
        evidence_present,
        evidence_total,
        hard_ready,
        len(top_drivers),
        has_synthetic_reserve_driver,
    )


def validate_evidence_bundle_payload(bundle_payload: dict) -> tuple[int, int, str, bool]:
    expect(bundle_payload.get("ok") is True, "evidence-bundle ok=false")
    expect(bundle_payload.get("dry_run") is True, "evidence-bundle is not dry-run")
    note = str(bundle_payload.get("note") or "")
    expect("read-only" in note, "evidence-bundle missing read-only note")
    commands = bundle_payload.get("collection_commands") or []
    rows = bundle_payload.get("evidence_bundle_rows") or []
    missing = bundle_payload.get("missing_live_artifacts") or []
    missing_rows = bundle_payload.get("missing_live_artifact_rows") or []
    artifacts = bundle_payload.get("artifacts") or {}
    launch_gate = bundle_payload.get("launch_proof_gate") or {}
    summary = bundle_payload.get("summary") or {}

    expect(isinstance(commands, list) and commands, "evidence-bundle has no collection commands")
    required_endpoints = [
        "/api/scheduler/traces",
        "/api/scheduler/kube-simulator-plan",
        "/api/scheduler/repair-plan",
        "/api/scheduler/production-safety",
        "/api/scheduler/demo-report",
        "/api/scheduler/vram-calibration",
        "/api/scheduler/operator-status",
        "/api/scheduler/evidence-bundle",
    ]
    command_text = "\n".join(str(command) for command in commands)
    for endpoint in required_endpoints:
        expect(
            endpoint in command_text,
            f"evidence-bundle collection commands missing {endpoint}",
        )

    expect(isinstance(rows, list) and rows, "evidence-bundle has no evidence rows")
    for idx, row in enumerate(rows):
        expect(isinstance(row, dict), f"evidence-bundle row {idx} is not an object")
        for field in ["artifact", "source", "pass_signal", "operator_action", "blocks_claim"]:
            expect(row.get(field), f"evidence-bundle row {idx} missing {field}")

    expect(isinstance(missing, list), "evidence-bundle missing-live-artifacts is not a list")
    expect(isinstance(missing_rows, list), "evidence-bundle missing-live-artifact-rows is not a list")
    expect(
        len(missing_rows) == len(missing),
        "evidence-bundle missing-artifact row count is inconsistent",
    )
    for idx, row in enumerate(missing_rows):
        expect(isinstance(row, dict), f"evidence-bundle missing-artifact row {idx} is not an object")
        for field in ["artifact", "category", "severity", "proof_gate", "next_action"]:
            expect(row.get(field), f"evidence-bundle missing-artifact row {idx} missing {field}")
        expect(
            row.get("artifact") in missing,
            f"evidence-bundle missing-artifact row {idx} artifact not present in compatibility list",
        )
        expect(
            row.get("severity") in ["blocked", "warn"],
            f"evidence-bundle missing-artifact row {idx} has unknown severity",
        )
    expect("production_safety" in artifacts, "evidence-bundle missing production_safety artifact")
    expect("demo_report" in artifacts, "evidence-bundle missing demo_report artifact")
    expect("vram_calibration" in artifacts, "evidence-bundle missing vram_calibration artifact")
    production_safety = artifacts.get("production_safety") or {}
    rollout = production_safety.get("rollout") or {}
    expect(
        production_safety.get("operator_claim"),
        "evidence-bundle production_safety artifact missing operator claim",
    )
    expect(
        rollout.get("mutation_allowed") is False,
        "evidence-bundle production_safety artifact allows mutation",
    )
    expect(
        (artifacts.get("demo_report") or {}).get("ok") is True,
        "evidence-bundle demo_report artifact is not ok",
    )
    expect(
        (artifacts.get("vram_calibration") or {}).get("available") is True,
        "evidence-bundle vram_calibration artifact is not available",
    )
    validate_vram_calibration_payload(artifacts.get("vram_calibration") or {})
    vram_model_drivers = ((artifacts.get("vram_calibration") or {}).get("model_drivers") or {})
    vram_top_drivers = vram_model_drivers.get("top_drivers") or []
    vram_real_top_drivers = vram_model_drivers.get("real_top_drivers") or [
        driver
        for driver in vram_top_drivers
        if isinstance(driver, dict) and driver.get("class") != "synthetic-pressure"
    ]
    vram_claim_safe_drivers = vram_model_drivers.get("claim_safe_drivers") or vram_real_top_drivers
    vram_synthetic_drivers = vram_model_drivers.get("synthetic_pressure_drivers") or [
        driver
        for driver in vram_top_drivers
        if isinstance(driver, dict) and driver.get("class") == "synthetic-pressure"
    ]
    vram_top_driver_labels = [
        str(driver.get("label") or driver.get("feature") or "")
        for driver in vram_top_drivers
        if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
    ][:5]
    vram_real_top_driver_labels = [
        str(driver.get("label") or driver.get("feature") or "")
        for driver in vram_real_top_drivers
        if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
    ][:5]
    vram_claim_safe_driver_labels = [
        str(driver.get("label") or driver.get("feature") or "")
        for driver in vram_claim_safe_drivers
        if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
    ][:5]
    vram_synthetic_driver_labels = [
        str(driver.get("label") or driver.get("feature") or "")
        for driver in vram_synthetic_drivers
        if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
    ][:5]
    vram_display_top_driver_labels = display_vram_driver_labels(vram_top_driver_labels)
    vram_display_real_top_driver_labels = display_vram_driver_labels(vram_real_top_driver_labels)
    vram_display_claim_safe_driver_labels = display_vram_driver_labels(
        vram_claim_safe_driver_labels
    )
    vram_display_synthetic_driver_labels = display_vram_driver_labels(
        vram_synthetic_driver_labels
    )
    expect(
        all(
            not (isinstance(driver, dict) and driver.get("class") == "synthetic-pressure")
            for driver in vram_claim_safe_drivers
        ),
        "vram-calibration claim-safe drivers include synthetic pressure",
    )
    vram_synthetic_reserve_driver = any(
        str(driver.get("feature") or "").startswith("reserve")
        and driver.get("class") == "synthetic-pressure"
        for driver in vram_synthetic_drivers
        if isinstance(driver, dict)
    )
    vram_readiness = ((artifacts.get("vram_calibration") or {}).get("scheduler_readiness") or {})
    vram_decision = vram_readiness.get("admission_decision") or {}
    simulator = production_safety.get("simulator") or {}
    launch_status = str(launch_gate.get("status") or "unknown")
    customer_claim_ready = launch_gate.get("customer_claim_ready") is True
    expect(summary, "evidence-bundle missing summary")
    expect(
        summary.get("collection_command_count") == len(commands),
        "evidence-bundle summary command count is inconsistent",
    )
    expect(
        summary.get("evidence_row_count") == len(rows),
        "evidence-bundle summary row count is inconsistent",
    )
    expect(
        summary.get("missing_live_artifact_count") == len(missing),
        "evidence-bundle summary missing-artifact count is inconsistent",
    )
    expect(
        str(summary.get("launch_status") or "unknown") == launch_status,
        "evidence-bundle summary launch status is inconsistent",
    )
    expect(
        (summary.get("customer_claim_ready") is True) == customer_claim_ready,
        "evidence-bundle summary customer-claim readiness is inconsistent",
    )
    expect(
        summary.get("mutation_allowed") is False,
        "evidence-bundle summary allows mutation",
    )
    expect(
        summary.get("vram_advisory_ready") is True,
        "evidence-bundle summary missing VRAM advisory readiness",
    )
    expect(
        summary.get("vram_hard_admission_ready") is vram_readiness.get("hard_admission_ready"),
        "evidence-bundle summary VRAM hard-admission readiness is inconsistent",
    )
    expect(
        summary.get("vram_admission_mode") == vram_decision.get("mode"),
        "evidence-bundle summary VRAM admission mode is inconsistent",
    )
    expect(
        summary.get("vram_scheduler_use") == vram_decision.get("scheduler_use"),
        "evidence-bundle summary VRAM scheduler use is inconsistent",
    )
    expect(
        summary.get("vram_hard_blocker_count") == vram_decision.get("blocker_count"),
        "evidence-bundle summary VRAM blocker count is inconsistent",
    )
    expect(
        summary.get("vram_next_evidence_target") == vram_decision.get("next_evidence_target"),
        "evidence-bundle summary VRAM next evidence is inconsistent",
    )
    expect(
        summary.get("vram_model_driver_count") == len(vram_top_drivers),
        "evidence-bundle summary VRAM model driver count is inconsistent",
    )
    expect(
        summary.get("vram_top_driver_labels") == vram_top_driver_labels,
        "evidence-bundle summary VRAM top driver labels are inconsistent",
    )
    expect(
        summary.get("vram_display_top_driver_labels") == vram_display_top_driver_labels,
        "evidence-bundle summary VRAM display top driver labels are inconsistent",
    )
    expect(
        summary.get("vram_claim_safe_driver_count") == len(vram_claim_safe_drivers),
        "evidence-bundle summary VRAM claim-safe driver count is inconsistent",
    )
    expect(
        summary.get("vram_claim_safe_driver_labels") == vram_claim_safe_driver_labels,
        "evidence-bundle summary VRAM claim-safe driver labels are inconsistent",
    )
    expect(
        summary.get("vram_display_claim_safe_driver_labels")
        == vram_display_claim_safe_driver_labels,
        "evidence-bundle summary VRAM display claim-safe driver labels are inconsistent",
    )
    expect(
        summary.get("vram_real_model_driver_count") == len(vram_real_top_drivers),
        "evidence-bundle summary VRAM real driver count is inconsistent",
    )
    expect(
        summary.get("vram_real_top_driver_labels") == vram_real_top_driver_labels,
        "evidence-bundle summary VRAM real driver labels are inconsistent",
    )
    expect(
        summary.get("vram_display_real_top_driver_labels")
        == vram_display_real_top_driver_labels,
        "evidence-bundle summary VRAM display real driver labels are inconsistent",
    )
    expect(
        summary.get("vram_synthetic_driver_count") == len(vram_synthetic_drivers),
        "evidence-bundle summary VRAM synthetic driver count is inconsistent",
    )
    expect(
        summary.get("vram_synthetic_driver_labels") == vram_synthetic_driver_labels,
        "evidence-bundle summary VRAM synthetic driver labels are inconsistent",
    )
    expect(
        summary.get("vram_display_synthetic_driver_labels")
        == vram_display_synthetic_driver_labels,
        "evidence-bundle summary VRAM display synthetic driver labels are inconsistent",
    )
    expect(
        summary.get("vram_synthetic_reserve_driver") is vram_synthetic_reserve_driver,
        "evidence-bundle summary VRAM synthetic headroom probe driver is inconsistent",
    )
    expect(
        summary.get("vram_driver_claim_boundary") == vram_model_drivers.get("claim_boundary"),
        "evidence-bundle summary VRAM driver claim boundary is inconsistent",
    )
    simulator = production_safety.get("simulator") or {}
    simulator_probe = simulator.get("readiness_probe") or {}
    expected_simulator_claim_ready = (
        (simulator.get("endpoint_count") or 0) > 0
        and simulator_probe.get("checked_count") == simulator.get("endpoint_count")
        and simulator_probe.get("ready_count") == simulator.get("endpoint_count")
    )
    expect(
        summary.get("simulator_claim_ready") is expected_simulator_claim_ready,
        "evidence-bundle summary simulator claim readiness is inconsistent",
    )
    expect(
        bool(summary.get("simulator_claim_mode")),
        "evidence-bundle summary simulator claim mode is missing",
    )
    expect(
        bool(summary.get("simulator_claim_next_action")),
        "evidence-bundle summary simulator claim next action is missing",
    )
    if not expected_simulator_claim_ready:
        expect(
            bool(summary.get("simulator_claim_blocker")),
            "evidence-bundle summary simulator claim blocker is missing",
        )
    vram_dataset = ((artifacts.get("vram_calibration") or {}).get("dataset") or {})
    reserve_definition = (
        vram_dataset.get("synthetic_headroom")
        or vram_dataset.get("reserve_pressure")
        or {}
    ).get("definition")
    expect(
        summary.get("vram_reserve_pressure_definition") == reserve_definition,
        "evidence-bundle summary VRAM synthetic headroom definition is inconsistent",
    )
    expect(
        summary.get("vram_synthetic_headroom_definition") == reserve_definition,
        "evidence-bundle summary VRAM synthetic headroom alias is inconsistent",
    )
    expect(
        "vram_synthetic_headroom_driver" in summary
        and synthetic_headroom_driver_enabled(summary)
        is summary.get("vram_synthetic_reserve_driver"),
        "evidence-bundle summary VRAM synthetic headroom driver alias is inconsistent",
    )
    live_gates = bundle_payload.get("live_validation_gates") or []
    expect(isinstance(live_gates, list) and live_gates, "evidence-bundle live validation gates are missing")
    status_counts = {
        "pass": sum(1 for gate in live_gates if gate.get("status") == "pass"),
        "warn": sum(1 for gate in live_gates if gate.get("status") == "warn"),
        "blocked": sum(1 for gate in live_gates if gate.get("status") == "blocked"),
    }
    expect(
        summary.get("live_validation_gate_count") == len(live_gates),
        "evidence-bundle live validation gate count is inconsistent",
    )
    expect(
        summary.get("live_validation_pass_count") == status_counts["pass"],
        "evidence-bundle live validation pass count is inconsistent",
    )
    expect(
        summary.get("live_validation_warn_count") == status_counts["warn"],
        "evidence-bundle live validation warn count is inconsistent",
    )
    expect(
        summary.get("live_validation_blocked_count") == status_counts["blocked"],
        "evidence-bundle live validation blocked count is inconsistent",
    )
    expect(
        all(gate.get("gate") and gate.get("status") and gate.get("next_action") for gate in live_gates),
        "evidence-bundle live validation gates need gate/status/next_action",
    )
    production_readiness = production_safety.get("readiness") or {}
    expect(
        summary.get("production_readiness_blocker_class")
        == production_readiness.get("blocker_class"),
        "evidence-bundle summary production readiness blocker class is inconsistent",
    )
    production_debug_commands = production_readiness.get("debug_commands") or []
    expect(
        (summary.get("production_readiness_debug_commands") or []) == production_debug_commands,
        "evidence-bundle summary production readiness debug commands are inconsistent",
    )
    if production_debug_commands:
        expect(
            summary.get("production_readiness_first_debug_command") == production_debug_commands[0],
            "evidence-bundle summary production first debug command is inconsistent",
        )
    expect(
        summary.get("simulator_endpoint_count") == simulator.get("endpoint_count"),
        "evidence-bundle summary simulator endpoint count is inconsistent",
    )
    simulator_probe = simulator.get("readiness_probe") or {}
    expect(
        summary.get("simulator_probe_checked_count") == simulator_probe.get("checked_count"),
        "evidence-bundle summary simulator probe checked count is inconsistent",
    )
    expect(
        summary.get("simulator_probe_ready_count") == simulator_probe.get("ready_count"),
        "evidence-bundle summary simulator probe ready count is inconsistent",
    )
    expect(
        summary.get("simulator_probe_timeout_millis") == simulator_probe.get("timeout_millis"),
        "evidence-bundle summary simulator probe timeout is inconsistent",
    )
    expect(
        summary.get("simulator_readiness") == simulator.get("readiness"),
        "evidence-bundle summary simulator readiness is inconsistent",
    )
    expect(
        summary.get("simulator_readiness_note") == simulator.get("readiness_note"),
        "evidence-bundle summary simulator readiness note is inconsistent",
    )
    expect(
        isinstance(summary.get("claim_blockers") or [], list),
        "evidence-bundle summary claim blockers missing",
    )
    expect(
        summary.get("demo_gate_local_exit_code") == 0,
        "evidence-bundle local demo gate must exit 0",
    )
    strict_code = summary.get("demo_gate_strict_exit_code")
    expect(
        strict_code in (0, 2),
        "evidence-bundle strict demo gate exit code must be 0 or 2",
    )
    expect(
        (strict_code == 0) == (summary.get("review_ready") is True),
        "evidence-bundle strict demo gate exit code must match review readiness",
    )
    expect(
        summary.get("demo_gate_status") in ("strict-pass", "local-pass-strict-blocked"),
        "evidence-bundle demo gate status is invalid",
    )
    if summary.get("review_ready") is not True:
        expect(
            summary.get("claim_blockers"),
            "evidence-bundle summary is not review-ready without blockers",
        )
    if not customer_claim_ready:
        expect(
            missing or launch_status != "ready",
            "evidence-bundle customer claim is blocked without missing artifacts or non-ready launch gate",
        )
    return (
        len(commands),
        len(rows),
        launch_status,
        customer_claim_ready,
        summary.get("production_readiness_blocker_class"),
        summary.get("simulator_claim_ready"),
        summary.get("simulator_claim_mode"),
        summary.get("simulator_claim_blocker"),
        summary.get("simulator_claim_next_action"),
    )


def validate_operator_status_payload(status_payload: dict) -> tuple[str, str | None, str | None]:
    expect(status_payload.get("ok") is True, "operator-status ok=false")
    expect(status_payload.get("dry_run") is True, "operator-status is not dry-run")
    status = str(status_payload.get("status") or "")
    expect(
        status in {"ready", "blocked", "needs-evidence"},
        "operator-status has invalid status",
    )
    expect(
        status_payload.get("can_shadow_demo") is not None,
        "operator-status missing can_shadow_demo",
    )
    expect(
        status_payload.get("can_customer_claim") is not None,
        "operator-status missing can_customer_claim",
    )
    expect(
        (status_payload.get("demo_gate") or {}).get("strict_exit_code") is not None,
        "operator-status missing strict demo gate code",
    )
    decision = status_payload.get("decision_readiness") or {}
    expect(isinstance(decision, dict) and decision, "operator-status missing decision readiness")
    expect(decision.get("status"), "operator-status decision readiness missing status")
    expect(decision.get("summary"), "operator-status decision readiness missing summary")
    expect(decision.get("highest_risk"), "operator-status decision readiness missing highest risk")
    expect(decision.get("next_action"), "operator-status decision readiness missing next action")
    capabilities = decision.get("capabilities") or []
    expect(isinstance(capabilities, list) and capabilities, "operator-status decision readiness missing capabilities")
    capability_names = {row.get("name") for row in capabilities if isinstance(row, dict)}
    for required in [
        "shadow_demo",
        "customer_claim",
        "vram_scoring",
        "hard_vram_admission",
        "production_binding",
    ]:
        expect(required in capability_names, f"operator-status decision readiness missing {required}")
    for row in capabilities:
        expect(isinstance(row, dict), "operator-status decision readiness capability must be object")
        expect(row.get("label"), "operator-status decision readiness capability missing label")
        expect(row.get("status"), "operator-status decision readiness capability missing status")
        expect(row.get("can_execute") is not None, "operator-status decision readiness capability missing executable flag")
        expect(row.get("next_action"), "operator-status decision readiness capability missing next action")
    proof_gates = status_payload.get("proof_gates") or {}
    expect(proof_gates.get("total") is not None, "operator-status missing proof gate total")
    expect(proof_gates.get("pass") is not None, "operator-status missing proof gate pass count")
    expect(proof_gates.get("warn") is not None, "operator-status missing proof gate warn count")
    expect(proof_gates.get("blocked") is not None, "operator-status missing proof gate blocked count")
    rows = proof_gates.get("rows") or []
    expect(isinstance(rows, list), "operator-status proof gate rows must be a list")
    if rows:
        expect(proof_gates.get("total") == len(rows), "operator-status proof gate total mismatch")
        expect(
            proof_gates.get("pass") == sum(1 for row in rows if row.get("status") == "pass"),
            "operator-status proof gate pass count mismatch",
        )
        expect(
            proof_gates.get("warn") == sum(1 for row in rows if row.get("status") == "warn"),
            "operator-status proof gate warn count mismatch",
        )
        expect(
            proof_gates.get("blocked") == sum(1 for row in rows if row.get("status") == "blocked"),
            "operator-status proof gate blocked count mismatch",
        )
    evidence_gaps = status_payload.get("evidence_gaps") or {}
    expect(evidence_gaps.get("total") is not None, "operator-status missing evidence gap total")
    expect(evidence_gaps.get("blocked") is not None, "operator-status missing evidence gap blocked count")
    expect(evidence_gaps.get("warn") is not None, "operator-status missing evidence gap warn count")
    expect(
        isinstance(evidence_gaps.get("category_counts") or {}, dict),
        "operator-status missing evidence gap category counts",
    )
    expect(
        isinstance(evidence_gaps.get("category_rows") or [], list),
        "operator-status missing evidence gap category rows",
    )
    gap_rows = evidence_gaps.get("rows") or []
    expect(isinstance(gap_rows, list), "operator-status evidence gap rows must be a list")
    if gap_rows:
        expect(evidence_gaps.get("total") == len(gap_rows), "operator-status evidence gap total mismatch")
        expect(
            evidence_gaps.get("blocked") == sum(1 for row in gap_rows if row.get("severity") == "blocked"),
            "operator-status evidence gap blocked count mismatch",
        )
        expect(
            evidence_gaps.get("warn") == sum(1 for row in gap_rows if row.get("severity") == "warn"),
            "operator-status evidence gap warn count mismatch",
        )
        expect(
            (evidence_gaps.get("category_counts") or {}) == missing_artifact_category_counts(gap_rows),
            "operator-status evidence gap category counts mismatch",
        )
        expect(
            (evidence_gaps.get("category_rows") or []) == missing_artifact_category_rows(gap_rows),
            "operator-status evidence gap category rows mismatch",
        )
    expect(
        (status_payload.get("evidence") or {}).get("path") == "/api/scheduler/evidence-bundle",
        "operator-status missing evidence-bundle path",
    )
    action_items = status_payload.get("action_items") or []
    expect(isinstance(action_items, list), "operator-status action items must be a list")
    runbook = status_payload.get("operator_runbook") or {}
    expect(isinstance(runbook, dict), "operator-status operator runbook must be an object")
    if gap_rows:
        expect(action_items, "operator-status missing action items for evidence gaps")
        expect(runbook.get("step_count") == len(action_items), "operator-status runbook step count mismatch")
        expect(
            runbook.get("blocked_step_count")
            == sum(1 for item in action_items if item.get("severity") == "blocked"),
            "operator-status runbook blocked count mismatch",
        )
        expect(
            runbook.get("manual_step_count")
            == sum(1 for item in action_items if item.get("command_kind") == "manual"),
            "operator-status runbook manual count mismatch",
        )
        copyable_commands = runbook.get("copyable_commands") or []
        expect(isinstance(copyable_commands, list), "operator-status runbook missing copyable commands")
        copyable_command_rows = runbook.get("copyable_command_rows") or []
        expect(
            isinstance(copyable_command_rows, list),
            "operator-status runbook missing copyable command provenance rows",
        )
        if copyable_commands:
            expect(
                len(copyable_command_rows) == len(copyable_commands),
                "operator-status runbook copyable command provenance count mismatch",
            )
            row_commands = [row.get("command") for row in copyable_command_rows if isinstance(row, dict)]
            expect(
                row_commands == copyable_commands,
                "operator-status runbook copyable command provenance command mismatch",
            )
            first_command_row = copyable_command_rows[0] if copyable_command_rows else {}
            expect(
                isinstance(first_command_row, dict) and first_command_row.get("category"),
                "operator-status first copyable command missing provenance category",
            )
            expect(
                isinstance(first_command_row, dict) and first_command_row.get("next_action"),
                "operator-status first copyable command missing provenance next action",
            )
        first_action = action_items[0]
        expect(first_action.get("priority") == 1, "operator-status first action priority mismatch")
        expect(first_action.get("category"), "operator-status first action missing category")
        expect(first_action.get("next_action"), "operator-status first action missing next action")
        expect(
            first_action.get("command_kind") in ("shell", "manual", "none"),
            "operator-status first action missing command kind",
        )
        expect(
            isinstance(first_action.get("copyable"), bool),
            "operator-status first action missing copyable flag",
        )
        if first_action.get("copyable"):
            expect(first_action.get("command_kind") == "shell", "copyable operator action must be a shell command")
            expect(first_action.get("command_hint"), "copyable operator action missing command hint")
        gap_action = next(
            (
                item
                for item in action_items
                if item.get("category") != "simulator-baseline"
            ),
            None,
        )
        expect(gap_action, "operator-status missing evidence action for evidence gaps")
        expect(
            gap_action.get("category")
            == (evidence_gaps.get("category_rows") or [{}])[0].get("category"),
            "operator-status first evidence action category does not match first gap category",
        )
    if status != "ready":
        expect(status_payload.get("primary_blocker"), "operator-status missing primary blocker")
        expect(status_payload.get("next_action"), "operator-status missing next action")
        expect(status_payload.get("debug_commands"), "operator-status missing debug commands")
        production_debug_commands = (
            (status_payload.get("production_readiness") or {}).get("debug_commands") or []
        )
        if (
            str(status_payload.get("primary_blocker") or "").startswith(
                "production readiness blocked:"
            )
            and production_debug_commands
        ):
            expect(
                runbook.get("next_shell_command") == production_debug_commands[0],
                "operator-status runbook first shell command does not match production readiness first debug command",
            )
    validate_operator_vram_payload(status_payload.get("vram") or {})
    validate_operator_simulator_payload(status_payload.get("simulator") or {})
    validate_operator_scale_safety_payload(status_payload.get("scale_safety") or {})
    validate_operator_binding_safety_payload(status_payload.get("binding_safety") or {})
    return (
        status,
        status_payload.get("primary_blocker"),
        status_payload.get("next_action"),
    )


def operator_decision_summary(status_payload: dict) -> dict[str, Any]:
    decision = status_payload.get("decision_readiness") or {}
    binding = status_payload.get("binding_safety") or {}
    capabilities = decision.get("capabilities") or []
    production_binding = next(
        (
            row
            for row in capabilities
            if isinstance(row, dict) and row.get("name") == "production_binding"
        ),
        {},
    )
    return {
        "status": decision.get("status") or "unknown",
        "summary": decision.get("summary"),
        "highest_risk": decision.get("highest_risk"),
        "next_action": decision.get("next_action"),
        "production_binding_status": production_binding.get("status"),
        "production_binding_can_execute": production_binding.get("can_execute"),
        "production_binding_next_action": production_binding.get("next_action"),
        "reservation_pressure": binding.get("reservation_pressure"),
        "reservation_pressure_description": binding.get("reservation_pressure_description"),
        "reservation_pressure_scope": binding.get("reservation_pressure_scope"),
        "reservation_pressure_reason": binding.get("reservation_pressure_reason"),
        "reservation_pressure_next_action": binding.get("reservation_pressure_next_action"),
    }


def validate_operator_simulator_payload(simulator: dict) -> None:
    expect(isinstance(simulator, dict) and simulator, "operator-status missing simulator summary")
    expect(
        simulator.get("claim_ready") is not None,
        "operator-status simulator missing claim readiness",
    )
    expect(
        bool(simulator.get("claim_mode")),
        "operator-status simulator missing claim mode",
    )
    expect(
        bool(simulator.get("claim_next_action")),
        "operator-status simulator missing claim next action",
    )
    if simulator.get("claim_ready") is not True:
        expect(
            bool(simulator.get("claim_blocker")),
            "operator-status simulator missing claim blocker",
        )
        expect(
            bool(simulator.get("recovery_command")),
            "operator-status simulator missing recovery command",
        )


def validate_operator_vram_payload(vram: dict) -> None:
    expect(isinstance(vram, dict) and vram, "operator-status missing VRAM summary")
    expect(bool(vram.get("mode")), "operator-status VRAM missing admission mode")
    expect(bool(vram.get("scheduler_use")), "operator-status VRAM missing scheduler use")
    expect(vram.get("hard_blocker_count") is not None, "operator-status VRAM missing hard blocker count")
    hard_blockers = vram.get("hard_admission_blockers") or []
    evidence_plan = vram.get("evidence_collection_plan") or []
    expect(isinstance(hard_blockers, list), "operator-status VRAM hard blockers must be a list")
    expect(isinstance(evidence_plan, list), "operator-status VRAM evidence plan must be a list")
    if int(vram.get("hard_blocker_count") or 0) > 0:
        expect(hard_blockers, "operator-status VRAM missing hard admission blockers")
        expect(evidence_plan, "operator-status VRAM missing evidence collection plan")
        first_plan = evidence_plan[0] or {}
        expect(first_plan.get("target"), "operator-status VRAM first evidence plan missing target")
        expect(first_plan.get("unblocks"), "operator-status VRAM first evidence plan missing unblock text")
        expect(isinstance(first_plan.get("commands") or [], list), "operator-status VRAM evidence plan commands must be a list")
    expect(vram.get("next_evidence_target") is not None, "operator-status VRAM missing next evidence target")
    expect(vram.get("model_driver_count") is not None, "operator-status VRAM missing model driver count")
    expect(isinstance(vram.get("top_driver_labels") or [], list), "operator-status VRAM top labels must be a list")
    expect(
        vram.get("display_top_driver_labels")
        == display_vram_driver_labels(vram.get("top_driver_labels") or []),
        "operator-status VRAM display top driver labels are inconsistent",
    )
    expect(
        vram.get("claim_safe_driver_count") is not None,
        "operator-status VRAM missing claim-safe driver count",
    )
    expect(
        vram.get("display_claim_safe_driver_labels")
        == display_vram_driver_labels(vram.get("claim_safe_driver_labels") or []),
        "operator-status VRAM display claim-safe driver labels are inconsistent",
    )
    expect(
        vram.get("real_model_driver_count") is not None,
        "operator-status VRAM missing real driver count",
    )
    expect(
        vram.get("display_real_top_driver_labels")
        == display_vram_driver_labels(vram.get("real_top_driver_labels") or []),
        "operator-status VRAM display real driver labels are inconsistent",
    )
    expect(
        vram.get("synthetic_driver_count") is not None,
        "operator-status VRAM missing synthetic driver count",
    )
    expect(
        vram.get("display_synthetic_driver_labels")
        == display_vram_driver_labels(vram.get("synthetic_driver_labels") or []),
        "operator-status VRAM display synthetic driver labels are inconsistent",
    )
    expect(
        vram.get("synthetic_reserve_driver") is not None,
        "operator-status VRAM missing synthetic headroom probe flag",
    )
    expect(
        "synthetic_headroom_driver" in vram
        and synthetic_headroom_driver_enabled(vram)
        is vram.get("synthetic_reserve_driver"),
        "operator-status VRAM synthetic headroom driver alias is inconsistent",
    )
    expect(
        vram.get("synthetic_headroom_definition") == vram.get("reserve_pressure_definition"),
        "operator-status VRAM synthetic headroom definition alias is inconsistent",
    )


def validate_operator_scale_safety_payload(scale: dict) -> None:
    expect(isinstance(scale, dict) and scale, "operator-status missing scale safety summary")
    expect(scale.get("available") is not None, "operator-status scale safety missing availability")
    expect(bool(scale.get("status")), "operator-status scale safety missing status")
    expect(bool(scale.get("regret_status")), "operator-status scale safety missing regret status")
    expect(scale.get("next_action"), "operator-status scale safety missing next action")
    if scale.get("available") is True:
        expect(scale.get("candidate_node_limit") is not None, "operator-status scale safety missing candidate limit")
        expect(scale.get("unpruned_candidate_edges") is not None, "operator-status scale safety missing unpruned edges")
        expect(scale.get("final_candidate_edges") is not None, "operator-status scale safety missing final edges")
        expect(scale.get("edge_reduction_milli") is not None, "operator-status scale safety missing edge reduction")
        if "unknown" in str(scale.get("regret_status") or ""):
            expect(
                "candidate_node_limit=0" in str(scale.get("next_action") or ""),
                "operator-status scale safety unknown regret must request full candidate comparison",
            )


def validate_operator_binding_safety_payload(binding: dict) -> None:
    expect(isinstance(binding, dict) and binding, "operator-status missing binding safety summary")
    expect(binding.get("available") is not None, "operator-status binding safety missing availability")
    expect(bool(binding.get("status")), "operator-status binding safety missing status")
    expect(binding.get("mutation_allowed") is not None, "operator-status binding safety missing mutation flag")
    expect(binding.get("real_binding_dry_run") is not None, "operator-status binding safety missing dry-run flag")
    expect(binding.get("binding_kill_switch") is not None, "operator-status binding safety missing kill switch flag")
    expect(binding.get("latest_outcome_count") is not None, "operator-status binding safety missing outcome count")
    for key in ["bound", "validated", "skipped", "failed"]:
        expect(binding.get(key) is not None, f"operator-status binding safety missing {key} count")
    expect(isinstance(binding.get("reservations") or {}, dict), "operator-status binding safety missing reservations")
    expect(binding.get("reservation_pressure"), "operator-status binding safety missing reservation pressure")
    expect(
        binding.get("reservation_pressure_description"),
        "operator-status binding safety missing reservation pressure description",
    )
    expect(
        "pending or reserved GPU capacity" in str(binding.get("reservation_pressure_description") or ""),
        "operator-status reservation pressure description must define pending or reserved GPU capacity risk",
    )
    expect(
        "unrelated to CUDA" in str(binding.get("reservation_pressure_scope") or ""),
        "operator-status reservation pressure scope must distinguish scheduler reservations from framework VRAM",
    )
    expect(
        binding.get("reservation_pressure_reason"),
        "operator-status binding safety missing reservation pressure reason",
    )
    expect(
        binding.get("reservation_pressure_next_action"),
        "operator-status binding safety missing reservation pressure next action",
    )
    expect(isinstance(binding.get("skip_breakdown") or {}, dict), "operator-status binding safety missing skip breakdown")
    expect(binding.get("next_action"), "operator-status binding safety missing next action")
    if binding.get("reservation_pressure") == "blocking":
        expect(
            "rejected" in str(binding.get("reservation_pressure_reason") or "").lower(),
            "operator-status blocking reservation pressure must explain rejected reservations",
        )
    if binding.get("mutation_allowed") is True and binding.get("real_binding_dry_run") is not True:
        expect(
            "kill switch" in str(binding.get("next_action") or "").lower()
            or "production binding" in str(binding.get("next_action") or "").lower(),
            "operator-status live binding safety must mention production binding or kill switch",
        )


def inline_dashboard_scripts(html: str) -> list[str]:
    scripts: list[str] = []
    cursor = 0
    while True:
        start = html.find("<script>", cursor)
        if start < 0:
            break
        body_start = start + len("<script>")
        end = html.find("</script>", body_start)
        expect(end >= 0, "dashboard inline script missing closing tag")
        scripts.append(html[body_start:end])
        cursor = end + len("</script>")
    return scripts


def validate_dashboard_javascript(html: str) -> None:
    scripts = inline_dashboard_scripts(html)
    if not scripts:
        return
    validate_dashboard_helper_contract(scripts)
    node = shutil.which("node")
    if not node:
        return
    for idx, script in enumerate(scripts):
        proc = subprocess.run(
            [node, "--check", "-"],
            input=script,
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        expect(
            proc.returncode == 0,
            f"dashboard inline script {idx} has invalid JavaScript: {(proc.stderr or proc.stdout).strip()}",
        )


def validate_dashboard_helper_contract(scripts: list[str]) -> None:
    combined = "\n".join(scripts)
    dashboard_markers = [
        "/api/scheduler/",
        "api/scheduler",
        "renderOperatorBanner",
        "operatorStatusSig",
        "diagSig",
    ]
    if not any(marker in combined for marker in dashboard_markers):
        return
    required_helpers = [
        "fmt",
        "money",
        "pctMilli",
        "shortText",
        "categoryCountsText",
        "getJSON",
        "vramDisplayLabels",
        "modelMetric",
        "renderOperatorBanner",
        "renderApiErrorBanner",
        "diagSig",
        "operatorBannerSig",
        "operatorStatusSig",
        "poll",
    ]
    for helper in required_helpers:
        expect(
            f"function {helper}(" in combined,
            f"dashboard JavaScript missing required helper function {helper}",
        )


def validate_dashboard_dom_id_contract(html: str) -> None:
    id_counts = collections.Counter(re.findall(r'id="([^"]+)"', html))
    duplicates = sorted(id_value for id_value, count in id_counts.items() if count > 1)
    expect(
        not duplicates,
        f"dashboard markup defines duplicate DOM id(s): {', '.join(duplicates)}",
    )
    defined_ids = set(id_counts)
    referenced_ids = set(re.findall(r'\$\("([^"]+)"\)', html))
    missing = sorted(referenced_ids - defined_ids)
    expect(
        not missing,
        f"dashboard JavaScript references missing DOM id(s): {', '.join(missing)}",
    )


def validate_dashboard_tab_panel_contract(html: str) -> None:
    parser = DashboardMarkupParser()
    parser.feed(html)
    tabs = parser.tabs
    panels = parser.panels
    expect(tabs, "dashboard markup defines no tabs")
    expect(panels, "dashboard markup defines no tab panels")
    tab_ids = [tab.get("id", "") for tab in tabs]
    tab_targets = [tab.get("data-panel", "") for tab in tabs]
    tab_controls = [tab.get("aria-controls", "") for tab in tabs]
    tab_indexes = [tab.get("tabindex", "") for tab in tabs]
    panel_ids = [panel.get("id", "") for panel in panels]
    panel_labels = [panel.get("aria-labelledby", "") for panel in panels]
    expect(
        all(tab_ids),
        "dashboard tab is missing an id",
    )
    expect(
        all(tab_targets),
        "dashboard tab is missing data-panel",
    )
    expect(
        all(tab_controls),
        "dashboard tab is missing aria-controls",
    )
    expect(
        all(tab_indexes),
        "dashboard tab is missing tabindex",
    )
    expect(
        all(panel_ids),
        "dashboard tab panel is missing an id",
    )
    expect(
        all(panel_labels),
        "dashboard tab panel is missing aria-labelledby",
    )
    duplicate_targets = sorted(
        target for target, count in collections.Counter(tab_targets).items() if count > 1
    )
    expect(
        not duplicate_targets,
        f"dashboard tabs target duplicate panel id(s): {', '.join(duplicate_targets)}",
    )
    missing_panels = sorted(set(tab_targets) - set(panel_ids))
    expect(
        not missing_panels,
        f"dashboard tabs target missing panel id(s): {', '.join(missing_panels)}",
    )
    mismatched_controls = sorted(
        tab.get("id", "")
        for tab in tabs
        if tab.get("aria-controls", "") != tab.get("data-panel", "")
    )
    expect(
        not mismatched_controls,
        f"dashboard tab aria-controls must match data-panel for tab id(s): {', '.join(mismatched_controls)}",
    )
    orphan_panels = sorted(set(panel_ids) - set(tab_targets))
    expect(
        not orphan_panels,
        f"dashboard tab panels have no tab target: {', '.join(orphan_panels)}",
    )
    missing_tab_labels = sorted(set(panel_labels) - set(tab_ids))
    expect(
        not missing_tab_labels,
        f"dashboard tab panels reference missing tab id(s): {', '.join(missing_tab_labels)}",
    )
    selected_tabs = [tab for tab in tabs if tab.get("aria-selected") == "true"]
    active_panels = [
        panel
        for panel in panels
        if "active" in set(panel.get("class", "").split())
    ]
    expect(
        len(selected_tabs) == 1,
        f"dashboard must define exactly one selected tab, found {len(selected_tabs)}",
    )
    expect(
        len(active_panels) == 1,
        f"dashboard must define exactly one active panel, found {len(active_panels)}",
    )
    selected_target = selected_tabs[0].get("data-panel", "")
    active_panel_id = active_panels[0].get("id", "")
    expect(
        selected_target == active_panel_id,
        f"dashboard selected tab targets {selected_target}, but active panel is {active_panel_id}",
    )
    bad_hidden_state = sorted(
        panel.get("id", "")
        for panel in panels
        if (
            "active" in set(panel.get("class", "").split())
            and "hidden" in panel
        )
        or (
            "active" not in set(panel.get("class", "").split())
            and "hidden" not in panel
        )
    )
    expect(
        not bad_hidden_state,
        "dashboard tab panel hidden state must match active class; bad panel id(s): "
        + ", ".join(bad_hidden_state),
    )
    bad_tabindex = sorted(
        tab.get("id", "")
        for tab in tabs
        if (tab.get("aria-selected") == "true" and tab.get("tabindex") != "0")
        or (tab.get("aria-selected") != "true" and tab.get("tabindex") != "-1")
    )
    expect(
        not bad_tabindex,
        f"dashboard tab tabindex must be 0 only for the selected tab; bad tab id(s): {', '.join(bad_tabindex)}",
    )


def validate_dashboard_html(html: str) -> None:
    validate_dashboard_javascript(html)
    validate_dashboard_dom_id_contract(html)
    validate_dashboard_tab_panel_contract(html)
    required_fragments = [
        "ksolver · GPU Scheduler Studio",
        ".scen-page-filter .btn.active",
        "aria-pressed",
        "aria-controls",
        "tabindex=\"0\"",
        "tabindex=\"-1\"",
        "panel.hidden = !on",
        "function focusTab",
        "addEventListener(\"keydown\"",
        "ArrowRight",
        "ArrowLeft",
        "Home",
        "End",
        "payload && payload.report) || lastReport",
        "var effectiveCalibration = calibration || lastVramCalibration",
        "renderVramInvestmentDemo(report, effectiveCalibration)",
        "lastVramCalibration = calibration",
        "renderScenarios(lastReport, lastVramCalibration)",
        "@media (max-width: 700px)",
        ".proof-section .card { margin-bottom: 10px; overflow-x: auto; }",
        "simulator_cache_coverage_milli",
        "diag-gates",
        "All live evidence gates",
        "diag-gate-list",
        "diag-command-list",
        "\"curl -s \" + window.location.origin",
        "Shadow readiness",
        "Decision readiness",
        "decision_readiness",
        "highest risk",
        "Scale safety",
        "scale_safety",
        "Binding safety",
        "binding_safety",
        "reservation_pressure_description",
        "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky",
        "candidate_node_limit",
        "candidate edges",
        "edge reduction",
        "latest outcomes",
        "reservations",
        "reservation pressure",
        "pressure reason",
        "pressure action",
        "/healthz",
        "/readyz",
        "watch",
        "last error",
        "last error at",
        "diagnostic hint",
        "diagnostic_hint",
        "next action",
        "debug_commands",
        "First readiness debug command",
        "All readiness debug commands",
        "debugCommands.forEach",
        "copyDiagCommand",
        "diagCommand",
        "Copy command",
        "navigator.clipboard.writeText",
        "fallbackCopy",
        "document.execCommand(\"copy\")",
        "document.createElement(\"textarea\")",
        "Admission mode",
        "Scheduler use",
        "Hard blockers",
        "Next evidence",
        "VRAM source",
        "VRAM hard-admission blockers",
        "VRAM evidence collection plan",
        "hard_admission_blockers",
        "evidence_collection_plan",
        "Shadow advisory only",
        "Score and warn; do not reject pods",
        "Synthetic headroom probes",
        "Max synthetic headroom",
        "not organic model demand",
        "What the VRAM model is using",
        "model_drivers",
        "top_drivers",
        "top_driver_labels",
        "vram_display_top_driver_labels",
        "vram_display_claim_safe_driver_labels",
        "vram_display_real_top_driver_labels",
        "vram_display_synthetic_driver_labels",
        "display_top_driver_labels",
        "display_claim_safe_driver_labels",
        "display_real_top_driver_labels",
        "display_synthetic_driver_labels",
        "VRAM drivers",
        "VRAM claim-safe drivers",
        "VRAM claim-safe top",
        "VRAM top drivers",
        "VRAM headroom probes",
        "VRAM synthetic probes",
        "VRAM synthetic headroom",
        "VRAM headroom meaning",
        "synthetic VRAM headroom probe",
        "synthetic-pressure",
        "vramDriverClassLabel",
        "vramDriverClassTitle",
        "headroom probe",
        "not organic model demand",
        "aria-label",
        "mean_abs_contribution_mib",
        "Evidence bundle",
        "/api/scheduler/evidence-bundle",
        "scripts/demo-gate.py --base-url",
        "--require-review-ready",
        "local exit ",
        "strict exit ",
        "demo_gate_strict_exit_code",
        "scripts/collect-evidence-bundle.py --base-url",
        "Live proof gates",
        "live proof gates",
        "live_validation_gates",
        "live_validation_pass_count",
        "Operator action queue",
        "Operator runbook commands",
        "operator action source",
        "operator action",
        "operator runbook",
        "next shell command",
        "missing_live_artifact_action_items",
        "opStatus.action_items",
        "opStatus.operator_runbook",
        "action_items",
        "operator_runbook",
        "command_hint",
        "command_kind",
        "copyable",
        "Missing live artifacts",
        "gap severity",
        "missing_live_artifact_rows",
        "missing_live_artifact_blocked_count",
        "missing_live_artifact_warn_count",
        "evidence_gaps",
        "gaps blocked",
        "vram_advisory_ready",
        "review_ready",
        "claim_blockers",
        "primary blocker",
        "readiness note",
        "simulator.readiness",
        "simulator endpoints",
        "simulator probe",
        "simulator probe timeout",
        "simulator readiness",
        "simulator readiness note",
        "simulator claim",
        "simulator claim mode",
        "simulator claim blocker",
        "simulator claim action",
        "simulator claim ready",
        "simulator claim blocked",
        "recovery_command",
        "simModeLabel",
        "simTrust",
        "prov-badge",
        "cached simulator",
        "live simulator",
        "invalid legacy fallback marker",
        "missing simulator provenance",
        "invalid fallback baselines",
        "simulator_endpoint_count",
        "simulator_readiness_note",
        "configured_not_probed",
        "readiness_probe",
        "probe checked",
        "probe ready",
        "probe timeout",
    ]
    for fragment in required_fragments:
        expect(fragment in html, f"dashboard missing required fragment: {fragment}")
    forbidden_fragments = [
        "filling missing baselines",
        "per-baseline timeout",
        "live baseline cap\"",
    ]
    for fragment in forbidden_fragments:
        expect(fragment not in html, f"dashboard leaked debug summary text: {fragment}")


def smoke_result(
    *,
    base_url: str,
    readiness_mode: str,
    readiness_blocker_class: str | None,
    cached: int,
    total: int,
    missing: int,
    scenario_count: int,
    win_count: int,
    live_gate_count: int,
    first_gate: str,
    first_endpoint: str,
    vram_rows: int,
    vram_samples: int,
    vram_reserve_rows: int,
    vram_evidence_present: int,
    vram_evidence_total: int,
    vram_hard_ready: bool,
    vram_driver_count: int,
    vram_synthetic_reserve_driver: bool,
    vram_investment_rows: int,
    vram_investment_oom_risk_reduction: int,
    vram_investment_high_vram_preserved: int,
    vram_investment_advisory_rows: int,
    vram_investment_average_baseline_oom_risk_percent: int,
    vram_investment_average_ksolver_oom_risk_percent: int,
    evidence_command_count: int,
    evidence_row_count: int,
    evidence_launch_status: str,
    evidence_customer_claim_ready: bool,
    evidence_production_blocker_class: str | None = None,
    simulator_claim_ready: bool | None = None,
    simulator_claim_mode: str | None = None,
    simulator_claim_blocker: str | None = None,
    simulator_claim_next_action: str | None = None,
    operator_status: str = "unknown",
    operator_primary_blocker: str | None = None,
    operator_next_action: str | None = None,
    operator_decision_status: str = "unknown",
    operator_decision_summary: str | None = None,
    operator_decision_highest_risk: str | None = None,
    operator_decision_next_action: str | None = None,
    operator_production_binding_status: str | None = None,
    operator_production_binding_can_execute: bool | None = None,
    operator_production_binding_next_action: str | None = None,
    operator_reservation_pressure: str | None = None,
    operator_reservation_pressure_description: str | None = None,
    operator_reservation_pressure_scope: str | None = None,
    operator_reservation_pressure_reason: str | None = None,
    operator_reservation_pressure_next_action: str | None = None,
    operator_first_shell_command: str | None = None,
    operator_first_shell_command_category: str | None = None,
    operator_first_shell_command_severity: str | None = None,
    operator_first_shell_command_artifact: str | None = None,
    operator_first_shell_command_next_action: str | None = None,
    operator_first_shell_command_kind: str | None = None,
) -> dict[str, Any]:
    return {
        "ok": True,
        "base_url": base_url,
        "readiness_mode": readiness_mode,
        "readiness_blocker_class": readiness_blocker_class,
        "simulator_cache_cached_baselines": cached,
        "simulator_cache_total_baselines": total,
        "simulator_cache_missing_baselines": missing,
        "scenario_count": scenario_count,
        "ksolver_win_count": win_count,
        "demo_readiness_live_gate_count": live_gate_count,
        "demo_readiness_first_gate": first_gate,
        "demo_readiness_first_endpoint": first_endpoint,
        "vram_calibration_rows": vram_rows,
        "vram_calibration_time_series_samples": vram_samples,
        "vram_calibration_reserve_pressure_rows": vram_reserve_rows,
        "vram_calibration_synthetic_headroom_rows": vram_reserve_rows,
        "vram_calibration_evidence_columns_present": vram_evidence_present,
        "vram_calibration_evidence_columns_total": vram_evidence_total,
        "vram_calibration_hard_admission_ready": vram_hard_ready,
        "vram_calibration_model_driver_count": vram_driver_count,
        "vram_calibration_synthetic_reserve_driver": vram_synthetic_reserve_driver,
        "vram_calibration_synthetic_headroom_driver": vram_synthetic_reserve_driver,
        "vram_calibration": "advisory-ready",
        "vram_investment_demo_rows": vram_investment_rows,
        "vram_investment_oom_risk_reduction_pods": vram_investment_oom_risk_reduction,
        "vram_investment_high_vram_nodes_preserved": vram_investment_high_vram_preserved,
        "vram_investment_advisory_rows": vram_investment_advisory_rows,
        "vram_investment_average_baseline_oom_risk_percent": (
            vram_investment_average_baseline_oom_risk_percent
        ),
        "vram_investment_average_ksolver_oom_risk_percent": (
            vram_investment_average_ksolver_oom_risk_percent
        ),
        "evidence_bundle_collection_commands": evidence_command_count,
        "evidence_bundle_rows": evidence_row_count,
        "evidence_bundle_launch_status": evidence_launch_status,
        "evidence_bundle_customer_claim_ready": evidence_customer_claim_ready,
        "evidence_bundle_production_blocker_class": evidence_production_blocker_class,
        "evidence_bundle": "validated",
        "simulator_claim_ready": simulator_claim_ready,
        "simulator_claim_mode": simulator_claim_mode,
        "simulator_claim_blocker": simulator_claim_blocker,
        "simulator_claim_next_action": simulator_claim_next_action,
        "operator_status": operator_status,
        "operator_primary_blocker": operator_primary_blocker,
        "operator_next_action": operator_next_action,
        "operator_decision_status": operator_decision_status,
        "operator_decision_summary": operator_decision_summary,
        "operator_decision_highest_risk": operator_decision_highest_risk,
        "operator_decision_next_action": operator_decision_next_action,
        "operator_production_binding_status": operator_production_binding_status,
        "operator_production_binding_can_execute": operator_production_binding_can_execute,
        "operator_production_binding_next_action": operator_production_binding_next_action,
        "operator_reservation_pressure": operator_reservation_pressure,
        "operator_reservation_pressure_description": operator_reservation_pressure_description,
        "operator_reservation_pressure_scope": operator_reservation_pressure_scope,
        "operator_reservation_pressure_reason": operator_reservation_pressure_reason,
        "operator_reservation_pressure_next_action": operator_reservation_pressure_next_action,
        "operator_first_shell_command": operator_first_shell_command,
        "operator_first_shell_command_category": operator_first_shell_command_category,
        "operator_first_shell_command_severity": operator_first_shell_command_severity,
        "operator_first_shell_command_artifact": operator_first_shell_command_artifact,
        "operator_first_shell_command_next_action": operator_first_shell_command_next_action,
        "operator_first_shell_command_kind": operator_first_shell_command_kind,
        "refresh_contract": "lightweight",
        "dashboard_markup": "current",
        "demo_readiness": "passing",
    }


def smoke_summary(result: dict[str, Any]) -> str:
    readiness = "ready"
    if result.get("readiness_mode") == "degraded":
        readiness = f"degraded/{result.get('readiness_blocker_class') or 'unknown'}"
    simulator_claim = (
        f"simulator claim {result.get('simulator_claim_mode') or 'unknown'} "
        f"({'ready' if result.get('simulator_claim_ready') is True else 'blocked'})"
    )
    if result.get("simulator_claim_ready") is not True:
        if result.get("simulator_claim_blocker"):
            simulator_claim += f": {result['simulator_claim_blocker']}"
        if result.get("simulator_claim_next_action"):
            simulator_claim += f" -> {result['simulator_claim_next_action']}"
    first_shell = ""
    if result.get("operator_first_shell_command_category") or result.get(
        "operator_first_shell_command_next_action"
    ):
        first_shell = (
            f", first shell command reason "
            f"{result.get('operator_first_shell_command_category') or 'unknown'}: "
            f"{result.get('operator_first_shell_command_next_action') or 'unknown'}"
        )
    return (
        "shadow smoke ok: "
        f"{readiness}, simulator cache "
        f"{result['simulator_cache_cached_baselines']}/"
        f"{result['simulator_cache_total_baselines']} cached "
        f"({result['simulator_cache_missing_baselines']} missing), "
        f"{simulator_claim}, "
        f"{result['scenario_count']} scenarios "
        f"({result['ksolver_win_count']} wins), "
        f"{result['demo_readiness_live_gate_count']} live gates, "
        f"first gate {result['demo_readiness_first_gate']} "
        f"({result['demo_readiness_first_endpoint']}), "
        f"VRAM calibration {result['vram_calibration_rows']} rows/"
        f"{result['vram_calibration_time_series_samples']} samples "
        f"({result['vram_calibration_reserve_pressure_rows']} synthetic headroom rows), "
        f"{result.get('vram_calibration_model_driver_count', 0)} model drivers, "
        f"VRAM demo {result.get('vram_investment_demo_rows', 0)} rows "
        f"({result.get('vram_investment_oom_risk_reduction_pods', 0)} OOM-risk pods reduced, "
        f"{result.get('vram_investment_high_vram_nodes_preserved', 0)} high-VRAM preserved), "
        f"evidence bundle {result['evidence_bundle_rows']} rows/"
        f"{result['evidence_bundle_collection_commands']} commands "
        f"(launch {result['evidence_bundle_launch_status']}), "
        f"production blocker {result.get('evidence_bundle_production_blocker_class') or 'unknown'}, "
        f"operator status {result.get('operator_status') or 'unknown'}, "
        f"decision {result.get('operator_decision_status') or 'unknown'}, "
        f"bind {result.get('operator_production_binding_status') or 'unknown'}, "
        f"binding reservation pressure {result.get('operator_reservation_pressure') or 'unknown'}"
        f"{first_shell}, "
        "demo readiness passing, refresh contract lightweight, dashboard markup current"
    )


def smoke_failure(
    error: Exception | str, readiness: dict[str, Any] | None = None
) -> dict[str, Any]:
    result = {"ok": False, "error": str(error)}
    if readiness is not None:
        result["readiness_probe"] = readiness
        result["readiness_blocker_class"] = classify_readiness_blocker(readiness)
    return result


def readiness_probe_summary_lines(probe: dict[str, Any]) -> list[str]:
    lines: list[str] = []
    readyz = probe.get("readyz") or {}
    if readyz:
        detail = readyz.get("body") or readyz.get("error") or "unknown"
        lines.append(f"readiness probe: /readyz status={readyz.get('status')} {detail}")
    production = probe.get("production_readiness") or {}
    if production.get("blocker"):
        lines.append(f"production blocker: {production.get('blocker')}")
    readiness_class = classify_readiness_blocker(probe)
    if readiness_class != "unknown":
        lines.append(f"class: {readiness_class}")
    if production.get("diagnostic_hint"):
        lines.append(f"diagnostic hint: {production.get('diagnostic_hint')}")
    if production.get("last_error_at"):
        lines.append(f"last error at: {production.get('last_error_at')}")
    debug_commands = production.get("debug_commands") or []
    if debug_commands:
        lines.append(f"debug command: {debug_commands[0]}")
    simulator = probe.get("simulator_readiness") or {}
    evidence = probe.get("evidence_summary") or {}
    simulator_readiness = evidence.get("simulator_readiness") or simulator.get("readiness")
    simulator_endpoint_count = evidence.get("simulator_endpoint_count", simulator.get("endpoint_count"))
    simulator_probe = simulator.get("readiness_probe") or {}
    simulator_probe_checked = evidence.get(
        "simulator_probe_checked_count", simulator_probe.get("checked_count")
    )
    simulator_probe_ready = evidence.get(
        "simulator_probe_ready_count", simulator_probe.get("ready_count")
    )
    if simulator_readiness:
        if simulator_probe_checked is not None:
            lines.append(
                f"simulator: {simulator_readiness} "
                f"({count_label(simulator_probe_ready)}/{simulator_probe_checked} ready, "
                f"{count_label(simulator_endpoint_count)} endpoint(s))"
            )
        else:
            lines.append(f"simulator: {simulator_readiness} ({count_label(simulator_endpoint_count)} endpoint(s))")
    if evidence.get("simulator_claim_mode") or evidence.get("simulator_claim_ready") is not None:
        simulator_claim = (
            f"simulator claim: {evidence.get('simulator_claim_mode') or 'unknown'} "
            f"({'ready' if evidence.get('simulator_claim_ready') is True else 'blocked'})"
        )
        if evidence.get("simulator_claim_blocker"):
            simulator_claim += f": {evidence['simulator_claim_blocker']}"
        lines.append(simulator_claim)
    if evidence.get("simulator_claim_next_action"):
        lines.append(f"simulator action: {evidence['simulator_claim_next_action']}")
    operator_status = probe.get("operator_status") or {}
    if isinstance(operator_status, dict) and operator_status:
        decision = operator_decision_summary(operator_status)
        if decision.get("status") and decision.get("status") != "unknown":
            lines.append(f"operator decision: {decision['status']}")
        if decision.get("summary"):
            lines.append(f"operator decision summary: {decision['summary']}")
        if decision.get("highest_risk"):
            lines.append(f"operator risk: {decision['highest_risk']}")
        if decision.get("production_binding_status"):
            bind = f"production binding: {decision['production_binding_status']}"
            if decision.get("production_binding_can_execute") is not None:
                bind += (
                    " (executable)"
                    if decision.get("production_binding_can_execute") is True
                    else " (not executable)"
                )
            lines.append(bind)
        if decision.get("production_binding_next_action"):
            lines.append(f"production binding action: {decision['production_binding_next_action']}")
        if decision.get("reservation_pressure"):
            lines.append(f"binding reservation pressure: {decision['reservation_pressure']}")
        if decision.get("reservation_pressure_description"):
            lines.append(f"binding reservation pressure means: {decision['reservation_pressure_description']}")
        if decision.get("reservation_pressure_scope"):
            lines.append(f"binding reservation pressure scope: {decision['reservation_pressure_scope']}")
        if decision.get("reservation_pressure_reason"):
            lines.append(f"binding reservation pressure reason: {decision['reservation_pressure_reason']}")
        if decision.get("reservation_pressure_next_action"):
            lines.append(f"binding reservation pressure action: {decision['reservation_pressure_next_action']}")
    vram_mode = evidence.get("vram_admission_mode")
    if vram_mode:
        lines.append(f"VRAM: {vram_mode}")
    vram_next = evidence.get("vram_next_evidence_target")
    if vram_next:
        lines.append(f"next VRAM evidence: {vram_next}")
    production_class = evidence.get("production_readiness_blocker_class")
    if production_class:
        lines.append(f"production class: {production_class}")
    blockers = evidence.get("claim_blockers") or []
    primary_blocker = evidence.get("primary_claim_blocker") or (blockers[0] if blockers else None)
    if primary_blocker:
        lines.append(f"primary blocker: {primary_blocker}")
    primary_action = evidence.get("primary_claim_blocker_next_action")
    if primary_action:
        lines.append(f"next action: {primary_action}")
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Validate a running ksolver shadow dashboard."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8090",
        help="shadow server URL; default: %(default)s",
    )
    parser.add_argument(
        "--refresh-timeout-ms",
        type=int,
        default=10_000,
        help="per-baseline simulator refresh timeout passed to shadow",
    )
    parser.add_argument(
        "--allow-incomplete-cache",
        action="store_true",
        help="allow missing simulator baselines; useful for development, not demo readiness",
    )
    parser.add_argument(
        "--allow-readiness-blocked",
        action="store_true",
        help=(
            "continue validation when /readyz is blocked; useful for observe-only "
            "dashboard checks when the Kubernetes watch is unavailable"
        ),
    )
    parser.add_argument(
        "--min-scenarios",
        type=int,
        default=1,
        help="minimum number of demo report scenarios required; default: %(default)s",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="emit a machine-readable JSON result instead of the human summary",
    )
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    status, body = fetch(f"{base}/readyz")
    readyz_ok = status == 200 and body.strip() == b"ready"
    readiness_blocker_class = None
    readiness_mode = "strict"
    if not readyz_ok:
        readiness = readiness_probe(base)
        readiness_blocker_class = classify_readiness_blocker(readiness)
        if args.allow_readiness_blocked:
            readiness_mode = "degraded"
        else:
            expect(False, "/readyz did not return ready")

    status, body = fetch(f"{base}/api/scheduler/simulator-cache-coverage")
    expect(status == 200, "simulator-cache-coverage endpoint failed")
    coverage = json.loads(body)
    total, _, _ = validate_cache_coverage(
        coverage, label="pre-refresh", allow_incomplete_cache=True
    )

    refresh_url = (
        f"{base}/api/scheduler/demo-report/refresh"
        f"?refresh_simulator_cache=true&simulator_timeout_ms={args.refresh_timeout_ms}"
    )
    status, body = fetch(refresh_url, method="POST")
    expect(status == 200, "demo-report refresh endpoint failed")
    refresh_payload = json.loads(body)
    validate_refresh_payload(refresh_payload, total)

    status, body = fetch(f"{base}/api/scheduler/simulator-cache-coverage")
    expect(status == 200, "post-refresh simulator-cache-coverage endpoint failed")
    final_coverage = json.loads(body)
    total, cached, missing = validate_cache_coverage(
        final_coverage,
        label="post-refresh",
        allow_incomplete_cache=args.allow_incomplete_cache,
    )

    status, body = fetch(f"{base}/api/scheduler/demo-report")
    expect(status == 200, "demo-report endpoint failed")
    demo_report_payload = json.loads(body)
    (
        scenario_count,
        win_count,
        live_gate_count,
        first_gate,
        first_endpoint,
        vram_investment,
    ) = validate_demo_report_payload(demo_report_payload, min_scenarios=args.min_scenarios)

    status, body = fetch(f"{base}/api/scheduler/vram-calibration")
    expect(status == 200, "vram-calibration endpoint failed")
    (
        vram_rows,
        vram_samples,
        vram_reserve_rows,
        vram_evidence_present,
        vram_evidence_total,
        vram_hard_ready,
        vram_driver_count,
        vram_synthetic_reserve_driver,
    ) = validate_vram_calibration_payload(json.loads(body))

    status, body = fetch(f"{base}/api/scheduler/evidence-bundle")
    expect(status == 200, "evidence-bundle endpoint failed")
    (
        evidence_command_count,
        evidence_row_count,
        evidence_launch_status,
        evidence_customer_claim_ready,
        evidence_production_blocker_class,
        simulator_claim_ready,
        simulator_claim_mode,
        simulator_claim_blocker,
        simulator_claim_next_action,
    ) = validate_evidence_bundle_payload(json.loads(body))

    status, body = fetch(f"{base}/api/scheduler/operator-status")
    expect(status == 200, "operator-status endpoint failed")
    operator_payload = json.loads(body)
    operator_status, operator_primary_blocker, operator_next_action = validate_operator_status_payload(
        operator_payload
    )
    operator_decision = operator_decision_summary(operator_payload)
    operator_runbook = operator_payload.get("operator_runbook") or {}
    operator_command_rows = operator_runbook_command_rows(operator_runbook)
    operator_first_command = operator_command_rows[0] if operator_command_rows else {}

    status, body = fetch(f"{base}/")
    expect(status == 200, "dashboard HTML failed")
    html = body.decode("utf-8", errors="replace")
    validate_dashboard_html(html)

    result = smoke_result(
        base_url=base,
        readiness_mode=readiness_mode,
        readiness_blocker_class=readiness_blocker_class,
        cached=cached,
        total=total,
        missing=missing,
        scenario_count=scenario_count,
        win_count=win_count,
        live_gate_count=live_gate_count,
        first_gate=first_gate,
        first_endpoint=first_endpoint,
        vram_rows=vram_rows,
        vram_samples=vram_samples,
        vram_reserve_rows=vram_reserve_rows,
        vram_evidence_present=vram_evidence_present,
        vram_evidence_total=vram_evidence_total,
        vram_hard_ready=vram_hard_ready,
        vram_driver_count=vram_driver_count,
        vram_synthetic_reserve_driver=vram_synthetic_reserve_driver,
        vram_investment_rows=vram_investment["rows"],
        vram_investment_oom_risk_reduction=vram_investment[
            "cuda_oom_risk_reduction_pods"
        ],
        vram_investment_high_vram_preserved=vram_investment[
            "high_vram_nodes_preserved"
        ],
        vram_investment_advisory_rows=vram_investment["unknown_or_advisory_rows"],
        vram_investment_average_baseline_oom_risk_percent=vram_investment[
            "average_baseline_oom_risk_percent"
        ],
        vram_investment_average_ksolver_oom_risk_percent=vram_investment[
            "average_ksolver_oom_risk_percent"
        ],
        evidence_command_count=evidence_command_count,
        evidence_row_count=evidence_row_count,
        evidence_launch_status=evidence_launch_status,
        evidence_customer_claim_ready=evidence_customer_claim_ready,
        evidence_production_blocker_class=evidence_production_blocker_class,
        simulator_claim_ready=simulator_claim_ready,
        simulator_claim_mode=simulator_claim_mode,
        simulator_claim_blocker=simulator_claim_blocker,
        simulator_claim_next_action=simulator_claim_next_action,
        operator_status=operator_status,
        operator_primary_blocker=operator_primary_blocker,
        operator_next_action=operator_next_action,
        operator_decision_status=operator_decision["status"],
        operator_decision_summary=operator_decision["summary"],
        operator_decision_highest_risk=operator_decision["highest_risk"],
        operator_decision_next_action=operator_decision["next_action"],
        operator_production_binding_status=operator_decision["production_binding_status"],
        operator_production_binding_can_execute=operator_decision[
            "production_binding_can_execute"
        ],
        operator_production_binding_next_action=operator_decision[
            "production_binding_next_action"
        ],
        operator_reservation_pressure=operator_decision["reservation_pressure"],
        operator_reservation_pressure_description=operator_decision[
            "reservation_pressure_description"
        ],
        operator_reservation_pressure_scope=operator_decision["reservation_pressure_scope"],
        operator_reservation_pressure_reason=operator_decision[
            "reservation_pressure_reason"
        ],
        operator_reservation_pressure_next_action=operator_decision[
            "reservation_pressure_next_action"
        ],
        operator_first_shell_command=operator_first_command.get("command"),
        operator_first_shell_command_category=operator_first_command.get("category"),
        operator_first_shell_command_severity=operator_first_command.get("severity"),
        operator_first_shell_command_artifact=operator_first_command.get("artifact"),
        operator_first_shell_command_next_action=operator_first_command.get("next_action"),
        operator_first_shell_command_kind=operator_first_command.get("command_kind"),
    )
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(smoke_summary(result))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - CLI should show one concise failure.
        if "--json" in sys.argv:
            base_url = base_url_from_argv(sys.argv)
            print(json.dumps(smoke_failure(exc, readiness_probe(base_url)), sort_keys=True))
        else:
            print(f"shadow smoke failed: {exc}", file=sys.stderr)
            probe = readiness_probe(base_url_from_argv(sys.argv))
            for line in readiness_probe_summary_lines(probe):
                print(line, file=sys.stderr)
        raise SystemExit(1)
