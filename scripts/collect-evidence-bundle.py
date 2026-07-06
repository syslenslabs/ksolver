#!/usr/bin/env python3
"""Collect a ksolver shadow SRE evidence packet into a local directory."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import re
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from evidence_helpers import (  # noqa: E402
    display_vram_driver_label,
    display_vram_driver_labels,
    missing_artifact_action_items,
    missing_artifact_category_counts,
    missing_artifact_category_rows,
    operator_action_runbook,
    operator_runbook_command_rows,
    synthetic_headroom_driver_enabled,
)


DEFAULT_ENDPOINTS = [
    "/api/scheduler/traces",
    "/api/scheduler/kube-simulator-plan",
    "/api/scheduler/repair-plan",
    "/api/scheduler/production-safety",
    "/api/scheduler/demo-report",
    "/api/scheduler/vram-calibration",
    "/api/scheduler/operator-status",
    "/api/scheduler/evidence-bundle",
]


def fetch_json(url: str) -> tuple[int, dict[str, Any]]:
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=75) as resp:
            body = resp.read()
            return resp.status, json.loads(body)
    except urllib.error.HTTPError as exc:
        body = exc.read()
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            payload = {"error": body.decode("utf-8", errors="replace")}
        return exc.code, payload


def endpoint_from_command(command: str) -> str | None:
    match = re.search(r"https?://[^/\s]+(?P<path>/api/[^\s>]+)", command)
    if match:
        return match.group("path")
    match = re.search(r"(?P<path>/api/[^\s>]+)", command)
    return match.group("path") if match else None


def endpoint_filename(endpoint: str) -> str:
    name = endpoint.strip("/").replace("/", "-")
    name = re.sub(r"[^A-Za-z0-9_.-]+", "-", name).strip("-")
    return f"{name or 'root'}.json"


def endpoints_from_bundle(bundle: dict[str, Any]) -> list[str]:
    commands = bundle.get("collection_commands") or []
    endpoints: list[str] = []
    for command in commands:
        endpoint = endpoint_from_command(str(command))
        if endpoint and endpoint not in endpoints:
            endpoints.append(endpoint)
    for endpoint in DEFAULT_ENDPOINTS:
        if endpoint not in endpoints:
            endpoints.append(endpoint)
    return endpoints


def timestamp_slug(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%SZ")


def write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def file_metadata(path: pathlib.Path) -> dict[str, Any]:
    data = path.read_bytes()
    return {
        "bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
    }


def load_optional_json(path: pathlib.Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return {"present": True, "parse_error": exc.msg}
    if not isinstance(payload, dict):
        return {"present": True, "parse_error": "not a JSON object"}
    return payload


def summarize_demo_gate_result(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    summary = {
        "present": True,
        "ok": payload.get("ok"),
        "stage": payload.get("stage"),
        "exit_code": payload.get("exit_code"),
        "failed_command": payload.get("failed_command"),
        "failed_returncode": payload.get("failed_returncode"),
        "parse_error": payload.get("parse_error"),
    }
    return {key: value for key, value in summary.items() if value is not None}


def summarize_doctor_preflight(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if payload is None:
        return None
    failures = payload.get("failures")
    recommended = payload.get("recommended_commands")
    api_failures = payload.get("api_endpoint_failures")
    summary = {
        "present": True,
        "ok": payload.get("ok"),
        "status": payload.get("status"),
        "exit_code": payload.get("exit_code"),
        "first_recommended_command": payload.get("first_recommended_command"),
        "failure_count": len(failures) if isinstance(failures, list) else None,
        "recommended_command_count": len(recommended) if isinstance(recommended, list) else None,
        "api_endpoint_failure_count": len(api_failures) if isinstance(api_failures, list) else None,
        "first_api_endpoint_failure": api_failures[0] if isinstance(api_failures, list) and api_failures else None,
        "parse_error": payload.get("parse_error"),
    }
    return {key: value for key, value in summary.items() if value is not None}


def build_manifest(
    *,
    base_url: str,
    bundle_status: int,
    bundle: dict[str, Any],
    files: dict[str, dict[str, Any]],
    captured_payloads: dict[str, dict[str, Any]] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    summary = bundle.get("summary") or {}
    captured_payloads = captured_payloads or {}
    operator_payload = captured_payloads.get("/api/scheduler/operator-status") or {}
    operator_status = {
        "status": operator_payload.get("status"),
        "status_label": operator_payload.get("status_label"),
        "primary_blocker": operator_payload.get("primary_blocker"),
        "next_action": operator_payload.get("next_action"),
        "debug_commands": operator_payload.get("debug_commands") or [],
        "production_readiness": operator_payload.get("production_readiness") or {},
        "can_shadow_demo": operator_payload.get("can_shadow_demo"),
        "can_customer_claim": operator_payload.get("can_customer_claim"),
        "binding_safety": operator_payload.get("binding_safety") or {},
        "vram": operator_payload.get("vram") or {},
        "demo_gate": operator_payload.get("demo_gate") or {},
    } if operator_payload else {}
    vram_payload = captured_payloads.get("/api/scheduler/vram-calibration") or {}
    model_drivers = vram_payload.get("model_drivers") or {}
    top_drivers = model_drivers.get("top_drivers") or []
    real_top_drivers = model_drivers.get("real_top_drivers") or [
        driver
        for driver in top_drivers
        if isinstance(driver, dict) and driver.get("class") != "synthetic-pressure"
    ]
    claim_safe_drivers = model_drivers.get("claim_safe_drivers") or real_top_drivers
    synthetic_pressure_drivers = model_drivers.get("synthetic_pressure_drivers") or [
        driver
        for driver in top_drivers
        if isinstance(driver, dict) and driver.get("class") == "synthetic-pressure"
    ]
    synthetic_reserve_driver = any(
        str(driver.get("feature") or "").startswith("reserve")
        and driver.get("class") == "synthetic-pressure"
        for driver in top_drivers
        if isinstance(driver, dict)
    )
    vram_model_drivers = {
        "available": model_drivers.get("available") is True,
        "fit": model_drivers.get("fit"),
        "training_rows": model_drivers.get("training_rows"),
        "impact_basis": model_drivers.get("impact_basis"),
        "group_impacts": model_drivers.get("group_impacts") or [],
        "top_organic_driver_descriptions": model_drivers.get("top_organic_driver_descriptions") or [],
        "top_driver_count": len(top_drivers),
        "synthetic_reserve_driver": synthetic_reserve_driver,
        "synthetic_headroom_driver": synthetic_reserve_driver,
        "top_driver_labels": [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in top_drivers[:5]
            if isinstance(driver, dict)
        ],
        "display_top_driver_labels": display_vram_driver_labels([
            str(driver.get("label") or driver.get("feature") or "")
            for driver in top_drivers[:5]
            if isinstance(driver, dict)
        ]),
        "top_driver_descriptions": [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in top_drivers[:5]
            if isinstance(driver, dict)
        ],
        "claim_safe_driver_count": len(claim_safe_drivers),
        "claim_safe_driver_labels": [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in claim_safe_drivers[:5]
            if isinstance(driver, dict)
        ],
        "display_claim_safe_driver_labels": display_vram_driver_labels([
            str(driver.get("label") or driver.get("feature") or "")
            for driver in claim_safe_drivers[:5]
            if isinstance(driver, dict)
        ]),
        "claim_safe_driver_descriptions": [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in claim_safe_drivers[:5]
            if isinstance(driver, dict)
        ],
        "real_top_driver_count": len(real_top_drivers),
        "real_top_driver_labels": [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in real_top_drivers[:5]
            if isinstance(driver, dict)
        ],
        "display_real_top_driver_labels": display_vram_driver_labels([
            str(driver.get("label") or driver.get("feature") or "")
            for driver in real_top_drivers[:5]
            if isinstance(driver, dict)
        ]),
        "real_top_driver_descriptions": [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in real_top_drivers[:5]
            if isinstance(driver, dict)
        ],
        "synthetic_pressure_driver_count": len(synthetic_pressure_drivers),
        "synthetic_pressure_driver_labels": [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in synthetic_pressure_drivers[:5]
            if isinstance(driver, dict)
        ],
        "display_synthetic_pressure_driver_labels": display_vram_driver_labels([
            str(driver.get("label") or driver.get("feature") or "")
            for driver in synthetic_pressure_drivers[:5]
            if isinstance(driver, dict)
        ]),
        "synthetic_pressure_driver_descriptions": [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in synthetic_pressure_drivers[:5]
            if isinstance(driver, dict)
        ],
        "claim_boundary": model_drivers.get("claim_boundary"),
    } if model_drivers else {}
    missing_endpoints = [
        endpoint
        for endpoint in DEFAULT_ENDPOINTS
        if files.get(endpoint, {}).get("status") != 200
    ]
    packet_complete = bundle_status == 200 and bundle.get("ok") is True and not missing_endpoints
    claim_blockers: list[str] = list(summary.get("claim_blockers") or [])
    missing_count = int(summary.get("missing_live_artifact_count") or 0)
    if missing_count and not any("missing live artifact" in blocker for blocker in claim_blockers):
        claim_blockers.append(f"{missing_count} missing live artifact(s)")
    if summary.get("customer_claim_ready") is not True and "customer claim not ready" not in claim_blockers:
        claim_blockers.append("customer claim not ready")
    production_class = summary.get("production_readiness_blocker_class")
    if production_class and production_class != "none":
        production_blocker = f"production readiness blocked: {production_class}"
        if production_blocker not in claim_blockers:
            claim_blockers.append(production_blocker)
    if summary.get("mutation_allowed") is True and not any("mutation is allowed" in blocker for blocker in claim_blockers):
        claim_blockers.append("mutation is allowed; review rollout safety before sharing")
    if summary.get("vram_advisory_ready") is not True and "VRAM advisory evidence missing" not in claim_blockers:
        claim_blockers.append("VRAM advisory evidence missing")
    if missing_endpoints:
        claim_blockers.append("endpoint capture incomplete")
    operator_primary_blocker = operator_status.get("primary_blocker")
    if operator_primary_blocker and operator_primary_blocker not in claim_blockers:
        claim_blockers.append(operator_primary_blocker)
    primary_claim_blocker = (
        operator_primary_blocker
        or summary.get("primary_claim_blocker")
        or next((b for b in claim_blockers if str(b).startswith("production readiness blocked:")), None)
        or next((b for b in claim_blockers if str(b).startswith("mutation is allowed")), None)
        or next((b for b in claim_blockers if str(b).startswith("VRAM advisory")), None)
        or ("customer claim not ready" if "customer claim not ready" in claim_blockers else None)
        or (claim_blockers[0] if claim_blockers else None)
    )
    primary_claim_blocker_next_action = (
        operator_status.get("next_action")
        or summary.get("primary_claim_blocker_next_action")
    )
    if primary_claim_blocker_next_action is None and primary_claim_blocker:
        if str(primary_claim_blocker).startswith("production readiness blocked:"):
            primary_claim_blocker_next_action = summary.get(
                "production_readiness_next_action"
            ) or "restore production readiness before using this packet for launch or customer claims"
        elif str(primary_claim_blocker).startswith("mutation is allowed"):
            primary_claim_blocker_next_action = "switch to observe-only or review rollout safety before sharing"
        elif str(primary_claim_blocker).startswith("VRAM advisory"):
            primary_claim_blocker_next_action = "collect VRAM advisory evidence before making scheduler placement claims"
        elif primary_claim_blocker == "customer claim not ready":
            primary_claim_blocker_next_action = "resolve launch proof gaps before making customer-facing claims"
        elif "missing live artifact" in str(primary_claim_blocker):
            primary_claim_blocker_next_action = "capture the missing live artifacts listed in this evidence bundle"
    api_review_ready = summary.get("review_ready")
    review_ready = (
        packet_complete
        and not claim_blockers
        and (api_review_ready is not False)
    )
    missing_rows = bundle.get("missing_live_artifact_rows") or []
    missing_blocked_count = sum(
        1 for row in missing_rows if isinstance(row, dict) and row.get("severity") == "blocked"
    )
    missing_warn_count = sum(
        1 for row in missing_rows if isinstance(row, dict) and row.get("severity") == "warn"
    )
    missing_category_counts = (
        summary.get("missing_live_artifact_category_counts")
        or missing_artifact_category_counts(missing_rows)
    )
    missing_category_rows = (
        summary.get("missing_live_artifact_category_rows")
        or missing_artifact_category_rows(missing_rows)
    )
    missing_action_items = (
        summary.get("missing_live_artifact_action_items")
        or missing_artifact_action_items(missing_category_rows)
    )
    operator_runbook = summary.get("operator_runbook") or operator_action_runbook(missing_action_items)

    return {
        "ok": packet_complete,
        "packet_complete": packet_complete,
        "review_ready": review_ready,
        "base_url": base_url,
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "evidence_bundle_status": bundle_status,
        "summary": summary,
        "missing_endpoints": missing_endpoints,
        "claim_blockers": claim_blockers,
        "primary_claim_blocker": primary_claim_blocker,
        "primary_claim_blocker_next_action": primary_claim_blocker_next_action,
        "missing_live_artifact_count": int(summary.get("missing_live_artifact_count") or len(missing_rows)),
        "missing_live_artifact_blocked_count": int(
            summary.get("missing_live_artifact_blocked_count") or missing_blocked_count
        ),
        "missing_live_artifact_warn_count": int(
            summary.get("missing_live_artifact_warn_count") or missing_warn_count
        ),
        "missing_live_artifact_category_counts": missing_category_counts,
        "missing_live_artifact_category_rows": missing_category_rows,
        "missing_live_artifact_action_items": missing_action_items,
        "operator_runbook": operator_runbook,
        "operator_status": operator_status,
        "vram_model_drivers": vram_model_drivers,
        "live_validation_gates": bundle.get("live_validation_gates") or [],
        "missing_live_artifact_rows": missing_rows,
        "files": files,
    }


def render_review_markdown(manifest: dict[str, Any]) -> str:
    summary = manifest.get("summary") or {}
    blockers = manifest.get("claim_blockers") or []
    missing_endpoints = manifest.get("missing_endpoints") or []
    files = manifest.get("files") or {}
    model_drivers = manifest.get("vram_model_drivers") or {}
    claim_safe_driver_labels = display_vram_driver_labels(
        model_drivers.get("claim_safe_driver_labels") or []
    )
    real_driver_labels = display_vram_driver_labels(model_drivers.get("real_top_driver_labels") or [])
    synthetic_driver_labels = display_vram_driver_labels(
        model_drivers.get("synthetic_pressure_driver_labels") or []
    )
    lines = [
        "# ksolver SRE Evidence Bundle",
        "",
        f"- Base URL: `{manifest.get('base_url', 'unknown')}`",
        f"- Generated: `{manifest.get('generated_at', 'unknown')}`",
        f"- Packet complete: `{str(bool(manifest.get('packet_complete'))).lower()}`",
        f"- Review ready: `{str(bool(manifest.get('review_ready'))).lower()}`",
        f"- Launch status: `{summary.get('launch_status', 'unknown')}`",
        f"- Customer claim ready: `{str(summary.get('customer_claim_ready') is True).lower()}`",
        f"- Mutation allowed: `{str(summary.get('mutation_allowed') is True).lower()}`",
        f"- VRAM advisory ready: `{str(summary.get('vram_advisory_ready') is True).lower()}`",
        f"- VRAM hard admission ready: `{str(summary.get('vram_hard_admission_ready') is True).lower()}`",
        f"- VRAM admission mode: `{summary.get('vram_admission_mode', 'unknown')}`",
        f"- VRAM scheduler use: `{summary.get('vram_scheduler_use', 'unknown')}`",
        f"- VRAM hard blockers: `{summary.get('vram_hard_blocker_count', 'unknown')}`",
        f"- VRAM next evidence: `{summary.get('vram_next_evidence_target', 'unknown')}`",
        f"- VRAM model drivers: `{model_drivers.get('top_driver_count', 'unknown')}`",
        f"- VRAM claim-safe drivers: `{model_drivers.get('claim_safe_driver_count', 'unknown')}`",
        f"- VRAM claim-safe top drivers: `{', '.join(claim_safe_driver_labels) if claim_safe_driver_labels else 'unknown'}`",
        f"- VRAM real model drivers: `{model_drivers.get('real_top_driver_count', 'unknown')}`",
        f"- VRAM real top drivers: `{', '.join(real_driver_labels) if real_driver_labels else 'unknown'}`",
        f"- VRAM synthetic headroom drivers: `{model_drivers.get('synthetic_pressure_driver_count', 'unknown')}`",
        f"- VRAM synthetic headroom labels: `{', '.join(synthetic_driver_labels) if synthetic_driver_labels else 'unknown'}`",
        f"- VRAM driver claim boundary: `{model_drivers.get('claim_boundary') or summary.get('vram_driver_claim_boundary') or 'unknown'}`",
        f"- VRAM synthetic headroom probe driver: `{str(synthetic_headroom_driver_enabled(model_drivers)).lower()}`",
        f"- Production blocker class: `{summary.get('production_readiness_blocker_class', 'unknown')}`",
        f"- Production last error class: `{summary.get('production_readiness_last_error_class', 'unknown')}`",
        f"- Simulator endpoints: `{summary.get('simulator_endpoint_count', 'unknown')}`",
        f"- Simulator probe checked: `{summary.get('simulator_probe_checked_count', 'unknown')}`",
        f"- Simulator probe ready: `{summary.get('simulator_probe_ready_count', 'unknown')}`",
        f"- Simulator probe timeout: `{summary.get('simulator_probe_timeout_millis', 'unknown')} ms`",
        f"- Simulator readiness: `{summary.get('simulator_readiness', 'unknown')}`",
        f"- Simulator readiness note: `{summary.get('simulator_readiness_note', 'unknown')}`",
        f"- Simulator claim mode: `{summary.get('simulator_claim_mode', 'unknown')}`",
        f"- Simulator claim ready: `{str(summary.get('simulator_claim_ready') is True).lower()}`",
        f"- Simulator claim blocker: `{summary.get('simulator_claim_blocker') or 'none'}`",
        f"- Simulator claim next action: `{summary.get('simulator_claim_next_action') or 'none'}`",
        "",
        "## Operator Status",
        "",
        f"- Operator status: `{(manifest.get('operator_status') or {}).get('status', 'unknown')}`",
        f"- Primary blocker: `{manifest.get('primary_claim_blocker') or 'none'}`",
        f"- Next action: `{manifest.get('primary_claim_blocker_next_action') or 'none'}`",
    ]
    operator_binding = (manifest.get("operator_status") or {}).get("binding_safety") or {}
    if operator_binding:
        lines.extend(
            [
                f"- Binding safety: `{operator_binding.get('status', 'unknown')}`",
                f"- Binding mode: `{operator_binding.get('mode', 'unknown')}`",
                f"- Binding reservation pressure: `{operator_binding.get('reservation_pressure', 'unknown')}`",
                f"- Binding reservation pressure meaning: `{operator_binding.get('reservation_pressure_description') or 'unknown'}`",
                f"- Binding reservation pressure scope: `{operator_binding.get('reservation_pressure_scope') or 'unknown'}`",
                f"- Binding reservation pressure reason: `{operator_binding.get('reservation_pressure_reason') or 'unknown'}`",
                f"- Binding reservation pressure action: `{operator_binding.get('reservation_pressure_next_action') or 'unknown'}`",
            ]
        )
    operator_vram = (manifest.get("operator_status") or {}).get("vram") or {}
    operator_vram_labels = display_vram_driver_labels(operator_vram.get("top_driver_labels") or [])
    operator_claim_safe_labels = display_vram_driver_labels(operator_vram.get("claim_safe_driver_labels") or [])
    operator_real_labels = display_vram_driver_labels(operator_vram.get("real_top_driver_labels") or [])
    operator_synthetic_labels = display_vram_driver_labels(operator_vram.get("synthetic_driver_labels") or [])
    lines.append(
        f"- Operator VRAM drivers: `{operator_vram.get('model_driver_count', 'unknown')}`"
    )
    lines.append(
        f"- Operator VRAM all fitted top drivers: `{', '.join(operator_vram_labels) if operator_vram_labels else 'unknown'}`"
    )
    lines.append(
        f"- Operator VRAM claim-safe drivers: `{operator_vram.get('claim_safe_driver_count', 'unknown')}`"
    )
    lines.append(
        f"- Operator VRAM claim-safe top drivers: `{', '.join(operator_claim_safe_labels) if operator_claim_safe_labels else 'unknown'}`"
    )
    lines.append(
        f"- Operator VRAM real drivers: `{operator_vram.get('real_model_driver_count', 'unknown')}`"
    )
    lines.append(
        f"- Operator VRAM real top drivers: `{', '.join(operator_real_labels) if operator_real_labels else 'unknown'}`"
    )
    lines.append(
        f"- Operator VRAM synthetic headroom drivers: `{operator_vram.get('synthetic_driver_count', 'unknown')}`"
    )
    lines.append(
        f"- Operator VRAM synthetic headroom labels: `{', '.join(operator_synthetic_labels) if operator_synthetic_labels else 'unknown'}`"
    )
    if operator_vram.get("driver_claim_boundary"):
        lines.append(
            f"- Operator VRAM driver claim boundary: `{operator_vram.get('driver_claim_boundary')}`"
        )
    lines.append(
        f"- Operator VRAM synthetic headroom probe driver: `{str(synthetic_headroom_driver_enabled(operator_vram)).lower()}`"
    )
    if operator_vram.get("reserve_pressure_definition"):
        lines.append(
            f"- Operator VRAM synthetic headroom: `{operator_vram.get('reserve_pressure_definition')}`"
        )
    if operator_vram.get("investment_demo_rows") is not None:
        lines.append(
            f"- Operator VRAM investment demo: `{operator_vram.get('investment_demo_rows')} rows, "
            f"{operator_vram.get('investment_oom_risk_reduction_pods')} OOM-risk pods reduced, "
            f"{operator_vram.get('investment_high_vram_nodes_preserved')} high-VRAM preserved`"
        )
    demo_gate_result = manifest.get("demo_gate_result") or {}
    if demo_gate_result:
        lines.extend([
            "",
            "## Demo Gate Result",
            "",
            f"- Stage: `{demo_gate_result.get('stage') or 'unknown'}`",
            f"- Exit code: `{demo_gate_result.get('exit_code', 'unknown')}`",
        ])
        if demo_gate_result.get("failed_command"):
            lines.append(f"- Failed command: `{demo_gate_result.get('failed_command')}`")
        if demo_gate_result.get("failed_returncode") is not None:
            lines.append(f"- Failed returncode: `{demo_gate_result.get('failed_returncode')}`")
        if demo_gate_result.get("parse_error"):
            lines.append(f"- Parse error: `{demo_gate_result.get('parse_error')}`")
    doctor_preflight = manifest.get("doctor_preflight") or {}
    if doctor_preflight:
        lines.extend([
            "",
            "## Doctor Preflight",
            "",
            f"- Status: `{doctor_preflight.get('status') or 'unknown'}`",
            f"- Exit code: `{doctor_preflight.get('exit_code', 'unknown')}`",
        ])
        if doctor_preflight.get("first_recommended_command"):
            lines.append(
                f"- First recommended command: `{doctor_preflight.get('first_recommended_command')}`"
            )
        if doctor_preflight.get("failure_count") is not None:
            lines.append(f"- Failures: `{doctor_preflight.get('failure_count')}`")
        if doctor_preflight.get("recommended_command_count") is not None:
            lines.append(
                f"- Recommended commands: `{doctor_preflight.get('recommended_command_count')}`"
            )
        if doctor_preflight.get("api_endpoint_failure_count") is not None:
            lines.append(
                f"- API endpoint failures: `{doctor_preflight.get('api_endpoint_failure_count')}`"
            )
        first_api_failure = doctor_preflight.get("first_api_endpoint_failure") or {}
        if isinstance(first_api_failure, dict) and first_api_failure.get("endpoint"):
            lines.append(
                f"- First API endpoint failure: `{first_api_failure.get('endpoint')}`"
            )
        if doctor_preflight.get("parse_error"):
            lines.append(f"- Parse error: `{doctor_preflight.get('parse_error')}`")
    runbook = manifest.get("operator_runbook") or {}
    runbook_steps = runbook.get("steps") or []
    lines.extend([
        "",
        "## Operator Runbook",
        "",
        f"- Steps: `{runbook.get('step_count', 0)}`",
        f"- Blocked steps: `{runbook.get('blocked_step_count', 0)}`",
        f"- Copyable shell commands: `{runbook.get('copyable_command_count', 0)}`",
        f"- Manual evidence steps: `{runbook.get('manual_step_count', 0)}`",
        f"- Next shell command: `{runbook.get('next_shell_command') or 'none'}`",
    ])
    if runbook_steps:
        for step in runbook_steps:
            command_kind = step.get("command_kind") or "none"
            command_hints = step.get("command_hints")
            if isinstance(command_hints, list) and command_hints:
                command_hint = "`, `".join(str(command) for command in command_hints)
            else:
                command_hint = step.get("command_hint") or "none"
            lines.append(
                f"- `{step.get('severity', 'missing')}` {step.get('priority', '?')}. "
                f"{step.get('category', 'action')}: `{step.get('next_action') or 'none'}` "
                f"({command_kind}: `{command_hint}`)"
            )
    else:
        lines.append("- none")
    command_rows = operator_runbook_command_rows(runbook)
    if command_rows:
        lines.extend(["", "### Copyable Command Provenance", ""])
        for row in command_rows:
            detail_parts = []
            if row.get("severity"):
                detail_parts.append(f"severity `{row.get('severity')}`")
            if row.get("artifact"):
                detail_parts.append(f"artifact `{row.get('artifact')}`")
            detail = f" ({', '.join(detail_parts)})" if detail_parts else ""
            lines.append(
                f"- `{row.get('command')}` "
                f"from `{row.get('category') or 'unknown'}` "
                f"for `{row.get('next_action') or 'no action recorded'}`{detail}"
            )
    live_gates = manifest.get("live_validation_gates") or []
    if live_gates:
        pass_count = sum(1 for gate in live_gates if gate.get("status") == "pass")
        warn_count = sum(1 for gate in live_gates if gate.get("status") == "warn")
        blocked_count = sum(1 for gate in live_gates if gate.get("status") == "blocked")
        lines.extend([
            "",
            "## Live Proof Gates",
            "",
            f"- Gate summary: `{pass_count} pass, {warn_count} warn, {blocked_count} blocked`",
        ])
        for gate in live_gates:
            lines.append(
                f"- `{gate.get('status', 'unknown')}` {gate.get('gate', 'unknown gate')}: `{gate.get('next_action') or gate.get('reason') or 'none'}`"
            )
    missing_rows = manifest.get("missing_live_artifact_rows") or []
    if missing_rows:
        blocked_count = sum(1 for row in missing_rows if row.get("severity") == "blocked")
        warn_count = sum(1 for row in missing_rows if row.get("severity") == "warn")
        lines.extend([
            "",
            "## Missing Live Artifacts",
            "",
            f"- Gap summary: `{blocked_count} blocked, {warn_count} warn`",
        ])
        for row in missing_rows:
            lines.append(
                f"- `{row.get('severity', 'missing')}` {row.get('artifact', 'missing artifact')}: `{row.get('category', 'gap')}` via `{row.get('proof_gate', 'unknown gate')}`; next `{row.get('next_action') or 'none'}`"
            )
    debug_commands = (manifest.get("operator_status") or {}).get("debug_commands") or []
    if debug_commands:
        lines.append(f"- First debug command: `{debug_commands[0]}`")
    else:
        lines.append("- First debug command: `none`")
    production_debug_commands = (
        ((manifest.get("operator_status") or {}).get("production_readiness") or {}).get(
            "debug_commands"
        )
        or []
    )
    if production_debug_commands:
        lines.append(f"- Production first debug command: `{production_debug_commands[0]}`")
    vram_model_drivers = manifest.get("vram_model_drivers") or {}
    labels = (
        vram_model_drivers.get("display_top_driver_labels")
        or display_vram_driver_labels(vram_model_drivers.get("top_driver_labels") or [])
    )
    claim_safe_labels = (
        vram_model_drivers.get("display_claim_safe_driver_labels")
        or display_vram_driver_labels(vram_model_drivers.get("claim_safe_driver_labels") or [])
    )
    real_labels = display_vram_driver_labels(vram_model_drivers.get("real_top_driver_labels") or [])
    synthetic_labels = display_vram_driver_labels(
        vram_model_drivers.get("synthetic_pressure_driver_labels") or []
    )
    top_descriptions = vram_model_drivers.get("top_driver_descriptions") or []
    claim_safe_descriptions = vram_model_drivers.get("claim_safe_driver_descriptions") or []
    organic_descriptions = vram_model_drivers.get("top_organic_driver_descriptions") or []
    group_impacts = vram_model_drivers.get("group_impacts") or []
    top_group_impact = group_impacts[0] if group_impacts and isinstance(group_impacts[0], dict) else {}
    lines.extend([
        "",
        "## VRAM Model Drivers",
        "",
        f"- Available: `{str(vram_model_drivers.get('available') is True).lower()}`",
        f"- Fit: `{vram_model_drivers.get('fit') or 'unknown'}`",
        f"- Training rows: `{vram_model_drivers.get('training_rows', 'unknown')}`",
        f"- Impact basis: `{vram_model_drivers.get('impact_basis') or 'unknown'}`",
        f"- Top impact group: `{top_group_impact.get('group') or 'unknown'}`",
        f"- Top driver count: `{vram_model_drivers.get('top_driver_count', 'unknown')}`",
        f"- Claim-safe driver count: `{vram_model_drivers.get('claim_safe_driver_count', 'unknown')}`",
        f"- Claim-safe drivers: `{', '.join(claim_safe_labels) if claim_safe_labels else 'unknown'}`",
        f"- Claim-safe driver meaning: `{', '.join(claim_safe_descriptions[:3]) if claim_safe_descriptions else 'unknown'}`",
        f"- Real top driver count: `{vram_model_drivers.get('real_top_driver_count', 'unknown')}`",
        f"- Real top drivers: `{', '.join(real_labels) if real_labels else 'unknown'}`",
        f"- Synthetic headroom driver count: `{vram_model_drivers.get('synthetic_pressure_driver_count', 'unknown')}`",
        f"- Synthetic headroom drivers: `{', '.join(synthetic_labels) if synthetic_labels else 'unknown'}`",
        f"- Claim boundary: `{vram_model_drivers.get('claim_boundary') or 'unknown'}`",
        f"- Synthetic headroom probe driver: `{str(synthetic_headroom_driver_enabled(vram_model_drivers)).lower()}`",
        f"- All fitted top drivers: `{', '.join(labels) if labels else 'unknown'}`",
        f"- All fitted top driver meaning: `{', '.join(top_descriptions[:3]) if top_descriptions else 'unknown'}`",
        f"- Organic driver descriptions: `{', '.join(organic_descriptions[:3]) if organic_descriptions else 'unknown'}`",
    ])
    lines.extend([
        "",
        "## Claim Blockers",
        "",
    ])
    if blockers:
        lines.extend(f"- {blocker}" for blocker in blockers)
    else:
        lines.append("- none")
    lines.extend(["", "## Missing Endpoints", ""])
    if missing_endpoints:
        lines.extend(f"- `{endpoint}`" for endpoint in missing_endpoints)
    else:
        lines.append("- none")
    lines.extend(["", "## Captured Files", ""])
    for endpoint in sorted(files):
        row = files[endpoint] or {}
        digest = str(row.get("sha256") or "")
        digest_label = digest[:12] if digest else "missing"
        lines.append(
            f"- `{endpoint}` -> `{row.get('file', 'missing')}` "
            f"(status `{row.get('status', 'unknown')}`, "
            f"bytes `{row.get('bytes', 'unknown')}`, sha256 `{digest_label}`)"
        )
    lines.extend(["", "## Next Action", ""])
    if manifest.get("review_ready"):
        lines.append("- Packet is ready for SRE review.")
    elif blockers:
        lines.append("- Resolve the claim blockers above before using this packet for customer-facing claims.")
    else:
        lines.append("- Inspect captured files before using this packet for customer-facing claims.")
    lines.append("")
    return "\n".join(lines)


def exit_code_for_manifest(manifest: dict[str, Any], *, require_review_ready: bool) -> int:
    if manifest.get("packet_complete") is not True:
        return 1
    if require_review_ready and manifest.get("review_ready") is not True:
        return 2
    return 0


def result_from_manifest(
    manifest: dict[str, Any], *, output_dir: pathlib.Path, require_review_ready: bool
) -> dict[str, Any]:
    exit_code = exit_code_for_manifest(
        manifest, require_review_ready=require_review_ready
    )
    summary = manifest["summary"]
    vram_model_drivers = manifest.get("vram_model_drivers") or {}
    vram_display_top_driver_labels = (
        vram_model_drivers.get("display_top_driver_labels")
        or display_vram_driver_labels(vram_model_drivers.get("top_driver_labels") or [])
    )
    vram_display_claim_safe_driver_labels = (
        vram_model_drivers.get("display_claim_safe_driver_labels")
        or display_vram_driver_labels(vram_model_drivers.get("claim_safe_driver_labels") or [])
    )
    vram_display_real_top_driver_labels = (
        vram_model_drivers.get("display_real_top_driver_labels")
        or display_vram_driver_labels(vram_model_drivers.get("real_top_driver_labels") or [])
    )
    vram_display_synthetic_driver_labels = (
        vram_model_drivers.get("display_synthetic_pressure_driver_labels")
        or display_vram_driver_labels(vram_model_drivers.get("synthetic_pressure_driver_labels") or [])
    )
    operator_binding = (manifest.get("operator_status") or {}).get("binding_safety") or {}
    return {
        "ok": exit_code == 0,
        "exit_code": exit_code,
        "packet_complete": manifest["packet_complete"],
        "review_ready": manifest["review_ready"],
        "require_review_ready": require_review_ready,
        "output_dir": str(output_dir),
        "summary": manifest["summary"],
        "claim_blockers": manifest["claim_blockers"],
        "primary_claim_blocker": manifest.get("primary_claim_blocker"),
        "primary_claim_blocker_next_action": manifest.get("primary_claim_blocker_next_action"),
        "missing_live_artifact_count": manifest.get("missing_live_artifact_count"),
        "missing_live_artifact_blocked_count": manifest.get("missing_live_artifact_blocked_count"),
        "missing_live_artifact_warn_count": manifest.get("missing_live_artifact_warn_count"),
        "missing_live_artifact_category_counts": manifest.get("missing_live_artifact_category_counts") or {},
        "missing_live_artifact_category_rows": manifest.get("missing_live_artifact_category_rows") or [],
        "missing_live_artifact_action_items": manifest.get("missing_live_artifact_action_items") or [],
        "operator_runbook": manifest.get("operator_runbook") or {},
        "missing_live_artifact_rows": manifest.get("missing_live_artifact_rows") or [],
        "operator_status": manifest.get("operator_status") or {},
        "operator_binding_status": operator_binding.get("status"),
        "operator_reservation_pressure": operator_binding.get("reservation_pressure"),
        "operator_reservation_pressure_description": operator_binding.get("reservation_pressure_description"),
        "operator_reservation_pressure_scope": operator_binding.get("reservation_pressure_scope"),
        "operator_reservation_pressure_reason": operator_binding.get("reservation_pressure_reason"),
        "operator_reservation_pressure_next_action": operator_binding.get("reservation_pressure_next_action"),
        "live_validation_gates": manifest.get("live_validation_gates") or [],
        "vram_model_drivers": vram_model_drivers,
        "vram_model_driver_count": vram_model_drivers.get("top_driver_count"),
        "vram_driver_impact_basis": vram_model_drivers.get("impact_basis"),
        "vram_top_driver_descriptions": vram_model_drivers.get("top_driver_descriptions") or [],
        "vram_claim_safe_driver_descriptions": vram_model_drivers.get("claim_safe_driver_descriptions") or [],
        "vram_real_top_driver_descriptions": vram_model_drivers.get("real_top_driver_descriptions") or [],
        "vram_synthetic_driver_descriptions": vram_model_drivers.get("synthetic_pressure_driver_descriptions") or [],
        "vram_top_organic_driver_descriptions": vram_model_drivers.get("top_organic_driver_descriptions") or [],
        "vram_top_driver_group_impacts": vram_model_drivers.get("group_impacts") or [],
        "vram_top_driver_labels": vram_model_drivers.get("top_driver_labels") or [],
        "vram_display_top_driver_labels": vram_display_top_driver_labels,
        "vram_claim_safe_driver_count": vram_model_drivers.get("claim_safe_driver_count"),
        "vram_claim_safe_driver_labels": vram_model_drivers.get("claim_safe_driver_labels") or [],
        "vram_display_claim_safe_driver_labels": vram_display_claim_safe_driver_labels,
        "vram_real_model_driver_count": vram_model_drivers.get("real_top_driver_count"),
        "vram_real_top_driver_labels": vram_model_drivers.get("real_top_driver_labels") or [],
        "vram_display_real_top_driver_labels": vram_display_real_top_driver_labels,
        "vram_synthetic_driver_count": vram_model_drivers.get("synthetic_pressure_driver_count"),
        "vram_synthetic_driver_labels": vram_model_drivers.get("synthetic_pressure_driver_labels") or [],
        "vram_display_synthetic_driver_labels": vram_display_synthetic_driver_labels,
        "vram_synthetic_reserve_driver": vram_model_drivers.get("synthetic_reserve_driver"),
        "vram_synthetic_headroom_driver": vram_model_drivers.get("synthetic_headroom_driver"),
        "vram_reserve_pressure_definition": summary.get("vram_reserve_pressure_definition"),
        "vram_driver_claim_boundary": summary.get("vram_driver_claim_boundary"),
        "vram_investment_demo_rows": summary.get("vram_investment_demo_rows"),
        "vram_investment_oom_risk_reduction_pods": summary.get("vram_investment_oom_risk_reduction_pods"),
        "vram_investment_high_vram_nodes_preserved": summary.get("vram_investment_high_vram_nodes_preserved"),
        "vram_investment_advisory_rows": summary.get("vram_investment_advisory_rows"),
        "vram_investment_average_baseline_oom_risk_percent": summary.get("vram_investment_average_baseline_oom_risk_percent"),
        "vram_investment_average_ksolver_oom_risk_percent": summary.get("vram_investment_average_ksolver_oom_risk_percent"),
        "endpoint_file_count": len(manifest["files"]),
        "review_artifact_count": 2,
        "file_count": len(manifest["files"]) + 2,
        "vram_admission_mode": summary.get("vram_admission_mode"),
        "vram_scheduler_use": summary.get("vram_scheduler_use"),
        "vram_hard_blocker_count": summary.get("vram_hard_blocker_count"),
        "vram_next_evidence_target": summary.get("vram_next_evidence_target"),
        "production_readiness_blocker_class": summary.get("production_readiness_blocker_class"),
        "production_readiness_last_error_class": summary.get("production_readiness_last_error_class"),
        "production_readiness_debug_commands": summary.get("production_readiness_debug_commands") or [],
        "production_readiness_first_debug_command": summary.get(
            "production_readiness_first_debug_command"
        ),
        "simulator_endpoint_count": summary.get("simulator_endpoint_count"),
        "simulator_probe_checked_count": summary.get("simulator_probe_checked_count"),
        "simulator_probe_ready_count": summary.get("simulator_probe_ready_count"),
        "simulator_probe_timeout_millis": summary.get("simulator_probe_timeout_millis"),
        "simulator_readiness": summary.get("simulator_readiness"),
        "simulator_readiness_note": summary.get("simulator_readiness_note"),
        "simulator_claim_mode": summary.get("simulator_claim_mode"),
        "simulator_claim_ready": summary.get("simulator_claim_ready"),
        "simulator_claim_blocker": summary.get("simulator_claim_blocker"),
        "simulator_claim_next_action": summary.get("simulator_claim_next_action"),
    }


def collect_bundle(base_url: str, output_dir: pathlib.Path) -> dict[str, Any]:
    base = base_url.rstrip("/")
    output_dir.mkdir(parents=True, exist_ok=True)

    bundle_status, bundle = fetch_json(f"{base}/api/scheduler/evidence-bundle")
    endpoints = endpoints_from_bundle(bundle)
    files: dict[str, dict[str, Any]] = {}
    captured_payloads: dict[str, dict[str, Any]] = {}

    def capture_endpoint(endpoint: str) -> int:
        status, payload = fetch_json(f"{base}{endpoint}")
        captured_payloads[endpoint] = payload
        filename = endpoint_filename(endpoint)
        path = output_dir / filename
        write_json(path, payload)
        files[endpoint] = {
            "file": filename,
            "status": status,
            "ok": payload.get("ok"),
            **file_metadata(path),
        }
        return status

    for endpoint in endpoints:
        capture_endpoint(endpoint)

    for _ in range(3):
        evidence_summary = (
            captured_payloads.get("/api/scheduler/evidence-bundle", {}).get("summary")
            or {}
        )
        evidence_actions = evidence_summary.get("missing_live_artifact_action_items") or []
        evidence_runbook = evidence_summary.get("operator_runbook") or {}
        operator_payload = captured_payloads.get("/api/scheduler/operator-status") or {}
        operator_actions = operator_payload.get("action_items") or []
        operator_runbook = operator_payload.get("operator_runbook") or {}
        if evidence_actions == operator_actions and evidence_runbook == operator_runbook:
            break
        if (
            "/api/scheduler/evidence-bundle" not in endpoints
            or "/api/scheduler/operator-status" not in endpoints
        ):
            break
        capture_endpoint("/api/scheduler/evidence-bundle")
        capture_endpoint("/api/scheduler/operator-status")

    captured_bundle = captured_payloads.get("/api/scheduler/evidence-bundle")
    manifest_bundle = captured_bundle if isinstance(captured_bundle, dict) else bundle
    manifest_bundle_status = int(
        (files.get("/api/scheduler/evidence-bundle") or {}).get("status") or bundle_status
    )

    manifest = build_manifest(
        base_url=base,
        bundle_status=manifest_bundle_status,
        bundle=manifest_bundle,
        files=files,
        captured_payloads=captured_payloads,
    )
    demo_gate_result = summarize_demo_gate_result(
        load_optional_json(output_dir / "demo-gate-result.json")
    )
    if demo_gate_result:
        manifest["demo_gate_result"] = demo_gate_result
    doctor_preflight = summarize_doctor_preflight(
        load_optional_json(output_dir / "doctor-preflight.json")
    )
    if doctor_preflight:
        manifest["doctor_preflight"] = doctor_preflight
    write_json(output_dir / "manifest.json", manifest)
    (output_dir / "review.md").write_text(render_review_markdown(manifest), encoding="utf-8")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Collect ksolver shadow SRE evidence JSON files."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8090",
        help="shadow server URL; default: %(default)s",
    )
    parser.add_argument(
        "--output-root",
        default="evidence-bundles",
        help="parent directory for timestamped evidence bundles; default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        help="exact output directory; overrides --output-root timestamp creation",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="print machine-readable manifest summary",
    )
    parser.add_argument(
        "--require-review-ready",
        action="store_true",
        help="exit nonzero when capture succeeds but the packet is still claim-blocked",
    )
    args = parser.parse_args()

    output_dir = (
        pathlib.Path(args.output_dir)
        if args.output_dir
        else pathlib.Path(args.output_root) / f"evidence-bundle-{timestamp_slug()}"
    )
    manifest = collect_bundle(args.base_url, output_dir)
    result = result_from_manifest(
        manifest,
        output_dir=output_dir,
        require_review_ready=args.require_review_ready,
    )
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(
            "evidence bundle collected: "
            f"{output_dir} ({result['endpoint_file_count']} endpoint files "
            f"+ manifest/review, {result['file_count']} total artifacts, "
            f"launch {manifest['summary'].get('launch_status', 'unknown')}, "
            f"review {'ready' if manifest['review_ready'] else 'blocked'})"
        )
    return result["exit_code"]


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001 - CLI should return one concise failure.
        print(f"evidence bundle collection failed: {exc}", file=sys.stderr)
        raise SystemExit(1)
