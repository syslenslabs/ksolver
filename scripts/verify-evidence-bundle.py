#!/usr/bin/env python3
"""Verify a captured ksolver SRE evidence bundle directory."""

from __future__ import annotations

import argparse
import hashlib
import json
import pathlib
import sys
from typing import Any

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from evidence_helpers import (  # noqa: E402
    category_counts_text,
    display_vram_driver_label,
    display_vram_driver_labels,
    missing_artifact_action_items,
    missing_artifact_category_counts,
    missing_artifact_category_rows,
    operator_action_runbook,
    operator_runbook_command_rows,
    synthetic_headroom_driver_enabled,
)


def load_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def compare_summary_field(
    errors: list[str],
    manifest_summary: dict[str, Any],
    captured_summary: dict[str, Any],
    field: str,
) -> None:
    if manifest_summary.get(field) != captured_summary.get(field):
        errors.append(
            f"summary {field} mismatch manifest={manifest_summary.get(field)!r} captured={captured_summary.get(field)!r}"
        )


def markdown_section(text: str, heading: str) -> str | None:
    marker = f"\n## {heading}\n"
    start = text.find(marker)
    prefix_adjust = 1
    if start < 0 and text.startswith(f"## {heading}\n"):
        start = 0
        prefix_adjust = 0
    if start < 0:
        return None
    section_start = start + prefix_adjust
    next_heading = text.find("\n## ", section_start + len(f"## {heading}\n"))
    if next_heading < 0:
        return text[section_start:]
    return text[section_start:next_heading]


def validate_operator_runbook(errors: list[str], label: str, runbook: Any, action_items: list[Any]) -> None:
    if not isinstance(runbook, dict):
        errors.append(f"{label}: operator runbook missing")
        return
    expected = operator_action_runbook(action_items)
    for field in [
        "step_count",
        "blocked_step_count",
        "manual_step_count",
        "copyable_command_count",
        "next_shell_command",
        "copyable_commands",
        "copyable_command_rows",
    ]:
        if runbook.get(field) != expected.get(field):
            errors.append(f"{label}: operator runbook {field} mismatch")
    if runbook.get("steps") != expected.get("steps"):
        errors.append(f"{label}: operator runbook steps mismatch")
    if action_items and runbook.get("next_step") != action_items[0]:
        errors.append(f"{label}: operator runbook next_step mismatch")


def validate_doctor_preflight(bundle_dir: pathlib.Path, errors: list[str]) -> dict[str, Any] | None:
    path = bundle_dir / "doctor-preflight.json"
    if not path.exists():
        return None
    try:
        payload = load_json(path)
    except json.JSONDecodeError as exc:
        errors.append(f"doctor-preflight.json is not valid JSON ({exc.msg})")
        return None
    if not isinstance(payload, dict):
        errors.append("doctor-preflight.json is not an object")
        return None
    status = payload.get("status")
    if status not in ("ready", "degraded", "blocked"):
        errors.append("doctor-preflight.json has invalid status")
    exit_code = payload.get("exit_code")
    if not isinstance(exit_code, int):
        errors.append("doctor-preflight.json missing integer exit_code")
    elif payload.get("ok") is True and exit_code != 0:
        errors.append("doctor-preflight.json ok=true but exit_code is not 0")
    elif payload.get("ok") is False and exit_code == 0:
        errors.append("doctor-preflight.json ok=false but exit_code is 0")
    failures = payload.get("failures")
    if failures is not None and not isinstance(failures, list):
        errors.append("doctor-preflight.json failures is not a list")
    recommended = payload.get("recommended_commands")
    if recommended is not None and not isinstance(recommended, list):
        errors.append("doctor-preflight.json recommended_commands is not a list")
    api_failures = payload.get("api_endpoint_failures")
    if api_failures is not None and not isinstance(api_failures, list):
        errors.append("doctor-preflight.json api_endpoint_failures is not a list")
    elif isinstance(api_failures, list):
        for idx, row in enumerate(api_failures):
            if not isinstance(row, dict):
                errors.append(f"doctor-preflight.json api_endpoint_failures[{idx}] is not an object")
                continue
            if not row.get("endpoint"):
                errors.append(f"doctor-preflight.json api_endpoint_failures[{idx}] missing endpoint")
            if not row.get("reason"):
                errors.append(f"doctor-preflight.json api_endpoint_failures[{idx}] missing reason")
    first = payload.get("first_recommended_command")
    if first is not None:
        commands = [
            row.get("command")
            for row in recommended or []
            if isinstance(row, dict) and row.get("command")
        ]
        if first not in commands:
            errors.append("doctor-preflight.json first recommended command is not in recommended_commands")
    if payload.get("simulator_claim_ready") is False and not payload.get("simulator_claim_blocker"):
        errors.append("doctor-preflight.json simulator claim blocker missing while claim is not ready")
    if payload.get("kss_ready_count") is not None and not isinstance(payload.get("kss_ready_count"), int):
        errors.append("doctor-preflight.json kss_ready_count is not an integer")
    return payload


def doctor_preflight_manifest_summary(payload: dict[str, Any]) -> dict[str, Any]:
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


def validate_demo_gate_result(bundle_dir: pathlib.Path, errors: list[str]) -> dict[str, Any] | None:
    path = bundle_dir / "demo-gate-result.json"
    if not path.exists():
        return None
    try:
        payload = load_json(path)
    except json.JSONDecodeError as exc:
        errors.append(f"demo-gate-result.json is not valid JSON ({exc.msg})")
        return None
    if not isinstance(payload, dict):
        errors.append("demo-gate-result.json is not an object")
        return None
    if not isinstance(payload.get("ok"), bool):
        errors.append("demo-gate-result.json missing boolean ok")
    stage = payload.get("stage")
    if not stage:
        errors.append("demo-gate-result.json missing stage")
    exit_code = payload.get("exit_code")
    if not isinstance(exit_code, int):
        errors.append("demo-gate-result.json missing integer exit_code")
    elif payload.get("ok") is True and exit_code != 0:
        errors.append("demo-gate-result.json ok=true but exit_code is not 0")
    elif payload.get("ok") is False and exit_code == 0:
        errors.append("demo-gate-result.json ok=false but exit_code is 0")
    output_dir = payload.get("output_dir")
    if output_dir is not None and str(output_dir) != str(bundle_dir):
        errors.append("demo-gate-result.json output_dir does not match bundle directory")
    child_failure_stages = {"doctor-preflight", "kss-preflight", "smoke", "collect", "verify"}
    if payload.get("ok") is False and stage in child_failure_stages:
        if not payload.get("failed_command"):
            errors.append("demo-gate-result.json child failure missing failed_command")
        if not isinstance(payload.get("failed_returncode"), int):
            errors.append("demo-gate-result.json child failure missing integer failed_returncode")
        if not (
            payload.get("failed_stdout_excerpt")
            or payload.get("failed_stderr_excerpt")
            or payload.get("error")
        ):
            errors.append("demo-gate-result.json child failure missing stderr/stdout/error excerpt")
    return payload


def demo_gate_manifest_summary(payload: dict[str, Any]) -> dict[str, Any]:
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


def verify_bundle(bundle_dir: pathlib.Path) -> dict[str, Any]:
    errors: list[str] = []
    manifest_path = bundle_dir / "manifest.json"
    review_path = bundle_dir / "review.md"
    if not manifest_path.exists():
        return {
            "ok": False,
            "bundle_dir": str(bundle_dir),
            "errors": ["missing manifest.json"],
        }
    manifest = load_json(manifest_path)
    files = manifest.get("files") or {}
    summary = manifest.get("summary") or {}
    if not isinstance(files, dict) or not files:
        errors.append("manifest has no files map")
    expected_command_count = summary.get("collection_command_count")
    if expected_command_count is not None and expected_command_count != len(files):
        errors.append(
            f"summary collection_command_count mismatch expected {expected_command_count} got {len(files)}"
        )
    required_vram_fields = [
        "vram_advisory_ready",
        "vram_hard_admission_ready",
        "vram_admission_mode",
        "vram_scheduler_use",
        "vram_hard_blocker_count",
        "vram_next_evidence_target",
        "production_readiness_blocker_class",
        "production_readiness_last_error_class",
    ]
    for field in required_vram_fields:
        if field not in summary:
            errors.append(f"summary missing {field}")
    required_simulator_fields = [
        "simulator_endpoint_count",
        "simulator_probe_checked_count",
        "simulator_probe_ready_count",
        "simulator_probe_timeout_millis",
        "simulator_readiness",
        "simulator_readiness_note",
        "simulator_claim_ready",
        "simulator_claim_mode",
        "simulator_claim_next_action",
    ]
    for field in required_simulator_fields:
        if field not in summary:
            errors.append(f"summary missing {field}")
    required_operator_binding_fields = [
        "operator_binding_status",
        "operator_reservation_pressure",
        "operator_reservation_pressure_description",
        "operator_reservation_pressure_scope",
        "operator_reservation_pressure_reason",
        "operator_reservation_pressure_next_action",
    ]
    for field in required_operator_binding_fields:
        if field not in summary:
            errors.append(f"summary missing {field}")
    if not isinstance(summary.get("simulator_endpoint_count"), int):
        errors.append("summary simulator_endpoint_count is not an integer")
    if not isinstance(summary.get("simulator_probe_checked_count"), int):
        errors.append("summary simulator_probe_checked_count is not an integer")
    if not isinstance(summary.get("simulator_probe_ready_count"), int):
        errors.append("summary simulator_probe_ready_count is not an integer")
    if not isinstance(summary.get("simulator_probe_timeout_millis"), int):
        errors.append("summary simulator_probe_timeout_millis is not an integer")
    if not summary.get("simulator_readiness"):
        errors.append("summary missing simulator readiness")
    if not isinstance(summary.get("simulator_claim_ready"), bool):
        errors.append("summary simulator_claim_ready is not a boolean")
    if not summary.get("simulator_claim_mode"):
        errors.append("summary missing simulator claim mode")
    if summary.get("simulator_claim_ready") is False and not summary.get("simulator_claim_blocker"):
        errors.append("summary simulator claim blocker missing while claim is not ready")
    if not summary.get("simulator_claim_next_action"):
        errors.append("summary missing simulator claim next action")
    if summary.get("vram_advisory_ready") is not True:
        errors.append("summary vram_advisory_ready is not true")
    if summary.get("vram_hard_admission_ready") is False:
        if not summary.get("vram_admission_mode"):
            errors.append("summary missing VRAM admission mode while hard admission is blocked")
        if not summary.get("vram_next_evidence_target"):
            errors.append("summary missing VRAM next evidence while hard admission is blocked")
        blocker_count = summary.get("vram_hard_blocker_count")
        if not isinstance(blocker_count, int) or blocker_count <= 0:
            errors.append("summary missing positive VRAM hard blocker count while hard admission is blocked")
    claim_blockers = manifest.get("claim_blockers") or []
    primary_claim_blocker = manifest.get("primary_claim_blocker")
    primary_claim_blocker_next_action = manifest.get("primary_claim_blocker_next_action")
    operator_status = manifest.get("operator_status") or {}
    vram_model_drivers = manifest.get("vram_model_drivers") or {}
    missing_rows = manifest.get("missing_live_artifact_rows") or []
    missing_category_rows = manifest.get(
        "missing_live_artifact_category_rows",
        missing_artifact_category_rows(missing_rows),
    )
    missing_action_items = manifest.get(
        "missing_live_artifact_action_items",
        missing_artifact_action_items(missing_category_rows),
    )
    operator_runbook = manifest.get("operator_runbook") or {}
    validate_operator_runbook(errors, "manifest", operator_runbook, missing_action_items)
    doctor_preflight = validate_doctor_preflight(bundle_dir, errors)
    manifest_doctor_preflight = manifest.get("doctor_preflight")
    if doctor_preflight is not None:
        expected_doctor_preflight = doctor_preflight_manifest_summary(doctor_preflight)
        if manifest_doctor_preflight != expected_doctor_preflight:
            errors.append("manifest doctor preflight mismatch")
    demo_gate_result = validate_demo_gate_result(bundle_dir, errors)
    manifest_demo_gate_result = manifest.get("demo_gate_result")
    if demo_gate_result is not None:
        expected_demo_gate_result = demo_gate_manifest_summary(demo_gate_result)
        if manifest_demo_gate_result != expected_demo_gate_result:
            errors.append("manifest demo-gate result mismatch")
    production_class = summary.get("production_readiness_blocker_class")
    if production_class and production_class != "none":
        expected_production_blocker = f"production readiness blocked: {production_class}"
        if expected_production_blocker not in claim_blockers:
            errors.append("claim blockers missing production readiness blocker")
        if primary_claim_blocker != expected_production_blocker:
            errors.append("primary claim blocker must name production readiness blocker")
        if not primary_claim_blocker_next_action:
            errors.append("primary claim blocker missing next action")

    verified_files = 0
    captured_payloads: dict[str, dict[str, Any]] = {}
    for endpoint, row in sorted(files.items()):
        if not isinstance(row, dict):
            errors.append(f"{endpoint}: file entry is not an object")
            continue
        filename = row.get("file")
        if not filename:
            errors.append(f"{endpoint}: missing file name")
            continue
        path = bundle_dir / str(filename)
        if not path.exists():
            errors.append(f"{endpoint}: missing captured file {filename}")
            continue
        actual_bytes = path.stat().st_size
        expected_bytes = row.get("bytes")
        if expected_bytes != actual_bytes:
            errors.append(f"{endpoint}: byte count mismatch expected {expected_bytes} got {actual_bytes}")
        actual_sha = sha256_file(path)
        expected_sha = row.get("sha256")
        if expected_sha != actual_sha:
            errors.append(f"{endpoint}: sha256 mismatch")
        if row.get("status") != 200:
            errors.append(f"{endpoint}: captured status is {row.get('status')}")
        try:
            captured_payloads[endpoint] = load_json(path)
        except json.JSONDecodeError as exc:
            errors.append(f"{endpoint}: captured file is not valid JSON ({exc.msg})")
        verified_files += 1

    captured_bundle = captured_payloads.get("/api/scheduler/evidence-bundle")
    if captured_bundle:
        captured_summary = captured_bundle.get("summary") or {}
        if not isinstance(captured_summary, dict) or not captured_summary:
            errors.append("/api/scheduler/evidence-bundle: captured summary missing")
        else:
            for field in [
                "collection_command_count",
                "vram_advisory_ready",
                "vram_hard_admission_ready",
                "vram_admission_mode",
                "vram_scheduler_use",
                "vram_hard_blocker_count",
                "vram_next_evidence_target",
                "vram_model_driver_count",
                "vram_top_driver_labels",
                "vram_real_model_driver_count",
                "vram_real_top_driver_labels",
                "vram_synthetic_driver_count",
                "vram_synthetic_driver_labels",
                "vram_synthetic_reserve_driver",
                "vram_synthetic_headroom_driver",
                "vram_reserve_pressure_definition",
                "vram_driver_claim_boundary",
                "vram_investment_demo_rows",
                "vram_investment_oom_risk_reduction_pods",
                "vram_investment_high_vram_nodes_preserved",
                "vram_investment_advisory_rows",
                "vram_investment_average_baseline_oom_risk_percent",
                "vram_investment_average_ksolver_oom_risk_percent",
                "production_readiness_blocker_class",
                "simulator_endpoint_count",
                "simulator_probe_checked_count",
                "simulator_probe_ready_count",
                "simulator_probe_timeout_millis",
                "simulator_readiness",
                "simulator_readiness_note",
                "simulator_claim_ready",
                "simulator_claim_mode",
                "simulator_claim_blocker",
                "simulator_claim_next_action",
                "operator_binding_status",
                "operator_reservation_pressure",
                "operator_reservation_pressure_description",
                "operator_reservation_pressure_scope",
                "operator_reservation_pressure_reason",
                "operator_reservation_pressure_next_action",
                "live_validation_gate_count",
                "live_validation_pass_count",
                "live_validation_warn_count",
                "live_validation_blocked_count",
                "review_ready",
            ]:
                compare_summary_field(errors, summary, captured_summary, field)
            captured_action_items = captured_summary.get("missing_live_artifact_action_items") or []
            if missing_action_items != captured_action_items:
                errors.append("/api/scheduler/evidence-bundle: action items mismatch")
            captured_runbook = captured_summary.get("operator_runbook") or {}
            if operator_runbook != captured_runbook:
                errors.append("/api/scheduler/evidence-bundle: operator runbook mismatch")
            validate_operator_runbook(
                errors,
                "/api/scheduler/evidence-bundle",
                captured_runbook,
                captured_action_items,
            )
            captured_live_gates = captured_bundle.get("live_validation_gates") or []
            if (manifest.get("live_validation_gates") or []) != captured_live_gates:
                errors.append("manifest live validation gates mismatch captured evidence-bundle")
            if captured_summary.get("live_validation_gate_count") != len(captured_live_gates):
                errors.append("/api/scheduler/evidence-bundle: live validation gate count mismatch")
    else:
        errors.append("/api/scheduler/evidence-bundle: captured artifact missing")

    captured_operator = captured_payloads.get("/api/scheduler/operator-status")
    if not captured_operator:
        errors.append("/api/scheduler/operator-status: captured artifact missing")
    else:
        if captured_operator.get("ok") is not True:
            errors.append("/api/scheduler/operator-status: ok is not true")
        if captured_operator.get("dry_run") is not True:
            errors.append("/api/scheduler/operator-status: dry_run is not true")
        status = captured_operator.get("status")
        if status not in ("ready", "blocked", "needs-evidence"):
            errors.append("/api/scheduler/operator-status: invalid status")
        operator_blocker = captured_operator.get("primary_blocker")
        if primary_claim_blocker != operator_blocker:
            errors.append(
                f"/api/scheduler/operator-status: primary blocker mismatch manifest={primary_claim_blocker!r} captured={operator_blocker!r}"
            )
        operator_action = captured_operator.get("next_action")
        if primary_claim_blocker_next_action != operator_action:
            errors.append(
                f"/api/scheduler/operator-status: next action mismatch manifest={primary_claim_blocker_next_action!r} captured={operator_action!r}"
            )
        debug_commands = captured_operator.get("debug_commands") or []
        if status != "ready" and not debug_commands:
            errors.append("/api/scheduler/operator-status: blocked status missing debug commands")
        operator_production_class = (
            (captured_operator.get("production_readiness") or {}).get("blocker_class")
        )
        if production_class != operator_production_class:
            errors.append(
                f"/api/scheduler/operator-status: production blocker class mismatch manifest={production_class!r} captured={operator_production_class!r}"
            )
        captured_production = captured_operator.get("production_readiness") or {}
        manifest_production = (
            operator_status.get("production_readiness") or {}
        ) if isinstance(operator_status, dict) else {}
        if manifest_production != captured_production:
            errors.append("manifest operator-status production readiness mismatch")
        if manifest_production.get("blocker_class") != summary.get("production_readiness_blocker_class"):
            errors.append("manifest operator-status production blocker class mismatch")
        captured_binding = captured_operator.get("binding_safety") or {}
        manifest_binding = (
            operator_status.get("binding_safety") or {}
        ) if isinstance(operator_status, dict) else {}
        if manifest_binding != captured_binding:
            errors.append("manifest operator-status binding safety mismatch")
        if captured_binding:
            if not manifest_binding.get("reservation_pressure"):
                errors.append("manifest operator-status missing reservation pressure")
            if "pending or reserved GPU capacity" not in str(
                manifest_binding.get("reservation_pressure_description") or ""
            ):
                errors.append("manifest operator-status reservation pressure description missing capacity-risk definition")
            if "unrelated to CUDA" not in str(manifest_binding.get("reservation_pressure_scope") or ""):
                errors.append("manifest operator-status reservation pressure scope missing VRAM distinction")
            summary_binding_pairs = [
                ("operator_binding_status", "status"),
                ("operator_reservation_pressure", "reservation_pressure"),
                ("operator_reservation_pressure_description", "reservation_pressure_description"),
                ("operator_reservation_pressure_scope", "reservation_pressure_scope"),
                ("operator_reservation_pressure_reason", "reservation_pressure_reason"),
                ("operator_reservation_pressure_next_action", "reservation_pressure_next_action"),
            ]
            for summary_key, binding_key in summary_binding_pairs:
                if summary.get(summary_key) != manifest_binding.get(binding_key):
                    errors.append(
                        f"summary {summary_key} mismatch operator-status binding_safety.{binding_key}"
                    )
        manifest_production_debug = manifest_production.get("debug_commands") or []
        captured_production_debug = captured_production.get("debug_commands") or []
        if manifest_production_debug != captured_production_debug:
            errors.append("manifest operator-status production debug commands mismatch")
        summary_production_debug = summary.get("production_readiness_debug_commands") or []
        if summary_production_debug != captured_production_debug:
            errors.append("summary production readiness debug commands mismatch")
        summary_first_production_debug = summary.get("production_readiness_first_debug_command")
        if captured_production_debug and summary_first_production_debug != captured_production_debug[0]:
            errors.append("summary production first debug command mismatch")
        if (
            isinstance(primary_claim_blocker, str)
            and primary_claim_blocker.startswith("production readiness blocked:")
            and manifest_production_debug
            and operator_runbook.get("next_shell_command")
            != manifest_production_debug[0]
        ):
            errors.append("operator runbook first shell command does not match production readiness first debug command")
        operator_vram = captured_operator.get("vram") or {}
        manifest_operator_vram = (operator_status.get("vram") or {}) if isinstance(operator_status, dict) else {}
        if manifest_operator_vram.get("model_driver_count") != operator_vram.get("model_driver_count"):
            errors.append("manifest operator-status VRAM model driver count mismatch")
        if manifest_operator_vram.get("top_driver_labels") != operator_vram.get("top_driver_labels"):
            errors.append("manifest operator-status VRAM top driver labels mismatch")
        if manifest_operator_vram.get("display_top_driver_labels") != operator_vram.get("display_top_driver_labels"):
            errors.append("manifest operator-status VRAM display top driver labels mismatch")
        if manifest_operator_vram.get("display_claim_safe_driver_labels") != operator_vram.get("display_claim_safe_driver_labels"):
            errors.append("manifest operator-status VRAM display claim-safe driver labels mismatch")
        if manifest_operator_vram.get("real_model_driver_count") != operator_vram.get("real_model_driver_count"):
            errors.append("manifest operator-status VRAM real model driver count mismatch")
        if manifest_operator_vram.get("real_top_driver_labels") != operator_vram.get("real_top_driver_labels"):
            errors.append("manifest operator-status VRAM real top driver labels mismatch")
        if manifest_operator_vram.get("display_real_top_driver_labels") != operator_vram.get("display_real_top_driver_labels"):
            errors.append("manifest operator-status VRAM display real top driver labels mismatch")
        if manifest_operator_vram.get("synthetic_driver_count") != operator_vram.get("synthetic_driver_count"):
            errors.append("manifest operator-status VRAM synthetic driver count mismatch")
        if manifest_operator_vram.get("synthetic_driver_labels") != operator_vram.get("synthetic_driver_labels"):
            errors.append("manifest operator-status VRAM synthetic driver labels mismatch")
        if manifest_operator_vram.get("display_synthetic_driver_labels") != operator_vram.get("display_synthetic_driver_labels"):
            errors.append("manifest operator-status VRAM display synthetic driver labels mismatch")
        if manifest_operator_vram.get("synthetic_reserve_driver") != operator_vram.get("synthetic_reserve_driver"):
            errors.append("manifest operator-status VRAM synthetic headroom probe driver mismatch")
        if (
            "synthetic_headroom_driver" in manifest_operator_vram
            or "synthetic_headroom_driver" in operator_vram
        ) and manifest_operator_vram.get("synthetic_headroom_driver") != operator_vram.get("synthetic_headroom_driver"):
            errors.append("manifest operator-status VRAM synthetic headroom driver alias mismatch")
        if manifest_operator_vram.get("reserve_pressure_definition") != operator_vram.get("reserve_pressure_definition"):
            errors.append("manifest operator-status VRAM synthetic headroom definition mismatch")
        if manifest_operator_vram.get("driver_claim_boundary") != operator_vram.get("driver_claim_boundary"):
            errors.append("manifest operator-status VRAM driver claim boundary mismatch")
        for manifest_field, operator_field in [
            ("vram_investment_demo_rows", "investment_demo_rows"),
            ("vram_investment_oom_risk_reduction_pods", "investment_oom_risk_reduction_pods"),
            ("vram_investment_high_vram_nodes_preserved", "investment_high_vram_nodes_preserved"),
            ("vram_investment_advisory_rows", "investment_advisory_rows"),
            ("vram_investment_average_baseline_oom_risk_percent", "investment_average_baseline_oom_risk_percent"),
            ("vram_investment_average_ksolver_oom_risk_percent", "investment_average_ksolver_oom_risk_percent"),
        ]:
            if manifest_operator_vram.get(operator_field) != operator_vram.get(operator_field):
                errors.append(f"manifest operator-status VRAM {operator_field} mismatch")
            if operator_vram.get(operator_field) != summary.get(manifest_field):
                errors.append(f"/api/scheduler/operator-status: VRAM {operator_field} mismatch")
        if operator_vram.get("model_driver_count") != summary.get("vram_model_driver_count"):
            errors.append("/api/scheduler/operator-status: VRAM model driver count mismatch")
        if operator_vram.get("top_driver_labels") != summary.get("vram_top_driver_labels"):
            errors.append("/api/scheduler/operator-status: VRAM top driver labels mismatch")
        if operator_vram.get("display_top_driver_labels") != summary.get("vram_display_top_driver_labels"):
            errors.append("/api/scheduler/operator-status: VRAM display top driver labels mismatch")
        if operator_vram.get("display_claim_safe_driver_labels") != summary.get("vram_display_claim_safe_driver_labels"):
            errors.append("/api/scheduler/operator-status: VRAM display claim-safe driver labels mismatch")
        if operator_vram.get("real_model_driver_count") != summary.get("vram_real_model_driver_count"):
            errors.append("/api/scheduler/operator-status: VRAM real model driver count mismatch")
        if operator_vram.get("real_top_driver_labels") != summary.get("vram_real_top_driver_labels"):
            errors.append("/api/scheduler/operator-status: VRAM real top driver labels mismatch")
        if operator_vram.get("display_real_top_driver_labels") != summary.get("vram_display_real_top_driver_labels"):
            errors.append("/api/scheduler/operator-status: VRAM display real top driver labels mismatch")
        if operator_vram.get("synthetic_driver_count") != summary.get("vram_synthetic_driver_count"):
            errors.append("/api/scheduler/operator-status: VRAM synthetic driver count mismatch")
        if operator_vram.get("synthetic_driver_labels") != summary.get("vram_synthetic_driver_labels"):
            errors.append("/api/scheduler/operator-status: VRAM synthetic driver labels mismatch")
        if operator_vram.get("display_synthetic_driver_labels") != summary.get("vram_display_synthetic_driver_labels"):
            errors.append("/api/scheduler/operator-status: VRAM display synthetic driver labels mismatch")
        if operator_vram.get("synthetic_reserve_driver") != summary.get("vram_synthetic_reserve_driver"):
            errors.append("/api/scheduler/operator-status: VRAM synthetic headroom probe driver mismatch")
        if (
            "synthetic_headroom_driver" in operator_vram
            or "vram_synthetic_headroom_driver" in summary
        ) and operator_vram.get("synthetic_headroom_driver") != summary.get("vram_synthetic_headroom_driver"):
            errors.append("/api/scheduler/operator-status: VRAM synthetic headroom driver alias mismatch")
        if operator_vram.get("reserve_pressure_definition") != summary.get("vram_reserve_pressure_definition"):
            errors.append("/api/scheduler/operator-status: VRAM synthetic headroom definition mismatch")
        if operator_vram.get("driver_claim_boundary") != summary.get("vram_driver_claim_boundary"):
            errors.append("/api/scheduler/operator-status: VRAM driver claim boundary mismatch")
        operator_action_items = captured_operator.get("action_items") or []
        if missing_action_items != operator_action_items:
            errors.append("/api/scheduler/operator-status: action items mismatch")
        captured_operator_runbook = captured_operator.get("operator_runbook") or {}
        if operator_runbook != captured_operator_runbook:
            errors.append("/api/scheduler/operator-status: operator runbook mismatch")
        validate_operator_runbook(
            errors,
            "/api/scheduler/operator-status",
            captured_operator_runbook,
            operator_action_items,
        )

    captured_vram = captured_payloads.get("/api/scheduler/vram-calibration") or {}
    captured_model_drivers = captured_vram.get("model_drivers") or {}
    if captured_model_drivers:
        captured_top_drivers = captured_model_drivers.get("top_drivers") or []
        captured_top_driver_labels = [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in captured_top_drivers
            if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
        ][:5]
        captured_top_driver_descriptions = [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in captured_top_drivers
            if isinstance(driver, dict) and (driver.get("description") or driver.get("label") or driver.get("feature"))
        ][:5]
        captured_real_top_drivers = captured_model_drivers.get("real_top_drivers") or [
            driver
            for driver in captured_top_drivers
            if isinstance(driver, dict) and driver.get("class") != "synthetic-pressure"
        ]
        captured_claim_safe_drivers = captured_model_drivers.get("claim_safe_drivers") or captured_real_top_drivers
        captured_claim_safe_driver_labels = [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in captured_claim_safe_drivers
            if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
        ][:5]
        captured_claim_safe_driver_descriptions = [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in captured_claim_safe_drivers
            if isinstance(driver, dict) and (driver.get("description") or driver.get("label") or driver.get("feature"))
        ][:5]
        captured_real_top_driver_labels = [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in captured_real_top_drivers
            if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
        ][:5]
        captured_real_top_driver_descriptions = [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in captured_real_top_drivers
            if isinstance(driver, dict) and (driver.get("description") or driver.get("label") or driver.get("feature"))
        ][:5]
        captured_synthetic_drivers = captured_model_drivers.get("synthetic_pressure_drivers") or [
            driver
            for driver in captured_top_drivers
            if isinstance(driver, dict) and driver.get("class") == "synthetic-pressure"
        ]
        captured_synthetic_driver_labels = [
            str(driver.get("label") or driver.get("feature") or "")
            for driver in captured_synthetic_drivers
            if isinstance(driver, dict) and (driver.get("label") or driver.get("feature"))
        ][:5]
        captured_synthetic_driver_descriptions = [
            display_vram_driver_label(str(driver.get("description") or driver.get("label") or driver.get("feature") or ""))
            for driver in captured_synthetic_drivers
            if isinstance(driver, dict) and (driver.get("description") or driver.get("label") or driver.get("feature"))
        ][:5]
        captured_display_top_driver_labels = display_vram_driver_labels(captured_top_driver_labels)
        captured_display_claim_safe_driver_labels = display_vram_driver_labels(captured_claim_safe_driver_labels)
        captured_display_real_top_driver_labels = display_vram_driver_labels(captured_real_top_driver_labels)
        captured_display_synthetic_driver_labels = display_vram_driver_labels(captured_synthetic_driver_labels)
        captured_synthetic_reserve = any(
            isinstance(driver, dict)
            and str(driver.get("feature") or "").startswith("reserve")
            and driver.get("class") == "synthetic-pressure"
            for driver in captured_synthetic_drivers
        )
        if vram_model_drivers.get("available") is not True:
            errors.append("manifest missing available VRAM model drivers")
        if vram_model_drivers.get("top_driver_count") != len(captured_top_drivers):
            errors.append("manifest VRAM model driver count mismatch")
        if vram_model_drivers.get("top_driver_labels") != captured_top_driver_labels:
            errors.append("manifest VRAM top driver labels mismatch")
        if vram_model_drivers.get("top_driver_descriptions") != captured_top_driver_descriptions:
            errors.append("manifest VRAM top driver descriptions mismatch")
        if vram_model_drivers.get("impact_basis") != captured_model_drivers.get("impact_basis"):
            errors.append("manifest VRAM driver impact basis mismatch")
        if vram_model_drivers.get("group_impacts") != (captured_model_drivers.get("group_impacts") or []):
            errors.append("manifest VRAM driver group impacts mismatch")
        if vram_model_drivers.get("top_organic_driver_descriptions") != (
            captured_model_drivers.get("top_organic_driver_descriptions") or []
        ):
            errors.append("manifest VRAM organic driver descriptions mismatch")
        if vram_model_drivers.get("display_top_driver_labels") != captured_display_top_driver_labels:
            errors.append("manifest VRAM display top driver labels mismatch")
        if vram_model_drivers.get("claim_safe_driver_count") != len(captured_claim_safe_drivers):
            errors.append("manifest VRAM claim-safe driver count mismatch")
        if vram_model_drivers.get("claim_safe_driver_labels") != captured_claim_safe_driver_labels:
            errors.append("manifest VRAM claim-safe driver labels mismatch")
        if vram_model_drivers.get("claim_safe_driver_descriptions") != captured_claim_safe_driver_descriptions:
            errors.append("manifest VRAM claim-safe driver descriptions mismatch")
        if vram_model_drivers.get("display_claim_safe_driver_labels") != captured_display_claim_safe_driver_labels:
            errors.append("manifest VRAM display claim-safe driver labels mismatch")
        if any(
            isinstance(driver, dict) and driver.get("class") == "synthetic-pressure"
            for driver in captured_claim_safe_drivers
        ):
            errors.append("/api/scheduler/vram-calibration: claim-safe drivers include synthetic pressure")
        if vram_model_drivers.get("real_top_driver_count") != len(captured_real_top_drivers):
            errors.append("manifest VRAM real model driver count mismatch")
        if vram_model_drivers.get("real_top_driver_labels") != captured_real_top_driver_labels:
            errors.append("manifest VRAM real top driver labels mismatch")
        if vram_model_drivers.get("real_top_driver_descriptions") != captured_real_top_driver_descriptions:
            errors.append("manifest VRAM real top driver descriptions mismatch")
        if vram_model_drivers.get("display_real_top_driver_labels") != captured_display_real_top_driver_labels:
            errors.append("manifest VRAM display real top driver labels mismatch")
        if vram_model_drivers.get("synthetic_pressure_driver_count") != len(captured_synthetic_drivers):
            errors.append("manifest VRAM synthetic pressure driver count mismatch")
        if vram_model_drivers.get("synthetic_pressure_driver_labels") != captured_synthetic_driver_labels:
            errors.append("manifest VRAM synthetic pressure driver labels mismatch")
        if vram_model_drivers.get("synthetic_pressure_driver_descriptions") != captured_synthetic_driver_descriptions:
            errors.append("manifest VRAM synthetic pressure driver descriptions mismatch")
        if vram_model_drivers.get("display_synthetic_pressure_driver_labels") != captured_display_synthetic_driver_labels:
            errors.append("manifest VRAM display synthetic pressure driver labels mismatch")
        if summary.get("vram_model_driver_count") != len(captured_top_drivers):
            errors.append("summary VRAM model driver count mismatch")
        if summary.get("vram_top_driver_labels") != captured_top_driver_labels:
            errors.append("summary VRAM top driver labels mismatch")
        if summary.get("vram_display_top_driver_labels") != captured_display_top_driver_labels:
            errors.append("summary VRAM display top driver labels mismatch")
        if summary.get("vram_claim_safe_driver_count") != len(captured_claim_safe_drivers):
            errors.append("summary VRAM claim-safe driver count mismatch")
        if summary.get("vram_claim_safe_driver_labels") != captured_claim_safe_driver_labels:
            errors.append("summary VRAM claim-safe driver labels mismatch")
        if summary.get("vram_display_claim_safe_driver_labels") != captured_display_claim_safe_driver_labels:
            errors.append("summary VRAM display claim-safe driver labels mismatch")
        if summary.get("vram_real_model_driver_count") != len(captured_real_top_drivers):
            errors.append("summary VRAM real model driver count mismatch")
        if summary.get("vram_real_top_driver_labels") != captured_real_top_driver_labels:
            errors.append("summary VRAM real top driver labels mismatch")
        if summary.get("vram_display_real_top_driver_labels") != captured_display_real_top_driver_labels:
            errors.append("summary VRAM display real top driver labels mismatch")
        if summary.get("vram_synthetic_driver_count") != len(captured_synthetic_drivers):
            errors.append("summary VRAM synthetic driver count mismatch")
        if summary.get("vram_synthetic_driver_labels") != captured_synthetic_driver_labels:
            errors.append("summary VRAM synthetic driver labels mismatch")
        if summary.get("vram_display_synthetic_driver_labels") != captured_display_synthetic_driver_labels:
            errors.append("summary VRAM display synthetic driver labels mismatch")
        if summary.get("vram_driver_claim_boundary") != captured_model_drivers.get("claim_boundary"):
            errors.append("summary VRAM driver claim boundary mismatch")
        if vram_model_drivers.get("synthetic_reserve_driver") != captured_synthetic_reserve:
            errors.append("manifest VRAM synthetic headroom probe driver mismatch")
        if (
            "synthetic_headroom_driver" in vram_model_drivers
            and vram_model_drivers.get("synthetic_headroom_driver") != captured_synthetic_reserve
        ):
            errors.append("manifest VRAM synthetic headroom driver alias mismatch")
        if summary.get("vram_synthetic_reserve_driver") != captured_synthetic_reserve:
            errors.append("summary VRAM synthetic headroom probe driver mismatch")
        if (
            "vram_synthetic_headroom_driver" in summary
            and summary.get("vram_synthetic_headroom_driver") != captured_synthetic_reserve
        ):
            errors.append("summary VRAM synthetic headroom driver alias mismatch")
        if captured_synthetic_reserve is not True:
            errors.append("/api/scheduler/vram-calibration: missing synthetic headroom probe driver")
    elif "/api/scheduler/vram-calibration" in captured_payloads:
        errors.append("/api/scheduler/vram-calibration: missing model_drivers")

    captured_demo_report = captured_payloads.get("/api/scheduler/demo-report") or {}
    investment = (
        (captured_demo_report.get("report") or {}).get("vram_investment_demo_summary")
        or {}
    )
    if investment:
        expected_rows = investment.get("scenario_count")
        if expected_rows is None:
            expected_rows = len(investment.get("rows") or [])
        for field, expected in [
            ("vram_investment_demo_rows", expected_rows),
            ("vram_investment_oom_risk_reduction_pods", investment.get("cuda_oom_risk_reduction_pods")),
            ("vram_investment_high_vram_nodes_preserved", investment.get("high_vram_nodes_preserved")),
            ("vram_investment_advisory_rows", investment.get("unknown_or_advisory_rows")),
            ("vram_investment_average_baseline_oom_risk_percent", investment.get("average_baseline_oom_risk_percent")),
            ("vram_investment_average_ksolver_oom_risk_percent", investment.get("average_ksolver_oom_risk_percent")),
        ]:
            if summary.get(field) != expected:
                errors.append(f"summary {field} mismatch captured demo-report")

    if not review_path.exists():
        errors.append("missing review.md")
    else:
        review_text = review_path.read_text(encoding="utf-8")
        if "ksolver SRE Evidence Bundle" not in review_text:
            errors.append("review.md missing evidence bundle title")
        for label, field in [
            ("VRAM admission mode", "vram_admission_mode"),
            ("VRAM scheduler use", "vram_scheduler_use"),
            ("VRAM next evidence", "vram_next_evidence_target"),
            ("Production blocker class", "production_readiness_blocker_class"),
            ("Production last error class", "production_readiness_last_error_class"),
            ("Simulator probe checked", "simulator_probe_checked_count"),
            ("Simulator probe ready", "simulator_probe_ready_count"),
            ("Simulator readiness", "simulator_readiness"),
            ("Simulator claim mode", "simulator_claim_mode"),
            ("Simulator claim ready", "simulator_claim_ready"),
            ("Simulator claim next action", "simulator_claim_next_action"),
        ]:
            value = summary.get(field)
            rendered = str(value).lower() if isinstance(value, bool) else value
            expected = f"{label}: `{rendered}`"
            if value and expected not in review_text:
                errors.append(f"review.md missing {label}")
        claim_blocker = summary.get("simulator_claim_blocker")
        if claim_blocker:
            expected = f"Simulator claim blocker: `{claim_blocker}`"
            if expected not in review_text:
                errors.append("review.md missing Simulator claim blocker")
        operator_review_expectations = [
            ("Operator status", operator_status.get("status")),
            ("Primary blocker", primary_claim_blocker),
            ("Next action", primary_claim_blocker_next_action),
        ]
        operator_binding = (
            (operator_status.get("binding_safety") or {})
            if isinstance(operator_status, dict)
            else {}
        )
        if operator_binding:
            operator_review_expectations.extend([
                ("Binding safety", operator_binding.get("status")),
                ("Binding mode", operator_binding.get("mode")),
                ("Binding reservation pressure", operator_binding.get("reservation_pressure")),
                (
                    "Binding reservation pressure meaning",
                    operator_binding.get("reservation_pressure_description"),
                ),
                (
                    "Binding reservation pressure scope",
                    operator_binding.get("reservation_pressure_scope"),
                ),
                (
                    "Binding reservation pressure reason",
                    operator_binding.get("reservation_pressure_reason"),
                ),
                (
                    "Binding reservation pressure action",
                    operator_binding.get("reservation_pressure_next_action"),
                ),
            ])
        operator_vram = (operator_status.get("vram") or {}) if isinstance(operator_status, dict) else {}
        operator_review_expectations.extend([
            ("Operator VRAM drivers", operator_vram.get("model_driver_count")),
            (
                "Operator VRAM all fitted top drivers",
                ", ".join(display_vram_driver_labels(operator_vram.get("top_driver_labels") or [])),
            ),
            ("Operator VRAM claim-safe drivers", operator_vram.get("claim_safe_driver_count")),
            (
                "Operator VRAM claim-safe top drivers",
                ", ".join(display_vram_driver_labels(operator_vram.get("claim_safe_driver_labels") or [])),
            ),
            ("Operator VRAM real drivers", operator_vram.get("real_model_driver_count")),
            (
                "Operator VRAM real top drivers",
                ", ".join(display_vram_driver_labels(operator_vram.get("real_top_driver_labels") or [])),
            ),
            ("Operator VRAM synthetic headroom drivers", operator_vram.get("synthetic_driver_count")),
            (
                "Operator VRAM synthetic headroom labels",
                ", ".join(display_vram_driver_labels(operator_vram.get("synthetic_driver_labels") or [])),
            ),
            ("Operator VRAM driver claim boundary", operator_vram.get("driver_claim_boundary")),
            (
                "Operator VRAM synthetic headroom probe driver",
                str(synthetic_headroom_driver_enabled(operator_vram)).lower(),
            ),
            ("Operator VRAM synthetic headroom", operator_vram.get("reserve_pressure_definition")),
        ])
        if operator_vram.get("investment_demo_rows") is not None:
            operator_review_expectations.append((
                "Operator VRAM investment demo",
                f"{operator_vram.get('investment_demo_rows')} rows, "
                f"{operator_vram.get('investment_oom_risk_reduction_pods')} OOM-risk pods reduced, "
                f"{operator_vram.get('investment_high_vram_nodes_preserved')} high-VRAM preserved",
            ))
        if manifest_demo_gate_result:
            demo_gate_section = markdown_section(review_text, "Demo Gate Result")
            if demo_gate_section is None:
                errors.append("review.md missing Demo Gate Result")
            else:
                for label, value in [
                    ("Stage", manifest_demo_gate_result.get("stage")),
                    ("Exit code", manifest_demo_gate_result.get("exit_code")),
                    ("Failed command", manifest_demo_gate_result.get("failed_command")),
                    ("Failed returncode", manifest_demo_gate_result.get("failed_returncode")),
                    ("Parse error", manifest_demo_gate_result.get("parse_error")),
                ]:
                    expected = f"{label}: `{value}`"
                    if value is not None and expected not in demo_gate_section:
                        errors.append(f"review.md demo gate section missing {label}")
        if manifest_doctor_preflight:
            doctor_section = markdown_section(review_text, "Doctor Preflight")
            if doctor_section is None:
                errors.append("review.md missing Doctor Preflight")
            else:
                for label, value in [
                    ("Status", manifest_doctor_preflight.get("status")),
                    ("Exit code", manifest_doctor_preflight.get("exit_code")),
                    (
                        "First recommended command",
                        manifest_doctor_preflight.get("first_recommended_command"),
                    ),
                    ("Failures", manifest_doctor_preflight.get("failure_count")),
                    (
                        "Recommended commands",
                        manifest_doctor_preflight.get("recommended_command_count"),
                    ),
                    (
                        "API endpoint failures",
                        manifest_doctor_preflight.get("api_endpoint_failure_count"),
                    ),
                    (
                        "First API endpoint failure",
                        (manifest_doctor_preflight.get("first_api_endpoint_failure") or {}).get("endpoint"),
                    ),
                    ("Parse error", manifest_doctor_preflight.get("parse_error")),
                ]:
                    expected = f"{label}: `{value}`"
                    if value is not None and expected not in doctor_section:
                        errors.append(f"review.md doctor preflight section missing {label}")
        debug_commands = operator_status.get("debug_commands") or []
        if debug_commands:
            operator_review_expectations.append(("First debug command", debug_commands[0]))
        production_debug_commands = (
            (operator_status.get("production_readiness") or {}).get("debug_commands") or []
        )
        if production_debug_commands:
            operator_review_expectations.append(
                ("Production first debug command", production_debug_commands[0])
            )
        if operator_runbook:
            operator_review_expectations.extend([
                ("Steps", operator_runbook.get("step_count")),
                ("Blocked steps", operator_runbook.get("blocked_step_count")),
                ("Copyable shell commands", operator_runbook.get("copyable_command_count")),
                ("Manual evidence steps", operator_runbook.get("manual_step_count")),
                ("Next shell command", operator_runbook.get("next_shell_command")),
            ])
        for label, value in operator_review_expectations:
            expected = f"{label}: `{value}`"
            if value and expected not in review_text:
                errors.append(f"review.md missing {label}")
        command_rows = operator_runbook_command_rows(operator_runbook)
        if command_rows:
            if "### Copyable Command Provenance" not in review_text:
                errors.append("review.md missing Copyable Command Provenance")
            for row in command_rows:
                expected = (
                    f"`{row.get('command')}` from `{row.get('category') or 'unknown'}` "
                    f"for `{row.get('next_action') or 'no action recorded'}`"
                )
                if expected not in review_text:
                    errors.append(
                        f"review.md missing copyable command provenance for {row.get('command')}"
                    )
                if row.get("severity"):
                    severity = f"severity `{row.get('severity')}`"
                    if severity not in review_text:
                        errors.append(
                            f"review.md missing copyable command severity for {row.get('command')}"
                        )
                if row.get("artifact"):
                    artifact = f"artifact `{row.get('artifact')}`"
                    if artifact not in review_text:
                        errors.append(
                            f"review.md missing copyable command artifact for {row.get('command')}"
                        )
        if vram_model_drivers:
            for label, value in [
                ("VRAM model drivers", vram_model_drivers.get("top_driver_count")),
                ("VRAM claim-safe drivers", vram_model_drivers.get("claim_safe_driver_count")),
                (
                    "VRAM claim-safe top drivers",
                    ", ".join(display_vram_driver_labels(vram_model_drivers.get("claim_safe_driver_labels") or [])),
                ),
                ("VRAM real model drivers", vram_model_drivers.get("real_top_driver_count")),
                (
                    "VRAM real top drivers",
                    ", ".join(display_vram_driver_labels(vram_model_drivers.get("real_top_driver_labels") or [])),
                ),
                ("VRAM synthetic headroom drivers", vram_model_drivers.get("synthetic_pressure_driver_count")),
                (
                    "VRAM synthetic headroom labels",
                    ", ".join(display_vram_driver_labels(vram_model_drivers.get("synthetic_pressure_driver_labels") or [])),
                ),
                ("VRAM driver claim boundary", vram_model_drivers.get("claim_boundary")),
                (
                    "VRAM synthetic headroom probe driver",
                    str(synthetic_headroom_driver_enabled(vram_model_drivers)).lower(),
                ),
                ("Top driver count", vram_model_drivers.get("top_driver_count")),
                ("Impact basis", vram_model_drivers.get("impact_basis")),
                (
                    "All fitted top driver meaning",
                    ", ".join((vram_model_drivers.get("top_driver_descriptions") or [])[:3]),
                ),
                (
                    "Organic driver descriptions",
                    ", ".join((vram_model_drivers.get("top_organic_driver_descriptions") or [])[:3]),
                ),
                ("Claim-safe driver count", vram_model_drivers.get("claim_safe_driver_count")),
                (
                    "Claim-safe drivers",
                    ", ".join(display_vram_driver_labels(vram_model_drivers.get("claim_safe_driver_labels") or [])),
                ),
                ("Real top driver count", vram_model_drivers.get("real_top_driver_count")),
                (
                    "Real top drivers",
                    ", ".join(display_vram_driver_labels(vram_model_drivers.get("real_top_driver_labels") or [])),
                ),
                ("Synthetic headroom driver count", vram_model_drivers.get("synthetic_pressure_driver_count")),
                (
                    "Synthetic headroom drivers",
                    ", ".join(display_vram_driver_labels(vram_model_drivers.get("synthetic_pressure_driver_labels") or [])),
                ),
                ("Claim boundary", vram_model_drivers.get("claim_boundary")),
                (
                    "Synthetic headroom probe driver",
                    str(synthetic_headroom_driver_enabled(vram_model_drivers)).lower(),
                ),
                (
                    "All fitted top drivers",
                    ", ".join(display_vram_driver_labels(vram_model_drivers.get("top_driver_labels") or [])),
                ),
            ]:
                expected = f"{label}: `{value}`"
                if value is not None and expected not in review_text:
                    errors.append(f"review.md missing {label}")
        live_gates = manifest.get("live_validation_gates") or []
        if live_gates:
            pass_count = sum(1 for gate in live_gates if gate.get("status") == "pass")
            warn_count = sum(1 for gate in live_gates if gate.get("status") == "warn")
            blocked_count = sum(1 for gate in live_gates if gate.get("status") == "blocked")
            expected = f"Gate summary: `{pass_count} pass, {warn_count} warn, {blocked_count} blocked`"
            if "## Live Proof Gates" not in review_text:
                errors.append("review.md missing Live Proof Gates")
            if expected not in review_text:
                errors.append("review.md missing Live Proof Gates summary")
            for gate in live_gates:
                line = f"`{gate.get('status', 'unknown')}` {gate.get('gate', 'unknown gate')}:"
                if line not in review_text:
                    errors.append(f"review.md missing live proof gate {gate.get('gate', 'unknown gate')}")
        if missing_rows:
            blocked_count = sum(1 for row in missing_rows if row.get("severity") == "blocked")
            warn_count = sum(1 for row in missing_rows if row.get("severity") == "warn")
            expected = f"Gap summary: `{blocked_count} blocked, {warn_count} warn`"
            if "## Missing Live Artifacts" not in review_text:
                errors.append("review.md missing Missing Live Artifacts")
            if expected not in review_text:
                errors.append("review.md missing Missing Live Artifacts summary")
            for row in missing_rows:
                artifact = row.get("artifact", "missing artifact")
                severity = row.get("severity", "missing")
                if f"`{severity}` {artifact}:" not in review_text:
                    errors.append(f"review.md missing live artifact row {artifact}")

    packet_complete = manifest.get("packet_complete") is True
    if packet_complete and errors:
        errors.append("manifest says packet_complete=true but verification found errors")
    missing_rows = manifest.get("missing_live_artifact_rows") or []
    missing_blocked_count = sum(
        1 for row in missing_rows if isinstance(row, dict) and row.get("severity") == "blocked"
    )
    missing_warn_count = sum(
        1 for row in missing_rows if isinstance(row, dict) and row.get("severity") == "warn"
    )
    missing_category_counts = manifest.get(
        "missing_live_artifact_category_counts",
        missing_artifact_category_counts(missing_rows),
    )
    missing_category_rows = manifest.get(
        "missing_live_artifact_category_rows",
        missing_artifact_category_rows(missing_rows),
    )
    missing_action_items = manifest.get(
        "missing_live_artifact_action_items",
        missing_artifact_action_items(missing_category_rows),
    )
    operator_runbook = manifest.get("operator_runbook") or operator_action_runbook(missing_action_items)

    return {
        "ok": not errors,
        "integrity_ok": not errors,
        "bundle_dir": str(bundle_dir),
        "packet_complete": packet_complete,
        "review_ready": manifest.get("review_ready") is True,
        "claim_blockers": manifest.get("claim_blockers") or [],
        "primary_claim_blocker": manifest.get("primary_claim_blocker"),
        "primary_claim_blocker_next_action": manifest.get("primary_claim_blocker_next_action"),
        "missing_live_artifact_count": manifest.get("missing_live_artifact_count", len(missing_rows)),
        "missing_live_artifact_blocked_count": manifest.get(
            "missing_live_artifact_blocked_count", missing_blocked_count
        ),
        "missing_live_artifact_warn_count": manifest.get(
            "missing_live_artifact_warn_count", missing_warn_count
        ),
        "missing_live_artifact_category_counts": missing_category_counts,
        "missing_live_artifact_category_rows": missing_category_rows,
        "missing_live_artifact_action_items": missing_action_items,
        "operator_runbook": operator_runbook,
        "missing_live_artifact_rows": missing_rows,
        "operator_status": operator_status,
        "vram_model_drivers": vram_model_drivers,
        "vram_model_driver_count": summary.get("vram_model_driver_count"),
        "vram_driver_impact_basis": summary.get("vram_driver_impact_basis"),
        "vram_top_driver_descriptions": summary.get("vram_top_driver_descriptions") or [],
        "vram_claim_safe_driver_descriptions": summary.get("vram_claim_safe_driver_descriptions") or [],
        "vram_real_top_driver_descriptions": summary.get("vram_real_top_driver_descriptions") or [],
        "vram_synthetic_driver_descriptions": summary.get("vram_synthetic_driver_descriptions") or [],
        "vram_top_organic_driver_descriptions": summary.get("vram_top_organic_driver_descriptions") or [],
        "vram_top_driver_group_impacts": summary.get("vram_top_driver_group_impacts") or [],
        "vram_top_driver_labels": summary.get("vram_top_driver_labels") or [],
        "vram_display_top_driver_labels": summary.get("vram_display_top_driver_labels") or [],
        "vram_claim_safe_driver_count": summary.get("vram_claim_safe_driver_count"),
        "vram_claim_safe_driver_labels": summary.get("vram_claim_safe_driver_labels") or [],
        "vram_display_claim_safe_driver_labels": summary.get("vram_display_claim_safe_driver_labels") or [],
        "vram_real_model_driver_count": summary.get("vram_real_model_driver_count"),
        "vram_real_top_driver_labels": summary.get("vram_real_top_driver_labels") or [],
        "vram_display_real_top_driver_labels": summary.get("vram_display_real_top_driver_labels") or [],
        "vram_synthetic_driver_count": summary.get("vram_synthetic_driver_count"),
        "vram_synthetic_driver_labels": summary.get("vram_synthetic_driver_labels") or [],
        "vram_display_synthetic_driver_labels": summary.get("vram_display_synthetic_driver_labels") or [],
        "vram_driver_claim_boundary": summary.get("vram_driver_claim_boundary"),
        "vram_synthetic_reserve_driver": summary.get("vram_synthetic_reserve_driver"),
        "vram_synthetic_headroom_driver": summary.get(
            "vram_synthetic_headroom_driver",
            summary.get("vram_synthetic_reserve_driver"),
        ),
        "vram_admission_mode": summary.get("vram_admission_mode"),
        "vram_next_evidence_target": summary.get("vram_next_evidence_target"),
        "vram_investment_demo_rows": summary.get("vram_investment_demo_rows"),
        "vram_investment_oom_risk_reduction_pods": summary.get("vram_investment_oom_risk_reduction_pods"),
        "vram_investment_high_vram_nodes_preserved": summary.get("vram_investment_high_vram_nodes_preserved"),
        "vram_investment_advisory_rows": summary.get("vram_investment_advisory_rows"),
        "vram_investment_average_baseline_oom_risk_percent": summary.get("vram_investment_average_baseline_oom_risk_percent"),
        "vram_investment_average_ksolver_oom_risk_percent": summary.get("vram_investment_average_ksolver_oom_risk_percent"),
        "production_readiness_blocker_class": summary.get("production_readiness_blocker_class"),
        "production_readiness_last_error_class": summary.get("production_readiness_last_error_class"),
        "simulator_endpoint_count": summary.get("simulator_endpoint_count"),
        "simulator_probe_checked_count": summary.get("simulator_probe_checked_count"),
        "simulator_probe_ready_count": summary.get("simulator_probe_ready_count"),
        "simulator_probe_timeout_millis": summary.get("simulator_probe_timeout_millis"),
        "simulator_readiness": summary.get("simulator_readiness"),
        "simulator_claim_ready": summary.get("simulator_claim_ready"),
        "simulator_claim_mode": summary.get("simulator_claim_mode"),
        "simulator_claim_blocker": summary.get("simulator_claim_blocker"),
        "simulator_claim_next_action": summary.get("simulator_claim_next_action"),
        "doctor_preflight_present": doctor_preflight is not None,
        "doctor_status": (doctor_preflight or {}).get("status"),
        "doctor_first_recommended_command": (doctor_preflight or {}).get("first_recommended_command"),
        "doctor_failures": (doctor_preflight or {}).get("failures") or [],
        "doctor_api_endpoint_failures": (doctor_preflight or {}).get("api_endpoint_failures") or [],
        "demo_gate_result_present": demo_gate_result is not None,
        "demo_gate_stage": (demo_gate_result or {}).get("stage"),
        "demo_gate_exit_code": (demo_gate_result or {}).get("exit_code"),
        "demo_gate_failed_command": (demo_gate_result or {}).get("failed_command"),
        "demo_gate_failed_returncode": (demo_gate_result or {}).get("failed_returncode"),
        "demo_gate_parse_error": (demo_gate_result or {}).get("parse_error"),
        "verified_files": verified_files,
        "errors": errors,
    }


def exit_code_for_result(result: dict[str, Any], *, require_review_ready: bool) -> int:
    if result.get("integrity_ok") is not True:
        return 1
    if require_review_ready and result.get("review_ready") is not True:
        return 2
    return 0


def printable_summary(result: dict[str, Any]) -> str:
    lines = [
        "evidence bundle verified: "
        f"{result.get('verified_files', 0)} endpoint files, "
        f"review {'ready' if result.get('review_ready') else 'blocked'}"
    ]
    blockers = result.get("claim_blockers") or []
    if result.get("primary_claim_blocker"):
        lines.append(f"primary blocker: {result.get('primary_claim_blocker')}")
    if result.get("primary_claim_blocker_next_action"):
        lines.append(f"next action: {result.get('primary_claim_blocker_next_action')}")
    if result.get("missing_live_artifact_count"):
        lines.append(
            "evidence gaps: "
            f"{result.get('missing_live_artifact_blocked_count', 0)} blocked, "
            f"{result.get('missing_live_artifact_warn_count', 0)} warn"
        )
        category_summary = category_counts_text(
            result.get("missing_live_artifact_category_counts") or {}
        )
        if category_summary:
            lines.append(f"gap categories: {category_summary}")
        category_rows = result.get("missing_live_artifact_category_rows") or []
        if category_rows:
            first = category_rows[0]
            lines.append(
                "first gap category action: "
                f"{first.get('category')}: {first.get('next_action')}"
            )
        action_items = result.get("missing_live_artifact_action_items") or []
        if action_items:
            first_action = action_items[0]
            if first_action.get("command_hint"):
                lines.append(f"first action command: {first_action.get('command_hint')}")
        runbook = result.get("operator_runbook") or {}
        if runbook:
            lines.append(
                "operator runbook: "
                f"{runbook.get('step_count', 0)} steps, "
                f"{runbook.get('copyable_command_count', 0)} shell, "
                f"{runbook.get('manual_step_count', 0)} manual"
            )
            if runbook.get("next_shell_command"):
                lines.append(f"next shell command: {runbook.get('next_shell_command')}")
            command_rows = operator_runbook_command_rows(runbook)
            if command_rows:
                first_command = command_rows[0]
                lines.append(
                    "first shell command reason: "
                    f"{first_command.get('category')}: {first_command.get('next_action')}"
                )
            elif runbook.get("next_shell_command"):
                next_action = result.get("primary_claim_blocker_next_action") or "operator action"
                category = "environment" if "kubernetes" in str(next_action).lower() or "api" in str(next_action).lower() else "operator"
                lines.append(f"first shell command reason: {category}: {next_action}")
    if result.get("vram_admission_mode"):
        lines.append(f"VRAM mode: {result.get('vram_admission_mode')}")
    if result.get("vram_claim_safe_driver_count") is not None:
        claim_safe_labels = (
            result.get("vram_display_claim_safe_driver_labels")
            or result.get("vram_claim_safe_driver_labels")
            or []
        )
        label_text = ", ".join(str(label) for label in claim_safe_labels[:3] if label)
        suffix = f" ({label_text})" if label_text else ""
        lines.append(f"VRAM claim-safe drivers: {result.get('vram_claim_safe_driver_count')}{suffix}")
    elif result.get("vram_real_model_driver_count") is not None:
        real_labels = (
            result.get("vram_display_real_top_driver_labels")
            or result.get("vram_real_top_driver_labels")
            or []
        )
        label_text = ", ".join(str(label) for label in real_labels[:3] if label)
        suffix = f" ({label_text})" if label_text else ""
        lines.append(f"VRAM real drivers: {result.get('vram_real_model_driver_count')}{suffix}")
    if result.get("vram_synthetic_driver_count") is not None:
        synthetic_labels = (
            result.get("vram_display_synthetic_driver_labels")
            or result.get("vram_synthetic_driver_labels")
            or []
        )
        label_text = ", ".join(display_vram_driver_label(label) for label in synthetic_labels[:2] if label)
        suffix = f" ({label_text})" if label_text else ""
        lines.append(
            f"VRAM synthetic headroom drivers: {result.get('vram_synthetic_driver_count')}{suffix}"
        )
    if result.get("vram_driver_claim_boundary"):
        lines.append(f"VRAM claim boundary: {result.get('vram_driver_claim_boundary')}")
    if result.get("vram_next_evidence_target"):
        lines.append(f"next VRAM evidence: {result.get('vram_next_evidence_target')}")
    if result.get("production_readiness_blocker_class"):
        lines.append(f"production blocker class: {result.get('production_readiness_blocker_class')}")
    if result.get("production_readiness_last_error_class"):
        lines.append(
            f"production last error class: {result.get('production_readiness_last_error_class')}"
        )
    if result.get("simulator_probe_checked_count") is not None:
        lines.append(
            "simulator probe: "
            f"{result.get('simulator_probe_ready_count')}/"
            f"{result.get('simulator_probe_checked_count')} ready"
        )
    if result.get("simulator_readiness"):
        lines.append(f"simulator readiness: {result.get('simulator_readiness')}")
    if result.get("doctor_preflight_present"):
        lines.append(f"doctor preflight: {result.get('doctor_status') or 'unknown'}")
        if result.get("doctor_first_recommended_command"):
            lines.append(
                "doctor first command: "
                f"{result.get('doctor_first_recommended_command')}"
            )
        api_failures = result.get("doctor_api_endpoint_failures") or []
        if api_failures:
            lines.append(f"doctor API failures: {len(api_failures)}")
    if result.get("demo_gate_result_present"):
        lines.append(f"demo gate result: {result.get('demo_gate_stage') or 'unknown'}")
        if result.get("demo_gate_failed_command"):
            lines.append(f"demo gate failed command: {result.get('demo_gate_failed_command')}")
        if result.get("demo_gate_failed_returncode") is not None:
            lines.append(f"demo gate failed returncode: {result.get('demo_gate_failed_returncode')}")
        if result.get("demo_gate_parse_error"):
            lines.append(f"demo gate parse error: {result.get('demo_gate_parse_error')}")
    if blockers:
        lines.append("claim blockers:")
        lines.extend(f"- {blocker}" for blocker in blockers)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify a collected ksolver evidence bundle.")
    parser.add_argument("bundle_dir", help="directory containing manifest.json and captured endpoint files")
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    parser.add_argument(
        "--require-review-ready",
        action="store_true",
        help="exit nonzero when integrity passes but the packet is still claim-blocked",
    )
    args = parser.parse_args()

    result = verify_bundle(pathlib.Path(args.bundle_dir))
    exit_code = exit_code_for_result(
        result, require_review_ready=args.require_review_ready
    )
    result["exit_code"] = exit_code
    result["require_review_ready"] = args.require_review_ready
    if args.json:
        print(json.dumps(result, sort_keys=True))
    elif result["ok"]:
        print(printable_summary(result))
    else:
        print("evidence bundle verification failed:", file=sys.stderr)
        for error in result["errors"]:
            print(f"- {error}", file=sys.stderr)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
