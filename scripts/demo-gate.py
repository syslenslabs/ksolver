#!/usr/bin/env python3
"""Run the ksolver shadow demo readiness gate end to end."""

from __future__ import annotations

import argparse
import json
import pathlib
import shlex
import subprocess
import sys
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Callable

SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from evidence_helpers import (  # noqa: E402
    category_counts_text,
    display_vram_driver_labels,
    missing_artifact_action_items,
    missing_artifact_category_counts,
    missing_artifact_category_rows,
    operator_action_runbook,
    operator_runbook_command_rows,
    synthetic_headroom_driver_value,
)


CommandRunner = Callable[[list[str]], subprocess.CompletedProcess[str]]
FAILED_STREAM_EXCERPT_CHARS = 2000
DEMO_GATE_RESULT_FILENAME = "demo-gate-result.json"


def timestamp_slug(now: datetime | None = None) -> str:
    now = now or datetime.now(timezone.utc)
    return now.strftime("%Y%m%dT%H%M%SZ")


def run_command(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, check=False, text=True, capture_output=True)


def parse_json_output(process: subprocess.CompletedProcess[str]) -> dict[str, Any]:
    text = (process.stdout or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        return {"raw_stdout": text, "parse_error": exc.msg}


def write_json_file(path: pathlib.Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def count_urls(csv: str) -> int:
    return len([url for url in csv.split(",") if url.strip()])


def first_present(*values: Any) -> Any:
    for value in values:
        if value is not None:
            return value
    return None


def count_label(value: Any) -> str:
    return "unknown" if value is None else str(value)


def command_display(args: Any) -> str | None:
    if isinstance(args, str):
        return args
    if isinstance(args, (list, tuple)):
        return shlex.join(str(part) for part in args)
    return None


def stream_excerpt(value: str | None, limit: int = FAILED_STREAM_EXCERPT_CHARS) -> str | None:
    text = (value or "").strip()
    if not text:
        return None
    if len(text) <= limit:
        return text
    omitted = len(text) - limit
    return f"{text[:limit]}... [truncated {omitted} chars]"


def demo_gate_manifest_summary(result: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "present": True,
        "ok": result.get("ok"),
        "stage": result.get("stage"),
        "exit_code": result.get("exit_code"),
        "failed_command": result.get("failed_command"),
        "failed_returncode": result.get("failed_returncode"),
        "parse_error": result.get("parse_error"),
    }
    return {key: value for key, value in summary.items() if value is not None}


def demo_gate_review_section(summary: dict[str, Any]) -> str:
    lines = [
        "",
        "## Demo Gate Result",
        "",
        f"- Stage: `{summary.get('stage') or 'unknown'}`",
        f"- Exit code: `{summary.get('exit_code', 'unknown')}`",
    ]
    if summary.get("failed_command"):
        lines.append(f"- Failed command: `{summary.get('failed_command')}`")
    if summary.get("failed_returncode") is not None:
        lines.append(f"- Failed returncode: `{summary.get('failed_returncode')}`")
    if summary.get("parse_error"):
        lines.append(f"- Parse error: `{summary.get('parse_error')}`")
    return "\n".join(lines) + "\n"


def upsert_demo_gate_review_section(review: str, summary: dict[str, Any]) -> str:
    section = demo_gate_review_section(summary)
    marker = "\n## Demo Gate Result\n"
    start = review.find(marker)
    prefix_adjust = 1
    if start < 0 and review.startswith("## Demo Gate Result\n"):
        start = 0
        prefix_adjust = 0
    if start < 0:
        return review.rstrip() + "\n" + section

    section_start = start + prefix_adjust
    next_heading = review.find("\n## ", section_start + len("## Demo Gate Result\n"))
    if next_heading < 0:
        return review[:section_start].rstrip() + section
    return review[:section_start].rstrip() + section + review[next_heading:]


def persist_demo_gate_result(output_dir: pathlib.Path, result: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    write_json_file(output_dir / DEMO_GATE_RESULT_FILENAME, result)
    summary = demo_gate_manifest_summary(result)
    manifest_path = output_dir / "manifest.json"
    if manifest_path.exists():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            manifest = None
        if isinstance(manifest, dict):
            manifest["demo_gate_result"] = summary
            write_json_file(manifest_path, manifest)
    review_path = output_dir / "review.md"
    if review_path.exists():
        review = review_path.read_text(encoding="utf-8")
        review_path.write_text(upsert_demo_gate_review_section(review, summary), encoding="utf-8")


def non_negative_int(value: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{value!r} is not an integer") from exc
    if parsed < 0:
        raise argparse.ArgumentTypeError("value must be greater than or equal to 0")
    return parsed


def positive_int(value: str) -> int:
    parsed = non_negative_int(value)
    if parsed == 0:
        raise argparse.ArgumentTypeError("value must be greater than 0")
    return parsed


def vram_context_from_probe(probe: dict[str, Any]) -> dict[str, Any]:
    evidence = probe.get("evidence_summary") or {}
    operator = probe.get("operator_status") or {}
    operator_vram = operator.get("vram") or {}
    return {
        "vram_model_driver_count": first_present(
            evidence.get("vram_model_driver_count"),
            operator_vram.get("model_driver_count"),
        ),
        "vram_driver_impact_basis": evidence.get("vram_driver_impact_basis"),
        "vram_top_driver_descriptions": evidence.get("vram_top_driver_descriptions") or [],
        "vram_claim_safe_driver_descriptions": evidence.get("vram_claim_safe_driver_descriptions") or [],
        "vram_real_top_driver_descriptions": evidence.get("vram_real_top_driver_descriptions") or [],
        "vram_synthetic_driver_descriptions": evidence.get("vram_synthetic_driver_descriptions") or [],
        "vram_top_organic_driver_descriptions": evidence.get("vram_top_organic_driver_descriptions") or [],
        "vram_top_driver_group_impacts": evidence.get("vram_top_driver_group_impacts") or [],
        "vram_top_driver_labels": first_present(
            evidence.get("vram_top_driver_labels"),
            operator_vram.get("top_driver_labels"),
        ) or [],
        "vram_display_top_driver_labels": first_present(
            evidence.get("vram_display_top_driver_labels"),
            operator_vram.get("display_top_driver_labels"),
        ) or display_vram_driver_labels(
            first_present(
                evidence.get("vram_top_driver_labels"),
                operator_vram.get("top_driver_labels"),
            )
            or []
        ),
        "vram_claim_safe_driver_count": first_present(
            evidence.get("vram_claim_safe_driver_count"),
            operator_vram.get("claim_safe_driver_count"),
        ),
        "vram_claim_safe_driver_labels": first_present(
            evidence.get("vram_claim_safe_driver_labels"),
            operator_vram.get("claim_safe_driver_labels"),
        ) or [],
        "vram_display_claim_safe_driver_labels": first_present(
            evidence.get("vram_display_claim_safe_driver_labels"),
            operator_vram.get("display_claim_safe_driver_labels"),
        ) or display_vram_driver_labels(
            first_present(
                evidence.get("vram_claim_safe_driver_labels"),
                operator_vram.get("claim_safe_driver_labels"),
            )
            or []
        ),
        "vram_synthetic_reserve_driver": first_present(
            evidence.get("vram_synthetic_reserve_driver"),
            operator_vram.get("synthetic_reserve_driver"),
        ),
        "vram_synthetic_headroom_driver": first_present(
            synthetic_headroom_driver_value(evidence),
            synthetic_headroom_driver_value(operator_vram),
        ),
    }


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
        body = str(readyz.get("body") or readyz.get("error") or "").lower()
        if "watch" in body:
            return "kubernetes_watch"
        if "solver" in body:
            return "solver"
        return "readyz"
    evidence = probe.get("evidence_summary") or {}
    evidence_class = evidence.get("production_readiness_blocker_class")
    if evidence_class and evidence_class != "none":
        return str(evidence_class)
    operator = probe.get("operator_status") or {}
    operator_blocker = str(operator.get("primary_blocker") or "").lower()
    if operator_blocker:
        if "watch" in operator_blocker or "kubernetes" in operator_blocker:
            return "kubernetes_watch"
        if "solver" in operator_blocker:
            return "solver"
        return str(operator.get("status") or "operator_status")
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
        with urllib.request.urlopen(url, timeout=5) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return {
                "ok": 200 <= resp.status < 300,
                "status": resp.status,
                "body": body.strip(),
            }
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        return {
            "ok": False,
            "status": exc.code,
            "body": body.strip(),
        }
    except Exception as exc:  # noqa: BLE001 - diagnostic probe should not mask gate failure.
        return {
            "ok": False,
            "status": None,
            "error": str(exc),
        }


def readiness_probe(base_url: str) -> dict[str, Any]:
    base = base_url.rstrip("/")
    probe = {
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
    summary: dict[str, Any] = {}
    if bundle_probe.get("body"):
        try:
            payload = json.loads(str(bundle_probe.get("body") or "{}"))
            summary = payload.get("summary") or {}
            bundle_probe = {
                "ok": bundle_probe.get("ok"),
                "status": bundle_probe.get("status"),
            }
        except json.JSONDecodeError:
            pass
    probe["evidence_bundle"] = bundle_probe
    if summary:
        probe["evidence_summary"] = {
            "review_ready": summary.get("review_ready"),
            "demo_gate_status": summary.get("demo_gate_status"),
            "demo_gate_strict_exit_code": summary.get("demo_gate_strict_exit_code"),
            "primary_claim_blocker": summary.get("primary_claim_blocker"),
            "primary_claim_blocker_next_action": summary.get(
                "primary_claim_blocker_next_action"
            ),
            "claim_blockers": summary.get("claim_blockers") or [],
            "vram_admission_mode": summary.get("vram_admission_mode"),
            "vram_scheduler_use": summary.get("vram_scheduler_use"),
            "vram_hard_blocker_count": summary.get("vram_hard_blocker_count"),
            "vram_next_evidence_target": summary.get("vram_next_evidence_target"),
            "vram_model_driver_count": summary.get("vram_model_driver_count"),
            "vram_driver_impact_basis": summary.get("vram_driver_impact_basis"),
            "vram_top_driver_descriptions": summary.get("vram_top_driver_descriptions") or [],
            "vram_claim_safe_driver_descriptions": summary.get("vram_claim_safe_driver_descriptions") or [],
            "vram_real_top_driver_descriptions": summary.get("vram_real_top_driver_descriptions") or [],
            "vram_synthetic_driver_descriptions": summary.get("vram_synthetic_driver_descriptions") or [],
            "vram_top_organic_driver_descriptions": summary.get("vram_top_organic_driver_descriptions") or [],
            "vram_top_driver_group_impacts": summary.get("vram_top_driver_group_impacts") or [],
            "vram_top_driver_labels": summary.get("vram_top_driver_labels") or [],
            "vram_claim_safe_driver_count": summary.get("vram_claim_safe_driver_count"),
            "vram_claim_safe_driver_labels": summary.get("vram_claim_safe_driver_labels") or [],
            "vram_synthetic_reserve_driver": summary.get("vram_synthetic_reserve_driver"),
            "vram_synthetic_headroom_driver": first_present(
                synthetic_headroom_driver_value(summary),
            ),
            "production_readiness_blocker_class": summary.get(
                "production_readiness_blocker_class"
            ),
            "production_readiness_last_error_class": summary.get(
                "production_readiness_last_error_class"
            ),
            "simulator_endpoint_count": summary.get("simulator_endpoint_count"),
            "simulator_probe_checked_count": summary.get("simulator_probe_checked_count"),
            "simulator_probe_ready_count": summary.get("simulator_probe_ready_count"),
            "simulator_probe_timeout_millis": summary.get("simulator_probe_timeout_millis"),
            "simulator_readiness": summary.get("simulator_readiness"),
            "simulator_readiness_note": summary.get("simulator_readiness_note"),
            "simulator_claim_ready": summary.get("simulator_claim_ready"),
            "simulator_claim_mode": summary.get("simulator_claim_mode"),
            "simulator_claim_blocker": summary.get("simulator_claim_blocker"),
            "simulator_claim_next_action": summary.get("simulator_claim_next_action"),
        }
    operator_probe = fetch_probe(f"{base}/api/scheduler/operator-status")
    if operator_probe.get("body"):
        try:
            payload = json.loads(str(operator_probe.get("body") or "{}"))
            probe["operator_status"] = {
                "ok": payload.get("ok"),
                "status": payload.get("status"),
                "status_label": payload.get("status_label"),
                "can_shadow_demo": payload.get("can_shadow_demo"),
                "can_customer_claim": payload.get("can_customer_claim"),
                "primary_blocker": payload.get("primary_blocker"),
                "next_action": payload.get("next_action"),
                "debug_commands": payload.get("debug_commands") or [],
                "production_readiness": payload.get("production_readiness") or {},
                "simulator": payload.get("simulator") or {},
                "proof_gates": payload.get("proof_gates") or {},
                "vram": payload.get("vram") or {},
                "demo_gate": payload.get("demo_gate") or {},
            }
            operator_probe = {
                "ok": operator_probe.get("ok"),
                "status": operator_probe.get("status"),
            }
        except json.JSONDecodeError:
            pass
    probe["operator_status_probe"] = operator_probe
    return probe


def stage_failure(
    *,
    stage: str,
    base_url: str,
    output_dir: pathlib.Path,
    process: subprocess.CompletedProcess[str],
    readiness: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = parse_json_output(process)
    result = {
        "ok": False,
        "stage": stage,
        "exit_code": 2 if process.returncode == 2 else 1,
        "base_url": base_url,
        "output_dir": str(output_dir),
        "failed_command": command_display(process.args),
        "failed_returncode": process.returncode,
        "failed_stdout_excerpt": stream_excerpt(process.stdout),
        "failed_stderr_excerpt": stream_excerpt(process.stderr),
        "parse_error": payload.get("parse_error"),
        "error": payload.get("error") or (process.stderr or "").strip() or f"{stage} failed",
        stage: payload,
    }
    if readiness is not None:
        result["readiness_probe"] = readiness
        result["readiness_blocker_class"] = classify_readiness_blocker(readiness)
        result.update(vram_context_from_probe(readiness))
    return result


def attach_doctor_context(result: dict[str, Any], doctor: dict[str, Any]) -> dict[str, Any]:
    api_endpoint_failures = doctor.get("api_endpoint_failures") or []
    result["doctor_status"] = doctor.get("status")
    result["doctor_failures"] = doctor.get("failures") or []
    result["doctor_first_recommended_command"] = doctor.get("first_recommended_command")
    result["doctor_recommended_commands"] = doctor.get("recommended_commands") or []
    result["doctor_api_endpoint_failure_count"] = (
        len(api_endpoint_failures) if isinstance(api_endpoint_failures, list) else 0
    )
    result["doctor_first_api_endpoint_failure"] = (
        api_endpoint_failures[0] if isinstance(api_endpoint_failures, list) and api_endpoint_failures else None
    )
    result["production_readiness_blocker_class"] = first_present(
        result.get("production_readiness_blocker_class"),
        doctor.get("production_readiness_blocker_class"),
    )
    result["production_readiness_first_debug_command"] = first_present(
        result.get("production_readiness_first_debug_command"),
        doctor.get("first_debug_command"),
    )
    result["simulator_claim_ready"] = first_present(
        result.get("simulator_claim_ready"),
        doctor.get("simulator_claim_ready"),
    )
    result["simulator_claim_mode"] = first_present(
        result.get("simulator_claim_mode"),
        doctor.get("simulator_claim_mode"),
    )
    result["simulator_claim_blocker"] = first_present(
        result.get("simulator_claim_blocker"),
        doctor.get("simulator_claim_blocker"),
    )
    result["kss_ready_count"] = first_present(result.get("kss_ready_count"), doctor.get("kss_ready_count"))
    return result


def build_result(
    *,
    base_url: str,
    output_dir: pathlib.Path,
    smoke: dict[str, Any],
    collection: dict[str, Any],
    verification: dict[str, Any],
    require_review_ready: bool,
    require_simulator_claim_ready: bool,
    verify_returncode: int,
) -> dict[str, Any]:
    review_ready = verification.get("review_ready") is True
    claim_blockers = list(verification.get("claim_blockers") or collection.get("claim_blockers") or [])
    collection_summary = collection.get("summary") or {}
    verified_vram_drivers = verification.get("vram_model_drivers") or {}
    collection_missing_rows = collection.get("missing_live_artifact_rows") or []
    verification_missing_rows = verification.get("missing_live_artifact_rows") or []
    missing_rows = verification_missing_rows or collection_missing_rows
    missing_count = first_present(
        verification.get("missing_live_artifact_count"),
        collection.get("missing_live_artifact_count"),
        collection_summary.get("missing_live_artifact_count"),
        len(missing_rows),
    )
    missing_blocked_count = first_present(
        verification.get("missing_live_artifact_blocked_count"),
        collection.get("missing_live_artifact_blocked_count"),
        collection_summary.get("missing_live_artifact_blocked_count"),
    )
    if missing_blocked_count is None:
        missing_blocked_count = sum(
            1 for row in missing_rows if isinstance(row, dict) and row.get("severity") == "blocked"
        )
    missing_warn_count = first_present(
        verification.get("missing_live_artifact_warn_count"),
        collection.get("missing_live_artifact_warn_count"),
        collection_summary.get("missing_live_artifact_warn_count"),
    )
    if missing_warn_count is None:
        missing_warn_count = sum(
            1 for row in missing_rows if isinstance(row, dict) and row.get("severity") == "warn"
        )
    missing_category_counts = first_present(
        verification.get("missing_live_artifact_category_counts"),
        collection.get("missing_live_artifact_category_counts"),
        collection_summary.get("missing_live_artifact_category_counts"),
    )
    if missing_category_counts is None:
        missing_category_counts = missing_artifact_category_counts(missing_rows)
    missing_category_rows = first_present(
        verification.get("missing_live_artifact_category_rows"),
        collection.get("missing_live_artifact_category_rows"),
        collection_summary.get("missing_live_artifact_category_rows"),
    )
    if missing_category_rows is None:
        missing_category_rows = missing_artifact_category_rows(missing_rows)
    missing_action_items = first_present(
        verification.get("missing_live_artifact_action_items"),
        collection.get("missing_live_artifact_action_items"),
        collection_summary.get("missing_live_artifact_action_items"),
    )
    if missing_action_items is None:
        missing_action_items = missing_artifact_action_items(missing_category_rows)
    operator_runbook = first_present(
        verification.get("operator_runbook"),
        collection.get("operator_runbook"),
        collection_summary.get("operator_runbook"),
    )
    if operator_runbook is None:
        operator_runbook = operator_action_runbook(missing_action_items)
    operator_status = collection.get("operator_status") or smoke.get("operator_status") or {}
    operator_binding = operator_status.get("binding_safety") or {}
    production_readiness = operator_status.get("production_readiness") or {}
    production_debug_commands = (
        production_readiness.get("debug_commands")
        or operator_status.get("debug_commands")
        or collection_summary.get("production_readiness_debug_commands")
        or []
    )
    simulator_claim_ready = first_present(
        verification.get("simulator_claim_ready"),
        collection_summary.get("simulator_claim_ready"),
    )
    simulator_claim_mode = first_present(
        verification.get("simulator_claim_mode"),
        collection_summary.get("simulator_claim_mode"),
    )
    simulator_claim_blocker = first_present(
        verification.get("simulator_claim_blocker"),
        collection_summary.get("simulator_claim_blocker"),
    )
    simulator_claim_next_action = first_present(
        verification.get("simulator_claim_next_action"),
        collection_summary.get("simulator_claim_next_action"),
    )
    stage = "ready"
    exit_code = 0
    if verify_returncode == 2 or (require_review_ready and not review_ready):
        stage = "review-blocked"
        exit_code = 2
    elif require_simulator_claim_ready and simulator_claim_ready is not True:
        stage = "simulator-claim-blocked"
        exit_code = 2
    elif verify_returncode != 0:
        stage = "verify"
        exit_code = 1
    return {
        "ok": exit_code == 0,
        "stage": stage,
        "exit_code": exit_code,
        "base_url": base_url,
        "output_dir": str(output_dir),
        "require_review_ready": require_review_ready,
        "require_simulator_claim_ready": require_simulator_claim_ready,
        "review_ready": review_ready,
        "readiness_mode": smoke.get("readiness_mode"),
        "readiness_blocker_class": first_present(
            smoke.get("readiness_blocker_class"),
            verification.get("readiness_blocker_class"),
            collection_summary.get("readiness_blocker_class"),
        ),
        "claim_blockers": claim_blockers,
        "missing_live_artifact_count": missing_count,
        "missing_live_artifact_blocked_count": missing_blocked_count,
        "missing_live_artifact_warn_count": missing_warn_count,
        "missing_live_artifact_category_counts": missing_category_counts,
        "missing_live_artifact_category_rows": missing_category_rows,
        "missing_live_artifact_action_items": missing_action_items,
        "operator_runbook": operator_runbook,
        "operator_binding_status": first_present(
            collection.get("operator_binding_status"),
            operator_binding.get("status"),
        ),
        "operator_reservation_pressure": first_present(
            collection.get("operator_reservation_pressure"),
            smoke.get("operator_reservation_pressure"),
            operator_binding.get("reservation_pressure"),
        ),
        "operator_reservation_pressure_description": first_present(
            collection.get("operator_reservation_pressure_description"),
            smoke.get("operator_reservation_pressure_description"),
            operator_binding.get("reservation_pressure_description"),
        ),
        "operator_reservation_pressure_scope": first_present(
            collection.get("operator_reservation_pressure_scope"),
            smoke.get("operator_reservation_pressure_scope"),
            operator_binding.get("reservation_pressure_scope"),
        ),
        "operator_reservation_pressure_reason": first_present(
            collection.get("operator_reservation_pressure_reason"),
            smoke.get("operator_reservation_pressure_reason"),
            operator_binding.get("reservation_pressure_reason"),
        ),
        "operator_reservation_pressure_next_action": first_present(
            collection.get("operator_reservation_pressure_next_action"),
            smoke.get("operator_reservation_pressure_next_action"),
            operator_binding.get("reservation_pressure_next_action"),
        ),
        "missing_live_artifact_rows": missing_rows,
        "primary_claim_blocker": first_present(
            verification.get("primary_claim_blocker"),
            collection_summary.get("primary_claim_blocker"),
            claim_blockers[0] if claim_blockers else None,
        ),
        "primary_claim_blocker_next_action": first_present(
            verification.get("primary_claim_blocker_next_action"),
            collection_summary.get("primary_claim_blocker_next_action"),
        ),
        "vram_admission_mode": first_present(
            verification.get("vram_admission_mode"), collection_summary.get("vram_admission_mode")
        ),
        "vram_next_evidence_target": first_present(
            verification.get("vram_next_evidence_target"),
            collection_summary.get("vram_next_evidence_target"),
        ),
        "vram_model_driver_count": first_present(
            verification.get("vram_model_driver_count"),
            verified_vram_drivers.get("top_driver_count"),
            collection_summary.get("vram_model_driver_count"),
        ),
        "vram_driver_impact_basis": first_present(
            verification.get("vram_driver_impact_basis"),
            verified_vram_drivers.get("impact_basis"),
            collection_summary.get("vram_driver_impact_basis"),
        ),
        "vram_top_driver_descriptions": first_present(
            verification.get("vram_top_driver_descriptions"),
            verified_vram_drivers.get("top_driver_descriptions"),
            collection_summary.get("vram_top_driver_descriptions"),
        ) or [],
        "vram_claim_safe_driver_descriptions": first_present(
            verification.get("vram_claim_safe_driver_descriptions"),
            verified_vram_drivers.get("claim_safe_driver_descriptions"),
            collection_summary.get("vram_claim_safe_driver_descriptions"),
        ) or [],
        "vram_real_top_driver_descriptions": first_present(
            verification.get("vram_real_top_driver_descriptions"),
            verified_vram_drivers.get("real_top_driver_descriptions"),
            collection_summary.get("vram_real_top_driver_descriptions"),
        ) or [],
        "vram_synthetic_driver_descriptions": first_present(
            verification.get("vram_synthetic_driver_descriptions"),
            verified_vram_drivers.get("synthetic_pressure_driver_descriptions"),
            collection_summary.get("vram_synthetic_driver_descriptions"),
        ) or [],
        "vram_top_organic_driver_descriptions": first_present(
            verification.get("vram_top_organic_driver_descriptions"),
            verified_vram_drivers.get("top_organic_driver_descriptions"),
            collection_summary.get("vram_top_organic_driver_descriptions"),
        ) or [],
        "vram_top_driver_group_impacts": first_present(
            verification.get("vram_top_driver_group_impacts"),
            verified_vram_drivers.get("group_impacts"),
            collection_summary.get("vram_top_driver_group_impacts"),
        ) or [],
        "simulator_claim_ready": simulator_claim_ready,
        "simulator_claim_mode": simulator_claim_mode,
        "simulator_claim_blocker": simulator_claim_blocker,
        "simulator_claim_next_action": simulator_claim_next_action,
        "vram_top_driver_labels": first_present(
            verification.get("vram_top_driver_labels"),
            verified_vram_drivers.get("top_driver_labels"),
            collection_summary.get("vram_top_driver_labels"),
        ) or [],
        "vram_display_top_driver_labels": first_present(
            verification.get("vram_display_top_driver_labels"),
            verified_vram_drivers.get("display_top_driver_labels"),
            collection_summary.get("vram_display_top_driver_labels"),
        )
        or display_vram_driver_labels(
            first_present(
                verification.get("vram_top_driver_labels"),
                verified_vram_drivers.get("top_driver_labels"),
                collection_summary.get("vram_top_driver_labels"),
            )
            or []
        ),
        "vram_real_model_driver_count": first_present(
            verification.get("vram_real_model_driver_count"),
            verified_vram_drivers.get("real_top_driver_count"),
            collection_summary.get("vram_real_model_driver_count"),
        ),
        "vram_real_top_driver_labels": first_present(
            verification.get("vram_real_top_driver_labels"),
            verified_vram_drivers.get("real_top_driver_labels"),
            collection_summary.get("vram_real_top_driver_labels"),
        ) or [],
        "vram_display_real_top_driver_labels": first_present(
            verification.get("vram_display_real_top_driver_labels"),
            verified_vram_drivers.get("display_real_top_driver_labels"),
            collection_summary.get("vram_display_real_top_driver_labels"),
        )
        or display_vram_driver_labels(
            first_present(
                verification.get("vram_real_top_driver_labels"),
                verified_vram_drivers.get("real_top_driver_labels"),
                collection_summary.get("vram_real_top_driver_labels"),
            )
            or []
        ),
        "vram_synthetic_driver_count": first_present(
            verification.get("vram_synthetic_driver_count"),
            verified_vram_drivers.get("synthetic_pressure_driver_count"),
            collection_summary.get("vram_synthetic_driver_count"),
        ),
        "vram_synthetic_driver_labels": first_present(
            verification.get("vram_synthetic_driver_labels"),
            verified_vram_drivers.get("synthetic_pressure_driver_labels"),
            collection_summary.get("vram_synthetic_driver_labels"),
        ) or [],
        "vram_display_synthetic_driver_labels": first_present(
            verification.get("vram_display_synthetic_driver_labels"),
            verified_vram_drivers.get("display_synthetic_pressure_driver_labels"),
            collection_summary.get("vram_display_synthetic_driver_labels"),
        )
        or display_vram_driver_labels(
            first_present(
                verification.get("vram_synthetic_driver_labels"),
                verified_vram_drivers.get("synthetic_pressure_driver_labels"),
                collection_summary.get("vram_synthetic_driver_labels"),
            )
            or []
        ),
        "vram_claim_safe_driver_count": first_present(
            verification.get("vram_claim_safe_driver_count"),
            verified_vram_drivers.get("claim_safe_driver_count"),
            collection_summary.get("vram_claim_safe_driver_count"),
        ),
        "vram_claim_safe_driver_labels": first_present(
            verification.get("vram_claim_safe_driver_labels"),
            verified_vram_drivers.get("claim_safe_driver_labels"),
            collection_summary.get("vram_claim_safe_driver_labels"),
        ) or [],
        "vram_display_claim_safe_driver_labels": first_present(
            verification.get("vram_display_claim_safe_driver_labels"),
            verified_vram_drivers.get("display_claim_safe_driver_labels"),
            collection_summary.get("vram_display_claim_safe_driver_labels"),
        )
        or display_vram_driver_labels(
            first_present(
                verification.get("vram_claim_safe_driver_labels"),
                verified_vram_drivers.get("claim_safe_driver_labels"),
                collection_summary.get("vram_claim_safe_driver_labels"),
            )
            or []
        ),
        "vram_synthetic_reserve_driver": first_present(
            verification.get("vram_synthetic_reserve_driver"),
            verified_vram_drivers.get("synthetic_reserve_driver"),
            collection_summary.get("vram_synthetic_reserve_driver"),
        ),
        "vram_synthetic_headroom_driver": first_present(
            synthetic_headroom_driver_value(verification),
            synthetic_headroom_driver_value(verified_vram_drivers),
            synthetic_headroom_driver_value(collection_summary),
        ),
        "vram_investment_demo_rows": smoke.get("vram_investment_demo_rows"),
        "vram_investment_oom_risk_reduction_pods": smoke.get(
            "vram_investment_oom_risk_reduction_pods"
        ),
        "vram_investment_high_vram_nodes_preserved": smoke.get(
            "vram_investment_high_vram_nodes_preserved"
        ),
        "vram_investment_advisory_rows": smoke.get("vram_investment_advisory_rows"),
        "vram_investment_average_baseline_oom_risk_percent": smoke.get(
            "vram_investment_average_baseline_oom_risk_percent"
        ),
        "vram_investment_average_ksolver_oom_risk_percent": smoke.get(
            "vram_investment_average_ksolver_oom_risk_percent"
        ),
        "production_readiness_blocker_class": first_present(
            verification.get("production_readiness_blocker_class"),
            collection_summary.get("production_readiness_blocker_class"),
        ),
        "production_readiness_last_error_class": first_present(
            verification.get("production_readiness_last_error_class"),
            collection_summary.get("production_readiness_last_error_class"),
        ),
        "production_readiness": production_readiness,
        "production_readiness_debug_commands": production_debug_commands,
        "production_readiness_first_debug_command": (
            collection_summary.get("production_readiness_first_debug_command")
            or (production_debug_commands[0] if production_debug_commands else None)
        ),
        "simulator_endpoint_count": first_present(
            verification.get("simulator_endpoint_count"),
            collection_summary.get("simulator_endpoint_count"),
        ),
        "simulator_probe_checked_count": first_present(
            verification.get("simulator_probe_checked_count"),
            collection_summary.get("simulator_probe_checked_count"),
        ),
        "simulator_probe_ready_count": first_present(
            verification.get("simulator_probe_ready_count"),
            collection_summary.get("simulator_probe_ready_count"),
        ),
        "simulator_probe_timeout_millis": first_present(
            verification.get("simulator_probe_timeout_millis"),
            collection_summary.get("simulator_probe_timeout_millis"),
        ),
        "simulator_readiness": first_present(
            verification.get("simulator_readiness"), collection_summary.get("simulator_readiness")
        ),
        "simulator_readiness_note": first_present(
            verification.get("simulator_readiness_note"),
            collection_summary.get("simulator_readiness_note"),
        ),
        "smoke": smoke,
        "collection": collection,
        "verification": verification,
    }


def run_demo_gate(
    *,
    base_url: str,
    output_dir: pathlib.Path,
    min_scenarios: int,
    require_review_ready: bool,
    require_simulator_claim_ready: bool = False,
    require_kss_ready: bool = False,
    doctor_preflight: bool = False,
    allow_readiness_blocked: bool = False,
    kss_count: int = 4,
    kss_base_port: int = 12120,
    kss_cache_dir: str = "/tmp/ksolver-kss-cache",
    kss_wait_seconds: int = 0,
    runner: CommandRunner = run_command,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    scripts_dir = pathlib.Path(__file__).resolve().parent
    kss_ready_urls = ""
    kss_ready_count = 0
    doctor_payload: dict[str, Any] | None = None
    if doctor_preflight:
        doctor_cmd = [
            sys.executable,
            str(scripts_dir / "shadow-doctor.py"),
            "--base-url",
            base,
            "--kss-count",
            str(kss_count),
            "--kss-base-port",
            str(kss_base_port),
            "--kss-cache-dir",
            kss_cache_dir,
            "--timeout",
            "10",
            "--json",
        ]
        if not allow_readiness_blocked:
            doctor_cmd.append("--require-readyz")
        if require_kss_ready:
            doctor_cmd.append("--require-kss-ready")
        if require_simulator_claim_ready:
            doctor_cmd.append("--require-simulator-claim-ready")
        doctor_process = runner(doctor_cmd)
        doctor_payload = parse_json_output(doctor_process)
        doctor_payload.setdefault("exit_code", doctor_process.returncode)
        write_json_file(output_dir / "doctor-preflight.json", doctor_payload)
        if doctor_process.returncode != 0:
            result = stage_failure(
                stage="doctor-preflight",
                base_url=base,
                output_dir=output_dir,
                process=doctor_process,
            )
            return attach_doctor_context(result, doctor_payload)

    if require_kss_ready:
        kss_cmd = [
            str(scripts_dir / "kss-pool.sh"),
            "wait-ready-urls" if kss_wait_seconds > 0 else "require-ready-urls",
            str(kss_count),
            str(kss_base_port),
            kss_cache_dir,
        ]
        if kss_wait_seconds > 0:
            kss_cmd.append(str(kss_wait_seconds))
        kss_process = runner(kss_cmd)
        if kss_process.returncode != 0:
            return stage_failure(
                stage="kss-preflight",
                base_url=base,
                output_dir=output_dir,
                process=kss_process,
            )
        kss_ready_urls = (kss_process.stdout or "").strip()
        kss_ready_count = count_urls(kss_ready_urls)

    smoke_cmd = [
        sys.executable,
        str(scripts_dir / "shadow-smoke.py"),
        "--base-url",
        base,
        "--min-scenarios",
        str(min_scenarios),
        "--json",
    ]
    if allow_readiness_blocked:
        smoke_cmd.append("--allow-readiness-blocked")
    smoke_process = runner(smoke_cmd)
    if smoke_process.returncode != 0:
        result = stage_failure(
            stage="smoke",
            base_url=base,
            output_dir=output_dir,
            process=smoke_process,
            readiness=readiness_probe(base),
        )
        if require_kss_ready:
            result["kss_ready_urls"] = kss_ready_urls
            result["kss_ready_count"] = kss_ready_count
        if doctor_payload is not None:
            attach_doctor_context(result, doctor_payload)
        return result
    smoke = parse_json_output(smoke_process)

    collect_cmd = [
        sys.executable,
        str(scripts_dir / "collect-evidence-bundle.py"),
        "--base-url",
        base,
        "--output-dir",
        str(output_dir),
        "--json",
    ]
    collect_process = runner(collect_cmd)
    if collect_process.returncode != 0:
        result = stage_failure(
            stage="collect",
            base_url=base,
            output_dir=output_dir,
            process=collect_process,
        )
        if require_kss_ready:
            result["kss_ready_urls"] = kss_ready_urls
            result["kss_ready_count"] = kss_ready_count
        if doctor_payload is not None:
            attach_doctor_context(result, doctor_payload)
        return result
    collection = parse_json_output(collect_process)

    verify_cmd = [
        sys.executable,
        str(scripts_dir / "verify-evidence-bundle.py"),
        str(output_dir),
        "--json",
    ]
    if require_review_ready:
        verify_cmd.append("--require-review-ready")
    verify_process = runner(verify_cmd)
    verification = parse_json_output(verify_process)
    if verify_process.returncode not in (0, 2):
        result = stage_failure(
            stage="verify",
            base_url=base,
            output_dir=output_dir,
            process=verify_process,
        )
        if require_kss_ready:
            result["kss_ready_urls"] = kss_ready_urls
            result["kss_ready_count"] = kss_ready_count
        if doctor_payload is not None:
            attach_doctor_context(result, doctor_payload)
        return result
    result = build_result(
        base_url=base,
        output_dir=output_dir,
        smoke=smoke,
        collection=collection,
        verification=verification,
        require_review_ready=require_review_ready,
        require_simulator_claim_ready=require_simulator_claim_ready,
        verify_returncode=verify_process.returncode,
    )
    if require_kss_ready:
        result["kss_ready_urls"] = kss_ready_urls
        result["kss_ready_count"] = kss_ready_count
    if doctor_payload is not None:
        attach_doctor_context(result, doctor_payload)
    return result


def printable_summary(result: dict[str, Any]) -> str:
    status = "passed" if result.get("ok") else str(result.get("stage") or "failed")
    parts = [
        f"demo gate {status}: {result.get('output_dir')}",
        f"review {'ready' if result.get('review_ready') else 'blocked'}",
    ]
    kss_ready_urls = str(result.get("kss_ready_urls") or "")
    if kss_ready_urls:
        kss_count = result.get("kss_ready_count")
        if not isinstance(kss_count, int):
            kss_count = count_urls(kss_ready_urls)
        parts.append(f"KSS: {kss_count} ready")
    readiness = result.get("readiness_probe") or {}
    production = readiness.get("production_readiness") or {}
    operator = readiness.get("operator_status") or {}
    if production.get("blocker"):
        parts.append(f"production blocker: {production.get('blocker')}")
    readiness_class = result.get("readiness_blocker_class")
    readiness_mode = result.get("readiness_mode")
    if readiness_mode == "degraded":
        parts.append(f"readiness: degraded/{readiness_class or 'unknown'}")
    if readiness_class:
        parts.append(f"class: {readiness_class}")
    if result.get("doctor_status"):
        parts.append(f"doctor: {result.get('doctor_status')}")
    doctor_api_failure = result.get("doctor_first_api_endpoint_failure") or {}
    if isinstance(doctor_api_failure, dict) and doctor_api_failure.get("endpoint"):
        parts.append(f"doctor API failure: {doctor_api_failure.get('endpoint')}")
        if doctor_api_failure.get("command"):
            parts.append(f"doctor API command: {doctor_api_failure.get('command')}")
    if result.get("doctor_first_recommended_command"):
        parts.append(f"doctor first command: {result.get('doctor_first_recommended_command')}")
    if result.get("failed_command"):
        parts.append(f"failed command: {result.get('failed_command')}")
    if result.get("failed_returncode") is not None:
        parts.append(f"failed returncode: {result.get('failed_returncode')}")
    if result.get("failed_stderr_excerpt"):
        parts.append(f"stderr: {result.get('failed_stderr_excerpt')}")
    if result.get("failed_stdout_excerpt"):
        parts.append(f"stdout: {result.get('failed_stdout_excerpt')}")
    if result.get("parse_error"):
        parts.append(f"parse error: {result.get('parse_error')}")
    if production.get("last_error_at"):
        parts.append(f"last error at: {production.get('last_error_at')}")
    debug_commands = operator.get("debug_commands") or production.get("debug_commands") or []
    if debug_commands:
        parts.append(f"debug command: {debug_commands[0]}")
    evidence = readiness.get("evidence_summary") or {}
    simulator = readiness.get("simulator_readiness") or {}
    simulator_readiness = result.get("simulator_readiness") or evidence.get("simulator_readiness") or simulator.get("readiness")
    simulator_endpoint_count = result.get("simulator_endpoint_count")
    if simulator_endpoint_count is None:
        simulator_endpoint_count = evidence.get("simulator_endpoint_count", simulator.get("endpoint_count"))
    simulator_probe = simulator.get("readiness_probe") or {}
    simulator_probe_checked = result.get("simulator_probe_checked_count")
    if simulator_probe_checked is None:
        simulator_probe_checked = evidence.get(
            "simulator_probe_checked_count", simulator_probe.get("checked_count")
        )
    simulator_probe_ready = result.get("simulator_probe_ready_count")
    if simulator_probe_ready is None:
        simulator_probe_ready = evidence.get(
            "simulator_probe_ready_count", simulator_probe.get("ready_count")
        )
    if simulator_readiness:
        if simulator_probe_checked is not None:
            parts.append(
                f"simulator: {simulator_readiness} "
                f"({count_label(simulator_probe_ready)}/{simulator_probe_checked} ready, "
                f"{count_label(simulator_endpoint_count)} endpoint(s))"
            )
        else:
            parts.append(f"simulator: {simulator_readiness} ({count_label(simulator_endpoint_count)} endpoint(s))")
    simulator_claim_mode = first_present(
        result.get("simulator_claim_mode"),
        evidence.get("simulator_claim_mode"),
        (simulator or {}).get("claim_mode") if isinstance(simulator, dict) else None,
    )
    simulator_claim_ready = first_present(
        result.get("simulator_claim_ready"),
        evidence.get("simulator_claim_ready"),
        (simulator or {}).get("claim_ready") if isinstance(simulator, dict) else None,
    )
    simulator_claim_blocker = first_present(
        result.get("simulator_claim_blocker"),
        evidence.get("simulator_claim_blocker"),
        (simulator or {}).get("claim_blocker") if isinstance(simulator, dict) else None,
    )
    if simulator_claim_mode:
        suffix = "ready" if simulator_claim_ready is True else f"blocked: {simulator_claim_blocker or 'unknown'}"
        parts.append(f"simulator claim: {simulator_claim_mode} ({suffix})")
    operator_proof_gates = (operator.get("proof_gates") or {}) if isinstance(operator, dict) else {}
    if operator_proof_gates.get("total") is not None:
        parts.append(
            "proof gates: "
            f"{count_label(operator_proof_gates.get('pass'))} pass, "
            f"{count_label(operator_proof_gates.get('warn'))} warn, "
            f"{count_label(operator_proof_gates.get('blocked'))} blocked"
        )
    operator_binding = (operator.get("binding_safety") or {}) if isinstance(operator, dict) else {}
    reservation_pressure = first_present(
        result.get("operator_reservation_pressure"),
        operator_binding.get("reservation_pressure"),
    )
    if reservation_pressure:
        parts.append(f"binding reservation pressure: {reservation_pressure}")
    reservation_reason = first_present(
        result.get("operator_reservation_pressure_reason"),
        operator_binding.get("reservation_pressure_reason"),
    )
    reservation_scope = first_present(
        result.get("operator_reservation_pressure_scope"),
        operator_binding.get("reservation_pressure_scope"),
    )
    if reservation_scope:
        parts.append(f"binding reservation pressure scope: {reservation_scope}")
    if reservation_reason:
        parts.append(f"binding reservation pressure reason: {reservation_reason}")
    missing_count = result.get("missing_live_artifact_count")
    if missing_count:
        parts.append(
            "evidence gaps: "
            f"{count_label(result.get('missing_live_artifact_blocked_count'))} blocked, "
            f"{count_label(result.get('missing_live_artifact_warn_count'))} warn"
        )
        category_summary = category_counts_text(
            result.get("missing_live_artifact_category_counts") or {}
        )
        if category_summary:
            parts.append(f"gap categories: {category_summary}")
        category_rows = result.get("missing_live_artifact_category_rows") or []
        if category_rows:
            first = category_rows[0]
            parts.append(
                "first gap category action: "
                f"{first.get('category')}: {first.get('next_action')}"
            )
        action_items = result.get("missing_live_artifact_action_items") or []
        if action_items and action_items[0].get("command_hint"):
            parts.append(f"first action command: {action_items[0].get('command_hint')}")
        runbook = result.get("operator_runbook") or {}
        if runbook:
            parts.append(
                "operator runbook: "
                f"{count_label(runbook.get('step_count'))} steps, "
                f"{count_label(runbook.get('copyable_command_count'))} shell, "
                f"{count_label(runbook.get('manual_step_count'))} manual"
            )
            if runbook.get("next_shell_command"):
                parts.append(f"next shell command: {runbook.get('next_shell_command')}")
            command_rows = operator_runbook_command_rows(runbook)
            if command_rows:
                first_command = command_rows[0]
                parts.append(
                    "first shell command reason: "
                    f"{first_command.get('category')}: {first_command.get('next_action')}"
                )
            elif runbook.get("next_shell_command"):
                next_action = result.get("primary_claim_blocker_next_action") or "operator action"
                command = str(runbook.get("next_shell_command") or "")
                if "kubectl" in command or readiness_class == "kubernetes_watch":
                    category = "environment"
                    next_action = "restore Kubernetes API connectivity"
                else:
                    category = "environment" if "kubernetes" in str(next_action).lower() or "api" in str(next_action).lower() else "operator"
                parts.append(f"first shell command reason: {category}: {next_action}")
    vram_mode = result.get("vram_admission_mode") or evidence.get("vram_admission_mode")
    vram_next = result.get("vram_next_evidence_target") or evidence.get("vram_next_evidence_target")
    operator_vram = (operator.get("vram") or {}) if isinstance(operator, dict) else {}
    vram_driver_count = first_present(
        result.get("vram_model_driver_count"),
        evidence.get("vram_model_driver_count"),
        operator_vram.get("model_driver_count"),
    )
    vram_driver_labels = first_present(
        result.get("vram_top_driver_labels"),
        evidence.get("vram_top_driver_labels"),
        operator_vram.get("top_driver_labels"),
    ) or []
    vram_display_driver_labels = first_present(
        result.get("vram_display_top_driver_labels"),
        evidence.get("vram_display_top_driver_labels"),
        operator_vram.get("display_top_driver_labels"),
    ) or display_vram_driver_labels(vram_driver_labels)
    vram_real_driver_labels = first_present(
        result.get("vram_real_top_driver_labels"),
        evidence.get("vram_real_top_driver_labels"),
        operator_vram.get("real_top_driver_labels"),
    ) or []
    vram_display_real_driver_labels = first_present(
        result.get("vram_display_real_top_driver_labels"),
        evidence.get("vram_display_real_top_driver_labels"),
        operator_vram.get("display_real_top_driver_labels"),
    ) or display_vram_driver_labels(vram_real_driver_labels)
    vram_real_driver_count = first_present(
        result.get("vram_real_model_driver_count"),
        evidence.get("vram_real_model_driver_count"),
        operator_vram.get("real_model_driver_count"),
    )
    vram_claim_safe_driver_labels = first_present(
        result.get("vram_claim_safe_driver_labels"),
        evidence.get("vram_claim_safe_driver_labels"),
        operator_vram.get("claim_safe_driver_labels"),
    ) or []
    vram_display_claim_safe_driver_labels = first_present(
        result.get("vram_display_claim_safe_driver_labels"),
        evidence.get("vram_display_claim_safe_driver_labels"),
        operator_vram.get("display_claim_safe_driver_labels"),
    ) or display_vram_driver_labels(vram_claim_safe_driver_labels)
    vram_claim_safe_driver_count = first_present(
        result.get("vram_claim_safe_driver_count"),
        evidence.get("vram_claim_safe_driver_count"),
        operator_vram.get("claim_safe_driver_count"),
    )
    vram_synthetic_driver_count = first_present(
        result.get("vram_synthetic_driver_count"),
        evidence.get("vram_synthetic_driver_count"),
        operator_vram.get("synthetic_driver_count"),
    )
    vram_synthetic_driver_labels = first_present(
        result.get("vram_synthetic_driver_labels"),
        evidence.get("vram_synthetic_driver_labels"),
        operator_vram.get("synthetic_driver_labels"),
    ) or []
    vram_display_synthetic_driver_labels = first_present(
        result.get("vram_display_synthetic_driver_labels"),
        evidence.get("vram_display_synthetic_driver_labels"),
        operator_vram.get("display_synthetic_driver_labels"),
    ) or display_vram_driver_labels(vram_synthetic_driver_labels)
    vram_impact_basis = first_present(
        result.get("vram_driver_impact_basis"),
        evidence.get("vram_driver_impact_basis"),
    )
    vram_organic_descriptions = first_present(
        result.get("vram_top_organic_driver_descriptions"),
        evidence.get("vram_top_organic_driver_descriptions"),
    ) or []
    if vram_mode:
        parts.append(f"VRAM: {vram_mode}")
    if vram_driver_count is not None:
        label_source = (
            vram_display_claim_safe_driver_labels
            or vram_display_real_driver_labels
            or vram_display_driver_labels
        )
        label_text = ", ".join(str(label) for label in label_source[:3] if label)
        suffix = f" ({label_text})" if label_text else ""
        if vram_claim_safe_driver_count is not None:
            parts.append(f"VRAM claim-safe drivers: {vram_claim_safe_driver_count}{suffix}")
        elif vram_real_driver_count is not None:
            parts.append(f"VRAM real drivers: {vram_real_driver_count}{suffix}")
        else:
            parts.append(f"VRAM drivers: {vram_driver_count}{suffix}")
        if vram_synthetic_driver_count is not None:
            synthetic_text = ", ".join(str(label) for label in vram_display_synthetic_driver_labels[:2] if label)
            synthetic_suffix = f" ({synthetic_text})" if synthetic_text else ""
            parts.append(
                f"VRAM synthetic headroom drivers: {vram_synthetic_driver_count}{synthetic_suffix}"
            )
        if vram_impact_basis:
            parts.append(f"VRAM impact basis: {vram_impact_basis}")
        if vram_organic_descriptions:
            organic_text = ", ".join(str(label) for label in vram_organic_descriptions[:2] if label)
            if organic_text:
                parts.append(f"VRAM organic drivers: {organic_text}")
    vram_investment_rows = first_present(
        result.get("vram_investment_demo_rows"),
        evidence.get("vram_investment_demo_rows"),
    )
    if vram_investment_rows is not None:
        vram_reduction = first_present(
            result.get("vram_investment_oom_risk_reduction_pods"),
            evidence.get("vram_investment_oom_risk_reduction_pods"),
        )
        vram_preserved = first_present(
            result.get("vram_investment_high_vram_nodes_preserved"),
            evidence.get("vram_investment_high_vram_nodes_preserved"),
        )
        parts.append(
            "VRAM demo: "
            f"{count_label(vram_investment_rows)} rows, "
            f"{count_label(vram_reduction)} "
            "OOM-risk pods reduced, "
            f"{count_label(vram_preserved)} "
            "high-VRAM preserved"
        )
    if vram_next:
        parts.append(f"next VRAM evidence: {vram_next}")
    production_class = (
        result.get("production_readiness_blocker_class")
        or evidence.get("production_readiness_blocker_class")
    )
    if production_class:
        parts.append(f"production class: {production_class}")
    production_error_class = (
        result.get("production_readiness_last_error_class")
        or evidence.get("production_readiness_last_error_class")
    )
    if production_error_class:
        parts.append(f"production error class: {production_error_class}")
    production_first_debug = first_present(
        result.get("production_readiness_first_debug_command"),
        evidence.get("production_readiness_first_debug_command"),
        ((operator.get("production_readiness") or {}).get("debug_commands") or [None])[0],
    )
    if production_first_debug:
        parts.append(f"production first debug command: {production_first_debug}")
    blockers = result.get("claim_blockers") or []
    if not blockers:
        blockers = evidence.get("claim_blockers") or []
    primary_blocker = (
        result.get("primary_claim_blocker")
        or operator.get("primary_blocker")
        or evidence.get("primary_claim_blocker")
        or (blockers[0] if blockers else None)
    )
    if primary_blocker:
        parts.append(f"primary blocker: {primary_blocker}")
    primary_action = (
        result.get("primary_claim_blocker_next_action")
        or operator.get("next_action")
        or evidence.get("primary_claim_blocker_next_action")
    )
    if primary_action:
        parts.append(f"next action: {primary_action}")
    return ", ".join(parts)


def compact_result(result: dict[str, Any]) -> dict[str, Any]:
    runbook = result.get("operator_runbook") or {}
    command_rows = operator_runbook_command_rows(runbook)
    first_command = command_rows[0] if command_rows else {}
    display_top_driver_labels = result.get("vram_display_top_driver_labels") or display_vram_driver_labels(
        result.get("vram_top_driver_labels") or []
    )
    display_real_top_driver_labels = result.get(
        "vram_display_real_top_driver_labels"
    ) or display_vram_driver_labels(result.get("vram_real_top_driver_labels") or [])
    display_claim_safe_driver_labels = result.get(
        "vram_display_claim_safe_driver_labels"
    ) or display_vram_driver_labels(result.get("vram_claim_safe_driver_labels") or [])
    display_synthetic_driver_labels = result.get(
        "vram_display_synthetic_driver_labels"
    ) or display_vram_driver_labels(result.get("vram_synthetic_driver_labels") or [])
    compact = {
        "ok": result.get("ok"),
        "stage": result.get("stage"),
        "exit_code": result.get("exit_code"),
        "base_url": result.get("base_url"),
        "output_dir": result.get("output_dir"),
        "failed_command": result.get("failed_command"),
        "failed_returncode": result.get("failed_returncode"),
        "failed_stdout_excerpt": result.get("failed_stdout_excerpt"),
        "failed_stderr_excerpt": result.get("failed_stderr_excerpt"),
        "parse_error": result.get("parse_error"),
        "review_ready": result.get("review_ready"),
        "require_review_ready": result.get("require_review_ready"),
        "require_simulator_claim_ready": result.get("require_simulator_claim_ready"),
        "readiness_mode": result.get("readiness_mode"),
        "readiness_blocker_class": result.get("readiness_blocker_class"),
        "primary_claim_blocker": result.get("primary_claim_blocker"),
        "primary_claim_blocker_next_action": result.get("primary_claim_blocker_next_action"),
        "doctor_status": result.get("doctor_status"),
        "doctor_failures": result.get("doctor_failures"),
        "doctor_first_recommended_command": result.get("doctor_first_recommended_command"),
        "doctor_recommended_commands": result.get("doctor_recommended_commands"),
        "doctor_api_endpoint_failure_count": result.get("doctor_api_endpoint_failure_count"),
        "doctor_first_api_endpoint_failure": result.get("doctor_first_api_endpoint_failure"),
        "missing_live_artifact_count": result.get("missing_live_artifact_count"),
        "missing_live_artifact_blocked_count": result.get("missing_live_artifact_blocked_count"),
        "missing_live_artifact_warn_count": result.get("missing_live_artifact_warn_count"),
        "operator_runbook_step_count": runbook.get("step_count"),
        "operator_runbook_copyable_command_count": runbook.get("copyable_command_count"),
        "operator_runbook_manual_step_count": runbook.get("manual_step_count"),
        "operator_next_shell_command": runbook.get("next_shell_command"),
        "operator_first_shell_command": first_command.get("command"),
        "operator_first_shell_command_category": first_command.get("category"),
        "operator_first_shell_command_severity": first_command.get("severity"),
        "operator_first_shell_command_artifact": first_command.get("artifact"),
        "operator_first_shell_command_next_action": first_command.get("next_action"),
        "operator_first_shell_command_kind": first_command.get("command_kind"),
        "operator_binding_status": result.get("operator_binding_status"),
        "operator_reservation_pressure": result.get("operator_reservation_pressure"),
        "operator_reservation_pressure_description": result.get("operator_reservation_pressure_description"),
        "operator_reservation_pressure_scope": result.get("operator_reservation_pressure_scope"),
        "operator_reservation_pressure_reason": result.get("operator_reservation_pressure_reason"),
        "operator_reservation_pressure_next_action": result.get("operator_reservation_pressure_next_action"),
        "production_readiness_blocker_class": result.get("production_readiness_blocker_class"),
        "production_readiness_last_error_class": result.get("production_readiness_last_error_class"),
        "production_readiness_first_debug_command": result.get("production_readiness_first_debug_command"),
        "simulator_readiness": result.get("simulator_readiness"),
        "simulator_endpoint_count": result.get("simulator_endpoint_count"),
        "simulator_probe_checked_count": result.get("simulator_probe_checked_count"),
        "simulator_probe_ready_count": result.get("simulator_probe_ready_count"),
        "simulator_claim_ready": result.get("simulator_claim_ready"),
        "simulator_claim_mode": result.get("simulator_claim_mode"),
        "simulator_claim_blocker": result.get("simulator_claim_blocker"),
        "simulator_claim_next_action": result.get("simulator_claim_next_action"),
        "kss_ready_count": result.get("kss_ready_count"),
        "vram_admission_mode": result.get("vram_admission_mode"),
        "vram_next_evidence_target": result.get("vram_next_evidence_target"),
        "vram_driver_impact_basis": result.get("vram_driver_impact_basis"),
        "vram_top_driver_descriptions": (result.get("vram_top_driver_descriptions") or [])[:5],
        "vram_claim_safe_driver_descriptions": (result.get("vram_claim_safe_driver_descriptions") or [])[:5],
        "vram_real_top_driver_descriptions": (result.get("vram_real_top_driver_descriptions") or [])[:5],
        "vram_synthetic_driver_descriptions": (result.get("vram_synthetic_driver_descriptions") or [])[:5],
        "vram_top_organic_driver_descriptions": (result.get("vram_top_organic_driver_descriptions") or [])[:5],
        "vram_top_driver_group_impacts": (result.get("vram_top_driver_group_impacts") or [])[:5],
        "vram_display_top_driver_labels": display_top_driver_labels[:5],
        "vram_display_real_top_driver_labels": display_real_top_driver_labels[:5],
        "vram_claim_safe_driver_count": result.get("vram_claim_safe_driver_count"),
        "vram_claim_safe_driver_labels": (result.get("vram_claim_safe_driver_labels") or [])[:5],
        "vram_display_claim_safe_driver_labels": display_claim_safe_driver_labels[:5],
        "vram_synthetic_driver_count": result.get("vram_synthetic_driver_count"),
        "vram_synthetic_driver_labels": (result.get("vram_synthetic_driver_labels") or [])[:5],
        "vram_display_synthetic_driver_labels": display_synthetic_driver_labels[:5],
        "vram_investment_demo_rows": result.get("vram_investment_demo_rows"),
        "vram_investment_oom_risk_reduction_pods": result.get(
            "vram_investment_oom_risk_reduction_pods"
        ),
        "summary": printable_summary(result),
    }
    return {key: value for key, value in compact.items() if value is not None}


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run shadow smoke, collect evidence, and verify the captured packet."
    )
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8090",
        help="shadow server URL; default: %(default)s",
    )
    parser.add_argument(
        "--output-root",
        default="evidence-bundles",
        help="parent directory for timestamped demo-gate bundles; default: %(default)s",
    )
    parser.add_argument(
        "--output-dir",
        help="exact output directory; overrides --output-root timestamp creation",
    )
    parser.add_argument(
        "--min-scenarios",
        type=non_negative_int,
        default=33,
        help="minimum scenario count required from demo-report; default: %(default)s",
    )
    parser.add_argument(
        "--require-review-ready",
        action="store_true",
        help="exit 2 when the packet is captured and verified but still claim-blocked",
    )
    parser.add_argument(
        "--require-simulator-claim-ready",
        action="store_true",
        help=(
            "exit 2 when the packet is captured and verified but the kube-scheduler "
            "baseline is not backed by a live simulator claim"
        ),
    )
    parser.add_argument(
        "--require-kss-ready",
        action="store_true",
        help="fail before smoke when no local kube-scheduler-simulator pool endpoint passes /api/v1/export",
    )
    parser.add_argument(
        "--doctor-preflight",
        action="store_true",
        help="run shadow-doctor.py before smoke and fail early with its recommended command when strict checks fail",
    )
    parser.add_argument(
        "--allow-readiness-blocked",
        action="store_true",
        help=(
            "pass --allow-readiness-blocked to shadow-smoke so observe-only demo "
            "validation can proceed when the Kubernetes watch blocks /readyz"
        ),
    )
    parser.add_argument(
        "--kss-count",
        type=positive_int,
        default=4,
        help="KSS pool instance count passed to kss-pool.sh when --require-kss-ready is set; default: %(default)s",
    )
    parser.add_argument(
        "--kss-base-port",
        type=non_negative_int,
        default=12120,
        help="KSS pool fallback base port passed to kss-pool.sh when --require-kss-ready is set; default: %(default)s",
    )
    parser.add_argument(
        "--kss-cache-dir",
        default="/tmp/ksolver-kss-cache",
        help="KSS simulator cache directory used in diagnostics; default: %(default)s",
    )
    parser.add_argument(
        "--wait-kss-ready-seconds",
        type=non_negative_int,
        default=0,
        help=(
            "when --require-kss-ready is set, wait up to this many seconds for a KSS "
            "endpoint to pass /api/v1/export before failing; default: %(default)s"
        ),
    )
    parser.add_argument(
        "--start-kss",
        action="store_true",
        help=(
            "start a local kube-scheduler-simulator pool via kss-pool.sh before the gate and "
            "tear it down after (opt-in; default off leaves pool lifecycle to the caller). Uses "
            "--kss-count/--kss-base-port/--kss-cache-dir; pair with --require-kss-ready to verify it "
            "came up. Since the F2 fix, a single pool serves all baselines, so this makes a live gate "
            "a one-command run."
        ),
    )
    output_group = parser.add_mutually_exclusive_group()
    output_group.add_argument("--json", action="store_true", help="emit full machine-readable JSON")
    output_group.add_argument(
        "--compact-json",
        action="store_true",
        help="emit compact CI/SRE JSON without nested smoke, collection, or verification payloads",
    )
    return parser


def main() -> int:
    parser = build_arg_parser()
    args = parser.parse_args()

    output_dir = (
        pathlib.Path(args.output_dir)
        if args.output_dir
        else pathlib.Path(args.output_root) / f"demo-gate-{timestamp_slug()}"
    )

    # Opt-in pool lifecycle. Started here (outside run_demo_gate) so teardown is guaranteed via
    # finally regardless of which stage the gate returns/raises at. Default off => unchanged behavior.
    scripts_dir = pathlib.Path(__file__).resolve().parent
    kss_pool_args = [str(args.kss_count), str(args.kss_base_port), args.kss_cache_dir]
    started_kss = False
    if args.start_kss:
        start = run_command(
            [str(scripts_dir / "kss-pool.sh"), "start", *kss_pool_args, "120"]
        )
        started_kss = start.returncode == 0
        if not started_kss:
            print(
                f"--start-kss: kss-pool.sh start failed (rc={start.returncode}): "
                f"{(start.stderr or start.stdout or '').strip()[:400]}",
                file=sys.stderr,
            )
            return 2

    try:
        result = run_demo_gate(
            base_url=args.base_url,
            output_dir=output_dir,
            min_scenarios=args.min_scenarios,
            require_review_ready=args.require_review_ready,
            require_simulator_claim_ready=args.require_simulator_claim_ready,
            require_kss_ready=args.require_kss_ready,
            doctor_preflight=args.doctor_preflight,
            allow_readiness_blocked=args.allow_readiness_blocked,
            kss_count=args.kss_count,
            kss_base_port=args.kss_base_port,
            kss_cache_dir=args.kss_cache_dir,
            kss_wait_seconds=args.wait_kss_ready_seconds,
        )
    finally:
        if started_kss:
            run_command([str(scripts_dir / "kss-pool.sh"), "stop", *kss_pool_args])
    write_json_file(output_dir / DEMO_GATE_RESULT_FILENAME, result)
    if args.compact_json:
        print(json.dumps(compact_result(result), sort_keys=True))
    elif args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(printable_summary(result))
    return int(result.get("exit_code") or 0)


if __name__ == "__main__":
    raise SystemExit(main())
