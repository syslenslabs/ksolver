#!/usr/bin/env python3
"""Unit tests for collect-evidence-bundle.py helpers."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest
from datetime import datetime, timezone


ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "collect_evidence_bundle", ROOT / "collect-evidence-bundle.py"
)
assert SPEC and SPEC.loader
collector = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(collector)

RESERVE_PRESSURE_DEFINITION = (
    "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM padding "
    "to stress scheduler headroom; this is a pressure-test signal, not organic model demand."
)
DRIVER_CLAIM_BOUNDARY = (
    "Use real_top_drivers for model-memory claims. synthetic_pressure_drivers are "
    "stress-test probes only and must not be presented as organic workload predictors."
)


class CollectEvidenceBundleTests(unittest.TestCase):
    def test_human_facing_source_uses_synthetic_headroom_wording(self) -> None:
        source = (ROOT / "collect-evidence-bundle.py").read_text(encoding="utf-8")
        self.assertNotIn("synthetic reserve pressure", source)
        self.assertNotIn("synthetic transformer reserve pressure", source)
        self.assertIn("synthetic headroom", source)

    def test_endpoint_from_curl_command(self) -> None:
        self.assertEqual(
            collector.endpoint_from_command(
                "curl -s http://127.0.0.1:8090/api/scheduler/evidence-bundle > evidence-bundle.json"
            ),
            "/api/scheduler/evidence-bundle",
        )

    def test_endpoint_filename_is_stable(self) -> None:
        self.assertEqual(
            collector.endpoint_filename("/api/scheduler/vram-calibration"),
            "api-scheduler-vram-calibration.json",
        )

    def test_file_metadata_reports_size_and_sha256(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "sample.json"
            path.write_text('{"ok": true}\n', encoding="utf-8")
            meta = collector.file_metadata(path)
        self.assertEqual(meta["bytes"], 13)
        self.assertEqual(
            meta["sha256"],
            "55f66c2c5aeb275ff5b1ae26b321d5c0b8ceda8c034b19c2643e046d024919f3",
        )

    def test_endpoints_from_bundle_preserves_commands_and_adds_defaults(self) -> None:
        endpoints = collector.endpoints_from_bundle(
            {
                "collection_commands": [
                    "curl -s http://127.0.0.1:8090/api/scheduler/evidence-bundle > evidence-bundle.json",
                ]
            }
        )
        self.assertEqual(endpoints[0], "/api/scheduler/evidence-bundle")
        self.assertIn("/api/scheduler/traces", endpoints)
        self.assertEqual(len(endpoints), len(set(endpoints)))

    def test_environment_action_item_preserves_debug_command_list(self) -> None:
        rows = [
            {
                "category": "environment",
                "severity": "blocked",
                "blocked": 1,
                "warn": 0,
                "artifact": "healthy Kubernetes watch-relist state",
                "next_action": "restore Kubernetes API connectivity",
            }
        ]
        items = collector.missing_artifact_action_items(rows)
        runbook = collector.operator_action_runbook(items)

        self.assertEqual(items[0]["command_hint"], "kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        self.assertEqual(
            items[0]["command_hints"],
            [
                "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "kubectl config current-context",
                "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
                "kubectl --request-timeout=10s get nodes",
            ],
        )
        self.assertEqual(runbook["copyable_command_count"], 4)
        self.assertIn(
            "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
            runbook["copyable_commands"],
        )
        self.assertEqual(
            runbook["copyable_command_rows"][0]["command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(runbook["copyable_command_rows"][0]["category"], "environment")
        self.assertEqual(
            runbook["copyable_command_rows"][0]["artifact"],
            "healthy Kubernetes watch-relist state",
        )

    def test_collect_bundle_uses_captured_evidence_bundle_for_manifest(self) -> None:
        stale_summary = {
            "customer_claim_ready": False,
            "missing_live_artifact_count": 1,
            "mutation_allowed": False,
            "vram_advisory_ready": True,
            "review_ready": False,
            "claim_blockers": ["stale"],
            "simulator_readiness": "configured_unreachable",
            "simulator_probe_ready_count": 0,
        }
        captured_summary = {
            "customer_claim_ready": False,
            "missing_live_artifact_count": 0,
            "mutation_allowed": False,
            "vram_advisory_ready": True,
            "review_ready": False,
            "claim_blockers": ["captured"],
            "simulator_readiness": "ready",
            "simulator_probe_ready_count": 1,
        }
        calls = []

        def fake_fetch_json(url: str):
            calls.append(url)
            if len(calls) == 1:
                return 200, {"ok": True, "summary": stale_summary}
            endpoint = "/" + url.split("/", 3)[3]
            if endpoint == "/api/scheduler/evidence-bundle":
                return 200, {"ok": True, "summary": captured_summary}
            if endpoint == "/api/scheduler/operator-status":
                return 200, {
                    "ok": True,
                    "status": "blocked",
                    "primary_blocker": "captured",
                    "next_action": "use captured endpoint state",
                }
            return 200, {"ok": True}

        original_fetch_json = collector.fetch_json
        collector.fetch_json = fake_fetch_json
        try:
            with tempfile.TemporaryDirectory() as tmp:
                manifest = collector.collect_bundle(
                    "http://127.0.0.1:8090",
                    pathlib.Path(tmp),
                )
                captured = (pathlib.Path(tmp) / "api-scheduler-evidence-bundle.json").read_text(
                    encoding="utf-8"
                )
        finally:
            collector.fetch_json = original_fetch_json

        self.assertEqual(manifest["summary"]["simulator_readiness"], "ready")
        self.assertEqual(manifest["summary"]["simulator_probe_ready_count"], 1)
        self.assertIn('"simulator_readiness": "ready"', captured)

    def test_collect_bundle_preserves_existing_doctor_preflight_artifact(self) -> None:
        doctor_payload = {
            "ok": True,
            "status": "degraded",
            "exit_code": 0,
            "first_recommended_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
            "recommended_commands": [
                {
                    "category": "kubernetes-readiness",
                    "severity": "blocked",
                    "command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                }
            ],
            "api_endpoint_failures": [
                {
                    "category": "shadow-api",
                    "severity": "blocked",
                    "endpoint": "/api/scheduler/operator-status",
                    "command": "curl -fsS http://127.0.0.1:8090/api/scheduler/operator-status",
                    "reason": "operator-status endpoint did not return a valid JSON object",
                }
            ],
        }
        calls = []

        def fake_fetch_json(url: str):
            calls.append(url)
            if len(calls) == 1:
                return 200, {
                    "ok": True,
                    "summary": {
                        "customer_claim_ready": False,
                        "missing_live_artifact_count": 0,
                        "mutation_allowed": False,
                        "vram_advisory_ready": True,
                        "review_ready": False,
                    },
                }
            endpoint = "/" + url.split("/", 3)[3]
            if endpoint == "/api/scheduler/evidence-bundle":
                return 200, {
                    "ok": True,
                    "summary": {
                        "customer_claim_ready": False,
                        "missing_live_artifact_count": 0,
                        "mutation_allowed": False,
                        "vram_advisory_ready": True,
                        "review_ready": False,
                    },
                }
            return 200, {"ok": True}

        original_fetch_json = collector.fetch_json
        collector.fetch_json = fake_fetch_json
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output_dir = pathlib.Path(tmp)
                doctor_path = output_dir / "doctor-preflight.json"
                doctor_path.write_text(json.dumps(doctor_payload) + "\n", encoding="utf-8")
                manifest = collector.collect_bundle("http://127.0.0.1:8090", output_dir)
                preserved = json.loads(doctor_path.read_text(encoding="utf-8"))
                review = (output_dir / "review.md").read_text(encoding="utf-8")
        finally:
            collector.fetch_json = original_fetch_json

        self.assertEqual(preserved, doctor_payload)
        self.assertEqual(manifest["doctor_preflight"]["status"], "degraded")
        self.assertEqual(manifest["doctor_preflight"]["exit_code"], 0)
        self.assertEqual(
            manifest["doctor_preflight"]["first_recommended_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(manifest["doctor_preflight"]["recommended_command_count"], 1)
        self.assertEqual(manifest["doctor_preflight"]["api_endpoint_failure_count"], 1)
        self.assertEqual(
            manifest["doctor_preflight"]["first_api_endpoint_failure"]["endpoint"],
            "/api/scheduler/operator-status",
        )
        self.assertIn("## Doctor Preflight", review)
        self.assertIn("Status: `degraded`", review)
        self.assertIn("API endpoint failures: `1`", review)
        self.assertIn("First API endpoint failure: `/api/scheduler/operator-status`", review)
        self.assertIn(
            "First recommended command: `kubectl --request-timeout=10s get --raw='/readyz?verbose'`",
            review,
        )

    def test_collect_bundle_summarizes_existing_demo_gate_result_artifact(self) -> None:
        demo_gate_payload = {
            "ok": False,
            "stage": "kss-preflight",
            "exit_code": 2,
            "failed_command": "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
            "failed_returncode": 2,
            "failed_stderr_excerpt": "no ready kube-scheduler-simulator endpoints",
        }
        calls = []

        def fake_fetch_json(url: str):
            calls.append(url)
            if len(calls) == 1:
                return 200, {
                    "ok": True,
                    "summary": {
                        "customer_claim_ready": False,
                        "missing_live_artifact_count": 0,
                        "mutation_allowed": False,
                        "vram_advisory_ready": True,
                        "review_ready": False,
                    },
                }
            endpoint = "/" + url.split("/", 3)[3]
            if endpoint == "/api/scheduler/evidence-bundle":
                return 200, {
                    "ok": True,
                    "summary": {
                        "customer_claim_ready": False,
                        "missing_live_artifact_count": 0,
                        "mutation_allowed": False,
                        "vram_advisory_ready": True,
                        "review_ready": False,
                    },
                }
            return 200, {"ok": True}

        original_fetch_json = collector.fetch_json
        collector.fetch_json = fake_fetch_json
        try:
            with tempfile.TemporaryDirectory() as tmp:
                output_dir = pathlib.Path(tmp)
                result_path = output_dir / "demo-gate-result.json"
                result_path.write_text(json.dumps(demo_gate_payload) + "\n", encoding="utf-8")
                manifest = collector.collect_bundle("http://127.0.0.1:8090", output_dir)
                review = (output_dir / "review.md").read_text(encoding="utf-8")
                preserved = json.loads(result_path.read_text(encoding="utf-8"))
        finally:
            collector.fetch_json = original_fetch_json

        self.assertEqual(preserved, demo_gate_payload)
        self.assertEqual(manifest["demo_gate_result"]["stage"], "kss-preflight")
        self.assertEqual(manifest["demo_gate_result"]["exit_code"], 2)
        self.assertEqual(
            manifest["demo_gate_result"]["failed_command"],
            "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
        )
        self.assertIn("## Demo Gate Result", review)
        self.assertIn("- Stage: `kss-preflight`", review)
        self.assertIn(
            "- Failed command: `scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache`",
            review,
        )

    def test_collect_bundle_recaptures_operator_pair_until_action_items_match(self) -> None:
        evidence_actions = [
            {
                "priority": 1,
                "category": "environment",
                "severity": "blocked",
                "command_hint": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "command_kind": "shell",
                "copyable": True,
            }
        ]
        stale_operator_actions = [
            {
                "priority": 1,
                "category": "baseline-proof",
                "severity": "blocked",
                "command_hint": "scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache",
                "command_kind": "shell",
                "copyable": True,
            },
            *evidence_actions,
        ]
        evidence_payload = {
            "ok": True,
            "summary": {
                "customer_claim_ready": False,
                "missing_live_artifact_count": 1,
                "mutation_allowed": False,
                "vram_advisory_ready": True,
                "review_ready": False,
                "claim_blockers": ["customer claim not ready"],
                "missing_live_artifact_action_items": evidence_actions,
                "operator_runbook": collector.operator_action_runbook(evidence_actions),
            },
        }
        calls: list[str] = []
        operator_calls = 0

        def fake_fetch_json(url: str):
            nonlocal operator_calls
            calls.append(url)
            endpoint = "/" + url.split("/", 3)[3]
            if len(calls) == 1:
                return 200, evidence_payload
            if endpoint == "/api/scheduler/evidence-bundle":
                return 200, evidence_payload
            if endpoint == "/api/scheduler/operator-status":
                operator_calls += 1
                actions = stale_operator_actions if operator_calls == 1 else evidence_actions
                return 200, {
                    "ok": True,
                    "status": "blocked",
                    "primary_blocker": "customer claim not ready",
                    "next_action": "collect evidence",
                    "action_items": actions,
                    "operator_runbook": collector.operator_action_runbook(actions),
                }
            return 200, {"ok": True}

        original_fetch_json = collector.fetch_json
        collector.fetch_json = fake_fetch_json
        try:
            with tempfile.TemporaryDirectory() as tmp:
                manifest = collector.collect_bundle(
                    "http://127.0.0.1:8090",
                    pathlib.Path(tmp),
                )
                operator_payload = json.loads(
                    (pathlib.Path(tmp) / "api-scheduler-operator-status.json").read_text(
                        encoding="utf-8"
                    )
                )
        finally:
            collector.fetch_json = original_fetch_json

        self.assertGreaterEqual(operator_calls, 2)
        self.assertEqual(manifest["missing_live_artifact_action_items"], evidence_actions)
        self.assertEqual(operator_payload["action_items"], evidence_actions)
        self.assertEqual(manifest["operator_runbook"], collector.operator_action_runbook(evidence_actions))

    def test_timestamp_slug_is_utc_stable(self) -> None:
        self.assertEqual(
            collector.timestamp_slug(datetime(2026, 7, 6, 1, 2, 3, tzinfo=timezone.utc)),
            "20260706T010203Z",
        )

    def test_build_manifest_marks_complete_packet_but_blocked_claim(self) -> None:
        files = {
            endpoint: {"file": collector.endpoint_filename(endpoint), "status": 200}
            for endpoint in collector.DEFAULT_ENDPOINTS
        }
        manifest = collector.build_manifest(
            base_url="http://127.0.0.1:8090",
            bundle_status=200,
            bundle={
                "ok": True,
                "summary": {
                    "customer_claim_ready": False,
                    "missing_live_artifact_count": 5,
                    "mutation_allowed": False,
                    "vram_advisory_ready": True,
                    "review_ready": False,
                    "claim_blockers": ["customer claim not ready"],
                },
                "live_validation_gates": [
                    {
                        "gate": "pending GPU trace",
                        "status": "blocked",
                        "next_action": "apply a deterministic GPU scenario",
                    },
                    {
                        "gate": "production mutation safety",
                        "status": "pass",
                        "next_action": "use safety posture as launch-gate evidence",
                    },
                ],
            },
            files=files,
            captured_payloads={
                "/api/scheduler/operator-status": {
                    "status": "blocked",
                    "status_label": "operator action required",
                    "primary_blocker": "production readiness blocked: kubernetes_watch",
                    "next_action": "restore Kubernetes API connectivity",
                    "debug_commands": ["kubectl config current-context"],
                    "production_readiness": {
                        "blocker_class": "kubernetes_watch",
                        "debug_commands": [
                            "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
                        ],
                    },
                    "can_shadow_demo": True,
                    "can_customer_claim": False,
                    "binding_safety": {
                        "status": "dry-run-validation",
                        "mode": "dry-run",
                        "reservation_pressure": "active",
                        "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                        "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                        "reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                        "reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
                    },
                    "vram": {
                        "model_driver_count": 2,
                        "top_driver_labels": ["layer count", "synthetic reserve pressure"],
                        "claim_safe_driver_count": 1,
                        "claim_safe_driver_labels": ["layer count"],
                        "real_model_driver_count": 1,
                        "real_top_driver_labels": ["layer count"],
                        "synthetic_driver_count": 1,
                        "synthetic_driver_labels": ["synthetic reserve pressure"],
                        "synthetic_reserve_driver": True,
                        "synthetic_headroom_driver": True,
                        "reserve_pressure_definition": RESERVE_PRESSURE_DEFINITION,
                        "driver_claim_boundary": DRIVER_CLAIM_BOUNDARY,
                    },
                    "demo_gate": {"strict_exit_code": 2},
                },
                "/api/scheduler/vram-calibration": {
                    "model_drivers": {
                        "available": True,
                        "fit": "ridge_linear_interactions",
                        "training_rows": 228,
                        "claim_boundary": DRIVER_CLAIM_BOUNDARY,
                        "top_drivers": [
                            {
                                "feature": "layers",
                                "label": "layer count",
                                "class": "model-size",
                            },
                            {
                                "feature": "reserve_extra_gib",
                                "label": "synthetic reserve pressure",
                                "class": "synthetic-pressure",
                            },
                        ],
                        "real_top_drivers": [
                            {
                                "feature": "layers",
                                "label": "layer count",
                                "class": "model-size",
                            },
                        ],
                        "synthetic_pressure_drivers": [
                            {
                                "feature": "reserve_extra_gib",
                                "label": "synthetic reserve pressure",
                                "class": "synthetic-pressure",
                            },
                        ],
                    },
                },
            },
            generated_at="2026-07-06T01:02:03+00:00",
        )
        self.assertEqual(manifest["packet_complete"], True)
        self.assertEqual(manifest["review_ready"], False)
        self.assertEqual(manifest["missing_live_artifact_count"], 5)
        self.assertEqual(manifest["missing_live_artifact_blocked_count"], 0)
        self.assertEqual(manifest["missing_live_artifact_warn_count"], 0)
        self.assertIn("5 missing live artifact(s)", manifest["claim_blockers"])
        self.assertIn("customer claim not ready", manifest["claim_blockers"])
        self.assertEqual(manifest["missing_endpoints"], [])
        self.assertEqual(manifest["operator_status"]["vram"]["model_driver_count"], 2)
        self.assertEqual(
            manifest["operator_status"]["binding_safety"]["reservation_pressure"],
            "active",
        )
        self.assertIn(
            "pending or reserved GPU capacity",
            manifest["operator_status"]["binding_safety"]["reservation_pressure_description"],
        )
        self.assertEqual(
            manifest["operator_status"]["vram"]["top_driver_labels"],
            ["layer count", "synthetic reserve pressure"],
        )
        self.assertEqual(
            manifest["vram_model_drivers"]["display_top_driver_labels"],
            ["layer count", "synthetic VRAM headroom probe"],
        )
        self.assertEqual(
            manifest["vram_model_drivers"]["display_synthetic_pressure_driver_labels"],
            ["synthetic VRAM headroom probe"],
        )
        self.assertEqual(
            manifest["operator_status"]["vram"]["reserve_pressure_definition"],
            RESERVE_PRESSURE_DEFINITION,
        )

    def test_build_manifest_marks_incomplete_endpoint_capture(self) -> None:
        files = {
            endpoint: {"file": collector.endpoint_filename(endpoint), "status": 200}
            for endpoint in collector.DEFAULT_ENDPOINTS
        }
        files["/api/scheduler/traces"]["status"] = 503
        manifest = collector.build_manifest(
            base_url="http://127.0.0.1:8090",
            bundle_status=200,
            bundle={
                "ok": True,
                "summary": {
                    "customer_claim_ready": True,
                    "missing_live_artifact_count": 0,
                    "mutation_allowed": False,
                    "vram_advisory_ready": True,
                    "review_ready": True,
                    "claim_blockers": [],
                },
            },
            files=files,
        )
        self.assertEqual(manifest["packet_complete"], False)
        self.assertEqual(manifest["review_ready"], False)
        self.assertEqual(manifest["missing_endpoints"], ["/api/scheduler/traces"])
        self.assertIn("endpoint capture incomplete", manifest["claim_blockers"])

    def test_build_manifest_marks_review_ready_when_unblocked(self) -> None:
        files = {
            endpoint: {"file": collector.endpoint_filename(endpoint), "status": 200}
            for endpoint in collector.DEFAULT_ENDPOINTS
        }
        manifest = collector.build_manifest(
            base_url="http://127.0.0.1:8090",
            bundle_status=200,
            bundle={
                "ok": True,
                "summary": {
                    "customer_claim_ready": True,
                    "missing_live_artifact_count": 0,
                    "mutation_allowed": False,
                    "vram_advisory_ready": True,
                    "review_ready": True,
                    "claim_blockers": [],
                },
            },
            files=files,
        )
        self.assertEqual(manifest["ok"], True)
        self.assertEqual(manifest["packet_complete"], True)
        self.assertEqual(manifest["review_ready"], True)
        self.assertEqual(manifest["claim_blockers"], [])

    def test_render_review_markdown_summarizes_blockers_and_files(self) -> None:
        files = {
            "/api/scheduler/evidence-bundle": {
                "file": "api-scheduler-evidence-bundle.json",
                "status": 200,
                "bytes": 128,
                "sha256": "a" * 64,
            },
        }
        manifest = collector.build_manifest(
            base_url="http://127.0.0.1:8090",
            bundle_status=200,
            bundle={
                "ok": True,
                "summary": {
                    "launch_status": "incomplete",
                    "customer_claim_ready": False,
                    "missing_live_artifact_count": 1,
                    "mutation_allowed": False,
                    "vram_advisory_ready": True,
                    "vram_hard_admission_ready": False,
                    "vram_admission_mode": "Shadow advisory only",
                    "vram_scheduler_use": "Score and warn; do not reject pods",
                    "vram_hard_blocker_count": 4,
                    "vram_next_evidence_target": "true CUDA OOM labels",
                    "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                    "simulator_endpoint_count": 2,
                    "simulator_probe_checked_count": 2,
                    "simulator_probe_ready_count": 2,
                    "simulator_probe_timeout_millis": 2000,
                    "simulator_readiness": "configured_not_probed",
                    "simulator_readiness_note": (
                        "endpoints are configured; export readiness is checked during live baseline calls"
                    ),
                    "simulator_claim_ready": True,
                    "simulator_claim_mode": "live-kube-scheduler-simulator-ready",
                    "simulator_claim_blocker": None,
                    "simulator_claim_next_action": "safe to use live kube-scheduler-simulator baseline evidence",
                    "operator_runbook": {
                        "step_count": 2,
                        "blocked_step_count": 1,
                        "copyable_command_count": 1,
                        "manual_step_count": 1,
                        "next_shell_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                        "steps": [
                            {
                                "priority": 1,
                                "severity": "blocked",
                                "category": "environment",
                                "artifact": "healthy Kubernetes watch/relist state",
                                "next_action": "restore Kubernetes API connectivity",
                                "command_kind": "shell",
                                "command_hint": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                            },
                            {
                                "priority": 2,
                                "severity": "warn",
                                "category": "customer-proof",
                                "next_action": "attach customer pricing",
                                "command_kind": "manual",
                                "command_hint": "attach customer pricing",
                            },
                        ],
                    },
                    "review_ready": False,
                    "claim_blockers": ["customer claim not ready"],
                },
                "live_validation_gates": [
                    {
                        "gate": "pending GPU trace",
                        "status": "blocked",
                        "next_action": "apply a deterministic GPU scenario",
                    },
                    {
                        "gate": "production mutation safety",
                        "status": "pass",
                        "next_action": "use safety posture as launch-gate evidence",
                    },
                ],
            },
            files=files,
            captured_payloads={
                "/api/scheduler/operator-status": {
                    "status": "blocked",
                    "status_label": "operator action required",
                    "primary_blocker": "production readiness blocked: kubernetes_watch",
                    "next_action": "restore Kubernetes API connectivity",
                    "debug_commands": ["kubectl config current-context"],
                    "can_shadow_demo": True,
                    "can_customer_claim": False,
                    "binding_safety": {
                        "status": "dry-run-validation",
                        "mode": "dry-run",
                        "reservation_pressure": "active",
                        "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                        "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                        "reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                        "reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
                    },
                    "vram": {
                        "model_driver_count": 2,
                        "top_driver_labels": ["layer count", "synthetic reserve pressure"],
                        "claim_safe_driver_count": 1,
                        "claim_safe_driver_labels": ["layer count"],
                        "real_model_driver_count": 1,
                        "real_top_driver_labels": ["layer count"],
                        "synthetic_driver_count": 1,
                        "synthetic_driver_labels": ["synthetic reserve pressure"],
                        "synthetic_reserve_driver": True,
                        "synthetic_headroom_driver": True,
                        "reserve_pressure_definition": RESERVE_PRESSURE_DEFINITION,
                        "driver_claim_boundary": DRIVER_CLAIM_BOUNDARY,
                    },
                    "demo_gate": {"strict_exit_code": 2},
                },
                "/api/scheduler/vram-calibration": {
                    "model_drivers": {
                        "available": True,
                        "fit": "ridge_linear_interactions",
                        "training_rows": 228,
                        "claim_boundary": DRIVER_CLAIM_BOUNDARY,
                        "top_drivers": [
                            {
                                "feature": "layers",
                                "label": "layer count",
                                "class": "model-size",
                            },
                            {
                                "feature": "reserve_extra_gib",
                                "label": "synthetic reserve pressure",
                                "class": "synthetic-pressure",
                            },
                        ],
                        "real_top_drivers": [
                            {
                                "feature": "layers",
                                "label": "layer count",
                                "class": "model-size",
                            },
                        ],
                        "synthetic_pressure_drivers": [
                            {
                                "feature": "reserve_extra_gib",
                                "label": "synthetic reserve pressure",
                                "class": "synthetic-pressure",
                            },
                        ],
                    },
                },
            },
            generated_at="2026-07-06T01:02:03+00:00",
        )
        manifest["demo_gate_result"] = {
            "present": True,
            "stage": "kss-preflight",
            "exit_code": 2,
            "failed_command": "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
            "failed_returncode": 2,
        }
        manifest["doctor_preflight"] = {
            "present": True,
            "status": "blocked",
            "exit_code": 2,
            "first_recommended_command": "scripts/kss-pool.sh status 4 12120 /tmp/cache",
            "failure_count": 1,
            "recommended_command_count": 2,
        }
        review = collector.render_review_markdown(manifest)
        self.assertIn("# ksolver SRE Evidence Bundle", review)
        self.assertIn("Packet complete: `false`", review)
        self.assertIn("Review ready: `false`", review)
        self.assertIn("VRAM admission mode: `Shadow advisory only`", review)
        self.assertIn("VRAM scheduler use: `Score and warn; do not reject pods`", review)
        self.assertIn("VRAM next evidence: `true CUDA OOM labels`", review)
        self.assertIn("Production blocker class: `kubernetes_watch`", review)
        self.assertIn("Simulator endpoints: `2`", review)
        self.assertIn("Simulator probe checked: `2`", review)
        self.assertIn("Simulator probe ready: `2`", review)
        self.assertIn("Simulator probe timeout: `2000 ms`", review)
        self.assertIn("Simulator readiness: `configured_not_probed`", review)
        self.assertIn("Simulator claim mode: `live-kube-scheduler-simulator-ready`", review)
        self.assertIn("Simulator claim ready: `true`", review)
        self.assertIn("Simulator claim blocker: `none`", review)
        self.assertIn("Operator status: `blocked`", review)
        self.assertIn("Primary blocker: `production readiness blocked: kubernetes_watch`", review)
        self.assertIn("Next action: `restore Kubernetes API connectivity`", review)
        self.assertIn("Binding safety: `dry-run-validation`", review)
        self.assertIn("Binding mode: `dry-run`", review)
        self.assertIn("Binding reservation pressure: `active`", review)
        self.assertIn(
            "Binding reservation pressure meaning: `Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.`",
            review,
        )
        self.assertIn(
            "Binding reservation pressure reason: `1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run`",
            review,
        )
        self.assertIn(
            "Binding reservation pressure action: `verify reservations are fresh and within TTL before binding the reserved placements`",
            review,
        )
        self.assertIn("Operator VRAM drivers: `2`", review)
        self.assertIn("Operator VRAM all fitted top drivers: `layer count, synthetic VRAM headroom probe`", review)
        self.assertIn("Operator VRAM claim-safe drivers: `1`", review)
        self.assertIn("Operator VRAM claim-safe top drivers: `layer count`", review)
        self.assertIn("Operator VRAM real drivers: `1`", review)
        self.assertIn("Operator VRAM real top drivers: `layer count`", review)
        self.assertIn("Operator VRAM synthetic headroom drivers: `1`", review)
        self.assertIn("Operator VRAM synthetic headroom labels: `synthetic VRAM headroom probe`", review)
        self.assertIn(f"Operator VRAM driver claim boundary: `{DRIVER_CLAIM_BOUNDARY}`", review)
        self.assertIn("Operator VRAM synthetic headroom probe driver: `true`", review)
        self.assertIn(f"Operator VRAM synthetic headroom: `{RESERVE_PRESSURE_DEFINITION}`", review)
        self.assertIn("## Demo Gate Result", review)
        self.assertIn("Stage: `kss-preflight`", review)
        self.assertIn("Exit code: `2`", review)
        self.assertIn(
            "Failed command: `scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache`",
            review,
        )
        self.assertIn("Failed returncode: `2`", review)
        self.assertIn("## Doctor Preflight", review)
        self.assertIn("Status: `blocked`", review)
        self.assertIn("Exit code: `2`", review)
        self.assertIn(
            "First recommended command: `scripts/kss-pool.sh status 4 12120 /tmp/cache`",
            review,
        )
        self.assertIn("Failures: `1`", review)
        self.assertIn("Recommended commands: `2`", review)
        self.assertIn("## Operator Runbook", review)
        self.assertIn("Steps: `2`", review)
        self.assertIn("Copyable shell commands: `1`", review)
        self.assertIn("Manual evidence steps: `1`", review)
        self.assertIn("Next shell command: `kubectl --request-timeout=10s get --raw='/readyz?verbose'`", review)
        self.assertIn("### Copyable Command Provenance", review)
        self.assertIn(
            "- `kubectl --request-timeout=10s get --raw='/readyz?verbose'` from `environment` for `restore Kubernetes API connectivity` (severity `blocked`, artifact `healthy Kubernetes watch/relist state`)",
            review,
        )
        self.assertIn("environment: `restore Kubernetes API connectivity`", review)
        self.assertIn("## Live Proof Gates", review)
        self.assertIn("Gate summary: `1 pass, 0 warn, 1 blocked`", review)
        self.assertIn("`blocked` pending GPU trace: `apply a deterministic GPU scenario`", review)
        self.assertIn("First debug command: `kubectl config current-context`", review)
        self.assertIn("VRAM model drivers: `2`", review)
        self.assertIn("VRAM claim-safe top drivers: `layer count`", review)
        self.assertIn("VRAM real model drivers: `1`", review)
        self.assertIn("VRAM real top drivers: `layer count`", review)
        self.assertIn("VRAM synthetic headroom drivers: `1`", review)
        self.assertIn("VRAM synthetic headroom labels: `synthetic VRAM headroom probe`", review)
        self.assertIn(f"VRAM driver claim boundary: `{DRIVER_CLAIM_BOUNDARY}`", review)
        self.assertIn("VRAM synthetic headroom probe driver: `true`", review)
        self.assertIn("Real top driver count: `1`", review)
        self.assertIn("Real top drivers: `layer count`", review)
        self.assertIn("Synthetic headroom driver count: `1`", review)
        self.assertIn("Synthetic headroom drivers: `synthetic VRAM headroom probe`", review)
        self.assertIn(f"Claim boundary: `{DRIVER_CLAIM_BOUNDARY}`", review)
        self.assertIn("All fitted top drivers: `layer count, synthetic VRAM headroom probe`", review)
        self.assertNotIn("synthetic reserve pressure", review)
        self.assertIn("- 1 missing live artifact(s)", review)
        self.assertIn("- production readiness blocked: kubernetes_watch", review)
        self.assertIn("`/api/scheduler/evidence-bundle` -> `api-scheduler-evidence-bundle.json`", review)
        self.assertIn("bytes `128`", review)
        self.assertIn("sha256 `aaaaaaaaaaaa`", review)

    def test_build_manifest_preserves_api_claim_blockers_without_duplication(self) -> None:
        files = {
            endpoint: {"file": collector.endpoint_filename(endpoint), "status": 200}
            for endpoint in collector.DEFAULT_ENDPOINTS
        }
        manifest = collector.build_manifest(
            base_url="http://127.0.0.1:8090",
            bundle_status=200,
            bundle={
                "ok": True,
                "summary": {
                    "customer_claim_ready": False,
                    "missing_live_artifact_count": 0,
                    "mutation_allowed": False,
                    "vram_advisory_ready": True,
                    "review_ready": False,
                    "claim_blockers": ["custom blocker"],
                },
            },
            files=files,
        )
        self.assertEqual(manifest["review_ready"], False)
        self.assertEqual(manifest["claim_blockers"], ["custom blocker", "customer claim not ready"])

    def test_exit_code_allows_complete_but_blocked_by_default(self) -> None:
        manifest = {
            "packet_complete": True,
            "review_ready": False,
        }
        self.assertEqual(
            collector.exit_code_for_manifest(manifest, require_review_ready=False),
            0,
        )
        self.assertEqual(
            collector.exit_code_for_manifest(manifest, require_review_ready=True),
            2,
        )

    def test_exit_code_fails_incomplete_packet(self) -> None:
        self.assertEqual(
            collector.exit_code_for_manifest(
                {"packet_complete": False, "review_ready": True},
                require_review_ready=False,
            ),
            1,
        )

    def test_result_from_manifest_reports_strict_mode_status(self) -> None:
        manifest = {
            "packet_complete": True,
            "review_ready": False,
            "summary": {
                "launch_status": "incomplete",
                "vram_admission_mode": "Shadow advisory only",
                "vram_scheduler_use": "Score and warn; do not reject pods",
                "vram_hard_blocker_count": 4,
                "vram_next_evidence_target": "true CUDA OOM labels",
                "vram_reserve_pressure_definition": RESERVE_PRESSURE_DEFINITION,
                "production_readiness_blocker_class": "kubernetes_watch",
                "production_readiness_last_error_class": "api_timeout",
                "production_readiness_debug_commands": [
                    "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
                ],
                "production_readiness_first_debug_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "simulator_endpoint_count": 2,
                "simulator_probe_checked_count": 2,
                "simulator_probe_ready_count": 2,
                "simulator_probe_timeout_millis": 2000,
                "simulator_readiness": "configured_not_probed",
                "simulator_readiness_note": (
                    "endpoints are configured; export readiness is checked during live baseline calls"
                ),
                "simulator_claim_ready": True,
                "simulator_claim_mode": "live-kube-scheduler-simulator-ready",
                "simulator_claim_blocker": None,
                "simulator_claim_next_action": "safe to use live kube-scheduler-simulator baseline evidence",
            },
            "missing_live_artifact_count": 2,
            "missing_live_artifact_blocked_count": 1,
            "missing_live_artifact_warn_count": 1,
            "missing_live_artifact_rows": [
                {"artifact": "latest shadow trace", "severity": "blocked"},
                {"artifact": "customer pricing source", "severity": "warn"},
            ],
            "claim_blockers": ["customer claim not ready"],
            "primary_claim_blocker": "production readiness blocked: kubernetes_watch",
            "primary_claim_blocker_next_action": "restore Kubernetes API connectivity",
            "operator_status": {
                "status": "blocked",
                "debug_commands": ["kubectl config current-context"],
                "production_readiness": {
                    "blocker_class": "kubernetes_watch",
                    "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                },
                "binding_safety": {
                    "status": "dry-run-validation",
                    "mode": "dry-run",
                    "reservation_pressure": "active",
                    "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                    "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                    "reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                    "reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
                },
                "vram": {
                    "model_driver_count": 2,
                    "top_driver_labels": ["layer count", "synthetic reserve pressure"],
                    "claim_safe_driver_count": 1,
                    "claim_safe_driver_labels": ["layer count"],
                    "real_model_driver_count": 1,
                    "real_top_driver_labels": ["layer count"],
                    "synthetic_driver_count": 1,
                    "synthetic_driver_labels": ["synthetic reserve pressure"],
                    "synthetic_reserve_driver": True,
                    "synthetic_headroom_driver": True,
                },
            },
            "vram_model_drivers": {
                "available": True,
                "top_driver_count": 2,
                "synthetic_reserve_driver": True,
                "synthetic_headroom_driver": True,
                "top_driver_labels": ["layer count", "synthetic reserve pressure"],
                "claim_safe_driver_count": 1,
                "claim_safe_driver_labels": ["layer count"],
                "real_top_driver_count": 1,
                "real_top_driver_labels": ["layer count"],
                "synthetic_pressure_driver_count": 1,
                "synthetic_pressure_driver_labels": ["synthetic reserve pressure"],
                "claim_boundary": DRIVER_CLAIM_BOUNDARY,
            },
            "files": {"/api/scheduler/evidence-bundle": {"status": 200}},
        }
        result = collector.result_from_manifest(
            manifest,
            output_dir=pathlib.Path("/tmp/bundle"),
            require_review_ready=True,
        )
        self.assertEqual(result["ok"], False)
        self.assertEqual(result["exit_code"], 2)
        self.assertEqual(result["packet_complete"], True)
        self.assertEqual(result["review_ready"], False)
        self.assertEqual(result["require_review_ready"], True)
        self.assertEqual(result["primary_claim_blocker"], "production readiness blocked: kubernetes_watch")
        self.assertEqual(result["primary_claim_blocker_next_action"], "restore Kubernetes API connectivity")
        self.assertEqual(result["missing_live_artifact_count"], manifest["missing_live_artifact_count"])
        self.assertEqual(result["missing_live_artifact_blocked_count"], manifest["missing_live_artifact_blocked_count"])
        self.assertEqual(result["missing_live_artifact_warn_count"], manifest["missing_live_artifact_warn_count"])
        self.assertEqual(result["operator_status"]["status"], "blocked")
        self.assertEqual(result["operator_binding_status"], "dry-run-validation")
        self.assertEqual(result["operator_reservation_pressure"], "active")
        self.assertIn("pending or reserved GPU capacity", result["operator_reservation_pressure_description"])
        self.assertIn("unrelated to CUDA", result["operator_reservation_pressure_scope"])
        self.assertIn("hold 4 GPU", result["operator_reservation_pressure_reason"])
        self.assertEqual(
            result["operator_status"]["production_readiness"]["debug_commands"][0],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(result["vram_reserve_pressure_definition"], RESERVE_PRESSURE_DEFINITION)
        self.assertEqual(result["operator_status"]["vram"]["model_driver_count"], 2)
        self.assertEqual(result["vram_model_drivers"]["top_driver_count"], 2)
        self.assertEqual(result["vram_model_drivers"]["synthetic_reserve_driver"], True)
        self.assertEqual(result["vram_model_drivers"]["synthetic_headroom_driver"], True)
        self.assertEqual(result["vram_model_driver_count"], 2)
        self.assertEqual(result["vram_top_driver_labels"], ["layer count", "synthetic reserve pressure"])
        self.assertEqual(
            result["vram_display_top_driver_labels"],
            ["layer count", "synthetic VRAM headroom probe"],
        )
        self.assertEqual(result["vram_claim_safe_driver_count"], 1)
        self.assertEqual(result["vram_claim_safe_driver_labels"], ["layer count"])
        self.assertEqual(result["vram_display_claim_safe_driver_labels"], ["layer count"])
        self.assertEqual(result["vram_real_model_driver_count"], 1)
        self.assertEqual(result["vram_real_top_driver_labels"], ["layer count"])
        self.assertEqual(result["vram_display_real_top_driver_labels"], ["layer count"])
        self.assertEqual(result["vram_synthetic_driver_count"], 1)
        self.assertEqual(result["vram_synthetic_driver_labels"], ["synthetic reserve pressure"])
        self.assertEqual(
            result["vram_display_synthetic_driver_labels"],
            ["synthetic VRAM headroom probe"],
        )
        self.assertEqual(result["vram_synthetic_reserve_driver"], True)
        self.assertEqual(result["vram_synthetic_headroom_driver"], True)
        self.assertEqual(result["endpoint_file_count"], 1)
        self.assertEqual(result["review_artifact_count"], 2)
        self.assertEqual(result["file_count"], 3)
        self.assertEqual(result["vram_admission_mode"], "Shadow advisory only")
        self.assertEqual(result["vram_scheduler_use"], "Score and warn; do not reject pods")
        self.assertEqual(result["vram_hard_blocker_count"], 4)
        self.assertEqual(result["vram_next_evidence_target"], "true CUDA OOM labels")
        self.assertEqual(result["production_readiness_blocker_class"], "kubernetes_watch")
        self.assertEqual(
            result["production_readiness_debug_commands"],
            ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
        )
        self.assertEqual(
            result["production_readiness_first_debug_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(result["simulator_endpoint_count"], 2)
        self.assertEqual(result["simulator_probe_checked_count"], 2)
        self.assertEqual(result["simulator_probe_ready_count"], 2)
        self.assertEqual(result["simulator_probe_timeout_millis"], 2000)
        self.assertEqual(result["simulator_readiness"], "configured_not_probed")
        self.assertEqual(result["simulator_claim_mode"], "live-kube-scheduler-simulator-ready")
        self.assertEqual(result["simulator_claim_ready"], True)
        self.assertEqual(result["simulator_claim_blocker"], None)


if __name__ == "__main__":
    unittest.main()
