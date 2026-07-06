#!/usr/bin/env python3
"""Unit tests for verify-evidence-bundle.py helpers."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import tempfile
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "verify_evidence_bundle", ROOT / "verify-evidence-bundle.py"
)
assert SPEC and SPEC.loader
verifier = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verifier)

RESERVE_PRESSURE_DEFINITION = (
    "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM padding "
    "to stress scheduler headroom; this is a pressure-test signal, not organic model demand."
)
DRIVER_CLAIM_BOUNDARY = (
    "Use real_top_drivers for model-memory claims. synthetic_pressure_drivers are "
    "stress-test probes only and must not be presented as organic workload predictors."
)
BINDING_SAFETY = {
    "status": "dry-run-validation",
    "mode": "dry-run",
    "reservation_pressure": "active",
    "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
    "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
    "reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
    "reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
}


def default_action_items() -> list[dict[str, object]]:
    return [
        {
            "priority": 1,
            "category": "environment",
            "severity": "blocked",
            "blocked": 1,
            "warn": 0,
            "artifact": "healthy Kubernetes watch/relist state",
            "next_action": "restore Kubernetes API connectivity",
            "command_hint": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
            "command_hints": [
                "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "kubectl config current-context",
                "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
                "kubectl --request-timeout=10s get nodes",
            ],
            "command_kind": "shell",
            "copyable": True,
        },
        {
            "priority": 2,
            "category": "customer-proof",
            "severity": "warn",
            "blocked": 0,
            "warn": 1,
            "artifact": "customer pricing source",
            "next_action": "attach customer pricing",
            "command_hint": "attach customer pricing",
            "command_kind": "manual",
            "copyable": False,
        },
    ]


def default_summary() -> dict[str, object]:
    action_items = default_action_items()
    return {
        "collection_command_count": 3,
        "vram_advisory_ready": True,
        "vram_hard_admission_ready": False,
        "vram_admission_mode": "Shadow advisory only",
        "vram_scheduler_use": "Score and warn; do not reject pods",
        "vram_hard_blocker_count": 4,
        "vram_next_evidence_target": "true CUDA OOM labels",
        "vram_model_driver_count": 2,
        "vram_driver_impact_basis": "coefficient_x_feature_std",
        "vram_top_driver_descriptions": ["model depth", "synthetic VRAM headroom probe allocation"],
        "vram_claim_safe_driver_descriptions": ["model depth"],
        "vram_real_top_driver_descriptions": ["model depth"],
        "vram_synthetic_driver_descriptions": ["synthetic VRAM headroom probe allocation"],
        "vram_top_organic_driver_descriptions": ["model depth"],
        "vram_top_driver_group_impacts": [
            {"group": "architecture", "abs_impact_mib_per_std_sum": 2202.2},
            {"group": "synthetic headroom", "abs_impact_mib_per_std_sum": 1953.7},
        ],
        "vram_top_driver_labels": ["layer count", "synthetic reserve pressure"],
        "vram_display_top_driver_labels": ["layer count", "synthetic VRAM headroom probe"],
        "vram_claim_safe_driver_count": 1,
        "vram_claim_safe_driver_labels": ["layer count"],
        "vram_display_claim_safe_driver_labels": ["layer count"],
        "vram_real_model_driver_count": 1,
        "vram_real_top_driver_labels": ["layer count"],
        "vram_display_real_top_driver_labels": ["layer count"],
        "vram_synthetic_driver_count": 1,
        "vram_synthetic_driver_labels": ["synthetic reserve pressure"],
        "vram_display_synthetic_driver_labels": ["synthetic VRAM headroom probe"],
        "vram_synthetic_reserve_driver": True,
        "vram_synthetic_headroom_driver": True,
        "vram_reserve_pressure_definition": RESERVE_PRESSURE_DEFINITION,
        "vram_driver_claim_boundary": DRIVER_CLAIM_BOUNDARY,
        "vram_investment_demo_rows": 6,
        "vram_investment_oom_risk_reduction_pods": 3,
        "vram_investment_high_vram_nodes_preserved": 1,
        "vram_investment_advisory_rows": 1,
        "vram_investment_average_baseline_oom_risk_percent": 68,
        "vram_investment_average_ksolver_oom_risk_percent": 17,
        "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
        "production_readiness_debug_commands": [
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
        ],
        "production_readiness_first_debug_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        "operator_binding_status": BINDING_SAFETY["status"],
        "operator_reservation_pressure": BINDING_SAFETY["reservation_pressure"],
        "operator_reservation_pressure_description": BINDING_SAFETY[
            "reservation_pressure_description"
        ],
        "operator_reservation_pressure_scope": BINDING_SAFETY["reservation_pressure_scope"],
        "operator_reservation_pressure_reason": BINDING_SAFETY[
            "reservation_pressure_reason"
        ],
        "operator_reservation_pressure_next_action": BINDING_SAFETY[
            "reservation_pressure_next_action"
        ],
        "simulator_endpoint_count": 1,
        "simulator_probe_checked_count": 1,
        "simulator_probe_ready_count": 1,
        "simulator_probe_timeout_millis": 2000,
        "simulator_readiness": "configured_not_probed",
        "simulator_readiness_note": (
            "endpoints are configured; export readiness is checked during live baseline calls"
        ),
        "simulator_claim_ready": True,
        "simulator_claim_mode": "live-kube-scheduler-simulator-ready",
        "simulator_claim_blocker": None,
        "simulator_claim_next_action": "safe to use live kube-scheduler-simulator baseline evidence",
        "live_validation_gate_count": 0,
        "live_validation_pass_count": 0,
        "live_validation_warn_count": 0,
        "live_validation_blocked_count": 0,
        "missing_live_artifact_count": 2,
        "missing_live_artifact_blocked_count": 1,
        "missing_live_artifact_warn_count": 1,
        "missing_live_artifact_action_items": action_items,
        "operator_runbook": verifier.operator_action_runbook(action_items),
        "review_ready": False,
        "demo_gate_strict_exit_code": 2,
    }


def write_bundle(root: pathlib.Path, *, content: str | None = None, sha: str | None = None) -> None:
    capture = root / "api-scheduler-evidence-bundle.json"
    if content is None:
        content = json.dumps({"ok": True, "summary": default_summary()}) + "\n"
    capture.write_text(content, encoding="utf-8")
    operator_capture = root / "api-scheduler-operator-status.json"
    operator_payload = {
        "ok": True,
        "dry_run": True,
        "status": "blocked",
        "primary_blocker": "production readiness blocked: kubernetes_watch",
        "next_action": "restore Kubernetes API connectivity",
        "debug_commands": ["kubectl config current-context"],
        "production_readiness": {
            "blocker_class": "kubernetes_watch",
            "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
        },
        "binding_safety": BINDING_SAFETY,
        "vram": {
            "model_driver_count": 2,
            "top_driver_labels": ["layer count", "synthetic reserve pressure"],
            "display_top_driver_labels": ["layer count", "synthetic VRAM headroom probe"],
            "claim_safe_driver_count": 1,
            "claim_safe_driver_labels": ["layer count"],
            "display_claim_safe_driver_labels": ["layer count"],
            "real_model_driver_count": 1,
            "real_top_driver_labels": ["layer count"],
            "display_real_top_driver_labels": ["layer count"],
            "synthetic_driver_count": 1,
            "synthetic_driver_labels": ["synthetic reserve pressure"],
            "display_synthetic_driver_labels": ["synthetic VRAM headroom probe"],
            "synthetic_reserve_driver": True,
            "synthetic_headroom_driver": True,
            "reserve_pressure_definition": RESERVE_PRESSURE_DEFINITION,
            "driver_claim_boundary": DRIVER_CLAIM_BOUNDARY,
            "investment_demo_rows": 6,
            "investment_oom_risk_reduction_pods": 3,
            "investment_high_vram_nodes_preserved": 1,
            "investment_advisory_rows": 1,
            "investment_average_baseline_oom_risk_percent": 68,
            "investment_average_ksolver_oom_risk_percent": 17,
        },
        "demo_gate": {"strict_exit_code": 2},
        "action_items": default_action_items(),
        "operator_runbook": verifier.operator_action_runbook(default_action_items()),
    }
    operator_capture.write_text(json.dumps(operator_payload) + "\n", encoding="utf-8")
    vram_capture = root / "api-scheduler-vram-calibration.json"
    vram_payload = {
        "available": True,
        "model_drivers": {
            "available": True,
            "fit": "ridge_linear_interactions",
            "training_rows": 228,
            "impact_basis": "coefficient_x_feature_std",
            "group_impacts": [
                {"group": "architecture", "abs_impact_mib_per_std_sum": 2202.2},
                {"group": "synthetic headroom", "abs_impact_mib_per_std_sum": 1953.7},
            ],
            "top_organic_driver_descriptions": ["model depth"],
            "claim_boundary": DRIVER_CLAIM_BOUNDARY,
            "top_drivers": [
                {
                    "feature": "layers",
                    "label": "layer count",
                    "description": "model depth",
                    "class": "model-size",
                    "mean_abs_contribution_mib": 2202.2,
                },
                {
                    "feature": "reserve_extra_gib",
                    "label": "synthetic reserve pressure",
                    "description": "synthetic VRAM headroom probe allocation",
                    "class": "synthetic-pressure",
                    "mean_abs_contribution_mib": 1953.7,
                },
            ],
            "real_top_drivers": [
                {
                    "feature": "layers",
                    "label": "layer count",
                    "description": "model depth",
                    "class": "model-size",
                    "mean_abs_contribution_mib": 2202.2,
                },
            ],
            "synthetic_pressure_drivers": [
                {
                    "feature": "reserve_extra_gib",
                    "label": "synthetic reserve pressure",
                    "description": "synthetic VRAM headroom probe allocation",
                    "class": "synthetic-pressure",
                    "mean_abs_contribution_mib": 1953.7,
                },
            ],
        },
    }
    vram_capture.write_text(json.dumps(vram_payload) + "\n", encoding="utf-8")
    root.joinpath("review.md").write_text(
        "\n".join(
            [
                "# ksolver SRE Evidence Bundle",
                "",
                "- VRAM admission mode: `Shadow advisory only`",
                "- VRAM scheduler use: `Score and warn; do not reject pods`",
                "- VRAM next evidence: `true CUDA OOM labels`",
                "- VRAM model drivers: `2`",
                "- VRAM claim-safe drivers: `1`",
                "- VRAM claim-safe top drivers: `layer count`",
                "- VRAM real model drivers: `1`",
                "- VRAM real top drivers: `layer count`",
                "- VRAM synthetic headroom drivers: `1`",
                "- VRAM synthetic headroom labels: `synthetic VRAM headroom probe`",
                f"- VRAM driver claim boundary: `{DRIVER_CLAIM_BOUNDARY}`",
                "- VRAM synthetic headroom probe driver: `true`",
                "- Production blocker class: `kubernetes_watch`",
                "- Production last error class: `api_timeout`",
                "- Simulator probe checked: `1`",
                "- Simulator probe ready: `1`",
                "- Simulator readiness: `configured_not_probed`",
                "- Simulator claim mode: `live-kube-scheduler-simulator-ready`",
                "- Simulator claim ready: `true`",
                "- Simulator claim blocker: `none`",
                "- Simulator claim next action: `safe to use live kube-scheduler-simulator baseline evidence`",
                "",
                "## Operator Status",
                "",
                "- Operator status: `blocked`",
                "- Primary blocker: `production readiness blocked: kubernetes_watch`",
                "- Next action: `restore Kubernetes API connectivity`",
                "- Binding safety: `dry-run-validation`",
                "- Binding mode: `dry-run`",
                "- Binding reservation pressure: `active`",
                f"- Binding reservation pressure meaning: `{BINDING_SAFETY['reservation_pressure_description']}`",
                f"- Binding reservation pressure scope: `{BINDING_SAFETY['reservation_pressure_scope']}`",
                f"- Binding reservation pressure reason: `{BINDING_SAFETY['reservation_pressure_reason']}`",
                f"- Binding reservation pressure action: `{BINDING_SAFETY['reservation_pressure_next_action']}`",
                "- Operator VRAM drivers: `2`",
                "- Operator VRAM all fitted top drivers: `layer count, synthetic VRAM headroom probe`",
                "- Operator VRAM claim-safe drivers: `1`",
                "- Operator VRAM claim-safe top drivers: `layer count`",
                "- Operator VRAM real drivers: `1`",
                "- Operator VRAM real top drivers: `layer count`",
                "- Operator VRAM synthetic headroom drivers: `1`",
                "- Operator VRAM synthetic headroom labels: `synthetic VRAM headroom probe`",
                f"- Operator VRAM driver claim boundary: `{DRIVER_CLAIM_BOUNDARY}`",
                "- Operator VRAM synthetic headroom probe driver: `true`",
                f"- Operator VRAM synthetic headroom: `{RESERVE_PRESSURE_DEFINITION}`",
                "- Operator VRAM investment demo: `6 rows, 3 OOM-risk pods reduced, 1 high-VRAM preserved`",
                "",
                "## Operator Runbook",
                "",
                "- Steps: `2`",
                "- Blocked steps: `1`",
                "- Copyable shell commands: `4`",
                "- Manual evidence steps: `1`",
                "- Next shell command: `kubectl --request-timeout=10s get --raw='/readyz?verbose'`",
                "- First debug command: `kubectl config current-context`",
                "- Production first debug command: `kubectl --request-timeout=10s get --raw='/readyz?verbose'`",
                "",
                "### Copyable Command Provenance",
                "",
                "- `kubectl --request-timeout=10s get --raw='/readyz?verbose'` from `environment` for `restore Kubernetes API connectivity` (severity `blocked`, artifact `healthy Kubernetes watch/relist state`)",
                "- `kubectl config current-context` from `environment` for `restore Kubernetes API connectivity` (severity `blocked`, artifact `healthy Kubernetes watch/relist state`)",
                "- `kubectl --request-timeout=10s auth can-i list pods --all-namespaces` from `environment` for `restore Kubernetes API connectivity` (severity `blocked`, artifact `healthy Kubernetes watch/relist state`)",
                "- `kubectl --request-timeout=10s get nodes` from `environment` for `restore Kubernetes API connectivity` (severity `blocked`, artifact `healthy Kubernetes watch/relist state`)",
                "",
                "## Missing Live Artifacts",
                "",
                "- Gap summary: `1 blocked, 1 warn`",
                "- `blocked` latest shadow trace: `live-trace` via `pending GPU trace`; next `apply a deterministic GPU scenario`",
                "- `warn` customer pricing source: `customer-proof` via `ROI pricing evidence`; next `attach customer pricing`",
                "",
                "## VRAM Model Drivers",
                "",
                "- Available: `true`",
                "- Fit: `ridge_linear_interactions`",
                "- Training rows: `228`",
                "- Impact basis: `coefficient_x_feature_std`",
                "- Top impact group: `architecture`",
                "- Top driver count: `2`",
                "- Claim-safe driver count: `1`",
                "- Claim-safe drivers: `layer count`",
                "- Claim-safe driver meaning: `model depth`",
                "- Real top driver count: `1`",
                "- Real top drivers: `layer count`",
                "- Synthetic headroom driver count: `1`",
                "- Synthetic headroom drivers: `synthetic VRAM headroom probe`",
                f"- Claim boundary: `{DRIVER_CLAIM_BOUNDARY}`",
                "- Synthetic headroom probe driver: `true`",
                "- All fitted top drivers: `layer count, synthetic VRAM headroom probe`",
                "- All fitted top driver meaning: `model depth, synthetic VRAM headroom probe allocation`",
                "- Organic driver descriptions: `model depth`",
                "",
            ]
        ),
        encoding="utf-8",
    )
    digest = sha or verifier.sha256_file(capture)
    operator_digest = verifier.sha256_file(operator_capture)
    vram_digest = verifier.sha256_file(vram_capture)
    root.joinpath("manifest.json").write_text(
        json.dumps(
            {
                "packet_complete": True,
                "review_ready": False,
                "claim_blockers": [
                    "customer claim not ready",
                    "production readiness blocked: kubernetes_watch",
                ],
                "primary_claim_blocker": "production readiness blocked: kubernetes_watch",
                "primary_claim_blocker_next_action": "restore Kubernetes API connectivity",
                "missing_live_artifact_count": 2,
                "missing_live_artifact_blocked_count": 1,
                "missing_live_artifact_warn_count": 1,
                "missing_live_artifact_rows": [
                    {
                        "artifact": "latest shadow trace",
                        "category": "live-trace",
                        "severity": "blocked",
                        "proof_gate": "pending GPU trace",
                        "next_action": "apply a deterministic GPU scenario",
                    },
                    {
                        "artifact": "customer pricing source",
                        "category": "customer-proof",
                        "severity": "warn",
                        "proof_gate": "ROI pricing evidence",
                        "next_action": "attach customer pricing",
                    },
                ],
                "missing_live_artifact_action_items": default_action_items(),
                "operator_runbook": verifier.operator_action_runbook(default_action_items()),
                "operator_status": {
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
                    "binding_safety": BINDING_SAFETY,
                    "vram": {
                        "model_driver_count": 2,
                        "top_driver_labels": ["layer count", "synthetic reserve pressure"],
                        "display_top_driver_labels": ["layer count", "synthetic VRAM headroom probe"],
                        "claim_safe_driver_count": 1,
                        "claim_safe_driver_labels": ["layer count"],
                        "display_claim_safe_driver_labels": ["layer count"],
                        "real_model_driver_count": 1,
                        "real_top_driver_labels": ["layer count"],
                        "display_real_top_driver_labels": ["layer count"],
                        "synthetic_driver_count": 1,
                        "synthetic_driver_labels": ["synthetic reserve pressure"],
                        "display_synthetic_driver_labels": ["synthetic VRAM headroom probe"],
                        "synthetic_reserve_driver": True,
                        "synthetic_headroom_driver": True,
                        "reserve_pressure_definition": RESERVE_PRESSURE_DEFINITION,
                        "driver_claim_boundary": DRIVER_CLAIM_BOUNDARY,
                        "investment_demo_rows": 6,
                        "investment_oom_risk_reduction_pods": 3,
                        "investment_high_vram_nodes_preserved": 1,
                        "investment_advisory_rows": 1,
                        "investment_average_baseline_oom_risk_percent": 68,
                        "investment_average_ksolver_oom_risk_percent": 17,
                    },
                    "demo_gate": {"strict_exit_code": 2},
                    "action_items": default_action_items(),
                    "operator_runbook": verifier.operator_action_runbook(default_action_items()),
                },
                "vram_model_drivers": {
                    "available": True,
                    "fit": "ridge_linear_interactions",
                    "training_rows": 228,
                    "impact_basis": "coefficient_x_feature_std",
                    "group_impacts": [
                        {"group": "architecture", "abs_impact_mib_per_std_sum": 2202.2},
                        {"group": "synthetic headroom", "abs_impact_mib_per_std_sum": 1953.7},
                    ],
                    "top_organic_driver_descriptions": ["model depth"],
                    "top_driver_count": 2,
                    "synthetic_reserve_driver": True,
                    "synthetic_headroom_driver": True,
                    "top_driver_labels": ["layer count", "synthetic reserve pressure"],
                    "top_driver_descriptions": ["model depth", "synthetic VRAM headroom probe allocation"],
                    "display_top_driver_labels": ["layer count", "synthetic VRAM headroom probe"],
                    "claim_safe_driver_count": 1,
                    "claim_safe_driver_labels": ["layer count"],
                    "claim_safe_driver_descriptions": ["model depth"],
                    "display_claim_safe_driver_labels": ["layer count"],
                    "real_top_driver_count": 1,
                    "real_top_driver_labels": ["layer count"],
                    "real_top_driver_descriptions": ["model depth"],
                    "display_real_top_driver_labels": ["layer count"],
                    "synthetic_pressure_driver_count": 1,
                    "synthetic_pressure_driver_labels": ["synthetic reserve pressure"],
                    "synthetic_pressure_driver_descriptions": ["synthetic VRAM headroom probe allocation"],
                    "display_synthetic_pressure_driver_labels": ["synthetic VRAM headroom probe"],
                    "claim_boundary": DRIVER_CLAIM_BOUNDARY,
                },
                "summary": default_summary(),
                "files": {
                    "/api/scheduler/evidence-bundle": {
                        "file": capture.name,
                        "status": 200,
                        "bytes": capture.stat().st_size,
                        "sha256": digest,
                    },
                    "/api/scheduler/operator-status": {
                        "file": operator_capture.name,
                        "status": 200,
                        "bytes": operator_capture.stat().st_size,
                        "sha256": operator_digest,
                    },
                    "/api/scheduler/vram-calibration": {
                        "file": vram_capture.name,
                        "status": 200,
                        "bytes": vram_capture.stat().st_size,
                        "sha256": vram_digest,
                    }
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )


class VerifyEvidenceBundleTests(unittest.TestCase):
    def test_human_facing_source_uses_synthetic_headroom_wording(self) -> None:
        source = (ROOT / "verify-evidence-bundle.py").read_text(encoding="utf-8")
        self.assertNotIn("synthetic reserve pressure", source)
        self.assertNotIn("synthetic transformer reserve pressure", source)
        self.assertIn("synthetic headroom", source)

    def test_markdown_section_extracts_exact_heading_body(self) -> None:
        text = (
            "# title\n"
            "\n"
            "## Demo Gate Result\n"
            "\n"
            "- Stage: `verify`\n"
            "\n"
            "## Captured Files\n"
            "\n"
            "- traces.json\n"
        )

        section = verifier.markdown_section(text, "Demo Gate Result")

        self.assertIsNotNone(section)
        self.assertIn("Stage: `verify`", section)
        self.assertNotIn("Captured Files", section)

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
        items = verifier.missing_artifact_action_items(rows)
        runbook = verifier.operator_action_runbook(items)

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
        self.assertIn("kubectl --request-timeout=10s get nodes", runbook["copyable_commands"])
        self.assertEqual(
            runbook["copyable_command_rows"][0]["command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(runbook["copyable_command_rows"][0]["category"], "environment")
        self.assertEqual(
            runbook["copyable_command_rows"][0]["artifact"],
            "healthy Kubernetes watch-relist state",
        )

    def test_verify_valid_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], True)
        self.assertEqual(result["verified_files"], 3)
        self.assertEqual(result["review_ready"], False)
        self.assertEqual(result["vram_admission_mode"], "Shadow advisory only")
        self.assertEqual(result["vram_synthetic_headroom_driver"], True)
        self.assertEqual(result["production_readiness_blocker_class"], "kubernetes_watch")
        self.assertEqual(result["simulator_endpoint_count"], 1)
        self.assertEqual(result["simulator_probe_checked_count"], 1)
        self.assertEqual(result["simulator_probe_ready_count"], 1)
        self.assertEqual(result["simulator_probe_timeout_millis"], 2000)
        self.assertEqual(result["simulator_readiness"], "configured_not_probed")
        self.assertEqual(result["simulator_claim_mode"], "live-kube-scheduler-simulator-ready")
        self.assertEqual(result["simulator_claim_ready"], True)
        self.assertEqual(result["simulator_claim_blocker"], None)
        self.assertEqual(result["missing_live_artifact_count"], 2)
        self.assertEqual(result["missing_live_artifact_blocked_count"], 1)
        self.assertEqual(result["missing_live_artifact_warn_count"], 1)
        self.assertEqual(result["operator_status"]["binding_safety"]["reservation_pressure_scope"], BINDING_SAFETY["reservation_pressure_scope"])

    def test_missing_manifest_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            result = verifier.verify_bundle(pathlib.Path(tmp))
        self.assertEqual(result["ok"], False)
        self.assertIn("missing manifest.json", result["errors"])

    def test_sha_mismatch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root, sha="0" * 64)
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(any("sha256 mismatch" in error for error in result["errors"]))

    def test_missing_review_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            root.joinpath("review.md").unlink()
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("missing review.md", result["errors"])

    def test_review_must_include_vram_posture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            root.joinpath("review.md").write_text("# ksolver SRE Evidence Bundle\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("review.md missing VRAM admission mode", result["errors"])
        self.assertIn("review.md missing VRAM next evidence", result["errors"])

    def test_invalid_captured_json_fails_even_when_hash_matches(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root, content="{not json}\n")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(any("not valid JSON" in error for error in result["errors"]))

    def test_summary_command_count_mismatch_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["summary"]["collection_command_count"] = 4
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(any("collection_command_count mismatch" in error for error in result["errors"]))

    def test_missing_vram_posture_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            del manifest["summary"]["vram_admission_mode"]
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("summary missing vram_admission_mode", result["errors"])

    def test_missing_simulator_readiness_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            del manifest["summary"]["simulator_readiness"]
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("summary missing simulator_readiness", result["errors"])

    def test_blocked_hard_admission_requires_positive_blocker_count(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["summary"]["vram_hard_blocker_count"] = 0
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(any("positive VRAM hard blocker count" in error for error in result["errors"]))

    def test_manifest_summary_must_match_captured_evidence_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["summary"]["vram_next_evidence_target"] = "cross-SKU calibration"
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(any("vram_next_evidence_target mismatch" in error for error in result["errors"]))

    def test_manifest_simulator_summary_must_match_captured_evidence_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["summary"]["simulator_readiness_note"] = "stale simulator note"
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(any("simulator_readiness_note mismatch" in error for error in result["errors"]))

    def test_manifest_production_blocker_class_must_match_captured_evidence_bundle(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["summary"]["production_readiness_blocker_class"] = "solver"
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(
            any("production_readiness_blocker_class mismatch" in error for error in result["errors"])
        )

    def test_production_blocker_class_requires_claim_blocker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["claim_blockers"] = ["customer claim not ready"]
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("claim blockers missing production readiness blocker", result["errors"])

    def test_captured_evidence_bundle_must_have_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root, content='{"ok": true}\n')
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("/api/scheduler/evidence-bundle: captured summary missing", result["errors"])

    def test_captured_operator_status_is_required(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            operator_row = manifest["files"].pop("/api/scheduler/operator-status")
            root.joinpath(operator_row["file"]).unlink()
            manifest["summary"]["collection_command_count"] = 1
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("/api/scheduler/operator-status: captured artifact missing", result["errors"])

    def test_captured_operator_status_must_match_primary_action(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            operator_path = root / "api-scheduler-operator-status.json"
            payload = json.loads(operator_path.read_text(encoding="utf-8"))
            payload["next_action"] = "do something else"
            operator_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            row = manifest["files"]["/api/scheduler/operator-status"]
            row["bytes"] = operator_path.stat().st_size
            row["sha256"] = verifier.sha256_file(operator_path)
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertTrue(any("next action mismatch" in error for error in result["errors"]))

    def test_captured_operator_status_vram_drivers_must_match_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            operator_path = root / "api-scheduler-operator-status.json"
            payload = json.loads(operator_path.read_text(encoding="utf-8"))
            payload["vram"]["top_driver_labels"] = ["stale driver"]
            operator_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            row = manifest["files"]["/api/scheduler/operator-status"]
            row["bytes"] = operator_path.stat().st_size
            row["sha256"] = verifier.sha256_file(operator_path)
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("/api/scheduler/operator-status: VRAM top driver labels mismatch", result["errors"])

    def test_manifest_operator_status_vram_must_match_captured_operator_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["operator_status"]["vram"]["top_driver_labels"] = ["stale driver"]
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("manifest operator-status VRAM top driver labels mismatch", result["errors"])

    def test_manifest_operator_status_production_readiness_must_match_capture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["operator_status"]["production_readiness"]["debug_commands"] = [
                "kubectl config current-context"
            ]
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn(
            "manifest operator-status production readiness mismatch",
            result["errors"],
        )
        self.assertIn(
            "manifest operator-status production debug commands mismatch",
            result["errors"],
        )

    def test_manifest_operator_status_binding_safety_must_match_capture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["operator_status"]["binding_safety"]["reservation_pressure"] = "none"
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("manifest operator-status binding safety mismatch", result["errors"])

    def test_summary_operator_reservation_pressure_scope_must_match_binding(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["summary"]["operator_reservation_pressure_scope"] = "framework VRAM"
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn(
            "summary operator_reservation_pressure_scope mismatch manifest='framework VRAM' captured='Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.'",
            result["errors"],
        )
        self.assertIn(
            "summary operator_reservation_pressure_scope mismatch operator-status binding_safety.reservation_pressure_scope",
            result["errors"],
        )

    def test_production_blocker_runbook_must_match_first_production_debug_command(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            stale_items = list(manifest["missing_live_artifact_action_items"])
            stale_items[0] = dict(stale_items[0])
            stale_items[0]["command_hint"] = "kubectl config current-context"
            stale_items[0]["command_hints"] = ["kubectl config current-context"]
            manifest["missing_live_artifact_action_items"] = stale_items
            manifest["operator_runbook"] = verifier.operator_action_runbook(stale_items)
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn(
            "operator runbook first shell command does not match production readiness first debug command",
            result["errors"],
        )

    def test_valid_doctor_preflight_artifact_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            first_command = "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
            root.joinpath("doctor-preflight.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "status": "degraded",
                        "exit_code": 0,
                        "failures": [],
                        "first_recommended_command": first_command,
                        "recommended_commands": [
                            {
                                "category": "kubernetes-readiness",
                                "severity": "blocked",
                                "command": first_command,
                                "reason": "shadow /readyz is blocked",
                            }
                        ],
                        "api_endpoint_failures": [
                            {
                                "category": "shadow-api",
                                "severity": "blocked",
                                "endpoint": "/api/scheduler/evidence-bundle",
                                "command": "curl -fsS http://shadow/api/scheduler/evidence-bundle",
                                "reason": "evidence-bundle endpoint did not return a valid JSON object",
                            }
                        ],
                        "simulator_claim_ready": True,
                        "kss_ready_count": 1,
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["doctor_preflight"] = {
                "present": True,
                "ok": True,
                "status": "degraded",
                "exit_code": 0,
                "first_recommended_command": first_command,
                "failure_count": 0,
                "recommended_command_count": 1,
                "api_endpoint_failure_count": 1,
                "first_api_endpoint_failure": {
                    "category": "shadow-api",
                    "severity": "blocked",
                    "endpoint": "/api/scheduler/evidence-bundle",
                    "command": "curl -fsS http://shadow/api/scheduler/evidence-bundle",
                    "reason": "evidence-bundle endpoint did not return a valid JSON object",
                },
            }
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review += (
                "\n## Doctor Preflight\n\n"
                "- Status: `degraded`\n"
                "- Exit code: `0`\n"
                f"- First recommended command: `{first_command}`\n"
                "- Failures: `0`\n"
                "- Recommended commands: `1`\n"
                "- API endpoint failures: `1`\n"
                "- First API endpoint failure: `/api/scheduler/evidence-bundle`\n"
            )
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertTrue(result["ok"])
        self.assertTrue(result["doctor_preflight_present"])
        self.assertEqual(result["doctor_status"], "degraded")
        self.assertEqual(
            result["doctor_first_recommended_command"],
            first_command,
        )
        self.assertEqual(
            result["doctor_api_endpoint_failures"][0]["endpoint"],
            "/api/scheduler/evidence-bundle",
        )

    def test_manifest_doctor_preflight_must_match_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            root.joinpath("doctor-preflight.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "status": "degraded",
                        "exit_code": 0,
                        "failures": [],
                        "recommended_commands": [],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["doctor_preflight"] = {"present": True, "status": "stale"}
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertFalse(result["ok"])
        self.assertIn("manifest doctor preflight mismatch", result["errors"])

    def test_review_doctor_preflight_must_match_section(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            first_command = "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
            root.joinpath("doctor-preflight.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "status": "degraded",
                        "exit_code": 0,
                        "failures": [],
                        "first_recommended_command": first_command,
                        "recommended_commands": [
                            {"command": first_command},
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["doctor_preflight"] = {
                "present": True,
                "ok": True,
                "status": "degraded",
                "exit_code": 0,
                "first_recommended_command": first_command,
                "failure_count": 0,
                "recommended_command_count": 1,
            }
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review += (
                "\n## Doctor Preflight\n\n"
                "- Status: `stale`\n"
                "- Exit code: `2`\n"
                "\n## Appendix\n\n"
                "- Status: `degraded`\n"
                "- Exit code: `0`\n"
                f"- First recommended command: `{first_command}`\n"
                "- Failures: `0`\n"
                "- Recommended commands: `1`\n"
            )
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertFalse(result["ok"])
        self.assertIn("review.md doctor preflight section missing Status", result["errors"])
        self.assertIn("review.md doctor preflight section missing First recommended command", result["errors"])

    def test_doctor_preflight_first_command_must_be_recommended(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            root.joinpath("doctor-preflight.json").write_text(
                json.dumps(
                    {
                        "ok": True,
                        "status": "degraded",
                        "exit_code": 0,
                        "first_recommended_command": "kubectl get nodes",
                        "recommended_commands": [
                            {
                                "category": "kubernetes-readiness",
                                "severity": "blocked",
                                "command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                            }
                        ],
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            result = verifier.verify_bundle(root)

        self.assertFalse(result["ok"])
        self.assertIn(
            "doctor-preflight.json first recommended command is not in recommended_commands",
            result["errors"],
        )

    def test_valid_demo_gate_result_artifact_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            failed_command = "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache"
            root.joinpath("demo-gate-result.json").write_text(
                json.dumps(
                    {
                        "ok": False,
                        "stage": "kss-preflight",
                        "exit_code": 2,
                        "output_dir": str(root),
                        "failed_command": failed_command,
                        "failed_returncode": 2,
                        "failed_stderr_excerpt": "no ready kube-scheduler-simulator endpoints",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["demo_gate_result"] = {
                "present": True,
                "ok": False,
                "stage": "kss-preflight",
                "exit_code": 2,
                "failed_command": failed_command,
                "failed_returncode": 2,
            }
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review += (
                "\n## Demo Gate Result\n\n"
                "- Stage: `kss-preflight`\n"
                "- Exit code: `2`\n"
                f"- Failed command: `{failed_command}`\n"
                "- Failed returncode: `2`\n"
            )
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertTrue(result["ok"])
        self.assertTrue(result["demo_gate_result_present"])
        self.assertEqual(result["demo_gate_stage"], "kss-preflight")
        self.assertEqual(result["demo_gate_exit_code"], 2)
        self.assertEqual(
            result["demo_gate_failed_command"],
            "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
        )
        self.assertEqual(result["demo_gate_failed_returncode"], 2)

    def test_valid_demo_gate_result_parse_error_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            root.joinpath("demo-gate-result.json").write_text(
                json.dumps(
                    {
                        "ok": False,
                        "stage": "collect",
                        "exit_code": 1,
                        "output_dir": str(root),
                        "failed_command": "scripts/collect-evidence-bundle.py --json",
                        "failed_returncode": 1,
                        "failed_stdout_excerpt": "not json",
                        "parse_error": "Expecting value",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["demo_gate_result"] = {
                "present": True,
                "ok": False,
                "stage": "collect",
                "exit_code": 1,
                "failed_command": "scripts/collect-evidence-bundle.py --json",
                "failed_returncode": 1,
                "parse_error": "Expecting value",
            }
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review += (
                "\n## Demo Gate Result\n\n"
                "- Stage: `collect`\n"
                "- Exit code: `1`\n"
                "- Failed command: `scripts/collect-evidence-bundle.py --json`\n"
                "- Failed returncode: `1`\n"
                "- Parse error: `Expecting value`\n"
            )
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertTrue(result["ok"])
        self.assertTrue(result["demo_gate_result_present"])
        self.assertEqual(result["demo_gate_stage"], "collect")
        self.assertEqual(result["demo_gate_exit_code"], 1)
        self.assertEqual(result["demo_gate_parse_error"], "Expecting value")

    def test_manifest_demo_gate_result_must_match_artifact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            root.joinpath("demo-gate-result.json").write_text(
                json.dumps(
                    {
                        "ok": False,
                        "stage": "kss-preflight",
                        "exit_code": 2,
                        "output_dir": str(root),
                        "failed_command": "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
                        "failed_returncode": 2,
                        "failed_stderr_excerpt": "no ready kube-scheduler-simulator endpoints",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["demo_gate_result"] = {"present": True, "stage": "stale"}
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertFalse(result["ok"])
        self.assertIn("manifest demo-gate result mismatch", result["errors"])

    def test_review_must_include_manifest_demo_gate_result(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            failed_command = "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache"
            root.joinpath("demo-gate-result.json").write_text(
                json.dumps(
                    {
                        "ok": False,
                        "stage": "kss-preflight",
                        "exit_code": 2,
                        "output_dir": str(root),
                        "failed_command": failed_command,
                        "failed_returncode": 2,
                        "failed_stderr_excerpt": "no ready kube-scheduler-simulator endpoints",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["demo_gate_result"] = {
                "present": True,
                "ok": False,
                "stage": "kss-preflight",
                "exit_code": 2,
                "failed_command": failed_command,
                "failed_returncode": 2,
            }
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertFalse(result["ok"])
        self.assertIn("review.md missing Demo Gate Result", result["errors"])

    def test_review_demo_gate_result_must_match_section_not_elsewhere(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            failed_command = "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache"
            root.joinpath("demo-gate-result.json").write_text(
                json.dumps(
                    {
                        "ok": False,
                        "stage": "kss-preflight",
                        "exit_code": 2,
                        "output_dir": str(root),
                        "failed_command": failed_command,
                        "failed_returncode": 2,
                        "failed_stderr_excerpt": "no ready kube-scheduler-simulator endpoints",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["demo_gate_result"] = {
                "present": True,
                "ok": False,
                "stage": "kss-preflight",
                "exit_code": 2,
                "failed_command": failed_command,
                "failed_returncode": 2,
            }
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review += (
                "\n## Demo Gate Result\n\n"
                "- Stage: `stale-smoke`\n"
                "- Exit code: `1`\n"
                "- Failed command: `old command`\n"
                "- Failed returncode: `1`\n"
                "\n## Appendix\n\n"
                "- Stage: `kss-preflight`\n"
                "- Exit code: `2`\n"
                f"- Failed command: `{failed_command}`\n"
                "- Failed returncode: `2`\n"
            )
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)

        self.assertFalse(result["ok"])
        self.assertIn("review.md demo gate section missing Stage", result["errors"])
        self.assertIn("review.md demo gate section missing Failed command", result["errors"])

    def test_demo_gate_child_failure_must_include_diagnostics(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            root.joinpath("demo-gate-result.json").write_text(
                json.dumps(
                    {
                        "ok": False,
                        "stage": "smoke",
                        "exit_code": 1,
                        "output_dir": str(root),
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            result = verifier.verify_bundle(root)

        self.assertFalse(result["ok"])
        self.assertIn(
            "demo-gate-result.json child failure missing failed_command",
            result["errors"],
        )
        self.assertIn(
            "demo-gate-result.json child failure missing integer failed_returncode",
            result["errors"],
        )
        self.assertIn(
            "demo-gate-result.json child failure missing stderr/stdout/error excerpt",
            result["errors"],
        )

    def test_summary_production_debug_commands_must_match_capture(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["summary"]["production_readiness_debug_commands"] = [
                "kubectl config current-context"
            ]
            manifest["summary"]["production_readiness_first_debug_command"] = (
                "kubectl config current-context"
            )
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("summary production readiness debug commands mismatch", result["errors"])
        self.assertIn("summary production first debug command mismatch", result["errors"])

    def test_manifest_runbook_must_match_action_items(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["operator_runbook"]["step_count"] = 99
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("manifest: operator runbook step_count mismatch", result["errors"])

    def test_captured_evidence_runbook_must_match_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            evidence_path = root / "api-scheduler-evidence-bundle.json"
            payload = json.loads(evidence_path.read_text(encoding="utf-8"))
            payload["summary"]["operator_runbook"]["next_shell_command"] = "stale command"
            evidence_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            row = manifest["files"]["/api/scheduler/evidence-bundle"]
            row["bytes"] = evidence_path.stat().st_size
            row["sha256"] = verifier.sha256_file(evidence_path)
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("/api/scheduler/evidence-bundle: operator runbook mismatch", result["errors"])
        self.assertIn(
            "/api/scheduler/evidence-bundle: operator runbook next_shell_command mismatch",
            result["errors"],
        )

    def test_captured_operator_runbook_must_match_manifest(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            operator_path = root / "api-scheduler-operator-status.json"
            payload = json.loads(operator_path.read_text(encoding="utf-8"))
            payload["operator_runbook"]["manual_step_count"] = 7
            operator_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            row = manifest["files"]["/api/scheduler/operator-status"]
            row["bytes"] = operator_path.stat().st_size
            row["sha256"] = verifier.sha256_file(operator_path)
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("/api/scheduler/operator-status: operator runbook mismatch", result["errors"])
        self.assertIn(
            "/api/scheduler/operator-status: operator runbook manual_step_count mismatch",
            result["errors"],
        )

    def test_review_must_include_operator_vram_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review = review.replace("- Operator VRAM all fitted top drivers: `layer count, synthetic VRAM headroom probe`\n", "")
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("review.md missing Operator VRAM all fitted top drivers", result["errors"])

    def test_review_must_include_operator_binding_safety_summary(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review = review.replace("- Binding reservation pressure: `active`\n", "")
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("review.md missing Binding reservation pressure", result["errors"])

    def test_review_must_include_copyable_command_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            review_path = root / "review.md"
            review = review_path.read_text(encoding="utf-8")
            review = review.replace("### Copyable Command Provenance\n\n", "")
            review = review.replace(
                "- `kubectl --request-timeout=10s get --raw='/readyz?verbose'` "
                "from `environment` for `restore Kubernetes API connectivity` "
                "(severity `blocked`, artifact `healthy Kubernetes watch/relist state`)\n",
                "",
            )
            review_path.write_text(review, encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("review.md missing Copyable Command Provenance", result["errors"])
        self.assertIn(
            "review.md missing copyable command provenance for kubectl --request-timeout=10s get --raw='/readyz?verbose'",
            result["errors"],
        )

    def test_manifest_vram_driver_labels_must_match_captured_calibration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            write_bundle(root)
            manifest_path = root / "manifest.json"
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["vram_model_drivers"]["top_driver_labels"] = ["stale driver"]
            manifest["summary"]["vram_top_driver_labels"] = ["stale driver"]
            manifest_path.write_text(json.dumps(manifest) + "\n", encoding="utf-8")
            result = verifier.verify_bundle(root)
        self.assertEqual(result["ok"], False)
        self.assertIn("manifest VRAM top driver labels mismatch", result["errors"])
        self.assertIn("summary VRAM top driver labels mismatch", result["errors"])

    def test_exit_code_allows_verified_but_blocked_by_default(self) -> None:
        result = {"integrity_ok": True, "review_ready": False}
        self.assertEqual(
            verifier.exit_code_for_result(result, require_review_ready=False),
            0,
        )
        self.assertEqual(
            verifier.exit_code_for_result(result, require_review_ready=True),
            2,
        )

    def test_exit_code_fails_integrity_error_first(self) -> None:
        result = {"integrity_ok": False, "review_ready": True}
        self.assertEqual(
            verifier.exit_code_for_result(result, require_review_ready=True),
            1,
        )

    def test_printable_summary_includes_claim_blockers(self) -> None:
        text = verifier.printable_summary(
            {
                "verified_files": 7,
                "review_ready": False,
                "claim_blockers": ["customer claim not ready"],
                "primary_claim_blocker": "production readiness blocked: kubernetes_watch",
                "primary_claim_blocker_next_action": "restore Kubernetes API connectivity",
                "missing_live_artifact_count": 2,
                "missing_live_artifact_blocked_count": 1,
                "missing_live_artifact_warn_count": 1,
                "vram_admission_mode": "Shadow advisory only",
                "vram_claim_safe_driver_count": 7,
                "vram_claim_safe_driver_labels": [
                    "layer count",
                    "parameter memory x precision",
                    "parameter count",
                ],
                "vram_real_model_driver_count": 7,
                "vram_real_top_driver_labels": [
                    "layer count",
                    "parameter memory x precision",
                    "parameter count",
                ],
                "vram_synthetic_driver_count": 2,
                "vram_synthetic_driver_labels": [
                    "synthetic reserve pressure",
                    "synthetic transformer reserve pressure",
                ],
                "vram_driver_claim_boundary": DRIVER_CLAIM_BOUNDARY,
                "vram_next_evidence_target": "true CUDA OOM labels",
                "production_readiness_blocker_class": "kubernetes_watch",
                "production_readiness_last_error_class": "api_timeout",
                "simulator_probe_checked_count": 2,
                "simulator_probe_ready_count": 1,
                "simulator_readiness": "configured_not_probed",
                "doctor_preflight_present": True,
                "doctor_status": "degraded",
                "doctor_first_recommended_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "demo_gate_result_present": True,
                "demo_gate_stage": "kss-preflight",
                "demo_gate_failed_command": "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
                "demo_gate_failed_returncode": 2,
                "demo_gate_parse_error": "Expecting value",
                "operator_runbook": {
                    "step_count": 2,
                    "copyable_command_count": 1,
                    "manual_step_count": 1,
                    "next_shell_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                },
            }
        )
        self.assertIn("evidence bundle verified: 7 endpoint files, review blocked", text)
        self.assertIn("primary blocker: production readiness blocked: kubernetes_watch", text)
        self.assertIn("evidence gaps: 1 blocked, 1 warn", text)
        self.assertIn("next action: restore Kubernetes API connectivity", text)
        self.assertIn("VRAM mode: Shadow advisory only", text)
        self.assertIn(
            "VRAM claim-safe drivers: 7 (layer count, parameter memory x precision, parameter count)",
            text,
        )
        self.assertIn(
            "VRAM synthetic headroom drivers: 2 (synthetic VRAM headroom probe, synthetic transformer headroom probe)",
            text,
        )
        self.assertIn(f"VRAM claim boundary: {DRIVER_CLAIM_BOUNDARY}", text)
        self.assertIn("next VRAM evidence: true CUDA OOM labels", text)
        self.assertIn("production blocker class: kubernetes_watch", text)
        self.assertIn("simulator probe: 1/2 ready", text)
        self.assertIn("simulator readiness: configured_not_probed", text)
        self.assertIn("doctor preflight: degraded", text)
        self.assertIn("doctor first command: kubectl --request-timeout=10s get --raw='/readyz?verbose'", text)
        self.assertIn("demo gate result: kss-preflight", text)
        self.assertIn(
            "demo gate failed command: scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
            text,
        )
        self.assertIn("demo gate failed returncode: 2", text)
        self.assertIn("demo gate parse error: Expecting value", text)
        self.assertIn("operator runbook: 2 steps, 1 shell, 1 manual", text)
        self.assertIn("next shell command: kubectl --request-timeout=10s get --raw='/readyz?verbose'", text)
        self.assertIn(
            "first shell command reason: environment: restore Kubernetes API connectivity",
            text,
        )
        self.assertIn("claim blockers:", text)
        self.assertIn("- customer claim not ready", text)


if __name__ == "__main__":
    unittest.main()
