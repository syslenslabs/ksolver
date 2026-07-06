#!/usr/bin/env python3
"""Unit tests for the shadow demo gate wrapper."""

from __future__ import annotations

import importlib.util
import contextlib
import io
import json
import pathlib
import subprocess
import tempfile
import unittest
from unittest import mock


SCRIPT_PATH = pathlib.Path(__file__).with_name("demo-gate.py")
SPEC = importlib.util.spec_from_file_location("demo_gate", SCRIPT_PATH)
demo_gate = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(demo_gate)


def completed(returncode: int, payload: dict | None = None, stderr: str = ""):
    return subprocess.CompletedProcess(
        args=["fake"],
        returncode=returncode,
        stdout=json.dumps(payload or {}, sort_keys=True),
        stderr=stderr,
    )


class DemoGateTests(unittest.TestCase):
    def test_demo_gate_source_uses_synthetic_headroom_wording(self):
        source = SCRIPT_PATH.read_text(encoding="utf-8")
        self.assertNotIn("synthetic reserve pressure", source)
        self.assertNotIn("synthetic transformer reserve pressure", source)
        self.assertIn("synthetic_headroom", source)

    def test_count_urls_ignores_blank_segments(self):
        self.assertEqual(
            demo_gate.count_urls("http://127.0.0.1:12130,, http://127.0.0.1:12131 "),
            2,
        )

    def test_command_display_quotes_copyable_argv(self):
        self.assertEqual(
            demo_gate.command_display(["scripts/kss-pool.sh", "status", "2", "12130", "/tmp/cache dir"]),
            "scripts/kss-pool.sh status 2 12130 '/tmp/cache dir'",
        )
        self.assertEqual(demo_gate.command_display("already quoted"), "already quoted")
        self.assertIsNone(demo_gate.command_display(None))

    def test_stream_excerpt_trims_blank_and_truncates_long_text(self):
        self.assertIsNone(demo_gate.stream_excerpt("  \n  "))
        self.assertEqual(demo_gate.stream_excerpt("  short output\n"), "short output")
        excerpt = demo_gate.stream_excerpt("abcdef", limit=3)

        self.assertEqual(excerpt, "abc... [truncated 3 chars]")

    def test_persist_demo_gate_result_updates_manifest_and_review(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = pathlib.Path(tmp)
            (output_dir / "manifest.json").write_text(
                json.dumps({"packet_complete": True}) + "\n",
                encoding="utf-8",
            )
            (output_dir / "review.md").write_text("# ksolver SRE Evidence Bundle\n", encoding="utf-8")
            demo_gate.persist_demo_gate_result(
                output_dir,
                {
                    "ok": False,
                    "stage": "kss-preflight",
                    "exit_code": 2,
                    "failed_command": "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
                    "failed_returncode": 2,
                    "parse_error": "invalid json",
                },
            )

            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            review = (output_dir / "review.md").read_text(encoding="utf-8")

        self.assertEqual(manifest["demo_gate_result"]["stage"], "kss-preflight")
        self.assertEqual(manifest["demo_gate_result"]["exit_code"], 2)
        self.assertEqual(
            manifest["demo_gate_result"]["failed_command"],
            "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/cache",
        )
        self.assertEqual(manifest["demo_gate_result"]["parse_error"], "invalid json")
        self.assertIn("## Demo Gate Result", review)
        self.assertIn("Stage: `kss-preflight`", review)
        self.assertIn("Failed returncode: `2`", review)
        self.assertIn("Parse error: `invalid json`", review)

    def test_persist_demo_gate_result_replaces_stale_review_section(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = pathlib.Path(tmp)
            (output_dir / "manifest.json").write_text(
                json.dumps({"packet_complete": True}) + "\n",
                encoding="utf-8",
            )
            (output_dir / "review.md").write_text(
                "# ksolver SRE Evidence Bundle\n"
                "\n"
                "## Demo Gate Result\n"
                "\n"
                "- Stage: `stale-smoke`\n"
                "- Exit code: `1`\n"
                "- Failed command: `old command`\n"
                "\n"
                "## Captured Files\n"
                "\n"
                "- api-scheduler-traces.json\n",
                encoding="utf-8",
            )
            demo_gate.persist_demo_gate_result(
                output_dir,
                {
                    "ok": False,
                    "stage": "verify",
                    "exit_code": 1,
                    "failed_command": "scripts/verify-evidence-bundle.py /tmp/bundle",
                    "failed_returncode": 1,
                },
            )

            manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
            review = (output_dir / "review.md").read_text(encoding="utf-8")

        self.assertEqual(manifest["demo_gate_result"]["stage"], "verify")
        self.assertIn("Stage: `verify`", review)
        self.assertIn("Failed command: `scripts/verify-evidence-bundle.py /tmp/bundle`", review)
        self.assertNotIn("stale-smoke", review)
        self.assertNotIn("old command", review)
        self.assertIn("## Captured Files", review)
        self.assertIn("api-scheduler-traces.json", review)

    def test_non_negative_int_accepts_zero_and_positive_values(self):
        self.assertEqual(demo_gate.non_negative_int("0"), 0)
        self.assertEqual(demo_gate.non_negative_int("45"), 45)

    def test_non_negative_int_rejects_invalid_values(self):
        with self.assertRaises(demo_gate.argparse.ArgumentTypeError):
            demo_gate.non_negative_int("-1")
        with self.assertRaises(demo_gate.argparse.ArgumentTypeError):
            demo_gate.non_negative_int("soon")

    def test_positive_int_accepts_positive_values(self):
        self.assertEqual(demo_gate.positive_int("1"), 1)
        self.assertEqual(demo_gate.positive_int("45"), 45)

    def test_positive_int_rejects_zero_negative_and_non_integer(self):
        with self.assertRaises(demo_gate.argparse.ArgumentTypeError):
            demo_gate.positive_int("0")
        with self.assertRaises(demo_gate.argparse.ArgumentTypeError):
            demo_gate.positive_int("-1")
        with self.assertRaises(demo_gate.argparse.ArgumentTypeError):
            demo_gate.positive_int("soon")

    def test_arg_parser_accepts_valid_numeric_demo_thresholds(self):
        parser = demo_gate.build_arg_parser()
        args = parser.parse_args(
            [
                "--min-scenarios",
                "0",
                "--kss-count",
                "1",
                "--kss-base-port",
                "0",
                "--wait-kss-ready-seconds",
                "30",
            ]
        )

        self.assertEqual(args.min_scenarios, 0)
        self.assertEqual(args.kss_count, 1)
        self.assertEqual(args.kss_base_port, 0)
        self.assertEqual(args.wait_kss_ready_seconds, 30)

    def test_arg_parser_rejects_invalid_numeric_demo_thresholds(self):
        invalid_args = [
            ["--min-scenarios", "-1"],
            ["--kss-count", "0"],
            ["--kss-base-port", "-1"],
            ["--wait-kss-ready-seconds", "-1"],
        ]

        for argv in invalid_args:
            with self.subTest(argv=argv):
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        demo_gate.build_arg_parser().parse_args(argv)

    def test_environment_action_item_preserves_debug_command_list(self):
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
        items = demo_gate.missing_artifact_action_items(rows)
        runbook = demo_gate.operator_action_runbook(items)

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
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
            runbook["copyable_commands"],
        )

    def test_classify_readiness_blocker_names_primary_gate(self):
        self.assertEqual(
            demo_gate.classify_readiness_blocker(
                {"production_readiness": {"blocker": "watch not healthy"}}
            ),
            "kubernetes_watch",
        )
        self.assertEqual(
            demo_gate.classify_readiness_blocker(
                {"production_readiness": {"blocker": "solver unavailable"}}
            ),
            "solver",
        )
        self.assertEqual(
            demo_gate.classify_readiness_blocker(
                {
                    "readyz": {"ok": True, "status": 200, "body": "ok"},
                    "evidence_summary": {"simulator_readiness": "configured_unreachable"},
                }
            ),
            "simulator",
        )
        self.assertEqual(
            demo_gate.classify_readiness_blocker(
                {
                    "readyz": {"ok": True, "status": 200, "body": "ok"},
                    "evidence_summary": {
                        "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                        "simulator_readiness": "ready",
                        "review_ready": False,
                    },
                }
            ),
            "kubernetes_watch",
        )
        self.assertEqual(
            demo_gate.classify_readiness_blocker(
                {
                    "readyz": {"ok": True, "status": 200, "body": "ok"},
                    "evidence_summary": {
                        "simulator_readiness": "ready",
                        "review_ready": False,
                    },
                }
            ),
            "review_claims",
        )

    def test_classify_readiness_blocker_stays_consistent_with_shadow_smoke(self):
        smoke_path = pathlib.Path(__file__).with_name("shadow-smoke.py")
        smoke_spec = importlib.util.spec_from_file_location("shadow_smoke_for_gate", smoke_path)
        shadow_smoke = importlib.util.module_from_spec(smoke_spec)
        assert smoke_spec and smoke_spec.loader
        smoke_spec.loader.exec_module(shadow_smoke)
        cases = [
            {"production_readiness": {"blocker": "watch not healthy"}},
            {"production_readiness": {"blocker": "solver unavailable"}},
            {
                "readyz": {"ok": False, "status": 503, "body": "some apiserver error"},
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {"simulator_readiness": "not_configured"},
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {"simulator_readiness": "configured_unreachable"},
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {
                    "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                    "simulator_readiness": "ready",
                    "review_ready": False,
                },
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {"simulator_readiness": "ready", "review_ready": False},
            },
        ]
        for probe in cases:
            self.assertEqual(
                demo_gate.classify_readiness_blocker(probe),
                shadow_smoke.classify_readiness_blocker(probe),
            )

    def test_build_result_passes_when_verification_is_review_ready(self):
        result = demo_gate.build_result(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            smoke={
                "ok": True,
                "vram_investment_demo_rows": 6,
                "vram_investment_oom_risk_reduction_pods": 3,
                "vram_investment_high_vram_nodes_preserved": 1,
                "vram_investment_advisory_rows": 1,
                "vram_investment_average_baseline_oom_risk_percent": 68,
                "vram_investment_average_ksolver_oom_risk_percent": 17,
            },
            collection={
                "review_ready": True,
                "operator_binding_status": "dry-run-validation",
                "operator_reservation_pressure": "active",
                "operator_reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                "operator_reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                "operator_reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                "operator_reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
            },
            verification={"integrity_ok": True, "review_ready": True},
            require_review_ready=True,
            require_simulator_claim_ready=False,
            verify_returncode=0,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["stage"], "ready")
        self.assertEqual(result["exit_code"], 0)
        self.assertTrue(result["review_ready"])
        self.assertEqual(result["operator_binding_status"], "dry-run-validation")
        self.assertEqual(result["operator_reservation_pressure"], "active")
        self.assertIn("pending or reserved GPU capacity", result["operator_reservation_pressure_description"])
        self.assertIn("unrelated to CUDA", result["operator_reservation_pressure_scope"])
        self.assertIn("hold 4 GPU", result["operator_reservation_pressure_reason"])

    def test_build_result_returns_two_when_strict_review_is_blocked(self):
        result = demo_gate.build_result(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            smoke={
                "ok": True,
                "vram_investment_demo_rows": 6,
                "vram_investment_oom_risk_reduction_pods": 3,
                "vram_investment_high_vram_nodes_preserved": 1,
                "vram_investment_advisory_rows": 1,
                "vram_investment_average_baseline_oom_risk_percent": 68,
                "vram_investment_average_ksolver_oom_risk_percent": 17,
            },
            collection={
                "claim_blockers": ["customer claim not ready"],
                "operator_status": {
                    "debug_commands": ["kubectl config current-context"],
                    "production_readiness": {
                        "blocker_class": "kubernetes_watch",
                        "debug_commands": [
                            "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
                        ],
                    },
                },
            },
            verification={
                "integrity_ok": True,
                "review_ready": False,
                "claim_blockers": ["customer claim not ready"],
                "primary_claim_blocker": "customer claim not ready",
                "primary_claim_blocker_next_action": "resolve launch proof gaps before making customer-facing claims",
                "missing_live_artifact_count": 2,
                "missing_live_artifact_blocked_count": 1,
                "missing_live_artifact_warn_count": 1,
                "vram_admission_mode": "Shadow advisory only",
                "vram_next_evidence_target": "true CUDA OOM labels",
                "vram_model_drivers": {
                    "top_driver_count": 8,
                    "top_driver_labels": [
                        "layer count",
                        "parameter memory x precision",
                        "synthetic reserve pressure",
                    ],
                    "synthetic_reserve_driver": True,
                },
                "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                "simulator_endpoint_count": 2,
                "simulator_probe_checked_count": 2,
                "simulator_probe_ready_count": 1,
                "simulator_probe_timeout_millis": 2000,
                "simulator_readiness": "configured_not_probed",
                "simulator_readiness_note": "endpoints are configured",
            },
            require_review_ready=True,
            require_simulator_claim_ready=False,
            verify_returncode=2,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "review-blocked")
        self.assertEqual(result["exit_code"], 2)
        self.assertEqual(result["claim_blockers"], ["customer claim not ready"])
        self.assertEqual(result["missing_live_artifact_count"], 2)
        self.assertEqual(result["missing_live_artifact_blocked_count"], 1)
        self.assertEqual(result["missing_live_artifact_warn_count"], 1)
        self.assertEqual(result["primary_claim_blocker"], "customer claim not ready")
        self.assertEqual(
            result["primary_claim_blocker_next_action"],
            "resolve launch proof gaps before making customer-facing claims",
        )
        self.assertEqual(result["vram_admission_mode"], "Shadow advisory only")
        self.assertEqual(result["vram_next_evidence_target"], "true CUDA OOM labels")
        self.assertEqual(result["vram_model_driver_count"], 8)
        self.assertEqual(
            result["vram_top_driver_labels"],
            ["layer count", "parameter memory x precision", "synthetic reserve pressure"],
        )
        self.assertEqual(
            result["vram_display_top_driver_labels"],
            ["layer count", "parameter memory x precision", "synthetic VRAM headroom probe"],
        )
        self.assertEqual(result["vram_synthetic_reserve_driver"], True)
        self.assertEqual(result["vram_synthetic_headroom_driver"], True)
        self.assertEqual(result["vram_investment_demo_rows"], 6)
        self.assertEqual(result["vram_investment_oom_risk_reduction_pods"], 3)
        self.assertEqual(result["vram_investment_high_vram_nodes_preserved"], 1)
        self.assertEqual(result["vram_investment_advisory_rows"], 1)
        self.assertEqual(result["vram_investment_average_baseline_oom_risk_percent"], 68)
        self.assertEqual(result["vram_investment_average_ksolver_oom_risk_percent"], 17)
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
        self.assertEqual(result["simulator_probe_ready_count"], 1)
        self.assertEqual(result["simulator_probe_timeout_millis"], 2000)
        self.assertEqual(result["simulator_readiness"], "configured_not_probed")

    def test_build_result_can_require_simulator_claim_ready(self):
        result = demo_gate.build_result(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            smoke={"ok": True},
            collection={
                "summary": {
                    "simulator_claim_ready": False,
                    "simulator_claim_mode": "baseline-proof-blocked",
                    "simulator_claim_blocker": "no kube-scheduler-simulator endpoint answered /api/v1/export",
                    "simulator_claim_next_action": "start the KSS pool and rerun the evidence capture",
                }
            },
            verification={"integrity_ok": True, "review_ready": True},
            require_review_ready=False,
            require_simulator_claim_ready=True,
            verify_returncode=0,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "simulator-claim-blocked")
        self.assertEqual(result["exit_code"], 2)
        self.assertTrue(result["require_simulator_claim_ready"])
        self.assertFalse(result["simulator_claim_ready"])
        self.assertEqual(result["simulator_claim_mode"], "baseline-proof-blocked")
        self.assertEqual(
            result["simulator_claim_blocker"],
            "no kube-scheduler-simulator endpoint answered /api/v1/export",
        )
        self.assertEqual(
            result["simulator_claim_next_action"],
            "start the KSS pool and rerun the evidence capture",
        )

    def test_build_result_preserves_zero_probe_counts_from_verification(self):
        result = demo_gate.build_result(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            smoke={"ok": True},
            collection={
                "summary": {
                    "simulator_endpoint_count": 2,
                    "simulator_probe_checked_count": 2,
                    "simulator_probe_ready_count": 2,
                }
            },
            verification={
                "integrity_ok": True,
                "review_ready": False,
                "simulator_endpoint_count": 0,
                "simulator_probe_checked_count": 0,
                "simulator_probe_ready_count": 0,
                "simulator_probe_timeout_millis": 0,
                "simulator_readiness": "not_configured",
            },
            require_review_ready=False,
            require_simulator_claim_ready=False,
            verify_returncode=0,
        )
        self.assertEqual(result["simulator_endpoint_count"], 0)
        self.assertEqual(result["simulator_probe_checked_count"], 0)
        self.assertEqual(result["simulator_probe_ready_count"], 0)
        self.assertEqual(result["simulator_probe_timeout_millis"], 0)
        self.assertEqual(result["simulator_readiness"], "not_configured")

    def test_smoke_failure_stops_before_collection(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            return completed(1, {"ok": False, "error": "readyz failed"})

        result = demo_gate.run_demo_gate(
            base_url="http://shadow/",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            runner=runner,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "smoke")
        self.assertEqual(result["exit_code"], 1)
        self.assertEqual(result["base_url"], "http://shadow")
        self.assertEqual(len(calls), 1)
        self.assertIn("readyz failed", result["error"])
        self.assertIn("readiness_probe", result)

    def test_run_demo_gate_collects_and_verifies_in_order(self):
        calls = []
        responses = [
            completed(
                0,
                {
                    "ok": True,
                    "scenario_count": 33,
                    "readiness_mode": "degraded",
                    "readiness_blocker_class": "kubernetes_watch",
                    "vram_investment_demo_rows": 6,
                    "vram_investment_oom_risk_reduction_pods": 3,
                    "vram_investment_high_vram_nodes_preserved": 1,
                    "vram_investment_advisory_rows": 1,
                    "vram_investment_average_baseline_oom_risk_percent": 68,
                    "vram_investment_average_ksolver_oom_risk_percent": 17,
                },
            ),
            completed(0, {"ok": True, "review_ready": False}),
            completed(
                0,
                {
                    "integrity_ok": True,
                    "review_ready": False,
                    "claim_blockers": ["5 missing live artifact(s)"],
                    "primary_claim_blocker": "5 missing live artifact(s)",
                    "primary_claim_blocker_next_action": "capture missing artifacts",
                    "missing_live_artifact_count": 5,
                    "missing_live_artifact_blocked_count": 3,
                    "missing_live_artifact_warn_count": 2,
                    "vram_admission_mode": "Shadow advisory only",
                    "vram_next_evidence_target": "true CUDA OOM labels",
                    "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                    "simulator_endpoint_count": 2,
                    "simulator_probe_checked_count": 2,
                    "simulator_probe_ready_count": 2,
                    "simulator_probe_timeout_millis": 2000,
                    "simulator_readiness": "configured_not_probed",
                },
            ),
        ]

        def runner(argv):
            calls.append(argv)
            return responses.pop(0)

        result = demo_gate.run_demo_gate(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            allow_readiness_blocked=True,
            runner=runner,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["stage"], "ready")
        self.assertEqual(result["readiness_mode"], "degraded")
        self.assertEqual(result["readiness_blocker_class"], "kubernetes_watch")
        self.assertFalse(result["review_ready"])
        self.assertEqual(result["claim_blockers"], ["5 missing live artifact(s)"])
        self.assertEqual(result["missing_live_artifact_count"], 5)
        self.assertEqual(result["missing_live_artifact_blocked_count"], 3)
        self.assertEqual(result["missing_live_artifact_warn_count"], 2)
        self.assertEqual(result["primary_claim_blocker"], "5 missing live artifact(s)")
        self.assertEqual(result["primary_claim_blocker_next_action"], "capture missing artifacts")
        self.assertEqual(result["vram_admission_mode"], "Shadow advisory only")
        self.assertEqual(result["vram_next_evidence_target"], "true CUDA OOM labels")
        self.assertEqual(result["vram_investment_demo_rows"], 6)
        self.assertEqual(result["vram_investment_oom_risk_reduction_pods"], 3)
        self.assertEqual(result["vram_investment_high_vram_nodes_preserved"], 1)
        self.assertEqual(result["production_readiness_blocker_class"], "kubernetes_watch")
        self.assertEqual(result["simulator_endpoint_count"], 2)
        self.assertEqual(result["simulator_probe_checked_count"], 2)
        self.assertEqual(result["simulator_probe_ready_count"], 2)
        self.assertEqual(result["simulator_readiness"], "configured_not_probed")
        self.assertEqual(len(calls), 3)
        self.assertIn("shadow-smoke.py", calls[0][1])
        self.assertIn("--allow-readiness-blocked", calls[0])
        self.assertIn("collect-evidence-bundle.py", calls[1][1])
        self.assertIn("verify-evidence-bundle.py", calls[2][1])

    def test_run_demo_gate_can_require_simulator_claim_ready_after_verification(self):
        calls = []
        responses = [
            completed(0, {"ok": True, "scenario_count": 33}),
            completed(0, {"ok": True, "review_ready": True}),
            completed(
                0,
                {
                    "integrity_ok": True,
                    "review_ready": True,
                    "simulator_claim_ready": False,
                    "simulator_claim_mode": "baseline-proof-blocked",
                    "simulator_claim_blocker": "no kube-scheduler-simulator endpoint answered /api/v1/export",
                    "simulator_claim_next_action": "start the KSS pool and rerun the evidence capture",
                },
            ),
        ]

        def runner(argv):
            calls.append(argv)
            return responses.pop(0)

        result = demo_gate.run_demo_gate(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            require_simulator_claim_ready=True,
            runner=runner,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "simulator-claim-blocked")
        self.assertEqual(result["exit_code"], 2)
        self.assertTrue(result["require_simulator_claim_ready"])
        self.assertEqual(len(calls), 3)
        self.assertIn("verify-evidence-bundle.py", calls[2][1])

    def test_run_demo_gate_can_fail_on_doctor_preflight(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            return completed(
                2,
                {
                    "ok": False,
                    "status": "blocked",
                    "failures": ["no kube-scheduler-simulator endpoint is ready"],
                    "first_recommended_command": "scripts/kss-pool.sh status 2 12130 /tmp/cache",
                    "recommended_commands": [
                        {
                            "category": "kube-scheduler-simulator",
                            "severity": "blocked",
                            "command": "scripts/kss-pool.sh status 2 12130 /tmp/cache",
                            "reason": "no kube-scheduler-simulator endpoint is ready",
                        }
                    ],
                    "api_endpoint_failures": [
                        {
                            "category": "shadow-api",
                            "severity": "blocked",
                            "endpoint": "/api/scheduler/operator-status",
                            "command": "curl -fsS http://shadow/api/scheduler/operator-status",
                            "reason": "operator-status endpoint did not return a valid JSON object",
                        }
                    ],
                    "production_readiness_blocker_class": "kubernetes_watch",
                    "first_debug_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                    "simulator_claim_ready": False,
                    "simulator_claim_mode": "baseline-proof-blocked",
                    "simulator_claim_blocker": "no kube-scheduler-simulator endpoint answered /api/v1/export",
                    "kss_ready_count": 0,
                },
            )

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = pathlib.Path(tmp) / "bundle"
            result = demo_gate.run_demo_gate(
                base_url="http://shadow",
                output_dir=output_dir,
                min_scenarios=33,
                require_review_ready=False,
                require_kss_ready=True,
                require_simulator_claim_ready=True,
                doctor_preflight=True,
                allow_readiness_blocked=True,
                kss_count=2,
                kss_base_port=12130,
                kss_cache_dir="/tmp/cache",
                runner=runner,
            )
            doctor_artifact = json.loads((output_dir / "doctor-preflight.json").read_text())

        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "doctor-preflight")
        self.assertEqual(result["exit_code"], 2)
        self.assertEqual(result["doctor_status"], "blocked")
        self.assertEqual(result["doctor_failures"], ["no kube-scheduler-simulator endpoint is ready"])
        self.assertEqual(result["doctor_api_endpoint_failure_count"], 1)
        self.assertEqual(
            result["doctor_first_api_endpoint_failure"]["endpoint"],
            "/api/scheduler/operator-status",
        )
        self.assertEqual(
            result["doctor_first_recommended_command"],
            "scripts/kss-pool.sh status 2 12130 /tmp/cache",
        )
        self.assertEqual(result["production_readiness_blocker_class"], "kubernetes_watch")
        self.assertEqual(result["production_readiness_first_debug_command"], "kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        self.assertEqual(result["simulator_claim_mode"], "baseline-proof-blocked")
        self.assertEqual(result["kss_ready_count"], 0)
        self.assertEqual(doctor_artifact["status"], "blocked")
        self.assertEqual(doctor_artifact["exit_code"], 2)
        self.assertEqual(doctor_artifact["api_endpoint_failures"][0]["category"], "shadow-api")
        self.assertEqual(
            doctor_artifact["first_recommended_command"],
            "scripts/kss-pool.sh status 2 12130 /tmp/cache",
        )
        self.assertEqual(len(calls), 1)
        self.assertIn("shadow-doctor.py", calls[0][1])
        self.assertIn("--require-kss-ready", calls[0])
        self.assertIn("--require-simulator-claim-ready", calls[0])
        self.assertNotIn("--require-readyz", calls[0])

    def test_run_demo_gate_doctor_preflight_can_require_readyz(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            return completed(2, {"ok": False, "status": "blocked", "failures": ["shadow readyz is not ready"]})

        result = demo_gate.run_demo_gate(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            doctor_preflight=True,
            allow_readiness_blocked=False,
            runner=runner,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "doctor-preflight")
        self.assertIn("--require-readyz", calls[0])

    def test_run_demo_gate_preserves_successful_doctor_context(self):
        calls = []
        responses = [
            completed(
                0,
                {
                    "ok": True,
                    "status": "degraded",
                    "first_recommended_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                    "recommended_commands": [
                        {
                            "category": "kubernetes-readiness",
                            "severity": "blocked",
                            "command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                        }
                    ],
                    "production_readiness_blocker_class": "kubernetes_watch",
                    "first_debug_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                    "simulator_claim_ready": True,
                    "simulator_claim_mode": "live-kube-scheduler-simulator-ready",
                    "kss_ready_count": 1,
                },
            ),
            completed(0, {"ok": True, "scenario_count": 33}),
            completed(0, {"ok": True, "review_ready": True}),
            completed(0, {"integrity_ok": True, "review_ready": True}),
        ]

        def runner(argv):
            calls.append(argv)
            return responses.pop(0)

        with tempfile.TemporaryDirectory() as tmp:
            output_dir = pathlib.Path(tmp) / "bundle"
            result = demo_gate.run_demo_gate(
                base_url="http://shadow",
                output_dir=output_dir,
                min_scenarios=33,
                require_review_ready=False,
                doctor_preflight=True,
                allow_readiness_blocked=True,
                runner=runner,
            )
            doctor_artifact = json.loads((output_dir / "doctor-preflight.json").read_text())

        self.assertTrue(result["ok"])
        self.assertEqual(result["stage"], "ready")
        self.assertEqual(result["doctor_status"], "degraded")
        self.assertEqual(
            result["doctor_first_recommended_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(result["doctor_recommended_commands"][0]["category"], "kubernetes-readiness")
        self.assertEqual(result["production_readiness_blocker_class"], "kubernetes_watch")
        self.assertEqual(result["simulator_claim_mode"], "live-kube-scheduler-simulator-ready")
        self.assertEqual(doctor_artifact["status"], "degraded")
        self.assertEqual(doctor_artifact["exit_code"], 0)
        self.assertIn("shadow-doctor.py", calls[0][1])
        self.assertIn("shadow-smoke.py", calls[1][1])

    def test_run_demo_gate_can_require_kss_ready_before_smoke(self):
        calls = []
        responses = [
            subprocess.CompletedProcess(
                args=["fake"],
                returncode=0,
                stdout="http://127.0.0.1:12130,http://127.0.0.1:12131\n",
                stderr="",
            ),
            completed(0, {"ok": True, "scenario_count": 33}),
            completed(0, {"ok": True, "review_ready": True}),
            completed(0, {"integrity_ok": True, "review_ready": True}),
        ]

        def runner(argv):
            calls.append(argv)
            return responses.pop(0)

        result = demo_gate.run_demo_gate(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            require_kss_ready=True,
            kss_count=2,
            kss_base_port=12130,
            kss_cache_dir="/tmp/ksolver-kss-cache",
            runner=runner,
        )
        self.assertTrue(result["ok"])
        self.assertEqual(
            result["kss_ready_urls"],
            "http://127.0.0.1:12130,http://127.0.0.1:12131",
        )
        self.assertEqual(result["kss_ready_count"], 2)
        self.assertEqual(calls[0][1], "require-ready-urls")
        self.assertEqual(calls[0][2:], ["2", "12130", "/tmp/ksolver-kss-cache"])
        self.assertIn("shadow-smoke.py", calls[1][1])

    def test_run_demo_gate_can_wait_for_kss_ready_before_smoke(self):
        calls = []
        responses = [
            subprocess.CompletedProcess(
                args=["fake"],
                returncode=0,
                stdout="http://127.0.0.1:12130\n",
                stderr="",
            ),
            completed(0, {"ok": True, "scenario_count": 33}),
            completed(0, {"ok": True, "review_ready": True}),
            completed(0, {"integrity_ok": True, "review_ready": True}),
        ]

        def runner(argv):
            calls.append(argv)
            return responses.pop(0)

        result = demo_gate.run_demo_gate(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            require_kss_ready=True,
            kss_count=2,
            kss_base_port=12130,
            kss_cache_dir="/tmp/ksolver-kss-cache",
            kss_wait_seconds=45,
            runner=runner,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["kss_ready_urls"], "http://127.0.0.1:12130")
        self.assertEqual(result["kss_ready_count"], 1)
        self.assertEqual(calls[0][1], "wait-ready-urls")
        self.assertEqual(calls[0][2:], ["2", "12130", "/tmp/ksolver-kss-cache", "45"])
        self.assertIn("shadow-smoke.py", calls[1][1])

    def test_run_demo_gate_fails_before_smoke_when_required_kss_is_not_ready(self):
        calls = []

        def runner(argv):
            calls.append(argv)
            return subprocess.CompletedProcess(
                args=argv,
                returncode=2,
                stdout="",
                stderr="no ready kube-scheduler-simulator endpoints passed /api/v1/export\n",
            )

        result = demo_gate.run_demo_gate(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            require_kss_ready=True,
            runner=runner,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "kss-preflight")
        self.assertEqual(result["exit_code"], 2)
        self.assertEqual(len(calls), 1)
        self.assertIn("kss-pool.sh require-ready-urls 4 12120", result["failed_command"])
        self.assertEqual(result["failed_returncode"], 2)
        self.assertIn(
            "no ready kube-scheduler-simulator endpoints",
            result["failed_stderr_excerpt"],
        )
        self.assertIn("no ready kube-scheduler-simulator endpoints", result["error"])

    def test_run_demo_gate_preserves_kss_urls_on_later_failure(self):
        calls = []
        responses = [
            subprocess.CompletedProcess(
                args=["fake"],
                returncode=0,
                stdout="http://127.0.0.1:12130\n",
                stderr="",
            ),
            completed(1, {"ok": False, "error": "readyz failed"}),
        ]

        def runner(argv):
            calls.append(argv)
            return responses.pop(0)

        result = demo_gate.run_demo_gate(
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            min_scenarios=33,
            require_review_ready=False,
            require_kss_ready=True,
            runner=runner,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["stage"], "smoke")
        self.assertEqual(result["kss_ready_urls"], "http://127.0.0.1:12130")
        self.assertEqual(result["kss_ready_count"], 1)
        self.assertEqual(len(calls), 2)

    def test_main_persists_demo_gate_result_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            output_dir = pathlib.Path(tmp) / "bundle"
            result = {
                "ok": False,
                "stage": "kss-preflight",
                "exit_code": 2,
                "output_dir": str(output_dir),
                "failed_command": "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/ksolver-kss-cache",
                "failed_returncode": 2,
                "failed_stderr_excerpt": "no ready kube-scheduler-simulator endpoints",
            }

            with mock.patch.object(
                demo_gate,
                "run_demo_gate",
                return_value=result,
            ), mock.patch.object(
                demo_gate.sys,
                "argv",
                ["demo-gate.py", "--output-dir", str(output_dir), "--json"],
            ), contextlib.redirect_stdout(io.StringIO()):
                exit_code = demo_gate.main()

            self.assertEqual(exit_code, 2)
            persisted = json.loads(
                (output_dir / demo_gate.DEMO_GATE_RESULT_FILENAME).read_text(encoding="utf-8")
            )
            self.assertEqual(persisted["stage"], "kss-preflight")
            self.assertEqual(
                persisted["failed_command"],
                "scripts/kss-pool.sh require-ready-urls 4 12120 /tmp/ksolver-kss-cache",
            )
            self.assertEqual(persisted["failed_returncode"], 2)
            self.assertEqual(
                persisted["failed_stderr_excerpt"],
                "no ready kube-scheduler-simulator endpoints",
            )

    def test_stage_failure_can_include_readiness_probe(self):
        result = demo_gate.stage_failure(
            stage="smoke",
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            process=completed(1, {"ok": False, "error": "readyz failed"}),
            readiness={
                "healthz": {"ok": True, "status": 200, "body": "ok"},
                "readyz": {"ok": False, "status": 503, "body": "watch not healthy"},
                "evidence_summary": {
                    "vram_model_driver_count": 8,
                    "vram_top_driver_labels": [
                        "layer count",
                        "parameter memory x precision",
                        "synthetic reserve pressure",
                    ],
                    "vram_synthetic_reserve_driver": True,
                    "vram_synthetic_headroom_driver": True,
                },
            },
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["failed_command"], "fake")
        self.assertEqual(result["failed_returncode"], 1)
        self.assertIn('"error": "readyz failed"', result["failed_stdout_excerpt"])
        self.assertEqual(result["readiness_probe"]["healthz"]["status"], 200)
        self.assertEqual(result["readiness_probe"]["readyz"]["body"], "watch not healthy")
        self.assertEqual(result["readiness_blocker_class"], "kubernetes_watch")
        self.assertEqual(result["vram_model_driver_count"], 8)
        self.assertEqual(
            result["vram_top_driver_labels"],
            ["layer count", "parameter memory x precision", "synthetic reserve pressure"],
        )
        self.assertEqual(
            result["vram_display_top_driver_labels"],
            ["layer count", "parameter memory x precision", "synthetic VRAM headroom probe"],
        )
        self.assertEqual(result["vram_synthetic_reserve_driver"], True)
        self.assertEqual(result["vram_synthetic_headroom_driver"], True)

    def test_stage_failure_preserves_raw_child_returncode(self):
        result = demo_gate.stage_failure(
            stage="kss-preflight",
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            process=completed(127, stderr="missing executable"),
        )

        self.assertEqual(result["exit_code"], 1)
        self.assertEqual(result["failed_returncode"], 127)
        self.assertEqual(result["failed_stderr_excerpt"], "missing executable")
        self.assertEqual(result["error"], "missing executable")

    def test_stage_failure_reports_malformed_child_json(self):
        result = demo_gate.stage_failure(
            stage="collect",
            base_url="http://shadow",
            output_dir=pathlib.Path("/tmp/bundle"),
            process=subprocess.CompletedProcess(
                args=["fake"],
                returncode=1,
                stdout="not-json",
                stderr="",
            ),
        )

        self.assertEqual(result["stage"], "collect")
        self.assertEqual(result["parse_error"], "Expecting value")
        self.assertEqual(result["collect"]["parse_error"], "Expecting value")
        self.assertEqual(result["collect"]["raw_stdout"], "not-json")

    def test_readiness_probe_summarizes_evidence_bundle(self):
        bodies = {
            "http://shadow/healthz": {"status": 200, "body": "ok"},
            "http://shadow/readyz": {"status": 503, "body": "watch not healthy"},
            "http://shadow/api/scheduler/production-safety": {
                "status": 200,
                "body": json.dumps(
                    {
                        "readiness": {
                            "ready": False,
                            "blocker": "watch not healthy",
                            "last_error_at": "2026-07-06T07:00:00Z",
                            "next_action": "restore Kubernetes API connectivity",
                            "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                        },
                        "simulator": {
                            "endpoint_count": 2,
                            "live_dashboard_baseline_configured": True,
                            "readiness": "configured_not_probed",
                            "readiness_note": "endpoints are configured; export readiness is checked during live baseline calls",
                            "readiness_probe": {
                                "checked_count": 2,
                                "ready_count": 1,
                                "timeout_millis": 2000,
                            },
                            "claim_guard": "live dashboard baselines can call kube-scheduler-simulator",
                        }
                    }
                ),
            },
            "http://shadow/api/scheduler/evidence-bundle": {
                "status": 200,
                "body": json.dumps(
                    {
                        "summary": {
                            "review_ready": False,
                            "demo_gate_status": "local-pass-strict-blocked",
                            "demo_gate_strict_exit_code": 2,
                            "primary_claim_blocker": "production readiness blocked: kubernetes_watch",
                            "primary_claim_blocker_next_action": "restore Kubernetes API connectivity",
                            "claim_blockers": ["watch not healthy"],
                            "vram_admission_mode": "Shadow advisory only",
                            "vram_scheduler_use": "Score and warn; do not reject pods",
                            "vram_hard_blocker_count": 4,
                            "vram_next_evidence_target": "true CUDA OOM labels",
                            "vram_model_driver_count": 8,
                            "vram_top_driver_labels": [
                                "layer count",
                                "parameter memory x precision",
                                "synthetic reserve pressure",
                            ],
                            "vram_synthetic_reserve_driver": True,
                            "vram_synthetic_headroom_driver": True,
                            "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                            "simulator_endpoint_count": 2,
                            "simulator_probe_checked_count": 2,
                            "simulator_probe_ready_count": 1,
                            "simulator_probe_timeout_millis": 2000,
                            "simulator_readiness": "configured_not_probed",
                            "simulator_readiness_note": (
                                "endpoints are configured; export readiness is checked during live baseline calls"
                            ),
                            "live_validation_gate_count": 3,
                            "live_validation_pass_count": 1,
                            "live_validation_warn_count": 1,
                            "live_validation_blocked_count": 1,
                        }
                    }
                ),
            },
            "http://shadow/api/scheduler/operator-status": {
                "status": 200,
                "body": json.dumps(
                    {
                        "ok": True,
                        "status": "blocked",
                        "status_label": "operator action required",
                        "can_shadow_demo": True,
                        "can_customer_claim": False,
                        "primary_blocker": "production readiness blocked: kubernetes_watch",
                        "next_action": "restore Kubernetes API connectivity",
                        "debug_commands": ["kubectl config current-context"],
                        "production_readiness": {
                            "blocker_class": "kubernetes_watch",
                            "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                        },
                        "simulator": {
                            "readiness": "configured_not_probed",
                            "ready_count": 1,
                            "checked_count": 2,
                            "endpoint_count": 2,
                        },
                        "proof_gates": {
                            "total": 3,
                            "pass": 1,
                            "warn": 1,
                            "blocked": 1,
                            "rows": [
                                {"gate": "pending GPU trace", "status": "blocked"},
                                {"gate": "kube baseline provenance", "status": "warn"},
                                {"gate": "production mutation safety", "status": "pass"},
                            ],
                        },
                        "vram": {
                            "mode": "Shadow advisory only",
                            "next_evidence_target": "true CUDA OOM labels",
                            "model_driver_count": 8,
                            "top_driver_labels": [
                                "layer count",
                                "parameter memory x precision",
                                "synthetic reserve pressure",
                            ],
                            "synthetic_reserve_driver": True,
                        },
                        "demo_gate": {
                            "strict_exit_code": 2,
                        },
                    }
                ),
            },
        }

        def fake_fetch(url):
            row = bodies[url]
            return {
                "ok": 200 <= row["status"] < 300,
                "status": row["status"],
                "body": row["body"],
            }

        original_fetch = demo_gate.fetch_probe
        try:
            demo_gate.fetch_probe = fake_fetch
            probe = demo_gate.readiness_probe("http://shadow")
        finally:
            demo_gate.fetch_probe = original_fetch

        self.assertEqual(probe["readyz"]["status"], 503)
        self.assertEqual(probe["production_safety"]["status"], 200)
        self.assertEqual(probe["production_readiness"]["blocker"], "watch not healthy")
        self.assertEqual(probe["production_readiness"]["last_error_at"], "2026-07-06T07:00:00Z")
        self.assertEqual(
            probe["production_readiness"]["debug_commands"],
            ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
        )
        self.assertEqual(probe["simulator_readiness"]["endpoint_count"], 2)
        self.assertEqual(probe["simulator_readiness"]["readiness"], "configured_not_probed")
        self.assertEqual(probe["simulator_readiness"]["readiness_probe"]["ready_count"], 1)
        self.assertEqual(
            probe["evidence_summary"]["demo_gate_status"],
            "local-pass-strict-blocked",
        )
        self.assertEqual(
            probe["evidence_summary"]["primary_claim_blocker"],
            "production readiness blocked: kubernetes_watch",
        )
        self.assertEqual(
            probe["evidence_summary"]["primary_claim_blocker_next_action"],
            "restore Kubernetes API connectivity",
        )
        self.assertEqual(probe["evidence_summary"]["claim_blockers"], ["watch not healthy"])
        self.assertEqual(probe["evidence_summary"]["vram_admission_mode"], "Shadow advisory only")
        self.assertEqual(probe["evidence_summary"]["vram_next_evidence_target"], "true CUDA OOM labels")
        self.assertEqual(probe["evidence_summary"]["vram_model_driver_count"], 8)
        self.assertEqual(
            probe["evidence_summary"]["vram_top_driver_labels"],
            ["layer count", "parameter memory x precision", "synthetic reserve pressure"],
        )
        self.assertEqual(probe["evidence_summary"]["vram_synthetic_reserve_driver"], True)
        self.assertEqual(probe["evidence_summary"]["vram_synthetic_headroom_driver"], True)
        self.assertEqual(
            probe["evidence_summary"]["production_readiness_blocker_class"],
            "kubernetes_watch",
        )
        self.assertEqual(probe["evidence_summary"]["simulator_endpoint_count"], 2)
        self.assertEqual(probe["evidence_summary"]["simulator_probe_checked_count"], 2)
        self.assertEqual(probe["evidence_summary"]["simulator_probe_ready_count"], 1)
        self.assertEqual(probe["evidence_summary"]["simulator_readiness"], "configured_not_probed")
        self.assertEqual(probe["operator_status_probe"]["status"], 200)
        self.assertEqual(probe["operator_status"]["status"], "blocked")
        self.assertEqual(probe["operator_status"]["proof_gates"]["total"], 3)
        self.assertEqual(probe["operator_status"]["proof_gates"]["blocked"], 1)
        self.assertEqual(probe["operator_status"]["vram"]["model_driver_count"], 8)
        self.assertEqual(
            probe["operator_status"]["primary_blocker"],
            "production readiness blocked: kubernetes_watch",
        )
        self.assertEqual(
            probe["operator_status"]["next_action"],
            "restore Kubernetes API connectivity",
        )
        self.assertEqual(
            probe["operator_status"]["debug_commands"],
            ["kubectl config current-context"],
        )

    def test_printable_summary_names_first_blocker(self):
        summary = demo_gate.printable_summary(
            {
                "ok": False,
                "stage": "review-blocked",
                "output_dir": "/tmp/bundle",
                "review_ready": False,
                "readiness_mode": "degraded",
                "readiness_blocker_class": "kubernetes_watch",
                "missing_live_artifact_count": 2,
                "missing_live_artifact_blocked_count": 1,
                "missing_live_artifact_warn_count": 1,
                "claim_blockers": ["customer claim not ready"],
                "primary_claim_blocker": "customer claim not ready",
                "primary_claim_blocker_next_action": "resolve launch proof gaps before making customer-facing claims",
                "vram_admission_mode": "Shadow advisory only",
                "vram_next_evidence_target": "true CUDA OOM labels",
                "vram_model_driver_count": 8,
                "vram_top_driver_labels": [
                    "layer count",
                    "parameter memory x precision",
                    "synthetic reserve pressure",
                ],
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
                "vram_synthetic_driver_count": 1,
                "vram_synthetic_driver_labels": ["synthetic reserve pressure"],
                "simulator_endpoint_count": 2,
                "simulator_probe_checked_count": 2,
                "simulator_probe_ready_count": 1,
                "simulator_readiness": "configured_not_probed",
                "production_readiness_first_debug_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "operator_runbook": {
                    "step_count": 2,
                    "copyable_command_count": 1,
                    "manual_step_count": 1,
                    "next_shell_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                },
            }
        )
        self.assertIn("demo gate review-blocked", summary)
        self.assertIn("readiness: degraded/kubernetes_watch", summary)
        self.assertIn("class: kubernetes_watch", summary)
        self.assertIn("evidence gaps: 1 blocked, 1 warn", summary)
        self.assertIn("simulator: configured_not_probed (1/2 ready, 2 endpoint(s))", summary)
        self.assertIn("VRAM: Shadow advisory only", summary)
        self.assertIn(
            "VRAM claim-safe drivers: 7 (layer count, parameter memory x precision, parameter count)",
            summary,
        )
        self.assertIn(
            "VRAM synthetic headroom drivers: 1 (synthetic VRAM headroom probe)",
            summary,
        )
        self.assertIn("next VRAM evidence: true CUDA OOM labels", summary)
        self.assertIn("primary blocker: customer claim not ready", summary)
        self.assertIn("next action: resolve launch proof gaps before making customer-facing claims", summary)
        self.assertIn("operator runbook: 2 steps, 1 shell, 1 manual", summary)
        self.assertIn("next shell command: kubectl --request-timeout=10s get --raw='/readyz?verbose'", summary)
        self.assertIn(
            "first shell command reason: environment: restore Kubernetes API connectivity",
            summary,
        )
        self.assertIn(
            "production first debug command: kubectl --request-timeout=10s get --raw='/readyz?verbose'",
            summary,
        )

    def test_printable_summary_names_probe_context(self):
        summary = demo_gate.printable_summary(
            {
                "ok": False,
                "stage": "smoke",
                "output_dir": "/tmp/bundle",
                "readiness_blocker_class": "kubernetes_watch",
                "readiness_probe": {
                    "production_readiness": {
                        "blocker": "watch not healthy",
                        "last_error_at": "2026-07-06T07:00:00Z",
                        "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                    },
                    "evidence_summary": {
                        "claim_blockers": ["customer claim not ready"],
                        "primary_claim_blocker": "customer claim not ready",
                        "primary_claim_blocker_next_action": "resolve launch proof gaps before making customer-facing claims",
                        "vram_admission_mode": "Shadow advisory only",
                        "vram_next_evidence_target": "true CUDA OOM labels",
                        "vram_model_driver_count": 8,
                        "vram_top_driver_labels": [
                            "layer count",
                            "parameter memory x precision",
                            "synthetic reserve pressure",
                        ],
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
                        "vram_synthetic_driver_count": 1,
                        "vram_synthetic_driver_labels": ["synthetic reserve pressure"],
                        "vram_investment_demo_rows": 6,
                        "vram_investment_oom_risk_reduction_pods": 3,
                        "vram_investment_high_vram_nodes_preserved": 1,
                        "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                        "simulator_endpoint_count": 1,
                        "simulator_probe_checked_count": 1,
                        "simulator_probe_ready_count": 1,
                        "simulator_readiness": "configured_not_probed",
                    },
                },
            }
        )
        self.assertIn("demo gate smoke", summary)
        self.assertIn("production blocker: watch not healthy", summary)
        self.assertIn("class: kubernetes_watch", summary)
        self.assertIn("last error at: 2026-07-06T07:00:00Z", summary)
        self.assertIn("debug command: kubectl --request-timeout=10s get --raw='/readyz?verbose'", summary)
        self.assertIn("simulator: configured_not_probed (1/1 ready, 1 endpoint(s))", summary)
        self.assertIn("VRAM: Shadow advisory only", summary)
        self.assertIn(
            "VRAM claim-safe drivers: 7 (layer count, parameter memory x precision, parameter count)",
            summary,
        )
        self.assertIn(
            "VRAM synthetic headroom drivers: 1 (synthetic VRAM headroom probe)",
            summary,
        )
        self.assertIn("production class: kubernetes_watch", summary)
        self.assertIn("next VRAM evidence: true CUDA OOM labels", summary)
        self.assertIn("VRAM demo: 6 rows, 3 OOM-risk pods reduced, 1 high-VRAM preserved", summary)
        self.assertIn("primary blocker: customer claim not ready", summary)
        self.assertIn("next action: resolve launch proof gaps before making customer-facing claims", summary)

    def test_printable_summary_prefers_operator_status(self):
        summary = demo_gate.printable_summary(
            {
                "ok": False,
                "stage": "smoke",
                "output_dir": "/tmp/bundle",
                "readiness_probe": {
                    "production_readiness": {
                        "blocker": "watch not healthy",
                        "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                    },
                    "operator_status": {
                        "primary_blocker": "production readiness blocked: kubernetes_watch",
                        "next_action": "restore Kubernetes API connectivity",
                        "debug_commands": ["kubectl config current-context"],
                        "proof_gates": {
                            "total": 3,
                            "pass": 1,
                            "warn": 1,
                            "blocked": 1,
                        },
                        "binding_safety": {
                            "status": "dry-run-validation",
                            "reservation_pressure": "active",
                            "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                            "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                            "reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                            "reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
                        },
                        "vram": {
                            "model_driver_count": 8,
                            "top_driver_labels": [
                                "layer count",
                                "parameter memory x precision",
                                "synthetic reserve pressure",
                            ],
                            "claim_safe_driver_count": 7,
                            "claim_safe_driver_labels": [
                                "layer count",
                                "parameter memory x precision",
                                "parameter count",
                            ],
                            "real_model_driver_count": 7,
                            "real_top_driver_labels": [
                                "layer count",
                                "parameter memory x precision",
                                "parameter count",
                            ],
                            "synthetic_driver_count": 1,
                            "synthetic_driver_labels": ["synthetic reserve pressure"],
                        },
                    },
                    "evidence_summary": {
                        "primary_claim_blocker": "customer claim not ready",
                        "primary_claim_blocker_next_action": "resolve launch proof gaps",
                    },
                },
            }
        )
        self.assertIn("debug command: kubectl config current-context", summary)
        self.assertIn(
            "VRAM claim-safe drivers: 7 (layer count, parameter memory x precision, parameter count)",
            summary,
        )
        self.assertIn(
            "VRAM synthetic headroom drivers: 1 (synthetic VRAM headroom probe)",
            summary,
        )
        self.assertIn("proof gates: 1 pass, 1 warn, 1 blocked", summary)
        self.assertIn("binding reservation pressure: active", summary)
        self.assertIn("binding reservation pressure reason: 1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run", summary)
        self.assertIn(
            "primary blocker: production readiness blocked: kubernetes_watch",
            summary,
        )
        self.assertIn("next action: restore Kubernetes API connectivity", summary)

    def test_printable_summary_does_not_invent_missing_simulator_counts(self):
        summary = demo_gate.printable_summary(
            {
                "ok": False,
                "stage": "smoke",
                "output_dir": "/tmp/bundle",
                "review_ready": False,
                "readiness_probe": {
                    "simulator_readiness": {
                        "readiness": "configured_not_probed",
                    },
                },
            }
        )
        self.assertIn("simulator: configured_not_probed (unknown endpoint(s))", summary)

    def test_printable_summary_names_kss_ready_count(self):
        summary = demo_gate.printable_summary(
            {
                "ok": False,
                "stage": "smoke",
                "output_dir": "/tmp/bundle",
                "review_ready": False,
                "kss_ready_urls": "http://127.0.0.1:12130,http://127.0.0.1:12131",
                "kss_ready_count": 2,
            }
        )
        self.assertIn("KSS: 2 ready", summary)

    def test_printable_summary_names_doctor_context(self):
        summary = demo_gate.printable_summary(
            {
                "ok": False,
                "stage": "doctor-preflight",
                "output_dir": "/tmp/bundle",
                "review_ready": False,
                "failed_command": "scripts/kss-pool.sh status 2 12130 /tmp/cache",
                "failed_returncode": 2,
                "failed_stderr_excerpt": "no ready kube-scheduler-simulator endpoints",
                "parse_error": "Expecting value",
                "doctor_status": "blocked",
                "doctor_first_api_endpoint_failure": {
                    "endpoint": "/api/scheduler/operator-status",
                    "command": "curl -fsS http://shadow/api/scheduler/operator-status",
                },
                "doctor_first_recommended_command": "scripts/kss-pool.sh status 2 12130 /tmp/cache",
            }
        )

        self.assertIn("demo gate doctor-preflight", summary)
        self.assertIn("doctor: blocked", summary)
        self.assertIn("doctor API failure: /api/scheduler/operator-status", summary)
        self.assertIn("doctor API command: curl -fsS http://shadow/api/scheduler/operator-status", summary)
        self.assertIn("doctor first command: scripts/kss-pool.sh status 2 12130 /tmp/cache", summary)
        self.assertIn("failed command: scripts/kss-pool.sh status 2 12130 /tmp/cache", summary)
        self.assertIn("failed returncode: 2", summary)
        self.assertIn("stderr: no ready kube-scheduler-simulator endpoints", summary)
        self.assertIn("parse error: Expecting value", summary)

    def test_printable_summary_names_blocked_simulator_claim(self):
        summary = demo_gate.printable_summary(
            {
                "ok": False,
                "stage": "simulator-claim-blocked",
                "output_dir": "/tmp/bundle",
                "review_ready": True,
                "simulator_claim_ready": False,
                "simulator_claim_mode": "baseline-proof-blocked",
                "simulator_claim_blocker": "no kube-scheduler-simulator endpoint answered /api/v1/export",
            }
        )

        self.assertIn("demo gate simulator-claim-blocked", summary)
        self.assertIn(
            "simulator claim: baseline-proof-blocked "
            "(blocked: no kube-scheduler-simulator endpoint answered /api/v1/export)",
            summary,
        )

    def test_compact_result_keeps_operator_contract_without_nested_payloads(self):
        compact = demo_gate.compact_result(
            {
                "ok": False,
                "stage": "simulator-claim-blocked",
                "exit_code": 2,
                "base_url": "http://shadow",
                "output_dir": "/tmp/bundle",
                "failed_command": "scripts/kss-pool.sh require-ready-urls 2 12130 /tmp/cache",
                "failed_returncode": 2,
                "failed_stdout_excerpt": "probe output",
                "failed_stderr_excerpt": "probe error",
                "parse_error": "Expecting value",
                "review_ready": True,
                "require_review_ready": False,
                "require_simulator_claim_ready": True,
                "readiness_mode": "degraded",
                "readiness_blocker_class": "kubernetes_watch",
                "primary_claim_blocker": "production readiness blocked: kubernetes_watch",
                "primary_claim_blocker_next_action": "restore Kubernetes API connectivity",
                "doctor_status": "blocked",
                "doctor_failures": ["no kube-scheduler-simulator endpoint is ready"],
                "doctor_first_recommended_command": "scripts/kss-pool.sh status 2 12130 /tmp/cache",
                "doctor_recommended_commands": [
                    {
                        "category": "kube-scheduler-simulator",
                        "severity": "blocked",
                        "command": "scripts/kss-pool.sh status 2 12130 /tmp/cache",
                    }
                ],
                "doctor_api_endpoint_failure_count": 1,
                "doctor_first_api_endpoint_failure": {
                    "category": "shadow-api",
                    "severity": "blocked",
                    "endpoint": "/api/scheduler/evidence-bundle",
                    "command": "curl -fsS http://shadow/api/scheduler/evidence-bundle",
                    "reason": "evidence-bundle endpoint did not return a valid JSON object",
                },
                "missing_live_artifact_count": 4,
                "missing_live_artifact_blocked_count": 3,
                "missing_live_artifact_warn_count": 1,
                "operator_runbook": {
                    "step_count": 4,
                    "copyable_command_count": 6,
                    "manual_step_count": 1,
                    "next_shell_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                    "copyable_command_rows": [
                        {
                            "command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                            "priority": 1,
                            "category": "environment",
                            "severity": "blocked",
                            "artifact": "healthy Kubernetes watch/relist state",
                            "next_action": "restore Kubernetes API connectivity",
                            "command_kind": "shell",
                        }
                    ],
                },
                "operator_binding_status": "dry-run-validation",
                "operator_reservation_pressure": "active",
                "operator_reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                "operator_reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                "operator_reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                "operator_reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
                "production_readiness_blocker_class": "kubernetes_watch",
                "production_readiness_last_error_class": "api_connect",
                "production_readiness_first_debug_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "simulator_readiness": "ready",
                "simulator_endpoint_count": 1,
                "simulator_probe_checked_count": 1,
                "simulator_probe_ready_count": 1,
                "simulator_claim_ready": False,
                "simulator_claim_mode": "baseline-proof-blocked",
                "simulator_claim_blocker": "no kube-scheduler-simulator endpoint answered /api/v1/export",
                "simulator_claim_next_action": "start the KSS pool",
                "vram_admission_mode": "Shadow advisory only",
                "vram_next_evidence_target": "true CUDA OOM labels",
                "vram_claim_safe_driver_count": 8,
                "vram_claim_safe_driver_labels": [
                    "layer count",
                    "parameter memory x precision",
                    "parameter count",
                    "batch size",
                    "activation footprint",
                    "extra ignored label",
                ],
                "vram_synthetic_driver_count": 2,
                "vram_synthetic_driver_labels": [
                    "synthetic reserve pressure",
                    "synthetic transformer reserve pressure",
                ],
                "vram_investment_demo_rows": 6,
                "vram_investment_oom_risk_reduction_pods": 3,
                "smoke": {"large": "payload"},
                "collection": {"large": "payload"},
                "verification": {"large": "payload"},
            }
        )

        self.assertEqual(compact["stage"], "simulator-claim-blocked")
        self.assertEqual(compact["exit_code"], 2)
        self.assertEqual(
            compact["failed_command"],
            "scripts/kss-pool.sh require-ready-urls 2 12130 /tmp/cache",
        )
        self.assertEqual(compact["failed_returncode"], 2)
        self.assertEqual(compact["failed_stdout_excerpt"], "probe output")
        self.assertEqual(compact["failed_stderr_excerpt"], "probe error")
        self.assertEqual(compact["parse_error"], "Expecting value")
        self.assertTrue(compact["require_simulator_claim_ready"])
        self.assertEqual(compact["doctor_status"], "blocked")
        self.assertEqual(compact["doctor_failures"], ["no kube-scheduler-simulator endpoint is ready"])
        self.assertEqual(compact["doctor_first_recommended_command"], "scripts/kss-pool.sh status 2 12130 /tmp/cache")
        self.assertEqual(compact["doctor_recommended_commands"][0]["category"], "kube-scheduler-simulator")
        self.assertEqual(compact["doctor_api_endpoint_failure_count"], 1)
        self.assertEqual(
            compact["doctor_first_api_endpoint_failure"]["endpoint"],
            "/api/scheduler/evidence-bundle",
        )
        self.assertEqual(compact["operator_runbook_step_count"], 4)
        self.assertEqual(
            compact["operator_next_shell_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(
            compact["operator_first_shell_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(compact["operator_first_shell_command_category"], "environment")
        self.assertEqual(compact["operator_first_shell_command_severity"], "blocked")
        self.assertEqual(
            compact["operator_first_shell_command_artifact"],
            "healthy Kubernetes watch/relist state",
        )
        self.assertEqual(
            compact["operator_first_shell_command_next_action"],
            "restore Kubernetes API connectivity",
        )
        self.assertEqual(compact["operator_first_shell_command_kind"], "shell")
        self.assertEqual(compact["operator_binding_status"], "dry-run-validation")
        self.assertEqual(compact["operator_reservation_pressure"], "active")
        self.assertIn("pending or reserved GPU capacity", compact["operator_reservation_pressure_description"])
        self.assertIn("unrelated to CUDA", compact["operator_reservation_pressure_scope"])
        self.assertIn("hold 4 GPU", compact["operator_reservation_pressure_reason"])
        self.assertEqual(compact["simulator_claim_mode"], "baseline-proof-blocked")
        self.assertEqual(compact["vram_claim_safe_driver_count"], 8)
        self.assertEqual(len(compact["vram_claim_safe_driver_labels"]), 5)
        self.assertEqual(len(compact["vram_display_claim_safe_driver_labels"]), 5)
        self.assertEqual(
            compact["vram_display_synthetic_driver_labels"],
            ["synthetic VRAM headroom probe", "synthetic transformer headroom probe"],
        )
        self.assertIn("summary", compact)
        self.assertNotIn("smoke", compact)
        self.assertNotIn("collection", compact)
        self.assertNotIn("verification", compact)


if __name__ == "__main__":
    unittest.main()
