#!/usr/bin/env python3
"""Unit tests for shadow-doctor.py."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import unittest


SCRIPT_PATH = pathlib.Path(__file__).with_name("shadow-doctor.py")
SPEC = importlib.util.spec_from_file_location("shadow_doctor", SCRIPT_PATH)
shadow_doctor = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(shadow_doctor)


def json_body(payload: dict) -> bytes:
    return json.dumps(payload, sort_keys=True).encode("utf-8")


class ShadowDoctorTests(unittest.TestCase):
    def test_split_urls_trims_blanks(self):
        self.assertEqual(
            shadow_doctor.split_urls(" http://a:1, ,http://b:2/ "),
            ["http://a:1", "http://b:2"],
        )

    def test_shell_join_quotes_unsafe_arguments(self):
        self.assertEqual(
            shadow_doctor.shell_join(["curl", "-fsS", "http://shadow.local/root path?a=1&b=2"]),
            "curl -fsS 'http://shadow.local/root path?a=1&b=2'",
        )
        self.assertEqual(
            shadow_doctor.kss_pool_command("status", 2, 12130, "/tmp/cache dir"),
            "scripts/kss-pool.sh status 2 12130 '/tmp/cache dir'",
        )

    def test_api_failure_command_quotes_base_url(self):
        rows = shadow_doctor.api_endpoint_failure_rows(
            healthz_ok=True,
            production={"ok": False, "status": 500, "error": None},
            operator={"ok": True},
            evidence={"ok": True},
            base_url="http://shadow.local/root path",
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(
            rows[0]["command"],
            "curl -fsS 'http://shadow.local/root path/api/scheduler/production-safety'",
        )

    def test_diagnose_allows_degraded_readyz_by_default(self):
        responses = {
            "http://shadow/healthz": (200, b"ok", None),
            "http://shadow/readyz": (503, b"watch not healthy", None),
            "http://shadow/api/scheduler/production-safety": (
                200,
                json_body(
                    {
                        "readiness": {
                            "blocker_class": "kubernetes_watch",
                            "last_error_class": "api_connect",
                            "next_action": "restore Kubernetes API connectivity",
                            "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                        }
                    }
                ),
                None,
            ),
            "http://shadow/api/scheduler/operator-status": (
                200,
                json_body(
                    {
                        "next_action": "restore Kubernetes API connectivity",
                        "simulator": {
                            "claim_ready": True,
                            "claim_mode": "live-kube-scheduler-simulator-ready",
                            "claim_next_action": "safe to use live baseline",
                        },
                    }
                ),
                None,
            ),
            "http://shadow/api/scheduler/evidence-bundle": (
                200,
                json_body({"summary": {"simulator_claim_ready": True}}),
                None,
            ),
            "http://kss:12120/api/v1/export": (200, json_body({"kind": "Export"}), None),
        }

        def fetcher(url, timeout):
            return responses.get(url, (None, b"", "missing"))

        result = shadow_doctor.diagnose(
            base_url="http://shadow",
            kss_urls=["http://kss:12120"],
            timeout=1.0,
            require_readyz=False,
            require_kss_ready=True,
            require_simulator_claim_ready=True,
            fetcher=fetcher,
        )

        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "degraded")
        self.assertFalse(result["readyz_ok"])
        self.assertEqual(result["production_readiness_blocker_class"], "kubernetes_watch")
        self.assertEqual(result["production_readiness_last_error_class"], "api_connect")
        self.assertTrue(result["simulator_claim_ready"])
        self.assertEqual(result["simulator_claim_mode"], "live-kube-scheduler-simulator-ready")
        self.assertEqual(result["kss_ready_count"], 1)
        self.assertEqual(
            result["first_debug_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(
            result["first_recommended_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(result["recommended_commands"][0]["category"], "kubernetes-readiness")

    def test_diagnose_fails_when_readyz_is_required(self):
        def fetcher(url, timeout):
            if url.endswith("/healthz"):
                return 200, b"ok", None
            if url.endswith("/readyz"):
                return 503, b"watch not healthy", None
            if url.endswith("/api/v1/export"):
                return 200, json_body({"kind": "Export"}), None
            if url.endswith("/api/scheduler/production-safety"):
                return 200, json_body({"readiness": {}}), None
            if url.endswith("/api/scheduler/operator-status"):
                return 200, json_body({"simulator": {"claim_ready": True}}), None
            if url.endswith("/api/scheduler/evidence-bundle"):
                return 200, json_body({"summary": {}}), None
            return 404, b"", "missing"

        result = shadow_doctor.diagnose(
            base_url="http://shadow",
            kss_urls=["http://kss:12120"],
            timeout=1.0,
            require_readyz=True,
            require_kss_ready=False,
            require_simulator_claim_ready=False,
            fetcher=fetcher,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "blocked")
        self.assertIn("shadow readyz is not ready", result["failures"])

    def test_diagnose_fails_when_shadow_api_payloads_are_invalid(self):
        responses = {
            "http://shadow/healthz": (200, b"ok", None),
            "http://shadow/readyz": (200, b"ready", None),
            "http://shadow/api/scheduler/production-safety": (200, b"", None),
            "http://shadow/api/scheduler/operator-status": (200, b"not-json", None),
            "http://shadow/api/scheduler/evidence-bundle": (503, b"", None),
            "http://kss:12120/api/v1/export": (200, json_body({"kind": "Export"}), None),
        }

        def fetcher(url, timeout):
            return responses.get(url, (None, b"", "missing"))

        result = shadow_doctor.diagnose(
            base_url="http://shadow",
            kss_urls=["http://kss:12120"],
            timeout=1.0,
            require_readyz=False,
            require_kss_ready=True,
            require_simulator_claim_ready=False,
            fetcher=fetcher,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(
            result["failures"],
            [
                "production-safety endpoint did not return a valid JSON object",
                "operator-status endpoint did not return a valid JSON object",
                "evidence-bundle endpoint did not return a valid JSON object",
            ],
        )
        self.assertEqual(len(result["api_endpoint_failures"]), 3)
        self.assertEqual(result["api_endpoint_failures"][0]["category"], "shadow-api")
        self.assertEqual(result["api_endpoint_failures"][0]["endpoint"], "/api/scheduler/production-safety")
        self.assertEqual(
            result["first_recommended_command"],
            "curl -fsS http://shadow/api/scheduler/production-safety",
        )
        self.assertIn(
            "curl -fsS http://shadow/api/scheduler/operator-status",
            [row.get("command") for row in result["recommended_commands"]],
        )

    def test_diagnose_fails_when_simulator_claim_is_required(self):
        responses = {
            "http://shadow/healthz": (200, b"ok", None),
            "http://shadow/readyz": (200, b"ready", None),
            "http://shadow/api/scheduler/production-safety": (200, json_body({"readiness": {}}), None),
            "http://shadow/api/scheduler/operator-status": (
                200,
                json_body(
                    {
                        "simulator": {
                            "claim_ready": False,
                            "claim_mode": "baseline-proof-blocked",
                            "claim_blocker": "no kube-scheduler-simulator endpoint answered /api/v1/export",
                            "claim_next_action": "start the KSS pool",
                        },
                        "decision_readiness": {
                            "status": "needs-action",
                            "summary": "demo=ready, claim=blocked, vram-score=ready, hard-admit=blocked, bind=read-only",
                            "highest_risk": "kube-scheduler baseline is not customer-claim ready",
                            "next_action": "repair kube-scheduler-simulator before making kube-vs-ksolver claims",
                            "capabilities": [
                                {
                                    "name": "production_binding",
                                    "label": "Production binding",
                                    "status": "read-only",
                                    "can_execute": False,
                                    "next_action": "enable real binding only after ownership, RBAC, canary, reservation, and kill-switch gates are approved",
                                }
                            ],
                        },
                            "binding_safety": {
                                "reservation_pressure": "active",
                                "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                                "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                                "reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                            "reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
                        },
                    }
                ),
                None,
            ),
            "http://shadow/api/scheduler/evidence-bundle": (200, json_body({"summary": {}}), None),
            "http://kss:12120/api/v1/export": (503, b"", None),
        }

        def fetcher(url, timeout):
            return responses.get(url, (None, b"", "missing"))

        result = shadow_doctor.diagnose(
            base_url="http://shadow",
            kss_urls=["http://kss:12120"],
            timeout=1.0,
            require_readyz=False,
            require_kss_ready=False,
            require_simulator_claim_ready=True,
            fetcher=fetcher,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["simulator_claim_mode"], "baseline-proof-blocked")
        self.assertEqual(
            result["simulator_claim_blocker"],
            "no kube-scheduler-simulator endpoint answered /api/v1/export",
        )
        self.assertEqual(result["kss_ready_count"], 0)
        self.assertEqual(result["operator_decision_status"], "needs-action")
        self.assertEqual(result["operator_production_binding_status"], "read-only")
        self.assertEqual(result["operator_production_binding_can_execute"], False)
        self.assertEqual(result["operator_reservation_pressure"], "active")
        self.assertIn("pending or reserved GPU capacity", result["operator_reservation_pressure_description"])
        self.assertIn("unrelated to CUDA", result["operator_reservation_pressure_scope"])
        self.assertIn("hold 4 GPU", result["operator_reservation_pressure_reason"])
        self.assertIn("kube-scheduler baseline", result["operator_decision_highest_risk"])
        self.assertIn("simulator claim is not ready", result["failures"])

    def test_diagnose_fails_when_kss_ready_is_required(self):
        def fetcher(url, timeout):
            if url.endswith("/healthz"):
                return 200, b"ok", None
            if url.endswith("/readyz"):
                return 200, b"ready", None
            if url.endswith("/api/v1/export"):
                return 503, b"", None
            if url.endswith("/api/scheduler/production-safety"):
                return 200, json_body({"readiness": {}}), None
            if url.endswith("/api/scheduler/operator-status"):
                return 200, json_body({"simulator": {"claim_ready": True}}), None
            if url.endswith("/api/scheduler/evidence-bundle"):
                return 200, json_body({"summary": {}}), None
            return 404, b"", "missing"

        result = shadow_doctor.diagnose(
            base_url="http://shadow",
            kss_urls=["http://kss:12120"],
            timeout=1.0,
            require_readyz=False,
            require_kss_ready=True,
            require_simulator_claim_ready=False,
            kss_count=2,
            kss_base_port=12130,
            kss_cache_dir="/tmp/ksolver kss-cache-test",
            fetcher=fetcher,
        )

        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["kss_checked_count"], 1)
        self.assertEqual(result["kss_ready_count"], 0)
        self.assertIn("no kube-scheduler-simulator endpoint is ready", result["failures"])
        self.assertEqual(
            result["first_recommended_command"],
            "scripts/kss-pool.sh status 2 12130 '/tmp/ksolver kss-cache-test'",
        )
        self.assertEqual(
            [row["command"] for row in result["recommended_commands"] if row.get("command")],
            [
                "scripts/kss-pool.sh status 2 12130 '/tmp/ksolver kss-cache-test'",
                "scripts/kss-pool.sh start 2 12130 '/tmp/ksolver kss-cache-test'",
            ],
        )

    def test_recommended_commands_warn_when_kss_pool_is_partial(self):
        commands = shadow_doctor.recommended_commands(
            healthz_ok=True,
            readyz_ok=True,
            kss_ready_count=1,
            kss_checked_count=2,
            kss_count=2,
            kss_base_port=12130,
            kss_cache_dir="/tmp/cache",
            first_debug=None,
            simulator_claim_ready=True,
            simulator_claim_next_action=None,
        )

        self.assertEqual(len(commands), 1)
        self.assertEqual(commands[0]["severity"], "warn")
        self.assertEqual(commands[0]["command"], "scripts/kss-pool.sh status 2 12130 /tmp/cache")

    def test_printable_summary_includes_next_actions(self):
        summary = shadow_doctor.printable_summary(
            {
                "status": "degraded",
                "base_url": "http://shadow",
                "healthz_ok": True,
                "readyz_ok": False,
                "kss_ready_count": 1,
                "kss_checked_count": 2,
                "simulator_claim_ready": True,
                "simulator_claim_mode": "live-kube-scheduler-simulator-ready",
                "operator_decision_status": "needs-action",
                "operator_decision_highest_risk": "kube-scheduler baseline is not customer-claim ready",
                "operator_production_binding_status": "read-only",
                "operator_production_binding_can_execute": False,
                "operator_reservation_pressure": "active",
                "production_readiness_blocker_class": "kubernetes_watch",
                "api_endpoint_failures": [
                    {
                        "endpoint": "/api/scheduler/operator-status",
                        "command": "curl -fsS http://shadow/api/scheduler/operator-status",
                    }
                ],
                "first_debug_command": "kubectl get nodes",
                "first_recommended_command": "kubectl get nodes",
                "next_action": "restore Kubernetes API connectivity",
                "failures": [],
            }
        )

        self.assertIn("shadow doctor degraded", summary)
        self.assertIn("KSS=1/2 ready", summary)
        self.assertIn("simulator claim=live-kube-scheduler-simulator-ready (ready)", summary)
        self.assertIn("decision=needs-action", summary)
        self.assertIn("binding=read-only not-executable", summary)
        self.assertIn("binding reservation pressure=active", summary)
        self.assertIn("risk=kube-scheduler baseline is not customer-claim ready", summary)
        self.assertIn("production blocker=kubernetes_watch", summary)
        self.assertIn("API failures=1", summary)
        self.assertIn("first API failure=/api/scheduler/operator-status", summary)
        self.assertIn("API command=curl -fsS http://shadow/api/scheduler/operator-status", summary)
        self.assertIn("debug=kubectl get nodes", summary)
        self.assertIn("first command=kubectl get nodes", summary)

    def test_first_debug_command_falls_back_to_evidence_summary(self):
        command = shadow_doctor.first_debug_command(
            {},
            {"readiness": {}},
            {
                "summary": {
                    "production_readiness_first_debug_command": (
                        "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
                    )
                }
            },
        )

        self.assertEqual(command, "kubectl --request-timeout=10s get --raw='/readyz?verbose'")


if __name__ == "__main__":
    unittest.main()
