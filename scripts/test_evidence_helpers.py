#!/usr/bin/env python3
"""Unit tests for shared shadow evidence helper functions."""

from __future__ import annotations

import pathlib
import sys
import unittest


SCRIPT_DIR = pathlib.Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import evidence_helpers


class EvidenceHelpersTests(unittest.TestCase):
    def test_category_rows_prioritize_environment_before_other_blockers(self) -> None:
        rows = [
            {
                "category": "repair-proof",
                "severity": "blocked",
                "artifact": "repair rows",
                "proof_gate": "repair action safety",
                "next_action": "capture repair proof",
            },
            {
                "category": "environment",
                "severity": "blocked",
                "artifact": "healthy Kubernetes watch",
                "proof_gate": "production mutation safety",
                "next_action": "run readyz",
            },
            {
                "category": "customer-proof",
                "severity": "warn",
                "artifact": "pricing source",
                "proof_gate": "ROI pricing evidence",
                "next_action": "attach pricing",
            },
            {"category": "environment", "severity": "warn"},
            "ignored",
        ]

        categories = evidence_helpers.missing_artifact_category_rows(rows)

        self.assertEqual([row["category"] for row in categories], [
            "environment",
            "repair-proof",
            "customer-proof",
        ])
        self.assertEqual(categories[0]["total"], 2)
        self.assertEqual(categories[0]["blocked"], 1)
        self.assertEqual(categories[0]["warn"], 1)
        self.assertEqual(categories[0]["severity"], "blocked")
        self.assertEqual(categories[0]["artifact"], "healthy Kubernetes watch")
        self.assertEqual(categories[0]["next_action"], "run readyz")

    def test_action_items_and_runbook_expose_copyable_shell_commands_once(self) -> None:
        category_rows = [
            {
                "category": "environment",
                "severity": "blocked",
                "blocked": 1,
                "warn": 0,
                "artifact": "healthy Kubernetes watch",
                "next_action": "restore API connectivity",
            },
            {
                "category": "live-trace",
                "severity": "blocked",
                "blocked": 1,
                "warn": 0,
                "artifact": "pending trace",
                "next_action": "apply pending GPU scenario",
            },
            {
                "category": "customer-proof",
                "severity": "warn",
                "blocked": 0,
                "warn": 1,
                "artifact": "pricing source",
                "next_action": "attach pricing source",
            },
        ]

        action_items = evidence_helpers.missing_artifact_action_items(category_rows)
        runbook = evidence_helpers.operator_action_runbook(action_items)

        self.assertEqual(action_items[0]["command_hint"], "kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        self.assertEqual(action_items[0]["command_kind"], "shell")
        self.assertTrue(action_items[0]["copyable"])
        self.assertEqual(action_items[2]["command_kind"], "manual")
        self.assertFalse(action_items[2]["copyable"])
        self.assertEqual(runbook["step_count"], 3)
        self.assertEqual(runbook["blocked_step_count"], 2)
        self.assertEqual(runbook["manual_step_count"], 1)
        self.assertEqual(runbook["next_shell_command"], "kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        self.assertEqual(
            runbook["copyable_command_rows"][0]["command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(runbook["copyable_command_rows"][0]["category"], "environment")
        self.assertEqual(
            runbook["copyable_command_rows"][0]["next_action"],
            "restore API connectivity",
        )
        legacy_runbook = dict(runbook)
        legacy_runbook.pop("copyable_command_rows")
        self.assertEqual(
            evidence_helpers.operator_runbook_command_rows(legacy_runbook)[0]["command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertIn(
            "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
            runbook["copyable_commands"],
        )

    def test_environment_command_hints_follow_next_action(self) -> None:
        self.assertEqual(
            evidence_helpers.environment_command_hints("restore Kubernetes API connectivity")[0],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(
            evidence_helpers.environment_command_hints("verify RBAC list/watch permissions")[0],
            "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
        )
        self.assertEqual(
            evidence_helpers.environment_command_hints("collect generic environment proof")[0],
            "kubectl config current-context",
        )

    def test_counts_text_omits_zeroes_and_sorts_by_count_then_name(self) -> None:
        self.assertEqual(
            evidence_helpers.category_counts_text(
                {
                    "repair-proof": 1,
                    "environment": 2,
                    "customer-proof": 0,
                    "live-trace": 1,
                }
            ),
            "environment 2, live-trace 1, repair-proof 1",
        )

    def test_display_vram_driver_labels_use_operator_terms(self) -> None:
        self.assertEqual(
            evidence_helpers.display_vram_driver_labels(
                [
                    "layer count",
                    "synthetic reserve pressure",
                    "synthetic transformer reserve pressure",
                ]
            ),
            [
                "layer count",
                "synthetic VRAM headroom probe",
                "synthetic transformer headroom probe",
            ],
        )

    def test_synthetic_headroom_driver_helper_prefers_new_alias(self) -> None:
        self.assertTrue(
            evidence_helpers.synthetic_headroom_driver_enabled(
                {"synthetic_headroom_driver": True, "synthetic_reserve_driver": False}
            )
        )
        self.assertTrue(
            evidence_helpers.synthetic_headroom_driver_enabled(
                {"synthetic_reserve_driver": True}
            )
        )
        self.assertFalse(
            evidence_helpers.synthetic_headroom_driver_enabled(
                {"synthetic_headroom_driver": False, "synthetic_reserve_driver": True}
            )
        )
        self.assertTrue(
            evidence_helpers.synthetic_headroom_driver_enabled(
                {"vram_synthetic_headroom_driver": True, "vram_synthetic_reserve_driver": False}
            )
        )
        self.assertTrue(
            evidence_helpers.synthetic_headroom_driver_enabled(
                {"synthetic_headroom_driver": None, "synthetic_reserve_driver": True}
            )
        )
        self.assertIsNone(evidence_helpers.synthetic_headroom_driver_value({}))


if __name__ == "__main__":
    unittest.main()
