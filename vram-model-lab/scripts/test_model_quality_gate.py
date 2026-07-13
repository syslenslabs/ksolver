"""Guard: the COMMITTED VRAM model must satisfy its own declared quality gate.

The peak-VRAM model (`data/models/peak_vram_linear.json`) is refit offline as new
RTX-4090 samples land. Nothing currently fails if a refit lands a model that no longer
meets the accuracy bar the project advertises — it would silently ship a worse estimator.

This test reads only the committed artifacts (no refit, no data mutation) and asserts:
  1. the committed model actually satisfies the thresholds in its own `quality_gate` string;
  2. that gate has not been silently loosened past the documented policy ceilings; and
  3. the two committed artifacts (`peak_vram_linear.json` and `evaluation.json`) agree on
     the leave-one-out error, so a stale evaluation summary can't mask a regressed model.
"""
from __future__ import annotations

import json
import re
import unittest
from pathlib import Path

MODELS_DIR = Path(__file__).resolve().parent.parent / "data" / "models"
MODEL_PATH = MODELS_DIR / "peak_vram_linear.json"
EVALUATION_PATH = MODELS_DIR / "evaluation.json"

# Documented policy ceilings the gate must not be weakened beyond. These mirror the
# `quality_gate` string committed alongside the model; if a refit loosens the gate, the
# assertions in `test_gate_not_loosened_past_policy` flag it rather than letting it pass.
POLICY_MIN_ROWS = 8
POLICY_MAX_LOO_P95_MIB = 5000
POLICY_MAX_LOO_MAX_MIB = 25000

# Floating-point tolerance for cross-artifact numeric agreement (MiB).
AGREEMENT_TOLERANCE_MIB = 1e-6


def _parse_gate(gate: str) -> dict[str, float]:
    """Extract the numeric thresholds the model declares in its `quality_gate` string,
    e.g. "rows>=8 and loo_p95<=5000MiB and loo_max<=25000MiB"."""
    rows = re.search(r"rows\s*>=\s*(\d+)", gate)
    p95 = re.search(r"loo_p95\s*<=\s*(\d+)\s*MiB", gate)
    mx = re.search(r"loo_max\s*<=\s*(\d+)\s*MiB", gate)
    if not (rows and p95 and mx):
        raise AssertionError(f"quality_gate string not in the expected shape: {gate!r}")
    return {
        "min_rows": float(rows.group(1)),
        "max_loo_p95_mib": float(p95.group(1)),
        "max_loo_max_mib": float(mx.group(1)),
    }


class ModelQualityGateTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.model = json.loads(MODEL_PATH.read_text())
        cls.evaluation = json.loads(EVALUATION_PATH.read_text())

    def test_committed_model_satisfies_its_declared_gate(self) -> None:
        gate = _parse_gate(self.model["quality_gate"])
        rows = self.model["training_rows"]
        loo_p95 = self.model["leave_one_out_abs_error_p95_mib"]
        loo_max = self.model["leave_one_out_max_absolute_error_mib"]

        self.assertGreaterEqual(
            rows, gate["min_rows"],
            f"committed model was fit on {rows} rows, below its own gate of "
            f">= {gate['min_rows']:.0f}",
        )
        self.assertLessEqual(
            loo_p95, gate["max_loo_p95_mib"],
            f"leave-one-out p95 error {loo_p95:.1f} MiB exceeds the model's gate of "
            f"<= {gate['max_loo_p95_mib']:.0f} MiB — refit regressed accuracy",
        )
        self.assertLessEqual(
            loo_max, gate["max_loo_max_mib"],
            f"leave-one-out max error {loo_max:.1f} MiB exceeds the model's gate of "
            f"<= {gate['max_loo_max_mib']:.0f} MiB — refit regressed accuracy",
        )
        self.assertTrue(
            self.model.get("usable_for_prediction"),
            "committed model is not flagged usable_for_prediction",
        )

    def test_gate_not_loosened_past_policy(self) -> None:
        gate = _parse_gate(self.model["quality_gate"])
        self.assertGreaterEqual(
            gate["min_rows"], POLICY_MIN_ROWS,
            "quality_gate row floor was weakened below documented policy",
        )
        self.assertLessEqual(
            gate["max_loo_p95_mib"], POLICY_MAX_LOO_P95_MIB,
            "quality_gate p95 ceiling was loosened past documented policy",
        )
        self.assertLessEqual(
            gate["max_loo_max_mib"], POLICY_MAX_LOO_MAX_MIB,
            "quality_gate max-error ceiling was loosened past documented policy",
        )

    def test_evaluation_summary_agrees_with_model(self) -> None:
        g = self.evaluation["global"]
        self.assertAlmostEqual(
            g["loo_p95_abs_error_mib"],
            self.model["leave_one_out_abs_error_p95_mib"],
            delta=AGREEMENT_TOLERANCE_MIB,
            msg="evaluation.json LOO p95 disagrees with the model — stale summary",
        )
        self.assertAlmostEqual(
            g["loo_max_abs_error_mib"],
            self.model["leave_one_out_max_absolute_error_mib"],
            delta=AGREEMENT_TOLERANCE_MIB,
            msg="evaluation.json LOO max disagrees with the model — stale summary",
        )
        self.assertEqual(
            g["training_rows"], self.model["training_rows"],
            "evaluation.json training_rows disagrees with the model — stale summary",
        )

    def test_group_aware_generalization_within_policy(self) -> None:
        """The HONEST generalization must ALSO stay within the policy ceilings — not just the committed
        row-LOO, which is optimistic because the training data has many near-duplicate rows (repeated
        samples per config), so row-LOO leaks a config's twins between train and test. This recomputes
        leave-one-CONFIG-GROUP-out CV (holding out ALL rows of a config) and asserts real generalization
        to novel configs stays within policy — catching a refit that degrades true accuracy even if the
        leaky row-LOO still looks fine. Reproduces `group_aware_eval.py`. Read-only (no refit/mutation)."""
        try:
            import numpy as np

            import fit_peak_vram_model as fm
        except Exception as exc:  # minimal env without numpy/the fit module
            self.skipTest(f"numpy/fit_peak_vram_model unavailable: {exc}")

        alpha = float(self.model.get("alpha", 25.0))
        mode = self.model.get("feature_mode", "interactions")
        rows = fm.load_rows()
        x = np.array([fm.features(r, mode=mode) for r in rows], dtype=float)
        y = np.array([float(r["nvidia_smi_peak_used_mib"]) for r in rows], dtype=float)

        # One group per distinct (rounded) feature vector = one config; hold out the whole group.
        groups: dict[tuple, list[int]] = {}
        for i, row in enumerate(x):
            groups.setdefault(tuple(np.round(row, 6)), []).append(i)
        errs = []
        for members in groups.values():
            mask = np.ones(len(y), dtype=bool)
            for i in members:
                mask[i] = False
            coef = fm.fit_ridge(x[mask], y[mask], alpha=alpha)
            for i in members:
                errs.append(abs(float(x[i] @ coef) - y[i]))
        errs = np.array(errs)
        p95 = float(np.percentile(errs, 95))
        mx = float(errs.max())
        self.assertLessEqual(
            p95, POLICY_MAX_LOO_P95_MIB,
            f"group-aware (honest) p95 {p95:.0f} MiB exceeds policy {POLICY_MAX_LOO_P95_MIB} — "
            "real generalization to novel configs regressed (the leaky row-LOO may still look fine)",
        )
        self.assertLessEqual(
            mx, POLICY_MAX_LOO_MAX_MIB,
            f"group-aware (honest) max {mx:.0f} MiB exceeds policy {POLICY_MAX_LOO_MAX_MIB} — "
            "real generalization to novel configs regressed",
        )


if __name__ == "__main__":
    unittest.main()
