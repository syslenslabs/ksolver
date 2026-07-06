#!/usr/bin/env python3
"""Emit a compact model evaluation report for scheduler integration."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "data" / "models" / "peak_vram_linear.json"
OUT = ROOT / "data" / "models" / "evaluation.json"


def compact(model: dict) -> dict:
    feature_impacts = model.get("feature_impacts") or []
    group_impacts = model.get("group_impacts") or []
    return {
        "name": model.get("name"),
        "fit": model.get("fit"),
        "alpha": model.get("alpha"),
        "feature_mode": model.get("feature_mode"),
        "training_rows": model.get("training_rows"),
        "usable_for_prediction": model.get("usable_for_prediction"),
        "quality_gate": model.get("quality_gate"),
        "in_sample_mae_mib": model.get("in_sample_mean_absolute_error_mib"),
        "loo_mae_mib": model.get("leave_one_out_mean_absolute_error_mib"),
        "loo_p95_abs_error_mib": model.get("leave_one_out_abs_error_p95_mib"),
        "loo_max_abs_error_mib": model.get("leave_one_out_max_absolute_error_mib"),
        "top_driver_labels": model.get("top_driver_labels") or [
            row.get("description") or row.get("feature")
            for row in feature_impacts[:5]
        ],
        "top_organic_driver_labels": model.get("top_organic_driver_labels") or [
            row.get("description") or row.get("feature")
            for row in feature_impacts
            if row.get("group") != "synthetic headroom"
        ][:5],
        "top_feature_impacts": feature_impacts[:8],
        "top_group_impacts": group_impacts[:6],
    }


def main() -> int:
    if not MODEL.exists():
        raise SystemExit(f"missing model: {MODEL}")
    artifact = json.loads(MODEL.read_text())
    families = artifact.get("family_models") or {}
    usable = {
        family: compact(model)
        for family, model in sorted(families.items())
        if model.get("usable_for_prediction")
    }
    fallback = {
        family: compact(model)
        for family, model in sorted(families.items())
        if not model.get("usable_for_prediction")
    }
    report = {
        "schema_version": 1,
        "model": str(MODEL.relative_to(ROOT)),
        "selection": artifact.get("selection"),
        "global": compact(artifact.get("global") or artifact),
        "usable_family_models": usable,
        "fallback_family_models": fallback,
        "ready_for_scheduler_demo": set(usable.keys()) >= {"transformer", "cnn", "mlp"},
    }
    OUT.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
