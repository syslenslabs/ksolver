#!/usr/bin/env python3
"""Predict peak VRAM for a proposed job from the fitted lab model."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np

from fit_peak_vram_model import features


ROOT = Path(__file__).resolve().parents[1]
MODEL = ROOT / "data" / "models" / "peak_vram_linear.json"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--family", choices=["mlp", "transformer", "cnn"], required=True)
    parser.add_argument("--precision", choices=["fp32", "fp16", "bf16", "int8"], default="fp16")
    parser.add_argument("--batch-size", type=int, required=True)
    parser.add_argument("--seq-len", type=int, default=0)
    parser.add_argument("--image-size", type=int, default=0)
    parser.add_argument("--hidden-size", type=int, required=True)
    parser.add_argument("--layers", type=int, required=True)
    parser.add_argument("--heads", type=int, default=0)
    parser.add_argument("--optimizer", choices=["adamw", "sgd"], default="adamw")
    parser.add_argument("--activation-checkpointing", action="store_true")
    parser.add_argument("--param-count", type=int, default=0)
    parser.add_argument("--reserve-extra-mib", type=int, default=0)
    parser.add_argument("--gpu-total-mib", type=int, default=24563)
    parser.add_argument("--safety-mib", type=int, default=None)
    args = parser.parse_args()

    if not MODEL.exists():
        raise SystemExit(f"missing model: {MODEL}; run fit_peak_vram_model.py first")
    artifact = json.loads(MODEL.read_text())
    row = {
        "family": args.family,
        "precision": args.precision,
        "batch_size": args.batch_size,
        "seq_len": args.seq_len or None,
        "image_size": args.image_size or None,
        "hidden_size": args.hidden_size,
        "layers": args.layers,
        "heads": args.heads or None,
        "optimizer": args.optimizer,
        "activation_checkpointing": args.activation_checkpointing,
        "param_count": args.param_count,
        "reserve_extra_mib": args.reserve_extra_mib,
    }
    family_model = (artifact.get("family_models") or {}).get(args.family)
    global_model = artifact.get("global") or artifact
    selected = family_model if family_model and family_model.get("usable_for_prediction") else global_model
    x = np.array(features(row, mode=selected.get("feature_mode", "interactions")), dtype=float)
    point = float(x @ np.array(selected["coefficients"], dtype=float))
    # Prefer the HONEST group-aware (leave-one-config-out) p95 for the safety margin — row-level LOO is
    # optimistic (near-duplicate rows leak across folds), and here the margin has functional impact on
    # the fits/OOM decision. Fall back to row-level p95 (then 2*MAE) if group-aware isn't present.
    group_p95 = selected.get("group_leave_one_out_abs_error_p95_mib")
    p95 = selected.get("leave_one_out_abs_error_p95_mib")
    loo = selected.get("leave_one_out_mean_absolute_error_mib") or 0.0
    if group_p95 is not None:
        default_safety = max(1024.0, float(group_p95))
        safety_source = "group_leave_one_out_abs_error_p95_mib"
    elif p95 is not None:
        default_safety = max(1024.0, float(p95))
        safety_source = "leave_one_out_abs_error_p95_mib"
    else:
        default_safety = max(1024.0, 2.0 * float(loo))
        safety_source = "2x_leave_one_out_mae"
    safety = float(args.safety_mib) if args.safety_mib is not None else default_safety
    conservative = point + safety
    decision = "fits" if conservative < args.gpu_total_mib else "risk_or_oom"
    output = {
        "input": row,
        "model": str(MODEL.relative_to(ROOT)),
        "selected_model": selected.get("name", "global"),
        "selected_model_rows": selected.get("training_rows"),
        "selected_model_usable": selected.get("usable_for_prediction"),
        "point_estimate_mib": round(point, 1),
        "safety_margin_mib": round(safety, 1),
        "safety_source": "explicit" if args.safety_mib is not None else safety_source,
        "conservative_estimate_mib": round(conservative, 1),
        "gpu_total_mib": args.gpu_total_mib,
        "decision": decision,
        "caveat": "early lab model; use conservative estimate for scheduling decisions",
    }
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
