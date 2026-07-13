#!/usr/bin/env python3
"""Fit transparent baseline peak-VRAM models from collected probe results."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "data" / "results.jsonl"
MODELS = ROOT / "data" / "models"


BASE_FEATURES = [
    "intercept",
    "param_count_m",
    "activation_units_m",
    "batch_size",
    "layers",
    "hidden_size_k",
    "precision_bytes",
    "reserve_extra_gib",
    "adamw",
    "checkpointed",
    "family_transformer",
    "family_cnn",
]

INTERACTION_FEATURES = [
    "activation_x_precision",
    "activation_x_batch",
    "param_x_precision",
    "reserve_x_transformer",
]

FEATURE_SETS = {
    "base": BASE_FEATURES,
    "interactions": BASE_FEATURES + INTERACTION_FEATURES,
}

FEATURE_DESCRIPTIONS = {
    "param_count_m": "parameter count in millions",
    "activation_units_m": "batch * sequence/image shape * hidden/layers activation footprint",
    "batch_size": "training batch size",
    "layers": "model depth",
    "hidden_size_k": "hidden size in thousands",
    "precision_bytes": "bytes per tensor element from fp32/fp16/bf16/int8",
    "reserve_extra_gib": "synthetic VRAM headroom probe allocation",
    "adamw": "AdamW optimizer state indicator",
    "checkpointed": "activation checkpointing indicator",
    "family_transformer": "transformer model-family indicator",
    "family_cnn": "CNN model-family indicator",
    "activation_x_precision": "activation footprint multiplied by precision bytes",
    "activation_x_batch": "activation footprint multiplied by batch size",
    "param_x_precision": "parameter count multiplied by precision bytes",
    "reserve_x_transformer": "synthetic headroom probe on transformer rows",
}

FEATURE_GROUPS = {
    "param_count_m": "parameters",
    "param_x_precision": "parameters",
    "activation_units_m": "activations",
    "activation_x_precision": "activations",
    "activation_x_batch": "activations",
    "batch_size": "input shape",
    "layers": "architecture",
    "hidden_size_k": "architecture",
    "precision_bytes": "precision",
    "reserve_extra_gib": "synthetic headroom",
    "reserve_x_transformer": "synthetic headroom",
    "adamw": "optimizer",
    "checkpointed": "training strategy",
    "family_transformer": "model family",
    "family_cnn": "model family",
}


def precision_bytes(row: dict) -> float:
    return {
        "fp32": 4.0,
        "fp16": 2.0,
        "bf16": 2.0,
        "int8": 1.0,
    }.get(row.get("precision"), 4.0)


def base_features(row: dict) -> list[float]:
    family = row.get("family")
    batch = float(row.get("batch_size") or 0)
    if family == "cnn":
        image_size = float(row.get("image_size") or 0)
        activation_units = batch * image_size * image_size * float(row.get("layers") or 0)
    else:
        seq_len = float(row.get("seq_len") or 0)
        hidden = float(row.get("hidden_size") or 0)
        activation_units = batch * seq_len * hidden * float(row.get("layers") or 0)
    return [
        1.0,
        float(row.get("param_count") or 0) / 1_000_000.0,
        activation_units / 1_000_000.0,
        batch,
        float(row.get("layers") or 0),
        float(row.get("hidden_size") or 0) / 1000.0,
        precision_bytes(row),
        float(row.get("reserve_extra_mib") or 0) / 1024.0,
        1.0 if row.get("optimizer") == "adamw" else 0.0,
        1.0 if row.get("activation_checkpointing") else 0.0,
        1.0 if family == "transformer" else 0.0,
        1.0 if family == "cnn" else 0.0,
    ]


def features(row: dict, mode: str = "interactions") -> list[float]:
    base = base_features(row)
    if mode == "base":
        return base
    by_name = dict(zip(BASE_FEATURES, base))
    interactions = [
        by_name["activation_units_m"] * by_name["precision_bytes"],
        by_name["activation_units_m"] * by_name["batch_size"],
        by_name["param_count_m"] * by_name["precision_bytes"],
        by_name["reserve_extra_gib"] * by_name["family_transformer"],
    ]
    return base + interactions


def load_rows() -> list[dict]:
    if not RESULTS.exists():
        return []
    rows = []
    for line in RESULTS.read_text().splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if row.get("ok") and row.get("nvidia_smi_peak_used_mib") is not None:
            rows.append(row)
    return rows


def fit_ridge(x: np.ndarray, y: np.ndarray, alpha: float = 10.0) -> np.ndarray:
    penalty = np.eye(x.shape[1]) * alpha
    penalty[0, 0] = 0.0
    return np.linalg.solve(x.T @ x + penalty, x.T @ y)


def leave_one_out(x: np.ndarray, y: np.ndarray, alpha: float) -> list[float]:
    errors = []
    if len(y) < 3:
        return errors
    for idx in range(len(y)):
        mask = np.ones(len(y), dtype=bool)
        mask[idx] = False
        coef = fit_ridge(x[mask], y[mask], alpha=alpha)
        predicted = float(x[idx] @ coef)
        errors.append(abs(float(y[idx]) - predicted))
    return errors


def leave_one_group_out(x: np.ndarray, y: np.ndarray, alpha: float) -> list[float]:
    """Group-aware CV: hold out ALL rows sharing a feature vector (one config) at once. Row-level LOO
    leaks near-duplicate twins (repeated samples per config) across train/test and is optimistic; this
    is the honest generalization to NOVEL configs. Mirrors group_aware_eval.py. Returns [] if <2 groups."""
    errors: list[float] = []
    if len(y) < 3:
        return errors
    groups: dict[tuple, list[int]] = {}
    for i, row in enumerate(x):
        groups.setdefault(tuple(np.round(row, 6)), []).append(i)
    if len(groups) < 2:
        return errors
    for members in groups.values():
        mask = np.ones(len(y), dtype=bool)
        for i in members:
            mask[i] = False
        coef = fit_ridge(x[mask], y[mask], alpha=alpha)
        for i in members:
            errors.append(abs(float(y[i]) - float(x[i] @ coef)))
    return errors


def percentile(values: np.ndarray | list[float], pct: float) -> float | None:
    if len(values) == 0:
        return None
    return float(np.percentile(np.array(values, dtype=float), pct))


def feature_impact_rows(feature_names: list[str], coef: np.ndarray, x: np.ndarray) -> list[dict]:
    rows = []
    std = np.std(x, axis=0)
    mean = np.mean(x, axis=0)
    for idx, name in enumerate(feature_names):
        if name == "intercept":
            continue
        impact = float(coef[idx] * std[idx])
        rows.append(
            {
                "feature": name,
                "group": FEATURE_GROUPS.get(name, "other"),
                "description": FEATURE_DESCRIPTIONS.get(name, name),
                "coefficient_mib_per_unit": float(coef[idx]),
                "feature_mean": float(mean[idx]),
                "feature_std": float(std[idx]),
                "impact_mib_per_std": impact,
                "abs_impact_mib_per_std": abs(impact),
                "direction": "positive_model_weight" if impact >= 0 else "negative_model_weight",
            }
        )
    rows.sort(key=lambda row: row["abs_impact_mib_per_std"], reverse=True)
    return rows


def group_impact_rows(feature_rows: list[dict]) -> list[dict]:
    grouped: dict[str, float] = {}
    for row in feature_rows:
        grouped[row["group"]] = grouped.get(row["group"], 0.0) + float(row["abs_impact_mib_per_std"])
    rows = [
        {
            "group": group,
            "abs_impact_mib_per_std_sum": impact,
        }
        for group, impact in grouped.items()
    ]
    rows.sort(key=lambda row: row["abs_impact_mib_per_std_sum"], reverse=True)
    return rows


def fit_model(rows: list[dict], name: str, alpha: float = 25.0, mode: str = "interactions") -> dict:
    feature_names = FEATURE_SETS[mode]
    x = np.array([features(r, mode=mode) for r in rows], dtype=float)
    y = np.array([float(r["nvidia_smi_peak_used_mib"]) for r in rows], dtype=float)
    coef = fit_ridge(x, y, alpha=alpha)
    pred = x @ coef
    abs_err = np.abs(pred - y)
    loo = leave_one_out(x, y, alpha=alpha)
    group_loo = leave_one_group_out(x, y, alpha=alpha)
    residuals = y - pred
    loo_p95 = percentile(loo, 95)
    loo_max = float(np.max(loo)) if loo else None
    feature_impacts = feature_impact_rows(feature_names, coef, x)
    group_impacts = group_impact_rows(feature_impacts)
    organic_feature_impacts = [
        row for row in feature_impacts if row.get("group") != "synthetic headroom"
    ]
    usable = (
        len(rows) >= 8
        and loo_p95 is not None
        and loo_p95 <= 5000.0
        and (loo_max is None or loo_max <= 25000.0)
    )
    return {
        "name": name,
        "target": "nvidia_smi_peak_used_mib",
        "fit": f"ridge_linear_{mode}",
        "alpha": alpha,
        "feature_mode": mode,
        "features": feature_names,
        "coefficients": coef.tolist(),
        "feature_impacts": feature_impacts,
        "group_impacts": group_impacts,
        "top_driver_labels": [
            row["description"]
            for row in feature_impacts[:5]
        ],
        "top_organic_driver_labels": [
            row["description"]
            for row in organic_feature_impacts[:5]
        ],
        "training_rows": len(rows),
        "in_sample_mean_absolute_error_mib": float(abs_err.mean()),
        "in_sample_max_absolute_error_mib": float(abs_err.max()),
        "in_sample_abs_error_p50_mib": percentile(abs_err, 50),
        "in_sample_abs_error_p90_mib": percentile(abs_err, 90),
        "in_sample_abs_error_p95_mib": percentile(abs_err, 95),
        "in_sample_residual_p95_mib": percentile(residuals, 95),
        "leave_one_out_mean_absolute_error_mib": float(np.mean(loo)) if loo else None,
        "leave_one_out_max_absolute_error_mib": loo_max,
        "leave_one_out_abs_error_p90_mib": percentile(loo, 90),
        "leave_one_out_abs_error_p95_mib": loo_p95,
        # Group-aware (leave-one-CONFIG-out) CV = honest generalization to NOVEL configs. Row-level LOO
        # above is optimistic because near-duplicate rows (repeated samples per config) leak across
        # train/test. Reported alongside row-level so consumers can quote the honest novel-config error.
        "group_leave_one_out_mean_absolute_error_mib": (
            float(np.mean(group_loo)) if group_loo else None
        ),
        "group_leave_one_out_max_absolute_error_mib": (
            float(np.max(group_loo)) if group_loo else None
        ),
        "group_leave_one_out_abs_error_p95_mib": percentile(group_loo, 95),
        "usable_for_prediction": usable,
        "quality_gate": "rows>=8 and loo_p95<=5000MiB and loo_max<=25000MiB",
        "examples": [
            {
                "scenario": row.get("scenario"),
                "family": row.get("family"),
                "actual_mib": float(actual),
                "predicted_mib": float(predicted),
                "abs_error_mib": float(abs(actual - predicted)),
            }
            for row, actual, predicted in zip(rows, y, pred)
        ],
    }


def select_model(rows: list[dict], name: str) -> dict:
    candidates = []
    for mode in ["base", "interactions"]:
        for alpha in [10.0, 25.0, 100.0, 1000.0]:
            candidates.append(fit_model(rows, name, alpha=alpha, mode=mode))

    def score(model: dict) -> tuple[float, float, float]:
        p95 = model.get("leave_one_out_abs_error_p95_mib")
        mae = model.get("leave_one_out_mean_absolute_error_mib")
        max_err = model.get("leave_one_out_max_absolute_error_mib")
        return (
            float(p95) if p95 is not None else float("inf"),
            float(mae) if mae is not None else float("inf"),
            float(max_err) if max_err is not None else float("inf"),
        )

    best = min(candidates, key=score)
    best["candidate_count"] = len(candidates)
    best["candidate_scores"] = [
        {
            "fit": candidate["fit"],
            "alpha": candidate["alpha"],
            "loo_mae_mib": candidate.get("leave_one_out_mean_absolute_error_mib"),
            "loo_p95_mib": candidate.get("leave_one_out_abs_error_p95_mib"),
            "loo_max_mib": candidate.get("leave_one_out_max_absolute_error_mib"),
        }
        for candidate in sorted(candidates, key=score)
    ]
    return best


def main() -> int:
    rows = load_rows()
    if len(rows) < 2:
        print(f"need at least 2 successful rows to fit; found {len(rows)} in {RESULTS}")
        return 1

    global_model = select_model(rows, "global")
    family_models = {}
    for family in sorted({r.get("family") for r in rows}):
        family_rows = [r for r in rows if r.get("family") == family]
        if len(family_rows) >= 4:
            family_models[family] = select_model(family_rows, f"family:{family}")

    model = {
        "schema_version": 1,
        "model_family": "peak_vram_ridge_family_fallback",
        "selection": "use family model when available, otherwise global",
        "global": global_model,
        "family_models": family_models,
        **global_model,
    }
    MODELS.mkdir(parents=True, exist_ok=True)
    out = MODELS / "peak_vram_linear.json"
    out.write_text(json.dumps(model, indent=2, sort_keys=True) + "\n")
    print(f"wrote {out}")
    print(
        f"rows={len(rows)} "
        f"global_in_sample_mae={global_model['in_sample_mean_absolute_error_mib']:.1f} MiB "
        f"global_loo_mae={global_model['leave_one_out_mean_absolute_error_mib']:.1f} MiB "
        f"family_models={','.join(family_models.keys())}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
