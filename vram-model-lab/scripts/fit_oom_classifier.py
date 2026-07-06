#!/usr/bin/env python3
"""Fit a small logistic OOM-risk classifier from collected VRAM rows."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np

from fit_peak_vram_model import features, load_rows


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "models" / "oom_risk_classifier.json"


def label(row: dict) -> float:
    peak = float(row.get("nvidia_smi_peak_used_mib") or 0)
    total = float(row.get("gpu_total_mib") or 0)
    return 1.0 if row.get("oom") or (total and peak >= 0.90 * total) else 0.0


def sigmoid(x: np.ndarray) -> np.ndarray:
    x = np.clip(x, -50, 50)
    return 1.0 / (1.0 + np.exp(-x))


def fit_logistic(x: np.ndarray, y: np.ndarray, alpha: float = 1.0, steps: int = 5000, lr: float = 0.05) -> np.ndarray:
    coef = np.zeros(x.shape[1])
    for _ in range(steps):
        pred = sigmoid(x @ coef)
        grad = (x.T @ (pred - y)) / len(y)
        grad[1:] += alpha * coef[1:] / len(y)
        coef -= lr * grad
    return coef


def metrics(x: np.ndarray, y: np.ndarray, coef: np.ndarray) -> dict:
    p = sigmoid(x @ coef)
    pred = p >= 0.5
    truth = y >= 0.5
    tp = int(np.sum(pred & truth))
    fp = int(np.sum(pred & ~truth))
    tn = int(np.sum(~pred & ~truth))
    fn = int(np.sum(~pred & truth))
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0
    accuracy = (tp + tn) / len(y) if len(y) else 0.0
    return {
        "accuracy": accuracy,
        "precision": precision,
        "recall": recall,
        "tp": tp,
        "fp": fp,
        "tn": tn,
        "fn": fn,
        "positive_rows": int(np.sum(truth)),
        "negative_rows": int(np.sum(~truth)),
    }


def main() -> int:
    rows = load_rows()
    if len(rows) < 10:
        raise SystemExit("need at least 10 rows")
    x = np.array([features(r, mode="base") for r in rows], dtype=float)
    y = np.array([label(r) for r in rows], dtype=float)
    scale = np.maximum(np.std(x, axis=0), 1e-9)
    scale[0] = 1.0
    x_scaled = x / scale
    coef = fit_logistic(x_scaled, y)
    report = {
        "schema_version": 1,
        "fit": "logistic_regression_base_features",
        "target": "oom_or_peak_vram_fraction_ge_0.90",
        "training_rows": len(rows),
        "features": [
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
        ],
        "feature_scale": scale.tolist(),
        "coefficients": coef.tolist(),
        "metrics": metrics(x_scaled, y, coef),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
