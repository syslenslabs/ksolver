#!/usr/bin/env python3
"""Report the HONEST generalization accuracy of the peak-VRAM model via GROUP-AWARE CV.

Why: the committed evaluation uses row-level leave-one-out (LOO), but the training data has many
near-duplicate rows (repeated samples / grid sweeps of the same config). Row-LOO then leaks a config's
twins between train and test, so the reported LOO error is OPTIMISTIC — it understates error on NOVEL
configs. This tool groups rows by identical feature vector and runs leave-one-GROUP-out CV, which is
the honest estimate of accuracy on configs the model has never seen.

Read-only: it reuses fit_peak_vram_model.py's feature/ridge functions and the committed training data;
it does NOT refit or overwrite any committed model/evaluation artifact. Print-only.

Usage: python3 group_aware_eval.py [alpha] [mode]   (defaults: 25.0 interactions)
Findings 2026-07-12 (alpha=25, interactions): 276 rows = 132 distinct configs;
  committed row-LOO   MAE 1037 / p95 2805 / max  7636 MiB
  honest GROUP-LOO    MAE 1240 / p95 3365 / max 12200 MiB  (optimistic by ~20% typical, ~60% worst)
The model still passes its quality gate under the honest metric (p95<=5000, max<=25000).
"""
from __future__ import annotations

import sys

import numpy as np

import fit_peak_vram_model as fm


def _errors(pred: np.ndarray, y: np.ndarray) -> tuple[float, float, float]:
    e = np.abs(pred - y)
    return float(e.mean()), float(np.percentile(e, 95)), float(e.max())


def main() -> int:
    alpha = float(sys.argv[1]) if len(sys.argv) > 1 else 25.0
    mode = sys.argv[2] if len(sys.argv) > 2 else "interactions"

    rows = fm.load_rows()
    x = np.array([fm.features(r, mode=mode) for r in rows], dtype=float)
    y = np.array([float(r["nvidia_smi_peak_used_mib"]) for r in rows], dtype=float)
    n = len(y)

    # Group rows by identical (rounded) feature vector — one group per distinct config.
    groups: dict[tuple, list[int]] = {}
    for i, row in enumerate(x):
        groups.setdefault(tuple(np.round(row, 6)), []).append(i)
    gids = list(groups.values())

    # Row-level LOO (matches the committed, optimistic metric).
    row_pred = np.empty(n)
    for i in range(n):
        mask = np.ones(n, dtype=bool)
        mask[i] = False
        coef = fm.fit_ridge(x[mask], y[mask], alpha=alpha)
        row_pred[i] = float(x[i] @ coef)

    # Group-aware LOO: hold out ALL rows of a config, so no twin leaks into training.
    grp_pred = np.empty(n)
    for g in gids:
        mask = np.ones(n, dtype=bool)
        for i in g:
            mask[i] = False
        coef = fm.fit_ridge(x[mask], y[mask], alpha=alpha)
        for i in g:
            grp_pred[i] = float(x[i] @ coef)

    print(f"model: ridge_linear_{mode} alpha={alpha}")
    print(f"rows={n}  distinct feature-configs={len(gids)}")
    for name, pred in [
        ("row-LOO   (committed, optimistic)", row_pred),
        ("GROUP-LOO (honest, novel configs) ", grp_pred),
    ]:
        mae, p95, mx = _errors(pred, y)
        print(f"  {name}: MAE {mae:7.1f} | p95 {p95:7.1f} | max {mx:8.1f} MiB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
