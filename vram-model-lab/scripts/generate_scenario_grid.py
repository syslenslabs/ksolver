#!/usr/bin/env python3
"""Generate a deterministic VRAM probe grid for automatic data collection."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "generated" / "scenario_grid.yaml"


def transformer_grid() -> list[dict]:
    rows = []
    for precision in ["fp32", "fp16"]:
        for seq_len in [256, 768, 1536]:
            for batch in [2, 6]:
                name = f"grid-transformer-{precision}-b{batch}-s{seq_len}"
                rows.append({
                    "name": name,
                    "family": "transformer",
                    "precision": precision,
                    "batch_size": batch,
                    "seq_len": seq_len,
                    "hidden_size": 768 if seq_len <= 768 else 1024,
                    "layers": 6 if seq_len <= 768 else 8,
                    "heads": 12 if seq_len <= 768 else 16,
                    "steps": 3,
                    "optimizer": "adamw",
                    "activation_checkpointing": False,
                })
    return rows


def cnn_grid() -> list[dict]:
    rows = []
    for precision in ["fp32", "fp16"]:
        for image_size in [160, 256, 384]:
            batch = 64 if image_size <= 160 else 24 if image_size <= 256 else 8
            rows.append({
                "name": f"grid-cnn-{precision}-b{batch}-i{image_size}",
                "family": "cnn",
                "precision": precision,
                "batch_size": batch,
                "image_size": image_size,
                "hidden_size": 128 if image_size <= 256 else 192,
                "layers": 8 if image_size <= 256 else 12,
                "steps": 3,
                "optimizer": "sgd" if precision == "fp32" else "adamw",
                "activation_checkpointing": False,
            })
    return rows


def pressure_grid() -> list[dict]:
    return [
        {
            "name": "grid-pressure-transformer-pad8g",
            "family": "transformer",
            "precision": "fp16",
            "batch_size": 2,
            "seq_len": 1024,
            "hidden_size": 1536,
            "layers": 10,
            "heads": 16,
            "steps": 2,
            "optimizer": "adamw",
            "activation_checkpointing": False,
            "reserve_extra_mib": 8192,
        },
        {
            "name": "grid-pressure-transformer-pad18g",
            "family": "transformer",
            "precision": "fp16",
            "batch_size": 2,
            "seq_len": 1024,
            "hidden_size": 1536,
            "layers": 10,
            "heads": 16,
            "steps": 2,
            "optimizer": "adamw",
            "activation_checkpointing": False,
            "reserve_extra_mib": 18432,
        },
    ]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    scenarios = transformer_grid() + cnn_grid() + pressure_grid()
    if args.limit:
        scenarios = scenarios[: args.limit]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(yaml.safe_dump({"scenarios": scenarios}, sort_keys=False))
    print(f"wrote {args.out} with {len(scenarios)} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
