#!/usr/bin/env python3
"""Generate a larger sequential 4090 sweep for overnight data collection."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "generated" / "overnight_4090_sweep.yaml"


def cnn_scenarios() -> list[dict]:
    rows = []
    for arch in ["resnet", "efficientnet", "convnext"]:
        for precision in ["fp32", "fp16"]:
            for image_size, batch in [(160, 64), (224, 32), (320, 12), (384, 8)]:
                rows.append({
                    "name": f"overnight-cnn-{arch}-{precision}-b{batch}-i{image_size}",
                    "family": "cnn",
                    "model_arch": arch,
                    "trainer_style": "pytorch-image-classification",
                    "precision": precision,
                    "batch_size": batch,
                    "image_size": image_size,
                    "hidden_size": 192 if arch == "convnext" else 128,
                    "layers": 8 if image_size <= 224 else 12,
                    "steps": 3,
                    "optimizer": "sgd" if precision == "fp32" else "adamw",
                    "activation_checkpointing": False,
                    "gradient_accumulation_steps": 1,
                    "requested_gpu_type": "rtx-4090",
                    "requested_gpu_count": 1,
                })
    return rows


def transformer_scenarios() -> list[dict]:
    rows = []
    for arch in ["bert", "gpt", "t5"]:
        for precision in ["fp32", "fp16"]:
            for checkpointing in [False, True]:
                for seq_len, batch in [(256, 6), (768, 4), (1536, 2)]:
                    rows.append({
                        "name": "overnight-transformer-{arch}-{precision}-ckpt{ckpt}-b{batch}-s{seq}".format(
                            arch=arch,
                            precision=precision,
                            ckpt=int(checkpointing),
                            batch=batch,
                            seq=seq_len,
                        ),
                        "family": "transformer",
                        "model_arch": arch,
                        "trainer_style": "hf-trainer-style",
                        "precision": precision,
                        "batch_size": batch,
                        "seq_len": seq_len,
                        "hidden_size": 768 if seq_len <= 768 else 1024,
                        "layers": 6 if seq_len <= 768 else 8,
                        "heads": 12 if seq_len <= 768 else 16,
                        "steps": 3,
                        "optimizer": "adamw",
                        "activation_checkpointing": checkpointing,
                        "gradient_accumulation_steps": 1,
                        "requested_gpu_type": "rtx-4090",
                        "requested_gpu_count": 1,
                    })
    return rows


def mlp_scenarios() -> list[dict]:
    rows = []
    for precision in ["fp32", "fp16"]:
        for optimizer in ["sgd", "adamw"]:
            for seq_len, batch in [(512, 128), (1024, 128), (2048, 64)]:
                rows.append({
                    "name": f"overnight-mlp-{precision}-{optimizer}-b{batch}-s{seq_len}",
                    "family": "mlp",
                    "model_arch": "tabular-mlp",
                    "trainer_style": "pytorch-tabular",
                    "precision": precision,
                    "batch_size": batch,
                    "seq_len": seq_len,
                    "hidden_size": 2048,
                    "layers": 6,
                    "steps": 3,
                    "optimizer": optimizer,
                    "activation_checkpointing": False,
                    "gradient_accumulation_steps": 1,
                    "requested_gpu_type": "rtx-4090",
                    "requested_gpu_count": 1,
                })
    return rows


def pressure_scenarios() -> list[dict]:
    rows = []
    for reserve in [4096, 8192, 12288, 16384]:
        rows.append({
            "name": f"overnight-pressure-transformer-pad{reserve//1024}g",
            "family": "transformer",
            "model_arch": "gpt",
            "trainer_style": "hf-trainer-style",
            "precision": "fp16",
            "batch_size": 2,
            "seq_len": 1024,
            "hidden_size": 1536,
            "layers": 10,
            "heads": 16,
            "steps": 2,
            "optimizer": "adamw",
            "activation_checkpointing": False,
            "reserve_extra_mib": reserve,
            "gradient_accumulation_steps": 1,
            "requested_gpu_type": "rtx-4090",
            "requested_gpu_count": 1,
        })
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    scenarios = cnn_scenarios() + transformer_scenarios() + mlp_scenarios() + pressure_scenarios()
    if args.limit:
        scenarios = scenarios[: args.limit]
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(yaml.safe_dump({"scenarios": scenarios}, sort_keys=False))
    print(f"wrote {args.out} with {len(scenarios)} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
