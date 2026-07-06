#!/usr/bin/env python3
"""Generate longer 4090 probe scenarios with more realistic training dynamics."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "generated" / "realistic_4090_sweep.yaml"


def base(name: str, family: str, arch: str, steps: int) -> dict:
    return {
        "name": name,
        "family": family,
        "model_arch": arch,
        "steps": steps,
        "input_pipeline": "cpu_to_gpu",
        "dataloader_sleep_ms": 5,
        "sample_interval_seconds": 0.1,
        "requested_gpu_type": "rtx-4090",
        "requested_gpu_count": 1,
    }


def cnn_scenarios(steps: int) -> list[dict]:
    rows = []
    for arch, hidden in [("resnet", 192), ("efficientnet", 192), ("convnext", 256)]:
        for precision in ["fp32", "fp16"]:
            for image_size, batch, layers in [(224, 32, 10), (320, 12, 14)]:
                row = base(f"realistic-cnn-{arch}-{precision}-b{batch}-i{image_size}", "cnn", arch, steps)
                row.update({
                    "trainer_style": "pytorch-image-classification-style",
                    "precision": precision,
                    "batch_size": batch,
                    "image_size": image_size,
                    "hidden_size": hidden,
                    "layers": layers,
                    "optimizer": "sgd" if precision == "fp32" else "adamw",
                    "activation_checkpointing": False,
                    "gradient_accumulation_steps": 1,
                })
                rows.append(row)
    return rows


def transformer_scenarios(steps: int) -> list[dict]:
    rows = []
    for arch in ["bert", "gpt", "t5"]:
        for precision in ["fp32", "fp16"]:
            for checkpointing in [False, True]:
                for seq_len, batch, hidden, layers, heads in [
                    (512, 4, 768, 6, 12),
                    (1024, 2, 1024, 8, 16),
                ]:
                    row = base(
                        "realistic-transformer-{arch}-{precision}-ckpt{ckpt}-b{batch}-s{seq}".format(
                            arch=arch,
                            precision=precision,
                            ckpt=int(checkpointing),
                            batch=batch,
                            seq=seq_len,
                        ),
                        "transformer",
                        arch,
                        steps,
                    )
                    row.update({
                        "trainer_style": "hf-trainer-style",
                        "precision": precision,
                        "batch_size": batch,
                        "seq_len": seq_len,
                        "hidden_size": hidden,
                        "layers": layers,
                        "heads": heads,
                        "optimizer": "adamw",
                        "activation_checkpointing": checkpointing,
                        "gradient_accumulation_steps": 1,
                    })
                    rows.append(row)
    return rows


def mlp_scenarios(steps: int) -> list[dict]:
    rows = []
    for precision in ["fp32", "fp16"]:
        for optimizer in ["sgd", "adamw"]:
            for seq_len, batch, accum in [(1024, 128, 1), (2048, 64, 2), (4096, 32, 4)]:
                row = base(
                    f"realistic-mlp-{precision}-{optimizer}-b{batch}-s{seq_len}-acc{accum}",
                    "mlp",
                    "tabular-mlp",
                    steps,
                )
                row.update({
                    "trainer_style": "pytorch-tabular-style",
                    "precision": precision,
                    "batch_size": batch,
                    "seq_len": seq_len,
                    "hidden_size": 2048,
                    "layers": 8,
                    "optimizer": optimizer,
                    "activation_checkpointing": False,
                    "gradient_accumulation_steps": accum,
                })
                rows.append(row)
    return rows


def pressure_scenarios(steps: int) -> list[dict]:
    rows = []
    for reserve in [4096, 8192, 12288, 16384]:
        row = base(f"realistic-pressure-gpt-fp16-pad{reserve // 1024}g", "transformer", "gpt", max(5, steps // 2))
        row.update({
            "trainer_style": "hf-trainer-style",
            "precision": "fp16",
            "batch_size": 2,
            "seq_len": 1024,
            "hidden_size": 1536,
            "layers": 10,
            "heads": 16,
            "optimizer": "adamw",
            "activation_checkpointing": False,
            "gradient_accumulation_steps": 1,
            "reserve_extra_mib": reserve,
        })
        rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, default=OUT)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--steps", type=int, default=30)
    args = parser.parse_args()

    scenarios = cnn_scenarios(args.steps) + transformer_scenarios(args.steps) + mlp_scenarios(args.steps) + pressure_scenarios(args.steps)
    if args.limit and args.limit < len(scenarios):
        if args.limit == 1:
            scenarios = [scenarios[0]]
        else:
            last = len(scenarios) - 1
            scenarios = [scenarios[round(i * last / (args.limit - 1))] for i in range(args.limit)]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(yaml.safe_dump({"scenarios": scenarios}, sort_keys=False))
    print(f"wrote {args.out} with {len(scenarios)} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
