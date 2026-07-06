#!/usr/bin/env python3
"""Generate targeted follow-up 4090 probes for variance and near-capacity data."""

from __future__ import annotations

import argparse
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "generated" / "iteration_4090_sweep.yaml"


def base(name: str, family: str, arch: str, steps: int, iteration: int) -> dict:
    return {
        "name": f"iter{iteration}-{name}",
        "family": family,
        "model_arch": arch,
        "steps": steps,
        "input_pipeline": "cpu_to_gpu",
        "dataloader_sleep_ms": 8,
        "sample_interval_seconds": 0.1,
        "requested_gpu_type": "rtx-4090",
        "requested_gpu_count": 1,
    }


def repeatability(iteration: int) -> list[dict]:
    seeds = []
    templates = [
        {
            "name": "repeat-bert-fp16-b2-s1024-ckpt1",
            "family": "transformer",
            "model_arch": "bert",
            "trainer_style": "hf-trainer-style",
            "precision": "fp16",
            "batch_size": 2,
            "seq_len": 1024,
            "hidden_size": 1024,
            "layers": 8,
            "heads": 16,
            "optimizer": "adamw",
            "activation_checkpointing": True,
            "gradient_accumulation_steps": 1,
        },
        {
            "name": "repeat-gpt-fp16-b4-s512-ckpt1",
            "family": "transformer",
            "model_arch": "gpt",
            "trainer_style": "hf-trainer-style",
            "precision": "fp16",
            "batch_size": 4,
            "seq_len": 512,
            "hidden_size": 768,
            "layers": 6,
            "heads": 12,
            "optimizer": "adamw",
            "activation_checkpointing": True,
            "gradient_accumulation_steps": 1,
        },
        {
            "name": "repeat-efficientnet-fp16-b32-i224",
            "family": "cnn",
            "model_arch": "efficientnet",
            "trainer_style": "pytorch-image-classification-style",
            "precision": "fp16",
            "batch_size": 32,
            "image_size": 224,
            "hidden_size": 192,
            "layers": 10,
            "optimizer": "adamw",
            "activation_checkpointing": False,
            "gradient_accumulation_steps": 1,
        },
        {
            "name": "repeat-mlp-fp32-adamw-b32-s4096-acc4",
            "family": "mlp",
            "model_arch": "tabular-mlp",
            "trainer_style": "pytorch-tabular-style",
            "precision": "fp32",
            "batch_size": 32,
            "seq_len": 4096,
            "hidden_size": 2048,
            "layers": 8,
            "optimizer": "adamw",
            "activation_checkpointing": False,
            "gradient_accumulation_steps": 4,
        },
    ]
    for repeat in range(1, 4):
        for template in templates:
            row = base(f"{template['name']}-r{repeat}", template["family"], template["model_arch"], 60, iteration)
            row.update({k: v for k, v in template.items() if k not in ("name", "family", "model_arch")})
            seeds.append(row)
    return seeds


def long_sequence(iteration: int) -> list[dict]:
    rows = []
    for arch in ["bert", "gpt"]:
        for precision, checkpointing in [("fp32", False), ("fp16", False), ("fp16", True)]:
            row = base(
                f"long-{arch}-{precision}-ckpt{int(checkpointing)}-b1-s2048",
                "transformer",
                arch,
                40,
                iteration,
            )
            row.update({
                "trainer_style": "hf-trainer-style",
                "precision": precision,
                "batch_size": 1,
                "seq_len": 2048,
                "hidden_size": 1024,
                "layers": 8,
                "heads": 16,
                "optimizer": "adamw",
                "activation_checkpointing": checkpointing,
                "gradient_accumulation_steps": 1,
            })
            rows.append(row)
    return rows


def pressure(iteration: int) -> list[dict]:
    rows = []
    for reserve in [18432, 19456, 20480, 21504, 22528]:
        row = base(f"pressure-gpt-fp16-pad{reserve // 1024}g", "transformer", "gpt", 12, iteration)
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
    parser.add_argument("--iteration", type=int, default=2)
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()

    scenarios = repeatability(args.iteration) + long_sequence(args.iteration) + pressure(args.iteration)
    if args.limit:
        scenarios = scenarios[: args.limit]

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(yaml.safe_dump({"scenarios": scenarios}, sort_keys=False))
    print(f"wrote {args.out} with {len(scenarios)} scenarios")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
