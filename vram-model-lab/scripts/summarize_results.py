#!/usr/bin/env python3
"""Write a compact markdown summary of collected VRAM probe results."""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "data" / "results.jsonl"
MODEL = ROOT / "data" / "models" / "peak_vram_linear.json"
SUMMARY = ROOT / "data" / "summary.md"


def load_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def main() -> int:
    rows = load_jsonl(RESULTS)
    model = json.loads(MODEL.read_text()) if MODEL.exists() else None
    ok_rows = [r for r in rows if r.get("ok")]
    oom_rows = [r for r in rows if r.get("oom")]
    risk_rows = [
        r
        for r in rows
        if r.get("nvidia_smi_peak_used_mib") is not None
        and r.get("gpu_total_mib") is not None
        and r["nvidia_smi_peak_used_mib"] >= 0.9 * r["gpu_total_mib"]
    ]
    families = Counter(r.get("family") for r in rows)
    precisions = Counter(r.get("precision") for r in rows)

    lines = [
        "# VRAM Probe Summary",
        "",
        f"- total rows: {len(rows)}",
        f"- successful rows: {len(ok_rows)}",
        f"- OOM rows: {len(oom_rows)}",
        f"- near-capacity risk rows (>=90% reported GPU memory): {len(risk_rows)}",
        f"- families: {dict(families)}",
        f"- precisions: {dict(precisions)}",
    ]
    if rows:
        gpu_names = sorted({r.get("gpu_name") for r in rows if r.get("gpu_name")})
        gpu_skus = sorted({r.get("gpu_sku_label") or r.get("requested_gpu_type") for r in rows if r.get("gpu_sku_label") or r.get("requested_gpu_type")})
        gpu_totals = sorted({r.get("gpu_total_mib") for r in rows if r.get("gpu_total_mib")})
        gpu_total_gib = sorted({round(float(v) / 1024.0, 2) for v in gpu_totals})
        lines += [
            f"- GPU SKU labels: {gpu_skus}",
            f"- GPUs: {gpu_names}",
            f"- GPU total MiB values: {gpu_totals}",
            f"- GPU total GiB values: {gpu_total_gib}",
            "",
            "Note: this WSL/NVIDIA runtime allowed synthetic allocations that reached",
            "the reported card limit without surfacing a hard CUDA OOM. Treat the",
            "near-capacity rows as risk calibration data, not as definitive bare-metal",
            "OOM labels.",
        ]
    if model:
        lines += [
            "",
            "## Fitted Model",
            "",
            f"- fit: {model.get('fit')}",
            f"- alpha: {model.get('alpha')}",
            f"- feature mode: {model.get('feature_mode')}",
            f"- training rows: {model.get('training_rows')}",
            f"- in-sample MAE MiB: {model.get('in_sample_mean_absolute_error_mib'):.1f}",
            f"- in-sample p95 absolute error MiB: {model.get('in_sample_abs_error_p95_mib'):.1f}",
            f"- leave-one-out MAE MiB: {model.get('leave_one_out_mean_absolute_error_mib'):.1f}",
            f"- leave-one-out p95 absolute error MiB: {model.get('leave_one_out_abs_error_p95_mib'):.1f}",
            f"- leave-one-out max error MiB: {model.get('leave_one_out_max_absolute_error_mib'):.1f}",
        ]
        group_impacts = model.get("group_impacts") or []
        feature_impacts = model.get("feature_impacts") or []
        if group_impacts:
            lines += [
                "",
                "## Top VRAM Driver Groups",
                "",
                "| rank | group | normalized impact MiB/std |",
                "| ---: | --- | ---: |",
            ]
            for idx, row in enumerate(group_impacts[:8], start=1):
                lines.append(
                    "| {rank} | {group} | {impact:.1f} |".format(
                        rank=idx,
                        group=row.get("group"),
                        impact=row.get("abs_impact_mib_per_std_sum") or 0.0,
                    )
                )
        if feature_impacts:
            lines += [
                "",
                "## Top VRAM Model Drivers",
                "",
                "Impact is coefficient multiplied by observed feature standard deviation,",
                "so columns with different units can be compared directionally. Negative",
                "weights are model weights under correlated features, not causal claims",
                "that the feature lowers true VRAM.",
                "",
                "| rank | feature | group | model weight | impact MiB/std | meaning |",
                "| ---: | --- | --- | --- | ---: | --- |",
            ]
            for idx, row in enumerate(feature_impacts[:10], start=1):
                lines.append(
                    "| {rank} | {feature} | {group} | {direction} | {impact:.1f} | {meaning} |".format(
                        rank=idx,
                        feature=row.get("feature"),
                        group=row.get("group"),
                        direction=row.get("direction"),
                        impact=row.get("impact_mib_per_std") or 0.0,
                        meaning=row.get("description"),
                    )
                )
        family_models = model.get("family_models") or {}
        if family_models:
            lines += [
                "",
                "## Family Models",
                "",
                "| family | rows | usable | fit | alpha | in-sample MAE MiB | LOO MAE MiB | LOO p95 abs error MiB |",
                "| --- | ---: | --- | --- | ---: | ---: | ---: | ---: |",
            ]
            for family, family_model in sorted(family_models.items()):
                lines.append(
                    "| {family} | {rows} | {usable} | {fit} | {alpha:.1f} | {mae:.1f} | {loo:.1f} | {p95:.1f} |".format(
                        family=family,
                        rows=family_model.get("training_rows") or 0,
                        usable=family_model.get("usable_for_prediction"),
                        fit=family_model.get("fit"),
                        alpha=family_model.get("alpha") or 0.0,
                        mae=family_model.get("in_sample_mean_absolute_error_mib") or 0.0,
                        loo=family_model.get("leave_one_out_mean_absolute_error_mib") or 0.0,
                        p95=family_model.get("leave_one_out_abs_error_p95_mib") or 0.0,
                    )
                )
    lines += [
        "",
        "## Rows",
        "",
        "| scenario | family | precision | optimizer | checkpoint | reserve MiB | peak nvidia-smi MiB | peak torch reserved MiB | params |",
        "| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for row in rows:
        lines.append(
            "| {scenario} | {family} | {precision} | {optimizer} | {checkpoint} | {reserve} | {smi} | {reserved} | {params} |".format(
                scenario=row.get("scenario"),
                family=row.get("family"),
                precision=row.get("precision"),
                optimizer=row.get("optimizer"),
                checkpoint=row.get("activation_checkpointing"),
                reserve=row.get("reserve_extra_mib") or 0,
                smi=row.get("nvidia_smi_peak_used_mib"),
                reserved=row.get("torch_peak_reserved_mib"),
                params=row.get("param_count"),
            )
        )

    SUMMARY.write_text("\n".join(lines) + "\n")
    print(f"wrote {SUMMARY}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
