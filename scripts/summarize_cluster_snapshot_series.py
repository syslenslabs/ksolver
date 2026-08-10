#!/usr/bin/env python3
"""Summarize per-snapshot `snapshot_pack` reports into an advisory time series."""

from __future__ import annotations

import argparse
import json
import statistics
from pathlib import Path


METRICS = (
    "scanned_gpus",
    "active_gpu_workloads",
    "unknown_or_idle_gpus",
    "baseline_active_gpus",
    "packed_active_gpus",
    "recoverable_h100_equivalents",
    "exclusive_workloads",
)


def quantiles(values: list[int]) -> dict[str, int]:
    ordered = sorted(values)
    return {
        "min": ordered[0],
        "median": int(statistics.median(ordered)),
        "max": ordered[-1],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("series_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    reports: list[tuple[str, dict]] = []
    for sample_dir in sorted(path for path in args.series_dir.iterdir() if path.is_dir()):
        report_path = sample_dir / "ksolver-snapshot-pack.json"
        if not report_path.is_file():
            continue
        try:
            report = json.loads(report_path.read_text())
        except json.JSONDecodeError:
            continue
        if all(isinstance(report.get(metric), int) for metric in METRICS):
            reports.append((sample_dir.name, report))
    if not reports:
        raise SystemExit("no valid ksolver-snapshot-pack.json reports found")

    samples = [
        {"sample": name, **{metric: report[metric] for metric in METRICS}}
        for name, report in reports
    ]
    aggregate = {metric: quantiles([report[metric] for _, report in reports]) for metric in METRICS}
    expected_scanned_gpus = aggregate["scanned_gpus"]["max"]
    complete_reports = [
        report for _, report in reports if report["scanned_gpus"] == expected_scanned_gpus
    ]
    complete_summary = {
        metric: quantiles([report[metric] for report in complete_reports]) for metric in METRICS
    }
    output = {
        "source": "read-only cluster multi-snapshot advisory",
        "sample_count": len(samples),
        "sample_window": {"first": samples[0]["sample"], "last": samples[-1]["sample"]},
        "policy": reports[0][1].get("policy", {}),
        "coverage": {
            "expected_scanned_gpus": expected_scanned_gpus,
            "complete_sample_count": len(complete_reports),
            "incomplete_sample_count": len(samples) - len(complete_reports),
        },
        "summary": aggregate,
        "complete_coverage_summary": complete_summary,
        "samples": samples,
        "conservative_recoverable_h100_equivalents": complete_summary[
            "recoverable_h100_equivalents"
        ]["min"],
        "note": (
            "Read-only point-in-time advisory across multiple samples. The conservative figure is "
            "the minimum observed recoverable H100-equivalent count among complete-coverage "
            "samples; it does not prove job-level safety, authorize migration, or establish "
            "realized savings."
        ),
    }
    destination = args.output or args.series_dir / "ksolver-snapshot-series.json"
    destination.write_text(json.dumps(output, indent=2) + "\n")
    print(destination)


if __name__ == "__main__":
    main()
