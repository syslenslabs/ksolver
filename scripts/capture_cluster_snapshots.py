#!/usr/bin/env python3
"""Capture read-only GPU and Slurm snapshots through a bastion.

The output directory is deliberately ignored by Git because it can contain
operational process metadata. Each sample has the same layout expected by
``cluster_snapshot_manifest.py``.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path


def run(command: list[str], timeout: int) -> str:
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    output = result.stdout
    if result.stderr:
        output += "\n__KSOLVER_STDERR__\n" + result.stderr
    return output


def ssh(bastion: str, remote: str, timeout: int = 25) -> str:
    return run(
        [
            "ssh",
            "-o",
            "BatchMode=yes",
            "-o",
            "ConnectTimeout=8",
            bastion,
            remote,
        ],
        timeout,
    )


def capture_worker(bastion: str, node: str) -> tuple[str, str]:
    remote = (
        "ssh -o BatchMode=yes -o ConnectTimeout=6 "
        f"{node} 'hostname; printf \"__KSOLVER_XML__\\n\"; nvidia-smi -q -x'"
    )
    return node, ssh(bastion, remote, timeout=35)


def timestamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bastion", required=True)
    parser.add_argument("--nodes-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=8)
    parser.add_argument("--interval-seconds", type=int, default=30)
    parser.add_argument("--workers", type=int, default=12)
    args = parser.parse_args()
    if args.samples <= 0 or args.interval_seconds < 0 or args.workers <= 0:
        parser.error("samples/workers must be positive and interval-seconds non-negative")

    nodes = [line.strip() for line in args.nodes_file.read_text().splitlines() if line.strip()]
    if not nodes:
        parser.error("nodes-file contains no nodes")
    args.output.mkdir(parents=True, exist_ok=True)
    (args.output / "gpu-nodes.txt").write_text("\n".join(nodes) + "\n")

    for sample_index in range(args.samples):
        sample_dir = args.output / f"{sample_index:03d}-{timestamp()}"
        node_dir = sample_dir / "nodes"
        node_dir.mkdir(parents=True)
        (sample_dir / "squeue.json").write_text(ssh(args.bastion, "squeue --json"))
        (sample_dir / "sinfo.json").write_text(ssh(args.bastion, "sinfo --json"))
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
            futures = [pool.submit(capture_worker, args.bastion, node) for node in nodes]
            for future in concurrent.futures.as_completed(futures):
                node, payload = future.result()
                (node_dir / f"{node}.txt").write_text(payload)
        print(sample_dir, flush=True)
        if sample_index + 1 < args.samples:
            time.sleep(args.interval_seconds)


if __name__ == "__main__":
    main()
