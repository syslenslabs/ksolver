#!/usr/bin/env python3
"""Convert a read-only NVIDIA cluster snapshot into ksolver packer input."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


def number(value: str | None) -> int:
    if not value:
        return 0
    match = re.search(r"-?\d+", value)
    return int(match.group(0)) if match else 0


def tag(block: str, name: str) -> str:
    match = re.search(rf"<{name}>(.*?)</{name}>", block, re.DOTALL)
    return match.group(1).strip() if match else ""


def parse_node(path: Path) -> list[dict]:
    text = path.read_text(encoding="utf-8", errors="replace")
    marker = "__KSOLVER_XML__\n"
    if marker not in text:
        return []
    prefix, xml = text.split(marker, 1)
    header = prefix.splitlines()
    if not header:
        return []
    node = header[0].strip()
    result = []
    for gpu in re.findall(r"<gpu(?:\s[^>]*)?>(.*?)</gpu>", xml, re.DOTALL):
        processes = re.findall(r"<process_info>(.*?)</process_info>", gpu, re.DOTALL)
        fb_memory = tag(gpu, "fb_memory_usage")
        utilization = tag(gpu, "utilization")
        result.append(
            {
                "node": node,
                "uuid": tag(gpu, "uuid"),
                "index": number(tag(gpu, "minor_number")),
                "model": tag(gpu, "product_name"),
                "total_mib": number(tag(fb_memory, "total")),
                "used_mib": number(tag(fb_memory, "used")),
                "gpu_util_pct": number(tag(utilization, "gpu_util")),
                "memory_util_pct": number(tag(utilization, "memory_util")),
                "process_count": len(processes),
                "processes": [
                    {
                        "pid": number(tag(process, "pid")),
                        "used_mib": number(tag(process, "used_memory")),
                        "name": tag(process, "process_name"),
                    }
                    for process in processes
                ],
                "source": path.name,
            }
        )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("snapshot_dir", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()

    node_dir = args.snapshot_dir / "nodes"
    gpus = []
    for path in sorted(node_dir.glob("*.txt")):
        gpus.extend(parse_node(path))

    payload = {
        "source": "read-only nvidia-smi snapshot",
        "snapshot_dir": str(args.snapshot_dir),
        "gpu_count": len(gpus),
        "node_count": len({gpu["node"] for gpu in gpus}),
        "gpus": gpus,
    }
    output = args.output or args.snapshot_dir / "snapshot-manifest.json"
    output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(output)


if __name__ == "__main__":
    main()
