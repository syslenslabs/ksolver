#!/usr/bin/env python3
"""Simple sidecar collector for ksolver VRAM profile files.

The training process writes /ksolver/profile/vram-profile.json through the
ksolver_vram_profile SDK. This sidecar tails that file from a shared emptyDir
volume and emits a normalized JSON record to stdout. In a production agent, this
same loop would POST to ksolver or write to a node-local collector.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Any


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_profile(path: Path) -> dict[str, Any]:
    profile = json.loads(path.read_text())
    return {
        "schema_version": 1,
        "event": "ksolver_vram_profile_observed",
        "observed_at_unix": int(time.time()),
        "profile_path": str(path),
        "profile_hash": file_hash(path),
        "pod_name": os.environ.get("HOSTNAME") or profile.get("pod_name"),
        "namespace": profile.get("namespace"),
        "profile": profile,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", type=Path, default=Path("/ksolver/profile/vram-profile.json"))
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--timeout-seconds", type=float, default=0.0, help="0 means run forever")
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    started = time.time()
    last_hash = None
    while True:
        if args.profile.exists():
            current_hash = file_hash(args.profile)
            if current_hash != last_hash:
                print(json.dumps(read_profile(args.profile), sort_keys=True), flush=True)
                last_hash = current_hash
                if args.once:
                    return 0
        if args.timeout_seconds and time.time() - started >= args.timeout_seconds:
            return 2 if last_hash is None else 0
        time.sleep(args.poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
