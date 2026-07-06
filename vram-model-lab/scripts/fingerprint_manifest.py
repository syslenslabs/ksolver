#!/usr/bin/env python3
"""Extract scheduler-relevant fingerprints from Kubernetes training manifests."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import yaml


def sha256_json(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def pod_templates(obj: dict[str, Any]) -> list[dict[str, Any]]:
    kind = obj.get("kind")
    spec = obj.get("spec") or {}
    if kind in ("Pod",):
        return [obj]
    if kind == "CronJob":
        template = (((spec.get("jobTemplate") or {}).get("spec") or {}).get("template"))
        return [template] if template else []
    if kind in ("Job", "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet"):
        template = spec.get("template")
        if template:
            return [template]
    templates = []
    if kind == "RayJob":
        cluster = spec.get("rayClusterSpec") or {}
        head = ((cluster.get("headGroupSpec") or {}).get("template"))
        if head:
            templates.append(head)
        for worker in cluster.get("workerGroupSpecs") or []:
            template = worker.get("template")
            if template:
                templates.append(template)
        return templates
    if kind == "Workflow":
        for template in spec.get("templates") or []:
            container = template.get("container")
            if container:
                pod_template = {
                    "metadata": {"annotations": (obj.get("metadata") or {}).get("annotations") or {}},
                    "spec": {
                        "containers": [container],
                    },
                }
                templates.append(pod_template)
        return templates
    for value in spec.values():
        if isinstance(value, dict):
            for replica_spec in value.values():
                if isinstance(replica_spec, dict) and "template" in replica_spec:
                    templates.append(replica_spec["template"])
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict) and "template" in item:
                    templates.append(item["template"])
    return templates


def containers(template: dict[str, Any]) -> list[dict[str, Any]]:
    spec = template.get("spec") or {}
    return list(spec.get("containers") or []) + list(spec.get("initContainers") or [])


def fingerprint(path: Path) -> list[dict[str, Any]]:
    docs = [doc for doc in yaml.safe_load_all(path.read_text()) if doc]
    rows = []
    for idx, obj in enumerate(docs):
        metadata = obj.get("metadata") or {}
        for template_idx, template in enumerate(pod_templates(obj)):
            pod_spec = template.get("spec") or {}
            for container in containers(template):
                command = container.get("command") or []
                args = container.get("args") or []
                env = {e.get("name"): e.get("value") for e in container.get("env") or [] if e.get("name")}
                row = {
                    "source": str(path),
                    "document_index": idx,
                    "api_version": obj.get("apiVersion"),
                    "kind": obj.get("kind"),
                    "name": metadata.get("name"),
                    "namespace": metadata.get("namespace"),
                    "template_index": template_idx,
                    "scheduler_name": pod_spec.get("schedulerName"),
                    "runtime_class_name": pod_spec.get("runtimeClassName"),
                    "container_name": container.get("name"),
                    "image": container.get("image"),
                    "command": command,
                    "args": args,
                    "env": env,
                    "manifest_hash": sha256_json(obj),
                    "pod_template_hash": sha256_json(template),
                    "command_hash": sha256_json({"command": command, "args": args, "env": env}),
                }
                rows.append(row)
    return rows


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("manifest", type=Path)
    args = parser.parse_args()
    for row in fingerprint(args.manifest):
        print(json.dumps(row, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
