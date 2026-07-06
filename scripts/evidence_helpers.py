#!/usr/bin/env python3
"""Shared helpers for ksolver shadow evidence/operator artifacts."""

from __future__ import annotations

from typing import Any


CATEGORY_PRIORITY = {
    "environment": 0,
    "baseline-proof": 1,
    "live-trace": 2,
    "repair-proof": 3,
    "customer-proof": 4,
    "trust-proof": 5,
}


COMMAND_SPECS = {
    "environment": (
        [
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
            "kubectl config current-context",
            "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
            "kubectl --request-timeout=10s get nodes",
        ],
        "shell",
        True,
    ),
    "baseline-proof": (["scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache"], "shell", True),
    "live-trace": (
        ["kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending"],
        "shell",
        True,
    ),
    "repair-proof": (
        ["curl -s http://127.0.0.1:8090/api/scheduler/repair-plan | jq .proof_status"],
        "shell",
        True,
    ),
    "customer-proof": (
        ["attach pricing catalog, chargeback export, contract rate sheet, or invoice sample"],
        "manual",
        False,
    ),
    "trust-proof": (
        ["collect completed-job prediction calibration and candidate-regret evidence"],
        "manual",
        False,
    ),
}


def environment_command_hints(next_action: Any = None) -> list[str]:
    commands = [
        "kubectl config current-context",
        "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
        "kubectl --request-timeout=10s get nodes",
    ]
    action = str(next_action or "").lower()
    if "get --raw='/readyz?verbose'" in action or "api connectivity" in action:
        commands[0], commands[1] = commands[1], commands[0]
    elif "rbac" in action or "can-i" in action or "list/watch" in action:
        commands[0], commands[2] = commands[2], commands[0]
    return commands


def missing_artifact_category_counts(rows: list[Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or "unknown")
        counts[category] = counts.get(category, 0) + 1
    return dict(sorted(counts.items()))


def missing_artifact_category_rows(rows: list[Any]) -> list[dict[str, Any]]:
    categories: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or "unknown")
        severity = str(row.get("severity") or "missing")
        entry = categories.setdefault(
            category,
            {
                "category": category,
                "total": 0,
                "blocked": 0,
                "warn": 0,
                "severity": "missing",
                "artifact": None,
                "proof_gate": None,
                "next_action": None,
            },
        )
        entry["total"] += 1
        if severity == "blocked":
            entry["blocked"] += 1
        elif severity == "warn":
            entry["warn"] += 1
        if entry["artifact"] is None or (severity == "blocked" and entry["severity"] != "blocked"):
            entry["severity"] = severity
            entry["artifact"] = row.get("artifact")
            entry["proof_gate"] = row.get("proof_gate")
            entry["next_action"] = row.get("next_action")
    for entry in categories.values():
        entry["severity"] = "blocked" if entry["blocked"] else ("warn" if entry["warn"] else entry["severity"])
    return sorted(
        categories.values(),
        key=lambda row: (
            -int(row["blocked"]),
            -int(row["warn"]),
            CATEGORY_PRIORITY.get(str(row["category"]), 100),
            -int(row["total"]),
            str(row["category"]),
        ),
    )


def missing_artifact_action_items(category_rows: list[Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for idx, row in enumerate(category_rows):
        if not isinstance(row, dict):
            continue
        category = str(row.get("category") or "unknown")
        command_hints, command_kind, copyable = COMMAND_SPECS.get(category, ([], "none", False))
        if category == "environment":
            command_hints = environment_command_hints(row.get("next_action"))
        command_hint = command_hints[0] if command_hints else None
        items.append(
            {
                "priority": idx + 1,
                "category": category,
                "severity": row.get("severity") or "missing",
                "blocked": row.get("blocked") or 0,
                "warn": row.get("warn") or 0,
                "artifact": row.get("artifact"),
                "next_action": row.get("next_action")
                or "collect the missing evidence for this category",
                "command_hint": command_hint,
                "command_hints": command_hints,
                "command_kind": command_kind,
                "copyable": copyable,
            }
        )
    return items


def operator_action_runbook(action_items: list[Any]) -> dict[str, Any]:
    steps = [item for item in action_items if isinstance(item, dict)]
    copyable_commands: list[str] = []
    copyable_command_rows: list[dict[str, Any]] = []

    def add_copyable_command(command: Any, item: dict[str, Any]) -> None:
        text = str(command)
        if not text or text in copyable_commands:
            return
        copyable_commands.append(text)
        copyable_command_rows.append(
            {
                "command": text,
                "priority": item.get("priority"),
                "category": item.get("category"),
                "severity": item.get("severity"),
                "artifact": item.get("artifact"),
                "next_action": item.get("next_action"),
                "command_kind": item.get("command_kind") or "shell",
            }
        )

    for item in steps:
        if item.get("copyable") is not True:
            continue
        commands = item.get("command_hints")
        if isinstance(commands, list):
            for command in commands:
                if command:
                    add_copyable_command(command, item)
        elif item.get("command_hint"):
            add_copyable_command(item.get("command_hint"), item)
    return {
        "step_count": len(steps),
        "blocked_step_count": sum(1 for item in steps if item.get("severity") == "blocked"),
        "manual_step_count": sum(1 for item in steps if item.get("command_kind") == "manual"),
        "copyable_command_count": len(copyable_commands),
        "next_step": steps[0] if steps else None,
        "next_shell_command": copyable_commands[0] if copyable_commands else None,
        "copyable_commands": copyable_commands,
        "copyable_command_rows": copyable_command_rows,
        "steps": steps,
    }


def operator_runbook_command_rows(runbook: Any) -> list[dict[str, Any]]:
    if not isinstance(runbook, dict):
        return []
    rows = runbook.get("copyable_command_rows")
    if isinstance(rows, list) and rows:
        return [row for row in rows if isinstance(row, dict)]

    synthesized: list[dict[str, Any]] = []
    seen: set[str] = set()
    steps = runbook.get("steps") or []
    if not isinstance(steps, list):
        return []
    for item in steps:
        if not isinstance(item, dict):
            continue
        commands = item.get("command_hints")
        is_copyable = item.get("copyable") is True or item.get("command_kind") == "shell"
        if not is_copyable:
            continue
        if not isinstance(commands, list):
            commands = [item.get("command_hint")] if item.get("command_hint") else []
        for command in commands:
            text = str(command or "")
            if not text or text in seen:
                continue
            seen.add(text)
            synthesized.append(
                {
                    "command": text,
                    "priority": item.get("priority"),
                    "category": item.get("category"),
                    "severity": item.get("severity"),
                    "artifact": item.get("artifact"),
                    "next_action": item.get("next_action"),
                    "command_kind": item.get("command_kind") or "shell",
                }
            )
    return synthesized


def category_counts_text(counts: dict[str, Any]) -> str:
    rows = [
        (str(category), int(count or 0))
        for category, count in (counts or {}).items()
        if int(count or 0) > 0
    ]
    rows.sort(key=lambda item: (-item[1], item[0]))
    return ", ".join(f"{category} {count}" for category, count in rows)


def display_vram_driver_label(label: Any) -> str:
    text = str(label or "")
    if text == "synthetic reserve pressure":
        return "synthetic VRAM headroom probe"
    if text == "synthetic transformer reserve pressure":
        return "synthetic transformer headroom probe"
    return text


def display_vram_driver_labels(labels: list[Any]) -> list[str]:
    return [display_vram_driver_label(label) for label in labels]


def synthetic_headroom_driver_value(vram: dict[str, Any]) -> bool | None:
    if "synthetic_headroom_driver" in vram and vram.get("synthetic_headroom_driver") is not None:
        return vram.get("synthetic_headroom_driver") is True
    if "vram_synthetic_headroom_driver" in vram and vram.get("vram_synthetic_headroom_driver") is not None:
        return vram.get("vram_synthetic_headroom_driver") is True
    if "synthetic_reserve_driver" in vram and vram.get("synthetic_reserve_driver") is not None:
        return vram.get("synthetic_reserve_driver") is True
    if "vram_synthetic_reserve_driver" in vram and vram.get("vram_synthetic_reserve_driver") is not None:
        return vram.get("vram_synthetic_reserve_driver") is True
    return None


def synthetic_headroom_driver_enabled(vram: dict[str, Any]) -> bool:
    return synthetic_headroom_driver_value(vram) is True
