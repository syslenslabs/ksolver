#!/usr/bin/env python3
"""Fast local diagnostics for a ksolver shadow demo environment."""

from __future__ import annotations

import argparse
import json
import shlex
import sys
import urllib.error
import urllib.request
from typing import Any, Callable


Fetch = Callable[[str, float], tuple[int | None, bytes, str | None]]


def shell_join(parts: list[Any]) -> str:
    return shlex.join(str(part) for part in parts)


def kss_pool_command(action: str, count: int | None, base_port: int | None, cache_dir: str | None) -> str:
    if count and base_port and cache_dir:
        return shell_join(["scripts/kss-pool.sh", action, count, base_port, cache_dir])
    return shell_join(["scripts/kss-pool.sh", action, 4, 12120, "/tmp/ksolver-kss-cache"])


def fetch_url(url: str, timeout: float) -> tuple[int | None, bytes, str | None]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read(), None
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), None
    except Exception as exc:  # pragma: no cover - exact urllib errors vary by platform.
        return None, b"", str(exc)


def parse_json(body: bytes) -> dict[str, Any]:
    if not body:
        return {}
    try:
        payload = json.loads(body.decode("utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def probe_http_text(base: str, path: str, *, timeout: float, fetcher: Fetch) -> dict[str, Any]:
    status, body, error = fetcher(f"{base}{path}", timeout)
    text = body.decode("utf-8", errors="replace").strip() if body else ""
    return {
        "ok": status is not None and 200 <= status < 300,
        "status": status,
        "body": text[:500],
        "error": error,
    }


def probe_http_json(base: str, path: str, *, timeout: float, fetcher: Fetch) -> dict[str, Any]:
    status, body, error = fetcher(f"{base}{path}", timeout)
    payload = parse_json(body)
    return {
        "ok": status is not None and 200 <= status < 300 and bool(payload),
        "status": status,
        "payload": payload,
        "error": error,
    }


def split_urls(value: str | None) -> list[str]:
    if not value:
        return []
    return [part.strip().rstrip("/") for part in value.split(",") if part.strip()]


def default_kss_urls(count: int, base_port: int) -> list[str]:
    return [f"http://127.0.0.1:{base_port + idx}" for idx in range(max(count, 0))]


def probe_kss(urls: list[str], *, timeout: float, fetcher: Fetch) -> dict[str, Any]:
    probes: list[dict[str, Any]] = []
    for url in urls:
        status, body, error = fetcher(f"{url.rstrip('/')}/api/v1/export", timeout)
        ready = status is not None and 200 <= status < 300 and bool(parse_json(body))
        probes.append(
            {
                "url": url.rstrip("/"),
                "ready": ready,
                "status": status,
                "error": error,
            }
        )
    ready_urls = [probe["url"] for probe in probes if probe["ready"]]
    return {
        "checked_count": len(probes),
        "ready_count": len(ready_urls),
        "ready_urls": ready_urls,
        "probes": probes,
    }


def first_debug_command(
    operator_status: dict[str, Any],
    production_safety: dict[str, Any],
    evidence_bundle: dict[str, Any] | None = None,
) -> str | None:
    for payload in (operator_status, production_safety):
        commands = (
            ((payload.get("production_readiness") or {}).get("debug_commands"))
            or payload.get("debug_commands")
            or []
        )
        if commands:
            return str(commands[0])
    readiness = production_safety.get("readiness") or {}
    commands = readiness.get("debug_commands") or []
    if commands:
        return str(commands[0])
    summary = (evidence_bundle or {}).get("summary") or {}
    first = summary.get("production_readiness_first_debug_command")
    if first:
        return str(first)
    commands = summary.get("production_readiness_debug_commands") or []
    return str(commands[0]) if commands else None


def simulator_claim_from(
    operator_status: dict[str, Any], evidence_bundle: dict[str, Any]
) -> tuple[bool | None, str | None, str | None, str | None]:
    simulator = operator_status.get("simulator") or {}
    summary = evidence_bundle.get("summary") or {}
    ready = simulator.get("claim_ready")
    if ready is None:
        ready = summary.get("simulator_claim_ready")
    return (
        ready if isinstance(ready, bool) else None,
        simulator.get("claim_mode") or summary.get("simulator_claim_mode"),
        simulator.get("claim_blocker") or summary.get("simulator_claim_blocker"),
        simulator.get("claim_next_action") or summary.get("simulator_claim_next_action"),
    )


def decision_readiness_from(operator_status: dict[str, Any]) -> dict[str, Any]:
    decision = operator_status.get("decision_readiness") or {}
    binding = operator_status.get("binding_safety") or {}
    capabilities = decision.get("capabilities") or []
    production_binding = next(
        (
            row
            for row in capabilities
            if isinstance(row, dict) and row.get("name") == "production_binding"
        ),
        {},
    )
    return {
        "status": decision.get("status"),
        "summary": decision.get("summary"),
        "highest_risk": decision.get("highest_risk"),
        "next_action": decision.get("next_action"),
        "production_binding_status": production_binding.get("status"),
        "production_binding_can_execute": production_binding.get("can_execute"),
        "production_binding_next_action": production_binding.get("next_action"),
        "reservation_pressure": binding.get("reservation_pressure"),
        "reservation_pressure_description": binding.get("reservation_pressure_description"),
        "reservation_pressure_scope": binding.get("reservation_pressure_scope"),
        "reservation_pressure_reason": binding.get("reservation_pressure_reason"),
        "reservation_pressure_next_action": binding.get("reservation_pressure_next_action"),
    }


def recommended_commands(
    *,
    healthz_ok: bool,
    readyz_ok: bool,
    kss_ready_count: int,
    kss_checked_count: int,
    kss_count: int | None,
    kss_base_port: int | None,
    kss_cache_dir: str | None,
    first_debug: str | None,
    simulator_claim_ready: bool | None,
    simulator_claim_next_action: str | None,
) -> list[dict[str, Any]]:
    commands: list[dict[str, Any]] = []
    if not healthz_ok:
        commands.append(
            {
                "category": "shadow-process",
                "severity": "blocked",
                "command": "cargo run --features rust-cp-sat -- shadow",
                "reason": "shadow /healthz is not reachable",
            }
        )
    if not readyz_ok and first_debug:
        commands.append(
            {
                "category": "kubernetes-readiness",
                "severity": "blocked",
                "command": first_debug,
                "reason": "shadow /readyz is blocked",
            }
        )
    if kss_checked_count > 0 and kss_ready_count == 0:
        status_command = kss_pool_command("status", kss_count, kss_base_port, kss_cache_dir)
        start_command = kss_pool_command("start", kss_count, kss_base_port, kss_cache_dir)
        commands.extend(
            [
                {
                    "category": "kube-scheduler-simulator",
                    "severity": "blocked",
                    "command": status_command,
                    "reason": "no kube-scheduler-simulator endpoint is ready",
                },
                {
                    "category": "kube-scheduler-simulator",
                    "severity": "blocked",
                    "command": start_command,
                    "reason": "start a local kube-scheduler-simulator pool",
                },
            ]
        )
    elif kss_checked_count > 0 and kss_ready_count < kss_checked_count:
        commands.append(
            {
                "category": "kube-scheduler-simulator",
                "severity": "warn",
                "command": kss_pool_command("status", kss_count, kss_base_port, kss_cache_dir),
                "reason": "only some kube-scheduler-simulator endpoints are ready",
            }
        )
    if simulator_claim_ready is False and simulator_claim_next_action:
        commands.append(
            {
                "category": "simulator-claim",
                "severity": "blocked",
                "command": None,
                "reason": simulator_claim_next_action,
            }
        )
    return commands


def api_endpoint_failure_rows(
    *,
    healthz_ok: bool,
    production: dict[str, Any],
    operator: dict[str, Any],
    evidence: dict[str, Any],
    base_url: str,
) -> list[dict[str, Any]]:
    if not healthz_ok:
        return []
    checks = [
        ("production-safety", "/api/scheduler/production-safety", production),
        ("operator-status", "/api/scheduler/operator-status", operator),
        ("evidence-bundle", "/api/scheduler/evidence-bundle", evidence),
    ]
    failures: list[dict[str, Any]] = []
    for name, path, probe in checks:
        if probe.get("ok"):
            continue
        failures.append(
            {
                "category": "shadow-api",
                "severity": "blocked",
                "endpoint": path,
                "status": probe.get("status"),
                "error": probe.get("error"),
                "command": shell_join(["curl", "-fsS", f"{base_url}{path}"]),
                "reason": f"{name} endpoint did not return a valid JSON object",
            }
        )
    return failures


def diagnose(
    *,
    base_url: str,
    kss_urls: list[str],
    timeout: float,
    require_readyz: bool,
    require_kss_ready: bool,
    require_simulator_claim_ready: bool,
    kss_count: int | None = None,
    kss_base_port: int | None = None,
    kss_cache_dir: str | None = None,
    fetcher: Fetch = fetch_url,
) -> dict[str, Any]:
    base = base_url.rstrip("/")
    healthz = probe_http_text(base, "/healthz", timeout=timeout, fetcher=fetcher)
    readyz = probe_http_text(base, "/readyz", timeout=timeout, fetcher=fetcher)
    production = probe_http_json(base, "/api/scheduler/production-safety", timeout=timeout, fetcher=fetcher)
    operator = probe_http_json(base, "/api/scheduler/operator-status", timeout=timeout, fetcher=fetcher)
    evidence = probe_http_json(base, "/api/scheduler/evidence-bundle", timeout=timeout, fetcher=fetcher)
    kss = probe_kss(kss_urls, timeout=timeout, fetcher=fetcher)

    production_payload = production.get("payload") or {}
    operator_payload = operator.get("payload") or {}
    evidence_payload = evidence.get("payload") or {}
    readiness = production_payload.get("readiness") or {}
    simulator_claim_ready, simulator_claim_mode, simulator_claim_blocker, simulator_claim_next_action = (
        simulator_claim_from(operator_payload, evidence_payload)
    )
    decision_readiness = decision_readiness_from(operator_payload)
    production_blocker_class = (
        readiness.get("blocker_class")
        or (operator_payload.get("production_readiness") or {}).get("blocker_class")
        or ((evidence_payload.get("summary") or {}).get("production_readiness_blocker_class"))
    )
    next_action = (
        operator_payload.get("next_action")
        or readiness.get("next_action")
        or (evidence_payload.get("summary") or {}).get("primary_claim_blocker_next_action")
    )
    debug_command = first_debug_command(operator_payload, production_payload, evidence_payload)
    api_failures = api_endpoint_failure_rows(
        healthz_ok=bool(healthz.get("ok")),
        production=production,
        operator=operator,
        evidence=evidence,
        base_url=base,
    )

    failures: list[str] = []
    if not healthz.get("ok"):
        failures.append("shadow healthz is not reachable")
    failures.extend(str(row["reason"]) for row in api_failures)
    if require_readyz and not readyz.get("ok"):
        failures.append("shadow readyz is not ready")
    if require_kss_ready and kss["ready_count"] == 0:
        failures.append("no kube-scheduler-simulator endpoint is ready")
    if require_simulator_claim_ready and simulator_claim_ready is not True:
        failures.append("simulator claim is not ready")

    status = "ready" if not failures else "blocked"
    if healthz.get("ok") and not readyz.get("ok") and not require_readyz and status == "ready":
        status = "degraded"
    recommendations = recommended_commands(
        healthz_ok=bool(healthz.get("ok")),
        readyz_ok=bool(readyz.get("ok")),
        kss_ready_count=int(kss["ready_count"]),
        kss_checked_count=int(kss["checked_count"]),
        kss_count=kss_count,
        kss_base_port=kss_base_port,
        kss_cache_dir=kss_cache_dir,
        first_debug=debug_command,
        simulator_claim_ready=simulator_claim_ready,
        simulator_claim_next_action=simulator_claim_next_action,
    )
    recommendations.extend(api_failures)
    first_recommended = next(
        (row.get("command") for row in recommendations if row.get("command")),
        None,
    )

    return {
        "ok": not failures,
        "status": status,
        "base_url": base,
        "healthz_ok": healthz.get("ok"),
        "healthz_status": healthz.get("status"),
        "readyz_ok": readyz.get("ok"),
        "readyz_status": readyz.get("status"),
        "readyz_body": readyz.get("body"),
        "production_safety_ok": production.get("ok"),
        "operator_status_ok": operator.get("ok"),
        "evidence_bundle_ok": evidence.get("ok"),
        "api_endpoint_failures": api_failures,
        "production_readiness_blocker_class": production_blocker_class,
        "production_readiness_last_error_class": readiness.get("last_error_class"),
        "next_action": next_action,
        "first_debug_command": debug_command,
        "simulator_claim_ready": simulator_claim_ready,
        "simulator_claim_mode": simulator_claim_mode,
        "simulator_claim_blocker": simulator_claim_blocker,
        "simulator_claim_next_action": simulator_claim_next_action,
        "operator_decision_status": decision_readiness.get("status"),
        "operator_decision_summary": decision_readiness.get("summary"),
        "operator_decision_highest_risk": decision_readiness.get("highest_risk"),
        "operator_decision_next_action": decision_readiness.get("next_action"),
        "operator_production_binding_status": decision_readiness.get("production_binding_status"),
        "operator_production_binding_can_execute": decision_readiness.get("production_binding_can_execute"),
        "operator_production_binding_next_action": decision_readiness.get("production_binding_next_action"),
        "operator_reservation_pressure": decision_readiness.get("reservation_pressure"),
        "operator_reservation_pressure_description": decision_readiness.get("reservation_pressure_description"),
        "operator_reservation_pressure_scope": decision_readiness.get("reservation_pressure_scope"),
        "operator_reservation_pressure_reason": decision_readiness.get("reservation_pressure_reason"),
        "operator_reservation_pressure_next_action": decision_readiness.get("reservation_pressure_next_action"),
        "kss_checked_count": kss["checked_count"],
        "kss_ready_count": kss["ready_count"],
        "kss_ready_urls": kss["ready_urls"],
        "kss_probes": kss["probes"],
        "recommended_commands": recommendations,
        "first_recommended_command": first_recommended,
        "failures": failures,
    }


def printable_summary(result: dict[str, Any]) -> str:
    parts = [
        f"shadow doctor {result.get('status')}: {result.get('base_url')}",
        f"healthz={'ok' if result.get('healthz_ok') else 'blocked'}",
        f"readyz={'ok' if result.get('readyz_ok') else 'blocked'}",
        f"KSS={result.get('kss_ready_count')}/{result.get('kss_checked_count')} ready",
    ]
    if result.get("simulator_claim_mode"):
        suffix = "ready" if result.get("simulator_claim_ready") is True else "blocked"
        parts.append(f"simulator claim={result.get('simulator_claim_mode')} ({suffix})")
    if result.get("operator_decision_status"):
        parts.append(f"decision={result.get('operator_decision_status')}")
    if result.get("operator_production_binding_status"):
        bind = f"binding={result.get('operator_production_binding_status')}"
        if result.get("operator_production_binding_can_execute") is not None:
            bind += (
                " executable"
                if result.get("operator_production_binding_can_execute") is True
                else " not-executable"
            )
        parts.append(bind)
    if result.get("operator_reservation_pressure"):
        parts.append(f"binding reservation pressure={result.get('operator_reservation_pressure')}")
    if result.get("operator_reservation_pressure_scope"):
        parts.append(f"binding reservation pressure scope={result.get('operator_reservation_pressure_scope')}")
    if result.get("operator_decision_highest_risk"):
        parts.append(f"risk={result.get('operator_decision_highest_risk')}")
    if result.get("production_readiness_blocker_class"):
        parts.append(f"production blocker={result.get('production_readiness_blocker_class')}")
    api_failures = result.get("api_endpoint_failures") or []
    if api_failures:
        parts.append(f"API failures={len(api_failures)}")
        first_api_failure = api_failures[0] if isinstance(api_failures[0], dict) else {}
        if first_api_failure.get("endpoint"):
            parts.append(f"first API failure={first_api_failure.get('endpoint')}")
        if first_api_failure.get("command"):
            parts.append(f"API command={first_api_failure.get('command')}")
    if result.get("first_debug_command"):
        parts.append(f"debug={result.get('first_debug_command')}")
    if result.get("first_recommended_command"):
        parts.append(f"first command={result.get('first_recommended_command')}")
    if result.get("next_action"):
        parts.append(f"next={result.get('next_action')}")
    if result.get("failures"):
        parts.append("failures=" + "; ".join(str(item) for item in result["failures"]))
    return ", ".join(parts)


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnose a local ksolver shadow and KSS demo setup.")
    parser.add_argument("--base-url", default="http://127.0.0.1:8090", help="shadow server URL")
    parser.add_argument(
        "--kss-urls",
        help="comma-separated kube-scheduler-simulator URLs; overrides --kss-count/--kss-base-port",
    )
    parser.add_argument("--kss-count", type=int, default=4, help="default local KSS pool size")
    parser.add_argument("--kss-base-port", type=int, default=12120, help="default local KSS pool base port")
    parser.add_argument(
        "--kss-cache-dir",
        default="/tmp/ksolver-kss-cache",
        help="KSS simulator cache directory used in recommended commands",
    )
    parser.add_argument("--timeout", type=float, default=2.0, help="HTTP timeout in seconds")
    parser.add_argument("--require-readyz", action="store_true", help="exit 2 unless /readyz is ready")
    parser.add_argument(
        "--require-kss-ready",
        action="store_true",
        help="exit 2 unless at least one configured kube-scheduler-simulator endpoint exports state",
    )
    parser.add_argument(
        "--require-simulator-claim-ready",
        action="store_true",
        help="exit 2 unless the evidence/operator APIs prove the simulator claim is ready",
    )
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()

    kss_urls = split_urls(args.kss_urls) or default_kss_urls(args.kss_count, args.kss_base_port)
    result = diagnose(
        base_url=args.base_url,
        kss_urls=kss_urls,
        timeout=args.timeout,
        require_readyz=args.require_readyz,
        require_kss_ready=args.require_kss_ready,
        require_simulator_claim_ready=args.require_simulator_claim_ready,
        kss_count=args.kss_count,
        kss_base_port=args.kss_base_port,
        kss_cache_dir=args.kss_cache_dir,
    )
    if args.json:
        print(json.dumps(result, sort_keys=True))
    else:
        print(printable_summary(result))
    return 0 if result.get("ok") else 2


if __name__ == "__main__":
    sys.exit(main())
