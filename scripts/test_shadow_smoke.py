#!/usr/bin/env python3
"""Unit tests for the shadow smoke validation helpers."""

from __future__ import annotations

import importlib.util
import json
import pathlib
import subprocess
import sys
import unittest


ROOT = pathlib.Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("shadow_smoke", ROOT / "shadow-smoke.py")
assert SPEC and SPEC.loader
shadow_smoke = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(shadow_smoke)


def winning_scenario() -> dict:
    def engine(useful_gpu: int, *, source: str, simulator: dict | None = None) -> dict:
        row = {
            "engine": source.split()[0],
            "source": source,
            "metrics": {"useful_gpu": useful_gpu},
            "placements": [],
        }
        if simulator is not None:
            row["simulator"] = simulator
        return row

    return {
        "name": "fragmentation-demo",
        "ksolver": engine(8, source="ksolver batch solver"),
        "kube": engine(
            2,
            source="kube-scheduler-simulator spread",
            simulator={
                "mode": "cached",
                "variant": "spread",
                "cache_key": "fragmentation-demo/spread",
                "timed_out": False,
            },
        ),
        "kube_binpack": engine(
            4,
            source="kube-scheduler-simulator binpack",
            simulator={
                "mode": "cached",
                "variant": "binpack",
                "cache_key": "fragmentation-demo/binpack",
                "timed_out": False,
            },
        ),
    }


def valid_vram_investment_demo_summary() -> dict:
    rows = [
        {
            "scenario": "oom-avoidance-a",
            "workload": "hf-transformer-13b",
            "predictor_source": "synthetic",
            "confidence": 82,
            "gpu_request": 1,
            "predicted_lower_vram_gib": 32,
            "predicted_peak_vram_gib": 39,
            "predicted_upper_vram_gib": 46,
            "kube_node": "l4-24g-1",
            "kube_node_vram_gib": 24,
            "kube_cuda_oom_risk_percent": 95,
            "kube_risk_label": "likely CUDA OOM",
            "kube_upper_band_headroom_gib": -22,
            "ksolver_node": "a100-80g-1",
            "ksolver_node_vram_gib": 80,
            "ksolver_cuda_oom_risk_percent": 10,
            "ksolver_risk_label": "low",
            "ksolver_upper_band_headroom_gib": 34,
            "risk_delta_percent": 85,
            "avoided_failure": True,
            "preserves_high_vram_capacity": False,
            "advisory_only": False,
            "decision_reason": "moves work from a likely-OOM node to a node with enough upper-band VRAM headroom",
            "investment_case": "avoid failed startup",
            "caveat": "synthetic predictor",
        },
        {
            "scenario": "preserve-high-vram",
            "workload": "small-cnn",
            "predictor_source": "synthetic",
            "confidence": 76,
            "gpu_request": 1,
            "predicted_lower_vram_gib": 4,
            "predicted_peak_vram_gib": 6,
            "predicted_upper_vram_gib": 8,
            "kube_node": "a100-80g-2",
            "kube_node_vram_gib": 80,
            "kube_cuda_oom_risk_percent": 5,
            "kube_risk_label": "low",
            "kube_upper_band_headroom_gib": 72,
            "ksolver_node": "t4-16g-1",
            "ksolver_node_vram_gib": 16,
            "ksolver_cuda_oom_risk_percent": 15,
            "ksolver_risk_label": "low",
            "ksolver_upper_band_headroom_gib": 8,
            "risk_delta_percent": -10,
            "avoided_failure": False,
            "preserves_high_vram_capacity": True,
            "advisory_only": False,
            "decision_reason": "keeps scarce high-memory GPUs available by placing low-memory work on a smaller fit",
            "investment_case": "preserve scarce 80Gi GPU",
            "caveat": "synthetic predictor",
        },
        {
            "scenario": "unknown-inventory",
            "workload": "jax-train",
            "predictor_source": "synthetic",
            "confidence": 45,
            "gpu_request": 1,
            "predicted_lower_vram_gib": 12,
            "predicted_peak_vram_gib": 16,
            "predicted_upper_vram_gib": 22,
            "kube_node": "unknown-gpu",
            "kube_node_vram_gib": 0,
            "kube_cuda_oom_risk_percent": 70,
            "kube_risk_label": "advisory",
            "kube_upper_band_headroom_gib": 0,
            "ksolver_node": "unknown-gpu",
            "ksolver_node_vram_gib": 0,
            "ksolver_cuda_oom_risk_percent": 70,
            "ksolver_risk_label": "advisory",
            "ksolver_upper_band_headroom_gib": 0,
            "risk_delta_percent": 0,
            "avoided_failure": False,
            "preserves_high_vram_capacity": False,
            "advisory_only": True,
            "decision_reason": "inventory missing; keep advisory until node VRAM is known",
            "investment_case": "needs inventory labels",
            "caveat": "Unknown node memory",
        },
    ]
    rows.extend(
        {
            **rows[0],
            "scenario": f"oom-avoidance-extra-{idx}",
            "workload": f"hf-transformer-extra-{idx}",
        }
        for idx in range(3)
    )
    return {
        "name": "vram-predictor-investment-demo",
        "passed": True,
        "headline": "Synthetic VRAM predictor demo reduces likely CUDA OOM placements.",
        "synthetic_prediction_notice": (
            "Predicted peaks, confidence bands, and OOM likelihoods are deterministic "
            "fake values for demo design; use them to argue for collecting real "
            "DCGM/NVML calibration data, not as production accuracy claims."
        ),
        "scenario_count": len(rows),
        "baseline_cuda_oom_risk_pods": 4,
        "ksolver_cuda_oom_risk_pods": 0,
        "cuda_oom_risk_reduction_pods": 4,
        "high_vram_nodes_preserved": 1,
        "unknown_or_advisory_rows": 1,
        "average_baseline_oom_risk_percent": 64,
        "average_ksolver_oom_risk_percent": 24,
        "rows": rows,
        "operator_claims": [
            "VRAM predictions can prevent known-bad placements before startup time is wasted.",
            "Upper confidence bands are the right scheduling primitive for OOM avoidance.",
            "Rightsizing by VRAM preserves scarce high-memory GPUs.",
        ],
        "required_real_predictor_evidence": [
            "per-pod GPU VRAM peak from DCGM/NVML or equivalent attribution",
            "prediction keys including image digest, command hash, framework, precision, batch, sequence length, optimizer, and strategy",
            "source-tier MAPE and upper-band miss rate by GPU SKU",
            "online audit rows with confidence and lower/upper VRAM band",
        ],
    }


def valid_report() -> dict:
    return {
        "ok": True,
        "report": {
            "scenario_pages": [
                {"slug": "vram-binpacking", "title": "VRAM usage prediction & binpacking"},
                {"slug": "gang-scheduling", "title": "Gang scheduling"},
                {"slug": "preemption-migration", "title": "Preemption / migration"},
            ],
            "scenarios": [winning_scenario()],
            "vram_investment_demo_summary": valid_vram_investment_demo_summary(),
            "demo_readiness_summary": {
                "passed": True,
                "primary_story": "ksolver proves the demo story",
                "kube_baseline_mode": "cached kube-scheduler-simulator baselines",
                "live_validation_rows": [
                    {
                        "gate": "pending GPU trace",
                        "live_endpoint": "/api/scheduler/traces",
                    },
                    {
                        "gate": "kube baseline provenance",
                        "live_endpoint": "/api/scheduler/kube-simulator-plan",
                    }
                ],
            },
        },
    }


def valid_html() -> str:
    return "\n".join(
        [
            "ksolver · GPU Scheduler Studio",
            '<button class="btn ghost" id="clear-btn" type="button" title="Clear browser-cached run comparisons only; live traces and scenario evidence are unchanged.">Clear all</button>',
            '<button class="btn ghost" id="rerun-btn" type="button" title="Bypass the browser run cache and ask ksolver to solve this configuration again.">Run fresh</button>',
            '<button class="btn primary" id="run-btn" type="button">Run simulation</button>',
            '<button class="tab" id="tab-runs" type="button" data-panel="panel-runs" aria-controls="panel-runs" tabindex="0" aria-selected="true"></button>',
            '<button class="tab" id="tab-live" type="button" data-panel="panel-live" aria-controls="panel-live" tabindex="-1" aria-selected="false"></button>',
            '<section class="panel active" role="tabpanel" id="panel-runs" aria-labelledby="tab-runs"></section>',
            '<section role="tabpanel" id="panel-live" aria-labelledby="tab-live" hidden></section>',
            '<div class="toast" id="toast" role="status" aria-live="polite" aria-atomic="false"></div>',
            ".scen-page-filter .btn.active",
            "aria-pressed",
            "aria-controls",
            'tabindex="0"',
            'tabindex="-1"',
            "panel.hidden = !on",
            "function focusTab",
            'addEventListener("keydown"',
            "ArrowRight",
            "ArrowLeft",
            "Home",
            "End",
            "function itemNsName(item)",
            "itemNsName(p)",
            "itemNsName(d)",
            "d.priority == null ? \"\" : String(d.priority)",
            "p.kind || \"\"",
            "p.reason || \"\"",
            "((d.caveats || []).join(\",\"))",
            "var liveTrace = traces[0] || null",
            "\"empty:\" + clusterSig(r[1]) + \"|\" + kubeSig(r[2])",
            "if (changed(\"live\", liveKey)) renderLive(liveTrace, r[1], r[2])",
            "if (!trace)",
            "no pending GPU decisions",
            "waiting for trace",
            "Waiting for a pending GPU trace before showing kube-scheduler-simulator placement.",
            "No live pending GPU workload to compare.",
            "o.requested_gpu_demand",
            "o.gpu_admission_percent_milli",
            "o.pod_admission_percent_milli",
            "o.predicted_deadline_misses",
            "p.target_gpu_request || 0",
            "p.explanation || \"\"",
            "a.action || \"\"",
            "a.pod || \"\"",
            "a.gpu_request || 0",
            "a.node || \"\"",
            "proof.headline || ((repairPlan && repairPlan.hero_reference)",
            "proof.operator_question || proof.evidence || proof.claim_guard",
            "proof.evidence ? \"evidence: \" + proof.evidence : \"\"",
            "proof.operator_question || \"\"",
            "((rp && rp.proof_status) || {}).operator_question || \"\"",
            "((rp && rp.proof_status) || {}).evidence || \"\"",
            "((rp && rp.proof_status) || {}).headline || \"\"",
            "((rp && rp.proof_status) || {}).claim_guard || \"\"",
            "var pagePart = (report && report.scenario_pages || []).map",
            "page.slug || \"\"",
            "page.title || \"\"",
            "((page.scenario_names || []).join(\",\"))",
            "function engineScenarioSig(engine)",
            "m.active_nodes || 0",
            "m.unplaced_pods || 0",
            "m.stranded_gpu_on_active_nodes || 0",
            "m.gpu_utilization_milli || 0",
            "m.partial_or_invalid_gangs || 0",
            "s.efficiency_headline || \"\"",
            "max-width: min(240px, 100%)",
            "var node = placedNode(pl)",
            "itemNsName(pl) + \" \" + itemGpus(pl) + \"g\"",
            "node ? \" → \" + shortName(node) : \" ×\"",
            "itemName(pl)",
            "placedNode(pl) || \"\"",
            "itemGpus(pl)",
            "payload && payload.report) || lastReport",
            "var effectiveCalibration = calibration || lastVramCalibration",
            "renderVramInvestmentDemo(report, effectiveCalibration)",
            "lastVramCalibration = calibration",
            "renderScenarios(lastReport, lastVramCalibration)",
            "var prevHtml = btn ? btn.innerHTML : \"\"",
            "btn.setAttribute(\"aria-busy\", \"true\")",
            "btn.appendChild(el(\"span\", \"spin\"))",
            "btn.appendChild(document.createTextNode(\" refreshing\"))",
            "btn.removeAttribute(\"aria-busy\")",
            "btn.innerHTML = prevHtml",
            "browser cache",
            "Restored from this browser's localStorage after a page reload",
            "Restored from browser cache; rerun this configuration to refresh solver and kube baseline evidence.",
            "Reused matching run from this page session.",
            "GPU-hour proxy",
            "Relative comparison only; not a cloud bill.",
            "Proxy/useful GPU",
            "id=\"price-proxy-note\"",
            "var gpuLabel = params.get(\"gpu_label\") || \"GPU\"",
            "var priceSource = params.get(\"price_source\") || \"demo default\"",
            "function gpuHourAssumptionText()",
            "GPU-hour proxy assumption: ",
            "$(\"price-proxy-note\").textContent = gpuHourAssumptionText()",
            "function currentPriceMeta()",
            "function priceKey(meta)",
            "function priceAssumptionText(meta)",
            "function runKey(c)",
            "configKey(c) + \"|price|\" + priceKey(currentPriceMeta())",
            "price: currentPriceMeta()",
            "Pricing assumption was not recorded for this cached run",
            "current page assumes",
            "Run fresh to capture gpu_hour, gpu_label, and price_source.",
            "price unknown",
            "price $",
            "Run pricing assumption: ",
            "Δ vs kube-scheduler-simulator baseline · ",
            "r.kubeProv || \"provenance unavailable\"",
            "No kube baseline captured for this run · ",
            "r.kubeProv || \"reason unavailable\"",
            "Bypass the browser run cache and ask ksolver to solve this configuration again.",
            "function setSolveButtonsDisabled(disabled)",
            "[\"run-btn\", \"rerun-btn\"].forEach",
            "if (b) b.disabled = disabled",
            "function runSimulation(force, triggerId)",
            "var btn = $(triggerId || \"run-btn\")",
            "setSolveButtonsDisabled(true)",
            "setSolveButtonsDisabled(false)",
            "$(\"run-btn\").addEventListener(\"click\", function () { runSimulation(false, \"run-btn\"); })",
            "$(\"rerun-btn\").addEventListener(\"click\", function () { runSimulation(true, \"rerun-btn\"); })",
            "Clear browser-cached run comparisons only; live traces and scenario evidence are unchanged.",
            "function clearRuns()",
            "localStorage.removeItem(RUNS_KEY)",
            "Cleared \" + count + \" browser-cached run",
            "No cached runs to clear",
            "$(\"clear-btn\").addEventListener(\"click\", clearRuns)",
            "engineScenarioSig(s.kube)",
            "engineScenarioSig(s.kube_binpack)",
            "demoRefresh.stale_report_reason || \"\"",
            "@media (max-width: 700px)",
            ".proof-section .card { margin-bottom: 10px; overflow-x: auto; }",
            "simulator_cache_coverage_milli",
            "diag-gates",
            "All live evidence gates",
            "diag-gate-list",
            "diag-command-list",
            "\"curl -s \" + window.location.origin",
            "Shadow readiness",
            "Decision readiness",
            "decision_readiness",
            "highest risk",
            "Scale safety",
            "scale_safety",
            "Binding safety",
            "binding_safety",
            "reservation_pressure_description",
            "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky",
            "active means fresh reservations temporarily hold GPU capacity while binding gates run.",
            "stale means expired reservation entries must be reconciled before trusting bind readiness.",
            "blocking means the reservation ledger rejected at least one planned placement.",
            "function reservePressureScopeNote(binding)",
            "reservation_pressure_scope",
            "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
            "state meaning",
            "reservePressureBannerMeta",
            "binding reservation pressure ",
            "var readyReserveMeta = reservePressureBannerMeta(binding)",
            "if (readyReserveMeta) readyMeta.push(readyReserveMeta)",
            "var summaryReadyMeta = [",
            "if (summaryReserveMeta) summaryReadyMeta.push(summaryReserveMeta)",
            "evidence.operator_reservation_pressure || \"\"",
            "evidence.operator_reservation_pressure_description || \"\"",
            "evidence.operator_reservation_pressure_scope || \"\"",
            "evidence.operator_reservation_pressure_reason || \"\"",
            "evidence.operator_reservation_pressure_next_action || \"\"",
            "var apiErrorBannerActive = false",
            "apiErrorBannerActive = true",
            "if (apiErrorBannerActive)",
            "sigs[\"operator-banner\"] = \"\"",
            "apiErrorBannerActive = false",
            "candidate_node_limit",
            "candidate edges",
            "edge reduction",
            "scale.explanation",
            "opScale.explanation || \"\"",
            "latest outcomes",
            "reservations",
            "binding reservation pressure",
            "reservation pressure reason",
            "reservation pressure action",
            "/healthz",
            "/readyz",
            "watch",
            "last error",
            "last error at",
            "kvRow(dlReady, \"last error\", shortText(rd.last_error, 120), \"bad\", rd.last_error)",
            "blocker_class",
            "diagnostic hint",
            "diagnostic_hint",
            "kvRow(dlReady, \"diagnostic hint\", shortText(rd.diagnostic_hint, 120), rd.ready ? \"ok\" : \"warn\", rd.diagnostic_hint)",
            "kvRow(dlReady, \"next action\", shortText(rd.next_action, 110), rd.ready ? \"ok\" : \"warn\", rd.next_action)",
            "next action",
            "kvRow(dlDecision, \"summary\", shortText(decision.summary, 150), decision.status === \"ready\" ? \"ok\" : \"warn\", decision.summary)",
            "kvRow(dlDecision, \"highest risk\", shortText(decision.highest_risk, 150), decision.status === \"ready\" ? \"ok\" : \"warn\", decision.highest_risk)",
            "kvRow(dlDecision, \"next action\", shortText(decision.next_action, 150), decision.status === \"ready\" ? \"ok\" : \"warn\", decision.next_action)",
            "kvRow(dlBinding, \"next action\", shortText(binding.next_action, 140), bindingFailures || bindingMutation ? \"warn\" : \"ok\", binding.next_action)",
            "kvRow(dl0, \"readiness note\", shortText(simulator.readiness_note, 110), simulator.live_dashboard_baseline_configured ? \"warn\" : \"warn\", simulator.readiness_note)",
            "kvRow(dlScale, \"widen reason\", shortText(scale.widening_reason, 120), \"warn\", scale.widening_reason)",
            "kvRow(dlScale, \"next action\", shortText(scale.next_action, 140), scaleRegretUnknown ? \"bad\" : \"ok\", scale.next_action)",
            "kvRow(dlScale, \"explanation\", shortText(scale.explanation, 140), scaleRegretUnknown ? \"warn\" : \"ok\", scale.explanation)",
            "debug_commands",
            "First readiness debug command",
            "All readiness debug commands",
            "debugCommands.forEach",
            "copyDiagCommand",
            "diagCommand",
            "row.title = [title, value].filter(Boolean).join(\" | \")",
            "text.title = value",
            "toast(ok ? \"Copied command\" : \"Copy failed\")",
            "Copy command",
            "navigator.clipboard.writeText",
            "fallbackCopy",
            "document.execCommand(\"copy\")",
            "document.createElement(\"textarea\")",
            "Admission mode",
            "Scheduler use",
            "Hard blockers",
            "Next evidence",
            "VRAM source",
            "VRAM hard-admission blockers",
            "VRAM evidence collection plan",
            "hard_admission_blockers",
            "evidence_collection_plan",
            "Shadow advisory only",
            "Score and warn; do not reject pods",
            "Synthetic headroom probes",
            "Max synthetic headroom",
            "not organic model demand",
            "What the VRAM model is using",
            "model_drivers",
            "top_drivers",
            "top_driver_labels",
            "vram_display_top_driver_labels",
            "vram_display_claim_safe_driver_labels",
            "vram_display_real_top_driver_labels",
            "vram_display_synthetic_driver_labels",
            "display_top_driver_labels",
            "display_claim_safe_driver_labels",
            "display_real_top_driver_labels",
            "display_synthetic_driver_labels",
            "VRAM drivers",
            "VRAM claim-safe drivers",
            "VRAM claim-safe top",
            "VRAM top drivers",
            "VRAM headroom probes",
            "VRAM synthetic probes",
            "VRAM synthetic headroom",
            "VRAM headroom meaning",
            "kvRow(dl5, \"simulator readiness note\", shortText(evSummary.simulator_readiness_note, 110), simReadinessStatus === \"ok\" ? \"ok\" : \"warn\", evSummary.simulator_readiness_note)",
            "kvRow(dl5, \"simulator claim blocker\", shortText(evSummary.simulator_claim_blocker, 120), \"bad\", evSummary.simulator_claim_blocker)",
            "kvRow(dl5, \"simulator claim action\", shortText(evSummary.simulator_claim_next_action, 140), simClaimReady ? \"ok\" : \"warn\", evSummary.simulator_claim_next_action)",
            "kvRow(dl5, \"primary blocker\", shortText(String(primaryBlocker), 90), \"warn\", String(primaryBlocker))",
            "kvRow(dl5, \"next action\", shortText(String(evSummary.primary_claim_blocker_next_action), 110), \"warn\", String(evSummary.primary_claim_blocker_next_action))",
            "var vramNextEvidence = opVram.next_evidence_target || evSummary.vram_next_evidence_target || \"unknown\"",
            "kvRow(dl5, \"VRAM next evidence\", vramNextEvidence, vramHardBlockerCount ? \"warn\" : \"ok\", vramNextEvidence)",
            "var vramClaimSafeTitle = vramClaimSafeLabels.join(\", \")",
            "kvRow(dl5, \"VRAM claim-safe top\", shortText(vramClaimSafeTitle, 120), \"ok\", vramClaimSafeTitle)",
            "var vramDriverTitle = vramDriverLabels.join(\", \")",
            "kvRow(dl5, \"VRAM top drivers\", shortText(vramDriverTitle, 120), \"ok\", vramDriverTitle)",
            "var vramSyntheticTitle = vramSyntheticLabels.join(\", \")",
            "kvRow(dl5, \"VRAM synthetic probes\", shortText(vramSyntheticTitle, 120), \"warn\", vramSyntheticTitle)",
            "kvRow(dl5, \"VRAM headroom meaning\", shortText(syntheticHeadroomDefinition, 140), \"warn\", syntheticHeadroomDefinition)",
            "opVram.next_evidence_target || \"\"",
            "opVram.model_driver_count || 0",
            "opVram.claim_safe_driver_count || 0",
            "opVram.synthetic_driver_count || 0",
            "opVram.synthetic_headroom_definition || opVram.reserve_pressure_definition || \"\"",
            "synthetic VRAM headroom probe",
            "synthetic-pressure",
            "vramDriverClassLabel",
            "vramDriverClassTitle",
            "headroom probe",
            "not organic model demand",
            "aria-label",
            "mean_abs_contribution_mib",
            "readinessRowsSig",
            "readiness.primary_story || \"\"",
            "((readiness.remaining_gaps || []).join(\";\"))",
            "row.required_evidence || \"\"",
            "row.pass_signal || \"\"",
            "row.failure_action || \"\"",
            "Evidence bundle",
            "/api/scheduler/evidence-bundle",
            "scripts/demo-gate.py --base-url",
            "--require-review-ready",
            "local exit ",
            "strict exit ",
            "demo_gate_strict_exit_code",
            "scripts/collect-evidence-bundle.py --base-url",
            "Live proof gates",
            "live proof gates",
            "live_validation_gates",
            "live_validation_pass_count",
            "Operator action queue",
            "Operator runbook commands",
            "operator action source",
            "operator action",
            "operator runbook",
            "next shell command",
            "kvRow(dl5, \"next shell command\", shortText(operatorRunbook.next_shell_command, 120), \"warn\", operatorRunbook.next_shell_command)",
            "diag-cmd-meta",
            "function diagCommand(value, title, meta)",
            "if (meta) body.appendChild(el(\"span\", \"diag-cmd-meta\", meta));",
            "missing_live_artifact_action_items",
            "opStatus.action_items",
            "opStatus.operator_runbook",
            "action_items",
            "operator_runbook",
            "copyable_command_rows",
            "function runbookCommandRowsSig(runbook)",
            "runbookCommandRowsSig(runbook)",
            "row.category || \"\"",
            "row.severity || \"\"",
            "row.artifact || \"\"",
            "row.next_action || \"\"",
            "var runbookCommandRows = runbook.copyable_command_rows || (runbook.copyable_commands || []).map(function (cmd) { return { command: cmd }; })",
            "\"Copyable operator runbook command\",",
            "row.category,",
            "row.severity,",
            "row.artifact,",
            "row.next_action",
            "var commandMeta = [",
            "commandList.appendChild(diagCommand(row.command, commandTitle, commandMeta))",
            "command_hint",
            "command_kind",
            "copyable",
            "Missing live artifacts",
            "gap severity",
            "missing_live_artifact_rows",
            "missing_live_artifact_blocked_count",
            "missing_live_artifact_warn_count",
            "evidence_gaps",
            "gaps blocked",
            "vram_advisory_ready",
            "review_ready",
            "claim_blockers",
            "primary blocker",
            "readiness note",
            "simulator.readiness",
            "simulator endpoints",
            "simulator probe",
            "simulator probe timeout",
            "simulator readiness",
            "simulator readiness note",
            "simulator claim",
            "simulator claim mode",
            "simulator claim blocker",
            "simulator claim action",
            "simulator claim ready",
            "simulator claim blocked",
            "recovery_command",
            "simModeLabel",
            "function simSourceLabel(plan, simulator)",
            "source.toLowerCase().endsWith(\" \" + variant.toLowerCase())",
            "source.slice(0, source.length - variant.length).trim()",
            "simTrust",
            "prov-badge",
            "cached simulator",
            "live simulator",
            "invalid legacy fallback marker",
            "missing simulator provenance",
            "invalid fallback baselines",
            "simReadinessStatus",
            "simulator_endpoint_count",
            "simulator_probe_checked_count",
            "simulator_probe_ready_count",
            "simulator_probe_timeout_millis",
            "simulator_readiness_note",
            "configured_not_probed",
            "readiness_probe",
            "probe checked",
            "probe ready",
            "probe timeout",
        ]
    )


def valid_vram_calibration() -> dict:
    return {
        "available": True,
        "dataset": {
            "rows": 228,
            "time_series_samples": 4615,
            "schema": {
                "evidence_columns_present": 7,
                "evidence_columns_total": 7,
            },
            "reserve_pressure": {
                "definition": (
                    "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM "
                    "padding to stress scheduler headroom; this is a headroom stress-test "
                    "signal, not organic model demand."
                ),
                "pressure_rows": 37,
            },
            "synthetic_headroom": {
                "definition": (
                    "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM "
                    "padding to stress scheduler headroom; this is a headroom stress-test "
                    "signal, not organic model demand."
                ),
                "pressure_rows": 37,
            },
        },
        "model_drivers": {
            "available": True,
            "fit": "ridge_linear_interactions",
            "training_rows": 228,
            "quality": {"loo_p95_mib": 2627.8},
            "claim_boundary": (
                "Use real_top_drivers for model-memory claims. synthetic headroom drivers "
                "are stress-test probes only and must not be presented as organic workload predictors."
            ),
            "top_drivers": [
                {
                    "feature": "layers",
                    "label": "layer count",
                    "class": "model-size",
                    "mean_abs_contribution_mib": 2202.2,
                    "interpretation": "Architecture depth/width affects retained activation tensors.",
                },
                {
                    "feature": "param_x_precision",
                    "label": "parameter memory x precision",
                    "class": "precision",
                    "mean_abs_contribution_mib": 2043.3,
                    "interpretation": "Weights, gradients, and optimizer state scale with parameter count and numeric precision.",
                },
                {
                    "feature": "reserve_extra_gib",
                    "label": "synthetic reserve pressure",
                    "class": "synthetic-pressure",
                    "mean_abs_contribution_mib": 1953.7,
                    "interpretation": "Synthetic padding used to stress headroom and OOM risk; do not treat as organic model memory.",
                },
            ],
            "real_top_drivers": [
                {
                    "feature": "layers",
                    "label": "layer count",
                    "class": "model-size",
                    "mean_abs_contribution_mib": 2202.2,
                    "interpretation": "Architecture depth/width affects retained activation tensors.",
                },
                {
                    "feature": "param_x_precision",
                    "label": "parameter memory x precision",
                    "class": "precision",
                    "mean_abs_contribution_mib": 2043.3,
                    "interpretation": "Weights, gradients, and optimizer state scale with parameter count and numeric precision.",
                },
            ],
            "claim_safe_drivers": [
                {
                    "feature": "layers",
                    "label": "layer count",
                    "class": "model-size",
                    "mean_abs_contribution_mib": 2202.2,
                    "interpretation": "Architecture depth/width affects retained activation tensors.",
                },
                {
                    "feature": "param_x_precision",
                    "label": "parameter memory x precision",
                    "class": "precision",
                    "mean_abs_contribution_mib": 2043.3,
                    "interpretation": "Weights, gradients, and optimizer state scale with parameter count and numeric precision.",
                },
            ],
            "synthetic_pressure_drivers": [
                {
                    "feature": "reserve_extra_gib",
                    "label": "synthetic reserve pressure",
                    "class": "synthetic-pressure",
                    "mean_abs_contribution_mib": 1953.7,
                    "interpretation": "Synthetic padding used to stress headroom and OOM risk; do not treat as organic model memory.",
                },
            ],
        },
        "scheduler_readiness": {
            "ready_for_shadow_demo": True,
            "advisory_ready": True,
            "hard_admission_ready": False,
            "admission_decision": {
                "mode": "Shadow advisory only",
                "scheduler_use": "Score and warn; do not reject pods",
                "blocker_count": 1,
                "next_evidence_target": "true CUDA OOM labels",
                "can_hard_admit": False,
                "can_shadow_advise": True,
            },
            "hard_admission_blockers": [
                "no true bare-metal/cloud CUDA OOM labels",
            ],
            "evidence_collection_plan": [
                {"step": "run cloud OOM boundary probes"},
            ],
        },
    }


def valid_evidence_bundle() -> dict:
    return {
        "ok": True,
        "dry_run": True,
        "note": "read-only SRE evidence bundle scaffold",
        "collection_commands": [
            "curl -s http://127.0.0.1:8090/api/scheduler/traces > traces.json",
            "curl -s http://127.0.0.1:8090/api/scheduler/kube-simulator-plan > kube-simulator-plan.json",
            "curl -s http://127.0.0.1:8090/api/scheduler/repair-plan > repair-plan.json",
            "curl -s http://127.0.0.1:8090/api/scheduler/production-safety > production-safety.json",
            "curl -s http://127.0.0.1:8090/api/scheduler/demo-report > demo-report.json",
            "curl -s http://127.0.0.1:8090/api/scheduler/vram-calibration > vram-calibration.json",
            "curl -s http://127.0.0.1:8090/api/scheduler/operator-status > operator-status.json",
            "curl -s http://127.0.0.1:8090/api/scheduler/evidence-bundle > evidence-bundle.json",
        ],
        "evidence_bundle_rows": [
            {
                "artifact": "live pending GPU trace",
                "source": "/api/scheduler/traces",
                "pass_signal": "trace has pending GPU pods and solver placements",
                "operator_action": "capture the trace JSON",
                "blocks_claim": "current-cluster scheduling decision",
            }
        ],
        "missing_live_artifacts": ["latest shadow trace"],
        "missing_live_artifact_rows": [
            {
                "artifact": "latest shadow trace",
                "category": "live-trace",
                "severity": "blocked",
                "proof_gate": "pending GPU trace",
                "next_action": "apply a deterministic GPU scenario",
            }
        ],
        "live_validation_gates": [
            {
                "gate": "pending GPU trace",
                "status": "blocked",
                "live_endpoint": "/api/scheduler/traces",
                "next_action": "apply a deterministic GPU scenario",
            },
            {
                "gate": "kube baseline provenance",
                "status": "warn",
                "live_endpoint": "/api/scheduler/kube-simulator-plan",
                "next_action": "keep cached simulator provenance visible",
            },
            {
                "gate": "production mutation safety",
                "status": "pass",
                "live_endpoint": "/api/scheduler/production-safety",
                "next_action": "use safety posture as launch-gate evidence",
            },
        ],
        "launch_proof_gate": {
            "status": "incomplete",
            "customer_claim_ready": False,
        },
        "summary": {
            "collection_command_count": 8,
            "evidence_row_count": 1,
            "missing_live_artifact_count": 1,
            "launch_status": "incomplete",
            "customer_claim_ready": False,
            "mutation_allowed": False,
            "vram_advisory_ready": True,
            "vram_hard_admission_ready": False,
            "vram_admission_mode": "Shadow advisory only",
            "vram_scheduler_use": "Score and warn; do not reject pods",
            "vram_hard_blocker_count": 1,
            "vram_next_evidence_target": "true CUDA OOM labels",
            "vram_model_driver_count": 3,
            "vram_top_driver_labels": [
                "layer count",
                "parameter memory x precision",
                "synthetic reserve pressure",
            ],
            "vram_display_top_driver_labels": [
                "layer count",
                "parameter memory x precision",
                "synthetic VRAM headroom probe",
            ],
            "vram_claim_safe_driver_count": 2,
            "vram_claim_safe_driver_labels": [
                "layer count",
                "parameter memory x precision",
            ],
            "vram_display_claim_safe_driver_labels": [
                "layer count",
                "parameter memory x precision",
            ],
            "vram_real_model_driver_count": 2,
            "vram_real_top_driver_labels": [
                "layer count",
                "parameter memory x precision",
            ],
            "vram_display_real_top_driver_labels": [
                "layer count",
                "parameter memory x precision",
            ],
            "vram_synthetic_driver_count": 1,
            "vram_synthetic_driver_labels": ["synthetic reserve pressure"],
            "vram_display_synthetic_driver_labels": ["synthetic VRAM headroom probe"],
            "vram_synthetic_reserve_driver": True,
            "vram_synthetic_headroom_driver": True,
            "vram_reserve_pressure_definition": (
                "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM "
                "padding to stress scheduler headroom; this is a headroom stress-test "
                "signal, not organic model demand."
            ),
            "vram_synthetic_headroom_definition": (
                "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM "
                "padding to stress scheduler headroom; this is a headroom stress-test "
                "signal, not organic model demand."
            ),
            "vram_driver_claim_boundary": (
                "Use real_top_drivers for model-memory claims. synthetic headroom drivers "
                "are stress-test probes only and must not be presented as organic workload predictors."
            ),
            "production_readiness_blocker_class": "none",
            "production_readiness_last_error_class": "none",
            "simulator_endpoint_count": 2,
            "simulator_probe_checked_count": 2,
            "simulator_probe_ready_count": 1,
            "simulator_probe_timeout_millis": 2000,
            "simulator_readiness": "configured_not_probed",
            "simulator_readiness_note": (
                "endpoints are configured; export readiness is checked during live baseline calls"
            ),
            "simulator_claim_ready": False,
            "simulator_claim_mode": "partial-live-baseline",
            "simulator_claim_blocker": "only some kube-scheduler-simulator endpoints are ready",
            "simulator_claim_next_action": "use scripts/kss-pool.sh status and restart or replace unhealthy simulator workers before refreshing scenario baselines",
            "live_validation_gate_count": 3,
            "live_validation_pass_count": 1,
            "live_validation_warn_count": 1,
            "live_validation_blocked_count": 1,
            "review_ready": False,
            "demo_gate_status": "local-pass-strict-blocked",
            "demo_gate_local_exit_code": 0,
            "demo_gate_strict_exit_code": 2,
            "primary_claim_blocker": "customer claim not ready",
            "primary_claim_blocker_next_action": "resolve launch proof gaps before making customer-facing claims",
            "claim_blockers": ["customer claim not ready"],
        },
        "artifacts": {
            "production_safety": {
                "operator_claim": "read-only shadow mode",
                "readiness": {"blocker_class": "none"},
                "rollout": {"mutation_allowed": False},
                "simulator": {
                    "endpoint_count": 2,
                    "readiness_probe": {
                        "checked_count": 2,
                        "ready_count": 1,
                        "timeout_millis": 2000,
                    },
                    "readiness": "configured_not_probed",
                    "readiness_note": (
                        "endpoints are configured; export readiness is checked during live baseline calls"
                    ),
                },
            },
            "demo_report": {"ok": True},
            "vram_calibration": valid_vram_calibration(),
        },
    }


def valid_decision_readiness(
    *,
    status: str = "ready",
    customer_claim: str = "ready",
    production_binding: str = "read-only",
) -> dict:
    return {
        "status": status,
        "summary": (
            "demo=ready, claim="
            + customer_claim
            + ", vram-score=ready, hard-admit=blocked, bind="
            + production_binding
        ),
        "highest_risk": "no blocking operator decision risk detected"
        if status == "ready"
        else "production readiness blocked: kubernetes_watch",
        "next_action": "continue with customer review or canary production binding"
        if status == "ready"
        else "restore Kubernetes API connectivity",
        "capabilities": [
            {
                "name": "shadow_demo",
                "label": "Shadow demo",
                "status": "ready",
                "can_execute": True,
                "next_action": "demo gate is locally runnable",
            },
            {
                "name": "customer_claim",
                "label": "Customer claim",
                "status": customer_claim,
                "can_execute": customer_claim == "ready",
                "next_action": "customer claim packet is ready"
                if customer_claim == "ready"
                else "collect the missing live evidence before making customer claims",
            },
            {
                "name": "vram_scoring",
                "label": "VRAM scoring",
                "status": "ready",
                "can_execute": True,
                "next_action": "score and warn; do not hard-reject pods",
            },
            {
                "name": "hard_vram_admission",
                "label": "Hard VRAM admission",
                "status": "blocked",
                "can_execute": False,
                "next_action": "collect true CUDA OOM labels and cross-SKU validation first",
            },
            {
                "name": "production_binding",
                "label": "Production binding",
                "status": production_binding,
                "can_execute": production_binding == "ready",
                "next_action": "enable real binding only after ownership, RBAC, canary, reservation, and kill-switch gates are approved",
            },
        ],
    }


def valid_operator_status() -> dict:
    return {
        "ok": True,
        "dry_run": True,
        "status": "ready",
        "can_shadow_demo": True,
        "can_customer_claim": True,
        "decision_readiness": valid_decision_readiness(),
        "demo_gate": {"strict_exit_code": 0},
        "proof_gates": {"total": 0, "pass": 0, "warn": 0, "blocked": 0, "rows": []},
        "evidence_gaps": {
            "total": 0,
            "blocked": 0,
            "warn": 0,
            "category_counts": {},
            "category_rows": [],
            "rows": [],
        },
        "action_items": [],
        "operator_runbook": {
            "step_count": 0,
            "blocked_step_count": 0,
            "manual_step_count": 0,
            "copyable_command_count": 0,
            "next_shell_command": None,
            "copyable_commands": [],
        },
        "simulator": {
            "claim_ready": True,
            "claim_mode": "live-baseline-ready",
            "claim_blocker": None,
            "claim_next_action": "keep kube-scheduler-simulator baselines fresh before customer claims",
        },
        "scale_safety": {
            "available": True,
            "status": "regret-bounded",
            "regret_status": "full_feasible_set",
            "next_action": "no candidate-pruning regret action required for this trace",
            "pruning_active": False,
            "widened": False,
            "edge_reduction_milli": 0,
            "candidate_node_limit": 0,
            "retry_count": 0,
            "unpruned_candidate_edges": 12,
            "initial_candidate_edges": 12,
            "final_candidate_edges": 12,
            "candidate_pruned_workloads": 0,
        },
        "binding_safety": {
            "available": True,
            "status": "read-only",
            "next_action": "no binding mutation action required while shadow remains read-only",
            "mutation_allowed": False,
            "mode": "observe-only",
            "enable_real_binding": False,
            "real_binding_dry_run": False,
            "binding_kill_switch": False,
            "binding_canary_mode": "all",
            "binding_low_risk_max_gpus": 1,
            "max_binds_per_pass": 10,
            "binding_reservation_ttl_seconds": 60,
            "latest_trace_sequence": 1,
            "latest_outcome_count": 0,
            "bound": 0,
            "validated": 0,
            "skipped": 0,
            "failed": 0,
            "reservations": {},
            "reservation_pressure": "none",
            "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
            "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
            "reservation_pressure_reason": "no active binding reservations are holding GPU capacity",
            "reservation_pressure_next_action": "no reservation pressure action required",
            "skip_breakdown": {},
        },
        "vram": {
            "mode": "Shadow advisory only",
            "scheduler_use": "Score and warn; do not reject pods",
            "hard_blocker_count": 1,
            "hard_admission_blockers": ["no true bare-metal/cloud CUDA OOM labels"],
            "evidence_collection_plan": [
                {
                    "target": "true CUDA OOM labels",
                    "unblocks": "hard VRAM admission",
                    "commands": ["python3 vram-model-lab/run_matrix.py --record-oom"],
                }
            ],
            "next_evidence_target": "true CUDA OOM labels",
            "model_driver_count": 1,
            "top_driver_labels": ["synthetic reserve pressure"],
            "display_top_driver_labels": ["synthetic VRAM headroom probe"],
            "claim_safe_driver_count": 0,
            "claim_safe_driver_labels": [],
            "display_claim_safe_driver_labels": [],
            "real_model_driver_count": 0,
            "real_top_driver_labels": [],
            "display_real_top_driver_labels": [],
            "synthetic_driver_count": 1,
            "synthetic_driver_labels": ["synthetic reserve pressure"],
            "display_synthetic_driver_labels": ["synthetic VRAM headroom probe"],
            "synthetic_reserve_driver": True,
            "synthetic_headroom_driver": True,
            "reserve_pressure_definition": "reserve_extra_mib padding",
            "synthetic_headroom_definition": "reserve_extra_mib padding",
        },
        "evidence": {"path": "/api/scheduler/evidence-bundle"},
    }


class ShadowSmokeValidationTests(unittest.TestCase):
    def test_cache_requires_complete_coverage_by_default(self) -> None:
        with self.assertRaisesRegex(AssertionError, "simulator cache is incomplete"):
            shadow_smoke.validate_cache_coverage(
                {
                    "ok": True,
                    "simulator_cache_total_baselines": 4,
                    "simulator_cache_cached_baselines": 3,
                    "simulator_cache_missing_baselines": 1,
                },
                label="test",
                allow_incomplete_cache=False,
            )

    def test_cache_can_allow_partial_coverage_for_development(self) -> None:
        self.assertEqual(
            shadow_smoke.validate_cache_coverage(
                {
                    "ok": True,
                    "simulator_cache_total_baselines": 4,
                    "simulator_cache_cached_baselines": 3,
                    "simulator_cache_missing_baselines": 1,
                },
                label="test",
                allow_incomplete_cache=True,
            ),
            (4, 3, 1),
        )

    def test_demo_report_requires_vram_gang_and_repair_pages(self) -> None:
        report = valid_report()
        report["report"]["scenario_pages"] = [
            {"slug": "vram-binpacking", "title": "VRAM usage prediction & binpacking"},
            {"slug": "gang-scheduling", "title": "Gang scheduling"},
        ]
        with self.assertRaisesRegex(AssertionError, "preemption/migration"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_requires_at_least_one_ksolver_win(self) -> None:
        report = valid_report()
        scenario = winning_scenario()
        scenario["ksolver"]["metrics"]["useful_gpu"] = 1
        scenario["kube"]["metrics"]["useful_gpu"] = 2
        scenario["kube_binpack"]["metrics"]["useful_gpu"] = 4
        report["report"]["scenarios"] = [scenario]
        with self.assertRaisesRegex(AssertionError, "no scenario where ksolver beats"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_requires_kube_simulator_provenance_per_scenario(self) -> None:
        report = valid_report()
        del report["report"]["scenarios"][0]["kube"]["simulator"]
        with self.assertRaisesRegex(AssertionError, "missing simulator provenance"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_blocks_fallback_simulator_provenance_per_scenario(self) -> None:
        report = valid_report()
        report["report"]["scenarios"][0]["kube_binpack"]["simulator"]["mode"] = "deterministic-fallback"
        with self.assertRaisesRegex(AssertionError, "invalid fallback simulator provenance"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_requires_demo_readiness_live_validation_rows(self) -> None:
        report = valid_report()
        report["report"]["demo_readiness_summary"]["live_validation_rows"] = []
        with self.assertRaisesRegex(AssertionError, "no live validation rows"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_requires_every_live_validation_row_to_have_endpoint(self) -> None:
        report = valid_report()
        report["report"]["demo_readiness_summary"]["live_validation_rows"][1]["live_endpoint"] = ""
        with self.assertRaisesRegex(AssertionError, "row 1 missing endpoint"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_requires_vram_investment_claim_boundary(self) -> None:
        report = valid_report()
        report["report"]["vram_investment_demo_summary"]["synthetic_prediction_notice"] = "looks accurate"
        with self.assertRaisesRegex(AssertionError, "fake-predictor claim boundary"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_requires_vram_investment_risk_reduction(self) -> None:
        report = valid_report()
        summary = report["report"]["vram_investment_demo_summary"]
        summary["ksolver_cuda_oom_risk_pods"] = summary["baseline_cuda_oom_risk_pods"]
        summary["cuda_oom_risk_reduction_pods"] = 0
        with self.assertRaisesRegex(AssertionError, "does not reduce likely CUDA OOM risk"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_requires_vram_investment_real_evidence(self) -> None:
        report = valid_report()
        report["report"]["vram_investment_demo_summary"]["required_real_predictor_evidence"] = [
            "some spreadsheet"
        ]
        with self.assertRaisesRegex(AssertionError, "missing required real predictor evidence"):
            shadow_smoke.validate_demo_report_payload(report, min_scenarios=1)

    def test_demo_report_returns_scenario_and_win_counts(self) -> None:
        self.assertEqual(
            shadow_smoke.validate_demo_report_payload(valid_report(), min_scenarios=1),
            (
                1,
                1,
                2,
                "pending GPU trace",
                "/api/scheduler/traces",
                {
                    "rows": 6,
                    "baseline_cuda_oom_risk_pods": 4,
                    "ksolver_cuda_oom_risk_pods": 0,
                    "cuda_oom_risk_reduction_pods": 4,
                    "high_vram_nodes_preserved": 1,
                    "unknown_or_advisory_rows": 1,
                    "average_baseline_oom_risk_percent": 64,
                    "average_ksolver_oom_risk_percent": 24,
                },
            ),
        )

    def test_dashboard_html_blocks_debug_summary_text(self) -> None:
        with self.assertRaisesRegex(AssertionError, "leaked debug summary text"):
            shadow_smoke.validate_dashboard_html(valid_html() + "\nfilling missing baselines")

    def test_dashboard_html_accepts_current_markup_contract(self) -> None:
        shadow_smoke.validate_dashboard_html(valid_html())

    def test_dashboard_javascript_accepts_valid_inline_script(self) -> None:
        shadow_smoke.validate_dashboard_javascript("<script>function ok(){ return 1; }</script>")

    def test_dashboard_javascript_rejects_invalid_inline_script(self) -> None:
        with self.assertRaisesRegex(AssertionError, "invalid JavaScript"):
            shadow_smoke.validate_dashboard_javascript("<script>function broken(){</script>")

    def test_dashboard_javascript_rejects_missing_required_helper(self) -> None:
        html = """
        <script>
        function getJSON(url) { return Promise.resolve(url); }
        function renderOperatorBanner() {}
        function renderApiErrorBanner() {}
        function operatorStatusSig() { return "/api/scheduler/operator-status"; }
        function diagSig() { return "/api/scheduler/traces"; }
        function poll() { return fmt(1); }
        </script>
        """
        with self.assertRaisesRegex(AssertionError, "missing required helper function fmt"):
            shadow_smoke.validate_dashboard_javascript(html)

    def test_dashboard_javascript_accepts_production_asset(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        shadow_smoke.validate_dashboard_javascript(html)

    def test_dashboard_run_cache_key_includes_pricing_assumption(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        self.assertIn(
            'function runKey(c) { return configKey(c) + "|price|" + priceKey(currentPriceMeta()); }',
            html,
        )
        self.assertIn("var cfg = readConfig(), key = runKey(cfg);", html)
        self.assertIn("var existing = runs.filter(function (r) { return r.key === key; })[0];", html)
        self.assertNotIn("var cfg = readConfig(), key = configKey(cfg);", html)
        self.assertIn("price: currentPriceMeta()", html)
        self.assertIn(
            "objective, weights, and pricing assumption match an existing run",
            html,
        )
        self.assertIn(
            "objective, weights, and pricing assumption are identical",
            html,
        )
        self.assertIn(
            "Delta uses the run's GPU-hour proxy assumption",
            html,
        )
        self.assertIn(
            "relative placement comparison only, not a cloud bill",
            html,
        )
        self.assertNotIn("because the objective and weights match an existing run", html)

    def test_dashboard_run_cards_preserve_kube_baseline_failure_reason(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        self.assertIn(
            '"No kube baseline captured for this run · " + (r.kubeProv || "reason unavailable")',
            html,
        )
        self.assertNotIn('"No kube baseline captured for this run."', html)
        self.assertIn(
            '"Δ vs kube-scheduler-simulator baseline · " + (r.kubeProv || "provenance unavailable")',
            html,
        )

    def test_dashboard_scenario_chips_use_namespace_identity(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        self.assertIn(
            'ch.textContent = itemNsName(pl) + " " + itemGpus(pl) + "g"',
            html,
        )
        self.assertIn("var node = placedNode(pl);", html)
        self.assertIn('node ? " → " + shortName(node) : " ×"', html)
        self.assertNotIn('ch.textContent = (pl.pod || "pod") + " " + (pl.gpus || 0) + "g"', html)
        self.assertNotIn('ch.textContent = itemNsName(pl) + " " + (pl.gpus || 0) + "g"', html)
        self.assertNotIn('pl.node ? " → " + shortName(pl.node) : " ×"', html)
        self.assertIn("function itemNsName(item)", html)
        self.assertIn("function itemGpus(item)", html)

    def test_dashboard_scenario_cost_deltas_explain_fixed_fleet_proxy(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        self.assertIn('label === "active-node cost/mo"', html)
        self.assertIn("Active-node cost is a fixed-fleet proxy", html)
        self.assertIn("no autoscaler, idle nodes priced at zero, not a cloud bill", html)
        self.assertIn("if (costProxyTitle) cell.title = costProxyTitle", html)
        self.assertIn("if (costProxyTitle) dv.title = costProxyTitle", html)
        self.assertIn("if (costProxyTitle) sub.title = costProxyTitle", html)

    def test_dashboard_stale_baseline_banner_has_kss_recovery_action(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        self.assertIn("Baseline refresh failed", html)
        self.assertIn("function simulatorRecoveryCommand(safety, refresh)", html)
        self.assertIn("function simulatorRecoverySource(safety, refresh)", html)
        self.assertIn("var simulator = (safety && safety.simulator) || {}", html)
        self.assertIn(
            "return (refresh && refresh.simulator_recovery_command)",
            html,
        )
        self.assertIn('return "refresh status"', html)
        self.assertIn('return "operator status"', html)
        self.assertIn('return "local default"', html)
        self.assertIn('|| simulator.recovery_command', html)
        self.assertIn("var recoveryCommand = simulatorRecoveryCommand(lastSafety, refresh)", html)
        self.assertIn('|simrec:" + (simulator.recovery_command || "")', html)
        self.assertIn("demoRefresh.simulator_recovery_command || \"\"", html)
        self.assertIn(
            "simulator.recovery_command || \"\", demoRefresh.simulator_recovery_command || \"\"",
            html,
        )
        self.assertIn(
            '"Next action: run " + recoveryCommand + " before refreshing baselines again."',
            html,
        )
        self.assertIn(
            'diagCommand(simulatorRecovery, "Copy kube-scheduler-simulator recovery command")',
            html,
        )
        self.assertIn("KSS recovery command source: ", html)
        self.assertIn("Use this before refreshing scenario baselines or making kube-vs-ksolver claims.", html)
        self.assertIn("demoRefresh.simulator_recovery_command || \"\"", html)
        self.assertIn("recCopy.title = recoveryCommand", html)
        self.assertIn(
            'recCopy.addEventListener("click", function () { copyDiagCommand(recoveryCommand, recCopy); })',
            html,
        )
        self.assertIn("Full simulator error", html)

    def test_dashboard_copy_command_buttons_are_non_submit_controls(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        self.assertIn('var actionCopy = el("button", "copy-btn", "Copy command")', html)
        self.assertIn('actionCopy.type = "button"', html)
        self.assertIn("actionCopy.title = row.command_hint", html)
        self.assertIn('var planCopy = el("button", "copy-btn", "Copy command")', html)
        self.assertIn('planCopy.type = "button"', html)
        self.assertIn("planCopy.title = commands[0]", html)
        self.assertIn('var cmdText = el("span", "endpoint", cmd)', html)
        self.assertIn("cmdText.title = cmd", html)
        self.assertIn("copyBtn.title = cmd", html)
        self.assertIn('var x = el("button", "run-x", "×"); x.type = "button"; x.title = "remove run"; x.setAttribute("aria-label", "Remove run " + configLabel(r.cfg));', html)
        self.assertIn('allBtn.type = "button"', html)
        self.assertIn('btn.type = "button"', html)
        self.assertIn('<button class="tab" id="tab-runs" type="button"', html)
        self.assertIn('<button class="tab" id="tab-live" type="button"', html)
        self.assertIn('<button class="tab" id="tab-scen" type="button"', html)
        self.assertIn('<button class="tab" id="tab-diag" type="button"', html)

    def test_dashboard_reserve_pressure_banner_explains_state_and_counts(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        self.assertIn('function fmtUnit(n, singular, plural)', html)
        self.assertIn("function reservePressureStateMeaning(pressure)", html)
        self.assertIn("function reservePressureCountSuffix(binding)", html)
        self.assertIn("reservePressureStateMeaning(pressure)", html)
        self.assertIn('fmtUnit(binding.reservations.active_entries || 0, "entry", "entries")', html)
        self.assertIn('fmtUnit(binding.reservations.reserved_gpus || 0, "GPU")', html)
        self.assertIn('" · " + fmtUnit(reserved, "GPU")', html)
        self.assertIn('" · " + fmtUnit(active, "reservation")', html)
        self.assertIn('"binding reservation pressure " + pressure + reservePressureCountSuffix(binding)', html)
        self.assertIn("function kvRow(dl, k, v, cls, title)", html)
        self.assertIn("if (title) dd.title = title;", html)
        self.assertIn("var pressureTitle = [", html)
        self.assertIn("reservePressureScopeNote(binding)", html)
        self.assertIn('kvRow(dlBinding, "binding reservation pressure"', html)
        self.assertIn('kvRow(dlBinding, "binding reservation pressure", binding.reservation_pressure, pressureClass, pressureTitle)', html)
        self.assertIn('kvRow(dlBinding, "state meaning", pressureStateMeaning, pressureClass, pressureTitle)', html)
        self.assertIn('kvRow(dlBinding, "reservation pressure reason"', html)
        self.assertIn('kvRow(dlBinding, "reservation pressure action"', html)
        self.assertIn('binding.reservation_pressure_reason);', html)
        self.assertIn('binding.reservation_pressure_next_action);', html)
        self.assertIn("((binding.reservations && binding.reservations.active_entries) || 0)", html)
        self.assertIn("((binding.reservations && binding.reservations.reserved_gpus) || 0)", html)
        self.assertNotIn('" · " + fmt(reserved) + " GPU"', html)
        self.assertNotIn('" · " + fmt(active) + " reservations"', html)
        self.assertNotIn('label: "reserve pressure " + pressure,', html)
        self.assertNotIn('kvRow(dlBinding, "scheduler reserve pressure"', html)

    def test_dashboard_dom_id_contract_rejects_missing_referenced_id(self) -> None:
        html = '<div id="present"></div><script>$("present"); $("missing");</script>'
        with self.assertRaisesRegex(AssertionError, "missing DOM id\\(s\\): missing"):
            shadow_smoke.validate_dashboard_dom_id_contract(html)

    def test_dashboard_dom_id_contract_rejects_duplicate_id(self) -> None:
        html = '<div id="dup"></div><section id="dup"></section><script>$("dup");</script>'
        with self.assertRaisesRegex(AssertionError, "duplicate DOM id\\(s\\): dup"):
            shadow_smoke.validate_dashboard_dom_id_contract(html)

    def test_dashboard_dom_id_contract_accepts_production_asset(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        shadow_smoke.validate_dashboard_dom_id_contract(html)

    def test_dashboard_tab_panel_contract_rejects_missing_panel(self) -> None:
        html = """
        <nav>
          <button class="tab" id="tab-one" data-panel="panel-one" aria-controls="panel-one" tabindex="-1" aria-selected="false"></button>
          <button class="tab" id="tab-two" data-panel="panel-two" aria-controls="panel-two" tabindex="0" aria-selected="true"></button>
        </nav>
        <main>
          <section class="panel active" role="tabpanel" id="panel-two" aria-labelledby="tab-two"></section>
        </main>
        """
        with self.assertRaisesRegex(AssertionError, "missing panel id\\(s\\): panel-one"):
            shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_dashboard_tab_panel_contract_rejects_missing_tab_label(self) -> None:
        html = """
        <nav>
          <button class="tab" id="tab-one" data-panel="panel-one" aria-controls="panel-one" tabindex="0" aria-selected="true"></button>
        </nav>
        <main>
          <section class="panel active" role="tabpanel" id="panel-one" aria-labelledby="tab-missing"></section>
        </main>
        """
        with self.assertRaisesRegex(AssertionError, "missing tab id\\(s\\): tab-missing"):
            shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_dashboard_tab_panel_contract_rejects_selected_active_mismatch(self) -> None:
        html = """
        <nav>
          <button class="tab" id="tab-one" data-panel="panel-one" aria-controls="panel-one" tabindex="0" aria-selected="true"></button>
          <button class="tab" id="tab-two" data-panel="panel-two" aria-controls="panel-two" tabindex="-1" aria-selected="false"></button>
        </nav>
        <main>
          <section class="panel" role="tabpanel" id="panel-one" aria-labelledby="tab-one" hidden></section>
          <section class="panel active" role="tabpanel" id="panel-two" aria-labelledby="tab-two"></section>
        </main>
        """
        with self.assertRaisesRegex(AssertionError, "selected tab targets panel-one, but active panel is panel-two"):
            shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_dashboard_tab_panel_contract_rejects_multiple_active_panels(self) -> None:
        html = """
        <nav>
          <button class="tab" id="tab-one" data-panel="panel-one" aria-controls="panel-one" tabindex="0" aria-selected="true"></button>
          <button class="tab" id="tab-two" data-panel="panel-two" aria-controls="panel-two" tabindex="-1" aria-selected="false"></button>
        </nav>
        <main>
          <section class="panel active" role="tabpanel" id="panel-one" aria-labelledby="tab-one"></section>
          <section class="panel active" role="tabpanel" id="panel-two" aria-labelledby="tab-two"></section>
        </main>
        """
        with self.assertRaisesRegex(AssertionError, "exactly one active panel, found 2"):
            shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_dashboard_tab_panel_contract_rejects_aria_controls_mismatch(self) -> None:
        html = """
        <nav>
          <button class="tab" id="tab-one" data-panel="panel-one" aria-controls="panel-other" tabindex="0" aria-selected="true"></button>
        </nav>
        <main>
          <section class="panel active" role="tabpanel" id="panel-one" aria-labelledby="tab-one"></section>
        </main>
        """
        with self.assertRaisesRegex(AssertionError, "aria-controls must match data-panel"):
            shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_dashboard_tab_panel_contract_rejects_bad_tabindex(self) -> None:
        html = """
        <nav>
          <button class="tab" id="tab-one" data-panel="panel-one" aria-controls="panel-one" tabindex="0" aria-selected="true"></button>
          <button class="tab" id="tab-two" data-panel="panel-two" aria-controls="panel-two" tabindex="0" aria-selected="false"></button>
        </nav>
        <main>
          <section class="panel active" role="tabpanel" id="panel-one" aria-labelledby="tab-one"></section>
          <section class="panel" role="tabpanel" id="panel-two" aria-labelledby="tab-two" hidden></section>
        </main>
        """
        with self.assertRaisesRegex(AssertionError, "tab tabindex must be 0 only for the selected tab"):
            shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_dashboard_tab_panel_contract_rejects_hidden_state_mismatch(self) -> None:
        html = """
        <nav>
          <button class="tab" id="tab-one" data-panel="panel-one" aria-controls="panel-one" tabindex="0" aria-selected="true"></button>
          <button class="tab" id="tab-two" data-panel="panel-two" aria-controls="panel-two" tabindex="-1" aria-selected="false"></button>
        </nav>
        <main>
          <section class="panel active" role="tabpanel" id="panel-one" aria-labelledby="tab-one" hidden></section>
          <section class="panel" role="tabpanel" id="panel-two" aria-labelledby="tab-two" hidden></section>
        </main>
        """
        with self.assertRaisesRegex(AssertionError, "hidden state must match active class"):
            shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_dashboard_tab_panel_contract_accepts_production_asset(self) -> None:
        html = (ROOT.parent / "ksolver" / "static" / "shadow.html").read_text(encoding="utf-8")
        shadow_smoke.validate_dashboard_tab_panel_contract(html)

    def test_vram_calibration_accepts_advisory_ready_dataset(self) -> None:
        self.assertEqual(
            shadow_smoke.validate_vram_calibration_payload(valid_vram_calibration()),
            (228, 4615, 37, 7, 7, False, 3, True),
        )

    def test_vram_calibration_requires_complete_evidence_columns(self) -> None:
        payload = valid_vram_calibration()
        payload["dataset"]["schema"]["evidence_columns_present"] = 6
        with self.assertRaisesRegex(AssertionError, "evidence columns are incomplete"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_vram_calibration_requires_blockers_when_hard_admission_is_false(self) -> None:
        payload = valid_vram_calibration()
        payload["scheduler_readiness"]["hard_admission_blockers"] = []
        with self.assertRaisesRegex(AssertionError, "false without blockers"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_vram_calibration_requires_matching_admission_decision(self) -> None:
        payload = valid_vram_calibration()
        payload["scheduler_readiness"]["admission_decision"]["blocker_count"] = 0
        with self.assertRaisesRegex(AssertionError, "blocker count does not match"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_vram_calibration_requires_synthetic_padding_caveat(self) -> None:
        payload = valid_vram_calibration()
        payload["dataset"]["synthetic_headroom"]["definition"] = "reserve_extra_mib padding"
        payload["dataset"]["reserve_pressure"]["definition"] = "reserve_extra_mib padding"
        with self.assertRaisesRegex(AssertionError, "synthetic headroom definition missing organic-demand caveat"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_vram_calibration_requires_synthetic_headroom_block(self) -> None:
        payload = valid_vram_calibration()
        del payload["dataset"]["synthetic_headroom"]
        with self.assertRaisesRegex(AssertionError, "missing synthetic_headroom block"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_vram_calibration_requires_reserve_pressure_compatibility_block(self) -> None:
        payload = valid_vram_calibration()
        del payload["dataset"]["reserve_pressure"]
        with self.assertRaisesRegex(AssertionError, "missing reserve_pressure compatibility block"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_vram_calibration_requires_headroom_compatibility_mirror(self) -> None:
        payload = valid_vram_calibration()
        payload["dataset"]["reserve_pressure"]["pressure_rows"] = 12
        with self.assertRaisesRegex(AssertionError, "synthetic_headroom/reserve_pressure mismatch"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_vram_calibration_requires_model_driver_explanation(self) -> None:
        payload = valid_vram_calibration()
        payload["model_drivers"]["top_drivers"][2]["class"] = "model-size"
        with self.assertRaisesRegex(AssertionError, "model drivers missing synthetic headroom caveat"):
            shadow_smoke.validate_vram_calibration_payload(payload)

    def test_evidence_bundle_accepts_read_only_collection_packet(self) -> None:
        self.assertEqual(
            shadow_smoke.validate_evidence_bundle_payload(valid_evidence_bundle()),
            (
                8,
                1,
                "incomplete",
                False,
                "none",
                False,
                "partial-live-baseline",
                "only some kube-scheduler-simulator endpoints are ready",
                "use scripts/kss-pool.sh status and restart or replace unhealthy simulator workers before refreshing scenario baselines",
            ),
        )

    def test_operator_status_accepts_blocked_status_with_action(self) -> None:
        self.assertEqual(
            shadow_smoke.validate_operator_status_payload(
                {
                    "ok": True,
                    "dry_run": True,
                    "status": "blocked",
                    "can_shadow_demo": True,
                    "can_customer_claim": False,
                    "decision_readiness": valid_decision_readiness(
                        status="needs-action",
                        customer_claim="blocked",
                        production_binding="blocked",
                    ),
                    "primary_blocker": "production readiness blocked: kubernetes_watch",
                    "next_action": "restore Kubernetes API connectivity",
                    "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                    "demo_gate": {"strict_exit_code": 2},
                    "proof_gates": {
                        "total": 3,
                        "pass": 1,
                        "warn": 1,
                        "blocked": 1,
                        "rows": [
                            {"gate": "pending GPU trace", "status": "blocked"},
                            {"gate": "kube baseline provenance", "status": "warn"},
                            {"gate": "production mutation safety", "status": "pass"},
                        ],
                    },
                    "evidence_gaps": {
                        "total": 2,
                        "blocked": 1,
                        "warn": 1,
                        "category_counts": {"customer-proof": 1, "live-trace": 1},
                        "category_rows": [
                            {
                                "category": "live-trace",
                                "total": 1,
                                "blocked": 1,
                                "warn": 0,
                                "severity": "blocked",
                                "artifact": "latest shadow trace",
                                "proof_gate": None,
                                "next_action": None,
                            },
                            {
                                "category": "customer-proof",
                                "total": 1,
                                "blocked": 0,
                                "warn": 1,
                                "severity": "warn",
                                "artifact": "customer pricing source",
                                "proof_gate": None,
                                "next_action": None,
                            },
                        ],
                        "rows": [
                            {
                                "artifact": "latest shadow trace",
                                "category": "live-trace",
                                "severity": "blocked",
                            },
                            {
                                "artifact": "customer pricing source",
                                "category": "customer-proof",
                                "severity": "warn",
                            },
                        ],
                    },
                    "action_items": [
                        {
                            "priority": 1,
                            "category": "live-trace",
                            "severity": "blocked",
                            "blocked": 1,
                            "warn": 0,
                            "artifact": "latest shadow trace",
                            "next_action": "apply a deterministic GPU scenario",
                            "command_hint": "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                            "command_kind": "shell",
                            "copyable": True,
                        },
                        {
                            "priority": 2,
                            "category": "customer-proof",
                            "severity": "warn",
                            "blocked": 0,
                            "warn": 1,
                            "artifact": "customer pricing source",
                            "next_action": "attach pricing catalog",
                            "command_hint": "attach pricing catalog, chargeback export, contract rate sheet, or invoice sample",
                            "command_kind": "manual",
                            "copyable": False,
                        },
                    ],
                    "operator_runbook": {
                        "step_count": 2,
                        "blocked_step_count": 1,
                        "manual_step_count": 1,
                        "copyable_command_count": 1,
                        "next_shell_command": "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                        "copyable_commands": ["kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending"],
                        "copyable_command_rows": [
                            {
                                "command": "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                                "priority": 1,
                                "category": "live-trace",
                                "severity": "blocked",
                                "artifact": "latest shadow trace",
                                "next_action": "apply a deterministic GPU scenario",
                                "command_kind": "shell",
                            }
                        ],
                    },
                    "simulator": {
                        "claim_ready": False,
                        "claim_mode": "partial-live-baseline",
                        "claim_blocker": "only some kube-scheduler-simulator endpoints are ready",
                        "claim_next_action": "use scripts/kss-pool.sh status before refreshing scenario baselines",
                        "recovery_command": "scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache",
                    },
                    "scale_safety": {
                        "available": True,
                        "status": "regret-unknown",
                        "regret_status": "pruned_regret_unknown",
                        "next_action": "rerun or compare with candidate_node_limit=0 before claiming pruning has no scheduling regret",
                        "pruning_active": True,
                        "widened": False,
                        "edge_reduction_milli": 75000,
                        "candidate_node_limit": 8,
                        "retry_count": 0,
                        "unpruned_candidate_edges": 400,
                        "initial_candidate_edges": 100,
                        "final_candidate_edges": 100,
                        "candidate_pruned_workloads": 12,
                    },
                    "binding_safety": {
                        "available": True,
                        "status": "dry-run-validation",
                        "next_action": "review validated dry-run binding outcomes before switching to non-dry-run mutation",
                        "mutation_allowed": True,
                        "mode": "dry-run",
                        "enable_real_binding": True,
                        "real_binding_dry_run": True,
                        "binding_kill_switch": False,
                        "binding_canary_mode": "all",
                        "binding_low_risk_max_gpus": 1,
                        "max_binds_per_pass": 10,
                        "binding_reservation_ttl_seconds": 60,
                        "latest_trace_sequence": 42,
                        "latest_outcome_count": 2,
                        "bound": 0,
                        "validated": 2,
                        "skipped": 0,
                        "failed": 0,
                        "reservations": {"active_entries": 1, "reserved_gpus": 4},
                        "reservation_pressure": "active",
                        "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                        "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                        "reservation_pressure_reason": "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
                        "reservation_pressure_next_action": "verify reservations are fresh and within TTL before binding the reserved placements",
                        "skip_breakdown": {},
                    },
                    "vram": {
                        "mode": "Shadow advisory only",
                        "scheduler_use": "Score and warn; do not reject pods",
                        "hard_blocker_count": 1,
                        "hard_admission_blockers": ["no true bare-metal/cloud CUDA OOM labels"],
                        "evidence_collection_plan": [
                            {
                                "target": "true CUDA OOM labels",
                                "unblocks": "hard VRAM admission",
                                "commands": ["python3 vram-model-lab/run_matrix.py --record-oom"],
                            }
                        ],
                        "next_evidence_target": "true CUDA OOM labels",
                        "model_driver_count": 3,
                        "top_driver_labels": [
                            "layer count",
                            "parameter memory x precision",
                            "synthetic reserve pressure",
                        ],
                        "display_top_driver_labels": [
                            "layer count",
                            "parameter memory x precision",
                            "synthetic VRAM headroom probe",
                        ],
                        "claim_safe_driver_count": 2,
                        "claim_safe_driver_labels": [
                            "layer count",
                            "parameter memory x precision",
                        ],
                        "display_claim_safe_driver_labels": [
                            "layer count",
                            "parameter memory x precision",
                        ],
                        "real_model_driver_count": 2,
                        "real_top_driver_labels": [
                            "layer count",
                            "parameter memory x precision",
                        ],
                        "display_real_top_driver_labels": [
                            "layer count",
                            "parameter memory x precision",
                        ],
                        "synthetic_driver_count": 1,
                        "synthetic_driver_labels": ["synthetic reserve pressure"],
                        "display_synthetic_driver_labels": ["synthetic VRAM headroom probe"],
                        "synthetic_reserve_driver": True,
                        "synthetic_headroom_driver": True,
                        "reserve_pressure_definition": (
                            "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM "
                            "padding to stress scheduler headroom; this is a headroom stress-test "
                            "signal, not organic model demand."
                        ),
                        "synthetic_headroom_definition": (
                            "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM "
                            "padding to stress scheduler headroom; this is a headroom stress-test "
                            "signal, not organic model demand."
                        ),
                    },
                    "evidence": {"path": "/api/scheduler/evidence-bundle"},
                }
            ),
            (
                "blocked",
                "production readiness blocked: kubernetes_watch",
                "restore Kubernetes API connectivity",
            ),
        )

    def test_operator_status_accepts_simulator_recovery_as_first_action(self) -> None:
        payload = valid_operator_status()
        payload["status"] = "blocked"
        payload["can_customer_claim"] = False
        payload["primary_blocker"] = "customer claim not ready"
        payload["next_action"] = "repair kube-scheduler-simulator before customer claims"
        payload["debug_commands"] = ["scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache"]
        payload["simulator"] = {
            "claim_ready": False,
            "claim_mode": "baseline-proof-blocked",
            "claim_blocker": "no kube-scheduler-simulator endpoint answered /api/v1/export",
            "claim_next_action": "start or repair the kube-scheduler-simulator pool before making kube-vs-ksolver placement claims",
            "recovery_command": "scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache",
        }
        payload["evidence_gaps"] = {
            "total": 1,
            "blocked": 1,
            "warn": 0,
            "category_counts": {"live-trace": 1},
            "category_rows": [
                {
                    "category": "live-trace",
                    "total": 1,
                    "blocked": 1,
                    "warn": 0,
                    "severity": "blocked",
                    "artifact": "latest shadow trace",
                    "proof_gate": None,
                    "next_action": "apply a deterministic GPU scenario",
                }
            ],
            "rows": [
                {
                    "artifact": "latest shadow trace",
                    "category": "live-trace",
                    "severity": "blocked",
                    "next_action": "apply a deterministic GPU scenario",
                }
            ],
        }
        payload["action_items"] = [
            {
                "priority": 1,
                "category": "simulator-baseline",
                "severity": "blocked",
                "blocked": 1,
                "warn": 0,
                "artifact": "kube-scheduler-simulator claim proof",
                "next_action": "start or repair the kube-scheduler-simulator pool before making kube-vs-ksolver placement claims",
                "command_hint": "scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache",
                "command_hints": ["scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache"],
                "command_kind": "shell",
                "copyable": True,
            },
            {
                "priority": 2,
                "category": "live-trace",
                "severity": "blocked",
                "blocked": 1,
                "warn": 0,
                "artifact": "latest shadow trace",
                "next_action": "apply a deterministic GPU scenario",
                "command_hint": "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                "command_hints": ["kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending"],
                "command_kind": "shell",
                "copyable": True,
            },
        ]
        payload["operator_runbook"] = {
            "step_count": 2,
            "blocked_step_count": 2,
            "manual_step_count": 0,
            "copyable_command_count": 2,
            "next_shell_command": "scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache",
            "copyable_commands": [
                "scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache",
                "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
            ],
            "copyable_command_rows": [
                {
                    "command": "scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache",
                    "priority": 1,
                    "category": "simulator-baseline",
                    "severity": "blocked",
                    "artifact": "kube-scheduler-simulator claim proof",
                    "next_action": "start or repair the kube-scheduler-simulator pool before making kube-vs-ksolver placement claims",
                    "command_kind": "shell",
                },
                {
                    "command": "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                    "priority": 2,
                    "category": "live-trace",
                    "severity": "blocked",
                    "artifact": "latest shadow trace",
                    "next_action": "apply a deterministic GPU scenario",
                    "command_kind": "shell",
                },
            ],
        }

        self.assertEqual(
            shadow_smoke.validate_operator_status_payload(payload),
            (
                "blocked",
                "customer claim not ready",
                "repair kube-scheduler-simulator before customer claims",
            ),
        )

    def test_operator_status_requires_copyable_command_provenance_rows(self) -> None:
        payload = valid_operator_status()
        payload.update(
            {
                "status": "blocked",
                "can_customer_claim": False,
                "decision_readiness": valid_decision_readiness(
                    status="needs-action",
                    customer_claim="blocked",
                    production_binding="blocked",
                ),
                "primary_blocker": "customer claim not ready",
                "next_action": "apply a deterministic GPU scenario",
                "debug_commands": [
                    "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending"
                ],
                "demo_gate": {"strict_exit_code": 2},
                "evidence_gaps": {
                    "total": 1,
                    "blocked": 1,
                    "warn": 0,
                    "category_counts": {"live-trace": 1},
                    "category_rows": [
                        {
                            "category": "live-trace",
                            "total": 1,
                            "blocked": 1,
                            "warn": 0,
                            "severity": "blocked",
                            "artifact": "latest shadow trace",
                            "proof_gate": None,
                            "next_action": "apply a deterministic GPU scenario",
                        }
                    ],
                    "rows": [
                        {
                            "artifact": "latest shadow trace",
                            "category": "live-trace",
                            "severity": "blocked",
                            "next_action": "apply a deterministic GPU scenario",
                        }
                    ],
                },
                "action_items": [
                    {
                        "priority": 1,
                        "category": "live-trace",
                        "severity": "blocked",
                        "blocked": 1,
                        "warn": 0,
                        "artifact": "latest shadow trace",
                        "next_action": "apply a deterministic GPU scenario",
                        "command_hint": "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                        "command_hints": [
                            "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending"
                        ],
                        "command_kind": "shell",
                        "copyable": True,
                    }
                ],
                "operator_runbook": {
                    "step_count": 1,
                    "blocked_step_count": 1,
                    "manual_step_count": 0,
                    "copyable_command_count": 1,
                    "next_shell_command": "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                    "copyable_commands": [
                        "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending"
                    ],
                    "copyable_command_rows": [],
                },
            }
        )

        with self.assertRaisesRegex(
            AssertionError,
            "copyable command provenance count mismatch",
        ):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_next_action_when_blocked(self) -> None:
        with self.assertRaisesRegex(AssertionError, "missing next action"):
            shadow_smoke.validate_operator_status_payload(
                {
                    "ok": True,
                    "dry_run": True,
                    "status": "blocked",
                    "can_shadow_demo": True,
                    "can_customer_claim": False,
                    "decision_readiness": valid_decision_readiness(
                        status="needs-action",
                        customer_claim="blocked",
                        production_binding="blocked",
                    ),
                    "primary_blocker": "customer claim not ready",
                    "debug_commands": ["scripts/demo-gate.py --base-url http://127.0.0.1:8090"],
                    "demo_gate": {"strict_exit_code": 2},
                    "proof_gates": {"total": 0, "pass": 0, "warn": 0, "blocked": 0, "rows": []},
                    "evidence_gaps": {
                        "total": 0,
                        "blocked": 0,
                        "warn": 0,
                        "category_counts": {},
                        "category_rows": [],
                        "rows": [],
                    },
                    "action_items": [],
                    "operator_runbook": {
                        "step_count": 0,
                        "blocked_step_count": 0,
                        "manual_step_count": 0,
                        "copyable_command_count": 0,
                        "next_shell_command": None,
                        "copyable_commands": [],
                    },
                    "simulator": {
                        "claim_ready": True,
                        "claim_mode": "live-baseline-ready",
                        "claim_blocker": None,
                        "claim_next_action": "keep kube-scheduler-simulator baselines fresh before customer claims",
                    },
                    "evidence": {"path": "/api/scheduler/evidence-bundle"},
                }
            )

    def test_operator_status_production_blocker_runbook_matches_first_debug_command(self) -> None:
        with self.assertRaisesRegex(
            AssertionError,
            "runbook first shell command does not match production readiness first debug command",
        ):
            shadow_smoke.validate_operator_status_payload(
                {
                    "ok": True,
                    "dry_run": True,
                    "status": "blocked",
                    "can_shadow_demo": True,
                    "can_customer_claim": False,
                    "decision_readiness": valid_decision_readiness(
                        status="needs-action",
                        customer_claim="blocked",
                        production_binding="blocked",
                    ),
                    "primary_blocker": "production readiness blocked: kubernetes_watch",
                    "next_action": "restore Kubernetes API connectivity",
                    "debug_commands": ["kubectl config current-context"],
                    "production_readiness": {
                        "blocker_class": "kubernetes_watch",
                        "debug_commands": [
                            "kubectl --request-timeout=10s get --raw='/readyz?verbose'"
                        ],
                    },
                    "demo_gate": {"strict_exit_code": 2},
                    "proof_gates": {"total": 0, "pass": 0, "warn": 0, "blocked": 0, "rows": []},
                    "evidence_gaps": {
                        "total": 0,
                        "blocked": 0,
                        "warn": 0,
                        "category_counts": {},
                        "category_rows": [],
                        "rows": [],
                    },
                    "action_items": [],
                    "operator_runbook": {
                        "step_count": 0,
                        "blocked_step_count": 0,
                        "manual_step_count": 0,
                        "copyable_command_count": 1,
                        "next_shell_command": "kubectl config current-context",
                        "copyable_commands": ["kubectl config current-context"],
                    },
                    "simulator": {
                        "claim_ready": True,
                        "claim_mode": "live-baseline-ready",
                        "claim_blocker": None,
                        "claim_next_action": "keep kube-scheduler-simulator baselines fresh before customer claims",
                    },
                    "evidence": {"path": "/api/scheduler/evidence-bundle"},
                }
            )

    def test_evidence_bundle_requires_all_collection_endpoints(self) -> None:
        payload = valid_evidence_bundle()
        payload["collection_commands"] = payload["collection_commands"][:-1]
        with self.assertRaisesRegex(AssertionError, "missing /api/scheduler/evidence-bundle"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_evidence_bundle_requires_complete_rows(self) -> None:
        payload = valid_evidence_bundle()
        del payload["evidence_bundle_rows"][0]["operator_action"]
        with self.assertRaisesRegex(AssertionError, "row 0 missing operator_action"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_evidence_bundle_requires_blocked_claim_to_explain_missing_evidence(self) -> None:
        payload = valid_evidence_bundle()
        payload["missing_live_artifacts"] = []
        payload["missing_live_artifact_rows"] = []
        payload["summary"]["missing_live_artifact_count"] = 0
        payload["launch_proof_gate"]["status"] = "ready"
        payload["summary"]["launch_status"] = "ready"
        with self.assertRaisesRegex(AssertionError, "customer claim is blocked"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_evidence_bundle_requires_consistent_summary_counts(self) -> None:
        payload = valid_evidence_bundle()
        payload["summary"]["evidence_row_count"] = 2
        with self.assertRaisesRegex(AssertionError, "summary row count is inconsistent"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_evidence_bundle_requires_consistent_simulator_readiness(self) -> None:
        payload = valid_evidence_bundle()
        payload["summary"]["simulator_readiness"] = "not_configured"
        with self.assertRaisesRegex(AssertionError, "simulator readiness is inconsistent"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_evidence_bundle_requires_consistent_simulator_probe_counts(self) -> None:
        payload = valid_evidence_bundle()
        payload["summary"]["simulator_probe_ready_count"] = 2
        with self.assertRaisesRegex(AssertionError, "simulator probe ready count is inconsistent"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_evidence_bundle_requires_display_vram_driver_labels(self) -> None:
        payload = valid_evidence_bundle()
        payload["summary"]["vram_display_synthetic_driver_labels"] = ["synthetic reserve pressure"]
        with self.assertRaisesRegex(AssertionError, "display synthetic driver labels"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_evidence_bundle_requires_synthetic_headroom_aliases(self) -> None:
        payload = valid_evidence_bundle()
        payload["summary"]["vram_synthetic_headroom_definition"] = "different"
        with self.assertRaisesRegex(AssertionError, "synthetic headroom alias is inconsistent"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

        payload = valid_evidence_bundle()
        payload["summary"]["vram_synthetic_headroom_driver"] = False
        with self.assertRaisesRegex(AssertionError, "synthetic headroom driver alias is inconsistent"):
            shadow_smoke.validate_evidence_bundle_payload(payload)

    def test_operator_status_requires_display_vram_driver_labels(self) -> None:
        payload = {
            "ok": True,
            "dry_run": True,
            "status": "ready",
            "can_shadow_demo": True,
            "can_customer_claim": True,
            "decision_readiness": valid_decision_readiness(),
            "demo_gate": {"strict_exit_code": 0},
            "proof_gates": {"total": 0, "pass": 0, "warn": 0, "blocked": 0, "rows": []},
            "evidence_gaps": {
                "total": 0,
                "blocked": 0,
                "warn": 0,
                "category_counts": {},
                "category_rows": [],
                "rows": [],
            },
            "action_items": [],
            "operator_runbook": {
                "step_count": 0,
                "blocked_step_count": 0,
                "manual_step_count": 0,
                "copyable_command_count": 0,
                "next_shell_command": None,
                "copyable_commands": [],
            },
            "scale_safety": {
                "available": True,
                "status": "regret-bounded",
                "regret_status": "full_feasible_set",
                "next_action": "no candidate-pruning regret action required for this trace",
                "pruning_active": False,
                "widened": False,
                "edge_reduction_milli": 0,
                "candidate_node_limit": 0,
                "retry_count": 0,
                "unpruned_candidate_edges": 10,
                "initial_candidate_edges": 10,
                "final_candidate_edges": 10,
                "candidate_pruned_workloads": 0,
            },
            "binding_safety": {
                "available": True,
                "status": "read-only",
                "next_action": "no binding mutation action required while shadow remains read-only",
                "mutation_allowed": False,
                "mode": "observe-only",
                "enable_real_binding": False,
                "real_binding_dry_run": False,
                "binding_kill_switch": False,
                "binding_canary_mode": "all",
                "binding_low_risk_max_gpus": 1,
                "max_binds_per_pass": 10,
                "binding_reservation_ttl_seconds": 60,
                "latest_trace_sequence": 1,
                "latest_outcome_count": 0,
                "bound": 0,
                "validated": 0,
                "skipped": 0,
                "failed": 0,
                "reservations": {},
                "reservation_pressure": "none",
                "reservation_pressure_description": "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
                "reservation_pressure_scope": "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
                "reservation_pressure_reason": "no active binding reservations are holding GPU capacity",
                "reservation_pressure_next_action": "no reservation pressure action required",
                "skip_breakdown": {},
            },
            "vram": {
                "mode": "Shadow advisory only",
                "scheduler_use": "Score and warn; do not reject pods",
                "hard_blocker_count": 1,
                "hard_admission_blockers": ["no true bare-metal/cloud CUDA OOM labels"],
                "evidence_collection_plan": [
                    {
                        "target": "true CUDA OOM labels",
                        "unblocks": "hard VRAM admission",
                        "commands": ["python3 vram-model-lab/run_matrix.py --record-oom"],
                    }
                ],
                "next_evidence_target": "true CUDA OOM labels",
                "model_driver_count": 1,
                "top_driver_labels": ["synthetic reserve pressure"],
                "display_top_driver_labels": ["synthetic reserve pressure"],
                "claim_safe_driver_count": 0,
                "claim_safe_driver_labels": [],
                "display_claim_safe_driver_labels": [],
                "real_model_driver_count": 0,
                "real_top_driver_labels": [],
                "display_real_top_driver_labels": [],
                "synthetic_driver_count": 1,
                "synthetic_driver_labels": ["synthetic reserve pressure"],
                "display_synthetic_driver_labels": ["synthetic reserve pressure"],
                "synthetic_reserve_driver": True,
                "synthetic_headroom_driver": True,
                "reserve_pressure_definition": "reserve_extra_mib padding",
                "synthetic_headroom_definition": "reserve_extra_mib padding",
            },
            "evidence": {"path": "/api/scheduler/evidence-bundle"},
        }
        with self.assertRaisesRegex(AssertionError, "display top driver labels"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_synthetic_headroom_aliases(self) -> None:
        payload = valid_operator_status()
        payload["vram"]["synthetic_headroom_driver"] = False
        with self.assertRaisesRegex(AssertionError, "synthetic headroom driver alias is inconsistent"):
            shadow_smoke.validate_operator_status_payload(payload)

        payload = valid_operator_status()
        payload["vram"]["synthetic_headroom_definition"] = "different"
        with self.assertRaisesRegex(AssertionError, "synthetic headroom definition alias is inconsistent"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_decision_readiness_capabilities(self) -> None:
        payload = valid_operator_status()
        payload["decision_readiness"]["capabilities"] = [
            row
            for row in payload["decision_readiness"]["capabilities"]
            if row["name"] != "production_binding"
        ]
        with self.assertRaisesRegex(AssertionError, "decision readiness missing production_binding"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_decision_summary_extracts_binding_capability(self) -> None:
        payload = valid_operator_status()
        payload["decision_readiness"]["status"] = "needs-action"
        payload["decision_readiness"]["capabilities"][-1]["status"] = "dry-run"
        payload["decision_readiness"]["capabilities"][-1]["can_execute"] = False
        payload["binding_safety"]["reservation_pressure"] = "active"
        payload["binding_safety"][
            "reservation_pressure_reason"
        ] = "1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run"

        summary = shadow_smoke.operator_decision_summary(payload)

        self.assertEqual(summary["status"], "needs-action")
        self.assertEqual(summary["production_binding_status"], "dry-run")
        self.assertEqual(summary["production_binding_can_execute"], False)
        self.assertIn("real binding", summary["production_binding_next_action"])
        self.assertEqual(summary["reservation_pressure"], "active")
        self.assertIn("pending or reserved GPU capacity", summary["reservation_pressure_description"])
        self.assertIn("unrelated to CUDA", summary["reservation_pressure_scope"])
        self.assertIn("hold 4 GPU", summary["reservation_pressure_reason"])

    def test_operator_status_requires_vram_hard_admission_blocker_plan(self) -> None:
        payload = valid_operator_status()
        payload["vram"]["hard_admission_blockers"] = []
        with self.assertRaisesRegex(AssertionError, "VRAM missing hard admission blockers"):
            shadow_smoke.validate_operator_status_payload(payload)

        payload = valid_operator_status()
        payload["vram"]["evidence_collection_plan"] = []
        with self.assertRaisesRegex(AssertionError, "VRAM missing evidence collection plan"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_scale_safety_full_comparison_for_unknown_regret(self) -> None:
        payload = valid_operator_status()
        payload["scale_safety"]["regret_status"] = "pruned_regret_unknown"
        payload["scale_safety"]["status"] = "regret-unknown"
        payload["scale_safety"]["next_action"] = "looks fine"
        with self.assertRaisesRegex(AssertionError, "unknown regret must request full candidate comparison"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_live_binding_next_action_guardrail(self) -> None:
        payload = valid_operator_status()
        payload["binding_safety"]["mutation_allowed"] = True
        payload["binding_safety"]["real_binding_dry_run"] = False
        payload["binding_safety"]["status"] = "mutation-capable"
        payload["binding_safety"]["next_action"] = "looks fine"
        with self.assertRaisesRegex(AssertionError, "live binding safety must mention production binding or kill switch"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_blocking_reservation_pressure_reason(self) -> None:
        payload = valid_operator_status()
        payload["binding_safety"]["reservation_pressure"] = "blocking"
        payload["binding_safety"]["reservation_pressure_reason"] = "ledger is unhappy"
        with self.assertRaisesRegex(AssertionError, "blocking reservation pressure must explain rejected reservations"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_reservation_pressure_description(self) -> None:
        payload = valid_operator_status()
        payload["binding_safety"].pop("reservation_pressure_description")
        with self.assertRaisesRegex(AssertionError, "missing reservation pressure description"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_reservation_pressure_scope(self) -> None:
        payload = valid_operator_status()
        payload["binding_safety"].pop("reservation_pressure_scope")
        with self.assertRaisesRegex(AssertionError, "reservation pressure scope"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_operator_status_requires_simulator_claim_contract(self) -> None:
        payload = valid_operator_status()
        del payload["simulator"]["claim_ready"]
        with self.assertRaisesRegex(AssertionError, "simulator missing claim readiness"):
            shadow_smoke.validate_operator_status_payload(payload)

        payload = valid_operator_status()
        payload["simulator"]["claim_ready"] = False
        payload["simulator"]["claim_blocker"] = None
        with self.assertRaisesRegex(AssertionError, "simulator missing claim blocker"):
            shadow_smoke.validate_operator_status_payload(payload)

        payload = valid_operator_status()
        payload["simulator"]["claim_next_action"] = ""
        with self.assertRaisesRegex(AssertionError, "simulator missing claim next action"):
            shadow_smoke.validate_operator_status_payload(payload)

        payload = valid_operator_status()
        payload["simulator"]["claim_ready"] = False
        payload["simulator"]["claim_blocker"] = "no kube-scheduler-simulator endpoint answered /api/v1/export"
        payload["simulator"]["recovery_command"] = ""
        with self.assertRaisesRegex(AssertionError, "simulator missing recovery command"):
            shadow_smoke.validate_operator_status_payload(payload)

    def test_smoke_result_is_machine_readable(self) -> None:
        result = shadow_smoke.smoke_result(
            base_url="http://127.0.0.1:8090",
            readiness_mode="strict",
            readiness_blocker_class=None,
            cached=66,
            total=66,
            missing=0,
            scenario_count=33,
            win_count=14,
            live_gate_count=6,
            first_gate="pending GPU trace",
            first_endpoint="/api/scheduler/traces",
            vram_rows=228,
            vram_samples=4615,
            vram_reserve_rows=37,
            vram_evidence_present=7,
            vram_evidence_total=7,
            vram_hard_ready=False,
            vram_driver_count=3,
            vram_synthetic_reserve_driver=True,
            vram_investment_rows=6,
            vram_investment_oom_risk_reduction=4,
            vram_investment_high_vram_preserved=1,
            vram_investment_advisory_rows=1,
            vram_investment_average_baseline_oom_risk_percent=64,
            vram_investment_average_ksolver_oom_risk_percent=24,
            evidence_command_count=8,
            evidence_row_count=9,
            evidence_launch_status="incomplete",
            evidence_customer_claim_ready=False,
            evidence_production_blocker_class="kubernetes_watch",
            operator_decision_status="needs-action",
            operator_decision_summary="demo=ready, claim=blocked, vram-score=ready, hard-admit=blocked, bind=read-only",
            operator_decision_highest_risk="kube-scheduler baseline is not customer-claim ready",
            operator_decision_next_action="repair kube-scheduler-simulator before making kube-vs-ksolver claims",
            operator_production_binding_status="read-only",
            operator_production_binding_can_execute=False,
            operator_production_binding_next_action="enable real binding only after ownership, RBAC, canary, reservation, and kill-switch gates are approved",
            operator_reservation_pressure="active",
            operator_reservation_pressure_description="Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
            operator_reservation_pressure_scope="Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.",
            operator_reservation_pressure_reason="1 active reservation entrie(s) hold 4 GPU(s) while binding safety gates run",
            operator_reservation_pressure_next_action="verify reservations are fresh and within TTL before binding the reserved placements",
            operator_first_shell_command="kubectl --request-timeout=10s get --raw='/readyz?verbose'",
            operator_first_shell_command_category="environment",
            operator_first_shell_command_severity="blocked",
            operator_first_shell_command_artifact="healthy Kubernetes watch/relist state",
            operator_first_shell_command_next_action="restore Kubernetes API connectivity",
            operator_first_shell_command_kind="shell",
        )
        self.assertEqual(result["ok"], True)
        self.assertEqual(result["base_url"], "http://127.0.0.1:8090")
        self.assertEqual(result["readiness_mode"], "strict")
        self.assertIsNone(result["readiness_blocker_class"])
        self.assertEqual(result["simulator_cache_cached_baselines"], 66)
        self.assertEqual(result["scenario_count"], 33)
        self.assertEqual(result["ksolver_win_count"], 14)
        self.assertEqual(result["demo_readiness_live_gate_count"], 6)
        self.assertEqual(result["demo_readiness_first_gate"], "pending GPU trace")
        self.assertEqual(result["demo_readiness_first_endpoint"], "/api/scheduler/traces")
        self.assertEqual(
            result["operator_first_shell_command"],
            "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
        )
        self.assertEqual(result["operator_first_shell_command_category"], "environment")
        self.assertEqual(result["operator_first_shell_command_severity"], "blocked")
        self.assertEqual(
            result["operator_first_shell_command_artifact"],
            "healthy Kubernetes watch/relist state",
        )
        self.assertEqual(
            result["operator_first_shell_command_next_action"],
            "restore Kubernetes API connectivity",
        )
        self.assertEqual(result["operator_first_shell_command_kind"], "shell")
        self.assertEqual(result["vram_calibration_rows"], 228)
        self.assertEqual(result["vram_calibration_time_series_samples"], 4615)
        self.assertEqual(result["vram_calibration_reserve_pressure_rows"], 37)
        self.assertEqual(result["vram_calibration_synthetic_headroom_rows"], 37)
        self.assertEqual(result["vram_calibration_evidence_columns_present"], 7)
        self.assertEqual(result["vram_calibration_evidence_columns_total"], 7)
        self.assertEqual(result["vram_calibration_hard_admission_ready"], False)
        self.assertEqual(result["vram_calibration_model_driver_count"], 3)
        self.assertEqual(result["vram_calibration_synthetic_reserve_driver"], True)
        self.assertEqual(result["vram_calibration_synthetic_headroom_driver"], True)
        self.assertEqual(result["vram_calibration"], "advisory-ready")
        self.assertEqual(result["vram_investment_demo_rows"], 6)
        self.assertEqual(result["vram_investment_oom_risk_reduction_pods"], 4)
        self.assertEqual(result["vram_investment_high_vram_nodes_preserved"], 1)
        self.assertEqual(result["vram_investment_advisory_rows"], 1)
        self.assertEqual(result["vram_investment_average_baseline_oom_risk_percent"], 64)
        self.assertEqual(result["vram_investment_average_ksolver_oom_risk_percent"], 24)
        self.assertEqual(result["evidence_bundle_collection_commands"], 8)
        self.assertEqual(result["evidence_bundle_rows"], 9)
        self.assertEqual(result["evidence_bundle_launch_status"], "incomplete")
        self.assertEqual(result["evidence_bundle_customer_claim_ready"], False)
        self.assertEqual(result["evidence_bundle_production_blocker_class"], "kubernetes_watch")
        self.assertEqual(result["operator_decision_status"], "needs-action")
        self.assertIn("claim=blocked", result["operator_decision_summary"])
        self.assertEqual(
            result["operator_decision_highest_risk"],
            "kube-scheduler baseline is not customer-claim ready",
        )
        self.assertEqual(result["operator_production_binding_status"], "read-only")
        self.assertEqual(result["operator_production_binding_can_execute"], False)
        self.assertEqual(result["operator_reservation_pressure"], "active")
        self.assertIn("pending or reserved GPU capacity", result["operator_reservation_pressure_description"])
        self.assertIn("unrelated to CUDA", result["operator_reservation_pressure_scope"])
        self.assertIn("hold 4 GPU", result["operator_reservation_pressure_reason"])
        self.assertEqual(result["evidence_bundle"], "validated")
        self.assertEqual(result["refresh_contract"], "lightweight")
        self.assertEqual(result["demo_readiness"], "passing")

    def test_smoke_summary_uses_same_result_fields(self) -> None:
        result = shadow_smoke.smoke_result(
            base_url="http://127.0.0.1:8090",
            readiness_mode="degraded",
            readiness_blocker_class="kubernetes_watch",
            cached=66,
            total=66,
            missing=0,
            scenario_count=33,
            win_count=14,
            live_gate_count=6,
            first_gate="pending GPU trace",
            first_endpoint="/api/scheduler/traces",
            vram_rows=228,
            vram_samples=4615,
            vram_reserve_rows=37,
            vram_evidence_present=7,
            vram_evidence_total=7,
            vram_hard_ready=False,
            vram_driver_count=3,
            vram_synthetic_reserve_driver=True,
            vram_investment_rows=6,
            vram_investment_oom_risk_reduction=4,
            vram_investment_high_vram_preserved=1,
            vram_investment_advisory_rows=1,
            vram_investment_average_baseline_oom_risk_percent=64,
            vram_investment_average_ksolver_oom_risk_percent=24,
            evidence_command_count=8,
            evidence_row_count=9,
            evidence_launch_status="incomplete",
            evidence_customer_claim_ready=False,
            evidence_production_blocker_class="kubernetes_watch",
            simulator_claim_ready=False,
            simulator_claim_mode="partial-live-baseline",
            simulator_claim_blocker="only some kube-scheduler-simulator endpoints are ready",
            simulator_claim_next_action="use scripts/kss-pool.sh status before refreshing baselines",
            operator_status="blocked",
            operator_decision_status="needs-action",
            operator_production_binding_status="read-only",
            operator_reservation_pressure="active",
            operator_first_shell_command_category="environment",
            operator_first_shell_command_next_action="restore Kubernetes API connectivity",
        )
        self.assertIn("degraded/kubernetes_watch", shadow_smoke.smoke_summary(result))
        self.assertIn("66/66 cached", shadow_smoke.smoke_summary(result))
        self.assertIn(
            "simulator claim partial-live-baseline (blocked): only some kube-scheduler-simulator endpoints are ready",
            shadow_smoke.smoke_summary(result),
        )
        self.assertIn(
            "-> use scripts/kss-pool.sh status before refreshing baselines",
            shadow_smoke.smoke_summary(result),
        )
        self.assertIn("33 scenarios (14 wins)", shadow_smoke.smoke_summary(result))
        self.assertIn("6 live gates", shadow_smoke.smoke_summary(result))
        self.assertIn("first gate pending GPU trace", shadow_smoke.smoke_summary(result))
        self.assertIn("VRAM calibration 228 rows/4615 samples", shadow_smoke.smoke_summary(result))
        self.assertIn("37 synthetic headroom rows", shadow_smoke.smoke_summary(result))
        self.assertIn("3 model drivers", shadow_smoke.smoke_summary(result))
        self.assertIn("VRAM demo 6 rows", shadow_smoke.smoke_summary(result))
        self.assertIn("4 OOM-risk pods reduced", shadow_smoke.smoke_summary(result))
        self.assertIn("1 high-VRAM preserved", shadow_smoke.smoke_summary(result))
        self.assertIn("evidence bundle 9 rows/8 commands", shadow_smoke.smoke_summary(result))
        self.assertIn("launch incomplete", shadow_smoke.smoke_summary(result))
        self.assertIn("production blocker kubernetes_watch", shadow_smoke.smoke_summary(result))
        self.assertIn("operator status blocked", shadow_smoke.smoke_summary(result))
        self.assertIn("decision needs-action", shadow_smoke.smoke_summary(result))
        self.assertIn("bind read-only", shadow_smoke.smoke_summary(result))
        self.assertIn("binding reservation pressure active", shadow_smoke.smoke_summary(result))
        self.assertIn(
            "first shell command reason environment: restore Kubernetes API connectivity",
            shadow_smoke.smoke_summary(result),
        )
        self.assertIn("demo readiness passing", shadow_smoke.smoke_summary(result))

    def test_smoke_failure_is_machine_readable(self) -> None:
        self.assertEqual(
            shadow_smoke.smoke_failure(AssertionError("cache missing")),
            {"ok": False, "error": "cache missing"},
        )
        self.assertEqual(
            shadow_smoke.smoke_failure(
                "readyz failed",
                {
                    "readyz": {
                        "ok": False,
                        "status": 503,
                        "body": "watch not healthy",
                    }
                },
            ),
            {
                "ok": False,
                "error": "readyz failed",
                "readiness_probe": {
                    "readyz": {
                        "ok": False,
                        "status": 503,
                        "body": "watch not healthy",
                    }
                },
                "readiness_blocker_class": "kubernetes_watch",
            },
        )

    def test_classify_readiness_blocker_names_primary_gate(self) -> None:
        self.assertEqual(
            shadow_smoke.classify_readiness_blocker(
                {"production_readiness": {"blocker": "watch not healthy"}}
            ),
            "kubernetes_watch",
        )
        self.assertEqual(
            shadow_smoke.classify_readiness_blocker(
                {"production_readiness": {"blocker": "solver unavailable"}}
            ),
            "solver",
        )
        self.assertEqual(
            shadow_smoke.classify_readiness_blocker(
                {
                    "readyz": {"ok": True, "status": 200, "body": "ready"},
                    "evidence_summary": {"simulator_readiness": "configured_unreachable"},
                }
            ),
            "simulator",
        )
        self.assertEqual(
            shadow_smoke.classify_readiness_blocker(
                {
                    "readyz": {"ok": True, "status": 200, "body": "ready"},
                    "evidence_summary": {
                        "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                        "simulator_readiness": "ready",
                        "review_ready": False,
                    },
                }
            ),
            "kubernetes_watch",
        )
        self.assertEqual(
            shadow_smoke.classify_readiness_blocker(
                {
                    "readyz": {"ok": True, "status": 200, "body": "ready"},
                    "evidence_summary": {
                        "simulator_readiness": "ready",
                        "review_ready": False,
                    },
                }
            ),
            "review_claims",
        )

    def test_classify_readiness_blocker_stays_consistent_with_demo_gate(self) -> None:
        gate_path = ROOT / "demo-gate.py"
        gate_spec = importlib.util.spec_from_file_location("demo_gate_for_smoke", gate_path)
        demo_gate = importlib.util.module_from_spec(gate_spec)
        assert gate_spec and gate_spec.loader
        gate_spec.loader.exec_module(demo_gate)
        cases = [
            {"production_readiness": {"blocker": "watch not healthy"}},
            {"production_readiness": {"blocker": "solver unavailable"}},
            {
                "readyz": {"ok": False, "status": 503, "body": "some apiserver error"},
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {"simulator_readiness": "not_configured"},
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {"simulator_readiness": "configured_unreachable"},
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {
                    "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                    "simulator_readiness": "ready",
                    "review_ready": False,
                },
            },
            {
                "readyz": {"ok": True, "status": 200, "body": "ready"},
                "evidence_summary": {"simulator_readiness": "ready", "review_ready": False},
            },
        ]
        for probe in cases:
            self.assertEqual(
                shadow_smoke.classify_readiness_blocker(probe),
                demo_gate.classify_readiness_blocker(probe),
            )

    def test_readiness_probe_summary_lines_include_actionable_context(self) -> None:
        lines = shadow_smoke.readiness_probe_summary_lines(
            {
                "readyz": {"status": 503, "body": "watch not healthy"},
                "production_readiness": {
                    "blocker": "watch not healthy",
                    "diagnostic_hint": "verify kube context and pod list RBAC",
                    "last_error_at": "2026-07-06T07:00:00Z",
                    "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                },
                "simulator_readiness": {
                    "endpoint_count": 1,
                    "readiness": "configured_not_probed",
                },
                "evidence_summary": {
                    "claim_blockers": ["customer claim not ready"],
                    "primary_claim_blocker": "customer claim not ready",
                    "primary_claim_blocker_next_action": "resolve launch proof gaps before making customer-facing claims",
                    "vram_admission_mode": "Shadow advisory only",
                    "vram_next_evidence_target": "true CUDA OOM labels",
                    "simulator_endpoint_count": 1,
                    "simulator_readiness": "configured_not_probed",
                    "simulator_claim_ready": False,
                    "simulator_claim_mode": "partial-live-baseline",
                    "simulator_claim_blocker": "only some kube-scheduler-simulator endpoints are ready",
                    "simulator_claim_next_action": "use scripts/kss-pool.sh status before refreshing baselines",
                },
                "operator_status": valid_operator_status(),
            }
        )
        self.assertIn("readiness probe: /readyz status=503 watch not healthy", lines)
        self.assertIn("production blocker: watch not healthy", lines)
        self.assertIn("class: kubernetes_watch", lines)
        self.assertIn("diagnostic hint: verify kube context and pod list RBAC", lines)
        self.assertIn("last error at: 2026-07-06T07:00:00Z", lines)
        self.assertIn("debug command: kubectl --request-timeout=10s get --raw='/readyz?verbose'", lines)
        self.assertIn("simulator: configured_not_probed (1 endpoint(s))", lines)
        self.assertIn(
            "simulator claim: partial-live-baseline (blocked): only some kube-scheduler-simulator endpoints are ready",
            lines,
        )
        self.assertIn("simulator action: use scripts/kss-pool.sh status before refreshing baselines", lines)
        self.assertIn("VRAM: Shadow advisory only", lines)
        self.assertIn("next VRAM evidence: true CUDA OOM labels", lines)
        self.assertIn("primary blocker: customer claim not ready", lines)
        self.assertIn(
            "next action: resolve launch proof gaps before making customer-facing claims",
            lines,
        )
        self.assertIn("binding reservation pressure: none", lines)
        self.assertIn(
            "binding reservation pressure means: Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.",
            lines,
        )
        self.assertIn(
            "binding reservation pressure reason: no active binding reservations are holding GPU capacity",
            lines,
        )
        self.assertIn("binding reservation pressure action: no reservation pressure action required", lines)

    def test_readiness_probe_summary_does_not_invent_missing_counts(self) -> None:
        lines = shadow_smoke.readiness_probe_summary_lines(
            {
                "simulator_readiness": {
                    "readiness": "configured_not_probed",
                },
            }
        )
        self.assertIn("simulator: configured_not_probed (unknown endpoint(s))", lines)

    def test_readiness_probe_summarizes_evidence_bundle(self) -> None:
        bodies = {
            "http://shadow/healthz": {"status": 200, "body": "ok"},
            "http://shadow/readyz": {"status": 503, "body": "watch not healthy"},
            "http://shadow/api/scheduler/production-safety": {
                "status": 200,
                "body": json.dumps(
                    {
                        "readiness": {
                            "ready": False,
                            "blocker": "watch not healthy",
                            "diagnostic_hint": "verify kube context and pod list RBAC",
                            "last_error_at": "2026-07-06T07:00:00Z",
                            "debug_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                        },
                        "simulator": {
                            "endpoint_count": 2,
                            "live_dashboard_baseline_configured": True,
                            "readiness": "configured_not_probed",
                            "readiness_note": "endpoints are configured; export readiness is checked during live baseline calls",
                            "readiness_probe": {
                                "checked_count": 2,
                                "ready_count": 1,
                                "timeout_millis": 2000,
                            },
                            "claim_guard": "live dashboard baselines can call kube-scheduler-simulator",
                        },
                    }
                ),
            },
            "http://shadow/api/scheduler/evidence-bundle": {
                "status": 200,
                "body": json.dumps(
                    {
                        "summary": {
                            "review_ready": False,
                            "demo_gate_status": "local-pass-strict-blocked",
                            "demo_gate_strict_exit_code": 2,
                            "primary_claim_blocker": "production readiness blocked: kubernetes_watch",
                            "primary_claim_blocker_next_action": "restore Kubernetes API connectivity",
                            "claim_blockers": ["customer claim not ready"],
                            "vram_admission_mode": "Shadow advisory only",
                            "vram_scheduler_use": "Score and warn; do not reject pods",
                            "vram_hard_blocker_count": 4,
                            "vram_next_evidence_target": "true CUDA OOM labels",
                            "production_readiness_blocker_class": "kubernetes_watch",
        "production_readiness_last_error_class": "api_timeout",
                            "simulator_endpoint_count": 2,
                            "simulator_probe_checked_count": 2,
                            "simulator_probe_ready_count": 1,
                            "simulator_probe_timeout_millis": 2000,
                            "simulator_readiness": "configured_not_probed",
                            "simulator_readiness_note": (
                                "endpoints are configured; export readiness is checked during live baseline calls"
                            ),
                        }
                    }
                ),
            },
            "http://shadow/api/scheduler/operator-status": {
                "status": 200,
                "body": json.dumps(
                    {
                        "ok": True,
                        "status": "blocked",
                        "primary_blocker": "production readiness blocked: kubernetes_watch",
                        "next_action": "restore Kubernetes API connectivity",
                        "decision_readiness": {
                            "status": "needs-action",
                            "summary": "demo=ready, claim=blocked, vram-score=ready, hard-admit=blocked, bind=read-only",
                            "highest_risk": "kube-scheduler baseline is not customer-claim ready",
                            "next_action": "repair kube-scheduler-simulator before making kube-vs-ksolver claims",
                            "capabilities": [
                                {
                                    "name": "production_binding",
                                    "label": "Production binding",
                                    "status": "read-only",
                                    "can_execute": False,
                                    "next_action": "enable real binding only after ownership, RBAC, canary, reservation, and kill-switch gates are approved",
                                }
                            ],
                        },
                        "action_items": [
                            {
                                "priority": 1,
                                "category": "environment",
                                "severity": "blocked",
                                "next_action": "restore Kubernetes API connectivity",
                                "command_hint": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                                "command_kind": "shell",
                                "copyable": True,
                            }
                        ],
                        "operator_runbook": {
                            "step_count": 1,
                            "blocked_step_count": 1,
                            "manual_step_count": 0,
                            "copyable_command_count": 1,
                            "next_shell_command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                            "copyable_commands": ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
                            "copyable_command_rows": [
                                {
                                    "command": "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                                    "priority": 1,
                                    "category": "environment",
                                    "severity": "blocked",
                                    "artifact": "healthy Kubernetes watch/relist state",
                                    "next_action": "restore Kubernetes API connectivity",
                                    "command_kind": "shell",
                                }
                            ],
                        },
                    }
                ),
            },
        }

        def fake_fetch(url: str) -> dict[str, object]:
            row = bodies[url]
            return {
                "ok": 200 <= int(row["status"]) < 300,
                "status": row["status"],
                "body": row["body"],
            }

        original_fetch = shadow_smoke.fetch_probe
        try:
            shadow_smoke.fetch_probe = fake_fetch
            probe = shadow_smoke.readiness_probe("http://shadow")
        finally:
            shadow_smoke.fetch_probe = original_fetch

        self.assertEqual(probe["readyz"]["status"], 503)
        self.assertEqual(probe["production_readiness"]["blocker"], "watch not healthy")
        self.assertEqual(
            probe["production_readiness"]["diagnostic_hint"],
            "verify kube context and pod list RBAC",
        )
        self.assertEqual(
            probe["production_readiness"]["debug_commands"],
            ["kubectl --request-timeout=10s get --raw='/readyz?verbose'"],
        )
        self.assertEqual(probe["simulator_readiness"]["endpoint_count"], 2)
        self.assertEqual(probe["simulator_readiness"]["readiness"], "configured_not_probed")
        self.assertEqual(probe["simulator_readiness"]["readiness_probe"]["ready_count"], 1)
        self.assertIn("export readiness", probe["simulator_readiness"]["readiness_note"])
        self.assertEqual(probe["evidence_bundle"]["status"], 200)
        self.assertEqual(probe["evidence_summary"]["demo_gate_strict_exit_code"], 2)
        self.assertEqual(probe["evidence_summary"]["vram_admission_mode"], "Shadow advisory only")
        self.assertEqual(probe["evidence_summary"]["vram_next_evidence_target"], "true CUDA OOM labels")
        self.assertEqual(
            probe["evidence_summary"]["production_readiness_blocker_class"],
            "kubernetes_watch",
        )
        self.assertEqual(probe["evidence_summary"]["simulator_endpoint_count"], 2)
        self.assertEqual(probe["evidence_summary"]["simulator_probe_checked_count"], 2)
        self.assertEqual(probe["evidence_summary"]["simulator_probe_ready_count"], 1)
        self.assertEqual(probe["evidence_summary"]["simulator_readiness"], "configured_not_probed")
        lines = shadow_smoke.readiness_probe_summary_lines(probe)
        self.assertIn("simulator: configured_not_probed (1/2 ready, 2 endpoint(s))", lines)
        self.assertIn("operator decision: needs-action", lines)
        self.assertIn(
            "operator decision summary: demo=ready, claim=blocked, vram-score=ready, hard-admit=blocked, bind=read-only",
            lines,
        )
        self.assertIn(
            "operator risk: kube-scheduler baseline is not customer-claim ready",
            lines,
        )
        self.assertIn("production binding: read-only (not executable)", lines)
        self.assertIn("production class: kubernetes_watch", lines)

    def test_base_url_from_argv_accepts_space_and_equals_forms(self) -> None:
        self.assertEqual(
            shadow_smoke.base_url_from_argv(["shadow-smoke.py", "--base-url", "http://x/"]),
            "http://x",
        )
        self.assertEqual(
            shadow_smoke.base_url_from_argv(["shadow-smoke.py", "--base-url=http://y/"]),
            "http://y",
        )

    def test_json_failure_mode_outputs_json(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT / "shadow-smoke.py"),
                "--base-url",
                "http://127.0.0.1:1",
                "--json",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertNotEqual(proc.returncode, 0)
        payload = json.loads(proc.stdout)
        self.assertEqual(payload["ok"], False)
        self.assertIn("error", payload)
        self.assertIn("readiness_probe", payload)
        self.assertIn("readiness_blocker_class", payload)
        self.assertIn("readyz", payload["readiness_probe"])

    def test_human_failure_mode_outputs_readiness_probe(self) -> None:
        proc = subprocess.run(
            [
                sys.executable,
                str(ROOT / "shadow-smoke.py"),
                "--base-url",
                "http://127.0.0.1:1",
            ],
            check=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("shadow smoke failed:", proc.stderr)
        self.assertIn("readiness probe: /readyz", proc.stderr)
        self.assertIn("class:", proc.stderr)

    def test_operator_docs_use_synthetic_headroom_wording(self) -> None:
        docs = [
            ROOT.parent / "README.md",
            ROOT.parent / "vram-model-lab" / "README.md",
        ]
        for path in docs:
            text = path.read_text()
            self.assertNotIn("reserve-pressure", text, path.name)
            self.assertNotIn("reserve pressure", text, path.name)
            self.assertIn("synthetic VRAM headroom", text, path.name)

    def test_smoke_source_uses_synthetic_headroom_wording(self) -> None:
        source = (ROOT / "shadow-smoke.py").read_text(encoding="utf-8")
        self.assertNotIn("synthetic reserve pressure", source)
        self.assertNotIn("synthetic transformer reserve pressure", source)
        self.assertIn("synthetic VRAM headroom", source)

    def test_launch_proof_actions_do_not_promote_fallback_baselines(self) -> None:
        source = (ROOT.parent / "ksolver" / "src" / "scheduler" / "shadow.rs").read_text()
        self.assertNotIn("cached/fallback provenance", source)
        self.assertIn("cached simulator provenance", source)

    def test_simulator_report_copy_does_not_offer_bounded_fallback(self) -> None:
        source = (ROOT.parent / "ksolver" / "src" / "scheduler" / "gpu_scenarios.rs").read_text()
        self.assertNotIn("bounded fallback", source)
        self.assertNotIn("timed-out fallback, or deterministic fallback", source)
        self.assertIn("invalid legacy fallback markers", source)


if __name__ == "__main__":
    unittest.main()
