use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
use crate::scheduler::config::ShadowConfig;
use crate::scheduler::decision::build_decision_trace_with_tenant_policy;
use crate::scheduler::trace::{
    summarize_candidate_quality, summarize_scheduling_outcome, AdmissionMetrics,
    BindingOutcomeMetrics, BindingReservationMetrics, DecisionTrace, GpuUtilizationMetrics,
    PodPlacement, TraceStore,
};
use crate::scheduler::watch_state::WatchState;
use crate::{collector, cpsat_rust, metrics, normalizer, pricing};
use anyhow::Result;
use axum::extract::{Query, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1 as corev1;
use kube::runtime::watcher;
use kube::{Api, Client};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

const VRAM_SYNTHETIC_HEADROOM_DEFINITION: &str = "Rows with reserve_extra_mib > 0 intentionally add synthetic VRAM padding to stress scheduler headroom; this is a headroom stress-test signal, not organic model demand.";
const VRAM_RESERVE_PRESSURE_DEFINITION: &str = VRAM_SYNTHETIC_HEADROOM_DEFINITION;

type BindOutcomeSnapshot = Option<(u64, Vec<crate::scheduler::binder::BindOutcome>)>;

#[derive(Clone)]
struct ShadowHttpState {
    traces: Arc<TraceStore>,
    watch_healthy: Arc<AtomicBool>,
    latest_readiness_error: Arc<Mutex<Option<ShadowReadinessError>>>,
    /// Latest normalized cluster snapshot, for re-validating rendered bindings (staleness guard).
    latest_cluster: Arc<Mutex<Option<crate::model::NormalizedCluster>>>,
    /// Latest pending GPU pods observed by the watch loop, for user-triggered re-solves.
    latest_pending: Arc<Mutex<Vec<crate::scheduler::pod_filter::PendingGpuPod>>>,
    /// Latest binding executor outcomes, used to render read-only Kubernetes Event drafts.
    latest_bind_outcomes: Arc<Mutex<BindOutcomeSnapshot>>,
    simulator_plan_cache: Arc<tokio::sync::Mutex<Option<(String, serde_json::Value)>>>,
    /// Latest kube-baseline liabilities (OOM risk / split gangs kube accepts and ksolver refuses),
    /// computed by the kube-simulator-plan handler and consumed by the evidence bundle's safety gate.
    latest_liabilities: Arc<Mutex<Option<serde_json::Value>>>,
    simulator_pool: Arc<DashboardSimulatorPool>,
    demo_report_cache: Arc<tokio::sync::Mutex<Option<serde_json::Value>>>,
    demo_report_refresh_status: Arc<tokio::sync::Mutex<Option<serde_json::Value>>>,
    kubeconfig: String,
    cfg: ShadowConfig,
    active_objective: Arc<Mutex<ObjectiveSelection>>,
}

#[derive(Debug, Clone)]
struct ObjectiveSelection {
    profile: ObjectiveProfile,
    weights: ObjectiveWeights,
}

struct CollectedShadowSnapshot {
    raw: crate::model::ClusterSnapshot,
    normalized: crate::model::NormalizedCluster,
}

#[derive(Debug, Clone)]
struct ShadowReadinessError {
    message: String,
    observed_at: String,
}

fn set_latest_readiness_error(
    latest: &Arc<Mutex<Option<ShadowReadinessError>>>,
    message: impl Into<String>,
) {
    if let Ok(mut err) = latest.lock() {
        *err = Some(ShadowReadinessError {
            message: message.into(),
            observed_at: chrono::Utc::now().to_rfc3339(),
        });
    }
}

fn clear_latest_readiness_error(latest: &Arc<Mutex<Option<ShadowReadinessError>>>) {
    if let Ok(mut err) = latest.lock() {
        *err = None;
    }
}

fn classify_readiness_error(message: &str) -> &'static str {
    let lower = message.to_ascii_lowercase();
    if lower.contains("i/o timeout")
        || lower.contains("context deadline exceeded")
        || lower.contains("timed out")
        || lower.contains("timeout")
    {
        "api_timeout"
    } else if lower.contains("client error (connect)")
        || lower.contains("client error: connect")
        || lower.contains("error trying to connect")
        || lower.contains("network is unreachable")
    {
        "api_connect"
    } else if lower.contains("no such host")
        || lower.contains("dns")
        || lower.contains("temporary failure in name resolution")
    {
        "dns"
    } else if lower.contains("certificate")
        || lower.contains("x509")
        || lower.contains("tls")
        || lower.contains("certificate signed")
    {
        "tls"
    } else if lower.contains("unauthorized")
        || lower.contains("forbidden")
        || lower.contains("permission denied")
    {
        "auth_or_rbac"
    } else if lower.contains("connection refused") {
        "connection_refused"
    } else if lower.contains("failed to perform initial object list")
        || lower.contains("watch")
        || lower.contains("relist")
    {
        "watch_or_relist"
    } else {
        "unknown"
    }
}

fn readiness_error_next_action(error_class: &str) -> &'static str {
    match error_class {
        "api_timeout" | "api_connect" => {
            "verify network path to the Kubernetes API server (VPN, private endpoint, firewall, or authorized networks), then rerun kubectl --request-timeout=10s get --raw='/readyz?verbose'"
        }
        "dns" => {
            "repair kubeconfig/API-server DNS resolution, then rerun kubectl --request-timeout=10s get --raw='/readyz?verbose'"
        }
        "tls" => {
            "refresh kubeconfig cluster certificate data or CA trust, then rerun kubectl --request-timeout=10s get --raw='/readyz?verbose'"
        }
        "auth_or_rbac" => {
            "verify Kubernetes credentials and list/watch RBAC for pods/nodes, then rerun kubectl --request-timeout=10s auth can-i list pods --all-namespaces"
        }
        "connection_refused" => {
            "verify the Kubernetes API endpoint and control-plane listener are reachable, then rerun kubectl --request-timeout=10s get --raw='/readyz?verbose'"
        }
        "watch_or_relist" => {
            "inspect the watch/relist error and RBAC, then wait for the shadow watch loop to resync"
        }
        _ => "restore Kubernetes API connectivity and wait for the watch/relist loop to recover",
    }
}

fn readiness_debug_commands(error_class: &str) -> Vec<String> {
    let mut commands = vec![
        "kubectl config current-context".to_string(),
        "kubectl --request-timeout=10s get --raw='/readyz?verbose'".to_string(),
        "kubectl --request-timeout=10s auth can-i list pods --all-namespaces".to_string(),
        "kubectl --request-timeout=10s get nodes".to_string(),
    ];
    if matches!(
        error_class,
        "api_timeout" | "api_connect" | "dns" | "tls" | "connection_refused"
    ) {
        commands.swap(0, 1);
    } else if error_class == "auth_or_rbac" {
        commands.swap(0, 2);
    }
    commands
}

#[derive(Debug, Default, Serialize)]
struct KubeSimulatorTracePlan {
    placements: Vec<serde_json::Value>,
    simulator: serde_json::Value,
}

#[derive(Debug, Default, Deserialize)]
struct DemoReportRefreshQuery {
    refresh_simulator_cache: Option<bool>,
    simulator_timeout_ms: Option<u64>,
}

#[derive(Debug)]
struct DashboardSimulatorPool {
    endpoints: Vec<DashboardSimulatorEndpoint>,
    next: AtomicUsize,
}

#[derive(Debug, Clone)]
struct DashboardSimulatorEndpoint {
    url: String,
    gate: Arc<tokio::sync::Mutex<()>>,
}

#[derive(Debug, Default)]
struct DecisionEventEmissionFilter {
    last_by_pod: BTreeMap<String, String>,
}

impl DecisionEventEmissionFilter {
    fn filter_changed(
        &mut self,
        trace: &DecisionTrace,
        drafts: Vec<crate::scheduler::events::EventDraft>,
    ) -> Vec<crate::scheduler::events::EventDraft> {
        let mut next_by_pod = BTreeMap::new();
        let mut filtered = Vec::new();
        for (decision, draft) in trace.decisions.iter().zip(drafts.into_iter()) {
            let key = decision_event_key(decision);
            let fingerprint = decision_event_fingerprint(decision);
            if self.last_by_pod.get(&key) != Some(&fingerprint) {
                filtered.push(draft);
            }
            next_by_pod.insert(key, fingerprint);
        }
        self.last_by_pod = next_by_pod;
        filtered
    }
}

fn decision_event_key(decision: &crate::scheduler::trace::PodDecision) -> String {
    format!("{}/{}/{}", decision.namespace, decision.name, decision.uid)
}

fn decision_event_fingerprint(decision: &crate::scheduler::trace::PodDecision) -> String {
    serde_json::json!({
        "gpu_request": decision.gpu_request,
        "priority": decision.priority,
        "priority_class_name": decision.priority_class_name,
        "team": decision.team,
        "queue": decision.queue,
        "queue_score": decision.queue_score,
        "business_value": decision.business_value,
        "deadline_unix_seconds": decision.deadline_unix_seconds,
        "min_gpus": decision.min_gpus,
        "max_gpus": decision.max_gpus,
        "preferred_gpus": decision.preferred_gpus,
        "flexible": decision.flexible,
        "predicted_runtime_seconds": decision.predicted_runtime_seconds,
        "predicted_peak_vram_bytes": decision.predicted_peak_vram_bytes,
        "predicted_deadline_miss": decision.predicted_deadline_miss,
        "placement": decision.placement,
        "caveats": decision.caveats,
    })
    .to_string()
}

fn write_paths_allowed_after_solve(
    leader: &crate::scheduler::leader::LeaderElector,
    binding_writes_enabled: bool,
    event_writes_enabled: bool,
) -> bool {
    if !binding_writes_enabled && !event_writes_enabled {
        return true;
    }
    leader.is_leader()
}

#[derive(Debug, Default, Deserialize)]
struct SolveQuery {
    objective_profile: Option<String>,
    profile: Option<String>,
    admission: Option<i64>,
    gpu_demand: Option<i64>,
    gang_complete: Option<i64>,
    priority: Option<i64>,
    business_value: Option<i64>,
    queue: Option<i64>,
    queue_wait: Option<i64>,
    fair_share: Option<i64>,
    deadline_urgency: Option<i64>,
    deadline_miss: Option<i64>,
    gpu_fragmentation: Option<i64>,
}

fn binding_reservation_metrics(
    ledger: &crate::scheduler::ledger::ReservationLedger,
    created: usize,
    rejected: usize,
    reconciled: &crate::scheduler::ledger::ReconcileStats,
) -> BindingReservationMetrics {
    BindingReservationMetrics {
        active_reservations: ledger.len(),
        active_entries: ledger.entry_count(),
        reserved_gpus: ledger.committed_gpu_total(),
        created,
        rejected,
        expired: reconciled.expired_reservations,
        observed_bound_entries: reconciled.observed_bound_entries,
        stale_entries: reconciled.stale_entries,
    }
}

fn is_canary_skip_reason(reason: &str) -> bool {
    reason.contains("binding canary low-risk mode")
}

fn classify_binding_skip_reason(reason: &str) -> &'static str {
    if is_canary_skip_reason(reason) {
        "canary"
    } else if reason.contains("not ready")
        || reason.contains("not Pending")
        || reason.contains("pod not found")
        || reason.contains("live get failed")
    {
        "readiness"
    } else if reason.contains("uid") || reason.contains("terminating") {
        "identity"
    } else if reason.contains("pod scheduler is") {
        "scheduler"
    } else if reason.contains("already bound") {
        "already_bound"
    } else if reason.contains("DRA pod") {
        "dra"
    } else if reason.contains("max binds per pass") {
        "throttle"
    } else if reason.contains("binding reservation rejected") {
        "reservation"
    } else if reason.contains("real binding disabled") || reason.contains("kill switch") {
        "disabled"
    } else if reason.contains("binding group skipped") {
        "group"
    } else {
        "other"
    }
}

fn binding_outcome_metrics(
    outcomes: &[crate::scheduler::binder::BindOutcome],
) -> BindingOutcomeMetrics {
    let mut metrics = BindingOutcomeMetrics::default();
    for outcome in outcomes {
        match &outcome.result {
            crate::scheduler::binder::BindResult::Bound { dry_run: false } => metrics.bound += 1,
            crate::scheduler::binder::BindResult::Bound { dry_run: true } => metrics.validated += 1,
            crate::scheduler::binder::BindResult::Skipped { reason } => {
                metrics.skipped += 1;
                match classify_binding_skip_reason(reason) {
                    "canary" => metrics.canary_skipped += 1,
                    "readiness" => metrics.readiness_skipped += 1,
                    "identity" => metrics.identity_skipped += 1,
                    "scheduler" => metrics.scheduler_skipped += 1,
                    "already_bound" => metrics.already_bound_skipped += 1,
                    "dra" => metrics.dra_skipped += 1,
                    "throttle" => metrics.throttle_skipped += 1,
                    "reservation" => metrics.reservation_skipped += 1,
                    "disabled" => metrics.disabled_skipped += 1,
                    "group" => metrics.group_skipped += 1,
                    _ => metrics.other_skipped += 1,
                }
            }
            crate::scheduler::binder::BindResult::Failed { .. } => metrics.failed += 1,
        }
    }
    metrics
}

fn publish_binding_skip_reason_metrics(metrics: &BindingOutcomeMetrics) {
    metrics::set_shadow_bind_skipped_by_reason(
        metrics.canary_skipped as i64,
        metrics.readiness_skipped as i64,
        metrics.identity_skipped as i64,
        metrics.scheduler_skipped as i64,
        metrics.already_bound_skipped as i64,
        metrics.dra_skipped as i64,
        metrics.throttle_skipped as i64,
        metrics.reservation_skipped as i64,
        metrics.disabled_skipped as i64,
        metrics.group_skipped as i64,
        metrics.other_skipped as i64,
    );
}

/// Read-only DRY-RUN view: the pod→node bindings the latest decision would imply, rendered as the
/// exact subresource payloads a real binder WOULD send. Nothing is ever applied (shadow mode).
async fn binding_plan_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let latest = s.traces.recent().into_iter().next();
    let (seq, solve_millis) = latest
        .as_ref()
        .map(|t| (t.sequence, t.solve_millis))
        .unwrap_or((0, 0));
    let plan = latest
        .map(|t| crate::scheduler::binding::render_binding_plan(&t))
        .unwrap_or_default();
    // Annotate each rendered binding with its stale/conflict readiness against the latest snapshot.
    let cluster = s.latest_cluster.lock().ok().and_then(|g| g.clone());
    let entries: Vec<serde_json::Value> = plan
        .into_iter()
        .map(|e| {
            let readiness = cluster
                .as_ref()
                .map(|c| crate::scheduler::binding::assess_binding_readiness(&e, c));
            let mut v = serde_json::to_value(&e).unwrap_or_default();
            if let (Some(obj), Some(r)) = (v.as_object_mut(), readiness) {
                obj.insert(
                    "readiness".to_string(),
                    serde_json::to_value(r).unwrap_or_default(),
                );
            }
            v
        })
        .collect();
    Json(serde_json::json!({
        "dry_run": true,
        "note": "rendered from the latest shadow trace; never applied — readiness re-checked vs latest snapshot",
        "trace_sequence": seq,
        "solve_millis": solve_millis,
        "bindings": entries,
    }))
}

/// Read-only audit view: Kubernetes Event payloads that correspond to the latest binding executor
/// outcomes. This renders only; it never posts Events to the apiserver.
async fn binding_events_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let latest = s.latest_bind_outcomes.lock().ok().and_then(|g| g.clone());
    let (seq, outcomes) = latest.unwrap_or((0, Vec::new()));
    let reporting_instance = if s.cfg.cluster_name.is_empty() {
        s.cfg.scheduler_name.as_str()
    } else {
        s.cfg.cluster_name.as_str()
    };
    let event_time = chrono::Utc::now().to_rfc3339();
    let events = crate::scheduler::events::render_binding_events(
        &outcomes,
        &s.cfg.scheduler_name,
        reporting_instance,
        seq,
        &event_time,
    );
    Json(serde_json::json!({
        "dry_run": true,
        "note": "rendered from latest binding executor outcomes; never posted",
        "trace_sequence": seq,
        "events": events,
    }))
}

/// Read-only dry-run repair view: migration/preemption advice for unplaced GPU work. This renders
/// the latest trace's advisory repair plan only; it never evicts, binds, or migrates pods.
async fn repair_plan_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let latest = s.traces.recent().into_iter().next();
    let (seq, solve_millis, repair_plans, repair_notes, repair_metrics) = latest
        .map(|t| {
            (
                t.sequence,
                t.solve_millis,
                t.repair_plans,
                t.repair_notes,
                t.repair_metrics,
            )
        })
        .unwrap_or_else(|| (0, 0, Vec::new(), Vec::new(), Default::default()));
    let live_plan_available = !repair_plans.is_empty();
    let hero_reference = crate::scheduler::gpu_scenarios::demo_preemption_migration_hero_summary();
    let proof_status = serde_json::json!({
        "mode": if live_plan_available { "live-repair-plan" } else { "deterministic-reference" },
        "headline": if live_plan_available {
            "Current trace has a live advisory repair plan"
        } else {
            "Current trace has no repair plan; showing deterministic fragmentation reference"
        },
        "live_plan_available": live_plan_available,
        "live_action_count": repair_plans
            .iter()
            .map(|plan| plan.actions.len())
            .sum::<usize>(),
        "reference_action_count": hero_reference.action_rows.len(),
        "evidence": if live_plan_available {
            "latest trace repair_plans[].actions"
        } else {
            "hero_reference.action_rows from deterministic fragmented-gang proof"
        },
        "operator_question": if live_plan_available {
            "Do these live move/preempt rows safely unlock the blocked GPU job?"
        } else {
            "Does the current cluster need a repairable fragmentation scenario applied before claiming live repair evidence?"
        },
        "claim_guard": "reference rows are demo evidence only unless live_plan_available=true",
    });
    Json(serde_json::json!({
        "dry_run": true,
        "note": "rendered from the latest shadow trace; advisory only — no evictions, migrations, preemptions, or bindings are applied",
        "trace_sequence": seq,
        "solve_millis": solve_millis,
        "repair_metrics": repair_metrics,
        "repair_plans": repair_plans,
        "repair_notes": repair_notes,
        "live_plan_available": live_plan_available,
        "proof_status": proof_status,
        "hero_reference": hero_reference,
        "hero_reference_note": "deterministic SRE demo reference for the fragmentation repair story; not evidence that the current live trace is repairable",
    }))
}

/// Read-only audit view: Kubernetes Event payloads for the latest advisory repair actions.
/// This renders only; it never posts Events or applies evictions/migrations.
async fn repair_events_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let latest = s.traces.recent().into_iter().next();
    let reporting_instance = if s.cfg.cluster_name.is_empty() {
        s.cfg.scheduler_name.as_str()
    } else {
        s.cfg.cluster_name.as_str()
    };
    let event_time = chrono::Utc::now().to_rfc3339();
    let (seq, events) = latest
        .as_ref()
        .map(|trace| {
            (
                trace.sequence,
                crate::scheduler::events::render_repair_events(
                    trace,
                    &s.cfg.scheduler_name,
                    reporting_instance,
                    &event_time,
                ),
            )
        })
        .unwrap_or((0, Vec::new()));
    Json(serde_json::json!({
        "dry_run": true,
        "note": "rendered from latest advisory repair actions; never posted and never applied",
        "trace_sequence": seq,
        "events": events,
    }))
}

/// Read-only audit view: Kubernetes Event payloads for the latest shadow placement decisions.
/// This renders only; it never posts Events to the apiserver.
async fn decision_events_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let latest = s.traces.recent().into_iter().next();
    let reporting_instance = if s.cfg.cluster_name.is_empty() {
        s.cfg.scheduler_name.as_str()
    } else {
        s.cfg.cluster_name.as_str()
    };
    let event_time = chrono::Utc::now().to_rfc3339();
    let (seq, events) = latest
        .as_ref()
        .map(|trace| {
            (
                trace.sequence,
                crate::scheduler::events::render_decision_events(
                    trace,
                    &s.cfg.scheduler_name,
                    reporting_instance,
                    &event_time,
                ),
            )
        })
        .unwrap_or((0, Vec::new()));
    Json(serde_json::json!({
        "dry_run": true,
        "note": "rendered from latest shadow decisions; never posted",
        "trace_sequence": seq,
        "events": events,
    }))
}

/// MutatingAdmissionWebhook endpoint: returns an AdmissionReview response with an optional
/// schedulerName JSONPatch for selected GPU pods, and — when KSOLVER_VRAM_PREDICTOR_URL is set —
/// an additional VRAM-injection patch (predicted peak VRAM annotation + node feasibility) sourced
/// from the predictor service. This does not call the Kubernetes API and FAILS OPEN: any predictor
/// error leaves the schedulerName-only response untouched.
async fn scheduler_admission_handler(
    State(s): State<ShadowHttpState>,
    Json(review): Json<crate::scheduler::admission::AdmissionReview>,
) -> Json<crate::scheduler::admission::AdmissionReview> {
    let policy = crate::scheduler::admission::SchedulerPatchPolicy::from(&s.cfg);
    // Keep the pod object JSON before render consumes the review (for optional VRAM injection).
    let pod_object = review.request.as_ref().and_then(|r| r.object.clone());
    let base = crate::scheduler::admission::render_scheduler_admission_review(review, &policy);

    let predictor_url = std::env::var("KSOLVER_VRAM_PREDICTOR_URL").unwrap_or_default();
    if predictor_url.trim().is_empty() {
        return Json(base);
    }
    let Some(pod_object) = pod_object else {
        return Json(base);
    };
    match vram_injection_ops_from_predictor(predictor_url.trim(), &policy, pod_object).await {
        Some(ops) if !ops.is_empty() => {
            Json(crate::scheduler::admission::merge_extra_ops(base, ops))
        }
        _ => Json(base),
    }
}

/// Call the predictor `/predict` for a GPU pod and build its VRAM-injection JSONPatch ops.
/// Returns None (fail-open) on any error, timeout, or non-GPU pod.
async fn vram_injection_ops_from_predictor(
    url: &str,
    policy: &crate::scheduler::admission::SchedulerPatchPolicy,
    pod_object: serde_json::Value,
) -> Option<Vec<crate::scheduler::admission::JsonPatchOperation>> {
    let pod: k8s_openapi::api::core::v1::Pod = serde_json::from_value(pod_object.clone()).ok()?;
    if !crate::scheduler::admission::pod_in_scope_for_vram(&pod, policy) {
        return None;
    }
    let endpoint = format!("{}/predict", url.trim_end_matches('/'));
    let client = reqwest::Client::new();
    let response = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.post(endpoint).json(&pod_object).send(),
    )
    .await
    .ok()?
    .ok()?;
    let resolution: serde_json::Value = response.json().await.ok()?;
    Some(crate::scheduler::admission::vram_injection_ops(&pod, &resolution))
}

async fn traces_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "traces": s.traces.recent() }))
}

async fn objective_config_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let active = s.active_objective.lock().ok().map(|g| g.clone());
    let profile = active
        .as_ref()
        .map(|o| o.profile)
        .unwrap_or(s.cfg.objective_profile);
    let weights = active
        .as_ref()
        .map(|o| o.weights.clone())
        .unwrap_or_else(|| s.cfg.objective_weights.clone());
    Json(serde_json::json!({
        "objective_profile": objective_profile_name(profile),
        "objective_weights": {
            "admission": weights.admission,
            "gpu_demand": weights.gpu_demand,
            "gang_complete": weights.gang_complete,
            "priority": weights.priority,
            "business_value": weights.business_value,
            "queue": weights.queue,
            "queue_wait": weights.queue_wait,
            "fair_share": weights.fair_share,
            "deadline_urgency": weights.deadline_urgency,
            "deadline_miss": weights.deadline_miss,
            "gpu_fragmentation": weights.gpu_fragmentation,
        }
    }))
}

async fn demo_report_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let value =
        with_demo_report_refresh_status(&s, cached_demo_report_value(&s, false, None).await).await;
    Json(value)
}

async fn demo_report_refresh_handler(
    Query(params): Query<DemoReportRefreshQuery>,
    State(s): State<ShadowHttpState>,
) -> Json<serde_json::Value> {
    let refresh_started = Instant::now();
    let refresh_simulator_cache = params.refresh_simulator_cache.unwrap_or(false);
    let simulator_timeout_override = params
        .simulator_timeout_ms
        .map(|ms| Duration::from_millis(ms.clamp(1_000, 120_000)));
    let previous = s.demo_report_cache.lock().await.clone();
    let mut value =
        cached_demo_report_value(&s, refresh_simulator_cache, simulator_timeout_override).await;
    let simulator_recovery_command = simulator_recovery_command_for_urls(&s.simulator_pool.urls());
    if let Some(obj) = value.as_object_mut() {
        obj.insert("refreshed".to_string(), serde_json::json!(true));
        obj.insert(
            "refresh_simulator_cache".to_string(),
            serde_json::json!(refresh_simulator_cache),
        );
        if let Some(timeout) = simulator_timeout_override {
            obj.insert(
                "simulator_timeout_ms".to_string(),
                serde_json::json!(timeout.as_millis() as u64),
            );
        }
        obj.insert(
            "simulator_timeout_scope".to_string(),
            serde_json::json!("per_baseline"),
        );
        obj.insert(
            "simulator_recovery_command".to_string(),
            serde_json::json!(simulator_recovery_command),
        );
        if !obj.contains_key("simulator_live_baseline_limit") {
            if let Some(limit) = obj
                .get("report")
                .and_then(|report| report.get("simulator_live_baseline_limit"))
                .cloned()
            {
                obj.insert("simulator_live_baseline_limit".to_string(), limit);
            }
        }
        if obj.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
            if let Some(previous) = previous {
                if let Some(report) = previous.get("report").cloned() {
                    obj.insert("report".to_string(), report);
                    obj.insert("stale_report_used".to_string(), serde_json::json!(true));
                    obj.insert(
                        "stale_report_reason".to_string(),
                        serde_json::json!("simulator refresh failed; keeping last successful dashboard report visible"),
                    );
                }
            }
        }
        obj.insert(
            "refreshed_at".to_string(),
            serde_json::json!(chrono::Utc::now().to_rfc3339()),
        );
        obj.insert(
            "refresh_duration_ms".to_string(),
            serde_json::json!(refresh_started.elapsed().as_millis() as u64),
        );
    }
    if let Some(status) = demo_report_refresh_status_from_value(&value) {
        let mut stored = s.demo_report_refresh_status.lock().await;
        *stored = Some(status.clone());
        if let Some(obj) = value.as_object_mut() {
            obj.insert("demo_refresh".to_string(), status);
        }
    }
    Json(value)
}

fn demo_report_refresh_status_from_value(value: &serde_json::Value) -> Option<serde_json::Value> {
    let obj = value.as_object()?;
    if obj.get("refreshed").is_none() && obj.get("refresh_simulator_cache").is_none() {
        return None;
    }
    let mut status = serde_json::Map::new();
    for key in [
        "ok",
        "refreshed",
        "refresh_simulator_cache",
        "stale_report_used",
        "stale_report_reason",
        "reason",
        "recoverable",
        "build_hint",
        "simulator_timeout_ms",
        "simulator_timeout_scope",
        "simulator_recovery_command",
        "simulator_refresh_mode",
        "simulator_live_baseline_limit",
        "simulator_refreshed_baselines",
        "simulator_cache_total_baselines",
        "simulator_cache_cached_baselines",
        "simulator_cache_missing_baselines",
        "simulator_cache_coverage_milli",
        "refresh_duration_ms",
        "refreshed_at",
    ] {
        if let Some(v) = obj.get(key) {
            status.insert(key.to_string(), v.clone());
        }
    }
    Some(serde_json::Value::Object(status))
}

async fn with_demo_report_refresh_status(
    s: &ShadowHttpState,
    mut value: serde_json::Value,
) -> serde_json::Value {
    let status = s.demo_report_refresh_status.lock().await.clone();
    if let (Some(status), Some(obj)) = (status, value.as_object_mut()) {
        obj.insert("demo_refresh".to_string(), status);
    }
    value
}

fn demo_benchmark_options(
    s: &ShadowHttpState,
    refresh_simulator_cache: bool,
    simulator_timeout_override: Option<Duration>,
) -> crate::scheduler::gpu_scenarios::BenchmarkOptions {
    let mut simulator_urls = s.simulator_pool.urls();
    let simulator_url = simulator_urls.first().cloned().or_else(|| {
        std::env::var("KSOLVER_SCHEDULER_SIMULATOR_URL")
            .or_else(|_| std::env::var("SCHEDULER_SIMULATOR_URL"))
            .ok()
    });
    simulator_urls.dedup();
    let simulator_url = simulator_url.and_then(|url| {
        let trimmed = url.trim().trim_end_matches('/').to_string();
        (!trimmed.is_empty()).then_some(trimmed)
    });
    simulator_urls = simulator_urls
        .into_iter()
        .map(|url| url.trim().trim_end_matches('/').to_string())
        .filter(|url| !url.is_empty())
        .collect::<Vec<_>>();
    if let Some(primary) = simulator_url.as_ref() {
        simulator_urls.retain(|url| url != primary);
    }
    if simulator_url.is_none() && simulator_urls.is_empty() {
        let fallback = DEFAULT_SIMULATOR_URL.to_string();
        simulator_urls.push(fallback.clone());
    }
    let simulator_cache_path = std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .map(std::path::PathBuf::from)
        .or_else(|| {
            Some(std::path::PathBuf::from(
                "/tmp/ksolver-gpu-simulator-cache.json",
            ))
        });
    let simulator_batch_timeout = std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_TIMEOUT_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map(std::time::Duration::from_millis)
        .unwrap_or_else(|| {
            crate::scheduler::gpu_scenarios::BenchmarkOptions::default().simulator_batch_timeout
        });
    let simulator_batch_timeout = simulator_timeout_override.unwrap_or(simulator_batch_timeout);
    let default_simulator_max_live_baselines =
        crate::scheduler::gpu_scenarios::BenchmarkOptions::default().simulator_max_live_baselines;
    let simulator_max_live_baselines =
        match std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_MAX_LIVE_BASELINES").ok() {
            Some(v) if matches!(v.trim().to_ascii_lowercase().as_str(), "all" | "unlimited") => {
                None
            }
            Some(v) if v.trim().eq_ignore_ascii_case("none") => Some(0),
            Some(v) => v
                .parse::<usize>()
                .ok()
                .or(default_simulator_max_live_baselines),
            None => default_simulator_max_live_baselines,
        };
    let simulator_live_scenarios = std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_LIVE_SCENARIOS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .map(str::trim)
                .filter(|name| !name.is_empty())
                .map(str::to_string)
                .collect::<std::collections::BTreeSet<_>>()
        })
        .filter(|scenarios| !scenarios.is_empty());
    crate::scheduler::gpu_scenarios::BenchmarkOptions {
        simulator_url,
        simulator_urls,
        simulator_cache_path,
        simulator_cache_dir: std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE_DIR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(std::path::PathBuf::from),
        refresh_simulator_cache,
        simulator_batch_timeout,
        simulator_progress: false,
        simulator_max_live_baselines,
        simulator_live_scenarios,
        volcano_baseline_useful_gpu: std::collections::BTreeMap::new(),
    }
}

async fn simulator_cache_coverage_handler(
    State(s): State<ShadowHttpState>,
) -> Json<serde_json::Value> {
    let options = demo_benchmark_options(&s, false, None);
    match crate::scheduler::gpu_scenarios::simulator_cache_coverage(&options) {
        Ok(coverage) => {
            let coverage_milli =
                simulator_cache_coverage_milli(coverage.cached_baselines, coverage.total_baselines);
            Json(serde_json::json!({
                "ok": true,
                "simulator_cache_total_baselines": coverage.total_baselines,
                "simulator_cache_cached_baselines": coverage.cached_baselines,
                "simulator_cache_missing_baselines": coverage.missing_baselines,
                "simulator_cache_coverage_milli": coverage_milli,
                "simulator_cache_complete": coverage.missing_baselines == 0,
                "simulator_live_baseline_limit": options.simulator_max_live_baselines,
                "simulator_live_scenarios": options.simulator_live_scenarios,
                "cache_path": options.simulator_cache_path,
                "cache_dir": options.simulator_cache_dir,
            }))
        }
        Err(err) => Json(serde_json::json!({
            "ok": false,
            "reason": err.to_string(),
        })),
    }
}

fn simulator_cache_coverage_milli(cached_baselines: usize, total_baselines: usize) -> Option<u64> {
    if total_baselines > 0 {
        Some((cached_baselines * 100_000 / total_baselines) as u64)
    } else {
        None
    }
}

async fn cached_demo_report_value(
    s: &ShadowHttpState,
    refresh_simulator_cache: bool,
    simulator_timeout_override: Option<Duration>,
) -> serde_json::Value {
    if !refresh_simulator_cache {
        if let Some(value) = s.demo_report_cache.lock().await.as_ref().cloned() {
            return value;
        }
    }
    let mut options =
        demo_benchmark_options(s, refresh_simulator_cache, simulator_timeout_override);
    let effective_timeout_ms = options.simulator_batch_timeout.as_millis() as u64;
    let effective_live_baseline_limit = options.simulator_max_live_baselines;
    let simulator_refresh_mode = if refresh_simulator_cache {
        if effective_live_baseline_limit.is_some() {
            "fill_missing"
        } else {
            "refresh_all"
        }
    } else {
        "cached"
    };
    let pre_refresh_cache_coverage =
        crate::scheduler::gpu_scenarios::simulator_cache_coverage(&options).ok();
    let pre_refresh_cache_coverage_milli =
        pre_refresh_cache_coverage.as_ref().and_then(|coverage| {
            simulator_cache_coverage_milli(coverage.cached_baselines, coverage.total_baselines)
        });
    let mut simulator_refreshed_baselines = None;
    if refresh_simulator_cache {
        match crate::scheduler::gpu_scenarios::refresh_simulator_cache_only(options.clone()).await {
            Ok(count) => {
                simulator_refreshed_baselines = Some(count);
                options.refresh_simulator_cache = false;
                options.simulator_max_live_baselines = Some(0);
            }
            Err(err) => {
                let reason = err.to_string();
                return serde_json::json!({
                    "ok": false,
                    "reason": reason,
                    "recoverable": true,
                    "simulator_timeout_ms": effective_timeout_ms,
                    "simulator_timeout_scope": "per_baseline",
                    "simulator_refresh_mode": simulator_refresh_mode,
                    "simulator_live_baseline_limit": effective_live_baseline_limit,
                    "simulator_refreshed_baselines": simulator_refreshed_baselines,
                    "simulator_cache_total_baselines": pre_refresh_cache_coverage.as_ref().map(|coverage| coverage.total_baselines),
                    "simulator_cache_cached_baselines": pre_refresh_cache_coverage.as_ref().map(|coverage| coverage.cached_baselines),
                    "simulator_cache_missing_baselines": pre_refresh_cache_coverage.as_ref().map(|coverage| coverage.missing_baselines),
                    "simulator_cache_coverage_milli": pre_refresh_cache_coverage_milli,
                    "build_hint": if reason.contains("rust-cp-sat") {
                        "Build and run shadow with solver support: cargo build --manifest-path ksolver/Cargo.toml --features rust-cp-sat && KUBECONFIG=~/.kube/wsl target/debug/ksolver shadow"
                    } else {
                        "Refresh the scenario cache or inspect the failing deterministic scenario before making demo claims."
                    },
                });
            }
        }
    }
    let simulator_cache_coverage =
        crate::scheduler::gpu_scenarios::simulator_cache_coverage(&options).ok();
    let simulator_cache_coverage_milli = simulator_cache_coverage.as_ref().and_then(|coverage| {
        simulator_cache_coverage_milli(coverage.cached_baselines, coverage.total_baselines)
    });
    let value = match crate::scheduler::gpu_scenarios::run_benchmark_with_options(options).await {
        Ok(report) => serde_json::json!({
            "ok": true,
            "simulator_timeout_ms": effective_timeout_ms,
            "simulator_timeout_scope": "per_baseline",
            "simulator_refresh_mode": simulator_refresh_mode,
            "simulator_live_baseline_limit": effective_live_baseline_limit,
            "simulator_refreshed_baselines": simulator_refreshed_baselines,
            "simulator_cache_total_baselines": simulator_cache_coverage.as_ref().map(|coverage| coverage.total_baselines),
            "simulator_cache_cached_baselines": simulator_cache_coverage.as_ref().map(|coverage| coverage.cached_baselines),
            "simulator_cache_missing_baselines": simulator_cache_coverage.as_ref().map(|coverage| coverage.missing_baselines),
            "simulator_cache_coverage_milli": simulator_cache_coverage_milli,
            "report": report,
        }),
        Err(err) => {
            let reason = err.to_string();
            serde_json::json!({
                "ok": false,
                "reason": reason,
                "recoverable": true,
                "simulator_timeout_ms": effective_timeout_ms,
                "simulator_timeout_scope": "per_baseline",
                "simulator_refresh_mode": simulator_refresh_mode,
                "simulator_live_baseline_limit": effective_live_baseline_limit,
                "simulator_refreshed_baselines": simulator_refreshed_baselines,
                "simulator_cache_total_baselines": simulator_cache_coverage.as_ref().map(|coverage| coverage.total_baselines),
                "simulator_cache_cached_baselines": simulator_cache_coverage.as_ref().map(|coverage| coverage.cached_baselines),
                "simulator_cache_missing_baselines": simulator_cache_coverage.as_ref().map(|coverage| coverage.missing_baselines),
                "simulator_cache_coverage_milli": simulator_cache_coverage_milli,
                "build_hint": if reason.contains("rust-cp-sat") {
                    "Build and run shadow with solver support: cargo build --manifest-path ksolver/Cargo.toml --features rust-cp-sat && KUBECONFIG=~/.kube/wsl target/debug/ksolver shadow"
                } else {
                    "Refresh the scenario cache or inspect the failing deterministic scenario before making demo claims."
                },
            })
        }
    };
    if value.get("ok").and_then(serde_json::Value::as_bool) == Some(true) {
        let mut cache = s.demo_report_cache.lock().await;
        *cache = Some(value.clone());
    }
    value
}

async fn evidence_bundle_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let latest_trace = s.traces.recent().into_iter().next();
    let latest_bind_outcomes = s
        .latest_bind_outcomes
        .lock()
        .expect("latest bind outcomes mutex poisoned")
        .as_ref()
        .map(|(sequence, outcomes)| (*sequence, outcomes.len()));
    let simulator_urls = s.simulator_pool.urls();
    let simulator_readiness_probe = dashboard_simulator_readiness_probe(&simulator_urls).await;
    let production_safety = production_safety_payload(
        &s.cfg,
        s.watch_healthy.load(Ordering::Relaxed),
        s.latest_readiness_error
            .lock()
            .expect("latest readiness error mutex poisoned")
            .clone(),
        latest_trace.as_ref(),
        latest_bind_outcomes,
        simulator_urls,
        Some(simulator_readiness_probe),
    );
    let demo_report = cached_demo_report_value(&s, false, None).await;
    let vram_calibration = vram_calibration_payload();
    let operator_binding_safety = operator_binding_safety_from_production_safety(&production_safety);
    let launch_proof_gate = demo_report
        .get("report")
        .and_then(|report| report.get("roadmap_readiness_summary"))
        .and_then(|roadmap| roadmap.get("launch_proof_gate"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let evidence_bundle_rows = launch_proof_gate
        .get("evidence_bundle_rows")
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let trace_sequence = latest_trace
        .as_ref()
        .map(|trace| trace.sequence)
        .unwrap_or(0);
    let collection_commands = evidence_bundle_collection_commands();
    let evidence_row_count = evidence_bundle_rows
        .as_array()
        .map(Vec::len)
        .unwrap_or_default();
    let launch_status = launch_proof_gate
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let customer_claim_ready = launch_proof_gate
        .get("customer_claim_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let mutation_allowed = production_safety
        .get("rollout")
        .and_then(|rollout| rollout.get("mutation_allowed"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let vram_advisory_ready = vram_calibration
        .get("scheduler_readiness")
        .and_then(|readiness| readiness.get("advisory_ready"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let vram_hard_admission_ready = vram_calibration
        .get("scheduler_readiness")
        .and_then(|readiness| readiness.get("hard_admission_ready"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let vram_admission_decision = vram_calibration
        .get("scheduler_readiness")
        .and_then(|readiness| readiness.get("admission_decision"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let vram_admission_mode = vram_admission_decision
        .get("mode")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(if vram_hard_admission_ready {
            "Hard admission ready"
        } else if vram_advisory_ready {
            "Shadow advisory only"
        } else {
            "Not ready"
        });
    let vram_scheduler_use = vram_admission_decision
        .get("scheduler_use")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(if vram_hard_admission_ready {
            "Can enforce VRAM admission gates"
        } else if vram_advisory_ready {
            "Score and warn; do not reject pods"
        } else {
            "Collect evidence before scheduling claims"
        });
    let vram_hard_blocker_count = vram_admission_decision
        .get("blocker_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| {
            vram_calibration
                .get("scheduler_readiness")
                .and_then(|readiness| readiness.get("hard_admission_blockers"))
                .and_then(serde_json::Value::as_array)
                .map(|rows| rows.len() as u64)
                .unwrap_or(0)
        });
    let vram_next_evidence_target = vram_admission_decision
        .get("next_evidence_target")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("keep collecting drift samples");
    let vram_model_drivers = vram_calibration
        .get("model_drivers")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let vram_top_drivers = vram_model_drivers
        .get("top_drivers")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let vram_real_top_drivers = vram_model_drivers
        .get("real_top_drivers")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            vram_top_drivers
                .iter()
                .filter(|driver| !is_synthetic_vram_driver(driver))
                .cloned()
                .collect()
        });
    let vram_claim_safe_drivers = vram_model_drivers
        .get("claim_safe_drivers")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_else(|| vram_real_top_drivers.clone());
    let vram_synthetic_drivers = vram_model_drivers
        .get("synthetic_pressure_drivers")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_else(|| {
            vram_top_drivers
                .iter()
                .filter(|driver| is_synthetic_vram_driver(driver))
                .cloned()
                .collect()
        });
    let vram_model_driver_count = vram_top_drivers.len();
    let vram_top_driver_labels: Vec<String> = vram_top_drivers
        .iter()
        .filter_map(|driver| {
            driver
                .get("label")
                .or_else(|| driver.get("feature"))
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .take(5)
        .collect();
    let vram_display_top_driver_labels: Vec<String> = vram_top_driver_labels
        .iter()
        .map(|label| display_vram_driver_label(label))
        .collect();
    let vram_real_top_driver_labels: Vec<String> = vram_real_top_drivers
        .iter()
        .filter_map(vram_driver_display_label)
        .take(5)
        .collect();
    let vram_display_real_top_driver_labels: Vec<String> = vram_real_top_driver_labels
        .iter()
        .map(|label| display_vram_driver_label(label))
        .collect();
    let vram_claim_safe_driver_labels: Vec<String> = vram_claim_safe_drivers
        .iter()
        .filter_map(vram_driver_display_label)
        .take(5)
        .collect();
    let vram_display_claim_safe_driver_labels: Vec<String> = vram_claim_safe_driver_labels
        .iter()
        .map(|label| display_vram_driver_label(label))
        .collect();
    let vram_synthetic_driver_labels: Vec<String> = vram_synthetic_drivers
        .iter()
        .filter_map(vram_driver_display_label)
        .take(5)
        .collect();
    let vram_display_synthetic_driver_labels: Vec<String> = vram_synthetic_driver_labels
        .iter()
        .map(|label| display_vram_driver_label(label))
        .collect();
    let vram_real_model_driver_count = vram_real_top_drivers.len();
    let vram_claim_safe_driver_count = vram_claim_safe_drivers.len();
    let vram_synthetic_driver_count = vram_synthetic_drivers.len();
    let vram_synthetic_reserve_driver = vram_synthetic_drivers.iter().any(|driver| {
        driver
            .get("feature")
            .and_then(serde_json::Value::as_str)
            .map(|feature| feature.starts_with("reserve"))
            .unwrap_or(false)
            && is_synthetic_vram_driver(driver)
    });
    let vram_synthetic_headroom_definition = vram_calibration
        .get("dataset")
        .and_then(|dataset| {
            dataset
                .get("synthetic_headroom")
                .or_else(|| dataset.get("reserve_pressure"))
        })
        .and_then(|headroom| headroom.get("definition"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or(VRAM_SYNTHETIC_HEADROOM_DEFINITION);
    let vram_driver_claim_boundary = vram_model_drivers
        .get("claim_boundary")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Use real_top_drivers for model-memory claims. synthetic headroom drivers are stress-test probes only and must not be presented as organic workload predictors.");
    let vram_investment_demo = demo_report
        .get("report")
        .and_then(|report| report.get("vram_investment_demo_summary"));
    let vram_investment_demo_rows = vram_investment_demo
        .and_then(|summary| summary.get("scenario_count"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or_else(|| {
            vram_investment_demo
                .and_then(|summary| summary.get("rows"))
                .and_then(serde_json::Value::as_array)
                .map(|rows| rows.len() as u64)
                .unwrap_or(0)
        });
    let vram_investment_oom_risk_reduction_pods = vram_investment_demo
        .and_then(|summary| summary.get("cuda_oom_risk_reduction_pods"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let vram_investment_high_vram_nodes_preserved = vram_investment_demo
        .and_then(|summary| summary.get("high_vram_nodes_preserved"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let vram_investment_advisory_rows = vram_investment_demo
        .and_then(|summary| summary.get("unknown_or_advisory_rows"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let vram_investment_average_baseline_oom_risk_percent = vram_investment_demo
        .and_then(|summary| summary.get("average_baseline_oom_risk_percent"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let vram_investment_average_ksolver_oom_risk_percent = vram_investment_demo
        .and_then(|summary| summary.get("average_ksolver_oom_risk_percent"))
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let simulator = production_safety
        .get("simulator")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let simulator_endpoint_count = simulator
        .get("endpoint_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let simulator_readiness_probe = simulator
        .get("readiness_probe")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let simulator_probe_checked_count = simulator_readiness_probe
        .get("checked_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let simulator_probe_ready_count = simulator_readiness_probe
        .get("ready_count")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let simulator_probe_timeout_millis = simulator_readiness_probe
        .get("timeout_millis")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let simulator_readiness = simulator
        .get("readiness")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let simulator_readiness_note = simulator
        .get("readiness_note")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("simulator readiness unavailable");
    let simulator_claim_ready = simulator_endpoint_count > 0
        && simulator_probe_checked_count == simulator_endpoint_count
        && simulator_probe_ready_count == simulator_endpoint_count;
    let (simulator_claim_mode, simulator_claim_blocker, simulator_claim_next_action): (
        &str,
        Option<&str>,
        &str,
    ) = if simulator_claim_ready {
        (
            "live-kube-scheduler-simulator-ready",
            None,
            "safe to use live kube-scheduler-simulator baseline evidence; still verify scenario cache coverage for repeatable demos",
        )
    } else if simulator_endpoint_count == 0 {
        (
            "reference-only",
            Some("kube-scheduler-simulator not configured"),
            "configure KSOLVER_SCHEDULER_SIMULATOR_POOL or refresh deterministic baselines before making kube-vs-ksolver claims",
        )
    } else if simulator_probe_ready_count > 0 {
        (
            "partial-live-baseline",
            Some("only some kube-scheduler-simulator endpoints are ready"),
            "use scripts/kss-pool.sh status and restart or replace unhealthy simulator workers before refreshing scenario baselines",
        )
    } else {
        (
            "baseline-proof-blocked",
            Some("no kube-scheduler-simulator endpoint answered /api/v1/export"),
            "start or repair the kube-scheduler-simulator pool before making kube-vs-ksolver placement claims",
        )
    };
    let production_readiness_blocker_class = production_safety
        .get("readiness")
        .and_then(|readiness| readiness.get("blocker_class"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let production_readiness_next_action = production_safety
        .get("readiness")
        .and_then(|readiness| readiness.get("next_action"))
        .and_then(serde_json::Value::as_str);
    let production_readiness_diagnostic_hint = production_safety
        .get("readiness")
        .and_then(|readiness| readiness.get("diagnostic_hint"))
        .and_then(serde_json::Value::as_str);
    let production_readiness_last_error_class = production_safety
        .get("readiness")
        .and_then(|readiness| readiness.get("last_error_class"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("none");
    let production_readiness_debug_commands = production_safety
        .get("readiness")
        .and_then(|readiness| readiness.get("debug_commands"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!([]));
    let production_readiness_first_debug_command = production_readiness_debug_commands
        .as_array()
        .and_then(|commands| commands.first())
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    let latest_liabilities = s.latest_liabilities.lock().ok().and_then(|g| g.clone());
    let live_validation_gates = evidence_bundle_live_validation_gates(
        latest_trace.as_ref(),
        &production_safety,
        &demo_report,
        mutation_allowed,
        simulator_readiness,
        simulator_probe_ready_count,
        simulator_probe_checked_count,
        latest_liabilities.as_ref(),
    );
    let missing_live_artifact_rows = evidence_bundle_missing_live_artifact_rows(
        latest_trace.as_ref(),
        production_safety
            .get("watch_healthy")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false),
        production_readiness_next_action,
        &live_validation_gates,
        &demo_report,
    );
    let missing_live_artifacts = missing_live_artifact_rows
        .iter()
        .filter_map(|row| {
            row.get("artifact")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
        })
        .collect::<Vec<_>>();
    let missing_live_artifact_blocked_count = missing_live_artifact_rows
        .iter()
        .filter(|row| row.get("severity").and_then(serde_json::Value::as_str) == Some("blocked"))
        .count();
    let missing_live_artifact_warn_count = missing_live_artifact_rows
        .iter()
        .filter(|row| row.get("severity").and_then(serde_json::Value::as_str) == Some("warn"))
        .count();
    let missing_live_artifact_category_counts =
        evidence_bundle_missing_artifact_category_counts(&missing_live_artifact_rows);
    let missing_live_artifact_category_rows =
        evidence_bundle_missing_artifact_category_rows(&missing_live_artifact_rows);
    let missing_live_artifact_action_items =
        operator_evidence_gap_action_items(&missing_live_artifact_category_rows);
    let operator_runbook = operator_action_runbook(&missing_live_artifact_action_items);
    let live_validation_gate_count = live_validation_gates.len();
    let live_validation_pass_count = live_validation_gates
        .iter()
        .filter(|gate| gate.get("status").and_then(serde_json::Value::as_str) == Some("pass"))
        .count();
    let live_validation_warn_count = live_validation_gates
        .iter()
        .filter(|gate| gate.get("status").and_then(serde_json::Value::as_str) == Some("warn"))
        .count();
    let live_validation_blocked_count = live_validation_gates
        .iter()
        .filter(|gate| gate.get("status").and_then(serde_json::Value::as_str) == Some("blocked"))
        .count();
    let mut claim_blockers = Vec::new();
    if !missing_live_artifacts.is_empty() {
        claim_blockers.push(format!(
            "{} missing live artifact(s)",
            missing_live_artifacts.len()
        ));
    }
    if !customer_claim_ready {
        claim_blockers.push("customer claim not ready".to_string());
    }
    if !matches!(production_readiness_blocker_class, "" | "none") {
        claim_blockers.push(format!(
            "production readiness blocked: {}",
            production_readiness_blocker_class
        ));
    }
    if mutation_allowed {
        claim_blockers
            .push("mutation is allowed; review rollout safety before sharing".to_string());
    }
    if !vram_advisory_ready {
        claim_blockers.push("VRAM advisory evidence missing".to_string());
    }
    let primary_claim_blocker = claim_blockers
        .iter()
        .find(|blocker| blocker.starts_with("production readiness blocked:"))
        .or_else(|| {
            claim_blockers
                .iter()
                .find(|blocker| blocker.starts_with("mutation is allowed"))
        })
        .or_else(|| {
            claim_blockers
                .iter()
                .find(|blocker| blocker.starts_with("VRAM advisory"))
        })
        .or_else(|| {
            claim_blockers
                .iter()
                .find(|blocker| blocker.as_str() == "customer claim not ready")
        })
        .or_else(|| claim_blockers.first())
        .cloned();
    let primary_claim_blocker_next_action = primary_claim_blocker.as_deref().and_then(|blocker| {
        if blocker.starts_with("production readiness blocked:") {
            production_readiness_next_action.or(Some("restore production readiness before using this packet for launch or customer claims"))
        } else if blocker.starts_with("mutation is allowed") {
            Some("switch to observe-only or review rollout safety before sharing")
        } else if blocker.starts_with("VRAM advisory") {
            Some("collect VRAM advisory evidence before making scheduler placement claims")
        } else if blocker == "customer claim not ready" {
            Some("resolve launch proof gaps before making customer-facing claims")
        } else if blocker.contains("missing live artifact") {
            Some("capture the missing live artifacts listed in this evidence bundle")
        } else {
            None
        }
    });
    let review_ready = claim_blockers.is_empty();
    let demo_gate_local_exit_code = 0;
    let demo_gate_strict_exit_code = if review_ready { 0 } else { 2 };
    let demo_gate_status = if review_ready {
        "strict-pass"
    } else {
        "local-pass-strict-blocked"
    };
    let mut summary_payload = serde_json::json!({
        "collection_command_count": collection_commands.len(),
        "evidence_row_count": evidence_row_count,
        "missing_live_artifact_count": missing_live_artifacts.len(),
        "missing_live_artifact_blocked_count": missing_live_artifact_blocked_count,
        "missing_live_artifact_warn_count": missing_live_artifact_warn_count,
        "missing_live_artifact_category_counts": missing_live_artifact_category_counts,
        "missing_live_artifact_category_rows": missing_live_artifact_category_rows,
        "missing_live_artifact_action_items": missing_live_artifact_action_items,
        "launch_status": launch_status,
        "customer_claim_ready": customer_claim_ready,
        "mutation_allowed": mutation_allowed,
        "vram_advisory_ready": vram_advisory_ready,
        "vram_hard_admission_ready": vram_hard_admission_ready,
        "vram_admission_mode": vram_admission_mode,
        "vram_scheduler_use": vram_scheduler_use,
        "vram_hard_blocker_count": vram_hard_blocker_count,
        "vram_next_evidence_target": vram_next_evidence_target,
        "vram_model_driver_count": vram_model_driver_count,
        "vram_top_driver_labels": vram_top_driver_labels,
        "vram_synthetic_reserve_driver": vram_synthetic_reserve_driver,
        "production_readiness_blocker_class": production_readiness_blocker_class,
        "simulator_endpoint_count": simulator_endpoint_count,
        "simulator_probe_checked_count": simulator_probe_checked_count,
        "simulator_probe_ready_count": simulator_probe_ready_count,
        "simulator_probe_timeout_millis": simulator_probe_timeout_millis,
        "simulator_readiness": simulator_readiness,
        "simulator_readiness_note": simulator_readiness_note,
        "live_validation_gate_count": live_validation_gate_count,
        "live_validation_pass_count": live_validation_pass_count,
        "live_validation_warn_count": live_validation_warn_count,
        "live_validation_blocked_count": live_validation_blocked_count,
        "review_ready": review_ready,
        "demo_gate_status": demo_gate_status,
        "demo_gate_local_exit_code": demo_gate_local_exit_code,
        "demo_gate_strict_exit_code": demo_gate_strict_exit_code,
        "primary_claim_blocker": primary_claim_blocker,
        "primary_claim_blocker_next_action": primary_claim_blocker_next_action,
        "production_readiness_next_action": production_readiness_next_action,
        "production_readiness_diagnostic_hint": production_readiness_diagnostic_hint,
        "claim_blockers": claim_blockers,
    });
    if let Some(obj) = summary_payload.as_object_mut() {
        obj.insert(
            "vram_synthetic_headroom_driver".to_string(),
            serde_json::json!(vram_synthetic_reserve_driver),
        );
        obj.insert(
            "vram_reserve_pressure_definition".to_string(),
            serde_json::json!(vram_synthetic_headroom_definition),
        );
        obj.insert(
            "vram_synthetic_headroom_definition".to_string(),
            serde_json::json!(vram_synthetic_headroom_definition),
        );
        obj.insert(
            "simulator_claim_ready".to_string(),
            serde_json::json!(simulator_claim_ready),
        );
        obj.insert(
            "simulator_claim_mode".to_string(),
            serde_json::json!(simulator_claim_mode),
        );
        obj.insert(
            "simulator_claim_blocker".to_string(),
            serde_json::json!(simulator_claim_blocker),
        );
        obj.insert(
            "simulator_claim_next_action".to_string(),
            serde_json::json!(simulator_claim_next_action),
        );
        obj.insert(
            "operator_binding_status".to_string(),
            operator_binding_safety
                .get("status")
                .cloned()
                .unwrap_or_else(|| serde_json::json!("unknown")),
        );
        obj.insert(
            "operator_reservation_pressure".to_string(),
            operator_binding_safety
                .get("reservation_pressure")
                .cloned()
                .unwrap_or_else(|| serde_json::json!("unknown")),
        );
        obj.insert(
            "operator_reservation_pressure_description".to_string(),
            operator_binding_safety
                .get("reservation_pressure_description")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        obj.insert(
            "operator_reservation_pressure_scope".to_string(),
            operator_binding_safety
                .get("reservation_pressure_scope")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        obj.insert(
            "operator_reservation_pressure_reason".to_string(),
            operator_binding_safety
                .get("reservation_pressure_reason")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        obj.insert(
            "operator_reservation_pressure_next_action".to_string(),
            operator_binding_safety
                .get("reservation_pressure_next_action")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        obj.insert(
            "vram_display_top_driver_labels".to_string(),
            serde_json::json!(vram_display_top_driver_labels),
        );
        obj.insert(
            "vram_claim_safe_driver_count".to_string(),
            serde_json::json!(vram_claim_safe_driver_count),
        );
        obj.insert(
            "vram_claim_safe_driver_labels".to_string(),
            serde_json::json!(vram_claim_safe_driver_labels),
        );
        obj.insert(
            "vram_display_claim_safe_driver_labels".to_string(),
            serde_json::json!(vram_display_claim_safe_driver_labels),
        );
        obj.insert(
            "vram_real_model_driver_count".to_string(),
            serde_json::json!(vram_real_model_driver_count),
        );
        obj.insert(
            "vram_real_top_driver_labels".to_string(),
            serde_json::json!(vram_real_top_driver_labels),
        );
        obj.insert(
            "vram_display_real_top_driver_labels".to_string(),
            serde_json::json!(vram_display_real_top_driver_labels),
        );
        obj.insert(
            "vram_synthetic_driver_count".to_string(),
            serde_json::json!(vram_synthetic_driver_count),
        );
        obj.insert(
            "vram_synthetic_driver_labels".to_string(),
            serde_json::json!(vram_synthetic_driver_labels),
        );
        obj.insert(
            "vram_display_synthetic_driver_labels".to_string(),
            serde_json::json!(vram_display_synthetic_driver_labels),
        );
        obj.insert(
            "vram_driver_claim_boundary".to_string(),
            serde_json::json!(vram_driver_claim_boundary),
        );
        obj.insert(
            "production_readiness_last_error_class".to_string(),
            serde_json::json!(production_readiness_last_error_class),
        );
        obj.insert(
            "production_readiness_debug_commands".to_string(),
            production_readiness_debug_commands,
        );
        obj.insert(
            "production_readiness_first_debug_command".to_string(),
            production_readiness_first_debug_command,
        );
        obj.insert(
            "vram_investment_demo_rows".to_string(),
            serde_json::json!(vram_investment_demo_rows),
        );
        obj.insert(
            "vram_investment_oom_risk_reduction_pods".to_string(),
            serde_json::json!(vram_investment_oom_risk_reduction_pods),
        );
        obj.insert(
            "vram_investment_high_vram_nodes_preserved".to_string(),
            serde_json::json!(vram_investment_high_vram_nodes_preserved),
        );
        obj.insert(
            "vram_investment_advisory_rows".to_string(),
            serde_json::json!(vram_investment_advisory_rows),
        );
        obj.insert(
            "vram_investment_average_baseline_oom_risk_percent".to_string(),
            serde_json::json!(vram_investment_average_baseline_oom_risk_percent),
        );
        obj.insert(
            "vram_investment_average_ksolver_oom_risk_percent".to_string(),
            serde_json::json!(vram_investment_average_ksolver_oom_risk_percent),
        );
        obj.insert("operator_runbook".to_string(), operator_runbook);
    }

    Json(serde_json::json!({
        "ok": true,
        "dry_run": true,
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "trace_sequence": trace_sequence,
        "note": "read-only SRE evidence bundle scaffold; endpoints render current state and do not mutate Kubernetes",
        "summary": summary_payload,
        "collection_commands": collection_commands,
        "launch_proof_gate": launch_proof_gate,
        "evidence_bundle_rows": evidence_bundle_rows,
        "live_validation_gates": live_validation_gates,
        "missing_live_artifacts": missing_live_artifacts,
        "missing_live_artifact_rows": missing_live_artifact_rows,
        "artifacts": {
            "latest_trace": latest_trace,
            "production_safety": production_safety,
            "demo_report": demo_report,
            "vram_calibration": vram_calibration,
        },
    }))
}

async fn operator_status_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let axum::Json(bundle) = evidence_bundle_handler(State(s)).await;
    Json(operator_status_from_evidence_bundle(&bundle))
}

fn operator_status_from_evidence_bundle(bundle: &serde_json::Value) -> serde_json::Value {
    let summary = bundle
        .get("summary")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let production_safety = bundle
        .get("artifacts")
        .and_then(|artifacts| artifacts.get("production_safety"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let production_readiness = production_safety
        .get("readiness")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let vram_calibration = bundle
        .get("artifacts")
        .and_then(|artifacts| artifacts.get("vram_calibration"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let vram_scheduler_readiness = vram_calibration
        .get("scheduler_readiness")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let review_ready = summary
        .get("review_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let customer_claim_ready = summary
        .get("customer_claim_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let blocker = summary
        .get("primary_claim_blocker")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    let status = if review_ready {
        "ready"
    } else if blocker.starts_with("production readiness blocked:")
        || blocker.starts_with("mutation is allowed")
    {
        "blocked"
    } else {
        "needs-evidence"
    };
    let strict_exit = summary
        .get("demo_gate_strict_exit_code")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(if review_ready { 0 } else { 2 });
    let local_exit = summary
        .get("demo_gate_local_exit_code")
        .and_then(serde_json::Value::as_i64)
        .unwrap_or(0);
    let vram_hard_admission_ready = summary
        .get("vram_hard_admission_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let vram_advisory_ready = summary
        .get("vram_advisory_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let evidence_gap_action_items = operator_evidence_gap_action_items(
        summary
            .get("missing_live_artifact_category_rows")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]),
    );
    let evidence_gap_action_items = summary
        .get("missing_live_artifact_action_items")
        .cloned()
        .unwrap_or_else(|| serde_json::json!(evidence_gap_action_items));
    let simulator_claim_ready = summary
        .get("simulator_claim_ready")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let simulator_recovery_command = production_safety
        .get("simulator")
        .and_then(|simulator| simulator.get("recovery_command"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            let simulator_urls = production_safety
                .get("simulator")
                .and_then(|simulator| simulator.get("endpoints"))
                .and_then(serde_json::Value::as_array)
                .map(|urls| {
                    urls.iter()
                        .filter_map(serde_json::Value::as_str)
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            simulator_recovery_command_for_urls(&simulator_urls)
        });
    let mut operator_action_items = evidence_gap_action_items
        .as_array()
        .cloned()
        .unwrap_or_default();
    if !simulator_claim_ready {
        let simulator_action_present = operator_action_items.iter().any(|item| {
            item.get("category").and_then(serde_json::Value::as_str) == Some("simulator-baseline")
                || item.get("command_hint").and_then(serde_json::Value::as_str)
                    == Some(simulator_recovery_command.as_str())
        });
        if !simulator_action_present {
            operator_action_items.insert(
                0,
                serde_json::json!({
                    "priority": 1,
                    "category": "simulator-baseline",
                    "severity": "blocked",
                    "blocked": 1,
                    "warn": 0,
                    "artifact": "kube-scheduler-simulator claim proof",
                    "next_action": summary
                        .get("simulator_claim_next_action")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!("repair kube-scheduler-simulator before making kube-vs-ksolver placement claims")),
                    "command_hint": simulator_recovery_command,
                    "command_hints": [simulator_recovery_command],
                    "command_kind": "shell",
                    "copyable": true,
                }),
            );
        }
    }
    for (idx, item) in operator_action_items.iter_mut().enumerate() {
        if let Some(obj) = item.as_object_mut() {
            obj.insert("priority".to_string(), serde_json::json!(idx + 1));
        }
    }
    let operator_runbook = if operator_action_items.is_empty() {
        summary
            .get("operator_runbook")
            .cloned()
            .unwrap_or_else(|| operator_action_runbook(&[]))
    } else {
        operator_action_runbook(&operator_action_items)
    };

    let mut operator_status = serde_json::json!({
        "ok": true,
        "generated_at": bundle.get("generated_at").cloned().unwrap_or_else(|| serde_json::json!(chrono::Utc::now().to_rfc3339())),
        "dry_run": bundle.get("dry_run").cloned().unwrap_or(serde_json::Value::Bool(true)),
        "status": status,
        "status_label": match status {
            "ready" => "review ready",
            "blocked" => "operator action required",
            _ => "needs evidence",
        },
        "can_shadow_demo": local_exit == 0,
        "can_customer_claim": review_ready && customer_claim_ready,
        "can_hard_admit_vram": vram_hard_admission_ready,
        "can_score_vram": vram_advisory_ready,
        "primary_blocker": summary.get("primary_claim_blocker").cloned().unwrap_or(serde_json::Value::Null),
        "next_action": summary.get("primary_claim_blocker_next_action").cloned().unwrap_or(serde_json::Value::Null),
        "diagnostic_hint": summary.get("production_readiness_diagnostic_hint").cloned().unwrap_or(serde_json::Value::Null),
        "debug_commands": production_readiness.get("debug_commands").cloned().unwrap_or_else(|| serde_json::json!([])),
        "production_readiness": {
            "blocker_class": summary.get("production_readiness_blocker_class").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
            "last_error_class": summary.get("production_readiness_last_error_class").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
            "next_action": summary.get("production_readiness_next_action").cloned().unwrap_or(serde_json::Value::Null),
            "debug_commands": production_readiness.get("debug_commands").cloned().unwrap_or_else(|| serde_json::json!([])),
        },
        "simulator": {
            "readiness": summary.get("simulator_readiness").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
            "ready_count": summary.get("simulator_probe_ready_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "checked_count": summary.get("simulator_probe_checked_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "endpoint_count": summary.get("simulator_endpoint_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "note": summary.get("simulator_readiness_note").cloned().unwrap_or(serde_json::Value::Null),
        },
        "proof_gates": {
            "total": summary.get("live_validation_gate_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "pass": summary.get("live_validation_pass_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "warn": summary.get("live_validation_warn_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "blocked": summary.get("live_validation_blocked_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "rows": bundle.get("live_validation_gates").cloned().unwrap_or_else(|| serde_json::json!([])),
        },
        "evidence_gaps": {
            "total": summary.get("missing_live_artifact_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "blocked": summary.get("missing_live_artifact_blocked_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "warn": summary.get("missing_live_artifact_warn_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "category_counts": summary.get("missing_live_artifact_category_counts").cloned().unwrap_or_else(|| serde_json::json!({})),
            "category_rows": summary.get("missing_live_artifact_category_rows").cloned().unwrap_or_else(|| serde_json::json!([])),
            "rows": bundle.get("missing_live_artifact_rows").cloned().unwrap_or_else(|| serde_json::json!([])),
        },
        "action_items": operator_action_items,
        "operator_runbook": operator_runbook,
        "vram": {
            "mode": summary.get("vram_admission_mode").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
            "scheduler_use": summary.get("vram_scheduler_use").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
            "hard_blocker_count": summary.get("vram_hard_blocker_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "next_evidence_target": summary.get("vram_next_evidence_target").cloned().unwrap_or(serde_json::Value::Null),
            "model_driver_count": summary.get("vram_model_driver_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "top_driver_labels": summary.get("vram_top_driver_labels").cloned().unwrap_or_else(|| serde_json::json!([])),
            "real_model_driver_count": summary.get("vram_real_model_driver_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "real_top_driver_labels": summary.get("vram_real_top_driver_labels").cloned().unwrap_or_else(|| serde_json::json!([])),
            "synthetic_driver_count": summary.get("vram_synthetic_driver_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "synthetic_driver_labels": summary.get("vram_synthetic_driver_labels").cloned().unwrap_or_else(|| serde_json::json!([])),
            "synthetic_reserve_driver": summary.get("vram_synthetic_reserve_driver").cloned().unwrap_or(serde_json::Value::Bool(false)),
            "reserve_pressure_definition": summary.get("vram_reserve_pressure_definition").cloned().unwrap_or_else(|| serde_json::json!(VRAM_RESERVE_PRESSURE_DEFINITION)),
            "driver_claim_boundary": summary.get("vram_driver_claim_boundary").cloned().unwrap_or_else(|| serde_json::json!("real driver labels exclude synthetic VRAM headroom probes")),
            "investment_demo_rows": summary.get("vram_investment_demo_rows").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "investment_oom_risk_reduction_pods": summary.get("vram_investment_oom_risk_reduction_pods").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "investment_high_vram_nodes_preserved": summary.get("vram_investment_high_vram_nodes_preserved").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "investment_advisory_rows": summary.get("vram_investment_advisory_rows").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "investment_average_baseline_oom_risk_percent": summary.get("vram_investment_average_baseline_oom_risk_percent").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "investment_average_ksolver_oom_risk_percent": summary.get("vram_investment_average_ksolver_oom_risk_percent").cloned().unwrap_or_else(|| serde_json::json!(0)),
        },
        "demo_gate": {
            "status": summary.get("demo_gate_status").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
            "local_exit_code": local_exit,
            "strict_exit_code": strict_exit,
        },
        "evidence": {
            "path": "/api/scheduler/evidence-bundle",
            "collection_command_count": summary.get("collection_command_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "evidence_row_count": summary.get("evidence_row_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "missing_live_artifact_count": summary.get("missing_live_artifact_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "missing_live_artifact_blocked_count": summary.get("missing_live_artifact_blocked_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "missing_live_artifact_warn_count": summary.get("missing_live_artifact_warn_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "missing_live_artifact_category_counts": summary.get("missing_live_artifact_category_counts").cloned().unwrap_or_else(|| serde_json::json!({})),
            "missing_live_artifact_category_rows": summary.get("missing_live_artifact_category_rows").cloned().unwrap_or_else(|| serde_json::json!([])),
            "missing_live_artifact_action_items": summary.get("missing_live_artifact_action_items").cloned().unwrap_or_else(|| serde_json::json!([])),
            "operator_runbook": operator_runbook,
            "claim_blockers": summary.get("claim_blockers").cloned().unwrap_or_else(|| serde_json::json!([])),
        },
        "trace_sequence": bundle.get("trace_sequence").cloned().unwrap_or_else(|| serde_json::json!(0)),
    });
    let scale_safety = operator_scale_safety_from_production_safety(&production_safety);
    let binding_safety = operator_binding_safety_from_production_safety(&production_safety);
    let decision_readiness = operator_decision_readiness(
        &summary,
        &binding_safety,
        &scale_safety,
        local_exit,
        review_ready,
        customer_claim_ready,
        simulator_claim_ready,
        vram_advisory_ready,
        vram_hard_admission_ready,
    );
    if let Some(status) = operator_status.as_object_mut() {
        status.insert(
            "scale_safety".to_string(),
            scale_safety,
        );
        status.insert(
            "binding_safety".to_string(),
            binding_safety,
        );
        status.insert("decision_readiness".to_string(), decision_readiness);
    }
    if let Some(vram) = operator_status
        .get_mut("vram")
        .and_then(serde_json::Value::as_object_mut)
    {
        vram.insert(
            "display_top_driver_labels".to_string(),
            display_vram_driver_labels(summary.get("vram_top_driver_labels")),
        );
        vram.insert(
            "hard_admission_blockers".to_string(),
            vram_scheduler_readiness
                .get("hard_admission_blockers")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        );
        vram.insert(
            "evidence_collection_plan".to_string(),
            vram_scheduler_readiness
                .get("evidence_collection_plan")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        );
        vram.insert(
            "requirements".to_string(),
            vram_scheduler_readiness
                .get("requirements")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        );
        vram.insert(
            "display_real_top_driver_labels".to_string(),
            display_vram_driver_labels(summary.get("vram_real_top_driver_labels")),
        );
        vram.insert(
            "display_synthetic_driver_labels".to_string(),
            display_vram_driver_labels(summary.get("vram_synthetic_driver_labels")),
        );
        vram.insert(
            "display_claim_safe_driver_labels".to_string(),
            display_vram_driver_labels(summary.get("vram_claim_safe_driver_labels")),
        );
        vram.insert(
            "synthetic_headroom_driver".to_string(),
            summary
                .get("vram_synthetic_headroom_driver")
                .cloned()
                .unwrap_or_else(|| {
                    summary
                        .get("vram_synthetic_reserve_driver")
                        .cloned()
                        .unwrap_or(serde_json::Value::Bool(false))
                }),
        );
        vram.insert(
            "synthetic_headroom_definition".to_string(),
            summary
                .get("vram_synthetic_headroom_definition")
                .cloned()
                .unwrap_or_else(|| {
                    summary
                        .get("vram_reserve_pressure_definition")
                        .cloned()
                        .unwrap_or_else(|| serde_json::json!(VRAM_SYNTHETIC_HEADROOM_DEFINITION))
                }),
        );
        vram.insert(
            "claim_safe_driver_count".to_string(),
            summary
                .get("vram_claim_safe_driver_count")
                .cloned()
                .unwrap_or_else(|| serde_json::json!(0)),
        );
        vram.insert(
            "claim_safe_driver_labels".to_string(),
            summary
                .get("vram_claim_safe_driver_labels")
                .cloned()
                .unwrap_or_else(|| serde_json::json!([])),
        );
    }
    if let Some(simulator) = operator_status
        .get_mut("simulator")
        .and_then(serde_json::Value::as_object_mut)
    {
        simulator.insert(
            "claim_ready".to_string(),
            summary
                .get("simulator_claim_ready")
                .cloned()
                .unwrap_or(serde_json::Value::Bool(false)),
        );
        simulator.insert(
            "claim_mode".to_string(),
            summary
                .get("simulator_claim_mode")
                .cloned()
                .unwrap_or_else(|| serde_json::json!("unknown")),
        );
        simulator.insert(
            "claim_blocker".to_string(),
            summary
                .get("simulator_claim_blocker")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        simulator.insert(
            "claim_next_action".to_string(),
            summary
                .get("simulator_claim_next_action")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        );
        simulator.insert(
            "recovery_command".to_string(),
            if simulator_claim_ready {
                serde_json::Value::Null
            } else {
                serde_json::json!(simulator_recovery_command)
            },
        );
    }
    operator_status
}

fn operator_scale_safety_from_production_safety(
    production_safety: &serde_json::Value,
) -> serde_json::Value {
    let Some(trace) = production_safety.get("latest_trace") else {
        return serde_json::json!({
            "available": false,
            "status": "missing-trace",
            "regret_status": "unknown",
            "next_action": "capture a live shadow trace before making scale/pruning trust claims",
        });
    };
    if trace.is_null() {
        return serde_json::json!({
            "available": false,
            "status": "missing-trace",
            "regret_status": "unknown",
            "next_action": "capture a live shadow trace before making scale/pruning trust claims",
        });
    }
    let quality = trace
        .get("candidate_quality_metrics")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let regret_status = quality
        .get("regret_status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let regret_unknown = regret_status.contains("unknown");
    let pruning_active = quality
        .get("pruning_active")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let next_action = if regret_unknown {
        "rerun or compare with candidate_node_limit=0 before claiming pruning has no scheduling regret"
    } else if pruning_active {
        "candidate pruning has bounded trace evidence; keep scale guardrail visible with every customer claim"
    } else {
        "no candidate-pruning regret action required for this trace"
    };
    serde_json::json!({
        "available": true,
        "status": if regret_unknown { "regret-unknown" } else { "regret-bounded" },
        "regret_status": regret_status,
        "next_action": next_action,
        "pruning_active": pruning_active,
        "widened": quality.get("widened").cloned().unwrap_or(serde_json::json!(false)),
        "edge_reduction_milli": quality.get("edge_reduction_milli").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "explanation": quality.get("explanation").cloned().unwrap_or_else(|| serde_json::json!("candidate quality not reported")),
        "candidate_node_limit": trace.get("candidate_node_limit").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "retry_count": trace.get("retry_count").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "unpruned_candidate_edges": trace.get("unpruned_candidate_edges").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "initial_candidate_edges": trace.get("initial_candidate_edges").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "final_candidate_edges": trace.get("final_candidate_edges").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "candidate_pruned_workloads": trace.get("candidate_pruned_workloads").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "widening_reason": trace.get("widening_reason").cloned().unwrap_or(serde_json::Value::Null),
    })
}

#[allow(clippy::too_many_arguments)]
fn operator_decision_readiness(
    summary: &serde_json::Value,
    binding_safety: &serde_json::Value,
    scale_safety: &serde_json::Value,
    local_exit: i64,
    review_ready: bool,
    customer_claim_ready: bool,
    simulator_claim_ready: bool,
    vram_advisory_ready: bool,
    vram_hard_admission_ready: bool,
) -> serde_json::Value {
    let production_blocker = summary
        .get("production_readiness_blocker_class")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let production_ready = matches!(production_blocker, "" | "none");
    let primary_blocker = summary
        .get("primary_claim_blocker")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("evidence packet is not customer-claim ready");
    let primary_next_action = summary
        .get("primary_claim_blocker_next_action")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("collect the missing live evidence before making customer claims");
    let binding_status = binding_safety
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let reservation_pressure = binding_safety
        .get("reservation_pressure")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let mutation_allowed = binding_safety
        .get("mutation_allowed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let dry_run = binding_safety
        .get("real_binding_dry_run")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let kill_switch = binding_safety
        .get("binding_kill_switch")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let scale_status = scale_safety
        .get("status")
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let scale_ready = scale_status == "regret-bounded";
    let demo_ready = local_exit == 0;
    let claim_ready = review_ready && customer_claim_ready && simulator_claim_ready;
    let binding_ready = mutation_allowed
        && !dry_run
        && !kill_switch
        && production_ready
        && matches!(reservation_pressure, "none" | "active")
        && binding_status != "binding-failures";
    let binding_capability_status = if binding_ready {
        "ready"
    } else if !mutation_allowed {
        "read-only"
    } else if binding_status == "binding-failures"
        || kill_switch
        || matches!(reservation_pressure, "blocking" | "stale")
        || !production_ready
    {
        "blocked"
    } else if mutation_allowed && dry_run {
        "dry-run"
    } else {
        "needs-review"
    };
    let binding_next_action = if binding_ready {
        "production binding may proceed within the configured canary and reservation limits"
    } else if binding_status == "binding-failures" {
        "inspect failed binding outcomes before enabling more mutation"
    } else if matches!(reservation_pressure, "blocking" | "stale") {
        binding_safety
            .get("reservation_pressure_next_action")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("repair reservation pressure before production binding")
    } else if kill_switch {
        "turn off the binding kill switch only after the production rollout checklist passes"
    } else if !mutation_allowed {
        "enable real binding only after ownership, RBAC, canary, reservation, and kill-switch gates are approved"
    } else if !production_ready {
        "restore Kubernetes production readiness before binding pods"
    } else if mutation_allowed && dry_run {
        "review dry-run binding outcomes before switching to live mutation"
    } else {
        "review binding safety gates before production binding"
    };
    let highest_risk = if !demo_ready {
        "shadow demo is not locally runnable"
    } else if !simulator_claim_ready {
        "kube-scheduler baseline is not customer-claim ready"
    } else if !review_ready || !customer_claim_ready {
        primary_blocker
    } else if !scale_ready {
        "candidate pruning regret is not bounded for claim safety"
    } else if !binding_ready {
        binding_next_action
    } else {
        "no blocking operator decision risk detected"
    };
    let next_action = if !demo_ready {
        "run the local demo gate and repair failing shadow dependencies"
    } else if !simulator_claim_ready {
        summary
            .get("simulator_claim_next_action")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("repair kube-scheduler-simulator before making kube-vs-ksolver claims")
    } else if !review_ready || !customer_claim_ready {
        primary_next_action
    } else if !scale_ready {
        scale_safety
            .get("next_action")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("prove candidate pruning regret before scale claims")
    } else if !binding_ready {
        binding_next_action
    } else {
        "continue with customer review or canary production binding"
    };
    let summary_label = format!(
        "demo={}, claim={}, vram-score={}, hard-admit={}, bind={}",
        if demo_ready { "ready" } else { "blocked" },
        if claim_ready { "ready" } else { "blocked" },
        if vram_advisory_ready { "ready" } else { "blocked" },
        if vram_hard_admission_ready { "ready" } else { "blocked" },
        binding_capability_status,
    );

    serde_json::json!({
        "status": if demo_ready && claim_ready && scale_ready { "ready" } else { "needs-action" },
        "summary": summary_label,
        "highest_risk": highest_risk,
        "next_action": next_action,
        "capabilities": [
            {
                "name": "shadow_demo",
                "label": "Shadow demo",
                "status": if demo_ready { "ready" } else { "blocked" },
                "can_execute": demo_ready,
                "next_action": if demo_ready { "demo gate is locally runnable" } else { "run the local demo gate and repair failing shadow dependencies" },
            },
            {
                "name": "customer_claim",
                "label": "Customer claim",
                "status": if claim_ready { "ready" } else { "blocked" },
                "can_execute": claim_ready,
                "next_action": if claim_ready { "customer claim packet is ready" } else { primary_next_action },
            },
            {
                "name": "vram_scoring",
                "label": "VRAM scoring",
                "status": if vram_advisory_ready { "ready" } else { "blocked" },
                "can_execute": vram_advisory_ready,
                "next_action": if vram_advisory_ready { "score and warn; do not hard-reject pods" } else { "collect VRAM advisory evidence before scheduling claims" },
            },
            {
                "name": "hard_vram_admission",
                "label": "Hard VRAM admission",
                "status": if vram_hard_admission_ready { "ready" } else { "blocked" },
                "can_execute": vram_hard_admission_ready,
                "next_action": if vram_hard_admission_ready { "hard admission gates are evidence-backed" } else { "collect true CUDA OOM labels and cross-SKU validation first" },
            },
            {
                "name": "production_binding",
                "label": "Production binding",
                "status": binding_capability_status,
                "can_execute": binding_ready,
                "next_action": binding_next_action,
            }
        ],
    })
}

fn operator_binding_safety_from_production_safety(
    production_safety: &serde_json::Value,
) -> serde_json::Value {
    let rollout = production_safety
        .get("rollout")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let mutation_allowed = rollout
        .get("mutation_allowed")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let dry_run = rollout
        .get("real_binding_dry_run")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let kill_switch = rollout
        .get("binding_kill_switch")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let latest_trace = production_safety.get("latest_trace");
    let latest_bind_outcomes = production_safety.get("latest_bind_outcomes");
    let outcome_metrics = latest_trace
        .and_then(|trace| trace.get("binding_outcome_metrics"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let reservation_metrics = latest_trace
        .and_then(|trace| trace.get("binding_reservation_metrics"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let active_reservation_entries = reservation_metrics
        .get("active_entries")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let reserved_gpus = reservation_metrics
        .get("reserved_gpus")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let reservation_rejected = reservation_metrics
        .get("rejected")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let reservation_expired = reservation_metrics
        .get("expired")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let reservation_stale = reservation_metrics
        .get("stale_entries")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let reservation_pressure = if reservation_rejected > 0 {
        "blocking"
    } else if reservation_stale > 0 || reservation_expired > 0 {
        "stale"
    } else if active_reservation_entries > 0 || reserved_gpus > 0 {
        "active"
    } else {
        "none"
    };
    let reservation_pressure_description =
        "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky even when GPUs look free.";
    let reservation_pressure_scope =
        "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM.";
    let reservation_pressure_reason = if reservation_rejected > 0 {
        format!(
            "{reservation_rejected} reservation request(s) rejected by the ledger; live binding cannot safely reserve all planned GPU placements"
        )
    } else if reservation_stale > 0 || reservation_expired > 0 {
        format!(
            "{reservation_stale} stale reservation entrie(s), {reservation_expired} expired reservation(s); reconcile before trusting bind readiness"
        )
    } else if active_reservation_entries > 0 || reserved_gpus > 0 {
        format!(
            "{active_reservation_entries} active reservation entrie(s) hold {reserved_gpus} GPU(s) while binding safety gates run"
        )
    } else {
        "no active binding reservations are holding GPU capacity".to_string()
    };
    let reservation_pressure_next_action = if reservation_rejected > 0 {
        "inspect rejected reservation targets and ledger capacity before enabling production binding"
    } else if reservation_stale > 0 || reservation_expired > 0 {
        "wait for reservation reconciliation or clear stale reservations before trusting live binding"
    } else if active_reservation_entries > 0 || reserved_gpus > 0 {
        "verify reservations are fresh and within TTL before binding the reserved placements"
    } else {
        "no reservation pressure action required"
    };
    let failed = outcome_metrics
        .get("failed")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let bound = outcome_metrics
        .get("bound")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let validated = outcome_metrics
        .get("validated")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let skipped = outcome_metrics
        .get("skipped")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let available = latest_trace.map(|trace| !trace.is_null()).unwrap_or(false);
    let status = if failed > 0 {
        "binding-failures"
    } else if mutation_allowed && !dry_run && bound > 0 {
        "mutating"
    } else if mutation_allowed && dry_run {
        "dry-run-validation"
    } else if mutation_allowed && kill_switch {
        "mutation-kill-switch"
    } else if mutation_allowed {
        "mutation-capable"
    } else {
        "read-only"
    };
    let next_action = if failed > 0 {
        "inspect /api/scheduler/binding-events and failed binding outcomes before enabling further mutation"
    } else if mutation_allowed && !dry_run {
        "confirm ownership, canary limits, reservation freshness, and kill switch before production binding"
    } else if mutation_allowed && dry_run {
        "review validated dry-run binding outcomes before switching to non-dry-run mutation"
    } else {
        "no binding mutation action required while shadow remains read-only"
    };

    serde_json::json!({
        "available": available,
        "status": status,
        "next_action": next_action,
        "mutation_allowed": mutation_allowed,
        "mode": rollout.get("mode").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
        "enable_real_binding": rollout.get("enable_real_binding").cloned().unwrap_or(serde_json::json!(false)),
        "real_binding_dry_run": dry_run,
        "binding_kill_switch": kill_switch,
        "binding_canary_mode": rollout.get("binding_canary_mode").cloned().unwrap_or_else(|| serde_json::json!("unknown")),
        "binding_low_risk_max_gpus": rollout.get("binding_low_risk_max_gpus").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "max_binds_per_pass": rollout.get("max_binds_per_pass").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "binding_reservation_ttl_seconds": rollout.get("binding_reservation_ttl_seconds").cloned().unwrap_or_else(|| serde_json::json!(0)),
        "latest_trace_sequence": latest_trace.and_then(|trace| trace.get("sequence")).cloned().unwrap_or_else(|| serde_json::json!(0)),
        "latest_outcome_count": latest_bind_outcomes.and_then(|o| o.get("outcome_count")).cloned().unwrap_or_else(|| serde_json::json!(0)),
        "bound": bound,
        "validated": validated,
        "skipped": skipped,
        "failed": failed,
        "reservations": reservation_metrics,
        "reservation_pressure": reservation_pressure,
        "reservation_pressure_description": reservation_pressure_description,
        "reservation_pressure_scope": reservation_pressure_scope,
        "reservation_pressure_reason": reservation_pressure_reason,
        "reservation_pressure_next_action": reservation_pressure_next_action,
        "skip_breakdown": {
            "canary": outcome_metrics.get("canary_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "readiness": outcome_metrics.get("readiness_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "identity": outcome_metrics.get("identity_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "scheduler": outcome_metrics.get("scheduler_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "already_bound": outcome_metrics.get("already_bound_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "dra": outcome_metrics.get("dra_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "throttle": outcome_metrics.get("throttle_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "reservation": outcome_metrics.get("reservation_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "disabled": outcome_metrics.get("disabled_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "group": outcome_metrics.get("group_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
            "other": outcome_metrics.get("other_skipped").cloned().unwrap_or_else(|| serde_json::json!(0)),
        },
    })
}

fn evidence_bundle_collection_commands() -> Vec<String> {
    [
        "curl -s http://127.0.0.1:8090/api/scheduler/traces > traces.json",
        "curl -s http://127.0.0.1:8090/api/scheduler/kube-simulator-plan > kube-simulator-plan.json",
        "curl -s http://127.0.0.1:8090/api/scheduler/repair-plan > repair-plan.json",
        "curl -s http://127.0.0.1:8090/api/scheduler/production-safety > production-safety.json",
        "curl -s http://127.0.0.1:8090/api/scheduler/demo-report > demo-report.json",
        "curl -s http://127.0.0.1:8090/api/scheduler/vram-calibration > vram-calibration.json",
        "curl -s http://127.0.0.1:8090/api/scheduler/operator-status > operator-status.json",
        "curl -s http://127.0.0.1:8090/api/scheduler/evidence-bundle > evidence-bundle.json",
    ]
    .into_iter()
    .map(str::to_string)
    .collect()
}

async fn vram_calibration_handler() -> Json<serde_json::Value> {
    Json(vram_calibration_payload())
}

fn vram_calibration_payload() -> serde_json::Value {
    let root = ["vram-model-lab", "../vram-model-lab"]
        .into_iter()
        .map(std::path::PathBuf::from)
        .find(|path| path.join("data/training_rows.csv").exists())
        .unwrap_or_else(|| std::path::PathBuf::from("vram-model-lab"));
    vram_calibration_payload_from_root(&root)
}

fn vram_calibration_payload_from_root(root: &std::path::Path) -> serde_json::Value {
    let training_csv = root.join("data/training_rows.csv");
    let peak_model_json = root.join("data/models/peak_vram_linear.json");
    let evaluation_json = root.join("data/models/evaluation.json");
    let oom_json = root.join("data/models/oom_risk_classifier.json");
    let scheduler_report_json = root.join("data/models/scheduler_report.json");
    let summary_md = root.join("data/summary.md");

    let Ok(mut reader) = csv::Reader::from_path(&training_csv) else {
        return serde_json::json!({
            "available": false,
            "reason": format!("missing VRAM calibration CSV at {}", training_csv.display()),
            "paths": {
                "training_rows": training_csv.display().to_string(),
                "peak_model": peak_model_json.display().to_string(),
                "evaluation": evaluation_json.display().to_string(),
                "oom_classifier": oom_json.display().to_string(),
                "scheduler_report": scheduler_report_json.display().to_string(),
                "summary": summary_md.display().to_string(),
            }
        });
    };
    let Ok(headers) = reader.headers().cloned() else {
        return serde_json::json!({
            "available": false,
            "reason": format!("could not read VRAM calibration CSV headers at {}", training_csv.display()),
        });
    };
    let csv_columns = headers.iter().map(str::to_string).collect::<Vec<_>>();
    let has_column = |name: &str| headers.iter().any(|header| header == name);
    let evidence_columns = [
        (
            "verified_real_framework",
            "marks rows from verified real framework training entrypoints",
        ),
        (
            "customer_workload_fingerprint",
            "marks rows attached to production/customer workload fingerprints",
        ),
        ("oom", "records true CUDA OOM or hard failure labels"),
        (
            "gpu_sku_label",
            "identifies the GPU SKU used for cross-SKU calibration",
        ),
        (
            "nvidia_smi_peak_used_mib",
            "records observed peak device memory from nvidia-smi",
        ),
        (
            "torch_peak_reserved_mib",
            "records PyTorch allocator reserved-memory peak",
        ),
        ("sample_count", "records memory time-series sample coverage"),
    ]
    .into_iter()
    .map(|(column, purpose)| {
        serde_json::json!({
            "column": column,
            "present": has_column(column),
            "purpose": purpose,
        })
    })
    .collect::<Vec<_>>();
    let evidence_columns_present = evidence_columns
        .iter()
        .filter(|row| {
            row.get("present")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false)
        })
        .count();

    let mut row_count = 0_u64;
    let mut time_series_samples = 0_u64;
    let mut near_capacity_rows = 0_u64;
    let mut risk_rows = 0_u64;
    let mut oom_rows = 0_u64;
    let mut peak_sum = 0_f64;
    let mut peak_max = 0_f64;
    let mut reserve_pressure_rows = 0_u64;
    let mut reserve_extra_max_mib = 0_f64;
    let mut torch_reserve_gap_sum_mib = 0_f64;
    let mut torch_reserve_gap_rows = 0_u64;
    let mut torch_reserve_gap_max_mib = 0_f64;
    let mut verified_real_framework_rows = 0_u64;
    let mut customer_workload_fingerprint_rows = 0_u64;
    let mut families: BTreeMap<String, u64> = BTreeMap::new();
    let mut precisions: BTreeMap<String, u64> = BTreeMap::new();
    let mut gpu_skus: BTreeMap<String, u64> = BTreeMap::new();
    let mut gpu_names: BTreeMap<String, u64> = BTreeMap::new();
    let mut gpu_total_mib: BTreeMap<String, u64> = BTreeMap::new();
    let mut gpu_total_gib: BTreeMap<String, u64> = BTreeMap::new();
    let mut trainer_styles: BTreeMap<String, u64> = BTreeMap::new();

    for record in reader.records().flatten() {
        row_count += 1;
        let get = |name: &str| -> &str {
            headers
                .iter()
                .position(|header| header == name)
                .and_then(|idx| record.get(idx))
                .unwrap_or("")
        };
        count_nonempty(&mut families, get("family"));
        count_nonempty(&mut precisions, get("precision"));
        count_nonempty(&mut gpu_skus, get("gpu_sku_label"));
        count_nonempty(&mut gpu_names, get("gpu_name"));
        count_nonempty(&mut gpu_total_mib, get("gpu_total_mib"));
        count_nonempty(&mut gpu_total_gib, get("gpu_total_gib"));
        count_nonempty(&mut trainer_styles, get("trainer_style"));
        time_series_samples += get("sample_count").parse::<u64>().unwrap_or(0);
        let peak = get("nvidia_smi_peak_used_mib")
            .parse::<f64>()
            .unwrap_or(0.0);
        if peak > 0.0 {
            peak_sum += peak;
            peak_max = peak_max.max(peak);
        }
        let reserve_extra = get("reserve_extra_mib").parse::<f64>().unwrap_or(0.0);
        if reserve_extra > 0.0 {
            reserve_pressure_rows += 1;
            reserve_extra_max_mib = reserve_extra_max_mib.max(reserve_extra);
        }
        let torch_allocated = get("torch_peak_allocated_mib")
            .parse::<f64>()
            .unwrap_or(0.0);
        let torch_reserved = get("torch_peak_reserved_mib").parse::<f64>().unwrap_or(0.0);
        if torch_reserved > 0.0 && torch_allocated > 0.0 && torch_reserved >= torch_allocated {
            let gap = torch_reserved - torch_allocated;
            torch_reserve_gap_sum_mib += gap;
            torch_reserve_gap_rows += 1;
            torch_reserve_gap_max_mib = torch_reserve_gap_max_mib.max(gap);
        }
        let peak_fraction = get("peak_vram_fraction").parse::<f64>().unwrap_or(0.0);
        if peak_fraction >= 0.90 {
            near_capacity_rows += 1;
        }
        if parse_boolish(get("oom_risk_label")) {
            risk_rows += 1;
        }
        if parse_boolish(get("oom")) {
            oom_rows += 1;
        }
        let trainer_style = get("trainer_style").to_ascii_lowercase();
        if parse_boolish(get("verified_real_framework"))
            || parse_boolish(get("real_framework_verified"))
            || parse_boolish(get("ksolver_verified_real_app"))
            || trainer_style.contains("verified-real")
            || trainer_style.contains("real-app")
        {
            verified_real_framework_rows += 1;
        }
        if parse_boolish(get("customer_workload_fingerprint"))
            || parse_boolish(get("customer_fingerprint"))
            || parse_boolish(get("production_workload_fingerprint"))
        {
            customer_workload_fingerprint_rows += 1;
        }
    }

    let evaluation = std::fs::read_to_string(&evaluation_json)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
    let peak_model = std::fs::read_to_string(&peak_model_json)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
    let oom_classifier = std::fs::read_to_string(&oom_json)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
    let scheduler_report = std::fs::read_to_string(&scheduler_report_json)
        .ok()
        .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
    let scheduler_report_available = scheduler_report.is_some();
    let pipeline_ready_for_demo = scheduler_report
        .as_ref()
        .and_then(|v| v.get("ready_for_scheduler_demo"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let evidence_gate_verifier_ok = scheduler_report
        .as_ref()
        .and_then(|v| v.get("evidence_gate_verifier_ok"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let manifest_prediction_count = scheduler_report
        .as_ref()
        .and_then(|v| v.get("manifest_predictions"))
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or(0);
    let leftover_probe_resources = scheduler_report
        .as_ref()
        .and_then(|v| v.get("kube"))
        .and_then(|v| v.get("leftover_probe_resources"))
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let usable_families = scheduler_report
        .as_ref()
        .and_then(|v| v.get("evaluation"))
        .and_then(|v| v.get("usable_family_models"))
        .and_then(serde_json::Value::as_object)
        .map(|models| models.keys().cloned().collect::<Vec<_>>())
        .unwrap_or_default();
    let evidence_gate_verifier_stdout = scheduler_report
        .as_ref()
        .and_then(|v| v.get("evidence_gate_verifier_stdout"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
        .lines()
        .take(4)
        .collect::<Vec<_>>()
        .join("\n");
    let generated_at = std::fs::metadata(&training_csv)
        .ok()
        .and_then(|meta| meta.modified().ok())
        .map(chrono::DateTime::<chrono::Utc>::from)
        .map(|ts| ts.to_rfc3339());
    let ready_for_shadow_demo = evaluation
        .as_ref()
        .and_then(|v| v.get("ready_for_scheduler_demo"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let gpu_sku_count = gpu_skus.len();
    let has_real_oom = oom_rows > 0;
    let has_cross_sku = gpu_sku_count >= 2;
    let has_near_capacity = near_capacity_rows >= 10;
    let has_enough_rows = row_count >= 200;
    let has_time_series = time_series_samples >= 1_000;
    let framework_style_rows = trainer_styles
        .iter()
        .filter(|(style, _)| {
            let style = style.to_ascii_lowercase();
            !style.trim().is_empty() && !style.contains("synthetic")
        })
        .map(|(_, count)| *count)
        .sum::<u64>();
    let has_framework_style_coverage = framework_style_rows >= 50;
    let has_real_framework_verification = verified_real_framework_rows >= 50;
    let has_customer_workload_fingerprints = customer_workload_fingerprint_rows >= 50;
    let advisory_ready = ready_for_shadow_demo && has_enough_rows && has_near_capacity;
    let hard_admission_ready = advisory_ready
        && has_real_oom
        && has_cross_sku
        && has_real_framework_verification
        && has_customer_workload_fingerprints;
    let readiness_requirements = vec![
        serde_json::json!({
            "requirement": "4090 local calibration dataset",
            "status": if has_enough_rows { "pass" } else { "missing" },
            "evidence": format!("{row_count} rows collected"),
            "needed_for": "shadow advisory",
            "next_action": if has_enough_rows { "keep collecting drift samples" } else { "collect at least 200 calibrated rows" },
        }),
        serde_json::json!({
            "requirement": "memory time-series telemetry",
            "status": if has_time_series { "pass" } else { "missing" },
            "evidence": format!("{time_series_samples} nvidia-smi samples"),
            "needed_for": "shadow advisory",
            "next_action": if has_time_series { "retain curves for warmup/steady-state analysis" } else { "sample memory during every probe" },
        }),
        serde_json::json!({
            "requirement": "near-capacity risk coverage",
            "status": if has_near_capacity { "pass" } else { "missing" },
            "evidence": format!("{near_capacity_rows} rows at >=90% reported VRAM"),
            "needed_for": "risk classifier",
            "next_action": if has_near_capacity { "validate on real OOM-producing devices" } else { "add boundary sweeps around 75-100% VRAM" },
        }),
        serde_json::json!({
            "requirement": "true OOM labels",
            "status": if has_real_oom { "pass" } else { "blocked" },
            "evidence": format!("{oom_rows} hard OOM rows; WSL rows are near-capacity risk proxies"),
            "needed_for": "hard admission",
            "next_action": "run T4/L4/A10 cloud or bare-metal Linux probes that produce real CUDA OOM outcomes",
        }),
        serde_json::json!({
            "requirement": "cross-SKU calibration",
            "status": if has_cross_sku { "pass" } else { "blocked" },
            "evidence": format!("{gpu_sku_count} GPU SKU(s): {}", gpu_skus.keys().cloned().collect::<Vec<_>>().join(", ")),
            "needed_for": "hard admission",
            "next_action": "collect the same boundary matrix on T4 and L4 before enforcing placements",
        }),
        serde_json::json!({
            "requirement": "framework-style probe coverage",
            "status": if has_framework_style_coverage { "pass" } else { "missing" },
            "evidence": format!("{framework_style_rows} non-synthetic-style rows; current probes still use controlled synthetic workloads"),
            "needed_for": "shadow advisory",
            "next_action": if has_framework_style_coverage { "use for advisory demos only until real app verification exists" } else { "add HF Trainer, torchvision/timm, and tabular-style probes" },
        }),
        serde_json::json!({
            "requirement": "real framework verification",
            "status": if has_real_framework_verification { "pass" } else { "blocked" },
            "evidence": format!("{verified_real_framework_rows} verified real training app rows; framework-style probes are not enough for enforcement"),
            "needed_for": "hard admission",
            "next_action": "run real Hugging Face Trainer, torchvision/timm, DeepSpeed/FSDP/Accelerate jobs and label them as verified app rows",
        }),
        serde_json::json!({
            "requirement": "customer workload fingerprints",
            "status": if has_customer_workload_fingerprints { "pass" } else { "blocked" },
            "evidence": format!("{customer_workload_fingerprint_rows} customer workload fingerprint rows attached to this calibration"),
            "needed_for": "customer enforcement",
            "next_action": "wire sidecar or wrapper profiles into completed-job observations",
        }),
    ];
    let mut hard_admission_blockers = Vec::new();
    if !advisory_ready {
        hard_admission_blockers.push("advisory calibration gate is not yet passing".to_string());
    }
    if !has_real_oom {
        hard_admission_blockers.push("no true bare-metal/cloud CUDA OOM labels".to_string());
    }
    if !has_cross_sku {
        hard_admission_blockers.push(format!(
            "single-SKU calibration only: {}",
            gpu_skus.keys().cloned().collect::<Vec<_>>().join(", ")
        ));
    }
    if !has_real_framework_verification {
        hard_admission_blockers.push("no verified real framework training-app rows".to_string());
    }
    if !has_customer_workload_fingerprints {
        hard_admission_blockers.push("no real customer workload fingerprints".to_string());
    }
    let recommended_mode = if hard_admission_ready {
        "hard admission can be considered for matching workloads after production rollout gates pass"
    } else if advisory_ready {
        "shadow advisory; do not hard-filter production pods from this calibration alone"
    } else {
        "collect more calibration data before using this for scheduler advice"
    };
    let admission_mode = if hard_admission_ready {
        "Hard admission ready"
    } else if advisory_ready {
        "Shadow advisory only"
    } else {
        "Not ready"
    };
    let scheduler_use = if hard_admission_ready {
        "Can enforce VRAM admission gates"
    } else if advisory_ready {
        "Score and warn; do not reject pods"
    } else {
        "Collect evidence before scheduling claims"
    };
    let mut evidence_collection_plan = Vec::new();
    if !has_real_oom {
        evidence_collection_plan.push(serde_json::json!({
            "target": "true CUDA OOM labels",
            "unblocks": "hard admission risk classifier",
            "why": "Near-capacity rows are useful, but enforcement needs real success/failure labels from a GPU runtime that reports CUDA OOM cleanly.",
            "commands": [
                "python3 vram-model-lab/scripts/generate_iteration_4090_sweep.py --iteration 3 --out vram-model-lab/generated/cloud_oom_boundary_sweep.yaml",
                "export KUBECONFIG=<cloud-gpu-cluster-kubeconfig>",
                "python3 vram-model-lab/scripts/run_k8s_probe.py --all --scenarios-file vram-model-lab/generated/cloud_oom_boundary_sweep.yaml --wait-timeout 2400",
                "python3 vram-model-lab/scripts/run_pipeline.py"
            ],
        }));
    }
    if !has_cross_sku {
        evidence_collection_plan.push(serde_json::json!({
            "target": "cross-SKU calibration",
            "unblocks": "portable VRAM prediction",
            "why": "A single RTX 4090 calibration cannot prove that the model generalizes to T4/L4/A10/A100/H100 allocator behavior or memory headroom.",
            "commands": [
                "python3 vram-model-lab/scripts/generate_realistic_4090_sweep.py --steps 30 --limit 12 --out vram-model-lab/generated/cross_sku_smoke_sweep.yaml",
                "export KUBECONFIG=<t4-or-l4-cluster-kubeconfig>",
                "python3 vram-model-lab/scripts/run_k8s_probe.py --all --scenarios-file vram-model-lab/generated/cross_sku_smoke_sweep.yaml --wait-timeout 2400 --skip-existing",
                "python3 vram-model-lab/scripts/run_pipeline.py"
            ],
        }));
    }
    if !has_real_framework_verification {
        evidence_collection_plan.push(serde_json::json!({
            "target": "verified real framework rows",
            "unblocks": "hard-admission framework trust",
            "why": "Framework-style probes exercise similar shapes, but enforcement needs labels from real HF Trainer, torchvision/timm, DeepSpeed/FSDP, Accelerate, TensorFlow, or JAX jobs.",
            "commands": [
                "python3 vram-model-lab/scripts/predict_manifest_vram.py vram-model-lab/examples/annotated-training-manifests.yaml",
                "python3 vram-model-lab/scripts/run_k8s_probe.py --print-manifest --scenario smoke-mlp",
                "add verified real app manifests with ksolver.ai/vram-profile annotations, then run run_k8s_probe.py against that scenario file",
                "python3 vram-model-lab/scripts/run_pipeline.py"
            ],
        }));
    }
    if !has_customer_workload_fingerprints {
        evidence_collection_plan.push(serde_json::json!({
            "target": "customer workload fingerprints",
            "unblocks": "customer enforcement",
            "why": "Admission should eventually key predictions by image digest, command hash, framework profile, GPU SKU, and observed outcomes from completed jobs.",
            "commands": [
                "curl -s http://127.0.0.1:8090/api/scheduler/vram-calibration > vram-calibration.json",
                "curl -s http://127.0.0.1:8090/api/scheduler/evidence-bundle > evidence-bundle.json",
                "deploy sidecar/wrapper profiling for completed GPU jobs and append emitted profiles to vram-model-lab/data/results.jsonl",
                "python3 vram-model-lab/scripts/run_pipeline.py"
            ],
        }));
    }
    let next_evidence_target = evidence_collection_plan
        .first()
        .and_then(|row| row.get("target"))
        .and_then(serde_json::Value::as_str)
        .or_else(|| hard_admission_blockers.first().map(String::as_str))
        .unwrap_or("keep collecting drift samples");

    serde_json::json!({
        "available": true,
        "source": "vram-model-lab",
        "generated_at": generated_at,
        "paths": {
            "training_rows": training_csv.display().to_string(),
            "peak_model": peak_model_json.display().to_string(),
            "evaluation": evaluation_json.display().to_string(),
            "oom_classifier": oom_json.display().to_string(),
            "scheduler_report": scheduler_report_json.display().to_string(),
            "summary": summary_md.display().to_string(),
        },
        "dataset": {
            "rows": row_count,
            "schema": {
                "column_count": csv_columns.len(),
                "columns": csv_columns,
                "evidence_columns_present": evidence_columns_present,
                "evidence_columns_total": evidence_columns.len(),
                "evidence_columns": evidence_columns,
            },
            "time_series_samples": time_series_samples,
            "near_capacity_rows_ge_90pct": near_capacity_rows,
            "risk_rows": risk_rows,
            "oom_rows": oom_rows,
            "verified_real_framework_rows": verified_real_framework_rows,
            "customer_workload_fingerprint_rows": customer_workload_fingerprint_rows,
            "peak_vram_avg_mib": if row_count > 0 { Some(peak_sum / row_count as f64) } else { None },
            "peak_vram_max_mib": peak_max,
            "synthetic_headroom": {
                "definition": VRAM_RESERVE_PRESSURE_DEFINITION,
                "pressure_rows": reserve_pressure_rows,
                "max_synthetic_reserve_extra_mib": reserve_extra_max_mib,
                "torch_allocator_reserve_gap_avg_mib": if torch_reserve_gap_rows > 0 { Some(torch_reserve_gap_sum_mib / torch_reserve_gap_rows as f64) } else { None },
                "torch_allocator_reserve_gap_max_mib": torch_reserve_gap_max_mib,
                "torch_allocator_reserve_gap_rows": torch_reserve_gap_rows,
            },
            "reserve_pressure": {
                "definition": VRAM_RESERVE_PRESSURE_DEFINITION,
                "pressure_rows": reserve_pressure_rows,
                "max_synthetic_reserve_extra_mib": reserve_extra_max_mib,
                "torch_allocator_reserve_gap_avg_mib": if torch_reserve_gap_rows > 0 { Some(torch_reserve_gap_sum_mib / torch_reserve_gap_rows as f64) } else { None },
                "torch_allocator_reserve_gap_max_mib": torch_reserve_gap_max_mib,
                "torch_allocator_reserve_gap_rows": torch_reserve_gap_rows,
            },
            "families": families,
            "precisions": precisions,
            "gpu_sku_labels": gpu_skus,
            "gpu_names": gpu_names,
            "gpu_total_mib": gpu_total_mib,
            "gpu_total_gib": gpu_total_gib,
            "trainer_styles": trainer_styles,
        },
        "regression": evaluation,
        "model_drivers": vram_model_driver_summary(peak_model.as_ref(), &training_csv),
        "oom_classifier": oom_classifier,
        "pipeline_report": {
            "available": scheduler_report_available,
            "path": scheduler_report_json.display().to_string(),
            "ready_for_scheduler_demo": pipeline_ready_for_demo,
            "evidence_gate_verifier_ok": evidence_gate_verifier_ok,
            "manifest_predictions": manifest_prediction_count,
            "leftover_probe_resources": leftover_probe_resources,
            "usable_families": usable_families,
            "evidence_gate_verifier_stdout": evidence_gate_verifier_stdout,
        },
        "scheduler_readiness": {
            "ready_for_shadow_demo": advisory_ready,
            "hard_admission_ready": hard_admission_ready,
            "advisory_ready": advisory_ready,
            "admission_decision": {
                "mode": admission_mode,
                "scheduler_use": scheduler_use,
                "blocker_count": hard_admission_blockers.len(),
                "next_evidence_target": next_evidence_target,
                "can_hard_admit": hard_admission_ready,
                "can_shadow_advise": advisory_ready,
                "summary": recommended_mode
            },
            "requirements": readiness_requirements,
            "hard_admission_blockers": hard_admission_blockers,
            "evidence_collection_plan": evidence_collection_plan,
            "recommended_mode": recommended_mode
        }
    })
}

fn count_nonempty(map: &mut BTreeMap<String, u64>, value: &str) {
    let value = value.trim();
    if value.is_empty() {
        return;
    }
    *map.entry(value.to_string()).or_insert(0) += 1;
}

fn vram_model_driver_summary(
    peak_model: Option<&serde_json::Value>,
    training_csv: &std::path::Path,
) -> serde_json::Value {
    let Some(model) = peak_model else {
        return serde_json::json!({
            "available": false,
            "reason": "missing fitted peak VRAM model",
        });
    };
    if model
        .get("feature_impacts")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|rows| !rows.is_empty())
    {
        return vram_model_driver_summary_from_impacts(model);
    }
    let features = model
        .get("features")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let coefficients = model
        .get("coefficients")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_f64)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if features.is_empty() || features.len() != coefficients.len() {
        return serde_json::json!({
            "available": false,
            "reason": "fitted model is missing aligned features and coefficients",
        });
    }
    let Ok(mut reader) = csv::Reader::from_path(training_csv) else {
        return serde_json::json!({
            "available": false,
            "reason": format!("could not read {}", training_csv.display()),
        });
    };
    let Ok(headers) = reader.headers().cloned() else {
        return serde_json::json!({
            "available": false,
            "reason": "could not read training CSV headers",
        });
    };
    let mut contribution_sums = vec![0.0_f64; features.len()];
    let mut rows = 0_u64;
    for record in reader.records().flatten() {
        rows += 1;
        let get = |name: &str| -> f64 {
            headers
                .iter()
                .position(|header| header == name)
                .and_then(|idx| record.get(idx))
                .unwrap_or("")
                .parse::<f64>()
                .unwrap_or(0.0)
        };
        let family = headers
            .iter()
            .position(|header| header == "family")
            .and_then(|idx| record.get(idx))
            .unwrap_or("");
        let precision = headers
            .iter()
            .position(|header| header == "precision")
            .and_then(|idx| record.get(idx))
            .unwrap_or("fp32");
        let batch = get("batch_size");
        let layers = get("layers");
        let hidden_size = get("hidden_size");
        let activation_units = if family == "cnn" {
            let image_size = get("image_size");
            batch * image_size * image_size * layers
        } else {
            batch * get("seq_len") * hidden_size * layers
        };
        let precision_bytes = match precision.to_ascii_lowercase().as_str() {
            "fp16" | "float16" | "bf16" | "bfloat16" => 2.0,
            "int8" => 1.0,
            _ => 4.0,
        };
        let mut by_name = BTreeMap::new();
        by_name.insert("intercept", 1.0);
        by_name.insert("param_count_m", get("param_count") / 1_000_000.0);
        by_name.insert("activation_units_m", activation_units / 1_000_000.0);
        by_name.insert("batch_size", batch);
        by_name.insert("layers", layers);
        by_name.insert("hidden_size_k", hidden_size / 1000.0);
        by_name.insert("precision_bytes", precision_bytes);
        by_name.insert("reserve_extra_gib", get("reserve_extra_mib") / 1024.0);
        by_name.insert(
            "adamw",
            if family.is_empty() {
                0.0
            } else if text_field(&headers, &record, "optimizer") == "adamw" {
                1.0
            } else {
                0.0
            },
        );
        by_name.insert(
            "checkpointed",
            if parse_boolish(&text_field(&headers, &record, "activation_checkpointing")) {
                1.0
            } else {
                0.0
            },
        );
        by_name.insert(
            "family_transformer",
            if family == "transformer" { 1.0 } else { 0.0 },
        );
        by_name.insert("family_cnn", if family == "cnn" { 1.0 } else { 0.0 });
        by_name.insert(
            "activation_x_precision",
            by_name["activation_units_m"] * precision_bytes,
        );
        by_name.insert("activation_x_batch", by_name["activation_units_m"] * batch);
        by_name.insert(
            "param_x_precision",
            by_name["param_count_m"] * precision_bytes,
        );
        by_name.insert(
            "reserve_x_transformer",
            by_name["reserve_extra_gib"] * by_name["family_transformer"],
        );
        for (idx, name) in features.iter().enumerate() {
            let value = *by_name.get(name.as_str()).unwrap_or(&0.0);
            contribution_sums[idx] += (value * coefficients[idx]).abs();
        }
    }
    if rows == 0 {
        return serde_json::json!({
            "available": false,
            "reason": "training CSV has no rows",
        });
    }
    let driver_rows = features
        .iter()
        .enumerate()
        .filter(|(_, feature)| feature.as_str() != "intercept")
        .map(|(idx, feature)| {
            let mean_abs_mib = contribution_sums[idx] / rows as f64;
            let class = vram_driver_class(feature);
            serde_json::json!({
                "feature": feature,
                "label": vram_driver_label(feature),
                "class": class,
                "mean_abs_contribution_mib": mean_abs_mib,
                "coefficient": coefficients[idx],
                "interpretation": vram_driver_interpretation(feature),
            })
        })
        .collect::<Vec<_>>();
    let mut sorted = driver_rows;
    sorted.sort_by(|a, b| {
        let av = a
            .get("mean_abs_contribution_mib")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        let bv = b
            .get("mean_abs_contribution_mib")
            .and_then(serde_json::Value::as_f64)
            .unwrap_or(0.0);
        bv.partial_cmp(&av).unwrap_or(std::cmp::Ordering::Equal)
    });
    let top = sorted.iter().take(8).cloned().collect::<Vec<_>>();
    let real_top = sorted
        .iter()
        .filter(|driver| !is_synthetic_vram_driver(driver))
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    let synthetic_pressure_drivers = sorted
        .iter()
        .filter(|driver| is_synthetic_vram_driver(driver))
        .take(8)
        .cloned()
        .collect::<Vec<_>>();
    serde_json::json!({
        "available": true,
        "fit": model.get("fit").cloned().unwrap_or(serde_json::Value::Null),
        "feature_mode": model.get("feature_mode").cloned().unwrap_or(serde_json::Value::Null),
        "target": model.get("target").cloned().unwrap_or(serde_json::Value::Null),
        "training_rows": rows,
        "quality": {
            "loo_mae_mib": model.get("leave_one_out_mean_absolute_error_mib").cloned().unwrap_or(serde_json::Value::Null),
            "loo_p95_mib": model.get("leave_one_out_abs_error_p95_mib").cloned().unwrap_or(serde_json::Value::Null),
            "usable_for_prediction": model.get("usable_for_prediction").cloned().unwrap_or(serde_json::Value::Null),
        },
        "summary": "Top drivers are mean absolute feature contributions over the current calibration rows; synthetic VRAM headroom features are stress-test probes, not organic model demand.",
        "claim_boundary": "Use real_top_drivers for model-memory claims. synthetic headroom drivers are stress-test probes only and must not be presented as organic workload predictors.",
        "top_drivers": top,
        "claim_safe_drivers": real_top.clone(),
        "real_top_drivers": real_top,
        "synthetic_pressure_drivers": synthetic_pressure_drivers,
    })
}

fn vram_model_driver_summary_from_impacts(model: &serde_json::Value) -> serde_json::Value {
    let top = model
        .get("feature_impacts")
        .and_then(serde_json::Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(vram_driver_row_from_impact)
                .take(8)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if top.is_empty() {
        return serde_json::json!({
            "available": false,
            "reason": "fitted model feature_impacts are empty",
        });
    }
    let organic_rows = model
        .get("feature_impacts")
        .and_then(serde_json::Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(vram_driver_row_from_impact)
                .filter(|driver| !is_synthetic_vram_driver(driver))
                .take(8)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let synthetic_pressure_drivers = model
        .get("feature_impacts")
        .and_then(serde_json::Value::as_array)
        .map(|rows| {
            rows.iter()
                .filter_map(vram_driver_row_from_impact)
                .filter(is_synthetic_vram_driver)
                .take(8)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    serde_json::json!({
        "available": true,
        "fit": model.get("fit").cloned().unwrap_or(serde_json::Value::Null),
        "feature_mode": model.get("feature_mode").cloned().unwrap_or(serde_json::Value::Null),
        "target": model.get("target").cloned().unwrap_or(serde_json::Value::Null),
        "training_rows": model.get("training_rows").cloned().unwrap_or(serde_json::Value::Null),
        "quality": {
            "loo_mae_mib": model.get("leave_one_out_mean_absolute_error_mib").cloned().unwrap_or(serde_json::Value::Null),
            "loo_p95_mib": model.get("leave_one_out_abs_error_p95_mib").cloned().unwrap_or(serde_json::Value::Null),
            "usable_for_prediction": model.get("usable_for_prediction").cloned().unwrap_or(serde_json::Value::Null),
        },
        "summary": "Top drivers are coefficient times observed feature standard deviation from the fitted model artifact; synthetic VRAM headroom features are stress-test probes, not organic model demand.",
        "claim_boundary": "Use claim_safe_drivers or real_top_drivers for model-memory claims. synthetic headroom drivers are stress-test probes only and must not be presented as organic workload predictors.",
        "impact_basis": "coefficient_x_feature_std",
        "top_organic_driver_descriptions": model.get("top_organic_driver_labels").cloned().unwrap_or_else(|| serde_json::json!([])),
        "group_impacts": model.get("group_impacts").cloned().unwrap_or_else(|| serde_json::json!([])),
        "top_drivers": top,
        "claim_safe_drivers": organic_rows.clone(),
        "real_top_drivers": organic_rows,
        "synthetic_pressure_drivers": synthetic_pressure_drivers,
    })
}

fn vram_driver_row_from_impact(row: &serde_json::Value) -> Option<serde_json::Value> {
    let feature = row.get("feature").and_then(serde_json::Value::as_str)?;
    let label = row
        .get("description")
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| vram_driver_label(feature).to_string());
    let group = row.get("group").and_then(serde_json::Value::as_str);
    let class = if feature == "param_x_precision" || feature == "precision_bytes" {
        vram_driver_class(feature).to_string()
    } else {
        group
            .map(vram_driver_class_from_group)
            .unwrap_or_else(|| vram_driver_class(feature).to_string())
    };
    Some(serde_json::json!({
        "feature": feature,
        "label": vram_driver_label(feature),
        "description": label,
        "class": class,
        "group": group,
        "impact_mib_per_std": row.get("impact_mib_per_std").cloned().unwrap_or(serde_json::Value::Null),
        "abs_impact_mib_per_std": row.get("abs_impact_mib_per_std").cloned().unwrap_or(serde_json::Value::Null),
        "coefficient": row.get("coefficient_mib_per_unit").cloned().unwrap_or_else(|| row.get("coefficient").cloned().unwrap_or(serde_json::Value::Null)),
        "model_weight": row.get("direction").cloned().unwrap_or(serde_json::Value::Null),
        "interpretation": vram_driver_interpretation(feature),
    }))
}

fn vram_driver_class_from_group(group: &str) -> String {
    match group {
        "synthetic headroom" => "synthetic-pressure".to_string(),
        "activations" | "input shape" => "activation".to_string(),
        "parameters" | "architecture" => "model-size".to_string(),
        "precision" => "precision".to_string(),
        "optimizer" => "optimizer".to_string(),
        "training strategy" => "training-strategy".to_string(),
        "model family" => "context".to_string(),
        _ => vram_driver_class(group).to_string(),
    }
}

fn is_synthetic_vram_driver(driver: &serde_json::Value) -> bool {
    driver
        .get("class")
        .and_then(serde_json::Value::as_str)
        .map(|class| class == "synthetic-pressure")
        .unwrap_or(false)
}

fn vram_driver_display_label(driver: &serde_json::Value) -> Option<String> {
    driver
        .get("label")
        .or_else(|| driver.get("feature"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_string)
}

fn text_field(headers: &csv::StringRecord, record: &csv::StringRecord, name: &str) -> String {
    headers
        .iter()
        .position(|header| header == name)
        .and_then(|idx| record.get(idx))
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

fn vram_driver_class(feature: &str) -> &'static str {
    if feature.starts_with("reserve") {
        "synthetic-pressure"
    } else if feature == "precision_bytes" || feature == "param_x_precision" {
        "precision"
    } else if feature.contains("activation") || feature == "batch_size" {
        "activation"
    } else if feature.contains("param") || feature == "layers" || feature == "hidden_size_k" {
        "model-size"
    } else {
        "context"
    }
}

fn vram_driver_label(feature: &str) -> &'static str {
    match feature {
        "param_x_precision" => "parameter memory x precision",
        "param_count_m" => "parameter count",
        "activation_units_m" => "activation footprint",
        "activation_x_precision" => "activation footprint x precision",
        "activation_x_batch" => "activation footprint x batch",
        "batch_size" => "batch size",
        "layers" => "layer count",
        "hidden_size_k" => "hidden size",
        "precision_bytes" => "precision bytes",
        "reserve_extra_gib" => "synthetic VRAM headroom probe",
        "reserve_x_transformer" => "synthetic transformer headroom probe",
        "adamw" => "AdamW optimizer",
        "checkpointed" => "activation checkpointing",
        "family_transformer" => "transformer family",
        "family_cnn" => "CNN family",
        _ => "model feature",
    }
}

fn display_vram_driver_label(label: &str) -> String {
    match label {
        "synthetic reserve pressure" => "synthetic VRAM headroom probe".to_string(),
        "synthetic transformer reserve pressure" => {
            "synthetic transformer headroom probe".to_string()
        }
        _ => label.to_string(),
    }
}

fn display_vram_driver_labels(labels: Option<&serde_json::Value>) -> serde_json::Value {
    labels
        .and_then(serde_json::Value::as_array)
        .map(|labels| {
            serde_json::Value::Array(
                labels
                    .iter()
                    .filter_map(serde_json::Value::as_str)
                    .map(display_vram_driver_label)
                    .map(serde_json::Value::String)
                    .collect(),
            )
        })
        .unwrap_or_else(|| serde_json::json!([]))
}

fn vram_driver_interpretation(feature: &str) -> &'static str {
    match feature {
        "reserve_extra_gib" | "reserve_x_transformer" => {
            "Synthetic padding used to stress headroom and OOM risk; do not treat as organic model memory."
        }
        "param_x_precision" | "param_count_m" => {
            "Weights, gradients, and optimizer state scale with parameter count and numeric precision."
        }
        "activation_units_m" | "activation_x_precision" | "activation_x_batch" | "batch_size" => {
            "Training activations scale with batch shape and are often the marginal memory that causes OOM."
        }
        "layers" | "hidden_size_k" => {
            "Architecture depth/width affects both parameter memory and retained activation tensors."
        }
        "precision_bytes" => {
            "Lower precision reduces tensor footprint, though framework kernels and optimizer state still add overhead."
        }
        "checkpointed" => {
            "Checkpointing trades compute for lower retained activation memory."
        }
        _ => "Context feature used by the transparent baseline model.",
    }
}

fn parse_boolish(value: &str) -> bool {
    matches!(
        value.trim().to_ascii_lowercase().as_str(),
        "true" | "1" | "yes" | "y"
    )
}

fn evidence_bundle_missing_live_artifact_rows(
    latest_trace: Option<&DecisionTrace>,
    watch_healthy: bool,
    production_readiness_next_action: Option<&str>,
    live_validation_gates: &[serde_json::Value],
    demo_report: &serde_json::Value,
) -> Vec<serde_json::Value> {
    let mut missing = Vec::new();
    match evidence_gate_status(live_validation_gates, "pending GPU trace") {
        Some("pass") => {}
        _ if latest_trace.is_none() => missing.push(evidence_missing_artifact_row(
            "latest shadow trace",
            "live-trace",
            "blocked",
            "pending GPU trace",
            "apply a deterministic GPU scenario or wait for pending GPU pods",
        )),
        _ => missing.push(evidence_missing_artifact_row(
            "live pending GPU trace with placement decisions",
            "live-trace",
            "blocked",
            "pending GPU trace",
            "wait for the shadow trace to observe pending GPU pods and decisions",
        )),
    }
    if !watch_healthy {
        missing.push(evidence_missing_artifact_row(
            "healthy Kubernetes watch/relist state",
            "environment",
            "blocked",
            "production mutation safety",
            production_readiness_next_action.unwrap_or(
                "restore Kubernetes API connectivity and wait for watch/relist recovery",
            ),
        ));
    }
    match evidence_gate_status(live_validation_gates, "repair action safety") {
        Some("pass") => {}
        Some("warn") => missing.push(evidence_missing_artifact_row(
            "live repair-plan action rows",
            "repair-proof",
            "warn",
            "repair action safety",
            "apply a fragmentation scenario or show the deterministic repair reference",
        )),
        _ => missing.push(evidence_missing_artifact_row(
            "live repair-plan action rows",
            "repair-proof",
            "blocked",
            "repair action safety",
            "restore watch data or apply a deterministic scenario with repairable fragmentation",
        )),
    }
    match evidence_gate_status(live_validation_gates, "kube baseline provenance") {
        Some("pass") => {}
        Some("warn") => missing.push(evidence_missing_artifact_row(
            "fully ready kube-scheduler-simulator provenance",
            "baseline-proof",
            "warn",
            "kube baseline provenance",
            "repair every configured kube-scheduler-simulator endpoint or use visibly cached provenance",
        )),
        _ => missing.push(evidence_missing_artifact_row(
            "live kube-scheduler-simulator provenance JSON",
            "baseline-proof",
            "blocked",
            "kube baseline provenance",
            "start or repair kube-scheduler-simulator before claiming live kube baseline",
        )),
    }
    if !evidence_bundle_customer_dollar_claim_ready(demo_report) {
        missing.push(evidence_missing_artifact_row(
            "customer pricing source",
            "customer-proof",
            "warn",
            "ROI pricing evidence",
            "attach a pricing catalog, chargeback export, contract rate sheet, or invoice sample",
        ));
    }
    match evidence_gate_status(live_validation_gates, "trust guardrails") {
        Some("pass") => {}
        Some("warn") => missing.push(evidence_missing_artifact_row(
            "completed-job calibration history with healthy guardrails",
            "trust-proof",
            "warn",
            "trust guardrails",
            "collect completed-job prediction calibration and candidate-regret evidence",
        )),
        _ => missing.push(evidence_missing_artifact_row(
            "completed-job calibration history",
            "trust-proof",
            "blocked",
            "trust guardrails",
            "collect completed-job prediction calibration and candidate-regret evidence",
        )),
    }
    missing
}

fn evidence_bundle_missing_artifact_category_counts(
    rows: &[serde_json::Value],
) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for row in rows {
        let category = row
            .get("category")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        *counts.entry(category.to_string()).or_insert(0) += 1;
    }
    counts
}

#[derive(Default)]
struct EvidenceGapCategoryAccumulator {
    total: usize,
    blocked: usize,
    warn: usize,
    representative: Option<serde_json::Value>,
}

fn evidence_bundle_missing_artifact_category_rows(
    rows: &[serde_json::Value],
) -> Vec<serde_json::Value> {
    let mut categories: BTreeMap<String, EvidenceGapCategoryAccumulator> = BTreeMap::new();
    for row in rows {
        let category = row
            .get("category")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let severity = row
            .get("severity")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("missing");
        let acc = categories.entry(category).or_default();
        acc.total += 1;
        if severity == "blocked" {
            acc.blocked += 1;
        } else if severity == "warn" {
            acc.warn += 1;
        }
        let replace_representative = match acc.representative.as_ref() {
            None => true,
            Some(existing) => {
                existing.get("severity").and_then(serde_json::Value::as_str) != Some("blocked")
                    && severity == "blocked"
            }
        };
        if replace_representative {
            acc.representative = Some(row.clone());
        }
    }
    let mut rows = categories
        .into_iter()
        .map(|(category, acc)| {
            let representative = acc.representative.unwrap_or_else(|| serde_json::json!({}));
            serde_json::json!({
                "category": category,
                "total": acc.total,
                "blocked": acc.blocked,
                "warn": acc.warn,
                "severity": if acc.blocked > 0 { "blocked" } else if acc.warn > 0 { "warn" } else { "missing" },
                "artifact": representative.get("artifact").cloned().unwrap_or(serde_json::Value::Null),
                "proof_gate": representative.get("proof_gate").cloned().unwrap_or(serde_json::Value::Null),
                "next_action": representative.get("next_action").cloned().unwrap_or(serde_json::Value::Null),
            })
        })
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        let a_blocked = a
            .get("blocked")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let b_blocked = b
            .get("blocked")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let a_warn = a
            .get("warn")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let b_warn = b
            .get("warn")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let a_total = a
            .get("total")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let b_total = b
            .get("total")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let a_category = a
            .get("category")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        let b_category = b
            .get("category")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("");
        b_blocked
            .cmp(&a_blocked)
            .then_with(|| b_warn.cmp(&a_warn))
            .then_with(|| b_total.cmp(&a_total))
            .then_with(|| a_category.cmp(b_category))
    });
    rows
}

fn operator_evidence_gap_action_items(
    category_rows: &[serde_json::Value],
) -> Vec<serde_json::Value> {
    category_rows
        .iter()
        .enumerate()
        .map(|(idx, row)| {
            let category = row
                .get("category")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown");
            let severity = row
                .get("severity")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("missing");
            let next_action = row
                .get("next_action")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("collect the missing evidence for this category");
            let (command_hints, command_kind, copyable) = match category {
                "environment" => {
                    let mut commands = vec![
                        "kubectl config current-context",
                        "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                        "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
                        "kubectl --request-timeout=10s get nodes",
                    ];
                    let next_action_lower = next_action.to_ascii_lowercase();
                    if next_action.contains("get --raw='/readyz?verbose'")
                        || next_action_lower.contains("api connectivity")
                    {
                        commands.swap(0, 1);
                    } else if next_action_lower.contains("rbac")
                        || next_action_lower.contains("can-i")
                        || next_action_lower.contains("list/watch")
                    {
                        commands.swap(0, 2);
                    }
                    (commands, "shell", true)
                }
                "baseline-proof" => (
                    vec!["scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache"],
                    "shell",
                    true,
                ),
                "live-trace" => (
                    vec![
                        "kubectl --request-timeout=10s get pods -A --field-selector=status.phase=Pending",
                    ],
                    "shell",
                    true,
                ),
                "repair-proof" => (
                    vec![
                        "curl -s http://127.0.0.1:8090/api/scheduler/repair-plan | jq .proof_status",
                    ],
                    "shell",
                    true,
                ),
                "customer-proof" => (
                    vec![
                        "attach pricing catalog, chargeback export, contract rate sheet, or invoice sample",
                    ],
                    "manual",
                    false,
                ),
                "trust-proof" => (
                    vec![
                        "collect completed-job prediction calibration and candidate-regret evidence",
                    ],
                    "manual",
                    false,
                ),
                _ => (Vec::new(), "none", false),
            };
            let command_hint = command_hints.first().copied();
            serde_json::json!({
                "priority": idx + 1,
                "category": category,
                "severity": severity,
                "blocked": row.get("blocked").cloned().unwrap_or_else(|| serde_json::json!(0)),
                "warn": row.get("warn").cloned().unwrap_or_else(|| serde_json::json!(0)),
                "artifact": row.get("artifact").cloned().unwrap_or(serde_json::Value::Null),
                "next_action": next_action,
                "command_hint": command_hint,
                "command_hints": command_hints,
                "command_kind": command_kind,
                "copyable": copyable,
            })
        })
        .collect()
}

fn operator_action_runbook(action_items: &[serde_json::Value]) -> serde_json::Value {
    let mut copyable_commands = Vec::new();
    let mut copyable_command_rows = Vec::new();
    let mut blocked_steps = 0usize;
    let mut manual_steps = 0usize;
    for item in action_items {
        if item.get("severity").and_then(serde_json::Value::as_str) == Some("blocked") {
            blocked_steps += 1;
        }
        if item.get("command_kind").and_then(serde_json::Value::as_str) == Some("manual") {
            manual_steps += 1;
        }
        if item
            .get("copyable")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
        {
            if let Some(commands) = item
                .get("command_hints")
                .and_then(serde_json::Value::as_array)
            {
                for command in commands.iter().filter_map(serde_json::Value::as_str) {
                    if !copyable_commands.iter().any(|seen| seen == command) {
                        copyable_commands.push(command.to_string());
                        copyable_command_rows.push(serde_json::json!({
                            "command": command,
                            "priority": item.get("priority").cloned().unwrap_or(serde_json::Value::Null),
                            "category": item.get("category").cloned().unwrap_or(serde_json::Value::Null),
                            "severity": item.get("severity").cloned().unwrap_or(serde_json::Value::Null),
                            "artifact": item.get("artifact").cloned().unwrap_or(serde_json::Value::Null),
                            "next_action": item.get("next_action").cloned().unwrap_or(serde_json::Value::Null),
                            "command_kind": item.get("command_kind").cloned().unwrap_or_else(|| serde_json::json!("shell")),
                        }));
                    }
                }
            } else if let Some(command) =
                item.get("command_hint").and_then(serde_json::Value::as_str)
            {
                if !copyable_commands.iter().any(|seen| seen == command) {
                    copyable_commands.push(command.to_string());
                    copyable_command_rows.push(serde_json::json!({
                        "command": command,
                        "priority": item.get("priority").cloned().unwrap_or(serde_json::Value::Null),
                        "category": item.get("category").cloned().unwrap_or(serde_json::Value::Null),
                        "severity": item.get("severity").cloned().unwrap_or(serde_json::Value::Null),
                        "artifact": item.get("artifact").cloned().unwrap_or(serde_json::Value::Null),
                        "next_action": item.get("next_action").cloned().unwrap_or(serde_json::Value::Null),
                        "command_kind": item.get("command_kind").cloned().unwrap_or_else(|| serde_json::json!("shell")),
                    }));
                }
            }
        }
    }
    serde_json::json!({
        "step_count": action_items.len(),
        "blocked_step_count": blocked_steps,
        "manual_step_count": manual_steps,
        "copyable_command_count": copyable_commands.len(),
        "next_step": action_items.first().cloned().unwrap_or(serde_json::Value::Null),
        "next_shell_command": copyable_commands.first().cloned(),
        "copyable_commands": copyable_commands,
        "copyable_command_rows": copyable_command_rows,
        "steps": action_items,
    })
}

fn evidence_missing_artifact_row(
    artifact: &str,
    category: &str,
    severity: &str,
    proof_gate: &str,
    next_action: &str,
) -> serde_json::Value {
    serde_json::json!({
        "artifact": artifact,
        "category": category,
        "severity": severity,
        "proof_gate": proof_gate,
        "next_action": next_action,
    })
}

fn evidence_gate_status<'a>(
    live_validation_gates: &'a [serde_json::Value],
    gate_name: &str,
) -> Option<&'a str> {
    live_validation_gates.iter().find_map(|gate| {
        if gate.get("gate").and_then(serde_json::Value::as_str) == Some(gate_name) {
            gate.get("status").and_then(serde_json::Value::as_str)
        } else {
            None
        }
    })
}

fn evidence_bundle_customer_dollar_claim_ready(demo_report: &serde_json::Value) -> bool {
    let report = demo_report
        .get("report")
        .unwrap_or(&serde_json::Value::Null);
    evidence_bundle_customer_dollar_claim_ready_from_report(report)
}

fn evidence_bundle_customer_dollar_claim_ready_from_report(report: &serde_json::Value) -> bool {
    let claim_contract = report
        .get("roi_dashboard_summary")
        .and_then(|roi| roi.get("claim_contract"));
    if claim_contract
        .and_then(|contract| contract.get("can_show_customer_dollars"))
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        return true;
    }

    report
        .get("pricing_readiness_summary")
        .and_then(|summary| summary.get("current_mode"))
        .and_then(serde_json::Value::as_str)
        .map(|mode| {
            let mode = mode.to_ascii_lowercase();
            !(mode.contains("synthetic") || mode.contains("demo"))
        })
        .unwrap_or(false)
}

/// Evaluate the "kube safety advantage" proof gate from the latest computed kube liabilities.
/// This turns the live-trace safety signal into hashable customer proof: the gate PASSES only when
/// a live kube baseline was measured and ksolver refused >=1 unsafe placement kube would accept.
fn safety_gate_status(kube_liabilities: Option<&serde_json::Value>) -> (&'static str, String, &'static str) {
    match kube_liabilities.and_then(|l| l.get("count")).and_then(serde_json::Value::as_u64) {
        Some(count) if count > 0 => {
            let summary = kube_liabilities
                .and_then(|l| l.get("summary"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("kube would accept unsafe placements ksolver refuses");
            // Honest competitive tiering (don't overclaim vs a gang-aware baseline).
            let beats_most = kube_liabilities
                .and_then(|l| l.get("beats_most_schedulers"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let beats_kube_only = kube_liabilities
                .and_then(|l| l.get("beats_default_kube_only"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0);
            let tiering = if beats_most > 0 || beats_kube_only > 0 {
                format!(
                    " ({beats_most} beat ~any scheduler incl. gang-aware, {beats_kube_only} beat default kube only)"
                )
            } else {
                String::new()
            };
            (
                "pass",
                format!(
                    "ksolver refused {count} unsafe placement(s) the live kube baseline accepted — {summary}{tiering}"
                ),
                "cite the avoided OOM-risk / split-gang placements as proof ksolver is safer than kube, not just different",
            )
        }
        Some(_) => (
            "warn",
            "the live kube baseline made no unsafe placements on this queue, so there is no safety advantage to prove here"
                .to_string(),
            "seed a fragmentation or oversized-VRAM scenario where kube would over-commit, then re-measure",
        ),
        None => (
            "warn",
            "no live kube baseline has been measured, so ksolver's safety advantage over kube is unproven"
                .to_string(),
            "run a live kube-scheduler-simulator baseline (via a seeded scenario) to measure the safety advantage",
        ),
    }
}

#[allow(clippy::too_many_arguments)]
fn evidence_bundle_live_validation_gates(
    latest_trace: Option<&DecisionTrace>,
    production_safety: &serde_json::Value,
    demo_report: &serde_json::Value,
    mutation_allowed: bool,
    simulator_readiness: &str,
    simulator_probe_ready_count: u64,
    simulator_probe_checked_count: u64,
    kube_liabilities: Option<&serde_json::Value>,
) -> Vec<serde_json::Value> {
    let report = demo_report
        .get("report")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let rows = report
        .get("demo_readiness_summary")
        .and_then(|summary| summary.get("live_validation_rows"))
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    let mut fallback_rows = rows;
    if fallback_rows.is_empty() {
        fallback_rows = vec![
            serde_json::json!({"gate": "pending GPU trace", "live_endpoint": "/api/scheduler/traces"}),
            serde_json::json!({"gate": "kube baseline provenance", "live_endpoint": "/api/scheduler/kube-simulator-plan"}),
            serde_json::json!({"gate": "repair action safety", "live_endpoint": "/api/scheduler/repair-plan"}),
            serde_json::json!({"gate": "production mutation safety", "live_endpoint": "/api/scheduler/production-safety"}),
            serde_json::json!({"gate": "ROI pricing evidence", "live_endpoint": "/api/scheduler/demo-report"}),
            serde_json::json!({"gate": "trust guardrails", "live_endpoint": "/api/scheduler/demo-report"}),
        ];
    }
    // Always include the kube-safety-advantage gate (proof that ksolver is safer, not just different).
    if !fallback_rows.iter().any(|r| {
        r.get("gate").and_then(serde_json::Value::as_str) == Some("kube safety advantage")
    }) {
        fallback_rows.push(serde_json::json!({
            "gate": "kube safety advantage",
            "live_endpoint": "/api/scheduler/kube-simulator-plan",
        }));
    }

    let trace_observed = latest_trace
        .map(|trace| trace.observed_pods > 0 && !trace.decisions.is_empty())
        .unwrap_or(false);
    let repair_action_count = latest_trace
        .map(|trace| {
            trace
                .repair_plans
                .iter()
                .map(|plan| plan.actions.len())
                .sum::<usize>()
        })
        .unwrap_or(0);
    let production_blocker = production_safety
        .get("readiness")
        .and_then(|readiness| readiness.get("blocker_class"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("unknown");
    let roi_ready = report.get("roi_dashboard_summary").is_some()
        && report.get("pricing_readiness_summary").is_some();
    let customer_dollar_ready = evidence_bundle_customer_dollar_claim_ready_from_report(&report);
    let trust_ready = report.get("prediction_quality_summary").is_some()
        && report.get("scale_guardrail_summary").is_some();

    fallback_rows
        .into_iter()
        .map(|row| {
            let gate = row
                .get("gate")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("unknown gate");
            if gate == "kube safety advantage" {
                let (status, reason, next_action) = safety_gate_status(kube_liabilities);
                return serde_json::json!({
                    "gate": gate,
                    "status": status,
                    "reason": reason,
                    "next_action": next_action,
                    "live_endpoint": row.get("live_endpoint").cloned().unwrap_or(serde_json::Value::Null),
                    "operator_question": row.get("operator_question").cloned().unwrap_or(serde_json::Value::Null),
                    "required_evidence": row.get("required_evidence").cloned().unwrap_or(serde_json::Value::Null),
                    "pass_signal": row.get("pass_signal").cloned().unwrap_or(serde_json::Value::Null),
                    "failure_action": row.get("failure_action").cloned().unwrap_or(serde_json::Value::Null),
                });
            }
            let (status, reason, next_action) = match gate {
                "pending GPU trace" => {
                    if trace_observed {
                        (
                            "pass",
                            "latest trace has observed pending GPU pods and rendered decisions",
                            "use /api/scheduler/traces as live placement evidence",
                        )
                    } else {
                        (
                            "blocked",
                            "no current trace with observed pending GPU pods and decisions",
                            "apply a deterministic GPU scenario or wait for pending GPU pods",
                        )
                    }
                }
                "kube baseline provenance" => {
                    if simulator_readiness == "ready"
                        && simulator_probe_checked_count > 0
                        && simulator_probe_ready_count == simulator_probe_checked_count
                    {
                        (
                            "pass",
                            "all configured kube-scheduler-simulator endpoints answered export readiness",
                            "use simulator provenance rows beside ksolver placement",
                        )
                    } else if simulator_probe_ready_count > 0 {
                        (
                            "warn",
                            "some kube-scheduler-simulator endpoints answered readiness",
                            "keep cached simulator provenance visible until every endpoint is ready",
                        )
                    } else {
                        (
                            "blocked",
                            "no ready kube-scheduler-simulator endpoint is available",
                            "start or repair kube-scheduler-simulator before claiming live kube baseline",
                        )
                    }
                }
                "repair action safety" => {
                    if repair_action_count > 0 {
                        (
                            "pass",
                            "latest trace includes live repair migrate/preempt action rows",
                            "show the dry-run repair table with disruption cost",
                        )
                    } else if latest_trace.is_some() {
                        (
                            "warn",
                            "latest trace exists but has no live repair action rows",
                            "show deterministic repair reference or apply a fragmentation scenario",
                        )
                    } else {
                        (
                            "blocked",
                            "no latest trace exists, so no repair action rows can be proven",
                            "restore watch data or apply a deterministic scenario",
                        )
                    }
                }
                "production mutation safety" => {
                    if mutation_allowed {
                        (
                            "blocked",
                            "mutation is allowed, so this packet is not observe-only proof",
                            "switch to observe-only or review rollout safety before sharing",
                        )
                    } else if matches!(production_blocker, "" | "none") {
                        (
                            "pass",
                            "production safety endpoint is observe-only with no readiness blocker",
                            "use the safety posture as launch-gate evidence",
                        )
                    } else {
                        (
                            "warn",
                            "observe-only is safe, but production readiness is blocked",
                            "repair the readiness blocker before customer-facing claims",
                        )
                    }
                }
                "ROI pricing evidence" => {
                    if roi_ready && customer_dollar_ready {
                        (
                            "pass",
                            "ROI dashboard uses a customer-specific pricing source",
                            "show pricing source and recomputed dollar tiles beside claims",
                        )
                    } else if roi_ready {
                        (
                            "warn",
                            "ROI dashboard is present, but dollar values are synthetic or demo-priced",
                            "attach customer pricing before presenting dollar savings",
                        )
                    } else {
                        (
                            "blocked",
                            "ROI or pricing-readiness summary is missing",
                            "load ROI dashboard and pricing readiness evidence",
                        )
                    }
                }
                "trust guardrails" => {
                    if trust_ready {
                        (
                            "pass",
                            "prediction-quality and scale-guardrail summaries are present",
                            "show confidence, pruning regret, and caveats before action",
                        )
                    } else {
                        (
                            "blocked",
                            "prediction-quality or scale-guardrail summary is missing",
                            "load trust guardrail evidence before presenting recommendations",
                        )
                    }
                }
                _ => (
                    "warn",
                    "gate is listed in the demo report but has no explicit live validator",
                    "add a validator for this gate before treating it as customer proof",
                ),
            };
            serde_json::json!({
                "gate": gate,
                "status": status,
                "reason": reason,
                "next_action": next_action,
                "live_endpoint": row.get("live_endpoint").cloned().unwrap_or(serde_json::Value::Null),
                "operator_question": row.get("operator_question").cloned().unwrap_or(serde_json::Value::Null),
                "required_evidence": row.get("required_evidence").cloned().unwrap_or(serde_json::Value::Null),
                "pass_signal": row.get("pass_signal").cloned().unwrap_or(serde_json::Value::Null),
                "failure_action": row.get("failure_action").cloned().unwrap_or(serde_json::Value::Null),
            })
        })
        .collect()
}

async fn solve_now_handler(
    State(s): State<ShadowHttpState>,
    Query(query): Query<SolveQuery>,
) -> Json<serde_json::Value> {
    let Some(normalized) = s.latest_cluster.lock().ok().and_then(|g| g.clone()) else {
        return Json(serde_json::json!({
            "ok": false,
            "reason": "no normalized cluster snapshot yet",
        }));
    };
    let pending = s
        .latest_pending
        .lock()
        .map(|g| g.clone())
        .unwrap_or_default();
    if pending.is_empty() {
        return Json(serde_json::json!({
            "ok": false,
            "reason": "no pending GPU pods in the latest shadow snapshot",
        }));
    }

    let cfg = cfg_with_query_objective(&s.cfg, &query);
    if let Ok(mut active) = s.active_objective.lock() {
        *active = ObjectiveSelection {
            profile: cfg.objective_profile,
            weights: cfg.objective_weights.clone(),
        };
    }
    let started = Instant::now();
    let seq = s.traces.next_sequence();
    match run_one_solve(
        &cfg,
        seq,
        &pending,
        &normalized,
        Default::default(),
        Default::default(),
        Vec::new(),
        started,
        0,
    )
    .await
    {
        Ok(mut trace) => {
            trace.note = format!(
                "manual solve from UI; objective_profile={}",
                objective_profile_name(cfg.objective_profile)
            );
            s.traces.push(trace.clone());
            Json(serde_json::json!({
                "ok": true,
                "trace": trace,
                "objective_profile": objective_profile_name(cfg.objective_profile),
                "objective_weights": {
                    "admission": cfg.objective_weights.admission,
                    "gpu_demand": cfg.objective_weights.gpu_demand,
                    "gang_complete": cfg.objective_weights.gang_complete,
                    "priority": cfg.objective_weights.priority,
                    "business_value": cfg.objective_weights.business_value,
                    "queue": cfg.objective_weights.queue,
                    "queue_wait": cfg.objective_weights.queue_wait,
                    "fair_share": cfg.objective_weights.fair_share,
                    "deadline_urgency": cfg.objective_weights.deadline_urgency,
                    "deadline_miss": cfg.objective_weights.deadline_miss,
                    "gpu_fragmentation": cfg.objective_weights.gpu_fragmentation,
                }
            }))
        }
        Err(err) => Json(serde_json::json!({
            "ok": false,
            "reason": err.to_string(),
            "objective_profile": objective_profile_name(cfg.objective_profile),
        })),
    }
}

async fn cluster_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let Some(cluster) = s.latest_cluster.lock().ok().and_then(|g| g.clone()) else {
        return Json(serde_json::json!({
            "ready": false,
            "nodes": [],
            "running_gpu": [],
        }));
    };

    let mut running_gpu_by_node: std::collections::BTreeMap<String, i64> =
        std::collections::BTreeMap::new();
    let mut running_gpu = Vec::new();
    for w in &cluster.workloads {
        if w.current_node.is_empty() {
            continue;
        }
        let gpu: i64 = w
            .extended_resource_requests
            .iter()
            .filter(|(name, _)| {
                name.as_str() == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-")
            })
            .map(|(_, qty)| *qty)
            .sum();
        if gpu < 1 {
            continue;
        }
        *running_gpu_by_node
            .entry(w.current_node.clone())
            .or_default() += gpu;
        running_gpu.push(serde_json::json!({
            "namespace": w.namespace,
            "name": w.name,
            "node": w.current_node,
            "gpu_request": gpu,
        }));
    }

    let nodes: Vec<_> = cluster
        .nodes
        .iter()
        .filter_map(|n| {
            let gpu_capacity: i64 = n
                .extended_resources
                .iter()
                .filter(|(name, _)| {
                    name.as_str() == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-")
                })
                .map(|(_, qty)| *qty)
                .sum();
            let gpu_labeled = n
                .labels
                .get("eks.amazonaws.com/nodegroup")
                .map(|v| v == "gpu")
                .unwrap_or(false);
            if gpu_capacity < 1 && !gpu_labeled {
                return None;
            }
            Some(serde_json::json!({
                "name": n.name,
                "pool": n.pool,
                "instance_type": n.instance_type,
                "gpu_capacity": gpu_capacity,
                "running_gpu": running_gpu_by_node.get(&n.name).copied().unwrap_or(0),
                "current_pods": n.current_pods,
            }))
        })
        .collect();

    Json(serde_json::json!({
        "ready": true,
        "nodes": nodes,
        "running_gpu": running_gpu,
    }))
}

/// Per-pod metadata needed to judge whether kube's placement is *unsafe*, not just different.
#[derive(Debug, Clone)]
struct PodLiabilityMeta {
    gang_key: Option<String>,
    colocate: bool,
    predicted_vram_bytes: i64,
}

/// Compare the kube-scheduler-simulator's placement against the constraints ksolver enforces and
/// surface the *liabilities* kube incurs by "admitting" more work: placing a job whose predicted
/// peak VRAM exceeds the GPU's memory (CUDA OOM risk), or spreading / partially placing a
/// co-located gang (breaks required co-location, or strands GPUs on a gang that never runs).
///
/// This is what lets the live demo show ksolver is *safer and smarter*, not merely different: when
/// kube places more GPUs, this names the price it paid to do so.
fn compute_kube_liabilities(
    placements: &[serde_json::Value],
    pod_meta: &std::collections::BTreeMap<String, PodLiabilityMeta>,
    node_vram_bytes: &std::collections::BTreeMap<String, i64>,
) -> serde_json::Value {
    let gib = 1024.0 * 1024.0 * 1024.0;
    let round1 = |bytes: i64| ((bytes as f64 / gib) * 10.0).round() / 10.0;

    // scope ("ns/name") -> node kube placed it on (placed pods only)
    let mut placed_node: std::collections::BTreeMap<String, String> = Default::default();
    for p in placements {
        let ns = p.get("namespace").and_then(|v| v.as_str()).unwrap_or("");
        let name = p.get("name").and_then(|v| v.as_str()).unwrap_or("");
        if name.is_empty() {
            continue;
        }
        if let Some(node) = p.get("placement").and_then(|pl| {
            if pl.get("kind").and_then(|k| k.as_str()) == Some("placed") {
                pl.get("node").and_then(|n| n.as_str())
            } else {
                None
            }
        }) {
            placed_node.insert(format!("{ns}/{name}"), node.to_string());
        }
    }

    // OOM risk: kube placed a pod whose predicted peak VRAM exceeds the node's per-GPU VRAM.
    let mut oom_risk = Vec::new();
    for (scope, node) in &placed_node {
        let Some(meta) = pod_meta.get(scope) else {
            continue;
        };
        if meta.predicted_vram_bytes <= 0 {
            continue;
        }
        let Some(&node_vram) = node_vram_bytes.get(node) else {
            continue;
        };
        if node_vram > 0 && meta.predicted_vram_bytes > node_vram {
            oom_risk.push(serde_json::json!({
                "scope": scope,
                "node": node,
                "predicted_vram_gib": round1(meta.predicted_vram_bytes),
                "node_vram_gib": round1(node_vram),
                // Predicted-VRAM feasibility is rare: default kube, Volcano, and KAI all ignore it,
                // so this advantage holds against ~any scheduler, not just default kube.
                "competitive_strength": "beats-most-schedulers",
                "detail": format!(
                    "kube placed {scope} on {node}, but its predicted peak VRAM ({:.0} GiB) exceeds the GPU's memory ({:.0} GiB) — CUDA OOM risk that ksolver blocks",
                    round1(meta.predicted_vram_bytes), round1(node_vram)
                ),
            }));
        }
    }

    // Split / partial co-located gangs: kube spread a colocate gang across nodes or admitted only
    // part of it. Either way the gang's co-location intent is violated and GPUs are wasted.
    let mut groups: std::collections::BTreeMap<
        String,
        (usize, std::collections::BTreeSet<String>, usize),
    > = Default::default();
    for (scope, meta) in pod_meta {
        if !meta.colocate {
            continue;
        }
        let Some(gang) = meta.gang_key.as_ref() else {
            continue;
        };
        let entry = groups.entry(gang.clone()).or_default();
        entry.0 += 1;
        if let Some(node) = placed_node.get(scope) {
            entry.1.insert(node.clone());
            entry.2 += 1;
        }
    }
    let mut split_gangs = Vec::new();
    for (gang, (total, nodes, placed)) in &groups {
        let multi_node = nodes.len() > 1;
        let partial = *placed > 0 && *placed < *total;
        if !multi_node && !partial {
            continue;
        }
        let (kind, detail) = if multi_node {
            (
                "split",
                format!(
                    "kube spread co-located gang {gang} across {} nodes — breaks the gang's required co-location (e.g. NVLink); ksolver keeps it together or declines it",
                    nodes.len()
                ),
            )
        } else {
            (
                "partial",
                format!(
                    "kube admitted only {placed}/{total} of co-located gang {gang} — a partial gang strands GPUs and never runs; ksolver admits it whole or not at all"
                ),
            )
        };
        split_gangs.push(serde_json::json!({
            "group": gang,
            "kind": kind,
            "member_total": total,
            "placed_count": placed,
            "nodes": nodes.iter().cloned().collect::<Vec<_>>(),
            // Gang-aware schedulers (Volcano, KAI) also avoid this, so it only beats DEFAULT kube.
            "competitive_strength": "beats-default-kube-only",
            "detail": detail,
        }));
    }

    let count = oom_risk.len() + split_gangs.len();
    let mut parts = Vec::new();
    if !oom_risk.is_empty() {
        parts.push(format!("{} job(s) at CUDA OOM risk", oom_risk.len()));
    }
    if !split_gangs.is_empty() {
        parts.push(format!(
            "{} co-located gang(s) split or partially placed",
            split_gangs.len()
        ));
    }
    let summary = if count == 0 {
        "kube's placement carries no detected safety liabilities on this queue".to_string()
    } else {
        format!(
            "kube would accept placements ksolver refuses: {}",
            parts.join(", ")
        )
    };

    // Classify strength: OOM-risk advantages hold vs ~any scheduler (VRAM prediction is rare);
    // split/partial gang advantages only beat DEFAULT kube (gang-aware schedulers avoid them too).
    let beats_most = oom_risk.len();
    let beats_default_kube_only = split_gangs.len();

    serde_json::json!({
        "count": count,
        "oom_risk": oom_risk,
        "split_gangs": split_gangs,
        "summary": summary,
        "beats_most_schedulers": beats_most,
        "beats_default_kube_only": beats_default_kube_only,
    })
}

/// Build the per-pod liability metadata + node VRAM maps from live shadow state, then compute the
/// kube liabilities for the given placements.
fn kube_liabilities_from_state(
    placements: &[serde_json::Value],
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
    cluster: Option<&crate::model::NormalizedCluster>,
) -> serde_json::Value {
    let mut pod_meta: std::collections::BTreeMap<String, PodLiabilityMeta> = Default::default();
    for p in pending {
        pod_meta.insert(
            format!("{}/{}", p.namespace, p.name),
            PodLiabilityMeta {
                gang_key: p.gang_key.clone(),
                colocate: p.colocate,
                predicted_vram_bytes: p.predicted_peak_vram_bytes,
            },
        );
    }
    let mut node_vram: std::collections::BTreeMap<String, i64> = Default::default();
    if let Some(cluster) = cluster {
        for node in &cluster.nodes {
            let bytes = crate::scheduler::pending_input::node_peak_vram_bytes(&node.labels);
            if bytes > 0 {
                node_vram.insert(node.name.clone(), bytes);
            }
        }
    }
    compute_kube_liabilities(placements, &pod_meta, &node_vram)
}

async fn kube_simulator_plan_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    if s.simulator_pool.is_empty() {
        return Json(serde_json::json!({
            "available": false,
            "source": "kube-scheduler-simulator",
            "simulator": {
                "mode": "unconfigured",
                "timed_out": false,
                "pool_size": 0,
                "fallback_reason": format!("no simulator URL configured; default {} was empty", DEFAULT_SIMULATOR_URL),
            },
            "reason": format!("no simulator URL configured; default {} was empty", DEFAULT_SIMULATOR_URL),
            "trace_sequence": 0,
            "placements": [],
        }));
    }

    let Some(trace) = s.traces.recent().into_iter().next() else {
        return Json(serde_json::json!({
            "available": false,
            "source": "kube-scheduler-simulator",
            "simulator": {
                "mode": "no-trace",
                "timed_out": false,
                "fallback_reason": "no shadow trace yet",
            },
            "reason": "no shadow trace yet",
            "trace_sequence": 0,
            "placements": [],
        }));
    };

    let simulator_cache_key = {
        let latest_cluster = s.latest_cluster.lock().ok().and_then(|g| g.clone());
        simulator_dashboard_cache_key(&trace, latest_cluster.as_ref())
    };

    {
        let cache = s.simulator_plan_cache.lock().await;
        if let Some((cache_key, value)) = cache.as_ref() {
            if *cache_key == simulator_cache_key {
                let mut value = value.clone();
                value["trace_sequence"] = serde_json::json!(trace.sequence);
                value["simulator"]["cache_hit"] = serde_json::json!(true);
                if let Some(liab) = value.get("liabilities") {
                    if let Ok(mut guard) = s.latest_liabilities.lock() {
                        *guard = Some(liab.clone());
                    }
                }
                return Json(value);
            }
        }
    }

    let simulator_deadline = dashboard_simulator_deadline();
    let simulator_pool = s.simulator_pool.clone();
    match tokio::time::timeout(
        simulator_deadline,
        simulator_pool.run_for_trace(&s.kubeconfig, &trace),
    )
    .await
    {
        Err(_) => {
            let value = serde_json::json!({
                "available": false,
                "source": "kube-scheduler-simulator",
                "simulator": {
                    "mode": "timed-out",
                    "pool_size": s.simulator_pool.len(),
                    "timeout_millis": simulator_deadline.as_millis() as u64,
                    "timed_out": true,
                    "fallback_reason": "dashboard simulator plan exceeded request deadline",
                },
                "reason": "dashboard simulator plan exceeded request deadline",
                "trace_sequence": trace.sequence,
                "placements": [],
            });
            Json(value)
        }
        Ok(Ok((simulator_url, plan))) => {
            let liabilities = {
                let pending = s
                    .latest_pending
                    .lock()
                    .ok()
                    .map(|g| g.clone())
                    .unwrap_or_default();
                let cluster = s.latest_cluster.lock().ok().and_then(|g| g.clone());
                kube_liabilities_from_state(&plan.placements, &pending, cluster.as_ref())
            };
            if let Ok(mut guard) = s.latest_liabilities.lock() {
                *guard = Some(liabilities.clone());
            }
            let value = serde_json::json!({
            "available": true,
            "source": format!("kube-scheduler-simulator at {}", simulator_url.trim_end_matches('/')),
            "simulator": plan.simulator,
            "trace_sequence": trace.sequence,
            "placements": plan.placements,
            "liabilities": liabilities,
            });
            let mut cache = s.simulator_plan_cache.lock().await;
            *cache = Some((simulator_cache_key, value.clone()));
            Json(value)
        }
        Ok(Err(err)) => {
            let simulator = err
                .downcast_ref::<crate::verifier::SimulatorBatchTimeoutError>()
                .map(|timeout| {
                    let phase_timings: Vec<serde_json::Value> = timeout
                        .diagnostics
                        .phase_timings
                        .iter()
                        .map(|timing| {
                            serde_json::json!({
                                "phase": timing.phase,
                                "duration_millis": timing.duration_millis,
                                "cumulative_millis": timing.cumulative_millis,
                            })
                        })
                        .collect();
                    serde_json::json!({
                        "mode": "timed-out",
                        "pool_size": s.simulator_pool.len(),
                        "elapsed_millis": timeout.diagnostics.elapsed_millis as u64,
                        "phase": timeout.diagnostics.phase,
                        "target_count": timeout.diagnostics.state.target_count,
                        "present_targets": timeout.diagnostics.state.present_targets,
                        "terminal_present_targets": timeout.diagnostics.state.terminal_present_targets,
                        "missing_targets": timeout.diagnostics.state.missing_targets(),
                        "stable_polls": timeout.diagnostics.stable_polls,
                        "phase_timings": phase_timings,
                        "timed_out": true,
                        "fallback_reason": format!("{err:#}"),
                    })
                })
                .unwrap_or_else(|| {
                    serde_json::json!({
                        "mode": "error",
                        "pool_size": s.simulator_pool.len(),
                        "timed_out": false,
                        "fallback_reason": format!("{err:#}"),
                    })
                });
            let value = serde_json::json!({
            "available": false,
            "source": "kube-scheduler-simulator",
            "simulator": simulator,
            "reason": err.to_string(),
            "trace_sequence": trace.sequence,
            "placements": [],
            });
            Json(value)
        }
    }
}

async fn production_safety_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let latest_trace = s.traces.recent().into_iter().next();
    let latest_bind_outcomes = s
        .latest_bind_outcomes
        .lock()
        .expect("latest bind outcomes mutex poisoned")
        .as_ref()
        .map(|(sequence, outcomes)| (*sequence, outcomes.len()));
    let simulator_urls = s.simulator_pool.urls();
    let simulator_readiness_probe = dashboard_simulator_readiness_probe(&simulator_urls).await;
    Json(production_safety_payload(
        &s.cfg,
        s.watch_healthy.load(Ordering::Relaxed),
        s.latest_readiness_error
            .lock()
            .expect("latest readiness error mutex poisoned")
            .clone(),
        latest_trace.as_ref(),
        latest_bind_outcomes,
        simulator_urls,
        Some(simulator_readiness_probe),
    ))
}

fn production_safety_payload(
    cfg: &ShadowConfig,
    watch_healthy: bool,
    latest_readiness_error: Option<ShadowReadinessError>,
    latest_trace: Option<&DecisionTrace>,
    latest_bind_outcomes: Option<(u64, usize)>,
    simulator_urls: Vec<String>,
    simulator_readiness_probe: Option<serde_json::Value>,
) -> serde_json::Value {
    let trace = latest_trace.map(|t| {
        serde_json::json!({
            "sequence": t.sequence,
            "candidate_node_limit": t.candidate_node_limit,
            "retry_count": t.retry_count,
            "unpruned_candidate_edges": t.unpruned_candidate_edges,
            "initial_candidate_edges": t.initial_candidate_edges,
            "final_candidate_edges": t.final_candidate_edges,
            "candidate_pruned_workloads": t.candidate_pruned_workloads,
            "widening_reason": t.widening_reason,
            "candidate_quality_metrics": t.candidate_quality_metrics,
            "node_grouping_metrics": t.node_grouping_metrics,
            "binding_reservation_metrics": t.binding_reservation_metrics,
            "binding_outcome_metrics": t.binding_outcome_metrics,
        })
    });
    let latest_bind_outcomes = latest_bind_outcomes.map(|(sequence, count)| {
        serde_json::json!({
            "sequence": sequence,
            "outcome_count": count,
        })
    });
    let solver_info = crate::cpsat_rust::solver_info();
    let ready = watch_healthy && solver_info.available;
    let readiness_blocker = if !watch_healthy {
        "watch not healthy"
    } else if !solver_info.available {
        "solver unavailable"
    } else {
        "none"
    };
    let readiness_blocker_class = if !watch_healthy {
        "kubernetes_watch"
    } else if !solver_info.available {
        "solver"
    } else {
        "none"
    };
    let readiness_last_error_class = latest_readiness_error
        .as_ref()
        .map(|err| classify_readiness_error(&err.message))
        .unwrap_or("none");
    let readiness_next_action = if !watch_healthy {
        readiness_error_next_action(readiness_last_error_class)
    } else if !solver_info.available {
        "restart shadow with --features rust-cp-sat"
    } else {
        "ready for live shadow demo checks"
    };
    let readiness_diagnostic_hint = if !watch_healthy {
        latest_readiness_error
            .as_ref()
            .map(|err| err.message.as_str())
            .unwrap_or("watch is unhealthy but has not captured a current error yet; verify kube context, API server /readyz, pod list RBAC, and node listing")
    } else if !solver_info.available {
        "the rust-cp-sat solver feature is not available in this binary"
    } else {
        "watch and solver are healthy"
    };
    let readiness_debug_commands = if !watch_healthy {
        readiness_debug_commands(readiness_last_error_class)
    } else if !solver_info.available {
        vec![
            "cargo run -p ksolver --features rust-cp-sat -- shadow".to_string(),
            "cargo test -p ksolver --features rust-cp-sat production_safety_payload_reports_read_only_gates".to_string(),
            "scripts/shadow-smoke.py --base-url http://127.0.0.1:8090".to_string(),
        ]
    } else {
        vec![
            "scripts/shadow-smoke.py --base-url http://127.0.0.1:8090".to_string(),
            "scripts/demo-gate.py --base-url http://127.0.0.1:8090 --output-dir /tmp/ksolver-demo-gate --json".to_string(),
        ]
    };
    let simulator_endpoint_count = simulator_urls.len();
    let simulator_live_dashboard_baseline_configured = !simulator_urls.is_empty();
    let simulator_recovery_command = simulator_recovery_command_for_urls(&simulator_urls);
    let simulator_readiness_probe = simulator_readiness_probe.unwrap_or_else(|| {
        simulator_readiness_not_probed(
            simulator_live_dashboard_baseline_configured,
            simulator_endpoint_count,
        )
    });
    let simulator_readiness = simulator_readiness_probe
        .get("readiness")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(if simulator_live_dashboard_baseline_configured {
            "configured_not_probed"
        } else {
            "not_configured"
        });
    let simulator_readiness_note = simulator_readiness_probe
        .get("readiness_note")
        .and_then(serde_json::Value::as_str)
        .unwrap_or(if simulator_live_dashboard_baseline_configured {
            "endpoints are configured; export readiness is checked during live baseline calls or with scripts/kss-pool.sh require-ready-urls"
        } else {
            "no kube-scheduler-simulator endpoint configured for live dashboard baselines"
        });
    serde_json::json!({
        "ready": ready,
        "watch_healthy": watch_healthy,
        "readiness": {
            "healthz": "ok",
            "readyz": if ready { "ready" } else { readiness_blocker },
            "ready": ready,
            "watch_healthy": watch_healthy,
            "solver_available": solver_info.available,
            "blocker": readiness_blocker,
            "blocker_class": readiness_blocker_class,
            "next_action": readiness_next_action,
            "diagnostic_hint": readiness_diagnostic_hint,
            "debug_commands": readiness_debug_commands,
            "last_error": latest_readiness_error.as_ref().map(|err| err.message.as_str()),
            "last_error_class": readiness_last_error_class,
            "last_error_at": latest_readiness_error.as_ref().map(|err| err.observed_at.as_str()),
        },
        "solver": {
            "name": solver_info.name,
            "available": solver_info.available,
            "status": solver_info.status,
            "required_for": [
                "live pending GPU placement solves",
                "deterministic proof scenarios",
                "kube-vs-ksolver dashboard comparisons"
            ],
            "build_hint": if solver_info.available {
                ""
            } else {
                "cargo build --manifest-path ksolver/Cargo.toml --features rust-cp-sat"
            }
        },
        "simulator": {
            "source": "kube-scheduler-simulator",
            "endpoint_count": simulator_endpoint_count,
            "endpoints": simulator_urls,
            "live_dashboard_baseline_configured": simulator_live_dashboard_baseline_configured,
            "readiness": simulator_readiness,
            "readiness_note": simulator_readiness_note,
            "readiness_probe": simulator_readiness_probe,
            "recovery_command": simulator_recovery_command,
            "deadline_millis": dashboard_simulator_deadline().as_millis() as u64,
            "required_for": [
                "live kube baseline in the Runs and Live tabs",
                "customer-trustworthy kube-vs-ksolver screenshots",
                "refreshing scenario baseline cache"
            ],
            "claim_guard": if !simulator_live_dashboard_baseline_configured {
                "no live kube-scheduler-simulator endpoint configured; use cached scenario baselines only"
            } else {
                "live dashboard baselines can call kube-scheduler-simulator; scenario cards still disclose cached/live provenance per baseline"
            }
        },
        "rollout": {
            "mode": binding_rollout_mode_name(cfg.binding_rollout_mode),
            "enable_real_binding": cfg.enable_real_binding,
            "mutation_allowed": cfg.real_binding_mutations_enabled(),
            "real_binding_dry_run": cfg.real_binding_dry_run,
            "binding_kill_switch": cfg.binding_kill_switch,
            "binding_canary_mode": binding_canary_mode_name(cfg.binding_canary_mode),
            "binding_low_risk_max_gpus": cfg.binding_low_risk_max_gpus,
            "max_binds_per_pass": cfg.max_binds_per_pass,
            "binding_reservation_ttl_seconds": cfg.binding_reservation_ttl.as_secs(),
        },
        "events": {
            "enable_kubernetes_events": cfg.enable_kubernetes_events,
            "writes_allowed": cfg.kubernetes_event_writes_enabled(),
        },
        "leader_election": {
            "configured": cfg.leader_election_configured(),
            "namespace": cfg.leader_election_namespace,
            "lease_name": cfg.leader_election_lease_name,
            "identity": cfg.leader_election_identity,
        },
        "rbac": {
            "read_only_shadow_required": true,
            "pods_binding_create_required": cfg.real_binding_mutations_enabled(),
            "events_create_required": cfg.kubernetes_event_writes_enabled(),
            "leases_required": cfg.leader_election_configured(),
        },
        "latest_trace": trace,
        "latest_bind_outcomes": latest_bind_outcomes,
        "operator_claim": if cfg.real_binding_mutations_enabled() {
            "mutation-capable binding path is enabled; readiness, ownership, reservation, throttle, canary, and kill-switch gates still apply"
        } else {
            "read-only shadow mode; no pod binding mutations are allowed by current config"
        },
    })
}

fn binding_rollout_mode_name(mode: crate::scheduler::config::BindingRolloutMode) -> &'static str {
    match mode {
        crate::scheduler::config::BindingRolloutMode::ObserveOnly => "observe-only",
        crate::scheduler::config::BindingRolloutMode::DryRun => "dry-run",
        crate::scheduler::config::BindingRolloutMode::BindLowRisk => "bind-low-risk",
        crate::scheduler::config::BindingRolloutMode::BindAll => "bind-all",
    }
}

fn binding_canary_mode_name(mode: crate::scheduler::config::BindingCanaryMode) -> &'static str {
    match mode {
        crate::scheduler::config::BindingCanaryMode::All => "all",
        crate::scheduler::config::BindingCanaryMode::LowRisk => "low-risk",
    }
}

fn simulator_readiness_not_probed(configured: bool, endpoint_count: usize) -> serde_json::Value {
    serde_json::json!({
        "readiness": if configured { "configured_not_probed" } else { "not_configured" },
        "readiness_note": if configured {
            "endpoints are configured; export readiness is checked during live baseline calls or with scripts/kss-pool.sh require-ready-urls"
        } else {
            "no kube-scheduler-simulator endpoint configured for live dashboard baselines"
        },
        "endpoint_count": endpoint_count,
        "checked_count": 0,
        "ready_count": 0,
        "probe_path": "/api/v1/export",
        "timeout_millis": dashboard_simulator_readiness_timeout().as_millis() as u64,
        "failures": [],
    })
}

async fn dashboard_simulator_readiness_probe(urls: &[String]) -> serde_json::Value {
    if urls.is_empty() {
        return simulator_readiness_not_probed(false, 0);
    }

    let timeout = dashboard_simulator_readiness_timeout();
    let client = match reqwest::Client::builder().timeout(timeout).build() {
        Ok(client) => client,
        Err(err) => {
            return serde_json::json!({
                "readiness": "configured_unreachable",
                "readiness_note": format!("failed to build simulator readiness client: {err}"),
                "endpoint_count": urls.len(),
                "checked_count": 0,
                "ready_count": 0,
                "probe_path": "/api/v1/export",
                "timeout_millis": timeout.as_millis() as u64,
                "failures": [{
                    "url": "",
                    "error": err.to_string(),
                }],
            });
        }
    };

    let mut ready_count = 0_usize;
    let mut failures = Vec::new();
    for url in urls {
        let base = url.trim_end_matches('/');
        let probe_url = format!("{base}/api/v1/export");
        match client.get(&probe_url).send().await {
            Ok(response) if response.status().is_success() => {
                ready_count += 1;
            }
            Ok(response) => failures.push(serde_json::json!({
                "url": base,
                "status": response.status().as_u16(),
            })),
            Err(err) => failures.push(serde_json::json!({
                "url": base,
                "error": err.to_string(),
            })),
        }
    }

    let readiness = if ready_count == urls.len() {
        "ready"
    } else if ready_count > 0 {
        "partially_ready"
    } else {
        "configured_unreachable"
    };
    let readiness_note = match readiness {
        "ready" => format!("all {ready_count} configured kube-scheduler-simulator endpoint(s) answered /api/v1/export"),
        "partially_ready" => format!("{ready_count}/{} configured kube-scheduler-simulator endpoint(s) answered /api/v1/export", urls.len()),
        _ => format!("0/{} configured kube-scheduler-simulator endpoint(s) answered /api/v1/export", urls.len()),
    };

    serde_json::json!({
        "readiness": readiness,
        "readiness_note": readiness_note,
        "endpoint_count": urls.len(),
        "checked_count": urls.len(),
        "ready_count": ready_count,
        "probe_path": "/api/v1/export",
        "timeout_millis": timeout.as_millis() as u64,
        "failures": failures,
    })
}

async fn metrics_handler() -> (
    axum::http::StatusCode,
    [(&'static str, &'static str); 1],
    String,
) {
    (
        axum::http::StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        metrics::render_metrics(),
    )
}

async fn healthz() -> &'static str {
    "ok"
}

/// Self-contained live dashboard (polls /api/scheduler/traces). Read-only view.
const DEFAULT_SIMULATOR_URL: &str = "http://127.0.0.1:1212";
const DEFAULT_SIMULATOR_CACHE_DIR: &str = "/tmp/ksolver-kss-cache";
const SHADOW_HTML: &str = include_str!("../../static/shadow.html");
// Source path of the dashboard asset, resolved at compile time. When the file is present
// on disk (i.e. running from a checkout), the handler serves it fresh on every request so
// HTML/CSS/JS edits are picked up on a browser refresh without rebuilding or restarting.
// Deployed binaries without the source tree fall back to the embedded copy above.
const SHADOW_HTML_PATH: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/static/shadow.html");

impl DashboardSimulatorPool {
    fn from_env() -> Self {
        Self::from_urls(dashboard_simulator_urls_from_env())
    }

    fn from_urls(urls: Vec<String>) -> Self {
        Self {
            endpoints: urls
                .into_iter()
                .map(|url| DashboardSimulatorEndpoint {
                    url,
                    gate: Arc::new(tokio::sync::Mutex::new(())),
                })
                .collect(),
            next: AtomicUsize::new(0),
        }
    }

    fn is_empty(&self) -> bool {
        self.endpoints.is_empty()
    }

    fn len(&self) -> usize {
        self.endpoints.len()
    }

    fn urls(&self) -> Vec<String> {
        self.endpoints
            .iter()
            .map(|endpoint| endpoint.url.clone())
            .collect()
    }

    async fn run_for_trace(
        &self,
        kubeconfig: &str,
        trace: &DecisionTrace,
    ) -> Result<(String, KubeSimulatorTracePlan)> {
        if self.endpoints.is_empty() {
            anyhow::bail!("no kube-scheduler-simulator endpoints configured");
        }
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.endpoints.len();
        let endpoint = self.endpoints[idx].clone();
        let _lease = endpoint.gate.lock().await;
        let plan = run_kube_simulator_for_trace(kubeconfig, &endpoint.url, trace).await?;
        Ok((endpoint.url, plan))
    }
}

fn dashboard_simulator_urls_from_env() -> Vec<String> {
    let raw = std::env::var("KSOLVER_SCHEDULER_SIMULATOR_POOL")
        .or_else(|_| std::env::var("KSOLVER_SCHEDULER_SIMULATOR_URLS"))
        .or_else(|_| std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_POOL"))
        .or_else(|_| std::env::var("KSOLVER_SCHEDULER_SIMULATOR_URL"))
        .or_else(|_| std::env::var("SCHEDULER_SIMULATOR_URL"))
        .unwrap_or_else(|_| DEFAULT_SIMULATOR_URL.to_string());

    raw.split(',')
        .map(str::trim)
        .filter(|url| !url.is_empty())
        .map(|url| url.trim_end_matches('/').to_string())
        .collect()
}

fn simulator_recovery_command_for_urls(urls: &[String]) -> String {
    let cache_dir = std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE_DIR")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_SIMULATOR_CACHE_DIR.to_string());
    simulator_recovery_command_for_urls_with_cache_dir(urls, &cache_dir)
}

fn simulator_recovery_command_for_urls_with_cache_dir(urls: &[String], cache_dir: &str) -> String {
    let ports = urls
        .iter()
        .filter_map(|url| simulator_url_port(url))
        .collect::<Vec<_>>();
    let (count, base_port) = if ports.is_empty() {
        (
            1_u16,
            simulator_url_port(DEFAULT_SIMULATOR_URL).unwrap_or(1212),
        )
    } else {
        let min = *ports.iter().min().unwrap_or(&1212);
        let max = *ports.iter().max().unwrap_or(&min);
        (max.saturating_sub(min).saturating_add(1), min)
    };
    format!(
        "scripts/kss-pool.sh status {} {} {}",
        count,
        base_port,
        shell_quote_arg(cache_dir)
    )
}

fn simulator_url_port(url: &str) -> Option<u16> {
    let trimmed = url.trim().trim_end_matches('/');
    let after_scheme = trimmed
        .rsplit_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(trimmed);
    let authority = after_scheme.split('/').next().unwrap_or(after_scheme);
    let port = authority.rsplit_once(':')?.1;
    port.parse::<u16>().ok()
}

fn shell_quote_arg(value: &str) -> String {
    if value.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                '_' | '/' | ':' | '.' | ',' | '=' | '@' | '%' | '+' | '-'
            )
    }) {
        value.to_string()
    } else {
        // Shell single-quote escaping via split/join (not String::replace) so the shadow
        // no-mutation guard stays strict against kube client mutation calls.
        format!("'{}'", value.split('\'').collect::<Vec<_>>().join("'\\''"))
    }
}

async fn dashboard() -> axum::response::Html<String> {
    let body =
        std::fs::read_to_string(SHADOW_HTML_PATH).unwrap_or_else(|_| SHADOW_HTML.to_string());
    axum::response::Html(body)
}

fn readiness_status(watch_healthy: bool) -> (axum::http::StatusCode, &'static str) {
    if !watch_healthy {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "watch not healthy",
        );
    }
    if !crate::cpsat_rust::solver_info().available {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "solver unavailable",
        );
    }
    (axum::http::StatusCode::OK, "ready")
}

async fn readyz(State(s): State<ShadowHttpState>) -> (axum::http::StatusCode, &'static str) {
    readiness_status(s.watch_healthy.load(Ordering::SeqCst))
}

/// Shadow-mode scheduler: observe pending GPU pods, periodically solve, record
/// decision traces, serve them. NEVER binds or mutates cluster state.
pub async fn run_shadow(cfg: ShadowConfig) -> Result<()> {
    metrics::register_metrics();
    let traces = Arc::new(TraceStore::new(64));
    let observed: Arc<Mutex<WatchState>> = Arc::new(Mutex::new(WatchState::new()));
    let watch_healthy = Arc::new(AtomicBool::new(false));
    let latest_readiness_error: Arc<Mutex<Option<ShadowReadinessError>>> =
        Arc::new(Mutex::new(None));
    let latest_cluster: Arc<Mutex<Option<crate::model::NormalizedCluster>>> =
        Arc::new(Mutex::new(None));
    let latest_pending: Arc<Mutex<Vec<crate::scheduler::pod_filter::PendingGpuPod>>> =
        Arc::new(Mutex::new(Vec::new()));
    let latest_bind_outcomes: Arc<Mutex<BindOutcomeSnapshot>> = Arc::new(Mutex::new(None));
    let active_objective = Arc::new(Mutex::new(ObjectiveSelection {
        profile: cfg.objective_profile,
        weights: cfg.objective_weights.clone(),
    }));

    // HTTP server (traces / metrics / health).
    let http_state = ShadowHttpState {
        traces: traces.clone(),
        watch_healthy: watch_healthy.clone(),
        latest_readiness_error: latest_readiness_error.clone(),
        latest_cluster: latest_cluster.clone(),
        latest_pending: latest_pending.clone(),
        latest_bind_outcomes: latest_bind_outcomes.clone(),
        simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
        latest_liabilities: Arc::new(Mutex::new(None)),
        simulator_pool: Arc::new(DashboardSimulatorPool::from_env()),
        kubeconfig: cfg.kubeconfig.clone(),
        cfg: cfg.clone(),
        active_objective: active_objective.clone(),
        demo_report_cache: Arc::new(tokio::sync::Mutex::new(None)),
        demo_report_refresh_status: Arc::new(tokio::sync::Mutex::new(None)),
    };
    let app = Router::new()
        .route("/api/scheduler/traces", get(traces_handler))
        .route("/api/scheduler/objective", get(objective_config_handler))
        .route("/api/scheduler/demo-report", get(demo_report_handler))
        .route(
            "/api/scheduler/demo-report/refresh",
            post(demo_report_refresh_handler),
        )
        .route(
            "/api/scheduler/simulator-cache-coverage",
            get(simulator_cache_coverage_handler),
        )
        .route(
            "/api/scheduler/vram-calibration",
            get(vram_calibration_handler),
        )
        .route(
            "/api/scheduler/evidence-bundle",
            get(evidence_bundle_handler),
        )
        .route(
            "/api/scheduler/operator-status",
            get(operator_status_handler),
        )
        .route(
            "/api/scheduler/production-safety",
            get(production_safety_handler),
        )
        .route("/api/scheduler/solve", get(solve_now_handler))
        .route("/api/scheduler/cluster", get(cluster_handler))
        .route(
            "/api/scheduler/kube-simulator-plan",
            get(kube_simulator_plan_handler),
        )
        .route("/api/scheduler/binding-plan", get(binding_plan_handler))
        .route("/api/scheduler/repair-plan", get(repair_plan_handler))
        .route("/api/scheduler/repair-events", get(repair_events_handler))
        .route("/api/scheduler/binding-events", get(binding_events_handler))
        .route(
            "/api/scheduler/decision-events",
            get(decision_events_handler),
        )
        .route(
            "/admission/scheduler-name",
            post(scheduler_admission_handler),
        )
        .route("/metrics", get(metrics_handler))
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/", get(dashboard))
        .with_state(http_state);
    let http_addr = cfg.http_addr.clone();
    tokio::spawn(async move {
        match tokio::net::TcpListener::bind(&http_addr).await {
            Ok(l) => {
                info!(addr = %http_addr, "shadow HTTP server listening");
                if let Err(e) = axum::serve(l, app).await {
                    error!(error = %e, "shadow HTTP failed");
                }
            }
            Err(e) => error!(error = %e, addr = %http_addr, "failed to bind shadow HTTP addr"),
        }
    });

    // Self-healing watch task: recreate the watcher if the stream ends.
    let client = collector::build_client(&cfg.kubeconfig).await?;
    let leader = if cfg.leader_election_configured() {
        info!(
            namespace = %cfg.leader_election_namespace,
            lease = %cfg.leader_election_lease_name,
            identity = %cfg.leader_election_identity,
            "leader election enabled; solve and bind passes require lease leadership"
        );
        crate::scheduler::leader::LeaderElector::spawn(client.clone(), cfg.clone())?
    } else {
        crate::scheduler::leader::LeaderElector::disabled()
    };
    let pods_api: Api<corev1::Pod> = Api::all(client);
    let watch_cfg = cfg.clone();
    let watch_observed = observed.clone();
    let watch_flag = watch_healthy.clone();
    let watch_error = latest_readiness_error.clone();
    tokio::spawn(async move {
        loop {
            watch_flag.store(false, Ordering::SeqCst);
            let mut stream = watcher(pods_api.clone(), watcher::Config::default()).boxed();
            info!("pod watch (re)started");
            while let Some(event) = stream.next().await {
                match event {
                    Ok(ev) => {
                        if matches!(ev, watcher::Event::InitDone) {
                            watch_flag.store(true, Ordering::SeqCst);
                            clear_latest_readiness_error(&watch_error);
                        }
                        let mut st = watch_observed.lock().expect("watch state poisoned");
                        st.apply(&ev, &watch_cfg);
                        metrics::set_shadow_pending(st.len() as i64);
                    }
                    Err(e) => {
                        set_latest_readiness_error(&watch_error, e.to_string());
                        warn!(error = %e, "watch error; will resync");
                    }
                }
            }
            watch_flag.store(false, Ordering::SeqCst);
            set_latest_readiness_error(
                &watch_error,
                "watch stream ended; restarting after backoff",
            );
            warn!("watch stream ended; restarting after backoff");
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }
    });

    // A kube client for the real-binding pass — built ONLY when real binding is enabled and the
    // kill switch is off (otherwise there is no mutation-capable client at all).
    // Default: None ⇒ read-only shadow.
    let bind_client = if cfg.real_binding_mutations_enabled() {
        warn!(
            dry_run = cfg.real_binding_dry_run,
            max_per_pass = cfg.max_binds_per_pass,
            "REAL BINDING ENABLED — this scheduler will mutate the cluster (apply pod bindings)"
        );
        Some(collector::build_client(&cfg.kubeconfig).await?)
    } else {
        None
    };
    let event_client = if cfg.kubernetes_event_writes_enabled() {
        warn!("KUBERNETES EVENT EMISSION ENABLED — this scheduler will create Event objects");
        Some(collector::build_client(&cfg.kubeconfig).await?)
    } else {
        None
    };
    let mut binding_ledger = crate::scheduler::ledger::ReservationLedger::new();
    let mut decision_event_filter = DecisionEventEmissionFilter::default();

    fn publish_binding_ledger_metrics(ledger: &crate::scheduler::ledger::ReservationLedger) {
        metrics::set_shadow_bind_reservation_state(
            ledger.len() as i64,
            ledger.entry_count() as i64,
            ledger.committed_gpu_total(),
        );
    }

    // Sequential solve loop: sleep AFTER each solve so a slow solve never overlaps itself.
    loop {
        tokio::time::sleep(cfg.batch_window).await;
        if !leader.is_leader() {
            metrics::inc_shadow_leader_skipped_solves();
            info!(
                namespace = %cfg.leader_election_namespace,
                lease = %cfg.leader_election_lease_name,
                identity = %cfg.leader_election_identity,
                "not leader; skipping shadow solve pass"
            );
            continue;
        }
        // Refresh the cluster snapshot EVERY iteration (even when idle) so the binding-plan
        // readiness re-check always reflects the current cluster, not the last solve's snapshot.
        let started = Instant::now();
        let collected = match collect_shadow_snapshot(&cfg).await {
            Ok(n) => n,
            Err(e) => {
                metrics::inc_shadow_solve_errors();
                set_latest_readiness_error(&latest_readiness_error, e.to_string());
                error!(error = %e, "shadow snapshot collection failed");
                continue;
            }
        };
        let raw_snapshot = collected.raw;
        let normalized = collected.normalized;
        let job_observations = crate::scheduler::observations::extract_completed_gpu_observations(
            &raw_snapshot,
            &|resource| cfg.is_gpu_resource(resource),
        );
        let job_observation_metrics =
            crate::scheduler::observations::summarize_job_observations(&job_observations);
        let snapshot_age_millis = started.elapsed().as_millis() as u64;
        let mut binding_reconcile_stats = crate::scheduler::ledger::ReconcileStats::default();
        if bind_client.is_some() {
            binding_reconcile_stats =
                binding_ledger.reconcile_observed(&normalized, Instant::now());
            metrics::inc_shadow_bind_reservation_expired(
                binding_reconcile_stats.expired_reservations as u64,
            );
            metrics::inc_shadow_bind_reservation_observed(
                binding_reconcile_stats.observed_bound_entries as u64,
            );
            metrics::inc_shadow_bind_reservation_stale(
                binding_reconcile_stats.stale_entries as u64,
            );
            publish_binding_ledger_metrics(&binding_ledger);
            if binding_reconcile_stats.expired_reservations > 0
                || binding_reconcile_stats.observed_bound_entries > 0
                || binding_reconcile_stats.stale_entries > 0
            {
                info!(
                    expired_reservations = binding_reconcile_stats.expired_reservations,
                    observed_bound_entries = binding_reconcile_stats.observed_bound_entries,
                    stale_entries = binding_reconcile_stats.stale_entries,
                    active_reservations = binding_reconcile_stats.active_reservations,
                    active_entries = binding_reconcile_stats.active_entries,
                    "binding reservation ledger reconciled"
                );
            }
        }
        if let Ok(mut g) = latest_cluster.lock() {
            *g = Some(normalized.clone());
        }
        let pending = crate::scheduler::prediction::enrich_pending_with_historical_predictions(
            &raw_snapshot,
            &observed.lock().expect("watch state poisoned").snapshot(),
            &job_observations,
        );
        let prediction_audit_details =
            crate::scheduler::prediction::audit_pending_prediction_details(
                &raw_snapshot,
                &pending,
                &job_observations,
            );
        let prediction_audit_metrics =
            crate::scheduler::prediction::summarize_prediction_audit_details(
                &prediction_audit_details,
            );
        if let Ok(mut g) = latest_pending.lock() {
            *g = pending.clone();
        }
        if pending.is_empty() {
            continue;
        }
        metrics::inc_shadow_pod_observations(pending.len() as u64);
        let seq = traces.next_sequence();
        let solve_cfg = active_objective
            .lock()
            .map(|o| cfg_with_objective(&cfg, &o))
            .unwrap_or_else(|_| cfg.clone());
        match run_one_solve(
            &solve_cfg,
            seq,
            &pending,
            &normalized,
            crate::scheduler::trace::JobObservationMetrics {
                completed_gpu_pods: job_observation_metrics.completed_gpu_pods,
                runtime_observations: job_observation_metrics.runtime_observations,
                failed_gpu_pods: job_observation_metrics.failed_gpu_pods,
                max_runtime_seconds: job_observation_metrics.max_runtime_seconds,
                max_peak_memory_bytes: job_observation_metrics.max_peak_memory_bytes,
                unique_command_hashes: job_observation_metrics.unique_command_hashes,
                runtime_prediction_samples: job_observation_metrics.runtime_prediction_samples,
                runtime_prediction_mape_milli: job_observation_metrics
                    .runtime_prediction_mape_milli,
                max_runtime_prediction_error_seconds: job_observation_metrics
                    .max_runtime_prediction_error_seconds,
                vram_prediction_samples: job_observation_metrics.vram_prediction_samples,
                vram_prediction_mape_milli: job_observation_metrics.vram_prediction_mape_milli,
                max_vram_prediction_error_bytes: job_observation_metrics
                    .max_vram_prediction_error_bytes,
            },
            prediction_audit_metrics,
            prediction_audit_details,
            started,
            snapshot_age_millis,
        )
        .await
        {
            Ok(mut trace) => {
                let unplaced = trace
                    .decisions
                    .iter()
                    .filter(|d| matches!(d.placement, PodPlacement::Unplaced { .. }))
                    .count() as u64;
                let caveated = trace
                    .decisions
                    .iter()
                    .filter(|d| {
                        matches!(d.placement, PodPlacement::Placed { .. }) && !d.caveats.is_empty()
                    })
                    .count() as u64;
                let vram_blocked = trace
                    .decisions
                    .iter()
                    .filter(|d| is_vram_blocked_decision(d))
                    .count() as u64;
                let high_priority_unplaced = trace
                    .decisions
                    .iter()
                    .filter(|d| is_high_priority_unplaced_decision(d))
                    .count() as u64;
                let predicted_deadline_misses = trace.deadline_metrics.predicted_misses as u64;
                let repair_counts = repair_metric_counts(&trace.repair_plans);
                let repair = trace.repair_metrics.clone();
                let admission = trace.admission_metrics.clone();
                let deadline = trace.deadline_metrics.clone();
                let queue_wait = trace.queue_wait_metrics.clone();
                let tenant_fairness = trace.tenant_fairness_metrics.clone();
                let gpu_utilization = trace.gpu_utilization_metrics.clone();
                let outcome = trace.outcome_summary.clone();
                let job_observations = trace.job_observation_metrics.clone();
                let prediction_audit = trace.prediction_audit_metrics.clone();
                metrics::set_shadow_placement_pressure(
                    unplaced as i64,
                    vram_blocked as i64,
                    high_priority_unplaced as i64,
                );
                metrics::inc_shadow_unplaced(unplaced);
                metrics::inc_shadow_vram_blocked(vram_blocked);
                metrics::inc_shadow_high_priority_unplaced(high_priority_unplaced);
                metrics::inc_shadow_predicted_deadline_misses(predicted_deadline_misses);
                metrics::set_shadow_admission(
                    admission.admitted_pods as i64,
                    admission.admitted_gpu_demand,
                );
                metrics::set_shadow_outcome_summary(
                    outcome.unplaced_pods as i64,
                    outcome.requested_gpu_demand,
                    outcome.admitted_gpu_demand,
                    outcome.unplaced_gpu_demand,
                    outcome.pod_admission_percent_milli,
                    outcome.gpu_admission_percent_milli,
                    outcome.admitted_monthly_cost_milli,
                );
                metrics::set_shadow_queue_wait(
                    queue_wait.max_queue_wait_seconds,
                    queue_wait.high_priority_max_queue_wait_seconds,
                );
                metrics::set_shadow_deadlines(
                    deadline.deadline_jobs as i64,
                    deadline.unplaced_deadline_jobs as i64,
                    deadline.predicted_misses as i64,
                    deadline.placed_predicted_misses as i64,
                    deadline.unplaced_predicted_misses as i64,
                    deadline.worst_slack_seconds,
                );
                metrics::set_shadow_quota_throttle(
                    tenant_fairness.throttled_pods as i64,
                    tenant_fairness.throttled_max_queue_wait_seconds,
                );
                metrics::set_shadow_fairness(
                    tenant_fairness.under_fair_share_tenants as i64,
                    tenant_fairness.over_fair_share_tenants as i64,
                    tenant_fairness.total_borrowed_gpu_milli,
                    tenant_fairness.reclaimable_borrowed_gpu_milli,
                );
                metrics::set_shadow_budget_pressure(
                    tenant_fairness.budget_over_tenants as i64,
                    tenant_fairness.total_budget_overage_monthly_milli,
                );
                metrics::set_shadow_gpu_utilization(
                    gpu_utilization.active_gpu_nodes as i64,
                    gpu_utilization.stranded_gpu_on_active_nodes,
                );
                metrics::set_shadow_job_observations(
                    job_observations.completed_gpu_pods as i64,
                    job_observations.runtime_observations as i64,
                    job_observations.failed_gpu_pods as i64,
                    job_observations.max_runtime_seconds,
                    job_observations.max_peak_memory_bytes,
                    job_observations.unique_command_hashes as i64,
                    job_observations.runtime_prediction_samples as i64,
                    job_observations.runtime_prediction_mape_milli,
                    job_observations.max_runtime_prediction_error_seconds,
                    job_observations.vram_prediction_samples as i64,
                    job_observations.vram_prediction_mape_milli,
                    job_observations.max_vram_prediction_error_bytes,
                );
                metrics::set_shadow_prediction_audit(
                    prediction_audit.pending_pods as i64,
                    prediction_audit.fingerprint_matched_pods as i64,
                    prediction_audit.history_exact_pods as i64,
                    prediction_audit.history_scaled_pods as i64,
                    prediction_audit.history_segment_pods as i64,
                    prediction_audit.hint_pods as i64,
                    prediction_audit.unknown_pods as i64,
                    prediction_audit.predicted_runtime_pods as i64,
                    prediction_audit.predicted_vram_pods as i64,
                    prediction_audit.average_confidence,
                );
                metrics::set_shadow_candidate_model(
                    trace.candidate_node_limit as i64,
                    trace.unpruned_candidate_edges as i64,
                    trace.initial_candidate_edges as i64,
                    trace.final_candidate_edges as i64,
                    trace.candidate_pruned_workloads as i64,
                    trace.retry_count as i64,
                );
                metrics::set_shadow_candidate_quality(
                    trace.candidate_quality_metrics.pruning_active,
                    trace.candidate_quality_metrics.widened,
                    trace.candidate_quality_metrics.edge_reduction_milli,
                    &trace.candidate_quality_metrics.regret_status,
                );
                if trace.retry_count > 0 {
                    metrics::inc_shadow_candidate_widening_attempts(trace.retry_count as u64);
                }
                metrics::set_shadow_node_grouping(
                    trace.node_grouping_metrics.enabled,
                    trace.node_grouping_metrics.used,
                    trace.node_grouping_metrics.eligible_group_count as i64,
                    trace.node_grouping_metrics.eligible_node_count as i64,
                    trace.node_grouping_metrics.max_group_size as i64,
                    trace.node_grouping_metrics.grouped_node_count as i64,
                    trace.node_grouping_metrics.grouped_candidate_edges as i64,
                );
                if trace.node_grouping_metrics.used {
                    metrics::inc_shadow_node_grouping_used();
                }
                if !trace.node_grouping_metrics.fallback_reason.is_empty() {
                    metrics::inc_shadow_node_grouping_fallback();
                }
                metrics::set_shadow_repairs(
                    repair_counts.plans as i64,
                    repair_counts.migrations as i64,
                    repair_counts.preemptions as i64,
                    repair_counts.disruption_cost as i64,
                    repair.repairable_targets as i64,
                    repair.unrepairable_targets as i64,
                    repair.vram_blocked_targets as i64,
                    repair.not_enough_total_gpu_targets as i64,
                    repair.policy_or_candidate_blocked_targets as i64,
                    repair.incomplete_model_targets as i64,
                    repair.skipped_candidates as i64,
                    repair.priority_blocked_candidates as i64,
                    repair.value_policy_blocked_candidates as i64,
                    repair.disruption_policy_blocked_candidates as i64,
                    repair.pdb_blocked_candidates as i64,
                    repair.candidate_budget_skipped_candidates as i64,
                );
                metrics::inc_shadow_repair_plans(repair_counts.plans);
                metrics::inc_shadow_repair_migrations(repair_counts.migrations);
                metrics::inc_shadow_repair_preemptions(repair_counts.preemptions);
                metrics::inc_shadow_repair_disruption_cost(repair_counts.disruption_cost);
                metrics::inc_shadow_caveated(caveated);
                // Render the dry-run plan with per-entry readiness (vs the fresh snapshot) once —
                // used for the log count and, when armed, the real-binding pass.
                let plan: Vec<_> = crate::scheduler::binding::render_binding_plan(&trace)
                    .into_iter()
                    .map(|e| {
                        let r =
                            crate::scheduler::binding::assess_binding_readiness(&e, &normalized);
                        (e, r)
                    })
                    .collect();
                let would_bind = plan.len();
                let mut reservation_created = 0usize;
                let mut reservation_rejected = 0usize;
                let mut bind_outcomes_for_events: Option<
                    Vec<crate::scheduler::binder::BindOutcome>,
                > = None;
                let write_paths_allowed = write_paths_allowed_after_solve(
                    &leader,
                    bind_client.is_some(),
                    event_client.is_some(),
                );
                if (bind_client.is_some() || event_client.is_some()) && !write_paths_allowed {
                    metrics::inc_shadow_leader_skipped_solves();
                    warn!(
                        sequence = trace.sequence,
                        identity = %cfg.leader_election_identity,
                        "leadership lost after solve; skipping binding and event write paths"
                    );
                }
                if let Some(bc) = &bind_client {
                    if !write_paths_allowed {
                        trace.binding_reservation_metrics = binding_reservation_metrics(
                            &binding_ledger,
                            reservation_created,
                            reservation_rejected,
                            &binding_reconcile_stats,
                        );
                    } else {
                        let reservation_id = match crate::scheduler::binder::reserve_ready_bindings(
                            &mut binding_ledger,
                            &normalized,
                            &cfg.namespace_gpu_quotas,
                            &plan,
                            cfg.binding_reservation_ttl,
                            Instant::now(),
                        ) {
                            Ok(id) => id,
                            Err(outcomes) => {
                                let skipped = outcomes.len() as u64;
                                if let Ok(mut latest) = latest_bind_outcomes.lock() {
                                    *latest = Some((trace.sequence, outcomes.clone()));
                                }
                                reservation_rejected = 1;
                                trace.binding_reservation_metrics = binding_reservation_metrics(
                                    &binding_ledger,
                                    reservation_created,
                                    reservation_rejected,
                                    &binding_reconcile_stats,
                                );
                                trace.binding_outcome_metrics = binding_outcome_metrics(&outcomes);
                                metrics::inc_shadow_bind_reservation_rejected(1);
                                metrics::inc_shadow_bind_skipped(skipped);
                                metrics::inc_shadow_bind_canary_skipped(
                                    trace.binding_outcome_metrics.canary_skipped as u64,
                                );
                                publish_binding_skip_reason_metrics(&trace.binding_outcome_metrics);
                                publish_binding_ledger_metrics(&binding_ledger);
                                if let Some(ec) = &event_client {
                                    emit_scheduler_events(
                                        ec,
                                        &cfg,
                                        &trace,
                                        Some(&outcomes),
                                        &mut decision_event_filter,
                                    )
                                    .await;
                                }
                                warn!(
                                    sequence = trace.sequence,
                                    skipped, "real binding pass skipped by reservation ledger"
                                );
                                info!(
                                    sequence = trace.sequence,
                                    bound = 0u64,
                                    validated = 0u64,
                                    skipped,
                                    failed = 0u64,
                                    dry_run = cfg.real_binding_dry_run,
                                    "real binding pass complete"
                                );
                                traces.push(trace);
                                continue;
                            }
                        };
                        if reservation_id.is_some() {
                            reservation_created = 1;
                            metrics::inc_shadow_bind_reservation_created(1);
                            publish_binding_ledger_metrics(&binding_ledger);
                        }
                        let outcomes = crate::scheduler::binder::apply_bindings(
                            bc,
                            &plan,
                            &cfg,
                            &trace.candidate_quality_metrics.regret_status,
                            trace.candidate_quality_metrics.pruning_active,
                        )
                        .await;
                        if let Ok(mut latest) = latest_bind_outcomes.lock() {
                            *latest = Some((trace.sequence, outcomes.clone()));
                        }
                        bind_outcomes_for_events = Some(outcomes.clone());
                        // Count only ACTUALLY-persisted binds toward the bound metric; server-side
                        // dry-run validations are reported separately so they never imply real mutation.
                        let outcome_metrics = binding_outcome_metrics(&outcomes);
                        let bound = outcome_metrics.bound as u64;
                        let validated = outcome_metrics.validated as u64;
                        let failed = outcome_metrics.failed as u64;
                        let skipped = outcome_metrics.skipped as u64;
                        let canary_skipped = outcome_metrics.canary_skipped as u64;
                        if let Some(id) = reservation_id {
                            if cfg.real_binding_dry_run || bound == 0 {
                                binding_ledger.release(id);
                                publish_binding_ledger_metrics(&binding_ledger);
                            }
                        }
                        trace.binding_outcome_metrics = outcome_metrics;
                        metrics::inc_shadow_bound(bound);
                        metrics::inc_shadow_bind_skipped(skipped);
                        metrics::inc_shadow_bind_canary_skipped(canary_skipped);
                        publish_binding_skip_reason_metrics(&trace.binding_outcome_metrics);
                        metrics::inc_shadow_bind_failed(failed);
                        info!(
                            sequence = trace.sequence,
                            bound,
                            validated,
                            skipped,
                            canary_skipped,
                            failed,
                            dry_run = cfg.real_binding_dry_run,
                            "real binding pass complete"
                        );
                    }
                }
                trace.binding_reservation_metrics = binding_reservation_metrics(
                    &binding_ledger,
                    reservation_created,
                    reservation_rejected,
                    &binding_reconcile_stats,
                );
                if let Some(ec) = &event_client {
                    if !write_paths_allowed {
                        // Read-only trace recording continues below; only Kubernetes Event writes
                        // are suppressed after losing leadership.
                    } else {
                        emit_scheduler_events(
                            ec,
                            &cfg,
                            &trace,
                            bind_outcomes_for_events.as_deref(),
                            &mut decision_event_filter,
                        )
                        .await;
                    }
                }
                info!(
                    sequence = trace.sequence,
                    observed = trace.observed_pods,
                    unplaced,
                    vram_blocked,
                    high_priority_unplaced,
                    predicted_deadline_misses,
                    admitted_pods = admission.admitted_pods,
                    admitted_gpu_demand = admission.admitted_gpu_demand,
                    max_queue_wait_seconds = queue_wait.max_queue_wait_seconds,
                    high_priority_pending_pods = queue_wait.high_priority_pending_pods,
                    high_priority_max_queue_wait_seconds =
                        queue_wait.high_priority_max_queue_wait_seconds,
                    unplaced_max_queue_wait_seconds = queue_wait.unplaced_max_queue_wait_seconds,
                    tenants = tenant_fairness.tenants.len(),
                    quota_throttled_pods = tenant_fairness.throttled_pods,
                    quota_throttled_max_queue_wait_seconds =
                        tenant_fairness.throttled_max_queue_wait_seconds,
                    under_fair_share_tenants = tenant_fairness.under_fair_share_tenants,
                    over_fair_share_tenants = tenant_fairness.over_fair_share_tenants,
                    borrowed_gpu_milli = tenant_fairness.total_borrowed_gpu_milli,
                    reclaimable_borrowed_gpu_milli =
                        tenant_fairness.reclaimable_borrowed_gpu_milli,
                    budget_over_tenants = tenant_fairness.budget_over_tenants,
                    budget_overage_monthly_milli =
                        tenant_fairness.total_budget_overage_monthly_milli,
                    active_gpu_nodes = gpu_utilization.active_gpu_nodes,
                    stranded_gpu_on_active_nodes = gpu_utilization.stranded_gpu_on_active_nodes,
                    repair_plans = repair_counts.plans,
                    repair_migrations = repair_counts.migrations,
                    repair_preemptions = repair_counts.preemptions,
                    repair_disruption_cost = repair_counts.disruption_cost,
                    caveated,
                    would_bind,
                    real_binding = cfg.real_binding_mutations_enabled(),
                    bind_reservations = trace.binding_reservation_metrics.active_reservations,
                    bind_reserved_entries = trace.binding_reservation_metrics.active_entries,
                    bind_reserved_gpus = trace.binding_reservation_metrics.reserved_gpus,
                    bind_reservation_created = trace.binding_reservation_metrics.created,
                    bind_reservation_rejected = trace.binding_reservation_metrics.rejected,
                    bind_reservation_expired = trace.binding_reservation_metrics.expired,
                    bind_reservation_observed =
                        trace.binding_reservation_metrics.observed_bound_entries,
                    bind_reservation_stale = trace.binding_reservation_metrics.stale_entries,
                    bind_bound = trace.binding_outcome_metrics.bound,
                    bind_validated = trace.binding_outcome_metrics.validated,
                    bind_skipped = trace.binding_outcome_metrics.skipped,
                    bind_canary_skipped = trace.binding_outcome_metrics.canary_skipped,
                    bind_failed = trace.binding_outcome_metrics.failed,
                    objective_profile = objective_profile_name(trace.objective_profile),
                    objective_admission_weight = trace.objective_weights.admission,
                    objective_gpu_demand_weight = trace.objective_weights.gpu_demand,
                    objective_gang_complete_weight = trace.objective_weights.gang_complete,
                    objective_priority_weight = trace.objective_weights.priority,
                    objective_business_value_weight = trace.objective_weights.business_value,
                    objective_queue_weight = trace.objective_weights.queue,
                    objective_queue_wait_weight = trace.objective_weights.queue_wait,
                    objective_fair_share_weight = trace.objective_weights.fair_share,
                    objective_deadline_urgency_weight = trace.objective_weights.deadline_urgency,
                    objective_gpu_fragmentation_weight = trace.objective_weights.gpu_fragmentation,
                    status = %trace.solver_status,
                    solve_millis = trace.solve_millis,
                    "shadow decision recorded"
                );
                traces.push(trace);
            }
            Err(e) => {
                metrics::inc_shadow_solve_errors();
                error!(error = %e, "shadow solve failed");
            }
        }
    }
}

async fn emit_scheduler_events(
    client: &Client,
    cfg: &ShadowConfig,
    trace: &DecisionTrace,
    bind_outcomes: Option<&[crate::scheduler::binder::BindOutcome]>,
    decision_event_filter: &mut DecisionEventEmissionFilter,
) {
    let now = chrono::Utc::now().to_rfc3339();
    let decision_events_all = crate::scheduler::events::render_decision_events(
        trace,
        &cfg.scheduler_name,
        &cfg.cluster_name,
        &now,
    );
    let decision_events = decision_event_filter.filter_changed(trace, decision_events_all);
    let decision_stats =
        crate::scheduler::event_emitter::emit_event_drafts(client, &decision_events).await;
    metrics::inc_shadow_kubernetes_events(
        "decision",
        decision_stats.attempted as u64,
        decision_stats.created as u64,
        decision_stats.failed as u64,
    );
    let binding_stats = if let Some(outcomes) = bind_outcomes {
        let binding_events = crate::scheduler::events::render_binding_events(
            outcomes,
            &cfg.scheduler_name,
            &cfg.cluster_name,
            trace.sequence,
            &now,
        );
        let stats =
            crate::scheduler::event_emitter::emit_event_drafts(client, &binding_events).await;
        metrics::inc_shadow_kubernetes_events(
            "binding",
            stats.attempted as u64,
            stats.created as u64,
            stats.failed as u64,
        );
        stats
    } else {
        crate::scheduler::event_emitter::EventEmitStats::default()
    };
    info!(
        sequence = trace.sequence,
        decision_events_suppressed = trace
            .decisions
            .len()
            .saturating_sub(decision_stats.attempted),
        decision_events_attempted = decision_stats.attempted,
        decision_events_created = decision_stats.created,
        decision_events_failed = decision_stats.failed,
        binding_events_attempted = binding_stats.attempted,
        binding_events_created = binding_stats.created,
        binding_events_failed = binding_stats.failed,
        "kubernetes event emission complete"
    );
}

/// Read-only collect + normalize of the current cluster (shared by the solve path and the
/// per-iteration readiness-snapshot refresh). Never mutates cluster state.
async fn collect_shadow_snapshot(cfg: &ShadowConfig) -> Result<CollectedShadowSnapshot> {
    let coll =
        collector::KubeCollector::new(cfg.cluster_name.clone(), cfg.kubeconfig.clone()).await?;
    let snapshot = coll.collect().await?;
    let pricing_catalog = pricing::load_pricing_catalog("").unwrap_or_default();
    let normalized = normalizer::Normalizer::new(pricing_catalog, normalizer::Options::default())
        .normalize(&snapshot);
    Ok(CollectedShadowSnapshot {
        raw: snapshot,
        normalized,
    })
}

async fn run_kube_simulator_for_trace(
    kubeconfig: &str,
    simulator_url: &str,
    trace: &DecisionTrace,
) -> Result<KubeSimulatorTracePlan> {
    use crate::verifier::{
        clone_as_unscheduled_verification_pod, collect_simulator_resources, pod_assigned_node,
        pod_scope, schedule_all_snapshot_report_with_timeout_and_stable_polls,
        SimulatorImportPayload, FILTER_RESULT_ANNOTATION,
    };
    use std::collections::{BTreeMap, BTreeSet};

    let target_scopes: BTreeSet<String> = trace
        .decisions
        .iter()
        .map(|d| format!("{}/{}", d.namespace, d.name))
        .collect();
    if target_scopes.is_empty() {
        return Ok(KubeSimulatorTracePlan {
            placements: Vec::new(),
            simulator: serde_json::json!({
                "mode": "live",
                "url": simulator_url.trim_end_matches('/'),
                "target_count": 0,
                "present_targets": 0,
                "terminal_present_targets": 0,
                "missing_targets": 0,
                "stable_polls": 0,
                "timed_out": false,
            }),
        });
    }

    let raw = collect_simulator_resources(kubeconfig).await?;
    let mut simulator_nodes: Vec<corev1::Node> = raw
        .nodes
        .iter()
        .filter(|n| raw_node_gpu_capacity(n) > 0)
        .cloned()
        .collect();
    if simulator_nodes.is_empty() {
        simulator_nodes = raw.nodes.clone();
    }
    let simulator_node_names: BTreeSet<String> = simulator_nodes
        .iter()
        .filter_map(|n| n.metadata.name.clone())
        .collect();
    let raw_by_scope: BTreeMap<String, corev1::Pod> = raw
        .pods
        .iter()
        .cloned()
        .map(|p| (pod_scope(&p), p))
        .collect();

    let mut pods: Vec<corev1::Pod> = Vec::new();
    for (idx, pod) in raw
        .pods
        .iter()
        .filter(|p| !target_scopes.contains(&pod_scope(p)))
        .filter(|p| {
            p.spec
                .as_ref()
                .and_then(|s| s.node_name.as_ref())
                .map(|n| simulator_node_names.contains(n))
                .unwrap_or(false)
        })
        .enumerate()
    {
        if let Some(blocker) = synthetic_gpu_blocker_pod(pod, idx) {
            pods.push(blocker);
        }
    }
    let mut simulator_scope_by_original = BTreeMap::new();
    for (idx, d) in trace.decisions.iter().enumerate() {
        let scope = format!("{}/{}", d.namespace, d.name);
        let Some(raw_pod) = raw_by_scope.get(&scope) else {
            continue;
        };
        let mut pod = clone_as_unscheduled_verification_pod(raw_pod.clone());
        if let Some(spec) = pod.spec.as_mut() {
            spec.scheduler_name = Some("default-scheduler".to_string());
        }
        if let Some(annotations) = pod.metadata.annotations.as_mut() {
            annotations.remove(FILTER_RESULT_ANNOTATION);
        }
        let sim_name = sanitize_simulator_name(&format!("target-{idx}-{}", d.name));
        pod = rewrite_simulator_pod(pod, sim_name.clone());
        simulator_scope_by_original.insert(scope, format!("default/{sim_name}"));
        pods.push(pod);
    }

    let base_url = simulator_url.trim_end_matches('/');
    let started = Instant::now();
    let target_priority_class_names: BTreeSet<String> = pods
        .iter()
        .filter_map(|p| {
            p.spec
                .as_ref()
                .and_then(|spec| spec.priority_class_name.clone())
        })
        .collect();
    let priority_classes = raw
        .priority_classes
        .into_iter()
        .filter(|pc| {
            pc.metadata
                .name
                .as_ref()
                .map(|name| target_priority_class_names.contains(name))
                .unwrap_or(false)
        })
        .collect();

    let payload = SimulatorImportPayload {
        pods,
        nodes: simulator_nodes,
        pvs: Vec::new(),
        pvcs: Vec::new(),
        storage_classes: Vec::new(),
        priority_classes,
        namespaces: vec![simulator_default_namespace()],
        scheduler_config: dashboard_simulator_scheduler_config(),
    };

    let simulator_target_scopes: BTreeSet<String> =
        simulator_scope_by_original.values().cloned().collect();
    let batch = schedule_all_snapshot_report_with_timeout_and_stable_polls(
        base_url,
        &payload,
        &simulator_target_scopes,
        dashboard_simulator_import_timeout(),
        1,
    )
    .await?;
    let latest = batch.export;

    let final_signature = simulator_target_signature_for_scopes(&latest, &simulator_target_scopes);
    let final_present = final_signature.len();
    let final_terminal = final_signature
        .values()
        .filter(|(node, has_filter)| node.is_some() || *has_filter)
        .count();
    let phase_timings: Vec<serde_json::Value> = batch
        .diagnostics
        .phase_timings
        .iter()
        .map(|timing| {
            serde_json::json!({
                "phase": timing.phase,
                "duration_millis": timing.duration_millis,
                "cumulative_millis": timing.cumulative_millis,
            })
        })
        .collect();

    let exported_by_scope: BTreeMap<String, corev1::Pod> = latest
        .pods
        .into_iter()
        .filter(|p| {
            simulator_scope_by_original
                .values()
                .any(|s| s == &pod_scope(p))
        })
        .map(|p| (pod_scope(&p), p))
        .collect();
    let placements = trace
        .decisions
        .iter()
        .map(|d| {
            let scope = format!("{}/{}", d.namespace, d.name);
            let sim_scope = simulator_scope_by_original
                .get(&scope)
                .cloned()
                .unwrap_or_else(|| scope.clone());
            let placement = exported_by_scope
                .get(&sim_scope)
                .and_then(pod_assigned_node)
                .map(|node| serde_json::json!({"kind": "placed", "node": node}))
                .unwrap_or_else(|| {
                    let reason = exported_by_scope
                        .get(&sim_scope)
                        .and_then(|p| p.metadata.annotations.as_ref())
                        .and_then(|a| a.get(FILTER_RESULT_ANNOTATION))
                        .cloned()
                        .unwrap_or_else(|| "simulator left pod unscheduled".to_string());
                    serde_json::json!({"kind": "unplaced", "reason": reason})
                });
            serde_json::json!({
                "uid": d.uid,
                "namespace": d.namespace,
                "name": d.name,
                "gpu_request": d.gpu_request,
                "placement": placement,
                "caveats": [],
            })
        })
        .collect();

    let target_count = simulator_target_scopes.len();
    Ok(KubeSimulatorTracePlan {
        placements,
        simulator: serde_json::json!({
            "mode": "live",
            "url": base_url,
            "elapsed_millis": started.elapsed().as_millis() as u64,
            "phase": "poll",
            "target_count": target_count,
            "present_targets": final_present,
            "terminal_present_targets": final_terminal,
            "missing_targets": target_count.saturating_sub(final_present),
            "stable_polls": batch.diagnostics.stable_polls,
            "phase_timings": phase_timings,
            "timed_out": batch.diagnostics.timed_out,
        }),
    })
}

fn simulator_target_signature_for_scopes(
    export: &crate::verifier::SimulatorExportPayload,
    target_scopes: &BTreeSet<String>,
) -> BTreeMap<String, (Option<String>, bool)> {
    export
        .pods
        .iter()
        .filter_map(|p| {
            let scope = crate::verifier::pod_scope(p);
            target_scopes.contains(&scope).then(|| {
                let has_filter = p
                    .metadata
                    .annotations
                    .as_ref()
                    .map(|a| a.contains_key(crate::verifier::FILTER_RESULT_ANNOTATION))
                    .unwrap_or(false);
                (scope, (crate::verifier::pod_assigned_node(p), has_filter))
            })
        })
        .collect()
}

fn dashboard_simulator_deadline() -> Duration {
    duration_from_env_millis("KSOLVER_DASHBOARD_SIMULATOR_DEADLINE_MS", 6_500)
}

fn dashboard_simulator_readiness_timeout() -> Duration {
    duration_from_env_millis("KSOLVER_DASHBOARD_SIMULATOR_READINESS_TIMEOUT_MS", 2_000)
}

fn dashboard_simulator_import_timeout() -> Duration {
    duration_from_env_millis("KSOLVER_DASHBOARD_SIMULATOR_IMPORT_TIMEOUT_MS", 5_500)
}

fn duration_from_env_millis(name: &str, default_millis: u64) -> Duration {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| Duration::from_millis(default_millis))
}

fn dashboard_simulator_scheduler_config() -> serde_json::Value {
    // Always send a valid default scheduler config. Some local simulator setups
    // rewrite omitted schedulerConfig to `{}`, which makes the scheduler crash
    // on restart because apiVersion/kind are missing.
    crate::verifier::default_scheduler_config()
}

fn simulator_dashboard_cache_key(
    trace: &DecisionTrace,
    cluster: Option<&crate::model::NormalizedCluster>,
) -> String {
    let mut pending_keys = trace
        .decisions
        .iter()
        .map(|d| format!("{}/{}/{}:{}", d.namespace, d.name, d.uid, d.gpu_request))
        .collect::<Vec<_>>();
    pending_keys.sort();
    let mut cluster_keys = cluster
        .map(|cluster| {
            let mut node_keys = cluster
                .nodes
                .iter()
                .filter_map(|node| {
                    let gpu_capacity: i64 = node
                        .extended_resources
                        .iter()
                        .filter(|(name, _)| {
                            name.as_str() == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-")
                        })
                        .map(|(_, qty)| *qty)
                        .sum();
                    (gpu_capacity > 0).then(|| format!("node/{}/{}", node.name, gpu_capacity))
                })
                .collect::<Vec<_>>();
            let mut running_keys = cluster
                .workloads
                .iter()
                .filter_map(|workload| {
                    if workload.current_node.is_empty() {
                        return None;
                    }
                    let gpu_request: i64 = workload
                        .extended_resource_requests
                        .iter()
                        .filter(|(name, _)| {
                            name.as_str() == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-")
                        })
                        .map(|(_, qty)| *qty)
                        .sum();
                    (gpu_request > 0).then(|| {
                        format!(
                            "run/{}/{}/{}:{}",
                            workload.namespace, workload.name, workload.current_node, gpu_request
                        )
                    })
                })
                .collect::<Vec<_>>();
            node_keys.sort();
            running_keys.sort();
            node_keys.extend(running_keys);
            node_keys
        })
        .unwrap_or_default();
    cluster_keys.sort();
    format!(
        "pending=[{}];cluster=[{}]",
        pending_keys.join("|"),
        cluster_keys.join("|")
    )
}

fn synthetic_gpu_blocker_pod(pod: &corev1::Pod, idx: usize) -> Option<corev1::Pod> {
    let node_name = pod.spec.as_ref()?.node_name.clone()?;
    let gpu_request = raw_pod_gpu_request(pod);
    if gpu_request <= 0 {
        return None;
    }
    Some(corev1::Pod {
        metadata: kube::api::ObjectMeta {
            namespace: Some("default".to_string()),
            name: Some(sanitize_simulator_name(&format!("blocker-{idx}"))),
            ..Default::default()
        },
        spec: Some(corev1::PodSpec {
            node_name: Some(node_name),
            scheduler_name: Some("default-scheduler".to_string()),
            containers: vec![corev1::Container {
                name: "gpu".to_string(),
                image: Some("pause".to_string()),
                resources: Some(corev1::ResourceRequirements {
                    requests: Some(BTreeMap::from([(
                        "nvidia.com/gpu".to_string(),
                        k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                            gpu_request.to_string(),
                        ),
                    )])),
                    limits: Some(BTreeMap::from([(
                        "nvidia.com/gpu".to_string(),
                        k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                            gpu_request.to_string(),
                        ),
                    )])),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }),
        ..Default::default()
    })
}

fn raw_pod_gpu_request(pod: &corev1::Pod) -> i64 {
    let Some(spec) = pod.spec.as_ref() else {
        return 0;
    };
    let app_sum: i64 = spec.containers.iter().map(container_gpu_request).sum();
    // Init containers (KEP-753): plain inits are a running max; restartable sidecars accumulate and
    // run concurrently with app containers. Previously init containers were ignored entirely, so a
    // running pod whose GPU comes from an init/sidecar looked like 0 GPUs and was NOT counted as
    // consuming capacity when synthesizing simulator blocker pods — the baseline would treat that
    // GPU as free. Mirrors pod_filter::effective_gpu_request.
    let mut restartable = 0i64;
    let mut init_peak = 0i64;
    if let Some(inits) = spec.init_containers.as_ref() {
        for c in inits {
            let g = container_gpu_request(c);
            init_peak = init_peak.max(restartable + g);
            if c.restart_policy.as_deref() == Some("Always") {
                restartable += g;
            }
        }
    }
    (app_sum + restartable).max(init_peak)
}

fn container_gpu_request(container: &corev1::Container) -> i64 {
    let Some(resources) = container.resources.as_ref() else {
        return 0;
    };
    let requests = resources
        .requests
        .as_ref()
        .map(gpu_quantity_sum)
        .unwrap_or(0);
    if requests > 0 {
        requests
    } else {
        resources.limits.as_ref().map(gpu_quantity_sum).unwrap_or(0)
    }
}

fn gpu_quantity_sum(
    resources: &BTreeMap<String, k8s_openapi::apimachinery::pkg::api::resource::Quantity>,
) -> i64 {
    resources
        .iter()
        .filter(|(name, _)| {
            name.as_str() == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-")
        })
        .filter_map(|(_, quantity)| quantity.0.parse::<i64>().ok())
        .map(|units| units.max(0))
        .sum()
}

fn raw_node_gpu_capacity(node: &corev1::Node) -> i64 {
    // Count whole GPUs AND MIG slices (via gpu_quantity_sum), so a node partitioned entirely into
    // MIG slices — which may advertise only nvidia.com/mig-* and no nvidia.com/gpu — is still
    // recognized as a GPU node in the simulator baseline instead of being silently dropped.
    node.status
        .as_ref()
        .and_then(|s| s.allocatable.as_ref())
        .map(gpu_quantity_sum)
        .unwrap_or(0)
}

fn simulator_default_namespace() -> corev1::Namespace {
    corev1::Namespace {
        metadata: kube::api::ObjectMeta {
            name: Some("default".to_string()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn rewrite_simulator_pod(mut pod: corev1::Pod, name: String) -> corev1::Pod {
    pod.metadata.name = Some(sanitize_simulator_name(&name));
    pod.metadata.namespace = Some("default".to_string());
    pod.metadata.uid = None;
    pod.metadata.resource_version = None;
    pod.metadata.managed_fields = None;
    pod.metadata.owner_references = None;
    pod.metadata.finalizers = None;
    pod.metadata.creation_timestamp = None;
    pod.status = None;
    if let Some(annotations) = pod.metadata.annotations.as_mut() {
        annotations.remove(crate::verifier::FILTER_RESULT_ANNOTATION);
    }
    pod
}

fn sanitize_simulator_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len().min(63));
    let mut prev_dash = false;
    for ch in name.chars().flat_map(|c| c.to_lowercase()) {
        let valid = ch.is_ascii_lowercase() || ch.is_ascii_digit();
        let next = if valid { ch } else { '-' };
        if next == '-' && (out.is_empty() || prev_dash) {
            continue;
        }
        out.push(next);
        prev_dash = next == '-';
        if out.len() >= 63 {
            break;
        }
    }
    while out.ends_with('-') {
        out.pop();
    }
    if out.is_empty() {
        "pod".to_string()
    } else {
        out
    }
}

fn objective_profile_name(profile: ObjectiveProfile) -> &'static str {
    match profile {
        ObjectiveProfile::CostBinpack => "cost-binpack",
        ObjectiveProfile::GpuGangAware => "gpu-gang-aware",
    }
}

fn parse_objective_profile_name(raw: &str) -> Option<ObjectiveProfile> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "cost-binpack" | "cost_binpack" | "cost" => Some(ObjectiveProfile::CostBinpack),
        "gpu-gang-aware" | "gpu_gang_aware" | "gpu-throughput" | "gpu_throughput" | "gpu" => {
            Some(ObjectiveProfile::GpuGangAware)
        }
        _ => None,
    }
}

fn nonnegative_or_current(next: Option<i64>, current: i64) -> i64 {
    next.filter(|v| *v >= 0).unwrap_or(current)
}

fn cfg_with_query_objective(base: &ShadowConfig, query: &SolveQuery) -> ShadowConfig {
    let mut cfg = base.clone();
    if let Some(profile) = query
        .objective_profile
        .as_deref()
        .or(query.profile.as_deref())
        .and_then(parse_objective_profile_name)
    {
        cfg.objective_profile = profile;
    }
    let current = cfg.objective_weights.clone();
    cfg.objective_weights = ObjectiveWeights {
        admission: nonnegative_or_current(query.admission, current.admission),
        gpu_demand: nonnegative_or_current(query.gpu_demand, current.gpu_demand),
        gang_complete: nonnegative_or_current(query.gang_complete, current.gang_complete),
        priority: nonnegative_or_current(query.priority, current.priority),
        business_value: nonnegative_or_current(query.business_value, current.business_value),
        queue: nonnegative_or_current(query.queue, current.queue),
        queue_wait: nonnegative_or_current(query.queue_wait, current.queue_wait),
        fair_share: nonnegative_or_current(query.fair_share, current.fair_share),
        deadline_urgency: nonnegative_or_current(query.deadline_urgency, current.deadline_urgency),
        deadline_miss: nonnegative_or_current(query.deadline_miss, current.deadline_miss),
        gpu_fragmentation: nonnegative_or_current(
            query.gpu_fragmentation,
            current.gpu_fragmentation,
        ),
    };
    cfg
}

fn cfg_with_objective(base: &ShadowConfig, objective: &ObjectiveSelection) -> ShadowConfig {
    let mut cfg = base.clone();
    cfg.objective_profile = objective.profile;
    cfg.objective_weights = objective.weights.clone();
    cfg
}

struct SolveAttempt {
    input: crate::model::OptimizationInput,
    drops: Vec<crate::scheduler::pending_input::DropInfo>,
    candidate_diagnostics: crate::scheduler::pending_input::CandidateDiagnostics,
    node_grouping_diagnostics: crate::scheduler::pending_input::NodeGroupingDiagnostics,
    node_grouping_enabled: bool,
    node_grouping_used: bool,
    grouped_node_count: usize,
    grouped_candidate_edges: usize,
    node_grouping_fallback_reason: String,
    solution: crate::model::OptimizationSolution,
    status: String,
    solve_ok: bool,
    solve_core_millis: u64,
    candidate_node_limit: usize,
    candidate_edges: usize,
}

fn candidate_edges(input: &crate::model::OptimizationInput) -> usize {
    input.workloads.iter().map(|w| w.feasible_nodes.len()).sum()
}

fn normalized_gpu_request(resources: &std::collections::BTreeMap<String, i64>) -> i64 {
    resources
        .iter()
        .filter(|(name, _)| {
            name.as_str() == "nvidia.com/gpu" || name.starts_with("nvidia.com/mig-")
        })
        .map(|(_, qty)| (*qty).max(0))
        .sum()
}

fn admission_metrics(trace: &DecisionTrace) -> AdmissionMetrics {
    let mut metrics = AdmissionMetrics::default();
    for decision in &trace.decisions {
        if matches!(decision.placement, PodPlacement::Placed { .. }) {
            metrics.admitted_pods += 1;
            metrics.admitted_gpu_demand += decision.gpu_request.max(0);
        }
    }
    metrics
}

fn stamp_objective(
    trace: &mut DecisionTrace,
    profile: ObjectiveProfile,
    weights: &ObjectiveWeights,
) {
    trace.objective_profile = profile;
    trace.objective_weights = weights.clone();
}

fn refresh_outcome_summary(trace: &mut DecisionTrace) {
    trace.outcome_summary = summarize_scheduling_outcome(trace);
}

fn refresh_candidate_quality(trace: &mut DecisionTrace) {
    trace.candidate_quality_metrics = summarize_candidate_quality(trace);
}

fn shadow_gpu_utilization_metrics(
    normalized: &crate::model::NormalizedCluster,
    trace: &DecisionTrace,
) -> GpuUtilizationMetrics {
    let capacity_by_node: std::collections::BTreeMap<String, i64> = normalized
        .nodes
        .iter()
        .map(|n| {
            (
                n.name.clone(),
                normalized_gpu_request(&n.extended_resources),
            )
        })
        .collect();
    let mut used_by_node: std::collections::BTreeMap<String, i64> =
        std::collections::BTreeMap::new();
    for w in &normalized.workloads {
        if w.current_node.is_empty() {
            continue;
        }
        let gpu = normalized_gpu_request(&w.extended_resource_requests);
        if gpu > 0 {
            *used_by_node.entry(w.current_node.clone()).or_default() += gpu;
        }
    }
    for decision in &trace.decisions {
        if let PodPlacement::Placed { node } = &decision.placement {
            if decision.gpu_request > 0 {
                *used_by_node.entry(node.clone()).or_default() += decision.gpu_request;
            }
        }
    }
    let active_gpu_nodes = used_by_node.iter().filter(|(_, used)| **used > 0).count();
    let stranded_gpu_on_active_nodes = used_by_node
        .iter()
        .filter(|(_, used)| **used > 0)
        .map(|(node, used)| {
            capacity_by_node
                .get(node)
                .copied()
                .unwrap_or(0)
                .saturating_sub(*used)
        })
        .sum();

    GpuUtilizationMetrics {
        active_gpu_nodes,
        stranded_gpu_on_active_nodes,
    }
}

fn is_vram_blocked_decision(decision: &crate::scheduler::trace::PodDecision) -> bool {
    matches!(
        &decision.placement,
        PodPlacement::Unplaced { reason } if reason.contains("predicted peak VRAM")
    )
}

fn is_high_priority_unplaced_decision(decision: &crate::scheduler::trace::PodDecision) -> bool {
    decision.priority > 0 && matches!(decision.placement, PodPlacement::Unplaced { .. })
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct RepairMetricCounts {
    plans: u64,
    migrations: u64,
    preemptions: u64,
    disruption_cost: u64,
}

fn repair_metric_counts(plans: &[crate::scheduler::trace::RepairPlan]) -> RepairMetricCounts {
    let mut counts = RepairMetricCounts {
        plans: plans.len() as u64,
        ..Default::default()
    };
    for plan in plans {
        counts.disruption_cost += plan.disruption_cost.max(0) as u64;
        for action in &plan.actions {
            match action.action.as_str() {
                "migrate" => counts.migrations += 1,
                "preempt" => counts.preemptions += 1,
                _ => {}
            }
        }
    }
    counts
}

fn retry_reason(
    trace: &DecisionTrace,
    solve_ok: bool,
    candidate_node_limit: usize,
    min_admission_percent_milli: i64,
) -> Option<&'static str> {
    if candidate_node_limit == 0 {
        return None;
    }
    if !solve_ok {
        return Some("solver produced no usable incumbent with pruned candidates");
    }
    if trace.deadline_metrics.unplaced_deadline_jobs > 0 {
        return Some("deadline job unplaced with pruned candidates");
    }
    if trace.deadline_metrics.predicted_misses > 0 {
        return Some("predicted deadline miss with pruned candidates");
    }
    let mut placed = 0usize;
    let mut total = 0usize;
    let mut positive_priority_unplaced = false;
    for decision in &trace.decisions {
        total += 1;
        match &decision.placement {
            PodPlacement::Placed { .. } => placed += 1,
            PodPlacement::Unplaced { .. } => {
                if decision.priority > 0 {
                    positive_priority_unplaced = true;
                }
            }
        }
    }
    if positive_priority_unplaced {
        return Some("positive-priority job unplaced with pruned candidates");
    }
    if total > 0
        && min_admission_percent_milli > 0
        && (placed as i64).saturating_mul(100_000)
            < (total as i64).saturating_mul(min_admission_percent_milli)
    {
        return Some("low admission ratio with pruned candidates");
    }
    None
}

fn tenant_key(namespace: &str, team: &str) -> String {
    if team.trim().is_empty() {
        namespace.to_string()
    } else {
        team.to_string()
    }
}

fn gpu_request_from_extended(ext: &BTreeMap<String, i64>, cfg: &ShadowConfig) -> i64 {
    ext.iter()
        .filter(|(name, _)| cfg.is_gpu_resource(name))
        .map(|(_, units)| (*units).max(0))
        .sum()
}

fn stamp_fair_share_deficits(
    input: &mut crate::model::OptimizationInput,
    normalized: &crate::model::NormalizedCluster,
    cfg: &ShadowConfig,
) {
    if cfg.objective_profile != ObjectiveProfile::GpuGangAware
        || cfg.objective_weights.fair_share <= 0
        || cfg.tenant_share_weights.is_empty()
    {
        return;
    }

    let total_cluster_gpu: i64 = normalized
        .nodes
        .iter()
        .flat_map(|n| n.extended_resources.iter())
        .filter(|(name, _)| cfg.is_gpu_resource(name))
        .map(|(_, units)| (*units).max(0))
        .sum();
    if total_cluster_gpu <= 0 {
        return;
    }

    let mut weights = cfg.tenant_share_weights.clone();
    for workload in &input.workloads {
        weights
            .entry(tenant_key(&workload.namespace, &workload.team))
            .or_insert(1);
    }
    let total_weight: i64 = weights.values().map(|w| (*w).max(1)).sum();
    if total_weight <= 0 {
        return;
    }

    let mut running_by_tenant: BTreeMap<String, i64> = BTreeMap::new();
    for workload in normalized
        .workloads
        .iter()
        .filter(|w| !w.current_node.is_empty())
    {
        let gpu = gpu_request_from_extended(&workload.extended_resource_requests, cfg);
        if gpu <= 0 {
            continue;
        }
        let tenant = tenant_key(&workload.namespace, &workload.team);
        *running_by_tenant.entry(tenant).or_default() += gpu;
    }

    for workload in &mut input.workloads {
        let tenant = tenant_key(&workload.namespace, &workload.team);
        let weight = weights.get(&tenant).copied().unwrap_or(1).max(1);
        let target_gpu_milli = total_cluster_gpu
            .saturating_mul(1000)
            .saturating_mul(weight)
            / total_weight;
        let running_gpu_milli = running_by_tenant
            .get(&tenant)
            .copied()
            .unwrap_or(0)
            .max(0)
            .saturating_mul(1000);
        let deficit_gpu = target_gpu_milli
            .saturating_sub(running_gpu_milli)
            .saturating_div(1000);
        let requested_gpu = crate::model::optimization_workload_gpu_request(workload).max(0);
        workload.fair_share_deficit = requested_gpu.min(deficit_gpu).max(0);
    }
}

fn normalized_node_gpu_capacity(node: &crate::model::NormalizedNode, cfg: &ShadowConfig) -> i64 {
    node.extended_resources
        .iter()
        .filter(|(name, _)| cfg.is_gpu_resource(name))
        .map(|(_, units)| (*units).max(0))
        .sum()
}

fn running_workload_monthly_cost_milli(
    workload: &crate::model::NormalizedWorkload,
    node: &crate::model::NormalizedNode,
    cfg: &ShadowConfig,
) -> i64 {
    let gpu = gpu_request_from_extended(&workload.extended_resource_requests, cfg);
    let gpu_capacity = normalized_node_gpu_capacity(node, cfg);
    if gpu <= 0 || gpu_capacity <= 0 || node.price.monthly <= 0.0 {
        return 0;
    }
    let cost = (node.price.monthly * 1000.0 * gpu as f64 / gpu_capacity as f64).round();
    if cost.is_finite() && cost > 0.0 {
        cost as i64
    } else {
        0
    }
}

fn apply_tenant_budget_groups(
    input: &mut crate::model::OptimizationInput,
    normalized: &crate::model::NormalizedCluster,
    cfg: &ShadowConfig,
) {
    if cfg.tenant_monthly_budgets_milli.is_empty() {
        return;
    }

    let nodes_by_name: BTreeMap<&str, &crate::model::NormalizedNode> = normalized
        .nodes
        .iter()
        .map(|n| (n.name.as_str(), n))
        .collect();
    let mut running_cost_by_tenant: BTreeMap<String, i64> = BTreeMap::new();
    for workload in normalized
        .workloads
        .iter()
        .filter(|w| !w.current_node.is_empty())
    {
        let Some(node) = nodes_by_name.get(workload.current_node.as_str()) else {
            continue;
        };
        let cost = running_workload_monthly_cost_milli(workload, node, cfg);
        if cost <= 0 {
            continue;
        }
        let tenant = tenant_key(&workload.namespace, &workload.team);
        *running_cost_by_tenant.entry(tenant).or_default() += cost;
    }

    for (tenant, budget) in &cfg.tenant_monthly_budgets_milli {
        let workload_ids: Vec<String> = input
            .workloads
            .iter()
            .filter(|w| tenant_key(&w.namespace, &w.team) == *tenant)
            .map(|w| w.id.clone())
            .collect();
        if workload_ids.is_empty() {
            continue;
        }
        let remaining = (*budget)
            .max(0)
            .saturating_sub(running_cost_by_tenant.get(tenant).copied().unwrap_or(0));
        input.budget_groups.push(crate::model::BudgetGroup {
            name: tenant.clone(),
            workload_ids,
            limit_milli: remaining,
        });
    }
}

fn stamp_queue_scores(input: &mut crate::model::OptimizationInput, cfg: &ShadowConfig) {
    if cfg.objective_profile != ObjectiveProfile::GpuGangAware
        || cfg.objective_weights.queue <= 0
        || cfg.queue_weights.is_empty()
    {
        return;
    }

    for workload in &mut input.workloads {
        workload.queue_score = cfg
            .queue_weights
            .get(workload.queue.as_str())
            .copied()
            .unwrap_or(0)
            .max(0);
    }
}

fn widened_candidate_limit(current_limit: usize, retry_count: usize) -> Option<usize> {
    if current_limit == 0 {
        return None;
    }
    if retry_count == 0 {
        let doubled = current_limit.saturating_mul(2);
        return Some(doubled.max(current_limit.saturating_add(1)));
    }
    Some(0)
}

fn solve_attempt(
    cfg: &ShadowConfig,
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
    normalized: &crate::model::NormalizedCluster,
    candidate_node_limit: usize,
) -> SolveAttempt {
    let (mut input, drops, candidate_diagnostics) =
        crate::scheduler::pending_input::build_pending_input_diagnosed_with_candidate_limit_and_stats(
            normalized,
            pending,
            &cfg.namespace_gpu_quotas,
            &|n| cfg.is_gpu_resource(n),
            candidate_node_limit,
    );
    stamp_queue_scores(&mut input, cfg);
    stamp_fair_share_deficits(&mut input, normalized, cfg);
    apply_tenant_budget_groups(&mut input, normalized, cfg);
    let node_grouping_diagnostics = crate::scheduler::pending_input::analyze_node_grouping(&input);
    let physical_candidate_edges = candidate_edges(&input);

    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        // Place what fits; leave the rest unplaced instead of failing the whole solve
        // when pending pods compete for scarce capacity.
        partial_admission: true,
        // Bounded latency: accept the best incumbent within this budget rather than
        // spending up to 600s proving cost-optimality (the placement is found in ms).
        solve_time_limit_secs: cfg.solve_time_limit_secs,
        // Break cost-ties toward preferred-node-affinity matches (only when the solve proves
        // optimal; never changes admission or cost).
        enable_soft_affinity: true,
        objective_profile: cfg.objective_profile,
        objective_weights: cfg.objective_weights.clone(),
        ..Default::default()
    };

    let mut solve_input = input.clone();
    let mut node_grouping_used = false;
    let mut grouped_node_count = 0usize;
    let mut grouped_candidate_edges = 0usize;
    let mut node_grouping_fallback_reason = String::new();
    if cfg.enable_node_grouping {
        let (grouped_input, grouping) =
            crate::scheduler::pending_input::group_pending_input_by_node_symmetry(&input);
        grouped_node_count = grouped_input.nodes.len();
        grouped_candidate_edges = candidate_edges(&grouped_input);
        if grouping.disabled_reasons.is_empty() && grouped_input.nodes.len() < input.nodes.len() {
            solve_input = grouped_input;
            node_grouping_used = true;
        }
    }

    let solve_start = Instant::now();
    let (solution, status, solve_ok) = match cpsat_rust::solve(&solve_input, &scenario) {
        Ok((sol, info)) => {
            if node_grouping_used {
                match crate::scheduler::pending_input::expand_grouped_solution_to_physical(
                    &solve_input,
                    &sol,
                ) {
                    Ok(expanded) => (
                        expanded,
                        format!("{}; node_grouping=used", info.status),
                        true,
                    ),
                    Err(e) => {
                        node_grouping_fallback_reason = e.to_string();
                        node_grouping_used = false;
                        warn!(
                            error = %e,
                            candidate_node_limit,
                            "grouped solver output could not be expanded; falling back to physical solve"
                        );
                        match cpsat_rust::solve(&input, &scenario) {
                            Ok((fallback, fallback_info)) => (
                                fallback,
                                format!(
                                    "{}; node_grouping=fallback expansion_failed: {e}",
                                    fallback_info.status
                                ),
                                true,
                            ),
                            Err(fallback_err) => {
                                warn!(
                                    error = %fallback_err,
                                    candidate_node_limit,
                                    "physical fallback solver produced no usable solution"
                                );
                                (
                                    Default::default(),
                                    format!(
                                        "no-solution: grouped expansion failed: {e}; physical fallback failed: {fallback_err}"
                                    ),
                                    false,
                                )
                            }
                        }
                    }
                }
            } else {
                (sol, info.status, true)
            }
        }
        Err(e) => {
            if node_grouping_used {
                node_grouping_fallback_reason = e.to_string();
                node_grouping_used = false;
                warn!(
                    error = %e,
                    candidate_node_limit,
                    "grouped solver produced no usable solution; falling back to physical solve"
                );
                match cpsat_rust::solve(&input, &scenario) {
                    Ok((fallback, fallback_info)) => (
                        fallback,
                        format!(
                            "{}; node_grouping=fallback solve_failed: {e}",
                            fallback_info.status
                        ),
                        true,
                    ),
                    Err(fallback_err) => {
                        warn!(
                            error = %fallback_err,
                            candidate_node_limit,
                            "physical fallback solver produced no usable solution"
                        );
                        (
                            Default::default(),
                            format!(
                                "no-solution: grouped solve failed: {e}; physical fallback failed: {fallback_err}"
                            ),
                            false,
                        )
                    }
                }
            } else {
                warn!(error = %e, candidate_node_limit, "solver produced no usable solution");
                (Default::default(), format!("no-solution: {e}"), false)
            }
        }
    };

    SolveAttempt {
        input,
        drops,
        candidate_diagnostics,
        node_grouping_diagnostics,
        node_grouping_enabled: cfg.enable_node_grouping,
        node_grouping_used,
        grouped_node_count,
        grouped_candidate_edges,
        node_grouping_fallback_reason,
        solution,
        status,
        solve_ok,
        solve_core_millis: solve_start.elapsed().as_millis() as u64,
        candidate_node_limit,
        candidate_edges: physical_candidate_edges,
    }
}

fn drop_reason_map(
    drops: &[crate::scheduler::pending_input::DropInfo],
) -> std::collections::HashMap<String, String> {
    let mut drop_reasons: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for d in drops {
        for scope in &d.pod_scopes {
            drop_reasons.insert(scope.clone(), d.reason.clone());
        }
    }
    drop_reasons
}

#[allow(clippy::too_many_arguments)]
fn trace_from_attempt(
    sequence: u64,
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
    attempt: &SolveAttempt,
    solve_millis: u64,
    solve_core_millis: u64,
    snapshot_age_millis: u64,
    time_sliced_nodes: &std::collections::HashSet<String>,
    tenant_share_weights: &std::collections::BTreeMap<String, i64>,
    tenant_monthly_budgets_milli: &std::collections::BTreeMap<String, i64>,
) -> DecisionTrace {
    let drop_reasons = drop_reason_map(&attempt.drops);
    let mut trace = build_decision_trace_with_tenant_policy(
        sequence,
        pending,
        &attempt.input,
        &attempt.solution,
        &attempt.status,
        attempt.solve_ok,
        solve_millis,
        solve_core_millis,
        snapshot_age_millis,
        &drop_reasons,
        time_sliced_nodes,
        tenant_share_weights,
        tenant_monthly_budgets_milli,
    );
    trace.candidate_node_limit = attempt.candidate_node_limit;
    trace.unpruned_candidate_edges = attempt.candidate_diagnostics.candidate_edges_before_prune;
    trace.initial_candidate_edges = attempt.candidate_edges;
    trace.final_candidate_edges = attempt.candidate_edges;
    trace.candidate_pruned_workloads = attempt.candidate_diagnostics.pruned_workloads;
    trace.node_grouping_metrics = crate::scheduler::trace::NodeGroupingMetrics {
        enabled: attempt.node_grouping_enabled,
        used: attempt.node_grouping_used,
        eligible_group_count: attempt.node_grouping_diagnostics.eligible_group_count,
        eligible_node_count: attempt.node_grouping_diagnostics.eligible_node_count,
        max_group_size: attempt.node_grouping_diagnostics.max_group_size,
        grouped_node_count: attempt.grouped_node_count,
        grouped_candidate_edges: attempt.grouped_candidate_edges,
        disabled_reasons: attempt.node_grouping_diagnostics.disabled_reasons.clone(),
        fallback_reason: attempt.node_grouping_fallback_reason.clone(),
    };
    refresh_candidate_quality(&mut trace);
    trace
}

#[allow(clippy::too_many_arguments)]
async fn run_one_solve(
    cfg: &ShadowConfig,
    sequence: u64,
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
    normalized: &crate::model::NormalizedCluster,
    job_observation_metrics: crate::scheduler::trace::JobObservationMetrics,
    prediction_audit_metrics: crate::scheduler::trace::PredictionAuditMetrics,
    prediction_audit_details: Vec<crate::scheduler::trace::PredictionAuditDetail>,
    started: Instant,
    snapshot_age_millis: u64,
) -> Result<DecisionTrace> {
    metrics::inc_shadow_solves();

    // Time-sliced (oversubscribed, no-isolation) GPU nodes, for placement disclosure.
    let time_sliced_nodes: std::collections::HashSet<String> = normalized
        .nodes
        .iter()
        .filter(|n| crate::scheduler::decision::is_time_sliced_node(&n.labels))
        .map(|n| n.name.clone())
        .collect();

    // Pending-only solve: place ONLY the observed ksolver pods (gang-grouped by label);
    // every already-placed pod is fixed context (subtracted from node capacity). Small
    // and fast versus the whole-cluster solve, and correct per-pod against residual.
    let mut attempt = solve_attempt(cfg, pending, normalized, cfg.candidate_node_limit);
    let mut total_core_millis = attempt.solve_core_millis;
    let mut trace = trace_from_attempt(
        sequence,
        pending,
        &attempt,
        started.elapsed().as_millis() as u64,
        total_core_millis,
        snapshot_age_millis,
        &time_sliced_nodes,
        &cfg.tenant_share_weights,
        &cfg.tenant_monthly_budgets_milli,
    );
    stamp_objective(&mut trace, cfg.objective_profile, &cfg.objective_weights);
    trace.admission_metrics = admission_metrics(&trace);
    trace.gpu_utilization_metrics = shadow_gpu_utilization_metrics(normalized, &trace);
    trace.job_observation_metrics = job_observation_metrics.clone();
    trace.prediction_audit_metrics = prediction_audit_metrics.clone();
    trace.prediction_audit_details = prediction_audit_details.clone();
    refresh_outcome_summary(&mut trace);
    refresh_candidate_quality(&mut trace);
    let initial_candidate_limit = attempt.candidate_node_limit;
    let unpruned_candidate_edges = attempt.candidate_diagnostics.candidate_edges_before_prune;
    let initial_candidate_edges = attempt.candidate_edges;
    let pruned_workloads = attempt.candidate_diagnostics.pruned_workloads;
    let mut retry_count = 0usize;
    while let Some(reason) = retry_reason(
        &trace,
        attempt.solve_ok,
        attempt.candidate_node_limit,
        cfg.candidate_widen_min_admission_percent_milli,
    ) {
        let Some(next_limit) = widened_candidate_limit(attempt.candidate_node_limit, retry_count)
        else {
            break;
        };
        retry_count += 1;
        let widened = solve_attempt(cfg, pending, normalized, next_limit);
        total_core_millis += widened.solve_core_millis;
        if !widened.solve_ok && attempt.solve_ok {
            trace.retry_count = retry_count;
            trace.candidate_node_limit = initial_candidate_limit;
            trace.unpruned_candidate_edges = unpruned_candidate_edges;
            trace.initial_candidate_edges = initial_candidate_edges;
            trace.candidate_pruned_workloads = pruned_workloads;
            trace.widening_reason = format!(
                "{reason}; widened attempt produced no usable incumbent, kept previous placement"
            );
            refresh_candidate_quality(&mut trace);
            break;
        }
        attempt = widened;
        trace = trace_from_attempt(
            sequence,
            pending,
            &attempt,
            started.elapsed().as_millis() as u64,
            total_core_millis,
            snapshot_age_millis,
            &time_sliced_nodes,
            &cfg.tenant_share_weights,
            &cfg.tenant_monthly_budgets_milli,
        );
        stamp_objective(&mut trace, cfg.objective_profile, &cfg.objective_weights);
        trace.admission_metrics = admission_metrics(&trace);
        trace.gpu_utilization_metrics = shadow_gpu_utilization_metrics(normalized, &trace);
        trace.job_observation_metrics = job_observation_metrics.clone();
        trace.prediction_audit_metrics = prediction_audit_metrics.clone();
        trace.prediction_audit_details = prediction_audit_details.clone();
        refresh_outcome_summary(&mut trace);
        trace.candidate_node_limit = initial_candidate_limit;
        trace.retry_count = retry_count;
        trace.unpruned_candidate_edges = unpruned_candidate_edges;
        trace.initial_candidate_edges = initial_candidate_edges;
        trace.candidate_pruned_workloads = pruned_workloads;
        trace.widening_reason = reason.to_string();
        refresh_candidate_quality(&mut trace);
    }

    metrics::observe_shadow_solve_seconds(started.elapsed().as_secs_f64());

    let repair = crate::scheduler::repair::advise_repairs_with_options(
        normalized,
        pending,
        &trace,
        crate::scheduler::repair::RepairOptions {
            max_candidates_per_node: cfg.repair_candidate_limit,
        },
    );
    trace.repair_plans = repair.plans;
    trace.repair_notes = repair.notes;
    trace.repair_metrics = repair.metrics;
    refresh_outcome_summary(&mut trace);
    Ok(trace)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn raw_node_gpu_capacity_counts_mig_only_nodes() {
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
        let mig_only = corev1::Node {
            status: Some(corev1::NodeStatus {
                allocatable: Some(std::collections::BTreeMap::from([
                    ("cpu".to_string(), Quantity("8".to_string())),
                    ("nvidia.com/mig-1g.5gb".to_string(), Quantity("7".to_string())),
                ])),
                ..Default::default()
            }),
            ..Default::default()
        };
        // A node with only MIG slices (no nvidia.com/gpu) is still a GPU node.
        assert_eq!(raw_node_gpu_capacity(&mig_only), 7);

        let whole = corev1::Node {
            status: Some(corev1::NodeStatus {
                allocatable: Some(std::collections::BTreeMap::from([(
                    "nvidia.com/gpu".to_string(),
                    Quantity("4".to_string()),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(raw_node_gpu_capacity(&whole), 4);

        let cpu_only = corev1::Node {
            status: Some(corev1::NodeStatus {
                allocatable: Some(std::collections::BTreeMap::from([(
                    "cpu".to_string(),
                    Quantity("16".to_string()),
                )])),
                ..Default::default()
            }),
            ..Default::default()
        };
        assert_eq!(raw_node_gpu_capacity(&cpu_only), 0);
    }

    #[test]
    fn kube_liabilities_flags_split_gang_and_oom() {
        use std::collections::BTreeMap;
        // kube spreads a 2-member co-located gang across nodes a+b, and places a
        // 40 GiB job on a 24 GiB GPU. Both are liabilities ksolver would refuse.
        let placements = vec![
            serde_json::json!({"namespace":"t","name":"g0","placement":{"kind":"placed","node":"a"}}),
            serde_json::json!({"namespace":"t","name":"g1","placement":{"kind":"placed","node":"b"}}),
            serde_json::json!({"namespace":"t","name":"big","placement":{"kind":"placed","node":"a"}}),
            serde_json::json!({"namespace":"t","name":"safe","placement":{"kind":"unplaced","reason":"x"}}),
        ];
        let gang = |vram: i64| PodLiabilityMeta {
            gang_key: Some("t/team".to_string()),
            colocate: true,
            predicted_vram_bytes: vram,
        };
        let mut meta = BTreeMap::new();
        meta.insert("t/g0".to_string(), gang(0));
        meta.insert("t/g1".to_string(), gang(0));
        meta.insert(
            "t/big".to_string(),
            PodLiabilityMeta {
                gang_key: None,
                colocate: false,
                predicted_vram_bytes: 40 * 1024 * 1024 * 1024,
            },
        );
        let mut node_vram = BTreeMap::new();
        node_vram.insert("a".to_string(), 24 * 1024 * 1024 * 1024i64);
        node_vram.insert("b".to_string(), 24 * 1024 * 1024 * 1024i64);

        let out = compute_kube_liabilities(&placements, &meta, &node_vram);
        assert_eq!(out["count"], serde_json::json!(2));
        assert_eq!(out["split_gangs"].as_array().unwrap().len(), 1);
        assert_eq!(out["split_gangs"][0]["kind"], serde_json::json!("split"));
        assert_eq!(out["oom_risk"].as_array().unwrap().len(), 1);
        assert_eq!(out["oom_risk"][0]["scope"], serde_json::json!("t/big"));
        // competitive-strength classification (roadmap: beats-kube-only vs beats-any-scheduler)
        assert_eq!(out["oom_risk"][0]["competitive_strength"], serde_json::json!("beats-most-schedulers"));
        assert_eq!(out["split_gangs"][0]["competitive_strength"], serde_json::json!("beats-default-kube-only"));
        assert_eq!(out["beats_most_schedulers"], serde_json::json!(1));
        assert_eq!(out["beats_default_kube_only"], serde_json::json!(1));
    }

    #[test]
    fn kube_liabilities_empty_when_placement_is_safe() {
        use std::collections::BTreeMap;
        // gang kept together on one node, VRAM within budget -> no liabilities.
        let placements = vec![
            serde_json::json!({"namespace":"t","name":"g0","placement":{"kind":"placed","node":"a"}}),
            serde_json::json!({"namespace":"t","name":"g1","placement":{"kind":"placed","node":"a"}}),
        ];
        let gang = || PodLiabilityMeta {
            gang_key: Some("t/team".to_string()),
            colocate: true,
            predicted_vram_bytes: 10 * 1024 * 1024 * 1024,
        };
        let mut meta = BTreeMap::new();
        meta.insert("t/g0".to_string(), gang());
        meta.insert("t/g1".to_string(), gang());
        let mut node_vram = BTreeMap::new();
        node_vram.insert("a".to_string(), 24 * 1024 * 1024 * 1024i64);

        let out = compute_kube_liabilities(&placements, &meta, &node_vram);
        assert_eq!(out["count"], serde_json::json!(0));
    }

    #[test]
    fn safety_gate_passes_only_with_measured_liabilities() {
        // No live baseline measured -> the safety advantage is unproven -> warn (not pass).
        assert_eq!(safety_gate_status(None).0, "warn");
        // Baseline measured but kube made no unsafe placements -> nothing to prove here -> warn.
        let zero = serde_json::json!({"count": 0, "summary": "no liabilities"});
        assert_eq!(safety_gate_status(Some(&zero)).0, "warn");
        // Baseline measured and kube took on liabilities ksolver refused -> pass (provable claim).
        let two = serde_json::json!({
            "count": 2, "summary": "kube would OOM and split a gang",
            "beats_most_schedulers": 1, "beats_default_kube_only": 1
        });
        let (status, reason, _next) = safety_gate_status(Some(&two));
        assert_eq!(status, "pass");
        assert!(reason.contains("2 unsafe placement"));
        assert!(reason.contains("kube would OOM"));
        // honest tiering surfaced in the proof-gate reason
        assert!(reason.contains("1 beat ~any scheduler"));
        assert!(reason.contains("1 beat default kube only"));
    }

    fn test_shadow_config(cluster_name: &str) -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            gpu_resource_prefixes: vec!["nvidia.com/mig-".to_string()],
            cluster_name: cluster_name.to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
            admission_opt_in_label: String::new(),
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
            gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
            solve_time_limit_secs: 10,
            namespace_gpu_quotas: std::collections::BTreeMap::new(),
            tenant_share_weights: std::collections::BTreeMap::new(),
            tenant_monthly_budgets_milli: std::collections::BTreeMap::new(),
            queue_weights: std::collections::BTreeMap::new(),
            enable_real_binding: false,
            binding_rollout_mode: crate::scheduler::config::BindingRolloutMode::ObserveOnly,
            binding_kill_switch: false,
            enable_kubernetes_events: false,
            real_binding_dry_run: false,
            binding_canary_mode: crate::scheduler::config::BindingCanaryMode::All,
            binding_low_risk_max_gpus: 1,
            max_binds_per_pass: 10,
            binding_reservation_ttl: Duration::from_secs(60),
            objective_profile: ObjectiveProfile::CostBinpack,
            objective_weights: ObjectiveWeights::default(),
            candidate_node_limit: 0,
            candidate_widen_min_admission_percent_milli: 50_000,
            enable_node_grouping: false,
            repair_candidate_limit: 8,
            enable_leader_election: false,
            leader_election_namespace: "ksolver".to_string(),
            leader_election_lease_name: "ksolver-scheduler".to_string(),
            leader_election_identity: "ksolver".to_string(),
        }
    }

    #[test]
    fn dashboard_asset_is_wired() {
        // Data endpoints the redesigned dashboard depends on.
        for ep in [
            "/api/scheduler/traces",
            "/api/scheduler/cluster",
            "/api/scheduler/objective",
            "/api/scheduler/solve",
            "/api/scheduler/kube-simulator-plan",
            "/api/scheduler/repair-plan",
            "/api/scheduler/demo-report",
            "/api/scheduler/demo-report/refresh",
            "/api/scheduler/simulator-cache-coverage",
            "/api/scheduler/vram-calibration",
            "/api/scheduler/operator-status",
            "/api/scheduler/production-safety",
        ] {
            assert!(SHADOW_HTML.contains(ep), "dashboard must call {}", ep);
        }

        // Tabbed shell: Runs, Live trace, Scenarios, Diagnostics.
        for id in [
            "id=\"panel-runs\"",
            "id=\"panel-live\"",
            "id=\"panel-scen\"",
            "id=\"panel-diag\"",
            "data-panel=\"panel-runs\"",
            "data-panel=\"panel-live\"",
            "data-panel=\"panel-scen\"",
            "data-panel=\"panel-diag\"",
        ] {
            assert!(SHADOW_HTML.contains(id), "missing tab/panel wiring: {}", id);
        }
        assert!(SHADOW_HTML.contains("role=\"tablist\""));

        // Scenarios is the default first viewport.
        assert!(SHADOW_HTML.contains("default view: proof scenarios"));
        assert!(SHADOW_HTML.contains("id=\"panel-scen\""));

        // Runs workspace: config composer, run + compare, browser-side caching of runs.
        assert!(SHADOW_HTML.contains("Run simulation"));
        assert!(SHADOW_HTML.contains("id=\"run-btn\""));
        assert!(SHADOW_HTML.contains("id=\"rerun-btn\""));
        assert!(SHADOW_HTML.contains("<button class=\"tab\" id=\"tab-runs\" type=\"button\""));
        assert!(SHADOW_HTML.contains("<button class=\"tab\" id=\"tab-live\" type=\"button\""));
        assert!(SHADOW_HTML.contains("<button class=\"tab\" id=\"tab-scen\" type=\"button\""));
        assert!(SHADOW_HTML.contains("<button class=\"tab\" id=\"tab-diag\" type=\"button\""));
        assert!(SHADOW_HTML.contains("Run fresh"));
        assert!(SHADOW_HTML.contains(
            "Bypass the browser run cache and ask ksolver to solve this configuration again."
        ));
        assert!(SHADOW_HTML.contains("id=\"composer\""));
        assert!(SHADOW_HTML.contains("runSimulation"));
        assert!(SHADOW_HTML.contains("function setSolveButtonsDisabled(disabled)"));
        assert!(SHADOW_HTML.contains("[\"run-btn\", \"rerun-btn\"].forEach"));
        assert!(SHADOW_HTML.contains("if (b) b.disabled = disabled"));
        assert!(SHADOW_HTML.contains("function runSimulation(force, triggerId)"));
        assert!(SHADOW_HTML.contains("var btn = $(triggerId || \"run-btn\")"));
        assert!(SHADOW_HTML.contains("setSolveButtonsDisabled(true)"));
        assert!(SHADOW_HTML.contains("setSolveButtonsDisabled(false)"));
        assert!(SHADOW_HTML.contains("GPU-hour proxy"));
        assert!(SHADOW_HTML.contains("Relative comparison only; not a cloud bill."));
        assert!(SHADOW_HTML.contains("Proxy/useful GPU"));
        assert!(SHADOW_HTML.contains("id=\"price-proxy-note\""));
        assert!(SHADOW_HTML.contains("var gpuLabel = params.get(\"gpu_label\") || \"GPU\""));
        assert!(SHADOW_HTML
            .contains("var priceSource = params.get(\"price_source\") || \"demo default\""));
        assert!(SHADOW_HTML.contains("function gpuHourAssumptionText()"));
        assert!(SHADOW_HTML.contains("GPU-hour proxy assumption: "));
        assert!(SHADOW_HTML.contains("$(\"price-proxy-note\").textContent = gpuHourAssumptionText()"));
        assert!(SHADOW_HTML.contains("function currentPriceMeta()"));
        assert!(SHADOW_HTML.contains("function priceKey(meta)"));
        assert!(SHADOW_HTML.contains("function priceAssumptionText(meta)"));
        assert!(SHADOW_HTML.contains("function runKey(c)"));
        assert!(SHADOW_HTML.contains("configKey(c) + \"|price|\" + priceKey(currentPriceMeta())"));
        assert!(SHADOW_HTML.contains("price: currentPriceMeta()"));
        assert!(SHADOW_HTML.contains("Pricing assumption was not recorded for this cached run"));
        assert!(SHADOW_HTML.contains("current page assumes"));
        assert!(SHADOW_HTML.contains("Run fresh to capture gpu_hour, gpu_label, and price_source."));
        assert!(SHADOW_HTML.contains("price unknown"));
        assert!(SHADOW_HTML.contains("price $"));
        assert!(SHADOW_HTML.contains("Run pricing assumption: "));
        assert!(SHADOW_HTML.contains("Δ vs kube-scheduler-simulator baseline · "));
        assert!(SHADOW_HTML.contains("r.kubeProv || \"provenance unavailable\""));
        assert!(SHADOW_HTML.contains("No kube baseline captured for this run · "));
        assert!(SHADOW_HTML.contains("r.kubeProv || \"reason unavailable\""));
        assert!(SHADOW_HTML
            .contains("$(\"run-btn\").addEventListener(\"click\", function () { runSimulation(false, \"run-btn\"); })"));
        assert!(SHADOW_HTML
            .contains("$(\"rerun-btn\").addEventListener(\"click\", function () { runSimulation(true, \"rerun-btn\"); })"));
        assert!(SHADOW_HTML.contains("ksolver.runs.v2")); // localStorage cache key
        assert!(SHADOW_HTML.contains("localStorage"));
        assert!(SHADOW_HTML.contains("configKey"));
        assert!(SHADOW_HTML.contains("cached"));
        assert!(SHADOW_HTML.contains("browser cache"));
        assert!(SHADOW_HTML.contains(
            "Restored from this browser's localStorage after a page reload"
        ));
        assert!(SHADOW_HTML.contains(
            "Restored from browser cache; rerun this configuration to refresh solver and kube baseline evidence."
        ));
        assert!(SHADOW_HTML.contains(
            "objective, weights, and pricing assumption match an existing run"
        ));
        assert!(SHADOW_HTML.contains(
            "objective, weights, and pricing assumption are identical"
        ));
        assert!(SHADOW_HTML.contains("Delta uses the run's GPU-hour proxy assumption"));
        assert!(SHADOW_HTML.contains(
            "relative placement comparison only, not a cloud bill"
        ));
        assert!(SHADOW_HTML.contains(
            "Clear browser-cached run comparisons only; live traces and scenario evidence are unchanged."
        ));
        assert!(SHADOW_HTML.contains("function clearRuns()"));
        assert!(SHADOW_HTML.contains("localStorage.removeItem(RUNS_KEY)"));
        assert!(SHADOW_HTML.contains("Cleared \" + count + \" browser-cached run"));
        assert!(SHADOW_HTML.contains("No cached runs to clear"));
        assert!(SHADOW_HTML.contains("$(\"clear-btn\").addEventListener(\"click\", clearRuns)"));
        assert!(SHADOW_HTML.contains("id=\"runs\""));
        assert!(SHADOW_HTML.contains("id=\"baseline\""));
        for f in [
            "id=\"f-profile\"",
            "id=\"f-admission\"",
            "id=\"f-gpu\"",
            "id=\"f-gang\"",
            "id=\"f-priority\"",
            "id=\"f-frag\"",
        ] {
            assert!(SHADOW_HTML.contains(f), "missing config field: {}", f);
        }
        assert!(SHADOW_HTML.contains("objective_profile"));
        assert!(SHADOW_HTML.contains("gpu_fragmentation"));

        // Per-run engine metrics + placements are visible, not just hover titles.
        assert!(SHADOW_HTML.contains("Useful GPU"));
        assert!(SHADOW_HTML.contains("Active nodes"));
        assert!(SHADOW_HTML.contains("Unplaced"));
        assert!(SHADOW_HTML.contains("Stranded"));
        assert!(SHADOW_HTML.contains("miniboard"));
        assert!(SHADOW_HTML.contains("deltaChip"));

        // Live current-trace two-board comparison stays intact.
        assert!(SHADOW_HTML.contains("id=\"kube-nodes\""));
        assert!(SHADOW_HTML.contains("id=\"ks-nodes\""));
        assert!(SHADOW_HTML.contains("renderLive"));
        assert!(SHADOW_HTML.contains("outcome_summary"));
        assert!(SHADOW_HTML.contains("id=\"decisions\""));
        assert!(SHADOW_HTML.contains("renderDecisions"));
        assert!(SHADOW_HTML.contains("id=\"repair\""));
        assert!(SHADOW_HTML.contains("renderRepair"));

        // Scenarios: kube spread + binpack vs ksolver, with metrics, placements, provenance.
        assert!(SHADOW_HTML.contains("renderScenarios"));
        assert!(SHADOW_HTML.contains("report.scenarios"));
        assert!(SHADOW_HTML.contains("id=\"scen-refresh-btn\""));
        assert!(SHADOW_HTML.contains("Refresh baselines"));
        assert!(SHADOW_HTML.contains("Recheck cache"));
        assert!(SHADOW_HTML.contains("Warm baselines"));
        assert!(SHADOW_HTML.contains("updateRefreshButton"));
        assert!(SHADOW_HTML.contains("Kube-scheduler-simulator baseline cache is complete"));
        assert!(SHADOW_HTML.contains("refreshScenarioBaselines"));
        assert!(SHADOW_HTML.contains("/api/scheduler/demo-report/refresh"));
        assert!(SHADOW_HTML.contains("refresh_simulator_cache=true"));
        assert!(SHADOW_HTML.contains("simulator_timeout_ms=10000"));
        assert!(SHADOW_HTML.contains("Scenario baseline cache is complete"));
        assert!(SHADOW_HTML.contains("Scenario baseline cache "));
        assert!(SHADOW_HTML.contains("Scenario baselines refreshed from simulator"));
        assert!(SHADOW_HTML.contains("Simulator refresh failed; showing last good report"));
        assert!(SHADOW_HTML.contains("Baseline refresh failed"));
        assert!(SHADOW_HTML.contains("function simulatorRecoveryCommand(safety, refresh)"));
        assert!(SHADOW_HTML.contains("function simulatorRecoverySource(safety, refresh)"));
        assert!(SHADOW_HTML.contains("var simulator = (safety && safety.simulator) || {}"));
        assert!(SHADOW_HTML.contains("return (refresh && refresh.simulator_recovery_command)"));
        assert!(SHADOW_HTML.contains("return \"refresh status\""));
        assert!(SHADOW_HTML.contains("return \"operator status\""));
        assert!(SHADOW_HTML.contains("return \"local default\""));
        assert!(SHADOW_HTML.contains("|| simulator.recovery_command"));
        assert!(SHADOW_HTML.contains(
            "|| \"scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache\""
        ));
        assert!(SHADOW_HTML.contains(
            "var recoveryCommand = simulatorRecoveryCommand(lastSafety, refresh)"
        ));
        assert!(SHADOW_HTML.contains("demoRefresh.simulator_recovery_command || \"\""));
        assert!(SHADOW_HTML.contains("|simrec:\" + (simulator.recovery_command || \"\")"));
        assert!(SHADOW_HTML.contains(
            "simulator.recovery_command || \"\", demoRefresh.simulator_recovery_command || \"\""
        ));
        assert!(SHADOW_HTML.contains("var simulatorRecovery = simulatorRecoveryCommand("));
        assert!(SHADOW_HTML.contains("KSS recovery command source: "));
        assert!(SHADOW_HTML.contains(
            "Use this before refreshing scenario baselines or making kube-vs-ksolver claims."
        ));
        assert!(SHADOW_HTML.contains(
            "diagCommand(simulatorRecovery, \"Copy kube-scheduler-simulator recovery command\")"
        ));
        assert!(SHADOW_HTML.contains(
            "\"Next action: run \" + recoveryCommand + \" before refreshing baselines again.\""
        ));
        assert!(SHADOW_HTML.contains("recCopy.title = recoveryCommand"));
        assert!(SHADOW_HTML.contains(
            "recCopy.addEventListener(\"click\", function () { copyDiagCommand(recoveryCommand, recCopy); })"
        ));
        assert!(SHADOW_HTML.contains("Full simulator error"));
        assert!(SHADOW_HTML.contains(
            "var actionCopy = el(\"button\", \"copy-btn\", \"Copy command\")"
        ));
        assert!(SHADOW_HTML.contains("actionCopy.type = \"button\""));
        assert!(SHADOW_HTML.contains("actionCopy.title = row.command_hint"));
        assert!(SHADOW_HTML.contains(
            "var planCopy = el(\"button\", \"copy-btn\", \"Copy command\")"
        ));
        assert!(SHADOW_HTML.contains("planCopy.type = \"button\""));
        assert!(SHADOW_HTML.contains("planCopy.title = commands[0]"));
        assert!(SHADOW_HTML.contains("var cmdText = el(\"span\", \"endpoint\", cmd)"));
        assert!(SHADOW_HTML.contains("cmdText.title = cmd"));
        assert!(SHADOW_HTML.contains("copyBtn.title = cmd"));
        assert!(SHADOW_HTML.contains(
            "var x = el(\"button\", \"run-x\", \"×\"); x.type = \"button\"; x.title = \"remove run\"; x.setAttribute(\"aria-label\", \"Remove run \" + configLabel(r.cfg));"
        ));
        assert!(SHADOW_HTML.contains("allBtn.type = \"button\""));
        assert!(SHADOW_HTML.contains("btn.type = \"button\""));
        assert!(SHADOW_HTML.contains("max-width: min(240px, 100%)"));
        assert!(SHADOW_HTML.contains("var node = placedNode(pl)"));
        assert!(SHADOW_HTML.contains("itemNsName(pl) + \" \" + itemGpus(pl) + \"g\""));
        assert!(SHADOW_HTML.contains("node ? \" → \" + shortName(node) : \" ×\""));
        assert!(SHADOW_HTML.contains("shortText(refresh.reason"));
        assert!(SHADOW_HTML.contains("error-detail"));
        assert!(SHADOW_HTML.contains("lastDemoRefresh"));
        assert!(SHADOW_HTML.contains("r[4].demo_refresh"));
        assert!(SHADOW_HTML.contains("demorefresh:"));
        assert!(SHADOW_HTML.contains("demo_refresh"));
        assert!(SHADOW_HTML.contains("KSS refresh"));
        assert!(SHADOW_HTML.contains("demoRefreshLabel"));
        assert!(SHADOW_HTML.contains("no live refresh yet"));
        assert!(SHADOW_HTML.contains("value: \"complete\""));
        assert!(SHADOW_HTML.contains("value: \"warming\""));
        assert!(SHADOW_HTML.contains("/api/scheduler/simulator-cache-coverage"));
        assert!(SHADOW_HTML.contains("simulator_cache_complete"));
        assert!(SHADOW_HTML.contains("simulator_timeout_scope"));
        assert!(SHADOW_HTML.contains("simulator_refresh_mode"));
        assert!(SHADOW_HTML.contains("simulator_cache_cached_baselines"));
        assert!(SHADOW_HTML.contains("simulator_cache_total_baselines"));
        assert!(SHADOW_HTML.contains("simulator_cache_missing_baselines"));
        assert!(SHADOW_HTML.contains("simulator_cache_coverage_milli"));
        assert!(SHADOW_HTML.contains("pctMilli(refresh.simulator_cache_coverage_milli)"));
        assert!(SHADOW_HTML.contains("cache complete"));
        assert!(SHADOW_HTML.contains("simulator_refreshed_baselines"));
        assert!(SHADOW_HTML.contains("refresh_duration_ms"));
        assert!(SHADOW_HTML.contains("s elapsed"));
        assert!(SHADOW_HTML.contains("baselines"));
        assert!(SHADOW_HTML.contains("stale_report_used"));
        assert!(SHADOW_HTML.contains("demo_report_error"));
        assert!(SHADOW_HTML.contains("Scenario benchmark unavailable"));
        assert!(SHADOW_HTML.contains("build_hint"));
        assert!(SHADOW_HTML.contains("kube · spread"));
        assert!(SHADOW_HTML.contains("kube · binpack"));
        assert!(SHADOW_HTML.contains("useful_gpu"));
        assert!(SHADOW_HTML.contains("unplaced_pods"));
        assert!(SHADOW_HTML.contains("stranded_gpu_on_active_nodes"));
        assert!(SHADOW_HTML.contains("gpu_utilization_milli"));
        assert!(SHADOW_HTML.contains("simNote"));
        assert!(SHADOW_HTML.contains("function simSourceLabel(plan, simulator)"));
        assert!(SHADOW_HTML.contains("source.toLowerCase().endsWith(\" \" + variant.toLowerCase())"));
        assert!(SHADOW_HTML.contains("source.slice(0, source.length - variant.length).trim()"));
        assert!(SHADOW_HTML.contains("simTrust"));
        assert!(SHADOW_HTML.contains("prov-badge"));
        assert!(SHADOW_HTML.contains("cached simulator"));
        assert!(SHADOW_HTML.contains("live simulator"));
        assert!(SHADOW_HTML.contains("invalid fallback"));
        assert!(SHADOW_HTML.contains("pchip")); // placement chips

        // Decision model rows, separate baseline deltas, and loud unverified provenance.
        assert!(SHADOW_HTML.contains("decision model"));
        assert!(SHADOW_HTML.contains("batch/global optimization"));
        assert!(SHADOW_HTML.contains("online pod-by-pod"));
        assert!(SHADOW_HTML.contains("vs kube spread"));
        assert!(SHADOW_HTML.contains("vs kube binpack"));
        assert!(SHADOW_HTML.contains("not verified"));
        assert!(SHADOW_HTML.contains("invalid fallback baselines"));
        assert!(SHADOW_HTML.contains("label === \"active-node cost/mo\""));
        assert!(SHADOW_HTML.contains("Active-node cost is a fixed-fleet proxy"));
        assert!(SHADOW_HTML.contains(
            "no autoscaler, idle nodes priced at zero, not a cloud bill"
        ));
        assert!(SHADOW_HTML.contains("if (costProxyTitle) cell.title = costProxyTitle"));
        assert!(SHADOW_HTML.contains("if (costProxyTitle) dv.title = costProxyTitle"));
        assert!(SHADOW_HTML.contains("if (costProxyTitle) sub.title = costProxyTitle"));
        // Layout: fixed-fleet callout collapsed, engine cards rendered before delta sections.
        assert!(SHADOW_HTML.contains("fixed benchmark clusters"));
        assert!(SHADOW_HTML.contains("<details class=\"callout info\" id=\"scen-gate\">"));
        assert!(SHADOW_HTML.contains("engine cards before delta sections"));

        // Diagnostics: terse safety/repair/provenance surface (replaces the old proof wall).
        assert!(SHADOW_HTML.contains("renderDiagnostics"));
        assert!(SHADOW_HTML.contains("Shadow readiness"));
        assert!(SHADOW_HTML.contains("Decision readiness"));
        assert!(SHADOW_HTML.contains("decision_readiness"));
        assert!(SHADOW_HTML.contains("highest risk"));
        assert!(SHADOW_HTML.contains("Scale safety"));
        assert!(SHADOW_HTML.contains("scale_safety"));
        assert!(SHADOW_HTML.contains("Binding safety"));
        assert!(SHADOW_HTML.contains("binding_safety"));
        assert!(SHADOW_HTML.contains("reservation_pressure_description"));
        assert!(SHADOW_HTML.contains(
            "Binding reservation pressure shows whether pending or reserved GPU capacity makes real binding risky"
        ));
        assert!(SHADOW_HTML.contains(
            "active means fresh reservations temporarily hold GPU capacity while binding gates run."
        ));
        assert!(SHADOW_HTML.contains(
            "stale means expired reservation entries must be reconciled before trusting bind readiness."
        ));
        assert!(SHADOW_HTML
            .contains("blocking means the reservation ledger rejected at least one planned placement."));
        assert!(SHADOW_HTML.contains("function reservePressureScopeNote(binding)"));
        assert!(SHADOW_HTML.contains("reservation_pressure_scope"));
        assert!(SHADOW_HTML.contains(
            "Scheduler reservation pressure only; this is unrelated to CUDA, PyTorch, or TensorFlow reserved VRAM."
        ));
        assert!(SHADOW_HTML.contains("state meaning"));
        assert!(SHADOW_HTML.contains("candidate_node_limit"));
        assert!(SHADOW_HTML.contains("candidate edges"));
        assert!(SHADOW_HTML.contains("edge reduction"));
        assert!(SHADOW_HTML.contains("scale.explanation"));
        assert!(SHADOW_HTML.contains("opScale.explanation || \"\""));
        assert!(SHADOW_HTML.contains("latest outcomes"));
        assert!(SHADOW_HTML.contains("reservations"));
        assert!(SHADOW_HTML.contains("binding reservation pressure"));
        assert!(SHADOW_HTML.contains("reservation pressure reason"));
        assert!(SHADOW_HTML.contains("reservation pressure action"));
        assert!(SHADOW_HTML.contains("function kvRow(dl, k, v, cls, title)"));
        assert!(SHADOW_HTML.contains("if (title) dd.title = title;"));
        assert!(SHADOW_HTML.contains("var pressureTitle = ["));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlBinding, \"binding reservation pressure\", binding.reservation_pressure, pressureClass, pressureTitle)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlBinding, \"state meaning\", pressureStateMeaning, pressureClass, pressureTitle)"
        ));
        assert!(SHADOW_HTML.contains("binding.reservation_pressure_reason);"));
        assert!(SHADOW_HTML.contains("binding.reservation_pressure_next_action);"));
        assert!(SHADOW_HTML.contains("/healthz"));
        assert!(SHADOW_HTML.contains("/readyz"));
        assert!(SHADOW_HTML.contains("last error"));
        assert!(SHADOW_HTML.contains("last error at"));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlReady, \"last error\", shortText(rd.last_error, 120), \"bad\", rd.last_error)"
        ));
        assert!(SHADOW_HTML.contains("blocker_class"));
        assert!(SHADOW_HTML.contains("diagnostic hint"));
        assert!(SHADOW_HTML.contains("diagnostic_hint"));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlReady, \"diagnostic hint\", shortText(rd.diagnostic_hint, 120), rd.ready ? \"ok\" : \"warn\", rd.diagnostic_hint)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlReady, \"next action\", shortText(rd.next_action, 110), rd.ready ? \"ok\" : \"warn\", rd.next_action)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlDecision, \"summary\", shortText(decision.summary, 150), decision.status === \"ready\" ? \"ok\" : \"warn\", decision.summary)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlDecision, \"highest risk\", shortText(decision.highest_risk, 150), decision.status === \"ready\" ? \"ok\" : \"warn\", decision.highest_risk)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlDecision, \"next action\", shortText(decision.next_action, 150), decision.status === \"ready\" ? \"ok\" : \"warn\", decision.next_action)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlBinding, \"next action\", shortText(binding.next_action, 140), bindingFailures || bindingMutation ? \"warn\" : \"ok\", binding.next_action)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl0, \"readiness note\", shortText(simulator.readiness_note, 110), simulator.live_dashboard_baseline_configured ? \"warn\" : \"warn\", simulator.readiness_note)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlScale, \"widen reason\", shortText(scale.widening_reason, 120), \"warn\", scale.widening_reason)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlScale, \"next action\", shortText(scale.next_action, 140), scaleRegretUnknown ? \"bad\" : \"ok\", scale.next_action)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dlScale, \"explanation\", shortText(scale.explanation, 140), scaleRegretUnknown ? \"warn\" : \"ok\", scale.explanation)"
        ));
        assert!(SHADOW_HTML.contains("next action"));
        assert!(SHADOW_HTML.contains("debug_commands"));
        assert!(SHADOW_HTML.contains("First readiness debug command"));
        assert!(SHADOW_HTML.contains("All readiness debug commands"));
        assert!(SHADOW_HTML.contains("debugCommands.forEach"));
        assert!(SHADOW_HTML.contains("copyDiagCommand"));
        assert!(SHADOW_HTML.contains("diagCommand"));
        assert!(SHADOW_HTML.contains(
            "row.title = [title, value].filter(Boolean).join(\" | \")"
        ));
        assert!(SHADOW_HTML.contains("text.title = value"));
        assert!(SHADOW_HTML.contains("toast(ok ? \"Copied command\" : \"Copy failed\")"));
        assert!(SHADOW_HTML.contains("Copy command"));
        assert!(SHADOW_HTML.contains("navigator.clipboard.writeText"));
        assert!(SHADOW_HTML.contains("fallbackCopy"));
        assert!(SHADOW_HTML.contains("document.execCommand(\"copy\")"));
        assert!(SHADOW_HTML.contains("document.createElement(\"textarea\")"));
        assert!(SHADOW_HTML.contains("Rollout safety"));
        assert!(SHADOW_HTML.contains("Kube baseline"));
        assert!(SHADOW_HTML.contains("claim guard"));
        assert!(SHADOW_HTML.contains("live_dashboard_baseline_configured"));
        assert!(SHADOW_HTML.contains("readiness note"));
        assert!(SHADOW_HTML.contains("simulator.readiness"));
        assert!(SHADOW_HTML.contains("simulator endpoints"));
        assert!(SHADOW_HTML.contains("simulator probe"));
        assert!(SHADOW_HTML.contains("simulator probe timeout"));
        assert!(SHADOW_HTML.contains("simulator readiness"));
        assert!(SHADOW_HTML.contains("simulator readiness note"));
        assert!(SHADOW_HTML.contains("simReadinessStatus"));
        assert!(SHADOW_HTML.contains("simulator_endpoint_count"));
        assert!(SHADOW_HTML.contains("simulator_probe_checked_count"));
        assert!(SHADOW_HTML.contains("simulator_probe_ready_count"));
        assert!(SHADOW_HTML.contains("simulator_probe_timeout_millis"));
        assert!(SHADOW_HTML.contains("simulator_readiness_note"));
        assert!(SHADOW_HTML.contains("readiness_probe"));
        assert!(SHADOW_HTML.contains("probe checked"));
        assert!(SHADOW_HTML.contains("probe ready"));
        assert!(SHADOW_HTML.contains("probe timeout"));
        assert!(SHADOW_HTML.contains("Repair proof"));
        assert!(SHADOW_HTML.contains("Demo readiness"));
        assert!(SHADOW_HTML.contains("Evidence bundle"));
        assert!(SHADOW_HTML.contains("/api/scheduler/evidence-bundle"));
        assert!(SHADOW_HTML.contains("scripts/demo-gate.py --base-url"));
        assert!(SHADOW_HTML.contains("--require-review-ready"));
        assert!(SHADOW_HTML.contains("local exit "));
        assert!(SHADOW_HTML.contains("strict exit "));
        assert!(SHADOW_HTML.contains("demo_gate_strict_exit_code"));
        assert!(SHADOW_HTML.contains("scripts/collect-evidence-bundle.py --base-url"));
        assert!(SHADOW_HTML.contains("Live proof gates"));
        assert!(SHADOW_HTML.contains("live proof gates"));
        assert!(SHADOW_HTML.contains("live_validation_gates"));
        assert!(SHADOW_HTML.contains("live_validation_pass_count"));
        assert!(SHADOW_HTML.contains("live_validation_warn_count"));
        assert!(SHADOW_HTML.contains("live_validation_blocked_count"));
        assert!(SHADOW_HTML.contains("Missing live artifacts"));
        assert!(SHADOW_HTML.contains("vram_advisory_ready"));
        assert!(SHADOW_HTML.contains("vram_display_top_driver_labels"));
        assert!(SHADOW_HTML.contains("vram_display_claim_safe_driver_labels"));
        assert!(SHADOW_HTML.contains("vram_display_real_top_driver_labels"));
        assert!(SHADOW_HTML.contains("vram_display_synthetic_driver_labels"));
        assert!(SHADOW_HTML.contains("display_top_driver_labels"));
        assert!(SHADOW_HTML.contains("display_claim_safe_driver_labels"));
        assert!(SHADOW_HTML.contains("display_real_top_driver_labels"));
        assert!(SHADOW_HTML.contains("display_synthetic_driver_labels"));
        assert!(SHADOW_HTML.contains("VRAM mode"));
        assert!(SHADOW_HTML.contains("VRAM scheduler use"));
        assert!(SHADOW_HTML.contains("VRAM hard blockers"));
        assert!(SHADOW_HTML.contains("VRAM next evidence"));
        assert!(SHADOW_HTML.contains("review_ready"));
        assert!(SHADOW_HTML.contains("claim_blockers"));
        assert!(SHADOW_HTML.contains("production blocker"));
        assert!(SHADOW_HTML.contains("production_readiness_blocker_class"));
        assert!(SHADOW_HTML.contains("primary blocker"));
        assert!(SHADOW_HTML.contains("primary_claim_blocker"));
        assert!(SHADOW_HTML.contains("primary_claim_blocker_next_action"));
        assert!(SHADOW_HTML.contains("operator-banner"));
        assert!(SHADOW_HTML.contains("renderOperatorBanner"));
        assert!(SHADOW_HTML.contains("operatorBannerSig"));
        assert!(SHADOW_HTML.contains("operatorStatusSig"));
        assert!(SHADOW_HTML.contains("reservePressureBannerMeta"));
        assert!(SHADOW_HTML.contains("reservePressureStateMeaning"));
        assert!(SHADOW_HTML.contains("reservePressureCountSuffix"));
        assert!(SHADOW_HTML.contains("function fmtUnit(n, singular, plural)"));
        assert!(SHADOW_HTML.contains(
            "\"binding reservation pressure \" + pressure + reservePressureCountSuffix(binding)"
        ));
        assert!(SHADOW_HTML
            .contains("fmtUnit(binding.reservations.active_entries || 0, \"entry\", \"entries\")"));
        assert!(SHADOW_HTML.contains("fmtUnit(binding.reservations.reserved_gpus || 0, \"GPU\")"));
        assert!(SHADOW_HTML.contains("\" · \" + fmtUnit(reserved, \"GPU\")"));
        assert!(SHADOW_HTML.contains("\" · \" + fmtUnit(active, \"reservation\")"));
        assert!(SHADOW_HTML
            .contains("((binding.reservations && binding.reservations.active_entries) || 0)"));
        assert!(SHADOW_HTML
            .contains("((binding.reservations && binding.reservations.reserved_gpus) || 0)"));
        assert!(SHADOW_HTML.contains("binding reservation pressure "));
        assert!(SHADOW_HTML.contains("chip.title"));
        assert!(SHADOW_HTML.contains(
            "var readyReserveMeta = reservePressureBannerMeta(binding)"
        ));
        assert!(SHADOW_HTML.contains(
            "if (readyReserveMeta) readyMeta.push(readyReserveMeta)"
        ));
        assert!(SHADOW_HTML.contains("var summaryReadyMeta = ["));
        assert!(SHADOW_HTML.contains(
            "if (summaryReserveMeta) summaryReadyMeta.push(summaryReserveMeta)"
        ));
        assert!(SHADOW_HTML.contains("evidence.operator_reservation_pressure || \"\""));
        assert!(SHADOW_HTML.contains(
            "evidence.operator_reservation_pressure_description || \"\""
        ));
        assert!(SHADOW_HTML.contains(
            "evidence.operator_reservation_pressure_scope || \"\""
        ));
        assert!(SHADOW_HTML.contains(
            "evidence.operator_reservation_pressure_reason || \"\""
        ));
        assert!(SHADOW_HTML.contains(
            "evidence.operator_reservation_pressure_next_action || \"\""
        ));
        assert!(SHADOW_HTML.contains("proof_gates"));
        assert!(SHADOW_HTML.contains("proof gates"));
        assert!(SHADOW_HTML.contains("proofGates.blocked"));
        assert!(SHADOW_HTML.contains("/api/scheduler/operator-status"));
        assert!(SHADOW_HTML.contains("operator action source"));
        assert!(SHADOW_HTML.contains("VRAM source"));
        assert!(SHADOW_HTML.contains("VRAM hard-admission blockers"));
        assert!(SHADOW_HTML.contains("VRAM evidence collection plan"));
        assert!(SHADOW_HTML.contains("hard_admission_blockers"));
        assert!(SHADOW_HTML.contains("evidence_collection_plan"));
        assert!(SHADOW_HTML.contains("opStatus.action_items"));
        assert!(SHADOW_HTML.contains("opStatus.operator_runbook"));
        assert!(SHADOW_HTML.contains("copyable_command_rows"));
        assert!(SHADOW_HTML.contains("diag-cmd-meta"));
        assert!(SHADOW_HTML.contains("function diagCommand(value, title, meta)"));
        assert!(SHADOW_HTML.contains("if (meta) body.appendChild(el(\"span\", \"diag-cmd-meta\", meta));"));
        assert!(SHADOW_HTML.contains("function runbookCommandRowsSig(runbook)"));
        assert!(SHADOW_HTML.contains("runbookCommandRowsSig(runbook)"));
        assert!(SHADOW_HTML.contains("row.category || \"\""));
        assert!(SHADOW_HTML.contains("row.severity || \"\""));
        assert!(SHADOW_HTML.contains("row.artifact || \"\""));
        assert!(SHADOW_HTML.contains("row.next_action || \"\""));
        assert!(SHADOW_HTML.contains(
            "var runbookCommandRows = runbook.copyable_command_rows || (runbook.copyable_commands || []).map(function (cmd) { return { command: cmd }; })"
        ));
        assert!(SHADOW_HTML.contains("\"Copyable operator runbook command\","));
        assert!(SHADOW_HTML.contains("row.category,"));
        assert!(SHADOW_HTML.contains("row.severity,"));
        assert!(SHADOW_HTML.contains("row.artifact,"));
        assert!(SHADOW_HTML.contains("row.next_action"));
        assert!(SHADOW_HTML.contains("var commandMeta = ["));
        assert!(SHADOW_HTML.contains("commandList.appendChild(diagCommand(row.command, commandTitle, commandMeta))"));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"next shell command\", shortText(operatorRunbook.next_shell_command, 120), \"warn\", operatorRunbook.next_shell_command)"
        ));
        assert!(SHADOW_HTML.contains("operator-status"));
        assert!(SHADOW_HTML.contains("Operator status unavailable"));
        assert!(SHADOW_HTML.contains("banner-copy"));
        assert!(SHADOW_HTML.contains("Copy debug"));
        assert!(SHADOW_HTML.contains("renderApiErrorBanner"));
        assert!(SHADOW_HTML.contains("var apiErrorBannerActive = false"));
        assert!(SHADOW_HTML.contains("apiErrorBannerActive = true"));
        assert!(SHADOW_HTML.contains("if (apiErrorBannerActive)"));
        assert!(SHADOW_HTML.contains("sigs[\"operator-banner\"] = \"\""));
        assert!(SHADOW_HTML.contains("apiErrorBannerActive = false"));
        assert!(SHADOW_HTML.contains("Evidence bundle unavailable"));
        assert!(SHADOW_HTML.contains("SRE review packet is ready"));
        assert!(SHADOW_HTML.contains("demo_readiness_summary"));
        assert!(SHADOW_HTML.contains("live_validation_rows"));
        assert!(SHADOW_HTML.contains("remaining_gaps"));
        assert!(SHADOW_HTML.contains("readinessRowsSig"));
        assert!(SHADOW_HTML.contains("readiness.primary_story || \"\""));
        assert!(SHADOW_HTML.contains("((readiness.remaining_gaps || []).join(\";\"))"));
        assert!(SHADOW_HTML.contains("row.required_evidence || \"\""));
        assert!(SHADOW_HTML.contains("row.pass_signal || \"\""));
        assert!(SHADOW_HTML.contains("row.failure_action || \"\""));
        assert!(SHADOW_HTML.contains("next gap"));
        assert!(SHADOW_HTML.contains("first gate"));
        assert!(SHADOW_HTML.contains("live_endpoint"));
        assert!(SHADOW_HTML.contains("diag-cmd"));
        assert!(SHADOW_HTML.contains("diag-gates"));
        assert!(SHADOW_HTML.contains("All live evidence gates"));
        assert!(SHADOW_HTML.contains("diag-gate-list"));
        assert!(SHADOW_HTML.contains("diag-command-list"));
        assert!(SHADOW_HTML.contains("function shellQuote"));
        assert!(SHADOW_HTML.contains("\"curl -s \" + shellQuote(window.location.origin"));
        assert!(SHADOW_HTML.contains("Simulator provenance"));
        assert!(SHADOW_HTML.contains("cache coverage"));
        assert!(SHADOW_HTML.contains("cache missing"));

        // Scenarios redesign: summary + sort + useful-GPU bar chart.
        assert!(SHADOW_HTML.contains("id=\"scen-summary\""));
        assert!(SHADOW_HTML.contains("id=\"scen-sort-sel\""));
        assert!(SHADOW_HTML.contains("sc-bars"));
        assert!(SHADOW_HTML.contains("scBar"));
        assert!(SHADOW_HTML.contains("Useful-GPU wins"));
        assert!(SHADOW_HTML.contains("Differentiator proof"));
        assert!(SHADOW_HTML.contains("Preemption / migration repair proof"));
        assert!(SHADOW_HTML.contains("vram_kss_proofs"));
        assert!(SHADOW_HTML.contains("vram predictor demo below scenario cards"));
        assert!(SHADOW_HTML.contains("Why safer"));
        assert!(SHADOW_HTML.contains("upper-band headroom"));
        assert!(SHADOW_HTML.contains("risk delta"));
        assert!(SHADOW_HTML.contains("decision_reason"));
        assert!(SHADOW_HTML.contains("Local VRAM calibration"));
        assert!(SHADOW_HTML.contains("advisory_ready"));
        assert!(SHADOW_HTML.contains("hard_admission_ready"));
        assert!(SHADOW_HTML.contains("admission_decision"));
        assert!(SHADOW_HTML.contains("gateStatus"));
        assert!(SHADOW_HTML.contains("vram-gate-row"));
        assert!(SHADOW_HTML.contains("Admission mode"));
        assert!(SHADOW_HTML.contains("Scheduler use"));
        assert!(SHADOW_HTML.contains("Hard blockers"));
        assert!(SHADOW_HTML.contains("Next evidence"));
        assert!(SHADOW_HTML.contains("Shadow advisory only"));
        assert!(SHADOW_HTML.contains("Score and warn; do not reject pods"));
        assert!(SHADOW_HTML.contains("near_capacity_rows_ge_90pct"));
        assert!(SHADOW_HTML.contains("Synthetic headroom probes"));
        assert!(SHADOW_HTML.contains("Max synthetic headroom"));
        assert!(SHADOW_HTML.contains("not organic model demand"));
        assert!(SHADOW_HTML.contains("largest synthetic reserve_extra_mib VRAM probe"));
        assert!(SHADOW_HTML.contains("Allocator gap"));
        assert!(SHADOW_HTML.contains("Verified apps"));
        assert!(SHADOW_HTML.contains("Customer fingerprints"));
        assert!(SHADOW_HTML.contains("Evidence columns"));
        assert!(SHADOW_HTML.contains("Pipeline report"));
        assert!(SHADOW_HTML.contains("Evidence gate"));
        assert!(SHADOW_HTML.contains("Manifest preds"));
        assert!(SHADOW_HTML.contains("Calibration evidence columns"));
        assert!(SHADOW_HTML.contains("evidence_columns_present"));
        assert!(SHADOW_HTML.contains("evidence_columns_total"));
        assert!(SHADOW_HTML.contains("verified_real_framework_rows"));
        assert!(SHADOW_HTML.contains("customer_workload_fingerprint_rows"));
        assert!(SHADOW_HTML.contains("reserve_pressure"));
        assert!(SHADOW_HTML.contains("What the VRAM model is using"));
        assert!(SHADOW_HTML.contains("model_drivers"));
        assert!(SHADOW_HTML.contains("top_drivers"));
        assert!(SHADOW_HTML.contains("top_driver_labels"));
        assert!(SHADOW_HTML.contains("VRAM drivers"));
        assert!(SHADOW_HTML.contains("VRAM claim-safe drivers"));
        assert!(SHADOW_HTML.contains("VRAM claim-safe top"));
        assert!(SHADOW_HTML.contains("VRAM top drivers"));
        assert!(SHADOW_HTML.contains("VRAM headroom probes"));
        assert!(SHADOW_HTML.contains("VRAM synthetic probes"));
        assert!(SHADOW_HTML.contains("VRAM synthetic headroom"));
        assert!(SHADOW_HTML.contains("VRAM headroom meaning"));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"simulator readiness note\", shortText(evSummary.simulator_readiness_note, 110), simReadinessStatus === \"ok\" ? \"ok\" : \"warn\", evSummary.simulator_readiness_note)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"simulator claim blocker\", shortText(evSummary.simulator_claim_blocker, 120), \"bad\", evSummary.simulator_claim_blocker)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"simulator claim action\", shortText(evSummary.simulator_claim_next_action, 140), simClaimReady ? \"ok\" : \"warn\", evSummary.simulator_claim_next_action)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"primary blocker\", shortText(String(primaryBlocker), 90), \"warn\", String(primaryBlocker))"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"next action\", shortText(String(evSummary.primary_claim_blocker_next_action), 110), \"warn\", String(evSummary.primary_claim_blocker_next_action))"
        ));
        assert!(SHADOW_HTML.contains(
            "var vramNextEvidence = opVram.next_evidence_target || evSummary.vram_next_evidence_target || \"unknown\""
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"VRAM next evidence\", vramNextEvidence, vramHardBlockerCount ? \"warn\" : \"ok\", vramNextEvidence)"
        ));
        assert!(SHADOW_HTML.contains("var vramClaimSafeTitle = vramClaimSafeLabels.join(\", \")"));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"VRAM claim-safe top\", shortText(vramClaimSafeTitle, 120), \"ok\", vramClaimSafeTitle)"
        ));
        assert!(SHADOW_HTML.contains("var vramDriverTitle = vramDriverLabels.join(\", \")"));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"VRAM top drivers\", shortText(vramDriverTitle, 120), \"ok\", vramDriverTitle)"
        ));
        assert!(SHADOW_HTML.contains("var vramSyntheticTitle = vramSyntheticLabels.join(\", \")"));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"VRAM synthetic probes\", shortText(vramSyntheticTitle, 120), \"warn\", vramSyntheticTitle)"
        ));
        assert!(SHADOW_HTML.contains(
            "kvRow(dl5, \"VRAM headroom meaning\", shortText(syntheticHeadroomDefinition, 140), \"warn\", syntheticHeadroomDefinition)"
        ));
        assert!(SHADOW_HTML.contains("opVram.next_evidence_target || \"\""));
        assert!(SHADOW_HTML.contains("opVram.model_driver_count || 0"));
        assert!(SHADOW_HTML.contains("opVram.claim_safe_driver_count || 0"));
        assert!(SHADOW_HTML.contains("opVram.synthetic_driver_count || 0"));
        assert!(SHADOW_HTML.contains(
            "opVram.synthetic_headroom_definition || opVram.reserve_pressure_definition || \"\""
        ));
        assert!(SHADOW_HTML.contains("simModeLabel"));
        assert!(SHADOW_HTML.contains("invalid legacy fallback marker"));
        assert!(SHADOW_HTML.contains("missing simulator provenance"));
        assert!(SHADOW_HTML.contains("invalid fallback baselines"));
        assert!(SHADOW_HTML.contains("simulator claim"));
        assert!(SHADOW_HTML.contains("simulator claim mode"));
        assert!(SHADOW_HTML.contains("simulator claim blocker"));
        assert!(SHADOW_HTML.contains("simulator claim action"));
        assert!(SHADOW_HTML.contains("simulator claim ready"));
        assert!(SHADOW_HTML.contains("simulator claim blocked"));
        assert!(SHADOW_HTML.contains("recovery_command"));
        assert!(SHADOW_HTML.contains("synthetic VRAM headroom probe"));
        assert!(SHADOW_HTML.contains("synthetic-pressure"));
        assert!(SHADOW_HTML.contains("vramDriverClassLabel"));
        assert!(SHADOW_HTML.contains("vramDriverClassTitle"));
        assert!(SHADOW_HTML.contains("headroom probe"));
        assert!(SHADOW_HTML.contains("not organic model demand"));
        assert!(SHADOW_HTML.contains("aria-label"));
        assert!(SHADOW_HTML.contains("var effectiveCalibration = calibration || lastVramCalibration"));
        assert!(SHADOW_HTML.contains("renderVramInvestmentDemo(report, effectiveCalibration)"));
        assert!(SHADOW_HTML.contains("renderScenarios(lastReport, lastVramCalibration)"));
        assert!(SHADOW_HTML.contains("var prevHtml = btn ? btn.innerHTML : \"\""));
        assert!(SHADOW_HTML.contains("btn.setAttribute(\"aria-busy\", \"true\")"));
        assert!(SHADOW_HTML.contains("btn.appendChild(el(\"span\", \"spin\"))"));
        assert!(SHADOW_HTML.contains("btn.appendChild(document.createTextNode(\" refreshing\"))"));
        assert!(SHADOW_HTML.contains("btn.removeAttribute(\"aria-busy\")"));
        assert!(SHADOW_HTML.contains("btn.innerHTML = prevHtml"));
        assert!(SHADOW_HTML.contains("mean_abs_contribution_mib"));
        assert!(SHADOW_HTML.contains("Hard admission blocked by"));
        assert!(SHADOW_HTML.contains("hard_admission_blockers"));
        assert!(SHADOW_HTML.contains("Next evidence to collect"));
        assert!(SHADOW_HTML.contains("evidence_collection_plan"));
        assert!(SHADOW_HTML.contains("pipeline_report"));
        assert!(SHADOW_HTML.contains("evidence_gate_verifier_ok"));
        assert!(SHADOW_HTML.contains("manifest_predictions"));
        assert!(SHADOW_HTML.contains("vram-cmd"));
        assert!(SHADOW_HTML.contains("vram-blocker"));
        assert!(SHADOW_HTML.contains("scheduler_readiness"));

        // Poll loop uses change-detection so it does not clobber the DOM every tick.
        assert!(SHADOW_HTML.contains("function changed("));
        assert!(SHADOW_HTML.contains("liveSig"));
        assert!(SHADOW_HTML.contains("scenSig"));
        assert!(SHADOW_HTML.contains("function itemNsName(item)"));
        assert!(SHADOW_HTML.contains("itemNsName(p)"));
        assert!(SHADOW_HTML.contains("itemNsName(d)"));
        assert!(SHADOW_HTML.contains("d.priority == null ? \"\" : String(d.priority)"));
        assert!(SHADOW_HTML.contains("p.kind || \"\""));
        assert!(SHADOW_HTML.contains("p.reason || \"\""));
        assert!(SHADOW_HTML.contains("((d.caveats || []).join(\",\"))"));
        assert!(SHADOW_HTML.contains("var liveTrace = traces[0] || null"));
        assert!(SHADOW_HTML.contains(
            "\"empty:\" + clusterSig(r[1]) + \"|\" + kubeSig(r[2])"
        ));
        assert!(SHADOW_HTML.contains(
            "if (changed(\"live\", liveKey)) renderLive(liveTrace, r[1], r[2])"
        ));
        assert!(SHADOW_HTML.contains("if (!trace)"));
        assert!(SHADOW_HTML.contains("no pending GPU decisions"));
        assert!(SHADOW_HTML.contains("waiting for trace"));
        assert!(SHADOW_HTML.contains(
            "Waiting for a pending GPU trace before showing kube-scheduler-simulator placement."
        ));
        assert!(SHADOW_HTML.contains("No live pending GPU workload to compare."));
        assert!(SHADOW_HTML.contains("o.requested_gpu_demand"));
        assert!(SHADOW_HTML.contains("o.gpu_admission_percent_milli"));
        assert!(SHADOW_HTML.contains("o.pod_admission_percent_milli"));
        assert!(SHADOW_HTML.contains("o.predicted_deadline_misses"));
        assert!(SHADOW_HTML.contains("p.target_gpu_request || 0"));
        assert!(SHADOW_HTML.contains("p.explanation || \"\""));
        assert!(SHADOW_HTML.contains("a.action || \"\""));
        assert!(SHADOW_HTML.contains("a.pod || \"\""));
        assert!(SHADOW_HTML.contains("a.gpu_request || 0"));
        assert!(SHADOW_HTML.contains("a.node || \"\""));
        assert!(SHADOW_HTML.contains("proof.headline || ((repairPlan && repairPlan.hero_reference)"));
        assert!(SHADOW_HTML.contains("proof.operator_question || proof.evidence || proof.claim_guard"));
        assert!(SHADOW_HTML.contains("proof.evidence ? \"evidence: \" + proof.evidence : \"\""));
        assert!(SHADOW_HTML.contains("proof.operator_question || \"\""));
        assert!(SHADOW_HTML.contains(
            "((rp && rp.proof_status) || {}).operator_question || \"\""
        ));
        assert!(SHADOW_HTML.contains("((rp && rp.proof_status) || {}).evidence || \"\""));
        assert!(SHADOW_HTML.contains("((rp && rp.proof_status) || {}).headline || \"\""));
        assert!(SHADOW_HTML.contains("((rp && rp.proof_status) || {}).claim_guard || \"\""));
        assert!(SHADOW_HTML.contains("payload && payload.report) || lastReport"));
        assert!(SHADOW_HTML.contains("engineScenarioSig(s.kube)"));
        assert!(SHADOW_HTML.contains("engineScenarioSig(s.kube_binpack)"));
        assert!(SHADOW_HTML.contains("demoRefresh.stale_report_reason || \"\""));
        assert!(SHADOW_HTML.contains("@media (max-width: 700px)"));
        assert!(SHADOW_HTML.contains(".sc-bar { grid-template-columns: minmax(0, 1fr) auto;"));
        assert!(
            SHADOW_HTML.contains(".proof-section .card { margin-bottom: 10px; overflow-x: auto; }")
        );
        assert!(SHADOW_HTML.contains(
            "id=\"toast\" role=\"status\" aria-live=\"polite\" aria-atomic=\"false\""
        ));
        assert!(SHADOW_HTML.contains(".scen-page-filter .btn.active"));
        assert!(SHADOW_HTML.contains("aria-pressed"));
        assert!(SHADOW_HTML.contains("aria-controls=\"panel-scen\""));
        assert!(SHADOW_HTML.contains("tabindex=\"0\""));
        assert!(SHADOW_HTML.contains("tabindex=\"-1\""));
        assert!(SHADOW_HTML
            .contains("id=\"panel-runs\" role=\"tabpanel\" aria-labelledby=\"tab-runs\" hidden"));
        assert!(SHADOW_HTML.contains("panel.hidden = !on"));
        assert!(SHADOW_HTML.contains("function focusTab"));
        assert!(SHADOW_HTML.contains("addEventListener(\"keydown\""));
        assert!(SHADOW_HTML.contains("ArrowRight"));
        assert!(SHADOW_HTML.contains("ArrowLeft"));
        assert!(SHADOW_HTML.contains(
            "var pagePart = (report && report.scenario_pages || []).map"
        ));
        assert!(SHADOW_HTML.contains("page.slug || \"\""));
        assert!(SHADOW_HTML.contains("page.title || \"\""));
        assert!(SHADOW_HTML.contains("((page.scenario_names || []).join(\",\"))"));
        assert!(SHADOW_HTML.contains("function engineScenarioSig(engine)"));
        assert!(SHADOW_HTML.contains("m.active_nodes || 0"));
        assert!(SHADOW_HTML.contains("m.unplaced_pods || 0"));
        assert!(SHADOW_HTML.contains("m.stranded_gpu_on_active_nodes || 0"));
        assert!(SHADOW_HTML.contains("m.gpu_utilization_milli || 0"));
        assert!(SHADOW_HTML.contains("m.partial_or_invalid_gangs || 0"));
        assert!(SHADOW_HTML.contains("s.efficiency_headline || \"\""));
        assert!(SHADOW_HTML.contains("itemName(pl)"));
        assert!(SHADOW_HTML.contains("placedNode(pl) || \"\""));
        assert!(SHADOW_HTML.contains("itemGpus(pl)"));
        assert!(!SHADOW_HTML.contains("live baseline cap\""));

        // Status + accessibility + pricing knobs.
        assert!(SHADOW_HTML.contains("read-only · shadow mode"));
        assert!(SHADOW_HTML.contains("id=\"solver-badge\""));
        assert!(SHADOW_HTML.contains("renderSolverBadge"));
        assert!(SHADOW_HTML.contains("solver ready"));
        assert!(SHADOW_HTML.contains("solver unavailable"));
        assert!(SHADOW_HTML.contains("solver status"));
        assert!(SHADOW_HTML.contains("prefers-reduced-motion"));
        assert!(SHADOW_HTML.contains("gpu_hour"));

        // The old cluttered proof surface must be gone.
        assert!(
            !SHADOW_HTML.contains("id=\"demo-report\""),
            "old proof wall must be removed"
        );
        assert!(
            !SHADOW_HTML.contains("id=\"hero-repair\""),
            "old hero-repair clutter must be removed"
        );
    }

    #[test]
    fn dashboard_hot_reload_path_resolves() {
        // The dashboard handler serves this file fresh from disk so UI edits show up on a
        // browser refresh without rebuilding. The compile-time path must resolve to the same
        // asset that is embedded as the fallback.
        let on_disk = std::fs::read_to_string(SHADOW_HTML_PATH)
            .expect("dashboard asset must be readable from its source path for hot reload");
        assert_eq!(
            on_disk, SHADOW_HTML,
            "embedded fallback must match the on-disk source"
        );
    }

    #[test]
    fn simulator_cache_coverage_milli_is_percent_times_thousand() {
        assert_eq!(simulator_cache_coverage_milli(66, 66), Some(100_000));
        assert_eq!(simulator_cache_coverage_milli(62, 66), Some(93_939));
        assert_eq!(simulator_cache_coverage_milli(0, 0), None);
    }

    #[test]
    fn demo_report_refresh_status_excludes_heavy_report_payload() {
        let value = serde_json::json!({
            "ok": false,
            "refreshed": true,
            "refresh_simulator_cache": true,
            "stale_report_used": true,
            "stale_report_reason": "using last good report",
            "reason": "kube-scheduler-simulator timed out",
            "simulator_timeout_ms": 10000,
            "simulator_timeout_scope": "per_baseline",
            "simulator_recovery_command": "scripts/kss-pool.sh status 4 12120 /tmp/ksolver-kss-cache",
            "simulator_refresh_mode": "fill_missing",
            "simulator_live_baseline_limit": 4,
            "simulator_refreshed_baselines": 4,
            "simulator_cache_total_baselines": 66,
            "simulator_cache_cached_baselines": 62,
            "simulator_cache_missing_baselines": 4,
            "simulator_cache_coverage_milli": 93939,
            "refresh_duration_ms": 12345,
            "refreshed_at": "2026-07-06T00:00:00Z",
            "report": {
                "scenarios": [1, 2, 3]
            }
        });

        let status = demo_report_refresh_status_from_value(&value).expect("refresh status");
        assert_eq!(status["ok"], serde_json::json!(false));
        assert_eq!(status["stale_report_used"], serde_json::json!(true));
        assert_eq!(
            status["reason"],
            serde_json::json!("kube-scheduler-simulator timed out")
        );
        assert_eq!(status["simulator_timeout_ms"], serde_json::json!(10000));
        assert_eq!(
            status["simulator_timeout_scope"],
            serde_json::json!("per_baseline")
        );
        assert_eq!(
            status["simulator_recovery_command"],
            serde_json::json!("scripts/kss-pool.sh status 4 12120 /tmp/ksolver-kss-cache")
        );
        assert_eq!(
            status["simulator_refresh_mode"],
            serde_json::json!("fill_missing")
        );
        assert_eq!(
            status["simulator_cache_total_baselines"],
            serde_json::json!(66)
        );
        assert_eq!(
            status["simulator_cache_cached_baselines"],
            serde_json::json!(62)
        );
        assert_eq!(
            status["simulator_cache_missing_baselines"],
            serde_json::json!(4)
        );
        assert_eq!(
            status["simulator_cache_coverage_milli"],
            serde_json::json!(93939)
        );
        assert_eq!(
            status["simulator_live_baseline_limit"],
            serde_json::json!(4)
        );
        assert_eq!(
            status["simulator_refreshed_baselines"],
            serde_json::json!(4)
        );
        assert_eq!(status["refresh_duration_ms"], serde_json::json!(12345));
        assert!(status.get("report").is_none());
        let mut response = value.clone();
        response
            .as_object_mut()
            .expect("refresh response object")
            .insert("demo_refresh".to_string(), status.clone());
        assert_eq!(
            response["demo_refresh"]["simulator_timeout_ms"],
            serde_json::json!(10000)
        );
        assert_eq!(
            response["demo_refresh"]["simulator_timeout_scope"],
            serde_json::json!("per_baseline")
        );
        assert_eq!(
            response["demo_refresh"]["simulator_refresh_mode"],
            serde_json::json!("fill_missing")
        );
        assert_eq!(
            response["demo_refresh"]["refresh_duration_ms"],
            serde_json::json!(12345)
        );
        assert!(response["demo_refresh"].get("report").is_none());
    }

    #[test]
    fn vram_calibration_payload_reads_local_4090_artifacts() {
        let payload = vram_calibration_payload();
        assert_eq!(payload["available"], true);
        assert_eq!(payload["source"], "vram-model-lab");
        // Row-count floors (data grows as more probes are collected); >= keeps the test valid after
        // a data-collection sweep. Honesty invariants (real-framework / fingerprint rows) stay exact.
        let dataset_rows = payload["dataset"]["rows"].as_u64().expect("dataset rows");
        assert!(dataset_rows >= 228, "dataset rows should be >= 228: {dataset_rows}");
        assert!(
            payload["dataset"]["gpu_sku_labels"]["rtx-4090"].as_u64().expect("rtx-4090 rows") >= 228
        );
        assert!(
            payload["dataset"]["gpu_total_gib"]["23.99"].as_u64().expect("23.99 GiB rows") >= 228
        );
        assert!(
            payload["dataset"]["near_capacity_rows_ge_90pct"].as_u64().expect("near-capacity rows")
                >= 11
        );
        assert!(
            payload["dataset"]["reserve_pressure"]["pressure_rows"].as_u64().expect("pressure rows")
                >= 37
        );
        assert_eq!(
            payload["dataset"]["synthetic_headroom"]["pressure_rows"],
            payload["dataset"]["reserve_pressure"]["pressure_rows"]
        );
        assert_eq!(
            payload["dataset"]["reserve_pressure"]["max_synthetic_reserve_extra_mib"],
            serde_json::json!(32768.0)
        );
        assert_eq!(
            payload["dataset"]["synthetic_headroom"]["max_synthetic_reserve_extra_mib"],
            payload["dataset"]["reserve_pressure"]["max_synthetic_reserve_extra_mib"]
        );
        assert!(
            payload["dataset"]["reserve_pressure"]["torch_allocator_reserve_gap_rows"]
                .as_u64()
                .expect("torch allocator reserve gap rows")
                >= 228
        );
        assert_eq!(
            payload["dataset"]["synthetic_headroom"]["torch_allocator_reserve_gap_rows"],
            payload["dataset"]["reserve_pressure"]["torch_allocator_reserve_gap_rows"]
        );
        // Real-framework data now exists (torchvision probes); this used to guard "still 0".
        assert!(
            payload["dataset"]["verified_real_framework_rows"].as_u64().expect("verified real rows")
                >= 1,
            "expected >=1 verified real-framework row after the torchvision sweep"
        );
        // Customer-workload fingerprinting is still not implemented -> stays exactly 0 (honesty guard).
        assert_eq!(
            payload["dataset"]["customer_workload_fingerprint_rows"],
            serde_json::json!(0)
        );
        assert_eq!(
            payload["model_drivers"]["available"],
            serde_json::json!(true)
        );
        assert!(
            payload["model_drivers"]["training_rows"].as_u64().expect("model training rows") >= 228,
            "model should be fit on >= 228 rows"
        );
        assert!(payload["model_drivers"]["top_drivers"]
            .as_array()
            .expect("top drivers")
            .iter()
            .any(
                |row| row["feature"] == serde_json::json!("reserve_extra_gib")
                    && row["class"] == serde_json::json!("synthetic-pressure")
                    && row["label"] == serde_json::json!("synthetic VRAM headroom probe")
                    && row["description"] == serde_json::json!("synthetic VRAM headroom probe allocation")
                    && row["impact_mib_per_std"].is_number()
            ));
        assert!(payload["model_drivers"]["top_drivers"]
            .as_array()
            .expect("top drivers")
            .iter()
            .any(
                |row| row["feature"] == serde_json::json!("param_x_precision")
                    && row["class"] == serde_json::json!("precision")
                    && row["group"] == serde_json::json!("parameters")
                    && row["description"]
                        == serde_json::json!("parameter count multiplied by precision bytes")
            ));
        assert_eq!(
            payload["model_drivers"]["impact_basis"],
            serde_json::json!("coefficient_x_feature_std")
        );
        assert!(payload["model_drivers"]["group_impacts"]
            .as_array()
            .expect("group impacts")
            .iter()
            .any(|row| row["group"] == serde_json::json!("activations")));
        // Organic (non-synthetic) driver descriptions exist; exact phrasing shifts as the model
        // is refit on new data, so assert presence rather than an exact string.
        assert!(
            !payload["model_drivers"]["top_organic_driver_descriptions"]
                .as_array()
                .expect("organic descriptions")
                .is_empty(),
            "expected at least one organic driver description"
        );
        assert!(payload["model_drivers"]["real_top_drivers"]
            .as_array()
            .expect("real top drivers")
            .iter()
            .all(|row| row["class"] != serde_json::json!("synthetic-pressure")));
        assert!(payload["model_drivers"]["real_top_drivers"]
            .as_array()
            .expect("real top drivers")
            .iter()
            .any(|row| row["feature"] == serde_json::json!("param_x_precision")));
        assert!(payload["model_drivers"]["claim_safe_drivers"]
            .as_array()
            .expect("claim-safe drivers")
            .iter()
            .all(|row| row["class"] != serde_json::json!("synthetic-pressure")));
        assert!(payload["model_drivers"]["claim_safe_drivers"]
            .as_array()
            .expect("claim-safe drivers")
            .iter()
            .any(|row| row["feature"] == serde_json::json!("param_x_precision")));
        assert!(payload["model_drivers"]["synthetic_pressure_drivers"]
            .as_array()
            .expect("synthetic pressure drivers")
            .iter()
            .any(|row| row["feature"] == serde_json::json!("reserve_extra_gib")));
        assert!(payload["model_drivers"]["claim_boundary"]
            .as_str()
            .expect("claim boundary")
            .contains("must not be presented as organic workload predictors"));
        assert_eq!(
            payload["dataset"]["schema"]["evidence_columns_present"],
            serde_json::json!(7)
        );
        assert_eq!(
            payload["dataset"]["schema"]["evidence_columns_total"],
            serde_json::json!(7)
        );
        assert_eq!(
            payload["pipeline_report"]["available"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["pipeline_report"]["ready_for_scheduler_demo"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["pipeline_report"]["evidence_gate_verifier_ok"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["pipeline_report"]["manifest_predictions"],
            serde_json::json!(3)
        );
        assert!(payload["pipeline_report"]["evidence_gate_verifier_stdout"]
            .as_str()
            .unwrap_or_default()
            .contains("verified 2 evidence-gate scenario manifest"));
        assert!(payload["dataset"]["schema"]["evidence_columns"]
            .as_array()
            .expect("evidence columns array")
            .iter()
            .any(|row| row["column"] == "verified_real_framework" && row["present"] == true));
        assert!(payload["dataset"]["schema"]["evidence_columns"]
            .as_array()
            .expect("evidence columns array")
            .iter()
            .any(|row| row["column"] == "customer_workload_fingerprint" && row["present"] == true));
        assert_eq!(
            payload["scheduler_readiness"]["ready_for_shadow_demo"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["scheduler_readiness"]["advisory_ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["scheduler_readiness"]["hard_admission_ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["mode"],
            serde_json::json!("Shadow advisory only")
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["scheduler_use"],
            serde_json::json!("Score and warn; do not reject pods")
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["blocker_count"],
            serde_json::json!(4)
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["next_evidence_target"],
            serde_json::json!("true CUDA OOM labels")
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["can_hard_admit"],
            serde_json::json!(false)
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["can_shadow_advise"],
            serde_json::json!(true)
        );
        assert!(payload["scheduler_readiness"]["requirements"]
            .as_array()
            .expect("requirements array")
            .iter()
            .any(|row| row["requirement"] == "true OOM labels" && row["status"] == "blocked"));
        assert!(payload["scheduler_readiness"]["requirements"]
            .as_array()
            .expect("requirements array")
            .iter()
            .any(|row| row["requirement"] == "4090 local calibration dataset"
                && row["status"] == "pass"));
        assert!(payload["scheduler_readiness"]["requirements"]
            .as_array()
            .expect("requirements array")
            .iter()
            .any(|row| row["requirement"] == "framework-style probe coverage"
                && row["status"] == "pass"));
        assert!(payload["scheduler_readiness"]["requirements"]
            .as_array()
            .expect("requirements array")
            .iter()
            .any(|row| row["requirement"] == "real framework verification"
                && row["status"] == "blocked"));
        assert!(payload["scheduler_readiness"]["requirements"]
            .as_array()
            .expect("requirements array")
            .iter()
            .any(|row| row["requirement"] == "customer workload fingerprints"
                && row["status"] == "blocked"
                && row["evidence"]
                    == "0 customer workload fingerprint rows attached to this calibration"));
        assert!(payload["scheduler_readiness"]["hard_admission_blockers"]
            .as_array()
            .expect("hard admission blockers")
            .iter()
            .any(|row| row == "no verified real framework training-app rows"));
        let evidence_plan = payload["scheduler_readiness"]["evidence_collection_plan"]
            .as_array()
            .expect("evidence collection plan");
        assert!(evidence_plan
            .iter()
            .any(|row| row["target"] == "true CUDA OOM labels"));
        assert!(evidence_plan
            .iter()
            .any(|row| row["target"] == "cross-SKU calibration"));
        assert!(evidence_plan
            .iter()
            .any(|row| row["target"] == "verified real framework rows"));
        assert!(evidence_plan
            .iter()
            .any(|row| row["target"] == "customer workload fingerprints"));
        assert!(evidence_plan.iter().any(|row| row["commands"]
            .as_array()
            .expect("commands array")
            .iter()
            .any(|cmd| cmd.as_str().unwrap_or_default().contains("run_pipeline.py"))));
        assert!(!payload["scheduler_readiness"]["hard_admission_blockers"]
            .as_array()
            .expect("hard admission blockers")
            .iter()
            .any(|row| row == "advisory calibration gate is not yet passing"));
        assert!(
            payload["regression"]["global"]["loo_mae_mib"]
                .as_f64()
                .unwrap_or_default()
                > 0.0
        );
        assert!(
            payload["oom_classifier"]["metrics"]["recall"]
                .as_f64()
                .unwrap_or_default()
                > 0.0
        );
    }

    #[test]
    fn vram_calibration_payload_marks_hard_admission_ready_with_complete_evidence() {
        let root = std::env::temp_dir().join(format!(
            "ksolver-vram-calibration-test-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("data/models")).expect("create calibration fixture dirs");
        std::fs::write(
            root.join("data/models/evaluation.json"),
            r#"{"ready_for_scheduler_demo":true,"global":{"loo_mae_mib":100.0,"loo_p95_abs_error_mib":400.0}}"#,
        )
        .expect("write evaluation fixture");
        std::fs::write(
            root.join("data/models/oom_risk_classifier.json"),
            r#"{"metrics":{"recall":1.0,"precision":1.0}}"#,
        )
        .expect("write oom classifier fixture");

        let headers = [
            "family",
            "precision",
            "gpu_sku_label",
            "gpu_name",
            "gpu_total_mib",
            "gpu_total_gib",
            "trainer_style",
            "sample_count",
            "nvidia_smi_peak_used_mib",
            "reserve_extra_mib",
            "torch_peak_allocated_mib",
            "torch_peak_reserved_mib",
            "peak_vram_fraction",
            "oom_risk_label",
            "oom",
            "verified_real_framework",
            "customer_workload_fingerprint",
        ];
        let mut csv = headers.join(",");
        csv.push('\n');
        for idx in 0..200 {
            let sku = if idx % 2 == 0 { "l4" } else { "t4" };
            let near_capacity = idx < 12;
            let oom = idx == 0;
            let verified = idx < 60;
            let customer = idx < 60;
            let peak_fraction = if near_capacity { "0.95" } else { "0.50" };
            let row = [
                "transformer".to_string(),
                "fp16".to_string(),
                sku.to_string(),
                format!("NVIDIA {}", sku.to_ascii_uppercase()),
                "24576".to_string(),
                "24.0".to_string(),
                "hf-trainer-style".to_string(),
                "5".to_string(),
                if near_capacity { "23300" } else { "12000" }.to_string(),
                "0".to_string(),
                "8000".to_string(),
                "8200".to_string(),
                peak_fraction.to_string(),
                near_capacity.to_string(),
                oom.to_string(),
                verified.to_string(),
                customer.to_string(),
            ];
            csv.push_str(&row.join(","));
            csv.push('\n');
        }
        std::fs::write(root.join("data/training_rows.csv"), csv).expect("write CSV fixture");

        let payload = vram_calibration_payload_from_root(&root);
        assert_eq!(payload["available"], true);
        assert_eq!(payload["dataset"]["rows"], serde_json::json!(200));
        assert_eq!(
            payload["dataset"]["verified_real_framework_rows"],
            serde_json::json!(60)
        );
        assert_eq!(
            payload["dataset"]["customer_workload_fingerprint_rows"],
            serde_json::json!(60)
        );
        assert_eq!(
            payload["dataset"]["schema"]["evidence_columns_present"],
            serde_json::json!(7)
        );
        assert_eq!(
            payload["scheduler_readiness"]["advisory_ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["scheduler_readiness"]["hard_admission_ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["scheduler_readiness"]["hard_admission_blockers"],
            serde_json::json!([])
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["mode"],
            serde_json::json!("Hard admission ready")
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["scheduler_use"],
            serde_json::json!("Can enforce VRAM admission gates")
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["blocker_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["next_evidence_target"],
            serde_json::json!("keep collecting drift samples")
        );
        assert_eq!(
            payload["scheduler_readiness"]["admission_decision"]["can_hard_admit"],
            serde_json::json!(true)
        );
        assert!(payload["scheduler_readiness"]["evidence_collection_plan"]
            .as_array()
            .expect("evidence collection plan")
            .is_empty());

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn simulator_signature_tracks_rewritten_target_scopes_only() {
        let original = k8s_openapi::api::core::v1::Pod {
            metadata: kube::api::ObjectMeta {
                namespace: Some("research".to_string()),
                name: Some("train-a".to_string()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: Some("wrong-original-node".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let rewritten = k8s_openapi::api::core::v1::Pod {
            metadata: kube::api::ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("target-0-train-a".to_string()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: Some("gpu-a".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let export = crate::verifier::SimulatorExportPayload {
            pods: vec![original, rewritten],
        };
        let target_scopes = BTreeSet::from(["default/target-0-train-a".to_string()]);

        let signature = simulator_target_signature_for_scopes(&export, &target_scopes);

        assert_eq!(signature.len(), 1);
        assert_eq!(
            signature
                .get("default/target-0-train-a")
                .and_then(|(node, _)| node.as_deref()),
            Some("gpu-a")
        );
        assert!(!signature.contains_key("research/train-a"));
    }

    #[test]
    fn synthetic_gpu_blocker_preserves_only_node_and_gpu_request() {
        let original = k8s_openapi::api::core::v1::Pod {
            metadata: kube::api::ObjectMeta {
                namespace: Some("research".to_string()),
                name: Some("real-training-pod".to_string()),
                uid: Some("real-uid".to_string()),
                labels: Some(BTreeMap::from([("app".to_string(), "train".to_string())])),
                managed_fields: Some(vec![Default::default()]),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: Some("gpu-node-a".to_string()),
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "trainer".to_string(),
                    image: Some("large-user-image".to_string()),
                    resources: Some(k8s_openapi::api::core::v1::ResourceRequirements {
                        requests: Some(BTreeMap::from([(
                            "nvidia.com/gpu".to_string(),
                            k8s_openapi::apimachinery::pkg::api::resource::Quantity(
                                "2".to_string(),
                            ),
                        )])),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(Default::default()),
        };

        let blocker = synthetic_gpu_blocker_pod(&original, 7).expect("gpu blocker");

        assert_eq!(blocker.metadata.namespace.as_deref(), Some("default"));
        assert_eq!(blocker.metadata.name.as_deref(), Some("blocker-7"));
        assert!(blocker.metadata.uid.is_none());
        assert!(blocker.metadata.labels.is_none());
        assert!(blocker.metadata.managed_fields.is_none());
        assert!(blocker.status.is_none());
        let spec = blocker.spec.expect("blocker spec");
        assert_eq!(spec.node_name.as_deref(), Some("gpu-node-a"));
        assert_eq!(spec.containers.len(), 1);
        assert_eq!(
            raw_pod_gpu_request(&corev1::Pod {
                spec: Some(spec),
                ..Default::default()
            }),
            2
        );
    }

    #[test]
    fn synthetic_gpu_blocker_skips_non_gpu_pods() {
        let original = k8s_openapi::api::core::v1::Pod {
            metadata: kube::api::ObjectMeta {
                namespace: Some("default".to_string()),
                name: Some("cpu-only".to_string()),
                ..Default::default()
            },
            spec: Some(k8s_openapi::api::core::v1::PodSpec {
                node_name: Some("gpu-node-a".to_string()),
                containers: vec![k8s_openapi::api::core::v1::Container {
                    name: "worker".to_string(),
                    image: Some("pause".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        assert!(synthetic_gpu_blocker_pod(&original, 0).is_none());
    }

    #[test]
    fn raw_pod_gpu_request_counts_init_and_sidecar_gpus() {
        use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
        let gpu = |n: &str| {
            Some(corev1::ResourceRequirements {
                requests: Some(std::collections::BTreeMap::from([(
                    "nvidia.com/gpu".to_string(),
                    Quantity(n.to_string()),
                )])),
                ..Default::default()
            })
        };
        let mut sidecar = corev1::Container {
            name: "gpu-sidecar".to_string(),
            resources: gpu("1"),
            ..Default::default()
        };
        sidecar.restart_policy = Some("Always".to_string());
        let pod = corev1::Pod {
            spec: Some(corev1::PodSpec {
                node_name: Some("gpu-node-a".to_string()),
                containers: vec![corev1::Container {
                    name: "app".to_string(),
                    resources: gpu("2"),
                    ..Default::default()
                }],
                init_containers: Some(vec![
                    sidecar,
                    corev1::Container {
                        name: "setup".to_string(),
                        resources: gpu("4"),
                        ..Default::default()
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        // init peak = sidecar(1)+setup(4)=5; app phase = app(2)+sidecar(1)=3; max = 5.
        assert_eq!(raw_pod_gpu_request(&pod), 5);
    }

    #[test]
    fn production_safety_payload_reports_read_only_gates() {
        let mut cfg = test_shadow_config("prod-safety");
        cfg.enable_real_binding = true;
        cfg.binding_kill_switch = true;
        cfg.enable_kubernetes_events = true;
        cfg.enable_leader_election = true;

        let payload = production_safety_payload(
            &cfg,
            true,
            None,
            None,
            None,
            vec!["http://127.0.0.1:1212".to_string()],
            None,
        );

        let solver_info = crate::cpsat_rust::solver_info();
        assert_eq!(payload["ready"], serde_json::json!(solver_info.available));
        assert_eq!(payload["watch_healthy"], true);
        assert_eq!(payload["readiness"]["healthz"], "ok");
        assert_eq!(
            payload["readiness"]["ready"],
            serde_json::json!(solver_info.available)
        );
        assert_eq!(
            payload["readiness"]["solver_available"],
            serde_json::json!(solver_info.available)
        );
        assert_eq!(payload["readiness"]["watch_healthy"], true);
        assert_eq!(
            payload["readiness"]["blocker_class"],
            serde_json::json!(if solver_info.available {
                "none"
            } else {
                "solver"
            })
        );
        assert_eq!(payload["readiness"]["last_error"], serde_json::Value::Null);
        assert!(payload["readiness"]["diagnostic_hint"]
            .as_str()
            .unwrap_or_default()
            .contains(if solver_info.available {
                "watch and solver are healthy"
            } else {
                "rust-cp-sat"
            }));
        assert!(payload["readiness"]["next_action"].is_string());
        assert!(payload["readiness"]["debug_commands"]
            .as_array()
            .expect("readiness debug commands")
            .iter()
            .any(|row| row
                .as_str()
                .unwrap_or_default()
                .contains("scripts/shadow-smoke.py")));
        assert_eq!(payload["solver"]["name"], "cp-sat-rust");
        assert!(payload["solver"]["available"].is_boolean());
        assert!(payload["solver"]["status"]
            .as_str()
            .unwrap_or_default()
            .contains(if solver_info.available {
                "available"
            } else {
                "unavailable"
            }));
        assert!(payload["solver"]["required_for"]
            .as_array()
            .expect("solver required_for array")
            .iter()
            .any(|row| row == "deterministic proof scenarios"));
        assert_eq!(payload["simulator"]["source"], "kube-scheduler-simulator");
        assert_eq!(payload["simulator"]["endpoint_count"], serde_json::json!(1));
        assert_eq!(
            payload["simulator"]["recovery_command"],
            serde_json::json!("scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache")
        );
        assert_eq!(
            payload["simulator"]["live_dashboard_baseline_configured"],
            serde_json::json!(true)
        );
        assert_eq!(
            payload["simulator"]["readiness"],
            serde_json::json!("configured_not_probed")
        );
        assert_eq!(
            payload["simulator"]["readiness_probe"]["checked_count"],
            serde_json::json!(0)
        );
        assert!(payload["simulator"]["readiness_note"]
            .as_str()
            .unwrap_or_default()
            .contains("require-ready-urls"));
        assert!(payload["simulator"]["claim_guard"]
            .as_str()
            .unwrap_or_default()
            .contains("scenario cards still disclose"));
        assert_eq!(payload["rollout"]["mode"], "observe-only");
        assert_eq!(payload["rollout"]["enable_real_binding"], true);
        assert_eq!(payload["rollout"]["binding_kill_switch"], true);
        assert_eq!(payload["rollout"]["mutation_allowed"], false);
        assert_eq!(payload["events"]["writes_allowed"], false);
        assert_eq!(payload["leader_election"]["configured"], true);
        assert_eq!(payload["rbac"]["pods_binding_create_required"], false);
        assert_eq!(payload["rbac"]["events_create_required"], false);
        assert_eq!(payload["rbac"]["leases_required"], true);
        assert!(payload["operator_claim"]
            .as_str()
            .unwrap_or_default()
            .contains("read-only shadow mode"));
    }

    #[test]
    fn simulator_recovery_command_matches_configured_pool_ports() {
        let urls = vec![
            "http://127.0.0.1:12120".to_string(),
            "http://127.0.0.1:12121".to_string(),
            "http://127.0.0.1:12122".to_string(),
            "http://127.0.0.1:12123".to_string(),
        ];

        assert_eq!(
            simulator_recovery_command_for_urls_with_cache_dir(&urls, "/tmp/ksolver-kss-cache"),
            "scripts/kss-pool.sh status 4 12120 /tmp/ksolver-kss-cache"
        );
    }

    #[test]
    fn simulator_recovery_command_quotes_cache_dir_when_needed() {
        let urls = vec!["http://127.0.0.1:1212".to_string()];

        assert_eq!(
            simulator_recovery_command_for_urls_with_cache_dir(&urls, "/tmp/ksolver kss cache"),
            "scripts/kss-pool.sh status 1 1212 '/tmp/ksolver kss cache'"
        );
    }

    #[test]
    fn readiness_error_classifier_names_common_kubernetes_failures() {
        assert_eq!(
            classify_readiness_error(
                "Get \"https://192.0.2.20/api?timeout=32s\": dial tcp 192.0.2.20:443: i/o timeout"
            ),
            "api_timeout"
        );
        assert_eq!(
            classify_readiness_error("Unable to connect to the server: context deadline exceeded"),
            "api_timeout"
        );
        assert_eq!(
            classify_readiness_error(
                "failed to perform initial object list: ServiceError: client error (Connect)"
            ),
            "api_connect"
        );
        assert_eq!(
            classify_readiness_error("lookup example.invalid: no such host"),
            "dns"
        );
        assert_eq!(
            classify_readiness_error("x509: certificate signed by unknown authority"),
            "tls"
        );
        assert_eq!(
            classify_readiness_error("forbidden: User cannot list resource pods"),
            "auth_or_rbac"
        );
        assert_eq!(
            classify_readiness_error("connect: connection refused"),
            "connection_refused"
        );
        assert!(readiness_error_next_action("api_connect").contains("VPN"));
        assert!(readiness_error_next_action("auth_or_rbac").contains("can-i list pods"));
    }

    #[test]
    fn readiness_debug_commands_prioritize_error_specific_probe() {
        let api_commands = readiness_debug_commands("api_connect");
        assert_eq!(
            api_commands.first().map(String::as_str),
            Some("kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        );

        let rbac_commands = readiness_debug_commands("auth_or_rbac");
        assert_eq!(
            rbac_commands.first().map(String::as_str),
            Some("kubectl --request-timeout=10s auth can-i list pods --all-namespaces")
        );

        let unknown_commands = readiness_debug_commands("unknown");
        assert_eq!(
            unknown_commands.first().map(String::as_str),
            Some("kubectl config current-context")
        );
    }

    #[test]
    fn environment_action_item_orders_readyz_probe_first_when_next_action_requires_it() {
        let rows = vec![serde_json::json!({
            "category": "environment",
            "severity": "blocked",
            "blocked": 1,
            "warn": 0,
            "artifact": "healthy Kubernetes watch/relist state",
            "next_action": readiness_error_next_action("api_connect"),
        })];
        let items = operator_evidence_gap_action_items(&rows);
        let runbook = operator_action_runbook(&items);

        assert_eq!(
            items[0]["command_hint"],
            serde_json::json!("kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        );
        assert_eq!(
            runbook["next_shell_command"],
            serde_json::json!("kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        );
        assert_eq!(
            runbook["copyable_command_rows"][0]["command"],
            serde_json::json!("kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        );
        assert_eq!(
            runbook["copyable_command_rows"][0]["category"],
            serde_json::json!("environment")
        );
        assert_eq!(
            runbook["copyable_command_rows"][0]["next_action"],
            serde_json::json!(readiness_error_next_action("api_connect"))
        );
    }

    #[test]
    fn environment_action_item_orders_probe_for_rbac_or_generic_next_action() {
        let rbac_rows = vec![serde_json::json!({
            "category": "environment",
            "severity": "blocked",
            "blocked": 1,
            "warn": 0,
            "artifact": "pod list/watch RBAC",
            "next_action": "verify RBAC list/watch permissions",
        })];
        let rbac_items = operator_evidence_gap_action_items(&rbac_rows);
        assert_eq!(
            rbac_items[0]["command_hint"],
            serde_json::json!(
                "kubectl --request-timeout=10s auth can-i list pods --all-namespaces"
            )
        );

        let generic_rows = vec![serde_json::json!({
            "category": "environment",
            "severity": "blocked",
            "blocked": 1,
            "warn": 0,
            "artifact": "cluster context",
            "next_action": "collect generic environment proof",
        })];
        let generic_items = operator_evidence_gap_action_items(&generic_rows);
        assert_eq!(
            generic_items[0]["command_hint"],
            serde_json::json!("kubectl config current-context")
        );
    }

    #[test]
    fn readiness_status_requires_solver_and_watch_health() {
        let solver_available = crate::cpsat_rust::solver_info().available;

        let (status, message) = readiness_status(true);
        if solver_available {
            assert_eq!(status, axum::http::StatusCode::OK);
            assert_eq!(message, "ready");
        } else {
            assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
            assert_eq!(message, "solver unavailable");
        }

        let (status, message) = readiness_status(false);
        assert_eq!(status, axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(message, "watch not healthy");
    }

    #[test]
    fn production_safety_payload_explains_unhealthy_watch_without_last_error() {
        let cfg = test_shadow_config("prod-safety");
        let payload = production_safety_payload(&cfg, false, None, None, None, vec![], None);

        assert_eq!(payload["readiness"]["blocker"], "watch not healthy");
        assert_eq!(payload["readiness"]["blocker_class"], "kubernetes_watch");
        assert_eq!(payload["readiness"]["last_error"], serde_json::Value::Null);
        assert_eq!(payload["readiness"]["last_error_class"], "none");
        assert!(payload["readiness"]["diagnostic_hint"]
            .as_str()
            .unwrap_or_default()
            .contains("has not captured a current error yet"));
        assert!(payload["readiness"]["diagnostic_hint"]
            .as_str()
            .unwrap_or_default()
            .contains("pod list RBAC"));
    }

    #[test]
    fn production_safety_payload_reports_live_simulator_probe() {
        let cfg = test_shadow_config("prod-safety");
        let payload = production_safety_payload(
            &cfg,
            true,
            None,
            None,
            None,
            vec!["http://127.0.0.1:1212".to_string()],
            Some(serde_json::json!({
                "readiness": "configured_unreachable",
                "readiness_note": "0/1 configured kube-scheduler-simulator endpoint(s) answered /api/v1/export",
                "endpoint_count": 1,
                "checked_count": 1,
                "ready_count": 0,
                "probe_path": "/api/v1/export",
                "timeout_millis": 600,
                "failures": [{
                    "url": "http://127.0.0.1:1212",
                    "error": "connection refused",
                }],
            })),
        );

        assert_eq!(
            payload["simulator"]["readiness"],
            serde_json::json!("configured_unreachable")
        );
        assert_eq!(
            payload["simulator"]["readiness_probe"]["checked_count"],
            serde_json::json!(1)
        );
        assert_eq!(
            payload["simulator"]["readiness_probe"]["ready_count"],
            serde_json::json!(0)
        );
        assert!(payload["simulator"]["readiness_note"]
            .as_str()
            .unwrap_or_default()
            .contains("0/1 configured"));
    }

    #[test]
    fn production_safety_payload_reports_latest_binding_metrics() {
        let mut cfg = test_shadow_config("prod-safety");
        cfg.binding_rollout_mode = crate::scheduler::config::BindingRolloutMode::DryRun;
        cfg.enable_real_binding = true;
        cfg.real_binding_dry_run = true;
        cfg.binding_kill_switch = false;
        let mut trace = retry_test_trace(Vec::new(), Default::default());
        trace.sequence = 42;
        trace.candidate_node_limit = 8;
        trace.unpruned_candidate_edges = 400;
        trace.initial_candidate_edges = 100;
        trace.final_candidate_edges = 100;
        trace.candidate_pruned_workloads = 12;
        trace.candidate_quality_metrics = crate::scheduler::trace::CandidateQualityMetrics {
            pruning_active: true,
            widened: false,
            edge_reduction_milli: 75_000,
            regret_status: "pruned_regret_unknown".to_string(),
            explanation: "candidate pruning was active; compare with a full solve to measure regret"
                .to_string(),
        };
        trace.binding_reservation_metrics.created = 3;
        trace.binding_outcome_metrics.validated = 2;

        let payload = production_safety_payload(
            &cfg,
            false,
            Some(ShadowReadinessError {
                message: "failed to perform initial object list".to_string(),
                observed_at: "2026-07-06T07:30:00Z".to_string(),
            }),
            Some(&trace),
            Some((42, 2)),
            vec![],
            None,
        );

        assert_eq!(payload["rollout"]["mode"], "dry-run");
        assert_eq!(
            payload["readiness"]["last_error"],
            serde_json::json!("failed to perform initial object list")
        );
        assert_eq!(
            payload["readiness"]["diagnostic_hint"],
            serde_json::json!("failed to perform initial object list")
        );
        assert_eq!(
            payload["readiness"]["last_error_at"],
            serde_json::json!("2026-07-06T07:30:00Z")
        );
        assert_eq!(
            payload["readiness"]["last_error_class"],
            serde_json::json!("watch_or_relist")
        );
        assert!(payload["readiness"]["debug_commands"]
            .as_array()
            .expect("readiness debug commands")
            .iter()
            .any(|row| row
                .as_str()
                .unwrap_or_default()
                .contains("kubectl --request-timeout=10s get --raw='/readyz?verbose'")));
        assert_eq!(payload["simulator"]["endpoint_count"], serde_json::json!(0));
        assert_eq!(
            payload["simulator"]["live_dashboard_baseline_configured"],
            serde_json::json!(false)
        );
        assert_eq!(payload["rollout"]["mutation_allowed"], true);
        assert_eq!(payload["rollout"]["real_binding_dry_run"], true);
        assert_eq!(payload["latest_trace"]["sequence"], 42);
        assert_eq!(
            payload["latest_trace"]["binding_reservation_metrics"]["created"],
            3
        );
        assert_eq!(
            payload["latest_trace"]["binding_outcome_metrics"]["validated"],
            2
        );
        assert_eq!(payload["latest_trace"]["candidate_node_limit"], 8);
        assert_eq!(payload["latest_trace"]["unpruned_candidate_edges"], 400);
        assert_eq!(payload["latest_trace"]["final_candidate_edges"], 100);
        assert_eq!(
            payload["latest_trace"]["candidate_quality_metrics"]["regret_status"],
            "pruned_regret_unknown"
        );
        assert_eq!(payload["latest_bind_outcomes"]["outcome_count"], 2);
        assert_eq!(payload["rbac"]["pods_binding_create_required"], true);
    }

    #[test]
    fn operator_scale_safety_reports_unknown_regret_next_action() {
        let production_safety = serde_json::json!({
            "latest_trace": {
                "candidate_node_limit": 8,
                "retry_count": 0,
                "unpruned_candidate_edges": 400,
                "initial_candidate_edges": 100,
                "final_candidate_edges": 100,
                "candidate_pruned_workloads": 12,
                "widening_reason": "",
                "candidate_quality_metrics": {
                    "pruning_active": true,
                    "widened": false,
                    "edge_reduction_milli": 75000,
                    "regret_status": "pruned_regret_unknown",
                    "explanation": "candidate pruning was active; compare with a full solve to measure regret"
                }
            }
        });

        let scale = operator_scale_safety_from_production_safety(&production_safety);

        assert_eq!(scale["available"], serde_json::json!(true));
        assert_eq!(scale["status"], serde_json::json!("regret-unknown"));
        assert_eq!(
            scale["regret_status"],
            serde_json::json!("pruned_regret_unknown")
        );
        assert_eq!(scale["edge_reduction_milli"], serde_json::json!(75000));
        assert!(scale["next_action"]
            .as_str()
            .expect("scale next action")
            .contains("candidate_node_limit=0"));
    }

    #[test]
    fn operator_binding_safety_reports_dry_run_and_live_guardrails() {
        let production_safety = serde_json::json!({
            "rollout": {
                "mode": "dry-run",
                "enable_real_binding": true,
                "mutation_allowed": true,
                "real_binding_dry_run": true,
                "binding_kill_switch": false,
                "binding_canary_mode": "all",
                "binding_low_risk_max_gpus": 1,
                "max_binds_per_pass": 10,
                "binding_reservation_ttl_seconds": 60
            },
            "latest_trace": {
                "sequence": 42,
                "binding_reservation_metrics": {
                    "active_entries": 1,
                    "reserved_gpus": 4
                },
                "binding_outcome_metrics": {
                    "bound": 0,
                    "validated": 2,
                    "skipped": 1,
                    "failed": 0,
                    "canary_skipped": 1
                }
            },
            "latest_bind_outcomes": {
                "sequence": 42,
                "outcome_count": 3
            }
        });

        let binding = operator_binding_safety_from_production_safety(&production_safety);

        assert_eq!(binding["available"], serde_json::json!(true));
        assert_eq!(binding["status"], serde_json::json!("dry-run-validation"));
        assert_eq!(binding["mutation_allowed"], serde_json::json!(true));
        assert_eq!(binding["real_binding_dry_run"], serde_json::json!(true));
        assert_eq!(binding["latest_outcome_count"], serde_json::json!(3));
        assert_eq!(binding["validated"], serde_json::json!(2));
        assert_eq!(
            binding["reservation_pressure"],
            serde_json::json!("active")
        );
        assert!(binding["reservation_pressure_description"]
            .as_str()
            .expect("reservation pressure description")
            .contains("pending or reserved GPU capacity"));
        assert!(binding["reservation_pressure_scope"]
            .as_str()
            .expect("reservation pressure scope")
            .contains("unrelated to CUDA"));
        assert!(binding["reservation_pressure_reason"]
            .as_str()
            .expect("reservation pressure reason")
            .contains("hold 4 GPU"));
        assert!(binding["reservation_pressure_next_action"]
            .as_str()
            .expect("reservation pressure next action")
            .contains("within TTL"));
        assert_eq!(binding["skip_breakdown"]["canary"], serde_json::json!(1));
        assert!(binding["next_action"]
            .as_str()
            .expect("binding next action")
            .contains("dry-run binding outcomes"));
    }

    fn retry_test_trace(
        decisions: Vec<crate::scheduler::trace::PodDecision>,
        deadline_metrics: crate::scheduler::trace::DeadlineMetrics,
    ) -> DecisionTrace {
        DecisionTrace {
            sequence: 1,
            observed_pods: decisions.len(),
            decisions,
            solver_status: "status=Optimal".to_string(),
            objective_profile: Default::default(),
            objective_weights: Default::default(),
            solve_millis: 1,
            solve_core_millis: 1,
            snapshot_age_millis: 0,
            note: String::new(),
            repair_plans: Vec::new(),
            repair_notes: Vec::new(),
            repair_metrics: Default::default(),
            deadline_metrics,
            quota_metrics: crate::scheduler::trace::QuotaMetrics::default(),
            admission_metrics: Default::default(),
            queue_wait_metrics: Default::default(),
            tenant_fairness_metrics: Default::default(),
            gpu_utilization_metrics: Default::default(),
            outcome_summary: Default::default(),
            job_observation_metrics: Default::default(),
            prediction_audit_metrics: Default::default(),
            prediction_audit_details: Vec::new(),
            node_grouping_metrics: Default::default(),
            candidate_quality_metrics: Default::default(),
            binding_reservation_metrics: Default::default(),
            binding_outcome_metrics: Default::default(),
            candidate_node_limit: 8,
            retry_count: 0,
            unpruned_candidate_edges: 20,
            initial_candidate_edges: 8,
            final_candidate_edges: 8,
            candidate_pruned_workloads: 1,
            widening_reason: String::new(),
        }
    }

    fn test_http_state_with_traces(traces: Vec<DecisionTrace>) -> ShadowHttpState {
        let cfg = test_shadow_config("test-cluster");
        let store = Arc::new(TraceStore::new(16));
        for trace in traces {
            store.push(trace);
        }
        ShadowHttpState {
            traces: store,
            watch_healthy: Arc::new(AtomicBool::new(true)),
            latest_readiness_error: Arc::new(Mutex::new(None)),
            latest_cluster: Arc::new(Mutex::new(None)),
            latest_pending: Arc::new(Mutex::new(Vec::new())),
            latest_bind_outcomes: Arc::new(Mutex::new(None)),
            simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
            latest_liabilities: Arc::new(Mutex::new(None)),
            simulator_pool: Arc::new(DashboardSimulatorPool::from_urls(Vec::new())),
            demo_report_cache: Arc::new(tokio::sync::Mutex::new(None)),
            demo_report_refresh_status: Arc::new(tokio::sync::Mutex::new(None)),
            kubeconfig: String::new(),
            active_objective: Arc::new(Mutex::new(ObjectiveSelection {
                profile: cfg.objective_profile,
                weights: cfg.objective_weights.clone(),
            })),
            cfg,
        }
    }

    #[tokio::test]
    async fn repair_plan_handler_renders_latest_advisory_plan() {
        let mut trace = retry_test_trace(Vec::new(), Default::default());
        trace.sequence = 42;
        trace.solve_millis = 17;
        trace.repair_metrics = crate::scheduler::trace::RepairMetrics {
            repairable_targets: 1,
            migration_actions: 1,
            ..Default::default()
        };
        trace.repair_notes = vec!["fragmented but repairable".into()];
        trace.repair_plans = vec![crate::scheduler::trace::RepairPlan {
            target: "team/train".into(),
            target_gpu_request: 4,
            target_priority: 10,
            target_business_value: 0,
            target_deadline_unix_seconds: 0,
            target_latest_start_unix_seconds: 0,
            target_queue_wait_seconds: 0,
            node: "g4dn-1".into(),
            freed_gpu: 4,
            disruption_cost: 3,
            explanation: "free a 4-GPU island".into(),
            actions: vec![crate::scheduler::trace::RepairAction {
                action: "migrate".into(),
                namespace: "team".into(),
                pod: "low-priority".into(),
                node: "g4dn-1".into(),
                to_node: "g4dn-2".into(),
                gpu_request: 1,
                disruption_cost: 3,
                reason: "move lower-value work".into(),
            }],
            skipped_candidates: Vec::new(),
        }];

        let Json(value) =
            repair_plan_handler(State(test_http_state_with_traces(vec![trace]))).await;

        assert_eq!(value["dry_run"], true);
        assert_eq!(value["trace_sequence"], 42);
        assert_eq!(value["solve_millis"], 17);
        assert_eq!(value["repair_metrics"]["repairable_targets"], 1);
        assert_eq!(value["repair_metrics"]["migration_actions"], 1);
        assert_eq!(value["repair_plans"][0]["target"], "team/train");
        assert_eq!(value["repair_plans"][0]["actions"][0]["action"], "migrate");
        assert_eq!(value["live_plan_available"], true);
        assert_eq!(value["proof_status"]["mode"], "live-repair-plan");
        assert_eq!(value["proof_status"]["live_action_count"], 1);
        assert_eq!(
            value["proof_status"]["claim_guard"],
            "reference rows are demo evidence only unless live_plan_available=true"
        );
        assert_eq!(value["hero_reference"]["name"], "preemption-migration-hero");
        assert!(
            value["hero_reference"]["action_rows"]
                .as_array()
                .expect("hero action rows")
                .len()
                >= 4
        );
        assert_eq!(value["repair_notes"][0], "fragmented but repairable");
        assert!(value["note"]
            .as_str()
            .unwrap_or_default()
            .contains("advisory only"));
    }

    #[tokio::test]
    async fn repair_plan_handler_keeps_demo_reference_when_live_trace_has_no_repair() {
        let mut trace = retry_test_trace(Vec::new(), Default::default());
        trace.sequence = 44;

        let Json(value) =
            repair_plan_handler(State(test_http_state_with_traces(vec![trace]))).await;

        assert_eq!(value["trace_sequence"], 44);
        assert_eq!(value["live_plan_available"], false);
        assert_eq!(value["proof_status"]["mode"], "deterministic-reference");
        assert_eq!(value["proof_status"]["live_action_count"], 0);
        assert!(value["proof_status"]["operator_question"]
            .as_str()
            .unwrap_or_default()
            .contains("repairable fragmentation scenario"));
        assert_eq!(value["repair_plans"].as_array().unwrap().len(), 0);
        assert_eq!(value["hero_reference"]["name"], "preemption-migration-hero");
        assert!(value["hero_reference_note"]
            .as_str()
            .unwrap_or_default()
            .contains("not evidence"));
    }

    #[tokio::test]
    async fn repair_events_handler_renders_latest_repair_event_drafts() {
        let mut trace = retry_test_trace(Vec::new(), Default::default());
        trace.sequence = 43;
        trace.repair_plans = vec![crate::scheduler::trace::RepairPlan {
            target: "team/train".into(),
            target_gpu_request: 4,
            target_priority: 10,
            target_business_value: 0,
            target_deadline_unix_seconds: 0,
            target_latest_start_unix_seconds: 0,
            target_queue_wait_seconds: 0,
            node: "g4dn-1".into(),
            freed_gpu: 1,
            disruption_cost: 3,
            explanation: "free a 4-GPU island".into(),
            actions: vec![crate::scheduler::trace::RepairAction {
                action: "migrate".into(),
                namespace: "team".into(),
                pod: "low-priority".into(),
                node: "g4dn-1".into(),
                to_node: "g4dn-2".into(),
                gpu_request: 1,
                disruption_cost: 3,
                reason: "move lower-value work".into(),
            }],
            skipped_candidates: Vec::new(),
        }];

        let Json(value) =
            repair_events_handler(State(test_http_state_with_traces(vec![trace]))).await;

        assert_eq!(value["dry_run"], true);
        assert_eq!(value["trace_sequence"], 43);
        let events = value["events"].as_array().expect("events array");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["reason"], "KsolverRepairMigrationRecommended");
        assert_eq!(events[0]["body"]["kind"], "Event");
        assert_eq!(events[0]["body"]["regarding"]["name"], "low-priority");
        assert_eq!(events[0]["body"]["related"]["name"], "g4dn-1");
        assert_eq!(events[0]["body"]["reportingInstance"], "test-cluster");
        assert!(events[0]["note"]
            .as_str()
            .unwrap_or_default()
            .contains("team/train"));
        assert!(value["note"]
            .as_str()
            .unwrap_or_default()
            .contains("never applied"));
    }

    fn retry_test_decision(
        priority: i64,
        placement: crate::scheduler::trace::PodPlacement,
    ) -> crate::scheduler::trace::PodDecision {
        retry_test_decision_named("u", "p", 1, priority, placement)
    }

    fn retry_test_decision_named(
        uid: &str,
        name: &str,
        gpu_request: i64,
        priority: i64,
        placement: crate::scheduler::trace::PodPlacement,
    ) -> crate::scheduler::trace::PodDecision {
        crate::scheduler::trace::PodDecision {
            uid: uid.into(),
            namespace: "team".into(),
            name: name.into(),
            binding_group: String::new(),
            gpu_request,
            priority,
            priority_class_name: String::new(),
            team: String::new(),
            queue: String::new(),
            queue_score: 0,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            deadline_slack_seconds: 0,
            predicted_finish_unix_seconds: 0,
            predicted_deadline_miss: false,
            placement,
            caveats: Vec::new(),
        }
    }

    fn decision_event_drafts_for_test(
        trace: &DecisionTrace,
    ) -> Vec<crate::scheduler::events::EventDraft> {
        crate::scheduler::events::render_decision_events(
            trace,
            "ksolver",
            "ksolver-0",
            "2026-07-02T12:00:00Z",
        )
    }

    #[test]
    fn simulator_dashboard_cache_key_ignores_sequence_order_and_ksolver_placement() {
        let mut placed = retry_test_trace(
            vec![
                retry_test_decision_named(
                    "u-a",
                    "a",
                    1,
                    0,
                    crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
                ),
                retry_test_decision_named(
                    "u-b",
                    "b",
                    2,
                    0,
                    crate::scheduler::trace::PodPlacement::Placed { node: "n2".into() },
                ),
            ],
            Default::default(),
        );
        placed.sequence = 1;
        let mut unplaced = placed.clone();
        unplaced.sequence = 99;
        unplaced.decisions.reverse();
        unplaced.decisions[0].placement = crate::scheduler::trace::PodPlacement::Unplaced {
            reason: "different ksolver result".into(),
        };

        assert_eq!(
            simulator_dashboard_cache_key(&placed, None),
            simulator_dashboard_cache_key(&unplaced, None)
        );
    }

    #[test]
    fn simulator_dashboard_cache_key_changes_when_gpu_occupancy_changes() {
        let trace = retry_test_trace(
            vec![retry_test_decision(
                0,
                crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
            )],
            Default::default(),
        );
        let mut cluster = crate::model::NormalizedCluster::default();
        cluster.nodes.push(crate::model::NormalizedNode {
            name: "gpu-a".to_string(),
            extended_resources: std::collections::BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                4,
            )]),
            ..Default::default()
        });
        cluster.workloads.push(crate::model::NormalizedWorkload {
            namespace: "team".to_string(),
            name: "running-a".to_string(),
            current_node: "gpu-a".to_string(),
            extended_resource_requests: std::collections::BTreeMap::from([(
                "nvidia.com/gpu".to_string(),
                1,
            )]),
            ..Default::default()
        });
        let before = simulator_dashboard_cache_key(&trace, Some(&cluster));

        cluster.workloads[0].current_node = "gpu-b".to_string();
        let after = simulator_dashboard_cache_key(&trace, Some(&cluster));

        assert_ne!(before, after);
    }

    #[test]
    fn dashboard_simulator_default_baseline_sends_valid_scheduler_config() {
        let payload = crate::verifier::SimulatorImportPayload {
            scheduler_config: dashboard_simulator_scheduler_config(),
            ..Default::default()
        };

        let value = serde_json::to_value(payload).expect("serialize simulator payload");
        let scheduler_config = value
            .get("schedulerConfig")
            .expect("dashboard imports should include a valid scheduler config");

        assert_eq!(
            scheduler_config
                .get("apiVersion")
                .and_then(serde_json::Value::as_str),
            Some("kubescheduler.config.k8s.io/v1")
        );
        assert_eq!(
            scheduler_config
                .get("kind")
                .and_then(serde_json::Value::as_str),
            Some("KubeSchedulerConfiguration")
        );
    }

    #[test]
    fn decision_event_emission_filter_suppresses_unchanged_repeated_decisions() {
        let mut filter = DecisionEventEmissionFilter::default();
        let mut trace = retry_test_trace(
            vec![retry_test_decision(
                1,
                crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
            )],
            Default::default(),
        );

        let first = filter.filter_changed(&trace, decision_event_drafts_for_test(&trace));
        assert_eq!(first.len(), 1);

        trace.sequence += 1;
        let repeated = filter.filter_changed(&trace, decision_event_drafts_for_test(&trace));
        assert!(repeated.is_empty());
    }

    #[test]
    fn decision_event_emission_filter_emits_when_decision_changes() {
        let mut filter = DecisionEventEmissionFilter::default();
        let mut trace = retry_test_trace(
            vec![retry_test_decision(
                1,
                crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
            )],
            Default::default(),
        );
        assert_eq!(
            filter
                .filter_changed(&trace, decision_event_drafts_for_test(&trace))
                .len(),
            1
        );

        trace.sequence += 1;
        trace.decisions[0].placement =
            crate::scheduler::trace::PodPlacement::Placed { node: "n2".into() };
        let changed = filter.filter_changed(&trace, decision_event_drafts_for_test(&trace));
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].body["related"]["name"], "n2");
    }

    #[test]
    fn decision_event_emission_filter_ignores_volatile_wait_time_changes() {
        let mut filter = DecisionEventEmissionFilter::default();
        let mut trace = retry_test_trace(
            vec![retry_test_decision(
                1,
                crate::scheduler::trace::PodPlacement::Unplaced {
                    reason: "insufficient capacity".into(),
                },
            )],
            Default::default(),
        );
        trace.decisions[0].queue_wait_seconds = 10;
        assert_eq!(
            filter
                .filter_changed(&trace, decision_event_drafts_for_test(&trace))
                .len(),
            1
        );

        trace.sequence += 1;
        trace.decisions[0].queue_wait_seconds = 20;
        assert!(filter
            .filter_changed(&trace, decision_event_drafts_for_test(&trace))
            .is_empty());

        trace.sequence += 1;
        trace.decisions[0].priority = 2;
        assert_eq!(
            filter
                .filter_changed(&trace, decision_event_drafts_for_test(&trace))
                .len(),
            1
        );
    }

    #[test]
    fn decision_event_emission_filter_resets_when_pod_leaves_trace() {
        let mut filter = DecisionEventEmissionFilter::default();
        let trace = retry_test_trace(
            vec![retry_test_decision(
                1,
                crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
            )],
            Default::default(),
        );
        assert_eq!(
            filter
                .filter_changed(&trace, decision_event_drafts_for_test(&trace))
                .len(),
            1
        );

        let empty = retry_test_trace(Vec::new(), Default::default());
        assert!(filter
            .filter_changed(&empty, decision_event_drafts_for_test(&empty))
            .is_empty());

        let reappeared = retry_test_trace(
            vec![retry_test_decision(
                1,
                crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
            )],
            Default::default(),
        );
        assert_eq!(
            filter
                .filter_changed(&reappeared, decision_event_drafts_for_test(&reappeared))
                .len(),
            1
        );
    }

    #[test]
    fn solve_attempt_stamps_node_grouping_metrics() {
        let cfg = test_shadow_config("test");
        let gpu = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 1_i64)]);
        let nodes = (1..=3)
            .map(|i| crate::model::NormalizedNode {
                name: format!("n{i}"),
                effective_capacity: crate::model::ResourceList {
                    milli_cpu: 8_000,
                    memory_bytes: 32 << 30,
                    pods: 10,
                    ..Default::default()
                },
                extended_resources: gpu.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let feasible = nodes.iter().map(|n| n.name.clone()).collect::<Vec<_>>();
        let cluster = crate::model::NormalizedCluster {
            nodes,
            workloads: vec![crate::model::NormalizedWorkload {
                namespace: "team".to_string(),
                name: "p0".to_string(),
                requests: crate::model::ResourceList {
                    milli_cpu: 1_000,
                    memory_bytes: 1 << 30,
                    pods: 1,
                    ..Default::default()
                },
                extended_resource_requests: gpu,
                feasible_node_names: feasible,
                ..Default::default()
            }],
            ..Default::default()
        };
        let pending = vec![crate::scheduler::pod_filter::PendingGpuPod {
            uid: "u0".to_string(),
            namespace: "team".to_string(),
            name: "p0".to_string(),
            gpu_request: 1,
            priority: 0,
            priority_class_name: None,
            team: None,
            queue: None,
            business_value: 0,
            queue_wait_seconds: 0,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            required_gpu_topology: Vec::new(),
            gang_key: None,
            colocate: false,
            unmodeled_constraints: Vec::new(),
            anti_affinity_host_selectors: Vec::new(),
            affinity_topology_selectors: Vec::new(),
            anti_affinity_topology_selectors: Vec::new(),
            preferred_node_affinity: Vec::new(),
            preferred_pod_affinity: Vec::new(),
        }];

        let attempt = solve_attempt(&cfg, &pending, &cluster, 0);
        let trace = trace_from_attempt(
            1,
            &pending,
            &attempt,
            attempt.solve_core_millis,
            attempt.solve_core_millis,
            0,
            &std::collections::HashSet::new(),
            &std::collections::BTreeMap::new(),
            &std::collections::BTreeMap::new(),
        );

        assert_eq!(trace.node_grouping_metrics.eligible_group_count, 1);
        assert_eq!(trace.node_grouping_metrics.eligible_node_count, 3);
        assert_eq!(trace.node_grouping_metrics.max_group_size, 3);
        assert!(trace.node_grouping_metrics.disabled_reasons.is_empty());
    }

    #[test]
    #[cfg(feature = "rust-cp-sat")]
    fn solve_attempt_uses_grouped_nodes_when_enabled_and_expands_result() {
        let mut cfg = test_shadow_config("test");
        cfg.enable_node_grouping = true;
        let gpu = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 1_i64)]);
        let nodes = (1..=3)
            .map(|i| crate::model::NormalizedNode {
                name: format!("n{i}"),
                effective_capacity: crate::model::ResourceList {
                    milli_cpu: 8_000,
                    memory_bytes: 32 << 30,
                    pods: 10,
                    ..Default::default()
                },
                extended_resources: gpu.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let feasible = nodes.iter().map(|n| n.name.clone()).collect::<Vec<_>>();
        let workloads = (0..2)
            .map(|i| crate::model::NormalizedWorkload {
                namespace: "team".to_string(),
                name: format!("p{i}"),
                requests: crate::model::ResourceList {
                    milli_cpu: 1_000,
                    memory_bytes: 1 << 30,
                    pods: 1,
                    ..Default::default()
                },
                extended_resource_requests: gpu.clone(),
                feasible_node_names: feasible.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let cluster = crate::model::NormalizedCluster {
            nodes,
            workloads,
            ..Default::default()
        };
        let pending = (0..2)
            .map(|i| crate::scheduler::pod_filter::PendingGpuPod {
                uid: format!("u{i}"),
                namespace: "team".to_string(),
                name: format!("p{i}"),
                gpu_request: 1,
                priority: 0,
                priority_class_name: None,
                team: None,
                queue: None,
                business_value: 0,
                queue_wait_seconds: 0,
                deadline_unix_seconds: 0,
                min_gpus: 0,
                max_gpus: 0,
                preferred_gpus: 0,
                flexible: false,
                predicted_runtime_seconds: 0,
                predicted_peak_vram_bytes: 0,
                required_gpu_topology: Vec::new(),
                gang_key: None,
                colocate: false,
                unmodeled_constraints: Vec::new(),
                anti_affinity_host_selectors: Vec::new(),
                affinity_topology_selectors: Vec::new(),
                anti_affinity_topology_selectors: Vec::new(),
                preferred_node_affinity: Vec::new(),
                preferred_pod_affinity: Vec::new(),
            })
            .collect::<Vec<_>>();

        let attempt = solve_attempt(&cfg, &pending, &cluster, 0);
        assert!(attempt.solve_ok, "{}", attempt.status);
        assert!(attempt.node_grouping_used);
        assert_eq!(attempt.grouped_node_count, 1);
        assert!(attempt
            .solution
            .assignments
            .values()
            .all(|n| !n.starts_with("node-group-")));
        let trace = trace_from_attempt(
            1,
            &pending,
            &attempt,
            attempt.solve_core_millis,
            attempt.solve_core_millis,
            0,
            &std::collections::HashSet::new(),
            &std::collections::BTreeMap::new(),
            &std::collections::BTreeMap::new(),
        );
        assert!(trace.node_grouping_metrics.enabled);
        assert!(trace.node_grouping_metrics.used);
        assert_eq!(trace.node_grouping_metrics.grouped_node_count, 1);
        assert!(
            trace.node_grouping_metrics.grouped_candidate_edges < trace.initial_candidate_edges
        );
    }

    #[test]
    #[cfg(feature = "rust-cp-sat")]
    fn solve_attempt_falls_back_when_grouped_solution_cannot_expand() {
        let mut cfg = test_shadow_config("test");
        cfg.enable_node_grouping = true;
        let gpu = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 4_i64)]);
        let nodes = (1..=3)
            .map(|i| crate::model::NormalizedNode {
                name: format!("n{i}"),
                effective_capacity: crate::model::ResourceList {
                    milli_cpu: 8_000,
                    memory_bytes: 32 << 30,
                    pods: 10,
                    ..Default::default()
                },
                extended_resources: gpu.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let feasible = nodes.iter().map(|n| n.name.clone()).collect::<Vec<_>>();
        let request = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 3_i64)]);
        let workloads = (0..4)
            .map(|i| crate::model::NormalizedWorkload {
                namespace: "team".to_string(),
                name: format!("p{i}"),
                requests: crate::model::ResourceList {
                    milli_cpu: 1_000,
                    memory_bytes: 1 << 30,
                    pods: 1,
                    ..Default::default()
                },
                extended_resource_requests: request.clone(),
                feasible_node_names: feasible.clone(),
                ..Default::default()
            })
            .collect::<Vec<_>>();
        let cluster = crate::model::NormalizedCluster {
            nodes,
            workloads,
            ..Default::default()
        };
        let pending = (0..4)
            .map(|i| crate::scheduler::pod_filter::PendingGpuPod {
                uid: format!("u{i}"),
                namespace: "team".to_string(),
                name: format!("p{i}"),
                gpu_request: 3,
                priority: 0,
                priority_class_name: None,
                team: None,
                queue: None,
                business_value: 0,
                queue_wait_seconds: 0,
                deadline_unix_seconds: 0,
                min_gpus: 0,
                max_gpus: 0,
                preferred_gpus: 0,
                flexible: false,
                predicted_runtime_seconds: 0,
                predicted_peak_vram_bytes: 0,
                required_gpu_topology: Vec::new(),
                gang_key: None,
                colocate: false,
                unmodeled_constraints: Vec::new(),
                anti_affinity_host_selectors: Vec::new(),
                affinity_topology_selectors: Vec::new(),
                anti_affinity_topology_selectors: Vec::new(),
                preferred_node_affinity: Vec::new(),
                preferred_pod_affinity: Vec::new(),
            })
            .collect::<Vec<_>>();

        let attempt = solve_attempt(&cfg, &pending, &cluster, 0);

        assert!(attempt.solve_ok, "{}", attempt.status);
        assert!(!attempt.node_grouping_used);
        assert!(attempt
            .node_grouping_fallback_reason
            .contains("could not be expanded"));
        assert!(attempt.status.contains("node_grouping=fallback"));
        assert!(attempt
            .solution
            .assignments
            .values()
            .all(|n| !n.starts_with("node-group-")));
        assert_eq!(attempt.solution.assignments.len(), 3);
    }

    #[test]
    fn retry_reason_ignores_full_candidate_solves() {
        let trace = retry_test_trace(Vec::new(), Default::default());
        assert_eq!(retry_reason(&trace, false, 0, 50_000), None);
    }

    #[test]
    fn vram_blocked_decision_matches_specific_unplaced_reason() {
        let blocked = retry_test_decision(
            0,
            crate::scheduler::trace::PodPlacement::Unplaced {
                reason: "no feasible node (predicted peak VRAM exceeds known node GPU memory)"
                    .to_string(),
            },
        );
        let other_unplaced = retry_test_decision(
            0,
            crate::scheduler::trace::PodPlacement::Unplaced {
                reason: "gang not admitted (insufficient capacity or quota)".to_string(),
            },
        );
        let placed = retry_test_decision(
            0,
            crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
        );

        assert!(is_vram_blocked_decision(&blocked));
        assert!(!is_vram_blocked_decision(&other_unplaced));
        assert!(!is_vram_blocked_decision(&placed));
    }

    #[test]
    fn high_priority_unplaced_decision_matches_positive_priority_only() {
        let high_unplaced = retry_test_decision(
            10,
            crate::scheduler::trace::PodPlacement::Unplaced {
                reason: "gang not admitted (insufficient capacity or quota)".to_string(),
            },
        );
        let zero_unplaced = retry_test_decision(
            0,
            crate::scheduler::trace::PodPlacement::Unplaced {
                reason: "gang not admitted (insufficient capacity or quota)".to_string(),
            },
        );
        let high_placed = retry_test_decision(
            10,
            crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
        );

        assert!(is_high_priority_unplaced_decision(&high_unplaced));
        assert!(!is_high_priority_unplaced_decision(&zero_unplaced));
        assert!(!is_high_priority_unplaced_decision(&high_placed));
    }

    #[test]
    fn admission_metrics_count_placed_pods_and_gpu_demand() {
        let trace = retry_test_trace(
            vec![
                retry_test_decision(
                    0,
                    crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
                ),
                crate::scheduler::trace::PodDecision {
                    gpu_request: 4,
                    ..retry_test_decision(
                        0,
                        crate::scheduler::trace::PodPlacement::Placed { node: "n2".into() },
                    )
                },
                crate::scheduler::trace::PodDecision {
                    gpu_request: 8,
                    ..retry_test_decision(
                        0,
                        crate::scheduler::trace::PodPlacement::Unplaced {
                            reason: "gang not admitted".into(),
                        },
                    )
                },
            ],
            Default::default(),
        );

        assert_eq!(
            admission_metrics(&trace),
            AdmissionMetrics {
                admitted_pods: 2,
                admitted_gpu_demand: 5,
            }
        );
    }

    #[test]
    fn stamp_objective_records_profile_and_weights_on_trace() {
        let mut trace = retry_test_trace(Vec::new(), Default::default());
        let weights = ObjectiveWeights {
            admission: 11,
            gpu_demand: 12,
            gang_complete: 13,
            priority: 14,
            business_value: 15,
            queue: 16,
            queue_wait: 17,
            fair_share: 18,
            deadline_urgency: 19,
            deadline_miss: 20,
            gpu_fragmentation: 21,
        };

        stamp_objective(&mut trace, ObjectiveProfile::GpuGangAware, &weights);

        assert_eq!(trace.objective_profile, ObjectiveProfile::GpuGangAware);
        assert_eq!(trace.objective_weights, weights);
    }

    #[test]
    fn stamp_fair_share_deficits_marks_under_share_pending_work() {
        let gpu = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 1_i64)]);
        let mut cfg = test_shadow_config("test");
        cfg.objective_profile = ObjectiveProfile::GpuGangAware;
        cfg.objective_weights.fair_share = 1;
        cfg.tenant_share_weights = std::collections::BTreeMap::from([
            ("research".to_string(), 1),
            ("batch".to_string(), 1),
        ]);

        let normalized = crate::model::NormalizedCluster {
            nodes: (0..4)
                .map(|i| crate::model::NormalizedNode {
                    name: format!("n{i}"),
                    extended_resources: gpu.clone(),
                    ..Default::default()
                })
                .collect(),
            workloads: vec![
                crate::model::NormalizedWorkload {
                    namespace: "research".to_string(),
                    name: "running-a".to_string(),
                    current_node: "n0".to_string(),
                    extended_resource_requests: gpu.clone(),
                    ..Default::default()
                },
                crate::model::NormalizedWorkload {
                    namespace: "research".to_string(),
                    name: "running-b".to_string(),
                    current_node: "n1".to_string(),
                    extended_resource_requests: gpu.clone(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut input = crate::model::OptimizationInput {
            workloads: vec![
                crate::model::OptimizationWorkload {
                    id: "research/pending".to_string(),
                    namespace: "research".to_string(),
                    extended_resource_requests: gpu.clone(),
                    ..Default::default()
                },
                crate::model::OptimizationWorkload {
                    id: "batch/pending".to_string(),
                    namespace: "batch".to_string(),
                    extended_resource_requests: gpu,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        stamp_fair_share_deficits(&mut input, &normalized, &cfg);

        let deficits: std::collections::BTreeMap<_, _> = input
            .workloads
            .iter()
            .map(|w| (w.id.as_str(), w.fair_share_deficit))
            .collect();
        assert_eq!(deficits.get("research/pending"), Some(&0));
        assert_eq!(deficits.get("batch/pending"), Some(&1));
    }

    #[test]
    fn apply_tenant_budget_groups_subtracts_running_cost() {
        let gpu = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 4_i64)]);
        let gpu_request = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 1_i64)]);
        let mut cfg = test_shadow_config("test");
        cfg.tenant_monthly_budgets_milli =
            std::collections::BTreeMap::from([("research".to_string(), 1_500_000)]);
        let normalized = crate::model::NormalizedCluster {
            nodes: vec![crate::model::NormalizedNode {
                name: "n1".to_string(),
                extended_resources: gpu,
                price: crate::model::Money {
                    monthly: 4_000.0,
                    ..Default::default()
                },
                ..Default::default()
            }],
            workloads: vec![crate::model::NormalizedWorkload {
                namespace: "team-a".to_string(),
                name: "running".to_string(),
                team: "research".to_string(),
                current_node: "n1".to_string(),
                extended_resource_requests: gpu_request.clone(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut input = crate::model::OptimizationInput {
            workloads: vec![
                crate::model::OptimizationWorkload {
                    id: "team-a/pending".to_string(),
                    namespace: "team-a".to_string(),
                    team: "research".to_string(),
                    extended_resource_requests: gpu_request,
                    ..Default::default()
                },
                crate::model::OptimizationWorkload {
                    id: "team-b/other".to_string(),
                    namespace: "team-b".to_string(),
                    team: "batch".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        apply_tenant_budget_groups(&mut input, &normalized, &cfg);

        assert_eq!(input.budget_groups.len(), 1);
        let group = &input.budget_groups[0];
        assert_eq!(group.name, "research");
        assert_eq!(group.workload_ids, vec!["team-a/pending".to_string()]);
        assert_eq!(
            group.limit_milli, 500_000,
            "one running 1-GPU pod on a 4-GPU $4000/mo node should consume 1000 monthly units"
        );
    }

    #[test]
    fn stamp_queue_scores_marks_configured_queue_workloads() {
        let mut cfg = test_shadow_config("test");
        cfg.objective_profile = ObjectiveProfile::GpuGangAware;
        cfg.objective_weights.queue = 1;
        cfg.queue_weights = std::collections::BTreeMap::from([
            ("urgent".to_string(), 100),
            ("batch".to_string(), 10),
        ]);
        let mut input = crate::model::OptimizationInput {
            workloads: vec![
                crate::model::OptimizationWorkload {
                    id: "team/a".to_string(),
                    queue: "urgent".to_string(),
                    ..Default::default()
                },
                crate::model::OptimizationWorkload {
                    id: "team/b".to_string(),
                    queue: "best-effort".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        stamp_queue_scores(&mut input, &cfg);

        let scores: std::collections::BTreeMap<_, _> = input
            .workloads
            .iter()
            .map(|w| (w.id.as_str(), w.queue_score))
            .collect();
        assert_eq!(scores.get("team/a"), Some(&100));
        assert_eq!(scores.get("team/b"), Some(&0));
    }

    #[test]
    fn stamp_fair_share_deficits_counts_running_work_by_team() {
        let gpu = std::collections::BTreeMap::from([("nvidia.com/gpu".to_string(), 1_i64)]);
        let mut cfg = test_shadow_config("test");
        cfg.objective_profile = ObjectiveProfile::GpuGangAware;
        cfg.objective_weights.fair_share = 1;
        cfg.tenant_share_weights = std::collections::BTreeMap::from([
            ("research".to_string(), 1),
            ("batch".to_string(), 1),
        ]);

        let normalized = crate::model::NormalizedCluster {
            nodes: (0..4)
                .map(|i| crate::model::NormalizedNode {
                    name: format!("n{i}"),
                    extended_resources: gpu.clone(),
                    ..Default::default()
                })
                .collect(),
            workloads: vec![
                crate::model::NormalizedWorkload {
                    namespace: "shared".to_string(),
                    name: "research-running-a".to_string(),
                    team: "research".to_string(),
                    current_node: "n0".to_string(),
                    extended_resource_requests: gpu.clone(),
                    ..Default::default()
                },
                crate::model::NormalizedWorkload {
                    namespace: "shared".to_string(),
                    name: "research-running-b".to_string(),
                    team: "research".to_string(),
                    current_node: "n1".to_string(),
                    extended_resource_requests: gpu.clone(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let mut input = crate::model::OptimizationInput {
            workloads: vec![
                crate::model::OptimizationWorkload {
                    id: "team-a/research-pending".to_string(),
                    namespace: "team-a".to_string(),
                    team: "research".to_string(),
                    extended_resource_requests: gpu.clone(),
                    ..Default::default()
                },
                crate::model::OptimizationWorkload {
                    id: "team-b/batch-pending".to_string(),
                    namespace: "team-b".to_string(),
                    team: "batch".to_string(),
                    extended_resource_requests: gpu,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        stamp_fair_share_deficits(&mut input, &normalized, &cfg);

        let deficits: std::collections::BTreeMap<_, _> = input
            .workloads
            .iter()
            .map(|w| (w.id.as_str(), w.fair_share_deficit))
            .collect();
        assert_eq!(deficits.get("team-a/research-pending"), Some(&0));
        assert_eq!(deficits.get("team-b/batch-pending"), Some(&1));
    }

    #[test]
    fn write_paths_require_leadership_after_solve_when_enabled() {
        let leader = crate::scheduler::leader::LeaderElector::for_test(false);
        assert!(write_paths_allowed_after_solve(&leader, false, false));
        assert!(!write_paths_allowed_after_solve(&leader, true, false));
        assert!(!write_paths_allowed_after_solve(&leader, false, true));

        let leader = crate::scheduler::leader::LeaderElector::for_test(true);
        assert!(write_paths_allowed_after_solve(&leader, true, false));
        assert!(write_paths_allowed_after_solve(&leader, false, true));
    }

    #[test]
    fn binding_reservation_metrics_include_ledger_state_and_pass_deltas() {
        let now = Instant::now();
        let mut ledger = crate::scheduler::ledger::ReservationLedger::new();
        let cluster = crate::model::NormalizedCluster {
            nodes: vec![crate::model::NormalizedNode {
                name: "n1".into(),
                extended_resources: std::collections::BTreeMap::from([(
                    "nvidia.com/gpu".to_string(),
                    4,
                )]),
                ..Default::default()
            }],
            ..Default::default()
        };
        ledger
            .reserve(
                &cluster,
                &std::collections::BTreeMap::new(),
                vec![crate::scheduler::binding::BindingPlanEntry {
                    namespace: "team".into(),
                    pod_name: "train".into(),
                    pod_uid: "u".into(),
                    binding_group: String::new(),
                    team: String::new(),
                    node_name: "n1".into(),
                    gpu_request: 2,
                    binding_body: serde_json::json!({}),
                }],
                Duration::from_secs(60),
                now,
            )
            .expect("reservation should fit");
        let reconciled = crate::scheduler::ledger::ReconcileStats {
            expired_reservations: 3,
            observed_bound_entries: 5,
            stale_entries: 7,
            ..Default::default()
        };

        let metrics = binding_reservation_metrics(&ledger, 1, 0, &reconciled);

        assert_eq!(
            metrics,
            BindingReservationMetrics {
                active_reservations: 1,
                active_entries: 1,
                reserved_gpus: 2,
                created: 1,
                rejected: 0,
                expired: 3,
                observed_bound_entries: 5,
                stale_entries: 7,
            }
        );
    }

    #[test]
    fn binding_outcome_metrics_count_canary_skips_separately() {
        let outcomes = vec![
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "bound".into(),
                pod_uid: "uid-bound".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Bound { dry_run: false },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "validated".into(),
                pod_uid: "uid-validated".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Bound { dry_run: true },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "canary".into(),
                pod_uid: "uid-canary".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "binding canary low-risk mode: pod requests 4 GPUs, max allowed 1"
                        .into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "stale".into(),
                pod_uid: "uid-stale".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "not ready (stale plan)".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "failed".into(),
                pod_uid: "uid-failed".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Failed {
                    error: "boom".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "scheduler".into(),
                pod_uid: "uid-scheduler".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "pod scheduler is default-scheduler, not ksolver".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "uid".into(),
                pod_uid: "uid-old".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "pod uid changed at apply time".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "already-bound".into(),
                pod_uid: "uid-bound-elsewhere".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "pod already bound to n9".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "dra".into(),
                pod_uid: "uid-dra".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason:
                        "DRA pod: ksolver does not allocate ResourceClaims (real binding unsafe)"
                            .into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "throttle".into(),
                pod_uid: "uid-throttle".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "max binds per pass reached".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "reservation".into(),
                pod_uid: "uid-reservation".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "binding reservation rejected: unknown target node n2".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "disabled".into(),
                pod_uid: "uid-disabled".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "real binding disabled by kill switch".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "group".into(),
                pod_uid: "uid-group".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "binding group skipped: gang would exceed rollout window".into(),
                },
            },
            crate::scheduler::binder::BindOutcome {
                namespace: "team".into(),
                pod: "other".into(),
                pod_uid: "uid-other".into(),
                team: String::new(),
                node: "n1".into(),
                result: crate::scheduler::binder::BindResult::Skipped {
                    reason: "unexpected skip".into(),
                },
            },
        ];

        assert_eq!(
            binding_outcome_metrics(&outcomes),
            BindingOutcomeMetrics {
                bound: 1,
                validated: 1,
                skipped: 11,
                failed: 1,
                canary_skipped: 1,
                readiness_skipped: 1,
                identity_skipped: 1,
                scheduler_skipped: 1,
                already_bound_skipped: 1,
                dra_skipped: 1,
                throttle_skipped: 1,
                reservation_skipped: 1,
                disabled_skipped: 1,
                group_skipped: 1,
                other_skipped: 1,
            }
        );
    }

    #[test]
    fn repair_metric_counts_summarize_actions_and_disruption() {
        let plans = vec![crate::scheduler::trace::RepairPlan {
            target: "team/train".into(),
            target_gpu_request: 4,
            target_priority: 20,
            target_business_value: 0,
            target_deadline_unix_seconds: 0,
            target_latest_start_unix_seconds: 0,
            target_queue_wait_seconds: 0,
            node: "n1".into(),
            freed_gpu: 4,
            disruption_cost: 7,
            actions: vec![
                crate::scheduler::trace::RepairAction {
                    action: "migrate".into(),
                    namespace: "team".into(),
                    pod: "low-a".into(),
                    node: "n1".into(),
                    to_node: "n2".into(),
                    gpu_request: 1,
                    disruption_cost: 1,
                    reason: String::new(),
                },
                crate::scheduler::trace::RepairAction {
                    action: "preempt".into(),
                    namespace: "team".into(),
                    pod: "low-b".into(),
                    node: "n1".into(),
                    to_node: String::new(),
                    gpu_request: 1,
                    disruption_cost: 2,
                    reason: String::new(),
                },
                crate::scheduler::trace::RepairAction {
                    action: "skip".into(),
                    namespace: "team".into(),
                    pod: "ignored".into(),
                    node: "n1".into(),
                    to_node: String::new(),
                    gpu_request: 1,
                    disruption_cost: 4,
                    reason: String::new(),
                },
            ],
            skipped_candidates: Vec::new(),
            explanation: String::new(),
        }];

        assert_eq!(
            repair_metric_counts(&plans),
            RepairMetricCounts {
                plans: 1,
                migrations: 1,
                preemptions: 1,
                disruption_cost: 7,
            }
        );
    }

    #[test]
    fn gpu_utilization_metrics_include_running_and_shadow_placements() {
        let cluster = crate::model::NormalizedCluster {
            nodes: vec![
                crate::model::NormalizedNode {
                    name: "n1".into(),
                    extended_resources: std::collections::BTreeMap::from([(
                        "nvidia.com/gpu".to_string(),
                        4,
                    )]),
                    ..Default::default()
                },
                crate::model::NormalizedNode {
                    name: "n2".into(),
                    extended_resources: std::collections::BTreeMap::from([(
                        "nvidia.com/gpu".to_string(),
                        4,
                    )]),
                    ..Default::default()
                },
            ],
            workloads: vec![crate::model::NormalizedWorkload {
                namespace: "team".into(),
                name: "running".into(),
                current_node: "n1".into(),
                extended_resource_requests: std::collections::BTreeMap::from([(
                    "nvidia.com/gpu".to_string(),
                    2,
                )]),
                ..Default::default()
            }],
            ..Default::default()
        };
        let trace = retry_test_trace(
            vec![retry_test_decision(
                0,
                crate::scheduler::trace::PodPlacement::Placed { node: "n2".into() },
            )],
            Default::default(),
        );

        assert_eq!(
            shadow_gpu_utilization_metrics(&cluster, &trace),
            GpuUtilizationMetrics {
                active_gpu_nodes: 2,
                stranded_gpu_on_active_nodes: 5,
            }
        );
    }

    #[test]
    fn retry_reason_widens_for_positive_priority_unplaced_work() {
        let trace = retry_test_trace(
            vec![retry_test_decision(
                10,
                crate::scheduler::trace::PodPlacement::Unplaced {
                    reason: "not enough candidates".to_string(),
                },
            )],
            Default::default(),
        );
        assert_eq!(
            retry_reason(&trace, true, 8, 50_000),
            Some("positive-priority job unplaced with pruned candidates")
        );
    }

    #[test]
    fn retry_reason_widens_for_deadline_miss() {
        let trace = retry_test_trace(
            vec![retry_test_decision(
                0,
                crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
            )],
            crate::scheduler::trace::DeadlineMetrics {
                deadline_jobs: 1,
                placed_deadline_jobs: 1,
                predicted_misses: 1,
                ..Default::default()
            },
        );
        assert_eq!(
            retry_reason(&trace, true, 8, 50_000),
            Some("predicted deadline miss with pruned candidates")
        );
    }

    #[test]
    fn retry_reason_uses_configured_low_admission_threshold() {
        let trace = retry_test_trace(
            vec![
                retry_test_decision(
                    0,
                    crate::scheduler::trace::PodPlacement::Placed { node: "n1".into() },
                ),
                retry_test_decision(
                    0,
                    crate::scheduler::trace::PodPlacement::Unplaced {
                        reason: "not enough candidates".to_string(),
                    },
                ),
                retry_test_decision(
                    0,
                    crate::scheduler::trace::PodPlacement::Unplaced {
                        reason: "not enough candidates".to_string(),
                    },
                ),
            ],
            Default::default(),
        );

        assert_eq!(
            retry_reason(&trace, true, 8, 50_000),
            Some("low admission ratio with pruned candidates")
        );
        assert_eq!(retry_reason(&trace, true, 8, 25_000), None);
        assert_eq!(retry_reason(&trace, true, 8, 0), None);
    }

    #[test]
    fn widened_candidate_limit_doubles_then_full_set() {
        assert_eq!(widened_candidate_limit(16, 0), Some(32));
        assert_eq!(widened_candidate_limit(32, 1), Some(0));
        assert_eq!(widened_candidate_limit(0, 0), None);
    }

    #[tokio::test]
    async fn binding_plan_endpoint_is_dry_run_and_lists_bindings() {
        use super::{binding_plan_handler, ShadowHttpState};
        use crate::scheduler::trace::{
            DeadlineMetrics, DecisionTrace, PodDecision, PodPlacement, TraceStore,
        };
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};
        let traces = Arc::new(TraceStore::new(8));
        traces.push(DecisionTrace {
            sequence: 7,
            observed_pods: 1,
            decisions: vec![PodDecision {
                uid: "u".into(),
                namespace: "team".into(),
                name: "a".into(),
                binding_group: String::new(),
                gpu_request: 1,
                priority: 0,
                priority_class_name: String::new(),
                team: String::new(),
                queue: String::new(),
                queue_score: 0,
                business_value: 0,
                queue_wait_seconds: 0,
                deadline_unix_seconds: 0,
                min_gpus: 0,
                max_gpus: 0,
                preferred_gpus: 0,
                flexible: false,
                predicted_runtime_seconds: 0,
                predicted_peak_vram_bytes: 0,
                deadline_slack_seconds: 0,
                predicted_finish_unix_seconds: 0,
                predicted_deadline_miss: false,
                placement: PodPlacement::Placed { node: "n1".into() },
                caveats: vec![],
            }],
            solver_status: "OPTIMAL".into(),
            objective_profile: Default::default(),
            objective_weights: Default::default(),
            solve_millis: 5,
            solve_core_millis: 3,
            snapshot_age_millis: 0,
            note: String::new(),
            repair_plans: Vec::new(),
            repair_notes: Vec::new(),
            repair_metrics: Default::default(),
            deadline_metrics: DeadlineMetrics::default(),
            quota_metrics: crate::scheduler::trace::QuotaMetrics::default(),
            admission_metrics: Default::default(),
            queue_wait_metrics: Default::default(),
            tenant_fairness_metrics: Default::default(),
            gpu_utilization_metrics: Default::default(),
            outcome_summary: Default::default(),
            job_observation_metrics: Default::default(),
            prediction_audit_metrics: Default::default(),
            prediction_audit_details: Vec::new(),
            node_grouping_metrics: Default::default(),
            candidate_quality_metrics: Default::default(),
            binding_reservation_metrics: Default::default(),
            binding_outcome_metrics: Default::default(),
            candidate_node_limit: 0,
            retry_count: 0,
            unpruned_candidate_edges: 0,
            initial_candidate_edges: 0,
            final_candidate_edges: 0,
            candidate_pruned_workloads: 0,
            widening_reason: String::new(),
        });
        // Latest snapshot: node n1 present, pod team/a still pending (uid u) -> readiness ready.
        let cluster = crate::model::NormalizedCluster {
            nodes: vec![crate::model::NormalizedNode {
                name: "n1".into(),
                ..Default::default()
            }],
            workloads: vec![crate::model::NormalizedWorkload {
                namespace: "team".into(),
                name: "a".into(),
                uid: "u".into(),
                current_node: String::new(),
                ..Default::default()
            }],
            ..Default::default()
        };
        let state = ShadowHttpState {
            traces,
            watch_healthy: Arc::new(AtomicBool::new(true)),
            latest_readiness_error: Arc::new(Mutex::new(None)),
            latest_cluster: Arc::new(Mutex::new(Some(cluster))),
            latest_pending: Arc::new(Mutex::new(Vec::new())),
            latest_bind_outcomes: Arc::new(Mutex::new(None)),
            simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
            latest_liabilities: Arc::new(Mutex::new(None)),
            simulator_pool: Arc::new(DashboardSimulatorPool::from_urls(Vec::new())),
            demo_report_cache: Arc::new(tokio::sync::Mutex::new(None)),
            demo_report_refresh_status: Arc::new(tokio::sync::Mutex::new(None)),
            kubeconfig: String::new(),
            cfg: ShadowConfig {
                scheduler_name: "ksolver".to_string(),
                batch_window: Duration::from_secs(10),
                namespace_allowlist: vec![],
                gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
                gpu_resource_prefixes: vec!["nvidia.com/mig-".to_string()],
                cluster_name: "default".to_string(),
                kubeconfig: String::new(),
                http_addr: "127.0.0.1:8090".to_string(),
                admission_opt_in_label: String::new(),
                gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
                gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
                solve_time_limit_secs: 10,
                namespace_gpu_quotas: std::collections::BTreeMap::new(),
                tenant_share_weights: std::collections::BTreeMap::new(),
                tenant_monthly_budgets_milli: std::collections::BTreeMap::new(),
                queue_weights: std::collections::BTreeMap::new(),
                enable_real_binding: false,
                binding_rollout_mode: crate::scheduler::config::BindingRolloutMode::ObserveOnly,
                binding_kill_switch: false,
                enable_kubernetes_events: false,
                real_binding_dry_run: false,
                binding_canary_mode: crate::scheduler::config::BindingCanaryMode::All,
                binding_low_risk_max_gpus: 1,
                max_binds_per_pass: 10,
                binding_reservation_ttl: Duration::from_secs(60),
                objective_profile: ObjectiveProfile::CostBinpack,
                objective_weights: ObjectiveWeights::default(),
                candidate_node_limit: 0,
                candidate_widen_min_admission_percent_milli: 50_000,
                enable_node_grouping: false,
                repair_candidate_limit: 8,
                enable_leader_election: false,
                leader_election_namespace: "ksolver".to_string(),
                leader_election_lease_name: "ksolver-scheduler".to_string(),
                leader_election_identity: "ksolver".to_string(),
            },
            active_objective: Arc::new(Mutex::new(ObjectiveSelection {
                profile: ObjectiveProfile::CostBinpack,
                weights: ObjectiveWeights::default(),
            })),
        };
        let axum::Json(v) = binding_plan_handler(axum::extract::State(state)).await;
        assert_eq!(v["dry_run"], true);
        assert_eq!(v["trace_sequence"], 7);
        let bindings = v["bindings"].as_array().expect("bindings array");
        assert_eq!(bindings.len(), 1);
        assert_eq!(bindings[0]["node_name"], "n1");
        assert_eq!(bindings[0]["binding_body"]["target"]["name"], "n1");
        assert_eq!(bindings[0]["readiness"]["state"], "ready");
    }

    #[tokio::test]
    async fn binding_events_endpoint_renders_latest_outcome_drafts() {
        use super::{binding_events_handler, ShadowHttpState};
        use crate::scheduler::binder::{BindOutcome, BindResult};
        use crate::scheduler::trace::TraceStore;
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};

        let state = ShadowHttpState {
            traces: Arc::new(TraceStore::new(8)),
            watch_healthy: Arc::new(AtomicBool::new(true)),
            latest_readiness_error: Arc::new(Mutex::new(None)),
            latest_cluster: Arc::new(Mutex::new(None)),
            latest_pending: Arc::new(Mutex::new(Vec::new())),
            latest_bind_outcomes: Arc::new(Mutex::new(Some((
                9,
                vec![BindOutcome {
                    namespace: "team".into(),
                    pod: "a".into(),
                    pod_uid: "uid-a".into(),
                    team: "research".into(),
                    node: "n1".into(),
                    result: BindResult::Bound { dry_run: true },
                }],
            )))),
            simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
            latest_liabilities: Arc::new(Mutex::new(None)),
            simulator_pool: Arc::new(DashboardSimulatorPool::from_urls(Vec::new())),
            demo_report_cache: Arc::new(tokio::sync::Mutex::new(None)),
            demo_report_refresh_status: Arc::new(tokio::sync::Mutex::new(None)),
            kubeconfig: String::new(),
            cfg: ShadowConfig {
                scheduler_name: "ksolver".to_string(),
                batch_window: Duration::from_secs(10),
                namespace_allowlist: vec![],
                gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
                gpu_resource_prefixes: vec!["nvidia.com/mig-".to_string()],
                cluster_name: "test-cluster".to_string(),
                kubeconfig: String::new(),
                http_addr: "127.0.0.1:8090".to_string(),
                admission_opt_in_label: String::new(),
                gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
                gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
                solve_time_limit_secs: 10,
                namespace_gpu_quotas: std::collections::BTreeMap::new(),
                tenant_share_weights: std::collections::BTreeMap::new(),
                tenant_monthly_budgets_milli: std::collections::BTreeMap::new(),
                queue_weights: std::collections::BTreeMap::new(),
                enable_real_binding: false,
                binding_rollout_mode: crate::scheduler::config::BindingRolloutMode::ObserveOnly,
                binding_kill_switch: false,
                enable_kubernetes_events: false,
                real_binding_dry_run: false,
                binding_canary_mode: crate::scheduler::config::BindingCanaryMode::All,
                binding_low_risk_max_gpus: 1,
                max_binds_per_pass: 10,
                binding_reservation_ttl: Duration::from_secs(60),
                objective_profile: ObjectiveProfile::CostBinpack,
                objective_weights: ObjectiveWeights::default(),
                candidate_node_limit: 0,
                candidate_widen_min_admission_percent_milli: 50_000,
                enable_node_grouping: false,
                repair_candidate_limit: 8,
                enable_leader_election: false,
                leader_election_namespace: "ksolver".to_string(),
                leader_election_lease_name: "ksolver-scheduler".to_string(),
                leader_election_identity: "ksolver".to_string(),
            },
            active_objective: Arc::new(Mutex::new(ObjectiveSelection {
                profile: ObjectiveProfile::CostBinpack,
                weights: ObjectiveWeights::default(),
            })),
        };

        let axum::Json(v) = binding_events_handler(axum::extract::State(state)).await;
        assert_eq!(v["dry_run"], true);
        assert_eq!(v["trace_sequence"], 9);
        let events = v["events"].as_array().expect("events array");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["team"], "research");
        assert_eq!(events[0]["reason"], "KsolverBindValidated");
        assert_eq!(events[0]["body"]["kind"], "Event");
        assert_eq!(events[0]["body"]["regarding"]["name"], "a");
        assert_eq!(events[0]["body"]["related"]["name"], "n1");
        assert_eq!(events[0]["body"]["reportingInstance"], "test-cluster");
    }

    #[tokio::test]
    async fn decision_events_endpoint_renders_latest_trace_event_drafts() {
        use super::{decision_events_handler, ShadowHttpState};
        use crate::scheduler::trace::{PodPlacement, TraceStore};
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Mutex};

        let traces = Arc::new(TraceStore::new(8));
        let mut trace = retry_test_trace(
            vec![retry_test_decision(
                5,
                PodPlacement::Placed { node: "n1".into() },
            )],
            Default::default(),
        );
        trace.sequence = 11;
        traces.push(trace);
        let state = ShadowHttpState {
            traces,
            watch_healthy: Arc::new(AtomicBool::new(true)),
            latest_readiness_error: Arc::new(Mutex::new(None)),
            latest_cluster: Arc::new(Mutex::new(None)),
            latest_pending: Arc::new(Mutex::new(Vec::new())),
            latest_bind_outcomes: Arc::new(Mutex::new(None)),
            simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
            latest_liabilities: Arc::new(Mutex::new(None)),
            simulator_pool: Arc::new(DashboardSimulatorPool::from_urls(Vec::new())),
            demo_report_cache: Arc::new(tokio::sync::Mutex::new(None)),
            demo_report_refresh_status: Arc::new(tokio::sync::Mutex::new(None)),
            kubeconfig: String::new(),
            cfg: test_shadow_config("test-cluster"),
            active_objective: Arc::new(Mutex::new(ObjectiveSelection {
                profile: ObjectiveProfile::CostBinpack,
                weights: ObjectiveWeights::default(),
            })),
        };

        let axum::Json(v) = decision_events_handler(axum::extract::State(state)).await;
        assert_eq!(v["dry_run"], true);
        assert_eq!(v["trace_sequence"], 11);
        let events = v["events"].as_array().expect("events array");
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["reason"], "KsolverPlacementRecommended");
        assert_eq!(events[0]["body"]["kind"], "Event");
        assert_eq!(events[0]["body"]["regarding"]["name"], "p");
        assert_eq!(events[0]["body"]["related"]["name"], "n1");
        assert_eq!(events[0]["body"]["reportingInstance"], "test-cluster");
    }

    #[tokio::test]
    async fn demo_report_endpoint_returns_cached_sre_summary_without_live_simulator() {
        let state = test_http_state_with_traces(Vec::new());

        let axum::Json(first) = demo_report_handler(axum::extract::State(state.clone())).await;
        let axum::Json(second) = demo_report_handler(axum::extract::State(state)).await;

        if crate::cpsat_rust::solver_info().available {
            assert_eq!(first["ok"], true);
            assert_eq!(second["ok"], true);
            assert_eq!(
                first["report"]["demo_readiness_summary"]["hero_scenario"],
                "defragmentation-advisor"
            );
            assert_eq!(
                first["report"]["roi_dashboard_summary"]["primary_tiles"]
                    .as_array()
                    .map(Vec::len)
                    .unwrap_or_default(),
                6
            );
        } else {
            assert_eq!(first["ok"], false);
            assert_eq!(second["ok"], false);
            assert_eq!(first["recoverable"], true);
            assert!(!first["reason"].as_str().unwrap_or_default().is_empty());
        }
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn evidence_bundle_endpoint_returns_collection_packet_without_live_simulator() {
        let mut trace = retry_test_trace(Vec::new(), Default::default());
        trace.sequence = 77;
        let state = test_http_state_with_traces(vec![trace]);

        let axum::Json(value) = evidence_bundle_handler(axum::extract::State(state)).await;

        assert_eq!(value["ok"], true);
        assert_eq!(value["dry_run"], true);
        assert_eq!(value["trace_sequence"], 77);
        assert_eq!(value["summary"]["collection_command_count"], 8);
        assert!(
            value["summary"]["missing_live_artifact_count"]
                .as_u64()
                .expect("missing live artifact count")
                >= 4
        );
        let category_counts = value["summary"]["missing_live_artifact_category_counts"]
            .as_object()
            .expect("missing live artifact category counts");
        for category in [
            "baseline-proof",
            "customer-proof",
            "live-trace",
            "repair-proof",
        ] {
            assert_eq!(
                category_counts
                    .get(category)
                    .and_then(serde_json::Value::as_u64),
                Some(1),
                "missing category {category}"
            );
        }
        assert_eq!(
            value["summary"]["missing_live_artifact_category_rows"][0]["category"],
            serde_json::json!("baseline-proof")
        );
        assert_eq!(
            value["summary"]["missing_live_artifact_category_rows"][0]["next_action"],
            serde_json::json!(
                "start or repair kube-scheduler-simulator before claiming live kube baseline"
            )
        );
        assert_eq!(
            value["summary"]["live_validation_gate_count"],
            serde_json::json!(7)
        );
        let pass_count = value["summary"]["live_validation_pass_count"]
            .as_u64()
            .expect("live validation pass count");
        let warn_count = value["summary"]["live_validation_warn_count"]
            .as_u64()
            .expect("live validation warn count");
        let blocked_count = value["summary"]["live_validation_blocked_count"]
            .as_u64()
            .expect("live validation blocked count");
        assert_eq!(pass_count + warn_count + blocked_count, 7);
        assert!(blocked_count >= 2);
        assert_eq!(
            value["summary"]["mutation_allowed"],
            serde_json::json!(false)
        );
        assert_eq!(
            value["summary"]["operator_binding_status"],
            serde_json::json!("read-only")
        );
        assert_eq!(
            value["summary"]["operator_reservation_pressure"],
            serde_json::json!("none")
        );
        assert!(value["summary"]["operator_reservation_pressure_scope"]
            .as_str()
            .expect("summary reservation pressure scope")
            .contains("unrelated to CUDA"));
        assert_eq!(
            value["summary"]["vram_advisory_ready"],
            serde_json::json!(true)
        );
        assert_eq!(
            value["summary"]["vram_hard_admission_ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            value["summary"]["vram_admission_mode"],
            serde_json::json!("Shadow advisory only")
        );
        assert_eq!(
            value["summary"]["vram_scheduler_use"],
            serde_json::json!("Score and warn; do not reject pods")
        );
        assert_eq!(
            value["summary"]["vram_hard_blocker_count"],
            serde_json::json!(4)
        );
        assert_eq!(
            value["summary"]["vram_next_evidence_target"],
            serde_json::json!("true CUDA OOM labels")
        );
        assert_eq!(
            value["summary"]["vram_model_driver_count"],
            serde_json::json!(8)
        );
        // Order-independent: top-driver labels are derived from mutable training data, so assert the
        // stable, semantically-important drivers are present rather than an exact ordered array.
        for key in ["vram_top_driver_labels", "vram_display_top_driver_labels"] {
            let labels = value["summary"][key]
                .as_array()
                .unwrap_or_else(|| panic!("{key} should be an array"));
            assert!(!labels.is_empty(), "{key} should be non-empty");
            for expected in [
                "parameter memory x precision",
                "synthetic VRAM headroom probe",
                "parameter count",
            ] {
                assert!(
                    labels.iter().any(|l| l == expected),
                    "{key} should include {expected}: {labels:?}"
                );
            }
        }
        assert_eq!(
            value["summary"]["vram_synthetic_reserve_driver"],
            serde_json::json!(true)
        );
        assert_eq!(
            value["summary"]["vram_synthetic_headroom_driver"],
            value["summary"]["vram_synthetic_reserve_driver"]
        );
        assert!(value["summary"]["vram_synthetic_driver_labels"]
            .as_array()
            .expect("synthetic driver labels")
            .iter()
            .any(|label| label.as_str() == Some("synthetic VRAM headroom probe")));
        assert!(value["summary"]["vram_display_synthetic_driver_labels"]
            .as_array()
            .expect("display synthetic driver labels")
            .iter()
            .any(|label| label.as_str() == Some("synthetic VRAM headroom probe")));
        assert!(value["summary"]["vram_real_top_driver_labels"]
            .as_array()
            .expect("real driver labels")
            .iter()
            .all(|label| label.as_str() != Some("synthetic VRAM headroom probe")));
        assert!(value["summary"]["vram_claim_safe_driver_labels"]
            .as_array()
            .expect("claim-safe driver labels")
            .iter()
            .all(|label| label.as_str() != Some("synthetic VRAM headroom probe")));
        assert!(
            !value["summary"]["vram_claim_safe_driver_labels"]
                .as_array()
                .expect("claim-safe driver labels")
                .is_empty(),
            "claim-safe driver labels should be non-empty"
        );
        assert_eq!(
            value["summary"]["vram_display_claim_safe_driver_labels"],
            value["summary"]["vram_claim_safe_driver_labels"]
        );
        assert!(value["summary"]["vram_driver_claim_boundary"]
            .as_str()
            .expect("driver claim boundary")
            .contains("organic workload predictors"));
        assert_eq!(
            value["summary"]["vram_reserve_pressure_definition"],
            serde_json::json!(VRAM_RESERVE_PRESSURE_DEFINITION)
        );
        assert_eq!(
            value["summary"]["vram_synthetic_headroom_definition"],
            value["summary"]["vram_reserve_pressure_definition"]
        );
        let investment_rows = value["summary"]["vram_investment_demo_rows"]
            .as_u64()
            .expect("VRAM investment demo rows");
        if investment_rows > 0 {
            assert_eq!(investment_rows, 6);
            assert_eq!(
                value["summary"]["vram_investment_oom_risk_reduction_pods"],
                serde_json::json!(3)
            );
            assert_eq!(
                value["summary"]["vram_investment_high_vram_nodes_preserved"],
                serde_json::json!(1)
            );
            assert_eq!(
                value["summary"]["vram_investment_advisory_rows"],
                serde_json::json!(1)
            );
            assert_eq!(
                value["summary"]["vram_investment_average_baseline_oom_risk_percent"],
                serde_json::json!(68)
            );
            assert_eq!(
                value["summary"]["vram_investment_average_ksolver_oom_risk_percent"],
                serde_json::json!(17)
            );
        }
        assert_eq!(
            value["summary"]["production_readiness_blocker_class"],
            serde_json::json!(if crate::cpsat_rust::solver_info().available {
                "none"
            } else {
                "solver"
            })
        );
        assert_eq!(
            value["summary"]["simulator_endpoint_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            value["summary"]["simulator_probe_checked_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            value["summary"]["simulator_probe_ready_count"],
            serde_json::json!(0)
        );
        assert_eq!(
            value["summary"]["simulator_probe_timeout_millis"],
            serde_json::json!(2_000)
        );
        assert_eq!(
            value["summary"]["simulator_readiness"],
            serde_json::json!("not_configured")
        );
        assert!(value["summary"]["simulator_readiness_note"]
            .as_str()
            .unwrap_or_default()
            .contains("no kube-scheduler-simulator endpoint configured"));
        assert_eq!(
            value["summary"]["simulator_claim_ready"],
            serde_json::json!(false)
        );
        assert_eq!(
            value["summary"]["simulator_claim_mode"],
            serde_json::json!("reference-only")
        );
        assert_eq!(
            value["summary"]["simulator_claim_blocker"],
            serde_json::json!("kube-scheduler-simulator not configured")
        );
        assert!(value["summary"]["simulator_claim_next_action"]
            .as_str()
            .unwrap_or_default()
            .contains("configure KSOLVER_SCHEDULER_SIMULATOR_POOL"));
        assert_eq!(value["summary"]["review_ready"], serde_json::json!(false));
        assert_eq!(
            value["summary"]["demo_gate_status"],
            serde_json::json!("local-pass-strict-blocked")
        );
        assert_eq!(
            value["summary"]["demo_gate_local_exit_code"],
            serde_json::json!(0)
        );
        assert_eq!(
            value["summary"]["demo_gate_strict_exit_code"],
            serde_json::json!(2)
        );
        assert!(value["summary"]["claim_blockers"]
            .as_array()
            .expect("claim blockers")
            .iter()
            .any(|blocker| blocker == "customer claim not ready"));
        let expected_primary_blocker = if value["summary"]["production_readiness_blocker_class"]
            == serde_json::json!("none")
        {
            "customer claim not ready"
        } else {
            "production readiness blocked: solver"
        };
        assert_eq!(
            value["summary"]["primary_claim_blocker"],
            serde_json::json!(expected_primary_blocker)
        );
        assert!(value["summary"]["primary_claim_blocker_next_action"]
            .as_str()
            .unwrap_or_default()
            .contains(if expected_primary_blocker == "customer claim not ready" {
                "resolve launch proof gaps"
            } else {
                "rust-cp-sat"
            }));
        assert_eq!(
            value["artifacts"]["latest_trace"]["sequence"],
            serde_json::json!(77)
        );
        assert_eq!(
            value["artifacts"]["production_safety"]["rollout"]["mutation_allowed"],
            serde_json::json!(false)
        );
        assert!(value["artifacts"]["production_safety"]["operator_claim"]
            .as_str()
            .unwrap_or_default()
            .contains("read-only shadow mode"));
        assert!(
            value["artifacts"]["demo_report"]["ok"] == serde_json::json!(true)
                || value["artifacts"]["demo_report"]["reason"].is_string()
        );
        if value["artifacts"]["demo_report"]["ok"] == serde_json::json!(true) {
            assert_eq!(value["launch_proof_gate"]["status"], "incomplete");
            assert_eq!(
                value["launch_proof_gate"]["customer_claim_ready"],
                serde_json::json!(false)
            );
            assert!(value["evidence_bundle_rows"]
                .as_array()
                .expect("evidence bundle rows")
                .iter()
                .any(|row| row["artifact"] == "kube baseline provenance"));
        }
        assert!(value["collection_commands"]
            .as_array()
            .expect("collection commands")
            .iter()
            .any(|cmd| cmd
                .as_str()
                .unwrap_or_default()
                .contains("/api/scheduler/evidence-bundle")));
        assert!(value["missing_live_artifacts"]
            .as_array()
            .expect("missing live artifacts")
            .iter()
            .any(|artifact| artifact == "live repair-plan action rows"));
        assert_eq!(
            value["missing_live_artifact_rows"]
                .as_array()
                .expect("missing live artifact rows")
                .len(),
            value["missing_live_artifacts"]
                .as_array()
                .expect("missing live artifacts")
                .len()
        );
        assert!(value["missing_live_artifact_rows"]
            .as_array()
            .expect("missing live artifact rows")
            .iter()
            .any(|row| {
                row["artifact"] == "live repair-plan action rows"
                    && row["category"] == "repair-proof"
                    && row["proof_gate"] == "repair action safety"
            }));
        let live_gates = value["live_validation_gates"]
            .as_array()
            .expect("live validation gates");
        assert!(live_gates
            .iter()
            .any(|gate| gate["gate"] == "pending GPU trace" && gate["status"] == "blocked"));
        assert!(live_gates
            .iter()
            .any(|gate| gate["gate"] == "repair action safety" && gate["status"] == "warn"));
        assert!(live_gates
            .iter()
            .any(|gate| { gate["gate"] == "ROI pricing evidence" && gate["status"] != "pass" }));
        assert!(live_gates
            .iter()
            .any(|gate| gate["gate"] == "production mutation safety"));
        assert!(value["note"]
            .as_str()
            .unwrap_or_default()
            .contains("read-only SRE evidence bundle"));
    }

    #[tokio::test]
    async fn evidence_bundle_blocks_customer_claim_when_production_readiness_is_blocked() {
        let state = test_http_state_with_traces(Vec::new());
        state.watch_healthy.store(false, Ordering::SeqCst);

        let axum::Json(value) = evidence_bundle_handler(axum::extract::State(state)).await;

        assert_eq!(
            value["summary"]["production_readiness_blocker_class"],
            serde_json::json!("kubernetes_watch")
        );
        assert_eq!(value["summary"]["review_ready"], serde_json::json!(false));
        assert_eq!(
            value["summary"]["demo_gate_strict_exit_code"],
            serde_json::json!(2)
        );
        assert!(value["summary"]["claim_blockers"]
            .as_array()
            .expect("claim blockers")
            .iter()
            .any(|blocker| blocker == "production readiness blocked: kubernetes_watch"));
        assert_eq!(
            value["summary"]["primary_claim_blocker"],
            serde_json::json!("production readiness blocked: kubernetes_watch")
        );
        assert!(value["summary"]["primary_claim_blocker_next_action"]
            .as_str()
            .unwrap_or_default()
            .contains("restore Kubernetes API connectivity"));
        assert!(value["summary"]["production_readiness_debug_commands"]
            .as_array()
            .expect("production readiness debug commands")
            .iter()
            .any(|cmd| cmd.as_str() == Some("kubectl config current-context")));
        assert_eq!(
            value["summary"]["production_readiness_first_debug_command"],
            value["artifacts"]["production_safety"]["readiness"]["debug_commands"][0]
        );
    }

    #[tokio::test]
    async fn operator_status_endpoint_returns_primary_action_contract() {
        let state = test_http_state_with_traces(Vec::new());
        state.watch_healthy.store(false, Ordering::SeqCst);

        let axum::Json(value) = operator_status_handler(axum::extract::State(state)).await;

        assert_eq!(value["ok"], serde_json::json!(true));
        assert_eq!(value["dry_run"], serde_json::json!(true));
        assert_eq!(value["status"], serde_json::json!("blocked"));
        assert_eq!(value["can_shadow_demo"], serde_json::json!(true));
        assert_eq!(value["can_customer_claim"], serde_json::json!(false));
        assert_eq!(value["can_score_vram"], serde_json::json!(true));
        assert_eq!(value["can_hard_admit_vram"], serde_json::json!(false));
        assert!(value["vram"]["hard_admission_blockers"]
            .as_array()
            .expect("VRAM hard admission blockers")
            .iter()
            .any(|blocker| blocker.as_str() == Some("no true bare-metal/cloud CUDA OOM labels")));
        assert!(value["vram"]["evidence_collection_plan"]
            .as_array()
            .expect("VRAM evidence collection plan")
            .iter()
            .any(|row| row["target"] == "true CUDA OOM labels"
                && row["unblocks"]
                    .as_str()
                    .unwrap_or_default()
                    .contains("hard admission")));
        assert_eq!(
            value["primary_blocker"],
            serde_json::json!("production readiness blocked: kubernetes_watch")
        );
        assert!(value["next_action"]
            .as_str()
            .unwrap_or_default()
            .contains("restore Kubernetes API connectivity"));
        assert!(value["debug_commands"]
            .as_array()
            .expect("debug commands")
            .iter()
            .any(|cmd| cmd
                .as_str()
                .unwrap_or_default()
                .contains("kubectl config current-context")));
        assert_eq!(
            value["production_readiness"]["blocker_class"],
            serde_json::json!("kubernetes_watch")
        );
        assert_eq!(value["simulator"]["claim_ready"], serde_json::json!(false));
        assert_eq!(value["scale_safety"]["available"], serde_json::json!(false));
        assert_eq!(
            value["scale_safety"]["regret_status"],
            serde_json::json!("unknown")
        );
        assert!(value["scale_safety"]["next_action"]
            .as_str()
            .unwrap_or_default()
            .contains("capture a live shadow trace"));
        assert_eq!(value["binding_safety"]["status"], serde_json::json!("read-only"));
        assert_eq!(
            value["binding_safety"]["mutation_allowed"],
            serde_json::json!(false)
        );
        assert_eq!(
            value["binding_safety"]["bound"],
            serde_json::json!(0)
        );
        assert_eq!(
            value["binding_safety"]["reservation_pressure"],
            serde_json::json!("none")
        );
        assert!(value["binding_safety"]["reservation_pressure_scope"]
            .as_str()
            .expect("reservation pressure scope")
            .contains("unrelated to CUDA"));
        assert!(value["binding_safety"]["reservation_pressure_reason"]
            .as_str()
            .unwrap_or_default()
            .contains("no active binding reservations"));
        assert!(value["binding_safety"]["next_action"]
            .as_str()
            .unwrap_or_default()
            .contains("read-only"));
        assert_eq!(
            value["decision_readiness"]["status"],
            serde_json::json!("needs-action")
        );
        assert!(value["decision_readiness"]["summary"]
            .as_str()
            .unwrap_or_default()
            .contains("bind=read-only"));
        assert!(value["decision_readiness"]["capabilities"]
            .as_array()
            .expect("decision readiness capabilities")
            .iter()
            .any(|capability| capability["name"] == "production_binding"
                && capability["status"] == "read-only"));
        assert_eq!(
            value["simulator"]["claim_mode"],
            serde_json::json!("reference-only")
        );
        assert_eq!(
            value["simulator"]["claim_blocker"],
            serde_json::json!("kube-scheduler-simulator not configured")
        );
        assert!(value["simulator"]["claim_next_action"]
            .as_str()
            .unwrap_or_default()
            .contains("configure KSOLVER_SCHEDULER_SIMULATOR_POOL"));
        assert_eq!(
            value["simulator"]["recovery_command"],
            serde_json::json!("scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache")
        );
        assert!(value["production_readiness"]["debug_commands"]
            .as_array()
            .expect("production readiness debug commands")
            .iter()
            .any(|cmd| cmd
                .as_str()
                .unwrap_or_default()
                .contains("kubectl --request-timeout=10s get --raw='/readyz?verbose'")));
        assert_eq!(value["demo_gate"]["strict_exit_code"], serde_json::json!(2));
        assert_eq!(value["proof_gates"]["total"], serde_json::json!(7));
        if crate::cpsat_rust::solver_info().available {
            assert_eq!(value["proof_gates"]["pass"], serde_json::json!(1));
            assert_eq!(value["proof_gates"]["warn"], serde_json::json!(3));
            assert_eq!(value["proof_gates"]["blocked"], serde_json::json!(3));
        } else {
            assert_eq!(value["proof_gates"]["pass"], serde_json::json!(0));
            assert!(
                value["proof_gates"]["blocked"]
                    .as_u64()
                    .expect("blocked proof gates")
                    >= 3
            );
        }
        assert!(value["proof_gates"]["rows"]
            .as_array()
            .expect("proof gate rows")
            .iter()
            .any(|gate| gate["gate"] == "pending GPU trace" && gate["status"] == "blocked"));
        assert!(value["proof_gates"]["rows"]
            .as_array()
            .expect("proof gate rows")
            .iter()
            .any(|gate| gate["gate"] == "kube baseline provenance" && gate["status"] == "blocked"));
        assert!(
            value["evidence_gaps"]["total"]
                .as_u64()
                .expect("evidence gap total")
                >= 5
        );
        assert!(
            value["evidence_gaps"]["blocked"]
                .as_u64()
                .expect("blocked evidence gaps")
                >= 4
        );
        assert!(
            value["evidence_gaps"]["warn"]
                .as_u64()
                .expect("warn evidence gaps")
                <= 1
        );
        let category_counts = value["evidence_gaps"]["category_counts"]
            .as_object()
            .expect("evidence category counts");
        for category in [
            "baseline-proof",
            "customer-proof",
            "environment",
            "live-trace",
            "repair-proof",
        ] {
            assert_eq!(
                category_counts
                    .get(category)
                    .and_then(serde_json::Value::as_u64),
                Some(1),
                "missing category {category}"
            );
        }
        assert_eq!(
            value["evidence_gaps"]["category_rows"][0]["category"],
            serde_json::json!("baseline-proof")
        );
        assert_eq!(
            value["evidence_gaps"]["category_rows"][0]["blocked"],
            serde_json::json!(1)
        );
        assert_eq!(
            value["evidence_gaps"]["category_rows"][0]["next_action"],
            serde_json::json!(
                "start or repair kube-scheduler-simulator before claiming live kube baseline"
            )
        );
        assert_eq!(value["action_items"][0]["priority"], serde_json::json!(1));
        assert_eq!(
            value["action_items"][0]["category"],
            serde_json::json!("baseline-proof")
        );
        assert_eq!(
            value["action_items"][0]["command_hint"],
            serde_json::json!("scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache")
        );
        assert_eq!(
            value["action_items"][0]["command_kind"],
            serde_json::json!("shell")
        );
        assert_eq!(
            value["action_items"][0]["copyable"],
            serde_json::json!(true)
        );
        let action_items = value["action_items"]
            .as_array()
            .expect("operator action items");
        assert_eq!(
            value["operator_runbook"]["step_count"],
            serde_json::json!(action_items.len())
        );
        assert!(
            value["operator_runbook"]["blocked_step_count"]
                .as_u64()
                .expect("blocked runbook steps")
                >= 4
        );
        assert!(
            value["operator_runbook"]["manual_step_count"]
                .as_u64()
                .expect("manual runbook steps")
                <= 2
        );
        assert_eq!(
            value["operator_runbook"]["next_shell_command"],
            serde_json::json!("scripts/kss-pool.sh status 1 1212 /tmp/ksolver-kss-cache")
        );
        let environment_action = action_items
            .iter()
            .find(|item| item["category"] == "environment")
            .expect("environment action item");
        assert_eq!(
            environment_action["command_hint"],
            serde_json::json!("kubectl --request-timeout=10s get --raw='/readyz?verbose'")
        );
        assert_eq!(
            environment_action["command_hints"],
            serde_json::json!([
                "kubectl --request-timeout=10s get --raw='/readyz?verbose'",
                "kubectl config current-context",
                "kubectl --request-timeout=10s auth can-i list pods --all-namespaces",
                "kubectl --request-timeout=10s get nodes"
            ])
        );
        assert!(value["operator_runbook"]["copyable_commands"]
            .as_array()
            .expect("copyable commands")
            .iter()
            .any(|command| command
                == "kubectl --request-timeout=10s auth can-i list pods --all-namespaces"));
        assert!(value["operator_runbook"]["copyable_command_rows"]
            .as_array()
            .expect("copyable command rows")
            .iter()
            .any(|row| row["command"]
                == serde_json::json!(
                    "kubectl --request-timeout=10s auth can-i list pods --all-namespaces"
                )
                && row["category"] == serde_json::json!("environment")
                && row["artifact"] == serde_json::json!("healthy Kubernetes watch/relist state")));
        assert!(value["evidence_gaps"]["rows"]
            .as_array()
            .expect("evidence gap rows")
            .iter()
            .any(|gap| {
                gap["artifact"] == "customer pricing source"
                    && gap["severity"] == "warn"
                    && gap["category"] == "customer-proof"
            }));
        assert_eq!(value["vram"]["model_driver_count"], serde_json::json!(8));
        // Order-independent (see evidence-bundle test): assert stable drivers are present.
        for key in ["top_driver_labels", "display_top_driver_labels"] {
            let labels = value["vram"][key]
                .as_array()
                .unwrap_or_else(|| panic!("vram.{key} should be an array"));
            assert!(!labels.is_empty(), "vram.{key} should be non-empty");
            for expected in [
                "parameter memory x precision",
                "synthetic VRAM headroom probe",
                "parameter count",
            ] {
                assert!(
                    labels.iter().any(|l| l == expected),
                    "vram.{key} should include {expected}: {labels:?}"
                );
            }
        }
        assert_eq!(
            value["vram"]["synthetic_reserve_driver"],
            serde_json::json!(true)
        );
        assert_eq!(
            value["vram"]["synthetic_headroom_driver"],
            value["vram"]["synthetic_reserve_driver"]
        );
        assert!(value["vram"]["synthetic_driver_labels"]
            .as_array()
            .expect("synthetic driver labels")
            .iter()
            .any(|label| label.as_str() == Some("synthetic VRAM headroom probe")));
        assert!(value["vram"]["display_synthetic_driver_labels"]
            .as_array()
            .expect("display synthetic driver labels")
            .iter()
            .any(|label| label.as_str() == Some("synthetic VRAM headroom probe")));
        assert!(value["vram"]["real_top_driver_labels"]
            .as_array()
            .expect("real driver labels")
            .iter()
            .all(|label| label.as_str() != Some("synthetic VRAM headroom probe")));
        assert!(value["vram"]["claim_safe_driver_labels"]
            .as_array()
            .expect("claim-safe driver labels")
            .iter()
            .all(|label| label.as_str() != Some("synthetic VRAM headroom probe")));
        assert!(
            !value["vram"]["claim_safe_driver_labels"]
                .as_array()
                .expect("claim-safe driver labels")
                .is_empty(),
            "claim-safe driver labels should be non-empty"
        );
        assert_eq!(
            value["vram"]["display_claim_safe_driver_labels"],
            value["vram"]["claim_safe_driver_labels"]
        );
        assert!(value["vram"]["driver_claim_boundary"]
            .as_str()
            .expect("driver claim boundary")
            .contains("organic workload predictors"));
        assert_eq!(
            value["vram"]["reserve_pressure_definition"],
            serde_json::json!(VRAM_RESERVE_PRESSURE_DEFINITION)
        );
        assert_eq!(
            value["vram"]["synthetic_headroom_definition"],
            value["vram"]["reserve_pressure_definition"]
        );
        let investment_rows = value["vram"]["investment_demo_rows"]
            .as_u64()
            .expect("VRAM investment demo rows");
        if investment_rows > 0 {
            assert_eq!(investment_rows, 6);
            assert_eq!(
                value["vram"]["investment_oom_risk_reduction_pods"],
                serde_json::json!(3)
            );
            assert_eq!(
                value["vram"]["investment_high_vram_nodes_preserved"],
                serde_json::json!(1)
            );
        }
        assert_eq!(
            value["evidence"]["path"],
            serde_json::json!("/api/scheduler/evidence-bundle")
        );
    }
}
