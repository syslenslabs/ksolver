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
use serde::Deserialize;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tracing::{error, info, warn};

#[derive(Clone)]
struct ShadowHttpState {
    traces: Arc<TraceStore>,
    watch_healthy: Arc<AtomicBool>,
    /// Latest normalized cluster snapshot, for re-validating rendered bindings (staleness guard).
    latest_cluster: Arc<Mutex<Option<crate::model::NormalizedCluster>>>,
    /// Latest pending GPU pods observed by the watch loop, for user-triggered re-solves.
    latest_pending: Arc<Mutex<Vec<crate::scheduler::pod_filter::PendingGpuPod>>>,
    /// Latest binding executor outcomes, used to render read-only Kubernetes Event drafts.
    latest_bind_outcomes: Arc<Mutex<Option<(u64, Vec<crate::scheduler::binder::BindOutcome>)>>>,
    simulator_plan_cache: Arc<tokio::sync::Mutex<Option<(u64, serde_json::Value)>>>,
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
    Json(serde_json::json!({
        "dry_run": true,
        "note": "rendered from the latest shadow trace; advisory only — no evictions, migrations, preemptions, or bindings are applied",
        "trace_sequence": seq,
        "solve_millis": solve_millis,
        "repair_metrics": repair_metrics,
        "repair_plans": repair_plans,
        "repair_notes": repair_notes,
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
/// schedulerName JSONPatch for selected GPU pods. This does not call the Kubernetes API.
async fn scheduler_admission_handler(
    State(s): State<ShadowHttpState>,
    Json(review): Json<crate::scheduler::admission::AdmissionReview>,
) -> Json<crate::scheduler::admission::AdmissionReview> {
    let policy = crate::scheduler::admission::SchedulerPatchPolicy::from(&s.cfg);
    Json(crate::scheduler::admission::render_scheduler_admission_review(review, &policy))
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

async fn kube_simulator_plan_handler(State(s): State<ShadowHttpState>) -> Json<serde_json::Value> {
    let simulator_url = std::env::var("KSOLVER_SCHEDULER_SIMULATOR_URL")
        .or_else(|_| std::env::var("SCHEDULER_SIMULATOR_URL"))
        .unwrap_or_else(|_| DEFAULT_SIMULATOR_URL.to_string());
    if simulator_url.trim().is_empty() {
        return Json(serde_json::json!({
            "available": false,
            "source": "kube-scheduler-simulator",
            "reason": format!("no simulator URL configured; default {} was empty", DEFAULT_SIMULATOR_URL),
            "trace_sequence": 0,
            "placements": [],
        }));
    }

    let Some(trace) = s.traces.recent().into_iter().next() else {
        return Json(serde_json::json!({
            "available": false,
            "source": "kube-scheduler-simulator",
            "reason": "no shadow trace yet",
            "trace_sequence": 0,
            "placements": [],
        }));
    };

    let mut cache = s.simulator_plan_cache.lock().await;
    if let Some((sequence, value)) = cache.as_ref() {
        if *sequence == trace.sequence {
            return Json(value.clone());
        }
    }

    match run_kube_simulator_for_trace(&s.kubeconfig, simulator_url.trim(), &trace).await {
        Ok(placements) => {
            let value = serde_json::json!({
            "available": true,
            "source": format!("kube-scheduler-simulator at {}", simulator_url.trim_end_matches('/')),
            "trace_sequence": trace.sequence,
            "placements": placements,
            });
            *cache = Some((trace.sequence, value.clone()));
            Json(value)
        }
        Err(err) => {
            let value = serde_json::json!({
            "available": false,
            "source": format!("kube-scheduler-simulator at {}", simulator_url.trim_end_matches('/')),
            "reason": err.to_string(),
            "trace_sequence": trace.sequence,
            "placements": [],
            });
            *cache = Some((trace.sequence, value.clone()));
            Json(value)
        }
    }
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
const SHADOW_HTML: &str = include_str!("../../static/shadow.html");

async fn dashboard() -> axum::response::Html<&'static str> {
    axum::response::Html(SHADOW_HTML)
}

async fn readyz(State(s): State<ShadowHttpState>) -> (axum::http::StatusCode, &'static str) {
    if s.watch_healthy.load(Ordering::SeqCst) {
        (axum::http::StatusCode::OK, "ready")
    } else {
        (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            "watch not healthy",
        )
    }
}

/// Shadow-mode scheduler: observe pending GPU pods, periodically solve, record
/// decision traces, serve them. NEVER binds or mutates cluster state.
pub async fn run_shadow(cfg: ShadowConfig) -> Result<()> {
    metrics::register_metrics();
    let traces = Arc::new(TraceStore::new(64));
    let observed: Arc<Mutex<WatchState>> = Arc::new(Mutex::new(WatchState::new()));
    let watch_healthy = Arc::new(AtomicBool::new(false));
    let latest_cluster: Arc<Mutex<Option<crate::model::NormalizedCluster>>> =
        Arc::new(Mutex::new(None));
    let latest_pending: Arc<Mutex<Vec<crate::scheduler::pod_filter::PendingGpuPod>>> =
        Arc::new(Mutex::new(Vec::new()));
    let latest_bind_outcomes: Arc<
        Mutex<Option<(u64, Vec<crate::scheduler::binder::BindOutcome>)>>,
    > = Arc::new(Mutex::new(None));
    let active_objective = Arc::new(Mutex::new(ObjectiveSelection {
        profile: cfg.objective_profile,
        weights: cfg.objective_weights.clone(),
    }));

    // HTTP server (traces / metrics / health).
    let http_state = ShadowHttpState {
        traces: traces.clone(),
        watch_healthy: watch_healthy.clone(),
        latest_cluster: latest_cluster.clone(),
        latest_pending: latest_pending.clone(),
        latest_bind_outcomes: latest_bind_outcomes.clone(),
        simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
        kubeconfig: cfg.kubeconfig.clone(),
        cfg: cfg.clone(),
        active_objective: active_objective.clone(),
    };
    let app = Router::new()
        .route("/api/scheduler/traces", get(traces_handler))
        .route("/api/scheduler/objective", get(objective_config_handler))
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
                        }
                        let mut st = watch_observed.lock().expect("watch state poisoned");
                        st.apply(&ev, &watch_cfg);
                        metrics::set_shadow_pending(st.len() as i64);
                    }
                    Err(e) => warn!(error = %e, "watch error; will resync"),
                }
            }
            watch_flag.store(false, Ordering::SeqCst);
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
                        let outcomes =
                            crate::scheduler::binder::apply_bindings(bc, &plan, &cfg).await;
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
) -> Result<Vec<serde_json::Value>> {
    use crate::verifier::{
        clone_as_unscheduled_verification_pod, collect_simulator_resources, import_snapshot,
        pod_assigned_node, pod_scope, reset_simulator, SimulatorImportPayload,
        FILTER_RESULT_ANNOTATION,
    };
    use anyhow::Context;
    use std::collections::{BTreeMap, BTreeSet};

    let target_scopes: BTreeSet<String> = trace
        .decisions
        .iter()
        .map(|d| format!("{}/{}", d.namespace, d.name))
        .collect();
    if target_scopes.is_empty() {
        return Ok(Vec::new());
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
        .cloned()
        .enumerate()
    {
        pods.push(rewrite_simulator_pod(pod, format!("blocker-{idx}")));
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

    let client = reqwest::Client::new();
    let base_url = simulator_url.trim_end_matches('/');
    let payload = SimulatorImportPayload {
        pods,
        nodes: simulator_nodes,
        pvs: raw.pvs,
        pvcs: raw.pvcs,
        storage_classes: raw.storage_classes,
        priority_classes: raw.priority_classes,
        namespaces: vec![simulator_default_namespace()],
        scheduler_config: crate::verifier::default_scheduler_config(),
    };
    reset_simulator(&client, base_url).await?;
    import_snapshot(&client, base_url, &payload).await?;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let latest = loop {
        let response = client
            .get(format!("{base_url}/api/v1/export"))
            .send()
            .await
            .context("send scheduler-simulator export request")?;
        if !response.status().is_success() {
            anyhow::bail!(
                "scheduler-simulator export failed with status {}",
                response.status()
            );
        }
        let export = response
            .json::<crate::verifier::SimulatorExportPayload>()
            .await
            .context("decode scheduler-simulator export response")?;

        let resolved = export
            .pods
            .iter()
            .filter(|p| target_scopes.contains(&pod_scope(p)))
            .filter(|p| {
                pod_assigned_node(p).is_some()
                    || p.metadata
                        .annotations
                        .as_ref()
                        .map(|a| a.contains_key(FILTER_RESULT_ANNOTATION))
                        .unwrap_or(false)
            })
            .count();
        if resolved >= target_scopes.len() || tokio::time::Instant::now() >= deadline {
            break export;
        }
        tokio::time::sleep(Duration::from_millis(350)).await;
    };

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

    Ok(placements)
}

fn raw_node_gpu_capacity(node: &corev1::Node) -> i64 {
    node.status
        .as_ref()
        .and_then(|s| s.allocatable.as_ref())
        .and_then(|r| r.get("nvidia.com/gpu"))
        .and_then(|q| q.0.parse::<i64>().ok())
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
        // The embedded dashboard must poll the traces API and render the decisions table, plus
        // the read-only dry-run binding-plan view.
        assert!(SHADOW_HTML.contains("/api/scheduler/traces"));
        assert!(SHADOW_HTML.contains("/api/scheduler/cluster"));
        assert!(SHADOW_HTML.contains("/api/scheduler/objective"));
        assert!(SHADOW_HTML.contains("/api/scheduler/solve"));
        assert!(SHADOW_HTML.contains("/api/scheduler/kube-simulator-plan"));
        assert!(SHADOW_HTML.contains("id=\"decisions\""));
        assert!(SHADOW_HTML.contains("/api/scheduler/binding-plan"));
        assert!(SHADOW_HTML.contains("id=\"bindings\""));
        assert!(SHADOW_HTML.contains("deadlineSummary"));
        assert!(SHADOW_HTML.contains("predictedFinish"));
        assert!(SHADOW_HTML.contains("gpuCellText"));
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
            latest_cluster: Arc::new(Mutex::new(None)),
            latest_pending: Arc::new(Mutex::new(Vec::new())),
            latest_bind_outcomes: Arc::new(Mutex::new(None)),
            simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
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
        assert_eq!(value["repair_notes"][0], "fragmented but repairable");
        assert!(value["note"]
            .as_str()
            .unwrap_or_default()
            .contains("advisory only"));
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
        crate::scheduler::trace::PodDecision {
            uid: "u".into(),
            namespace: "team".into(),
            name: "p".into(),
            binding_group: String::new(),
            gpu_request: 1,
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
            latest_cluster: Arc::new(Mutex::new(Some(cluster))),
            latest_pending: Arc::new(Mutex::new(Vec::new())),
            latest_bind_outcomes: Arc::new(Mutex::new(None)),
            simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
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
            latest_cluster: Arc::new(Mutex::new(None)),
            latest_pending: Arc::new(Mutex::new(Vec::new())),
            latest_bind_outcomes: Arc::new(Mutex::new(None)),
            simulator_plan_cache: Arc::new(tokio::sync::Mutex::new(None)),
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
}
