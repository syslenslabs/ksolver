use crate::model::{ObjectiveProfile, ObjectiveWeights, ScenarioConfig};
use crate::scheduler::config::ShadowConfig;
use crate::scheduler::decision::build_decision_trace;
use crate::scheduler::trace::{DecisionTrace, PodPlacement, TraceStore};
use crate::scheduler::watch_state::WatchState;
use crate::{collector, cpsat_rust, metrics, normalizer, pricing};
use anyhow::Result;
use axum::extract::{Query, State};
use axum::routing::get;
use axum::{Json, Router};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1 as corev1;
use kube::runtime::watcher;
use kube::Api;
use serde::Deserialize;
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

#[derive(Debug, Default, Deserialize)]
struct SolveQuery {
    objective_profile: Option<String>,
    profile: Option<String>,
    admission: Option<i64>,
    gpu_demand: Option<i64>,
    gang_complete: Option<i64>,
    gpu_fragmentation: Option<i64>,
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
    match run_one_solve(&cfg, seq, &pending, &normalized, started, 0).await {
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

    // A kube client for the real-binding pass — built ONLY when real binding is enabled (otherwise
    // there is no mutation-capable client at all). Default: None ⇒ read-only shadow.
    let bind_client = if cfg.enable_real_binding {
        warn!(
            dry_run = cfg.real_binding_dry_run,
            max_per_pass = cfg.max_binds_per_pass,
            "REAL BINDING ENABLED — this scheduler will mutate the cluster (apply pod bindings)"
        );
        Some(collector::build_client(&cfg.kubeconfig).await?)
    } else {
        None
    };

    // Sequential solve loop: sleep AFTER each solve so a slow solve never overlaps itself.
    loop {
        tokio::time::sleep(cfg.batch_window).await;
        // Refresh the cluster snapshot EVERY iteration (even when idle) so the binding-plan
        // readiness re-check always reflects the current cluster, not the last solve's snapshot.
        let started = Instant::now();
        let normalized = match collect_normalized(&cfg).await {
            Ok(n) => n,
            Err(e) => {
                metrics::inc_shadow_solve_errors();
                error!(error = %e, "shadow snapshot collection failed");
                continue;
            }
        };
        let snapshot_age_millis = started.elapsed().as_millis() as u64;
        if let Ok(mut g) = latest_cluster.lock() {
            *g = Some(normalized.clone());
        }
        let pending = { observed.lock().expect("watch state poisoned").snapshot() };
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
            started,
            snapshot_age_millis,
        )
        .await
        {
            Ok(trace) => {
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
                metrics::inc_shadow_unplaced(unplaced);
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
                if let Some(bc) = &bind_client {
                    let outcomes = crate::scheduler::binder::apply_bindings(bc, &plan, &cfg).await;
                    let bound = outcomes
                        .iter()
                        .filter(|o| {
                            matches!(o.result, crate::scheduler::binder::BindResult::Bound { .. })
                        })
                        .count() as u64;
                    let failed = outcomes
                        .iter()
                        .filter(|o| {
                            matches!(
                                o.result,
                                crate::scheduler::binder::BindResult::Failed { .. }
                            )
                        })
                        .count() as u64;
                    let skipped = outcomes.len() as u64 - bound - failed;
                    metrics::inc_shadow_bound(bound);
                    metrics::inc_shadow_bind_skipped(skipped);
                    metrics::inc_shadow_bind_failed(failed);
                    info!(
                        sequence = trace.sequence,
                        bound,
                        skipped,
                        failed,
                        dry_run = cfg.real_binding_dry_run,
                        "real binding pass complete"
                    );
                }
                info!(
                    sequence = trace.sequence,
                    observed = trace.observed_pods,
                    unplaced,
                    caveated,
                    would_bind,
                    real_binding = cfg.enable_real_binding,
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

/// Read-only collect + normalize of the current cluster (shared by the solve path and the
/// per-iteration readiness-snapshot refresh). Never mutates cluster state.
async fn collect_normalized(cfg: &ShadowConfig) -> Result<crate::model::NormalizedCluster> {
    let coll =
        collector::KubeCollector::new(cfg.cluster_name.clone(), cfg.kubeconfig.clone()).await?;
    let snapshot = coll.collect().await?;
    let pricing_catalog = pricing::load_pricing_catalog("").unwrap_or_default();
    Ok(
        normalizer::Normalizer::new(pricing_catalog, normalizer::Options::default())
            .normalize(&snapshot),
    )
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

async fn run_one_solve(
    cfg: &ShadowConfig,
    sequence: u64,
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
    normalized: &crate::model::NormalizedCluster,
    started: Instant,
    snapshot_age_millis: u64,
) -> Result<DecisionTrace> {
    metrics::inc_shadow_solves();

    // Pending-only solve: place ONLY the observed ksolver pods (gang-grouped by label);
    // every already-placed pod is fixed context (subtracted from node capacity). Small
    // and fast versus the whole-cluster solve, and correct per-pod against residual.
    let (input, drops) =
        crate::scheduler::pending_input::build_pending_input_diagnosed_with_candidate_limit(
            normalized,
            pending,
            &cfg.namespace_gpu_quotas,
            &|n| cfg.is_gpu_resource(n),
            cfg.candidate_node_limit,
        );
    // Flatten drop diagnostics into a pod-scope -> reason map for the decision trace.
    let mut drop_reasons: std::collections::HashMap<String, String> =
        std::collections::HashMap::new();
    for d in &drops {
        for scope in &d.pod_scopes {
            drop_reasons.insert(scope.clone(), d.reason.clone());
        }
    }

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
    let solve_start = Instant::now();
    let (solution, status, solve_ok) = match cpsat_rust::solve(&input, &scenario) {
        Ok((sol, info)) => (sol, info.status, true),
        Err(e) => {
            warn!(error = %e, "solver produced no usable solution");
            (Default::default(), format!("no-solution: {e}"), false)
        }
    };
    let solve_core_millis = solve_start.elapsed().as_millis() as u64;

    let solve_millis = started.elapsed().as_millis() as u64;
    metrics::observe_shadow_solve_seconds(started.elapsed().as_secs_f64());

    // Time-sliced (oversubscribed, no-isolation) GPU nodes, for placement disclosure.
    let time_sliced_nodes: std::collections::HashSet<String> = normalized
        .nodes
        .iter()
        .filter(|n| crate::scheduler::decision::is_time_sliced_node(&n.labels))
        .map(|n| n.name.clone())
        .collect();

    Ok(build_decision_trace(
        sequence,
        pending,
        &input,
        &solution,
        &status,
        solve_ok,
        solve_millis,
        solve_core_millis,
        snapshot_age_millis,
        &drop_reasons,
        &time_sliced_nodes,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    }

    #[tokio::test]
    async fn binding_plan_endpoint_is_dry_run_and_lists_bindings() {
        use super::{binding_plan_handler, ShadowHttpState};
        use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement, TraceStore};
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
                gpu_request: 1,
                placement: PodPlacement::Placed { node: "n1".into() },
                caveats: vec![],
            }],
            solver_status: "OPTIMAL".into(),
            solve_millis: 5,
            solve_core_millis: 3,
            snapshot_age_millis: 0,
            note: String::new(),
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
                gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
                gang_colocate_label: "scheduling.x-k8s.io/gang-colocate".to_string(),
                solve_time_limit_secs: 10,
                namespace_gpu_quotas: std::collections::BTreeMap::new(),
                enable_real_binding: false,
                real_binding_dry_run: false,
                max_binds_per_pass: 10,
                objective_profile: ObjectiveProfile::CostBinpack,
                objective_weights: ObjectiveWeights::default(),
                candidate_node_limit: 0,
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
}
