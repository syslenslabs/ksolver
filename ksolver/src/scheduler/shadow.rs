use crate::model::ScenarioConfig;
use crate::scheduler::config::ShadowConfig;
use crate::scheduler::decision::build_decision_trace;
use crate::scheduler::trace::{DecisionTrace, PodPlacement, TraceStore};
use crate::scheduler::watch_state::WatchState;
use crate::{collector, cpsat_rust, metrics, normalizer, pricing};
use anyhow::Result;
use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use futures_util::StreamExt;
use k8s_openapi::api::core::v1 as corev1;
use kube::runtime::watcher;
use kube::Api;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tracing::{error, info, warn};

#[derive(Clone)]
struct ShadowHttpState {
    traces: Arc<TraceStore>,
    watch_healthy: Arc<AtomicBool>,
    /// Latest normalized cluster snapshot, for re-validating rendered bindings (staleness guard).
    latest_cluster: Arc<Mutex<Option<crate::model::NormalizedCluster>>>,
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

    // HTTP server (traces / metrics / health).
    let http_state = ShadowHttpState {
        traces: traces.clone(),
        watch_healthy: watch_healthy.clone(),
        latest_cluster: latest_cluster.clone(),
    };
    let app = Router::new()
        .route("/api/scheduler/traces", get(traces_handler))
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
        if pending.is_empty() {
            continue;
        }
        metrics::inc_shadow_pod_observations(pending.len() as u64);
        let seq = traces.next_sequence();
        match run_one_solve(&cfg, seq, &pending, &normalized, started, snapshot_age_millis).await {
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
                        let r = crate::scheduler::binding::assess_binding_readiness(&e, &normalized);
                        (e, r)
                    })
                    .collect();
                let would_bind = plan.len();
                if let Some(bc) = &bind_client {
                    let outcomes = crate::scheduler::binder::apply_bindings(bc, &plan, &cfg).await;
                    let bound = outcomes
                        .iter()
                        .filter(|o| matches!(o.result, crate::scheduler::binder::BindResult::Bound { .. }))
                        .count() as u64;
                    let failed = outcomes
                        .iter()
                        .filter(|o| matches!(o.result, crate::scheduler::binder::BindResult::Failed { .. }))
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
    let (input, drops) = crate::scheduler::pending_input::build_pending_input_diagnosed(
        normalized,
        pending,
        &cfg.namespace_gpu_quotas,
        &|n| cfg.is_gpu_resource(n),
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
    use super::SHADOW_HTML;

    #[test]
    fn dashboard_asset_is_wired() {
        // The embedded dashboard must poll the traces API and render the decisions table, plus
        // the read-only dry-run binding-plan view.
        assert!(SHADOW_HTML.contains("/api/scheduler/traces"));
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
