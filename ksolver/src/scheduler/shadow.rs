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

    // HTTP server (traces / metrics / health).
    let http_state = ShadowHttpState {
        traces: traces.clone(),
        watch_healthy: watch_healthy.clone(),
    };
    let app = Router::new()
        .route("/api/scheduler/traces", get(traces_handler))
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

    // Sequential solve loop: sleep AFTER each solve so a slow solve never overlaps itself.
    loop {
        tokio::time::sleep(cfg.batch_window).await;
        let pending = { observed.lock().expect("watch state poisoned").snapshot() };
        if pending.is_empty() {
            continue;
        }
        metrics::inc_shadow_pod_observations(pending.len() as u64);
        let seq = traces.next_sequence();
        match run_one_solve(&cfg, seq, &pending).await {
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
                info!(
                    sequence = trace.sequence,
                    observed = trace.observed_pods,
                    unplaced,
                    caveated,
                    status = %trace.solver_status,
                    solve_millis = trace.solve_millis,
                    "shadow decision recorded (bound nothing)"
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

async fn run_one_solve(
    cfg: &ShadowConfig,
    sequence: u64,
    pending: &[crate::scheduler::pod_filter::PendingGpuPod],
) -> Result<DecisionTrace> {
    metrics::inc_shadow_solves();
    let started = Instant::now();

    // 1. Snapshot the cluster (read-only) via the existing collector.
    let coll =
        collector::KubeCollector::new(cfg.cluster_name.clone(), cfg.kubeconfig.clone()).await?;
    let snapshot = coll.collect().await?;
    let snapshot_age_millis = started.elapsed().as_millis() as u64;

    // 2. Normalize + build strict (ungrouped) input + solve, mirroring service::Analyzer.
    let pricing_catalog = pricing::load_pricing_catalog("").unwrap_or_default();
    let normalized = normalizer::Normalizer::new(pricing_catalog, normalizer::Options::default())
        .normalize(&snapshot);
    // Pending-only solve: place ONLY the observed ksolver pods (gang-grouped by label);
    // every already-placed pod is fixed context (subtracted from node capacity). Small
    // and fast versus the whole-cluster solve, and correct per-pod against residual.
    let (input, drops) = crate::scheduler::pending_input::build_pending_input_diagnosed(
        &normalized,
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
        // The embedded dashboard must poll the traces API and render the decisions table.
        assert!(SHADOW_HTML.contains("/api/scheduler/traces"));
        assert!(SHADOW_HTML.contains("id=\"decisions\""));
    }
}
