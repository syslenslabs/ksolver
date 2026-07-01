use crate::model::ScenarioConfig;
use crate::scheduler::config::ShadowConfig;
use crate::scheduler::decision::build_decision_trace;
use crate::scheduler::trace::{DecisionTrace, PodPlacement, TraceStore};
use crate::scheduler::watch_state::WatchState;
use crate::{collector, cpsat_rust, metrics, normalizer, optimizer_input, pricing};
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
                metrics::inc_shadow_unplaced(unplaced);
                info!(
                    sequence = trace.sequence,
                    observed = trace.observed_pods,
                    unplaced,
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
    // Strict (ungrouped) so workload ids are "{ns}/{name}" and node names are real.
    // ignore_unschedulable = true: drop infeasible workloads so an unrelated
    // unschedulable pod cannot make the whole CP-SAT solve bail (it errors on any
    // workload with no feasible nodes). Dropped ksolver pods are still reported
    // honestly by the decision builder as "not submitted (filtered as unschedulable)".
    let input = optimizer_input::build_input_strict(&normalized, true);

    let scenario = ScenarioConfig {
        solver: "cp-sat-rust".to_string(),
        ..Default::default()
    };
    let (solution, status) = match cpsat_rust::solve(&input, &scenario) {
        Ok((sol, info)) => (sol, info.status),
        Err(e) => {
            warn!(error = %e, "solver error; recording infeasible");
            (Default::default(), "error".to_string())
        }
    };

    let solve_millis = started.elapsed().as_millis() as u64;
    metrics::observe_shadow_solve_seconds(started.elapsed().as_secs_f64());

    Ok(build_decision_trace(
        sequence,
        pending,
        &input,
        &solution,
        &status,
        solve_millis,
        snapshot_age_millis,
    ))
}
