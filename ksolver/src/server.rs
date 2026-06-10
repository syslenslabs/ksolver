use crate::metrics::render_metrics;
use crate::model::{ProgressUpdate, SolveRequest};
use crate::service::Analyzer;
use crate::state_cache::StateCache;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::StreamExt;
use kube::config::Kubeconfig;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{broadcast, Mutex};
use tracing::{debug, error, info, warn};

const INDEX_HTML: &str = include_str!("../static/index.html");

#[derive(Clone, Default)]
pub struct ServerOptions {
    pub default_kubeconfig: String,
    pub default_pricing_file: String,
    pub default_verification_url: String,
    pub metrics_addr: String,
}

#[derive(Clone)]
pub struct AppState {
    options: Arc<ServerOptions>,
    analyzer: Analyzer,
    jobs: Arc<JobManager>,
}

pub fn app(options: ServerOptions) -> Router {
    let state = AppState {
        options: Arc::new(options),
        analyzer: Analyzer::new(),
        jobs: Arc::new(JobManager::default()),
    };

    Router::new()
        .route("/", get(index))
        .route("/api/health", get(health))
        .route("/metrics", get(metrics))
        .route("/api/defaults", get(defaults))
        .route("/api/latest", get(latest))
        .route("/api/snapshots", get(snapshots))
        .route("/api/scenarios/latest", get(latest_scenarios))
        .route("/api/scenarios/summary", get(scenario_summary))
        .route("/api/history", get(history))
        .route("/api/solve", post(solve))
        .route("/api/analyze", post(solve))
        .route("/api/solve/start", post(start_solve))
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/:id/status", get(job_status))
        .route("/api/jobs/:id/events", get(job_events))
        .route("/api/chat", post(chat))
        .with_state(state)
}

async fn index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn health() -> Json<Value> {
    Json(json!({"ok": true, "implementation": "rust"}))
}

async fn metrics() -> impl IntoResponse {
    (
        StatusCode::OK,
        [("content-type", "text/plain; version=0.0.4; charset=utf-8")],
        render_metrics(),
    )
}

async fn defaults(State(state): State<AppState>) -> Json<Value> {
    Json(json!({
        "kubeconfig": state.options.default_kubeconfig,
        "pricingFile": state.options.default_pricing_file,
        "snapshotFile": "",
        "defaultSolver": "cp-sat-rust",
        "scenario": {
            "Solver": "cp-sat-rust",
            "CPUHeadroomPercent": 0.0,
            "MemoryHeadroomPercent": 0.0,
            "StorageHeadroomPercent": 0.0,
            "PodsHeadroom": 0,
            "NamespaceInclude": [],
            "DisallowedPools": [],
            "NodePoolLabelKeys": [
                "eks.amazonaws.com/nodegroup",
                "cloud.google.com/gke-nodepool",
                "agentpool"
            ],
            "MaxWorkloads": 0,
            "RelaxPreferredAffinity": false,
            "RelaxRequiredAntiAffinity": false,
            "IgnoreTaints": false,
            "IgnoreUnschedulableWorkloads": true,
            "CPUOvercommitRatio": 1.0,
            "MemoryOvercommitRatio": 1.0,
            "UseUsageAdjustedRequests": false,
            "UsageRiskPreset": "custom",
            "UsageRequestFloorRatio": 0.5,
            "CPUUsageSafetyFactor": 1.5,
            "MemoryUsageSafetyFactor": 2.0,
            "MaxMemoryOverflowProbabilityPercent": 1.0,
            "HistoricalUsage": {
                "Enabled": false,
                "Provider": "prometheus",
                "Lookback": "24h",
                "Step": "5m",
                "PrometheusURL": "",
                "PrometheusUsername": "",
                "PrometheusToken": "",
                "SecretNamespace": "grafana-cloud",
                "SecretName": "grafana-external-creds",
                "SecretPrometheusURLKey": "prom_host",
                "SecretPrometheusUsernameKey": "prom_username",
                "SecretPrometheusTokenKey": "alloy_token"
            },
            "VerificationBackend": "scheduler-simulator",
            "VerificationURL": state.options.default_verification_url,
            "CostWeight": 10000000000_i64,
            "ActiveNodeWeight": 5000000000_i64,
            "MemorySlackWeight": 1_i64,
            "CPUSlackWeight": 1000000_i64,
            "ChurnWeight": 10000000_i64,
            "EnableJointRightsizing": false,
            "RightsizingWeight": 100000000_i64
        }
    }))
}

async fn latest() -> impl IntoResponse {
    match StateCache::load_latest_run_any().await {
        Ok(Some(run)) => (
            StatusCode::OK,
            Json(json!({
                "request": run.request,
                "report": report_for_ui(run.report),
                "stateRoot": run.state_root,
            })),
        )
            .into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no cached runs found"})),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn snapshots() -> impl IntoResponse {
    match StateCache::list_snapshots_any().await {
        Ok(items) => (
            StatusCode::OK,
            Json(json!({
                "items": items,
            })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn history(Query(query): Query<ScenarioQuery>) -> impl IntoResponse {
    let cluster_name = query.cluster.unwrap_or_else(|| "default".to_string());
    let cache = StateCache::for_request(&SolveRequest {
        cluster_name,
        ..Default::default()
    });
    match cache.load_history().await {
        Ok(entries) => (StatusCode::OK, Json(json!({"items": entries}))).into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

#[derive(serde::Deserialize)]
struct ScenarioQuery {
    cluster: Option<String>,
}

async fn latest_scenarios(Query(query): Query<ScenarioQuery>) -> impl IntoResponse {
    let cluster_name = query.cluster.unwrap_or_else(|| "default".to_string());
    let cache = StateCache::for_request(&SolveRequest {
        cluster_name,
        ..Default::default()
    });
    match cache.load_scenario_runs().await {
        Ok(runs) => (
            StatusCode::OK,
            Json(json!({
                "items": runs.into_iter().map(|run| json!({
                    "scenarioName": run.scenario_name,
                    "request": run.request,
                    "report": report_for_ui(run.report),
                    "stateRoot": run.state_root,
                })).collect::<Vec<_>>(),
            })),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn scenario_summary(Query(query): Query<ScenarioQuery>) -> impl IntoResponse {
    let cluster_name = query.cluster.unwrap_or_else(|| "default".to_string());
    let cache = StateCache::for_request(&SolveRequest {
        cluster_name,
        ..Default::default()
    });
    match cache.load_scenario_summary().await {
        Ok(Some(summary)) => (StatusCode::OK, Json(json!(summary))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "no cached scenario summary found"})),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

fn report_for_ui(report: crate::model::AnalysisReport) -> Value {
    let mut value = serde_json::to_value(report).unwrap_or_else(|_| json!({}));
    if let Value::Object(ref mut object) = value {
        object.remove("Snapshot");
        object.remove("snapshot");
    }
    value
}

async fn solve(
    State(state): State<AppState>,
    Json(mut req): Json<SolveRequest>,
) -> impl IntoResponse {
    apply_defaults(&state.options, &mut req);
    info!(
        path = "/api/solve",
        solver = %req.scenario.solver,
        cluster = %req.cluster_name,
        kubeconfig = if req.kubeconfig.is_empty() {
            "<default>"
        } else {
            req.kubeconfig.as_str()
        },
        pricing_file = if req.pricing_file.is_empty() {
            "<auto>"
        } else {
            req.pricing_file.as_str()
        },
        snapshot_file = if req.snapshot_file.is_empty() {
            "<none>"
        } else {
            req.snapshot_file.as_str()
        },
        "solve request received"
    );
    match state.analyzer.analyze(req).await {
        Ok(report) => (StatusCode::OK, Json(serde_json::to_value(report).unwrap())).into_response(),
        Err(err) => (
            StatusCode::NOT_IMPLEMENTED,
            Json(json!({"error": err.to_string()})),
        )
            .into_response(),
    }
}

async fn start_solve(
    State(state): State<AppState>,
    Json(mut req): Json<SolveRequest>,
) -> impl IntoResponse {
    apply_defaults(&state.options, &mut req);
    info!(
        path = "/api/solve/start",
        solver = %req.scenario.solver,
        cluster = %req.cluster_name,
        kubeconfig = if req.kubeconfig.is_empty() {
            "<default>"
        } else {
            req.kubeconfig.as_str()
        },
        pricing_file = if req.pricing_file.is_empty() {
            "<auto>"
        } else {
            req.pricing_file.as_str()
        },
        snapshot_file = if req.snapshot_file.is_empty() {
            "<none>"
        } else {
            req.snapshot_file.as_str()
        },
        "async solve request received"
    );
    let id = state.jobs.start(state.analyzer.clone(), req).await;
    (StatusCode::ACCEPTED, Json(json!({"jobId": id})))
}

async fn list_jobs(State(state): State<AppState>) -> Json<Value> {
    let jobs = state.jobs.list().await;
    let list: Vec<Value> = jobs
        .iter()
        .map(|j| {
            json!({
                "id": j.id,
                "progress": j.progress,
                "done": j.done,
                "error": j.error,
            })
        })
        .collect();
    Json(json!(list))
}

async fn job_status(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    debug!(job_id = %id, path = "/api/jobs/:id/status", "job status requested");
    match state.jobs.get(&id).await {
        Some(job) => (
            StatusCode::OK,
            Json(json!({
                "id": job.id,
                "progress": job.progress,
                "done": job.done,
                "error": job.error,
                "report": job.report,
            })),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "job not found"})),
        )
            .into_response(),
    }
}

async fn job_events(State(state): State<AppState>, Path(id): Path<String>) -> impl IntoResponse {
    debug!(job_id = %id, path = "/api/jobs/:id/events", "job events subscribed");
    let Some(mut rx) = state.jobs.subscribe(&id).await else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "job not found"})),
        )
            .into_response();
    };

    let initial = state
        .jobs
        .get(&id)
        .await
        .map(|j| j.progress)
        .unwrap_or_default();
    let initial_event =
        axum::response::sse::Event::default().data(serde_json::to_string(&initial).unwrap());
    let update_stream = async_stream::stream! {
        yield Ok::<_, std::convert::Infallible>(initial_event);
        loop {
            match rx.recv().await {
                Ok(update) => {
                    let event = axum::response::sse::Event::default()
                        .data(serde_json::to_string(&update).unwrap());
                    yield Ok(event);
                }
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    };

    Sse::new(update_stream).into_response()
}

fn apply_defaults(options: &ServerOptions, req: &mut SolveRequest) {
    if req.kubeconfig.is_empty() {
        req.kubeconfig = options.default_kubeconfig.clone();
    }
    if req.pricing_file.is_empty() {
        req.pricing_file = options.default_pricing_file.clone();
    }
    if req.cluster_name.is_empty() && !req.snapshot_file.is_empty() {
        req.cluster_name = "default".to_string();
    }
    if req.cluster_name.is_empty() {
        req.cluster_name = infer_cluster_name_from_kubeconfig(&req.kubeconfig)
            .unwrap_or_else(|| "default".to_string());
    }
    if req.scenario.solver.is_empty() {
        req.scenario.solver = "cp-sat-rust".to_string();
    }
    if req.scenario.verification_backend.is_empty() {
        req.scenario.verification_backend = "scheduler-simulator".to_string();
    }
    if req.scenario.verification_url.is_empty() {
        req.scenario.verification_url = options.default_verification_url.clone();
    }
}

fn infer_cluster_name_from_kubeconfig(path: &str) -> Option<String> {
    if path.is_empty() {
        return None;
    }
    let kubeconfig = Kubeconfig::read_from(path).ok()?;
    kubeconfig.current_context
}

#[derive(Clone, Default)]
struct JobManager {
    inner: Arc<Mutex<JobStateMap>>,
}

#[derive(Default)]
struct JobStateMap {
    seq: i64,
    jobs: HashMap<String, JobState>,
}

#[derive(Clone)]
struct JobState {
    id: String,
    progress: ProgressUpdate,
    report: Option<Value>,
    done: bool,
    error: String,
    tx: broadcast::Sender<ProgressUpdate>,
}

impl Default for JobState {
    fn default() -> Self {
        let (tx, _) = broadcast::channel(16);
        Self {
            id: String::new(),
            progress: ProgressUpdate::default(),
            report: None,
            done: false,
            error: String::new(),
            tx,
        }
    }
}

impl JobManager {
    async fn start(&self, analyzer: Analyzer, req: SolveRequest) -> String {
        let id = {
            let mut inner = self.inner.lock().await;
            inner.seq += 1;
            let id = format!("job-{}", inner.seq);
            let (tx, _) = broadcast::channel(32);
            inner.jobs.insert(
                id.clone(),
                JobState {
                    id: id.clone(),
                    progress: ProgressUpdate {
                        stage: "queued".to_string(),
                        message: "Queued".to_string(),
                        percent: 0,
                        done: false,
                        error: String::new(),
                        elapsed_ms: 0,
                    },
                    report: None,
                    done: false,
                    error: String::new(),
                    tx,
                },
            );
            id
        };

        let jobs = self.clone();
        let spawn_id = id.clone();
        info!(
            job_id = %spawn_id,
            solver = %req.scenario.solver,
            cluster = %req.cluster_name,
            kubeconfig = if req.kubeconfig.is_empty() {
                "<default>"
            } else {
                req.kubeconfig.as_str()
            },
            pricing_file = if req.pricing_file.is_empty() {
                "<auto>"
            } else {
                req.pricing_file.as_str()
            },
            "job queued"
        );
        tokio::spawn(async move {
            info!(job_id = %spawn_id, solver = %req.scenario.solver, "job started");
            let start = Instant::now();
            let result = analyzer
                .analyze_with_progress(
                    req,
                    Some(Box::new({
                        let jobs = jobs.clone();
                        let callback_id = spawn_id.clone();
                        move |update| {
                            let jobs = jobs.clone();
                            let id = callback_id.clone();
                            let update = if update.done {
                                ProgressUpdate {
                                    done: false,
                                    ..update
                                }
                            } else {
                                update
                            };
                            tokio::spawn(async move {
                                jobs.publish(&id, update).await;
                            });
                        }
                    })),
                )
                .await;

            match result {
                Ok(report) => {
                    let update = ProgressUpdate {
                        stage: "done".to_string(),
                        message: "Analysis complete".to_string(),
                        percent: 100,
                        done: true,
                        error: String::new(),
                        elapsed_ms: start.elapsed().as_millis() as i64,
                    };
                    jobs.finish_success(&spawn_id, serde_json::to_value(report).unwrap(), update)
                        .await;
                    info!(job_id = %spawn_id, elapsed_ms = start.elapsed().as_millis(), "job succeeded");
                    info!(job_id = %spawn_id, "job completed");
                }
                Err(err) => {
                    error!(job_id = %spawn_id, error = %err, "job failed");
                    warn!(
                        job_id = %spawn_id,
                        elapsed_ms = start.elapsed().as_millis(),
                        error = %err,
                        chain = %format_error_chain(&err),
                        "job failure details"
                    );
                    let update = ProgressUpdate {
                        stage: "done".to_string(),
                        message: "Analysis failed".to_string(),
                        percent: 100,
                        done: true,
                        error: err.to_string(),
                        elapsed_ms: start.elapsed().as_millis() as i64,
                    };
                    jobs.finish_error(&spawn_id, err.to_string(), update).await;
                }
            }
        });

        id
    }

    async fn list(&self) -> Vec<JobStateSnapshot> {
        let inner = self.inner.lock().await;
        inner
            .jobs
            .values()
            .map(|job| JobStateSnapshot {
                id: job.id.clone(),
                progress: job.progress.clone(),
                report: None,
                done: job.done,
                error: job.error.clone(),
            })
            .collect()
    }

    async fn get(&self, id: &str) -> Option<JobStateSnapshot> {
        let inner = self.inner.lock().await;
        inner.jobs.get(id).map(|job| JobStateSnapshot {
            id: job.id.clone(),
            progress: job.progress.clone(),
            report: job.report.clone(),
            done: job.done,
            error: job.error.clone(),
        })
    }

    async fn subscribe(&self, id: &str) -> Option<broadcast::Receiver<ProgressUpdate>> {
        let inner = self.inner.lock().await;
        inner.jobs.get(id).map(|job| job.tx.subscribe())
    }

    async fn publish(&self, id: &str, update: ProgressUpdate) {
        let mut inner = self.inner.lock().await;
        if let Some(job) = inner.jobs.get_mut(id) {
            if job.done {
                return;
            }
            debug!(
                job_id = %id,
                stage = %update.stage,
                percent = update.percent,
                done = update.done,
                message = %update.message,
                error = if update.error.is_empty() {
                    "<none>"
                } else {
                    update.error.as_str()
                },
                elapsed_ms = update.elapsed_ms,
                "job progress"
            );
            job.progress = update.clone();
            let _ = job.tx.send(update);
        }
    }

    async fn finish_success(&self, id: &str, report: Value, update: ProgressUpdate) {
        let mut inner = self.inner.lock().await;
        if let Some(job) = inner.jobs.get_mut(id) {
            info!(job_id = %id, stage = %update.stage, percent = update.percent, elapsed_ms = update.elapsed_ms, "job state marked successful");
            job.progress = update.clone();
            job.report = Some(report);
            job.done = true;
            job.error.clear();
            let _ = job.tx.send(update);
        }
    }

    async fn finish_error(&self, id: &str, error: String, update: ProgressUpdate) {
        let mut inner = self.inner.lock().await;
        if let Some(job) = inner.jobs.get_mut(id) {
            warn!(job_id = %id, stage = %update.stage, percent = update.percent, elapsed_ms = update.elapsed_ms, error = %error, "job state marked failed");
            job.progress = update.clone();
            job.done = true;
            job.error = error;
            let _ = job.tx.send(update);
        }
    }
}

fn format_error_chain(err: &anyhow::Error) -> String {
    err.chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

#[derive(Clone)]
struct JobStateSnapshot {
    id: String,
    progress: ProgressUpdate,
    report: Option<Value>,
    done: bool,
    error: String,
}

#[derive(serde::Deserialize)]
struct ChatRequest {
    provider: String,
    api_key: String,
    model: Option<String>,
    messages: Vec<ChatMessage>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone)]
struct ChatMessage {
    role: String,
    content: String,
}

fn build_system_prompt(report: &Value) -> String {
    let explainability = report
        .get("Explainability")
        .or_else(|| report.get("explainability"));
    let optimization = report
        .get("Optimization")
        .or_else(|| report.get("optimization"));

    let mut context_parts = Vec::new();

    if let Some(expl) = explainability {
        if let Some(drivers) = expl.get("Drivers").or_else(|| expl.get("drivers")) {
            context_parts.push(format!(
                "## Constraint Blockers\n{}",
                serde_json::to_string_pretty(drivers).unwrap_or_default()
            ));
        }
        if let Some(waterfall) = expl
            .get("SavingsWaterfall")
            .or_else(|| expl.get("savings_waterfall"))
        {
            context_parts.push(format!(
                "## Savings Waterfall\n{}",
                serde_json::to_string_pretty(waterfall).unwrap_or_default()
            ));
        }
        if let Some(actions) = expl.get("ActionItems").or_else(|| expl.get("action_items")) {
            context_parts.push(format!(
                "## Action Items\n{}",
                serde_json::to_string_pretty(actions).unwrap_or_default()
            ));
        }
        if let Some(pools) = expl
            .get("PoolSummaries")
            .or_else(|| expl.get("pool_summaries"))
        {
            context_parts.push(format!(
                "## Node Pool Summaries\n{}",
                serde_json::to_string_pretty(pools).unwrap_or_default()
            ));
        }
        if let Some(table) = expl
            .get("WorkloadCostTable")
            .or_else(|| expl.get("workload_cost_table"))
        {
            if let Some(arr) = table.as_array() {
                let top: Vec<_> = arr.iter().take(30).collect();
                context_parts.push(format!(
                    "## Top Workloads by Cost (top 30)\n{}",
                    serde_json::to_string_pretty(&top).unwrap_or_default()
                ));
            }
        }
        if let Some(vpa) = expl.get("VpaOverview").or_else(|| expl.get("vpa_overview")) {
            context_parts.push(format!(
                "## VPA Overview\n{}",
                serde_json::to_string_pretty(vpa).unwrap_or_default()
            ));
        }
        if let Some(vpa_modes) = expl
            .get("VpaUpdateModeImpacts")
            .or_else(|| expl.get("vpa_update_mode_impacts"))
        {
            context_parts.push(format!(
                "## VPA Update Mode Impacts\n{}",
                serde_json::to_string_pretty(vpa_modes).unwrap_or_default()
            ));
        }
        if let Some(vpa_gaps) = expl
            .get("VpaPolicyGaps")
            .or_else(|| expl.get("vpa_policy_gaps"))
        {
            context_parts.push(format!(
                "## VPA Policy Gaps\n{}",
                serde_json::to_string_pretty(vpa_gaps).unwrap_or_default()
            ));
        }
        if let Some(missing) = expl
            .get("MissingVpaOpportunities")
            .or_else(|| expl.get("missing_vpa_opportunities"))
        {
            context_parts.push(format!(
                "## Missing VPA Opportunities\n{}",
                serde_json::to_string_pretty(missing).unwrap_or_default()
            ));
        }
    }

    if let Some(opt) = optimization {
        context_parts.push(format!(
            "## Optimization Summary\n{}",
            serde_json::to_string_pretty(opt).unwrap_or_default()
        ));
    }

    format!(
        "You are an expert Kubernetes cost optimization assistant. The user is looking at a solver \
         report for their cluster. You help them understand why workloads can't pack better, what \
         constraints are blocking savings, and what actions to take.\n\n\
         Answer questions in practical terms. Reference specific workloads, nodes, and namespaces \
         by name. Suggest kubectl commands where relevant. Format responses with markdown.\n\n\
         # Solver Report Context\n\n{}",
        context_parts.join("\n\n")
    )
}

async fn chat(Json(req): Json<ChatRequest>) -> impl IntoResponse {
    if req.api_key.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "API key is required"})),
        )
            .into_response();
    }
    if req.provider != "claude" && req.provider != "openai" {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "Provider must be 'claude' or 'openai'"})),
        )
            .into_response();
    }

    let report = match StateCache::load_latest_run_any().await {
        Ok(Some(run)) => serde_json::to_value(run.report).unwrap_or_else(|_| json!({})),
        _ => json!({}),
    };

    let system_prompt = build_system_prompt(&report);
    let client = reqwest::Client::new();

    match req.provider.as_str() {
        "claude" => chat_claude(client, &req, &system_prompt).await,
        "openai" => chat_openai(client, &req, &system_prompt).await,
        _ => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "unknown provider"})),
        )
            .into_response(),
    }
}

async fn chat_claude(
    client: reqwest::Client,
    req: &ChatRequest,
    system_prompt: &str,
) -> axum::response::Response {
    let model = req.model.as_deref().unwrap_or("claude-sonnet-4-6");
    let messages: Vec<Value> = req
        .messages
        .iter()
        .map(|m| json!({"role": m.role, "content": m.content}))
        .collect();

    let body = json!({
        "model": model,
        "max_tokens": 4096,
        "system": system_prompt,
        "messages": messages,
        "stream": true,
    });

    let response = match client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", &req.api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
    {
        Ok(r) => r,
        Err(err) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": err.to_string()})),
            )
                .into_response();
        }
    };

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body_text = response.text().await.unwrap_or_default();
        return (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(json!({"error": body_text})),
        )
            .into_response();
    }

    let stream = response.bytes_stream();
    let sse_stream = async_stream::stream! {
        let mut byte_buf = Vec::new();
        tokio::pin!(stream);
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    byte_buf.extend_from_slice(&bytes);
                    let text = String::from_utf8_lossy(&byte_buf).to_string();
                    byte_buf.clear();
                    for line in text.lines() {
                        if let Some(data) = line.strip_prefix("data: ") {
                            if data == "[DONE]" { break; }
                            if let Ok(parsed) = serde_json::from_str::<Value>(data) {
                                if parsed.get("type").and_then(|t| t.as_str()) == Some("content_block_delta") {
                                    if let Some(text) = parsed.get("delta").and_then(|d| d.get("text")).and_then(|t| t.as_str()) {
                                        let event = axum::response::sse::Event::default()
                                            .data(json!({"text": text}).to_string());
                                        yield Ok::<_, std::convert::Infallible>(event);
                                    }
                                }
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
        let done_event = axum::response::sse::Event::default().data(json!({"done": true}).to_string());
        yield Ok(done_event);
    };

    Sse::new(sse_stream).into_response()
}

async fn chat_openai(
    client: reqwest::Client,
    req: &ChatRequest,
    system_prompt: &str,
) -> axum::response::Response {
    let model = req.model.as_deref().unwrap_or("gpt-4o");
    let mut messages = vec![json!({"role": "system", "content": system_prompt})];
    for m in &req.messages {
        messages.push(json!({"role": m.role, "content": m.content}));
    }

    let body = json!({
        "model": model,
        "messages": messages,
        "stream": true,
    });

    let response = match client
        .post("https://api.openai.com/v1/chat/completions")
        .header("Authorization", format!("Bearer {}", req.api_key))
        .header("content-type", "application/json")
        .body(body.to_string())
        .send()
        .await
    {
        Ok(r) => r,
        Err(err) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": err.to_string()})),
            )
                .into_response();
        }
    };

    if !response.status().is_success() {
        let status = response.status().as_u16();
        let body_text = response.text().await.unwrap_or_default();
        return (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
            Json(json!({"error": body_text})),
        )
            .into_response();
    }

    let stream = response.bytes_stream();
    let sse_stream = async_stream::stream! {
        let mut byte_buf = Vec::new();
        tokio::pin!(stream);
        while let Some(chunk) = stream.next().await {
            match chunk {
                Ok(bytes) => {
                    byte_buf.extend_from_slice(&bytes);
                    let text = String::from_utf8_lossy(&byte_buf).to_string();
                    byte_buf.clear();
                    for line in text.lines() {
                        if let Some(data) = line.strip_prefix("data: ") {
                            if data == "[DONE]" { break; }
                            if let Ok(parsed) = serde_json::from_str::<Value>(data) {
                                if let Some(content) = parsed
                                    .get("choices")
                                    .and_then(|c| c.get(0))
                                    .and_then(|c| c.get("delta"))
                                    .and_then(|d| d.get("content"))
                                    .and_then(|t| t.as_str())
                                {
                                    let event = axum::response::sse::Event::default()
                                        .data(json!({"text": content}).to_string());
                                    yield Ok::<_, std::convert::Infallible>(event);
                                }
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
        let done_event = axum::response::sse::Event::default().data(json!({"done": true}).to_string());
        yield Ok(done_event);
    };

    Sse::new(sse_stream).into_response()
}

#[cfg(test)]
mod tests {
    use super::JobManager;
    use crate::model::ProgressUpdate;
    use serde_json::json;

    #[tokio::test]
    async fn terminal_report_is_only_exposed_after_finish_success() {
        let jobs = JobManager::default();
        let id = {
            let mut inner = jobs.inner.lock().await;
            inner.seq += 1;
            let id = format!("job-{}", inner.seq);
            let (tx, _) = tokio::sync::broadcast::channel(32);
            inner.jobs.insert(
                id.clone(),
                super::JobState {
                    id: id.clone(),
                    progress: ProgressUpdate::default(),
                    report: None,
                    done: false,
                    error: String::new(),
                    tx,
                },
            );
            id
        };

        jobs.publish(
            &id,
            ProgressUpdate {
                stage: "done".to_string(),
                message: "Analysis complete".to_string(),
                percent: 100,
                done: false,
                error: String::new(),
                elapsed_ms: 10,
            },
        )
        .await;

        let mid = jobs.get(&id).await.unwrap();
        assert!(!mid.done);
        assert!(mid.report.is_none());

        jobs.finish_success(
            &id,
            json!({"Optimization": {"Solver": {"Status": "ok"}}}),
            ProgressUpdate {
                stage: "done".to_string(),
                message: "Analysis complete".to_string(),
                percent: 100,
                done: true,
                error: String::new(),
                elapsed_ms: 11,
            },
        )
        .await;

        let final_state = jobs.get(&id).await.unwrap();
        assert!(final_state.done);
        assert!(final_state.report.is_some());
        assert!(final_state.progress.done);
    }
}
