use anyhow::Result;
use ksolver::metrics;
use ksolver::model::{ScenarioConfig, SolveRequest};
use ksolver::server::{app, ServerOptions};
use ksolver::service::Analyzer;
use std::env;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

const DEFAULT_SIMULATOR_URL: &str = "http://127.0.0.1:1212";

#[tokio::main]
async fn main() -> Result<()> {
    // Logs go to stderr so stdout stays clean for the machine-readable JSON reports the subcommands
    // print (`analyze`, `gpu-scenarios --json`, `score-gang-baseline`, `version`). Without this, the
    // default fmt() writer is stdout and interleaves logs with the JSON, breaking `ksolver ... | jq`.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::from_default_env().add_directive("ksolver=debug".parse()?))
        .init();

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("serve") => {
            let addr = args.next().unwrap_or_else(|| "127.0.0.1:8080".to_string());
            let addr: SocketAddr = addr.parse()?;
            metrics::register_metrics();
            let listener = tokio::net::TcpListener::bind(addr).await?;
            let options = ServerOptions {
                default_kubeconfig: std::env::var("KUBECONFIG").unwrap_or_default(),
                default_pricing_file: std::env::var("KSOLVER_PRICING_FILE").unwrap_or_default(),
                default_verification_url: std::env::var("SCHEDULER_SIMULATOR_URL")
                    .or_else(|_| std::env::var("KSOLVER_SCHEDULER_SIMULATOR_URL"))
                    .unwrap_or_else(|_| DEFAULT_SIMULATOR_URL.to_string()),
                metrics_addr: std::env::var("SYSLENS_SOLVER_METRICS_ADDR").unwrap_or_default(),
            };
            info!(
                command = "serve",
                %addr,
                default_kubeconfig = if options.default_kubeconfig.is_empty() {
                    "<empty>"
                } else {
                    options.default_kubeconfig.as_str()
                },
                "solver command starting"
            );
            if !options.metrics_addr.is_empty() {
                let metrics_addr: SocketAddr = options.metrics_addr.parse()?;
                tokio::spawn(async move {
                    match tokio::net::TcpListener::bind(metrics_addr).await {
                        Ok(listener) => {
                            let app = axum::Router::new().route(
                                "/metrics",
                                axum::routing::get(|| async {
                                    (
                                        axum::http::StatusCode::OK,
                                        [(
                                            "content-type",
                                            "text/plain; version=0.0.4; charset=utf-8",
                                        )],
                                        metrics::render_metrics(),
                                    )
                                }),
                            );
                            info!(addr = %metrics_addr, "serving solver metrics");
                            if let Err(err) = axum::serve(listener, app).await {
                                tracing::error!(error = %err, addr = %metrics_addr, "metrics server failed");
                            }
                        }
                        Err(err) => {
                            tracing::error!(error = %err, addr = %metrics_addr, "failed to bind solver metrics address");
                        }
                    }
                });
            }
            let schedule_interval = std::env::var("SOLVER_SCHEDULE_SECONDS")
                .ok()
                .and_then(|v| v.parse::<u64>().ok());
            if let Some(interval_secs) = schedule_interval {
                let analyzer = ksolver::service::Analyzer::new();
                let kubeconfig = options.default_kubeconfig.clone();
                info!(interval_secs, "starting scheduled solve loop");
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
                        info!("scheduled solve starting");
                        let req = SolveRequest {
                            kubeconfig: kubeconfig.clone(),
                            scenario: ScenarioConfig {
                                solver: "cp-sat-rust".to_string(),
                                ignore_unschedulable_workloads: true,
                                ..Default::default()
                            },
                            ..Default::default()
                        };
                        match analyzer.analyze(req).await {
                            Ok(report) => {
                                let savings = report
                                    .optimization
                                    .as_ref()
                                    .map(|o| o.savings_monthly.monthly)
                                    .unwrap_or(0.0);
                                info!(savings_monthly = savings, "scheduled solve complete");
                            }
                            Err(err) => {
                                tracing::error!(error = %err, "scheduled solve failed");
                            }
                        }
                    }
                });
            }
            info!(%addr, "serving rust syslens-solver");
            axum::serve(listener, app(options)).await?;
        }
        Some("analyze") => {
            let mut kubeconfig = env::var("KUBECONFIG").unwrap_or_default();
            let mut snapshot_file = String::new();
            let mut cluster_name = "default".to_string();
            let mut remaining = args;
            while let Some(arg) = remaining.next() {
                match arg.as_str() {
                    "--snapshot" => {
                        snapshot_file = remaining.next().unwrap_or_default();
                    }
                    "--cluster" => {
                        cluster_name = remaining.next().unwrap_or_else(|| "default".to_string());
                    }
                    "--kubeconfig" => {
                        kubeconfig = remaining.next().unwrap_or_default();
                    }
                    other => {
                        eprintln!("unknown analyze flag: {other}");
                    }
                }
            }
            let req = SolveRequest {
                kubeconfig,
                pricing_file: String::new(),
                snapshot_file,
                cluster_name,
                scenario_name: String::new(),
                scenario: ScenarioConfig {
                    solver: "cp-sat-rust".to_string(),
                    ..Default::default()
                },
            };
            info!(
                command = "analyze",
                kubeconfig = if req.kubeconfig.is_empty() {
                    "<empty>"
                } else {
                    req.kubeconfig.as_str()
                },
                snapshot_file = if req.snapshot_file.is_empty() {
                    "<none>"
                } else {
                    req.snapshot_file.as_str()
                },
                "solver command starting"
            );
            let report = Analyzer::new().analyze(req).await?;
            serde_json::to_writer_pretty(std::io::stdout(), &report)?;
            println!();
        }
        Some("shadow") => {
            metrics::register_metrics();
            let cfg = ksolver::scheduler::config::ShadowConfig::from_env();
            let binding_status = if cfg.binding_kill_switch {
                "disabled by kill switch"
            } else if !cfg.enable_real_binding {
                "observe-only"
            } else if cfg.real_binding_dry_run {
                "dry-run validation"
            } else {
                match cfg.binding_canary_mode {
                    ksolver::scheduler::config::BindingCanaryMode::LowRisk => {
                        "live low-risk canary"
                    }
                    ksolver::scheduler::config::BindingCanaryMode::All => "live bind-all",
                }
            };
            info!(
                scheduler_name = %cfg.scheduler_name,
                batch_seconds = cfg.batch_window.as_secs(),
                http_addr = %cfg.http_addr,
                namespaces = ?cfg.namespace_allowlist,
                binding_rollout_mode = ?cfg.binding_rollout_mode,
                enable_real_binding = cfg.enable_real_binding,
                real_binding_dry_run = cfg.real_binding_dry_run,
                binding_kill_switch = cfg.binding_kill_switch,
                binding_status,
                "starting shadow-mode GPU scheduler"
            );
            ksolver::scheduler::shadow::run_shadow(cfg).await?;
        }
        Some("bench") => {
            use ksolver::scheduler::bench;
            let rest: Vec<String> = args.collect();
            let flag = |name: &str| -> Option<String> {
                rest.iter()
                    .position(|a| a == name)
                    .and_then(|i| rest.get(i + 1).cloned())
            };
            let results = match (flag("--jobs"), flag("--nodes")) {
                (Some(jobs), Some(nodes)) => {
                    let jobs = jobs.parse::<usize>()?;
                    let nodes = nodes.parse::<usize>()?;
                    let candidate_node_limit = flag("--candidate-nodes")
                        .or_else(|| std::env::var("KSOLVER_CANDIDATE_NODE_LIMIT").ok())
                        .and_then(|v| v.parse::<usize>().ok())
                        .unwrap_or(0);
                    bench::run_matrix(&[bench::custom_scenario(
                        "custom",
                        nodes,
                        jobs,
                        candidate_node_limit,
                    )])
                }
                _ => bench::run_matrix(&bench::default_matrix()),
            };
            bench::print_table(&results);
        }
        Some("gpu-scenarios") => {
            let rest: Vec<String> = args.collect();
            let flag = |name: &str| -> Option<String> {
                rest.iter()
                    .position(|a| a == name)
                    .and_then(|i| rest.get(i + 1).cloned())
            };
            let simulator_url = flag("--simulator")
                .or_else(|| std::env::var("KSOLVER_SCHEDULER_SIMULATOR_URL").ok())
                .or_else(|| std::env::var("SCHEDULER_SIMULATOR_URL").ok());
            let simulator_urls = flag("--simulator-pool")
                .or_else(|| std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_POOL").ok())
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|url| !url.is_empty())
                        .map(str::to_string)
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let simulator_cache_path = flag("--simulator-cache")
                .or_else(|| std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE").ok())
                .map(std::path::PathBuf::from);
            let simulator_cache_dir = flag("--simulator-cache-dir")
                .or_else(|| std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_CACHE_DIR").ok())
                .map(std::path::PathBuf::from);
            let refresh_simulator_cache = rest.iter().any(|a| a == "--refresh-simulator-cache");
            let simulator_batch_timeout = flag("--simulator-timeout-ms")
                .or_else(|| std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_TIMEOUT_MS").ok())
                .and_then(|v| v.parse::<u64>().ok())
                .map(std::time::Duration::from_millis)
                .unwrap_or_else(|| {
                    ksolver::scheduler::gpu_scenarios::BenchmarkOptions::default()
                        .simulator_batch_timeout
                });
            let simulator_progress =
                refresh_simulator_cache || rest.iter().any(|a| a == "--simulator-progress");
            let simulator_max_live_baselines = match flag("--simulator-max-live-baselines")
                .or_else(|| std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_MAX_LIVE_BASELINES").ok())
            {
                Some(v)
                    if matches!(v.trim().to_ascii_lowercase().as_str(), "all" | "unlimited") =>
                {
                    None
                }
                Some(v) if v.trim().eq_ignore_ascii_case("none") => Some(0),
                Some(v) => v.parse::<usize>().ok(),
                None => {
                    Some(ksolver::scheduler::gpu_scenarios::DEFAULT_SIMULATOR_LIVE_BASELINE_LIMIT)
                }
            };
            let simulator_live_scenarios = flag("--simulator-live-scenarios")
                .or_else(|| std::env::var("KSOLVER_GPU_SCENARIO_SIMULATOR_LIVE_SCENARIOS").ok())
                .map(|value| {
                    value
                        .split(',')
                        .map(str::trim)
                        .filter(|name| !name.is_empty())
                        .map(str::to_string)
                        .collect::<std::collections::BTreeSet<_>>()
                })
                .filter(|scenarios| !scenarios.is_empty());
            // Optional gang-aware (Volcano) baseline: a JSON map {scenario_name: volcano_safe_useful_gpu}
            // captured offline by scripts/volcano-baseline-run.sh. Feeds classify_win's gang_aware arg
            // so wins can be classified beats-gang-aware.
            let volcano_baseline_useful_gpu = flag("--volcano-baseline")
                .and_then(|p| std::fs::read_to_string(&p).ok())
                .and_then(|s| {
                    serde_json::from_str::<std::collections::BTreeMap<String, i64>>(&s).ok()
                })
                .unwrap_or_default();
            let json = rest.iter().any(|a| a == "--json");
            let options = ksolver::scheduler::gpu_scenarios::BenchmarkOptions {
                simulator_url: simulator_url.clone(),
                simulator_urls: simulator_urls.clone(),
                simulator_cache_path: simulator_cache_path.clone(),
                simulator_cache_dir: simulator_cache_dir.clone(),
                refresh_simulator_cache,
                simulator_batch_timeout,
                simulator_progress,
                simulator_max_live_baselines,
                simulator_live_scenarios,
                volcano_baseline_useful_gpu,
            };
            if rest.iter().any(|a| a == "--refresh-simulator-cache-only") {
                let refreshed =
                    ksolver::scheduler::gpu_scenarios::refresh_simulator_cache_only(options)
                        .await?;
                if json {
                    serde_json::to_writer_pretty(
                        std::io::stdout(),
                        &serde_json::json!({
                            "ok": true,
                            "refreshed_baselines": refreshed,
                        }),
                    )?;
                    println!();
                } else {
                    println!("refreshed {refreshed} kube-scheduler-simulator baseline(s)");
                }
                return Ok(());
            }
            let report =
                ksolver::scheduler::gpu_scenarios::run_benchmark_with_options(options).await?;
            if json {
                serde_json::to_writer_pretty(std::io::stdout(), &report)?;
                println!();
            } else {
                if simulator_url
                    .as_deref()
                    .unwrap_or_default()
                    .trim()
                    .is_empty()
                {
                    println!(
                        "no kube-scheduler-simulator URL configured; using cached kube-scheduler-simulator baselines only"
                    );
                }
                if let Some(path) = simulator_cache_path {
                    println!(
                        "kube-scheduler-simulator cache: {}{}",
                        path.display(),
                        if refresh_simulator_cache {
                            " (refreshed)"
                        } else {
                            ""
                        }
                    );
                }
                if let Some(path) = simulator_cache_dir {
                    println!(
                        "kube-scheduler-simulator cache dir: {}{}",
                        path.display(),
                        if refresh_simulator_cache {
                            " (refreshed)"
                        } else {
                            ""
                        }
                    );
                }
                if !simulator_urls.is_empty() {
                    println!(
                        "kube-scheduler-simulator pool: {}",
                        simulator_urls.join(",")
                    );
                }
                println!(
                    "kube-scheduler-simulator batch timeout: {} ms",
                    report.simulator_batch_timeout_millis
                );
                if let Some(limit) = report.simulator_live_baseline_limit {
                    println!("kube-scheduler-simulator live baseline limit: {limit}");
                }
                if let Some(scenarios) = &report.simulator_live_scenarios {
                    println!(
                        "kube-scheduler-simulator live scenarios: {}",
                        scenarios.join(",")
                    );
                }
                ksolver::scheduler::gpu_scenarios::print_table(&report);
            }
        }
        Some("conform") => {
            // Parse simple flags from the remaining args.
            let rest: Vec<String> = args.collect();
            let json = rest.iter().any(|a| a == "--json");
            let fail_on_strict_false_positive =
                rest.iter().any(|a| a == "--fail-on-strict-false-positive");
            let flag = |name: &str| -> Option<String> {
                rest.iter()
                    .position(|a| a == name)
                    .and_then(|i| rest.get(i + 1).cloned())
            };
            let simulator_url = flag("--simulator")
                .or_else(|| std::env::var("KSOLVER_SCHEDULER_SIMULATOR_URL").ok())
                .or_else(|| std::env::var("SCHEDULER_SIMULATOR_URL").ok())
                .unwrap_or_default();
            if simulator_url.trim().is_empty() {
                if json {
                    serde_json::to_writer_pretty(
                        std::io::stdout(),
                        &serde_json::json!({
                            "skipped": true,
                            "reason": "no kube-scheduler-simulator URL configured",
                            "configure": "set --simulator <url> or KSOLVER_SCHEDULER_SIMULATOR_URL"
                        }),
                    )?;
                    println!();
                } else {
                    println!(
                        "conformance skipped: no kube-scheduler-simulator URL configured (set --simulator <url> or KSOLVER_SCHEDULER_SIMULATOR_URL)"
                    );
                }
                return Ok(());
            }
            let sample: usize = flag("--sample").and_then(|v| v.parse().ok()).unwrap_or(20);
            let cluster = flag("--cluster")
                .or_else(|| std::env::var("KSOLVER_CLUSTER_NAME").ok())
                .unwrap_or_else(|| "default".to_string());
            let kubeconfig = flag("--kubeconfig")
                .or_else(|| std::env::var("KUBECONFIG").ok())
                .unwrap_or_default();
            info!(
                command = "conform",
                %cluster,
                sample,
                "running feasibility conformance vs kube-scheduler-simulator (read-only)"
            );
            let report = ksolver::conformance::run_conformance(
                &kubeconfig,
                &cluster,
                simulator_url.trim(),
                sample,
            )
            .await?;
            if json {
                let mut value = serde_json::to_value(&report)?;
                if let Some(obj) = value.as_object_mut() {
                    obj.insert(
                        "strict_gate_status".to_string(),
                        serde_json::Value::String(report.strict_gate_status().to_string()),
                    );
                }
                serde_json::to_writer_pretty(std::io::stdout(), &value)?;
                println!();
            } else {
                print!("{}", report.render());
            }
            if fail_on_strict_false_positive && report.has_strict_false_positives() {
                anyhow::bail!(
                    "conformance failed: {} strict false positives",
                    report.strict.false_positive
                );
            }
        }
        Some("dump-scenarios") => {
            // Emit the deterministic scenario library (node topology + gang jobs) as JSON, for an
            // external gang-aware baseline harness (Volcano) to reproduce scenarios faithfully.
            let lib = ksolver::scheduler::gpu_scenarios::dump_scenario_library();
            serde_json::to_writer_pretty(std::io::stdout(), &lib)?;
            println!();
        }
        Some("score-gang-baseline") => {
            // Read a gang-aware baseline's placements (from the Volcano harness) as JSON on stdin and
            // score VRAM-safe useful GPU the same way ksolver counts it (see the volcano baseline
            // spec). Keeps beats-gang-aware honest: unsafe Volcano placements don't count.
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf)?;
            let input: serde_json::Value = serde_json::from_str(&buf)?;
            let scored = ksolver::scheduler::gpu_scenarios::score_gang_baseline(&input);
            serde_json::to_writer_pretty(std::io::stdout(), &scored)?;
            println!();
        }
        Some("version") => {
            println!("syslens-solver rust dev");
        }
        _ => {
            println!(
                "syslens-solver rust\n\nUsage:\n  syslens-solver serve [addr]\n  syslens-solver analyze [--snapshot <path>] [--cluster <name>] [--kubeconfig <path>]\n  syslens-solver shadow\n  syslens-solver bench\n  syslens-solver gpu-scenarios [--simulator <url>] [--simulator-pool <url[,url...]>] [--simulator-cache <path>] [--simulator-cache-dir <dir>] [--refresh-simulator-cache] [--refresh-simulator-cache-only] [--simulator-timeout-ms <ms>] [--simulator-max-live-baselines <n|all>] [--simulator-live-scenarios <name[,name...]>] [--simulator-progress] [--volcano-baseline <cache.json>] [--json]\n  syslens-solver conform [--simulator <url>] [--sample <n>] [--cluster <name>] [--kubeconfig <path>] [--json] [--fail-on-strict-false-positive]\n  syslens-solver dump-scenarios\n  syslens-solver score-gang-baseline  (reads placements JSON on stdin)\n  syslens-solver version"
            );
        }
    }
    Ok(())
}
