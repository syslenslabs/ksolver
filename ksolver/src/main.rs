use anyhow::Result;
use ksolver::metrics;
use ksolver::model::{ScenarioConfig, SolveRequest};
use ksolver::server::{app, ServerOptions};
use ksolver::service::Analyzer;
use std::env;
use std::net::SocketAddr;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
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
                    .unwrap_or_default(),
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
        Some("version") => {
            println!("syslens-solver rust dev");
        }
        _ => {
            println!(
                "syslens-solver rust\n\nUsage:\n  syslens-solver serve [addr]\n  syslens-solver analyze [--snapshot <path>] [--cluster <name>] [--kubeconfig <path>]\n  syslens-solver version"
            );
        }
    }
    Ok(())
}
