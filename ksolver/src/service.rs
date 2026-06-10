use crate::collector::KubeCollector;
use crate::cpsat_rust;
use crate::explainability;
use crate::historical_usage::{overlay_historical_pod_usage, resolve_historical_usage_config};
use crate::metrics::{self, SolveMetricLabels};
use crate::model::{
    AnalysisReport, ConstraintCostRow, ConstraintCostTable, Money, ProgressUpdate, SolveRequest,
    SolverInfo,
};
use crate::normalizer::{Normalizer, Options as NormalizerOptions};
use crate::optimizer_input::{
    baseline_solution, build_input, build_input_strict, diagnose_current_assignment,
    inject_candidate_nodes, summarize_current_assignment,
};
use crate::planner::Planner;
use crate::pricing::{resolve_pricing_catalog, workspace_pricing_path, write_pricing_catalog};
use crate::state_cache::StateCache;
use crate::verifier::Verifier;
use anyhow::{bail, Result};
use std::time::Instant;
use tracing::{debug, info, warn};

pub type ProgressFn = dyn Fn(ProgressUpdate) + Send + Sync + 'static;

#[derive(Debug, Default, Clone)]
pub struct Analyzer;

impl Analyzer {
    pub fn new() -> Self {
        Self
    }

    pub async fn analyze(&self, req: SolveRequest) -> Result<AnalysisReport> {
        self.analyze_with_progress(req, None).await
    }

    pub async fn analyze_with_progress(
        &self,
        req: SolveRequest,
        progress: Option<Box<ProgressFn>>,
    ) -> Result<AnalysisReport> {
        let start = Instant::now();
        let cluster_name = if req.cluster_name.is_empty() {
            "default".to_string()
        } else {
            req.cluster_name.clone()
        };
        let metric_labels = SolveMetricLabels::from_request(&cluster_name, &req);
        metrics::solve_started(&metric_labels);
        info!(
            cluster = if req.cluster_name.is_empty() {
                "default"
            } else {
                req.cluster_name.as_str()
            },
            solver = if req.scenario.solver.is_empty() {
                "cp-sat-rust"
            } else {
                req.scenario.solver.as_str()
            },
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
            "analysis starting"
        );
        emit_progress(
            &progress,
            &start,
            "config",
            "Preparing analysis request",
            5,
            false,
            "",
        );
        debug!(cluster = %cluster_name, "analysis request normalized");
        let req_with_cluster = SolveRequest {
            cluster_name: cluster_name.clone(),
            ..req.clone()
        };
        let result = self
            .analyze_inner(req, req_with_cluster, cluster_name, start, progress)
            .await;
        let status = if result.is_ok() { "success" } else { "error" };
        metrics::solve_finished(&metric_labels, status, start.elapsed().as_secs_f64());
        result
    }

    async fn analyze_inner(
        &self,
        req: SolveRequest,
        req_with_cluster: SolveRequest,
        cluster_name: String,
        start: Instant,
        progress: Option<Box<ProgressFn>>,
    ) -> Result<AnalysisReport> {
        let effective_scenario = apply_usage_risk_preset(&req.scenario);
        let cache = StateCache::for_request(&req_with_cluster);
        let historical_usage =
            resolve_historical_usage_config(&req.kubeconfig, &effective_scenario).await?;
        let bypass_temporal_caches = effective_scenario.use_usage_adjusted_requests
            && (req.snapshot_file.is_empty() || historical_usage.is_some());
        if let Some(historical) = historical_usage.as_ref() {
            info!(
                provider = %historical.provider,
                source = %historical.source,
                lookback = %historical.lookback,
                step = %historical.step,
                prometheus_url = if historical.prometheus_url.is_empty() {
                    "<empty>"
                } else {
                    historical.prometheus_url.as_str()
                },
                prometheus_username = if historical.prometheus_username.is_empty() {
                    "<empty>"
                } else {
                    historical.prometheus_username.as_str()
                },
                token_present = !historical.prometheus_token.is_empty(),
                "historical usage configuration resolved"
            );
        }
        if let Err(err) = cache.write_request(&req).await {
            warn!(error = %err, path = %cache.root().display(), "failed to persist solve request");
        }

        emit_progress(
            &progress,
            &start,
            "collect",
            "Collecting cluster state",
            20,
            false,
            "",
        );
        let mut snapshot = if !req.snapshot_file.is_empty() {
            let snapshot = cache.load_snapshot_from_file(&req.snapshot_file).await?;
            info!(
                path = %req.snapshot_file,
                nodes = snapshot.nodes.len(),
                pods = snapshot.pods.len(),
                "using explicit snapshot file"
            );
            snapshot
        } else if !bypass_temporal_caches {
            if let Some(snapshot) = cache.load_snapshot_if_fresh().await? {
                info!(
                    path = %cache.root().join("snapshot.json").display(),
                    nodes = snapshot.nodes.len(),
                    pods = snapshot.pods.len(),
                    "using cached cluster snapshot"
                );
                snapshot
            } else {
                let collector =
                    KubeCollector::new(cluster_name.clone(), req.kubeconfig.clone()).await?;
                let snapshot = collector.collect().await?;
                if let Err(err) = cache.write_snapshot(&snapshot).await {
                    warn!(error = %err, path = %cache.root().display(), "failed to persist cluster snapshot");
                } else {
                    info!(
                        path = %cache.root().join("snapshot.json").display(),
                        "cluster snapshot cached"
                    );
                }
                snapshot
            }
        } else {
            info!(
                "usage-adjusted requests enabled; bypassing cached snapshot for live recollection"
            );
            let collector =
                KubeCollector::new(cluster_name.clone(), req.kubeconfig.clone()).await?;
            collector.collect().await?
        };
        let has_usage = snapshot
            .pods
            .iter()
            .any(|p| p.usage.cpu_usage_milli > 0 || p.usage.memory_bytes > 0);
        if !has_usage && req.snapshot_file.is_empty() {
            info!("no usage data in snapshot; attempting metrics refresh");
            if let Ok(collector) =
                KubeCollector::new(cluster_name.clone(), req.kubeconfig.clone()).await
            {
                if collector.refresh_usage(&mut snapshot).await {
                    if let Err(err) = cache.write_snapshot(&snapshot).await {
                        warn!(error = %err, "failed to update cached snapshot with usage data");
                    }
                }
            }
        }
        if let Some(historical) = historical_usage.as_ref() {
            emit_progress(
                &progress,
                &start,
                "collect-history",
                "Loading historical usage from Prometheus",
                32,
                false,
                "",
            );
            match overlay_historical_pod_usage(&mut snapshot, historical).await {
                Ok(updated) => {
                    info!(
                        updated_pods = updated,
                        source = %historical.source,
                        "historical pod usage overlaid onto snapshot"
                    );
                }
                Err(err) => {
                    warn!(error = %err, source = %historical.source, "failed to overlay historical usage; continuing with live metrics usage");
                    snapshot.warnings.push(format!(
                        "failed to load historical usage from {}: {}",
                        historical.source, err
                    ));
                }
            }
        }
        info!(
            nodes = snapshot.nodes.len(),
            pods = snapshot.pods.len(),
            volumes = snapshot.volumes.len(),
            storage_classes = snapshot.storage_classes.len(),
            daemonsets = snapshot.daemon_sets.len(),
            pdbs = snapshot.pdbs.len(),
            warnings = snapshot.warnings.len(),
            "cluster collection complete"
        );

        emit_progress(
            &progress,
            &start,
            "pricing",
            "Resolving node pricing",
            45,
            false,
            "",
        );
        let pricing_catalog = resolve_pricing_catalog(&req.pricing_file, &snapshot).await?;
        info!(
            instance_types = pricing_catalog.instance_types.len(),
            source = if req.pricing_file.is_empty() {
                "auto"
            } else {
                "file"
            },
            "pricing resolved"
        );
        if req.pricing_file.is_empty() {
            if let Ok(path) = workspace_pricing_path(&cluster_name) {
                let _ = write_pricing_catalog(&path, &pricing_catalog);
                debug!(path = %path.display(), "pricing cache written");
            }
        }

        let normalizer_options = NormalizerOptions {
            cpu_headroom_percent: effective_scenario.cpu_headroom_percent,
            memory_headroom_percent: effective_scenario.memory_headroom_percent,
            storage_headroom_percent: effective_scenario.storage_headroom_percent,
            pods_headroom: effective_scenario.pods_headroom,
            disallowed_pools: effective_scenario.disallowed_pools.clone(),
            node_pool_label_keys: effective_scenario.node_pool_label_keys.clone(),
            ignore_taints: effective_scenario.ignore_taints,
            relax_preferred_affinity: effective_scenario.relax_preferred_affinity,
            relax_required_anti_affinity: effective_scenario.relax_required_anti_affinity,
            cpu_overcommit_ratio: effective_scenario.cpu_overcommit_ratio,
            memory_overcommit_ratio: effective_scenario.memory_overcommit_ratio,
            use_usage_adjusted_requests: effective_scenario.use_usage_adjusted_requests,
            usage_request_floor_ratio: effective_scenario.usage_request_floor_ratio,
            cpu_usage_safety_factor: effective_scenario.cpu_usage_safety_factor,
            memory_usage_safety_factor: effective_scenario.memory_usage_safety_factor,
            max_memory_overflow_probability_percent: effective_scenario
                .max_memory_overflow_probability_percent,
        };
        let snapshot_hash = cache.snapshot_hash(&snapshot)?;
        let pricing_hash = cache.pricing_hash(&pricing_catalog)?;
        let normalization_key =
            cache.normalization_key(&snapshot_hash, &pricing_hash, &normalizer_options)?;

        emit_progress(
            &progress,
            &start,
            "normalize",
            "Normalizing cluster state",
            65,
            false,
            "",
        );
        let normalized = if !bypass_temporal_caches {
            if let Some(normalized) = cache.load_normalized(&normalization_key).await? {
                info!(
                    key = %normalization_key,
                    nodes = normalized.nodes.len(),
                    workloads = normalized.workloads.len(),
                    "using cached normalized cluster"
                );
                normalized
            } else {
                let normalized =
                    Normalizer::new(pricing_catalog.clone(), normalizer_options).normalize(&snapshot);
                if let Err(err) = cache
                    .write_normalized(&normalization_key, &normalized)
                    .await
                {
                    warn!(error = %err, key = %normalization_key, "failed to persist normalized cluster cache");
                } else {
                    debug!(key = %normalization_key, "normalized cluster cached");
                }
                normalized
            }
        } else {
            info!("usage-adjusted requests enabled; bypassing cached normalized cluster");
            Normalizer::new(pricing_catalog.clone(), normalizer_options).normalize(&snapshot)
        };
        info!(
            nodes = normalized.nodes.len(),
            workloads = normalized.workloads.len(),
            blockers = normalized.blockers.len(),
            warnings = normalized.warnings.len(),
            "normalization complete"
        );

        emit_progress(
            &progress,
            &start,
            "plan",
            "Preparing optimization report",
            85,
            false,
            "",
        );
        let grouped_input_key = cache.optimization_input_key(
            &normalization_key,
            effective_scenario.ignore_unschedulable_workloads,
            true,
            true,
        )?;
        let input = if !bypass_temporal_caches {
            if let Some(input) = cache.load_optimization_input(&grouped_input_key).await? {
                info!(
                    key = %grouped_input_key,
                    node_groups = input.nodes.len(),
                    workload_groups = input.workloads.len(),
                    "using cached optimization input"
                );
                input
            } else {
                let input = build_input(
                    &normalized,
                    effective_scenario.ignore_unschedulable_workloads,
                );
                if let Err(err) = cache
                    .write_optimization_input(&grouped_input_key, &input)
                    .await
                {
                    warn!(error = %err, key = %grouped_input_key, "failed to persist optimization input cache");
                } else {
                    debug!(key = %grouped_input_key, "optimization input cached");
                }
                input
            }
        } else {
            info!("usage-adjusted requests enabled; bypassing cached optimization input");
            build_input(
                &normalized,
                effective_scenario.ignore_unschedulable_workloads,
            )
        };
        let mut input = input;
        let mut candidates = effective_scenario.candidate_instance_types.clone();
        if candidates.is_empty() && !normalized.nodes.is_empty() {
            let catalog_for_suggest =
                crate::pricing::load_pricing_catalog(&req.pricing_file).unwrap_or_default();
            candidates =
                crate::pricing::suggest_candidate_instance_types(&catalog_for_suggest, &snapshot);
        }
        if !candidates.is_empty() {
            inject_candidate_nodes(&mut input, &candidates);
            info!(
                candidates = candidates.len(),
                "injected candidate instance types into optimization input"
            );
        }
        let input_diagnostics = diagnose_current_assignment(&input);
        let input_summary = summarize_current_assignment(&input);
        let input_diagnostic_text = if input_diagnostics.is_empty() {
            "<none>".to_string()
        } else {
            input_diagnostics.join(" | ")
        };
        let solver_name = normalize_solver_name(&effective_scenario.solver);
        info!(
            node_groups = input.nodes.len(),
            workload_groups = input.workloads.len(),
            ignore_unschedulable_workloads = effective_scenario.ignore_unschedulable_workloads,
            requested_solver = %effective_scenario.solver,
            normalized_solver = %solver_name,
            baseline_summary = %input_summary,
            baseline_diagnostics = %input_diagnostic_text,
            "optimization input built"
        );
        let (plan_input, solution, solver_info) = match solver_name.as_str() {
            "cp-sat-rust" => {
                let info = cpsat_rust::solver_info();
                info!(
                    available = info.available,
                    status = %info.status,
                    "cp-sat-rust backend checked"
                );
                if !info.available {
                    bail!(
                        "requested solver {solver_name} is unavailable: {}",
                        info.status
                    );
                }
                match cpsat_rust::solve(&input, &effective_scenario) {
                    Ok((solution, info)) => {
                        info!(
                            active_groups = solution.active_nodes.len(),
                            assignments = solution.assignments.len(),
                            grouped_assignments = solution.assignment_counts.len(),
                            status = %info.status,
                            "cp-sat-rust solve succeeded"
                        );
                        (input.clone(), solution, info)
                    }
                    Err(err) => {
                        let diagnostics = diagnose_current_assignment(&input);
                        let summary = summarize_current_assignment(&input);
                        let grouped_diagnostics = if diagnostics.is_empty() {
                            "<none>".to_string()
                        } else {
                            diagnostics.join(" | ")
                        };
                        warn!(
                            error = %err,
                            summary = %summary,
                            diagnostics = grouped_diagnostics.as_str(),
                            "cp-sat-rust solve failed"
                        );
                        if normalized.blockers.is_empty()
                            && input.workloads.len() < normalized.workloads.len()
                        {
                            let strict_input_key = cache.optimization_input_key(
                                &normalization_key,
                                effective_scenario.ignore_unschedulable_workloads,
                                false,
                                false,
                            )?;
                            let strict_input = if !bypass_temporal_caches {
                                if let Some(input) =
                                    cache.load_optimization_input(&strict_input_key).await?
                                {
                                    info!(
                                        key = %strict_input_key,
                                        node_groups = input.nodes.len(),
                                        workload_groups = input.workloads.len(),
                                        "using cached strict optimization input"
                                    );
                                    input
                                } else {
                                    let input = build_input_strict(
                                        &normalized,
                                        effective_scenario.ignore_unschedulable_workloads,
                                    );
                                    if let Err(err) = cache
                                        .write_optimization_input(&strict_input_key, &input)
                                        .await
                                    {
                                        warn!(error = %err, key = %strict_input_key, "failed to persist strict optimization input cache");
                                    } else {
                                        debug!(key = %strict_input_key, "strict optimization input cached");
                                    }
                                    input
                                }
                            } else {
                                info!(
                                    "usage-adjusted requests enabled; bypassing cached strict optimization input"
                                );
                                build_input_strict(
                                    &normalized,
                                    effective_scenario.ignore_unschedulable_workloads,
                                )
                            };
                            info!(
                                node_groups = strict_input.nodes.len(),
                                workload_groups = strict_input.workloads.len(),
                                "retrying cp-sat-rust with strict ungrouped optimization input"
                            );
                            match cpsat_rust::solve(&strict_input, &effective_scenario) {
                                Ok((solution, retry_info)) => {
                                    info!(
                                        active_groups = solution.active_nodes.len(),
                                        assignments = solution.assignments.len(),
                                        grouped_assignments = solution.assignment_counts.len(),
                                        status = %retry_info.status,
                                        "cp-sat-rust strict retry succeeded"
                                    );
                                    let retry_status = format!(
                                        "{}; grouped model infeasible, strict ungrouped retry succeeded",
                                        retry_info.status
                                    );
                                    return self
                                        .finish_report(
                                            req,
                                            progress,
                                            start,
                                            cache,
                                            snapshot,
                                            normalized,
                                            strict_input,
                                            solution,
                                            SolverInfo {
                                                name: retry_info.name,
                                                available: retry_info.available,
                                                status: retry_status,
                                            },
                                            pricing_catalog,
                                        )
                                        .await;
                                }
                                Err(retry_err) => {
                                    let strict_diagnostics =
                                        diagnose_current_assignment(&strict_input);
                                    let strict_summary =
                                        summarize_current_assignment(&strict_input);
                                    let strict_diagnostic_text = if strict_diagnostics.is_empty() {
                                        "<none>".to_string()
                                    } else {
                                        strict_diagnostics.join(" | ")
                                    };
                                    warn!(
                                        error = %retry_err,
                                        summary = %strict_summary,
                                        diagnostics = strict_diagnostic_text.as_str(),
                                        "cp-sat-rust strict retry failed"
                                    );
                                    if is_infeasible_error(&retry_err) {
                                        bail!(
                                            "cp-sat-rust strict retry infeasible after grouped model failure: {}; summary: {}; diagnostics: {}",
                                            retry_err,
                                            strict_summary,
                                            strict_diagnostic_text
                                        );
                                    }
                                }
                            }
                        }
                        if is_infeasible_error(&err) {
                            bail!(
                                "cp-sat-rust grouped model infeasible: {}; summary: {}; diagnostics: {}",
                                err,
                                summary,
                                grouped_diagnostics
                            );
                        }
                        let diagnostic_text = if diagnostics.is_empty() {
                            "current grouped assignment satisfies all modeled constraints"
                                .to_string()
                        } else {
                            format!("diagnostics: {}", diagnostics.join(" | "))
                        };
                        (
                            input.clone(),
                            baseline_solution(&input),
                            SolverInfo {
                                name: "cp-sat-rust".to_string(),
                                available: cpsat_rust::solver_info().available,
                                status: format!(
                                    "{}; falling back to baseline grouped plan: {err}; {diagnostic_text}",
                                    cpsat_rust::solver_info().status
                                ),
                            },
                        )
                    }
                }
            }
            _ => {
                warn!(solver = %solver_name, "unsupported solver requested, falling back to baseline");
                (
                    input.clone(),
                    baseline_solution(&input),
                    SolverInfo {
                        name: solver_name,
                        available: false,
                        status: "solver backend not ported yet; showing baseline grouped plan"
                            .to_string(),
                    },
                )
            }
        };
        let mut plan = Planner::new().build_plan(&normalized, &plan_input, &solution, solver_info);
        plan.verification = Verifier::new()
            .verify(
                &req.kubeconfig,
                &effective_scenario,
                &normalized,
                &plan_input,
                &plan,
            )
            .await;
        info!(
            current_monthly = plan.current_monthly_cost.monthly,
            optimized_monthly = plan.optimized_monthly_cost.monthly,
            savings_monthly = plan.savings_monthly.monthly,
            moves = plan.recommended_moves.len(),
            verification_backend = %plan.verification.backend,
            verification_confidence = %plan.verification.confidence,
            verified_moves = plan.verification.verified_moves,
            rejected_moves = plan.verification.rejected_moves,
            "optimization plan complete"
        );

        let constraint_cost_table = compute_constraint_costs(
            &progress,
            &start,
            &snapshot,
            &pricing_catalog,
            &effective_scenario,
            &plan,
        );

        emit_progress(
            &progress,
            &start,
            "done",
            "Analysis complete",
            100,
            true,
            "",
        );
        info!(
            elapsed_ms = start.elapsed().as_millis(),
            "analysis complete"
        );
        let mut report = AnalysisReport {
            snapshot,
            normalized,
            optimization: Some(plan),
            explainability: Default::default(),
        };
        explainability::populate_report(&mut report);
        report.explainability.constraint_cost_table = constraint_cost_table;
        apply_memory_risk_gate(&mut report, &effective_scenario);
        if let Err(err) = cache.write_report(&report).await {
            warn!(error = %err, path = %cache.root().display(), "failed to persist analysis report");
        }
        if let Err(err) = cache.append_history(&report).await {
            warn!(error = %err, "failed to append history entry");
        }
        if !req.scenario_name.is_empty() {
            if let Err(err) = cache.write_scenario_request(&req.scenario_name, &req).await {
                warn!(error = %err, scenario = %req.scenario_name, "failed to persist scenario request");
            }
            if let Err(err) = cache
                .write_scenario_report(&req.scenario_name, &report)
                .await
            {
                warn!(error = %err, scenario = %req.scenario_name, "failed to persist scenario report");
            }
        }
        Ok(report)
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_report(
        &self,
        req: SolveRequest,
        progress: Option<Box<ProgressFn>>,
        start: Instant,
        cache: StateCache,
        snapshot: crate::model::ClusterSnapshot,
        normalized: crate::model::NormalizedCluster,
        input: crate::model::OptimizationInput,
        solution: crate::model::OptimizationSolution,
        solver_info: SolverInfo,
        pricing_catalog: crate::pricing::PricingCatalog,
    ) -> Result<AnalysisReport> {
        let effective_scenario = apply_usage_risk_preset(&req.scenario);
        let mut plan = Planner::new().build_plan(&normalized, &input, &solution, solver_info);
        plan.verification = Verifier::new()
            .verify(
                &req.kubeconfig,
                &effective_scenario,
                &normalized,
                &input,
                &plan,
            )
            .await;
        info!(
            current_monthly = plan.current_monthly_cost.monthly,
            optimized_monthly = plan.optimized_monthly_cost.monthly,
            savings_monthly = plan.savings_monthly.monthly,
            moves = plan.recommended_moves.len(),
            verification_backend = %plan.verification.backend,
            verification_confidence = %plan.verification.confidence,
            verified_moves = plan.verification.verified_moves,
            rejected_moves = plan.verification.rejected_moves,
            "optimization plan complete"
        );

        let constraint_cost_table = compute_constraint_costs(
            &progress,
            &start,
            &snapshot,
            &pricing_catalog,
            &effective_scenario,
            &plan,
        );

        emit_progress(
            &progress,
            &start,
            "done",
            "Analysis complete",
            100,
            true,
            "",
        );
        info!(
            elapsed_ms = start.elapsed().as_millis() as i64,
            "analysis complete"
        );

        let mut report = AnalysisReport {
            snapshot,
            normalized,
            optimization: Some(plan),
            explainability: Default::default(),
        };
        explainability::populate_report(&mut report);
        report.explainability.constraint_cost_table = constraint_cost_table;
        apply_memory_risk_gate(&mut report, &effective_scenario);
        if let Err(err) = cache.write_report(&report).await {
            warn!(error = %err, path = %cache.root().display(), "failed to persist analysis report");
        }
        if !req.scenario_name.is_empty() {
            if let Err(err) = cache.write_scenario_request(&req.scenario_name, &req).await {
                warn!(error = %err, scenario = %req.scenario_name, "failed to persist scenario request");
            }
            if let Err(err) = cache
                .write_scenario_report(&req.scenario_name, &report)
                .await
            {
                warn!(error = %err, scenario = %req.scenario_name, "failed to persist scenario report");
            }
        }
        Ok(report)
    }
}

struct ConstraintScenario {
    key: &'static str,
    display_name: &'static str,
    action_template: &'static str,
    mutate: fn(&mut crate::model::ScenarioConfig),
}

const CONSTRAINT_SCENARIOS: &[ConstraintScenario] = &[
    ConstraintScenario {
        key: "taints",
        display_name: "Taints & Tolerations",
        action_template: "Review taint policies",
        mutate: |s| s.ignore_taints = true,
    },
    ConstraintScenario {
        key: "anti_affinity",
        display_name: "Required Anti-Affinity",
        action_template: "Review anti-affinity rules",
        mutate: |s| s.relax_required_anti_affinity = true,
    },
    ConstraintScenario {
        key: "affinity",
        display_name: "Preferred Affinity",
        action_template: "Review preferred affinity",
        mutate: |s| s.relax_preferred_affinity = true,
    },
    ConstraintScenario {
        key: "rightsizing",
        display_name: "Request Rightsizing",
        action_template: "Right-size requests",
        mutate: |s| s.enable_joint_rightsizing = true,
    },
];

fn compute_constraint_costs(
    progress: &Option<Box<ProgressFn>>,
    start: &Instant,
    snapshot: &crate::model::ClusterSnapshot,
    pricing_catalog: &crate::pricing::PricingCatalog,
    baseline_scenario: &crate::model::ScenarioConfig,
    baseline_plan: &crate::model::OptimizationPlan,
) -> ConstraintCostTable {
    let baseline_savings = baseline_plan.savings_monthly.clone();
    let total_nodes = snapshot.nodes.len() as i32;
    let baseline_active = baseline_plan.active_nodes.len() as i32;
    let baseline_removable = total_nodes - baseline_active;

    let mut rows = Vec::new();
    let scenario_count = CONSTRAINT_SCENARIOS.len() + 1;

    for (i, cs) in CONSTRAINT_SCENARIOS.iter().enumerate() {
        emit_progress(
            progress,
            start,
            "constraint-cost",
            &format!(
                "Analyzing constraint: {} ({}/{})",
                cs.display_name,
                i + 1,
                scenario_count
            ),
            85 + ((i as i32 + 1) * 12 / scenario_count as i32),
            false,
            "",
        );

        let mut scenario = baseline_scenario.clone();
        (cs.mutate)(&mut scenario);

        match solve_scenario_quick(snapshot, pricing_catalog, &scenario) {
            Some(plan) => {
                let relaxed_savings = plan.savings_monthly.clone();
                let delta_monthly = relaxed_savings.monthly - baseline_savings.monthly;
                let relaxed_active = plan.active_nodes.len() as i32;
                let relaxed_removable = total_nodes - relaxed_active;

                let moves_changed = plan.recommended_moves.len() as i32;

                if delta_monthly > 0.01 {
                    rows.push(ConstraintCostRow {
                        constraint_key: cs.key.to_string(),
                        display_name: cs.display_name.to_string(),
                        baseline_savings: baseline_savings.clone(),
                        relaxed_savings,
                        delta: Money {
                            currency: baseline_savings.currency.clone(),
                            monthly: delta_monthly,
                        },
                        affected_workload_count: moves_changed,
                        affected_node_count: (relaxed_removable - baseline_removable).max(0),
                        nodes_removable_baseline: baseline_removable,
                        nodes_removable_relaxed: relaxed_removable,
                        action: format!(
                            "{} \u{2014} {} workloads affected",
                            cs.action_template, moves_changed
                        ),
                    });
                }
            }
            None => {
                warn!(
                    constraint = cs.key,
                    "constraint cost scenario solve failed, skipping"
                );
            }
        }
    }

    emit_progress(
        progress,
        start,
        "constraint-cost",
        &format!(
            "Analyzing constraint: Theoretical Max ({}/{})",
            scenario_count, scenario_count
        ),
        97,
        false,
        "",
    );

    let mut max_scenario = baseline_scenario.clone();
    for cs in CONSTRAINT_SCENARIOS {
        (cs.mutate)(&mut max_scenario);
    }
    let theoretical_max_savings =
        solve_scenario_quick(snapshot, pricing_catalog, &max_scenario)
            .map(|p| p.savings_monthly)
            .unwrap_or_else(|| baseline_savings.clone());

    rows.sort_by(|a, b| b.delta.monthly.partial_cmp(&a.delta.monthly).unwrap_or(std::cmp::Ordering::Equal));

    info!(
        rows = rows.len(),
        baseline = baseline_savings.monthly,
        theoretical_max = theoretical_max_savings.monthly,
        "constraint cost table computed"
    );

    ConstraintCostTable {
        rows,
        baseline_savings,
        theoretical_max_savings,
    }
}

fn solve_scenario_quick(
    snapshot: &crate::model::ClusterSnapshot,
    pricing_catalog: &crate::pricing::PricingCatalog,
    scenario: &crate::model::ScenarioConfig,
) -> Option<crate::model::OptimizationPlan> {
    let normalizer_options = NormalizerOptions {
        cpu_headroom_percent: scenario.cpu_headroom_percent,
        memory_headroom_percent: scenario.memory_headroom_percent,
        storage_headroom_percent: scenario.storage_headroom_percent,
        pods_headroom: scenario.pods_headroom,
        disallowed_pools: scenario.disallowed_pools.clone(),
        node_pool_label_keys: scenario.node_pool_label_keys.clone(),
        ignore_taints: scenario.ignore_taints,
        relax_preferred_affinity: scenario.relax_preferred_affinity,
        relax_required_anti_affinity: scenario.relax_required_anti_affinity,
        cpu_overcommit_ratio: scenario.cpu_overcommit_ratio,
        memory_overcommit_ratio: scenario.memory_overcommit_ratio,
        use_usage_adjusted_requests: scenario.use_usage_adjusted_requests,
        usage_request_floor_ratio: scenario.usage_request_floor_ratio,
        cpu_usage_safety_factor: scenario.cpu_usage_safety_factor,
        memory_usage_safety_factor: scenario.memory_usage_safety_factor,
        max_memory_overflow_probability_percent: scenario.max_memory_overflow_probability_percent,
    };
    let normalized = Normalizer::new(pricing_catalog.clone(), normalizer_options).normalize(snapshot);
    let input = build_input(&normalized, scenario.ignore_unschedulable_workloads);

    match cpsat_rust::solve(&input, scenario) {
        Ok((solution, solver_info)) => {
            let plan = Planner::new().build_plan(&normalized, &input, &solution, solver_info);
            Some(plan)
        }
        Err(err) => {
            warn!(error = %err, "constraint cost quick solve failed");
            None
        }
    }
}

fn normalize_solver_name(name: &str) -> String {
    match name.trim() {
        "" | "cp-sat" => "cp-sat-rust".to_string(),
        other => other.to_string(),
    }
}

fn apply_usage_risk_preset(
    scenario: &crate::model::ScenarioConfig,
) -> crate::model::ScenarioConfig {
    let mut resolved = scenario.clone();
    match scenario
        .usage_risk_preset
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "safe" => {
            resolved.use_usage_adjusted_requests = true;
            resolved.usage_request_floor_ratio = 0.8;
            resolved.cpu_usage_safety_factor = 1.8;
            resolved.memory_usage_safety_factor = 2.5;
            resolved.max_memory_overflow_probability_percent = 0.5;
        }
        "balanced" => {
            resolved.use_usage_adjusted_requests = true;
            resolved.usage_request_floor_ratio = 0.65;
            resolved.cpu_usage_safety_factor = 1.5;
            resolved.memory_usage_safety_factor = 2.0;
            resolved.max_memory_overflow_probability_percent = 1.0;
        }
        "aggressive" => {
            resolved.use_usage_adjusted_requests = true;
            resolved.usage_request_floor_ratio = 0.5;
            resolved.cpu_usage_safety_factor = 1.25;
            resolved.memory_usage_safety_factor = 1.5;
            resolved.max_memory_overflow_probability_percent = 5.0;
        }
        _ => {}
    }
    resolved
}

fn apply_memory_risk_gate(report: &mut AnalysisReport, scenario: &crate::model::ScenarioConfig) {
    let optimized = &report.explainability.memory_risk.optimized;
    let threshold = scenario.max_memory_overflow_probability_percent.max(0.0);
    if threshold <= 0.0 || optimized.overflow_probability_percent <= threshold {
        return;
    }
    if let Some(plan) = report.optimization.as_mut() {
        plan.blockers.push(crate::model::Blocker {
            scope: "memory-risk".to_string(),
            message: format!(
                "optimized plan exceeds memory overflow threshold: {:.1}% empirical overflow probability vs {:.1}% allowed",
                optimized.overflow_probability_percent,
                threshold
            ),
        });
        if !plan.solver.status.to_ascii_lowercase().contains("unsafe") {
            plan.solver.status = format!(
                "{}; unsafe by memory risk gate ({:.1}% > {:.1}% overflow probability)",
                plan.solver.status, optimized.overflow_probability_percent, threshold
            );
        }
    }
}

fn is_infeasible_error(err: &anyhow::Error) -> bool {
    err.to_string().to_ascii_lowercase().contains("infeasible")
}

fn emit_progress(
    progress: &Option<Box<ProgressFn>>,
    start: &Instant,
    stage: &str,
    message: &str,
    percent: i32,
    done: bool,
    error: &str,
) {
    debug!(
        stage,
        percent,
        done,
        message,
        error = if error.is_empty() { "<none>" } else { error },
        elapsed_ms = start.elapsed().as_millis(),
        "progress update"
    );
    if let Some(progress) = progress {
        progress(ProgressUpdate {
            stage: stage.to_string(),
            message: message.to_string(),
            percent,
            done,
            error: error.to_string(),
            elapsed_ms: start.elapsed().as_millis() as i64,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        apply_memory_risk_gate, apply_usage_risk_preset, is_infeasible_error, normalize_solver_name,
    };
    use crate::model::{
        AnalysisReport, ExplainabilityReport, MemoryRiskSummary, OptimizationPlan,
        PlacementMemoryRisk, ScenarioConfig, SolverInfo,
    };
    use anyhow::anyhow;

    #[test]
    fn maps_cp_sat_alias_to_rust_backend() {
        assert_eq!(normalize_solver_name("cp-sat"), "cp-sat-rust");
        assert_eq!(normalize_solver_name(""), "cp-sat-rust");
        assert_eq!(normalize_solver_name("cp-sat-rust"), "cp-sat-rust");
    }

    #[test]
    fn detects_infeasible_errors() {
        assert!(is_infeasible_error(&anyhow!("solver returned INFEASIBLE")));
        assert!(!is_infeasible_error(&anyhow!("solver unavailable")));
    }

    #[test]
    fn safe_preset_enables_more_conservative_usage_policy() {
        let scenario = ScenarioConfig {
            usage_risk_preset: "safe".to_string(),
            ..Default::default()
        };
        let resolved = apply_usage_risk_preset(&scenario);
        assert!(resolved.use_usage_adjusted_requests);
        assert_eq!(resolved.usage_request_floor_ratio, 0.8);
        assert_eq!(resolved.memory_usage_safety_factor, 2.5);
        assert_eq!(resolved.max_memory_overflow_probability_percent, 0.5);
    }

    #[test]
    fn memory_risk_gate_marks_plan_unsafe_when_threshold_exceeded() {
        let mut report = AnalysisReport {
            optimization: Some(OptimizationPlan {
                solver: SolverInfo {
                    name: "cp-sat-rust".to_string(),
                    available: true,
                    status: "OPTIMAL".to_string(),
                },
                ..Default::default()
            }),
            explainability: ExplainabilityReport {
                memory_risk: MemoryRiskSummary {
                    optimized: PlacementMemoryRisk {
                        overflow_probability_percent: 3.0,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };
        let scenario = ScenarioConfig {
            max_memory_overflow_probability_percent: 1.0,
            ..Default::default()
        };

        apply_memory_risk_gate(&mut report, &scenario);

        let plan = report.optimization.unwrap();
        assert_eq!(plan.blockers.len(), 1);
        assert!(plan.solver.status.contains("unsafe by memory risk gate"));
    }
}
