use crate::model::{
    ActionItem, AnalysisReport, ClusterSnapshot, ExplainabilityReport, InflationDriver,
    MemoryRiskSummary, MissingVpaOpportunity, Money, NodeDrainability, NodeMemoryRisk,
    NormalizedCluster, OptimizationPlan, PdbImpact, PlacementMemoryRisk, PoolEmptiability,
    PoolSummary, ResourceAllocation, SavingsLayer, SavingsWaterfall, VerticalPodAutoscaler,
    VpaOverview, VpaPolicyGap, VpaUpdateModeImpact, WorkloadCostRow,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub struct ExplainabilityBuilder;

impl ExplainabilityBuilder {
    // @lineage
    // reads: report.normalized.*, report.optimization.*, report.snapshot.vpas
    pub fn build(
        &self,
        snapshot: &ClusterSnapshot,
        normalized: &NormalizedCluster,
        optimization: Option<&OptimizationPlan>,
    ) -> ExplainabilityReport {
        let mut drivers = Vec::new();
        if let Some(plan) = optimization {
            drivers.extend(autoscaler_drivers(plan));
            if let Some(driver) = requests_driver(normalized, plan) {
                drivers.push(driver);
            }
            if let Some(driver) = max_pods_driver(normalized, plan) {
                drivers.push(driver);
            }
            if let Some(driver) = vpa_driver(snapshot, normalized, plan) {
                drivers.push(driver);
            }
            if let Some(driver) = vpa_safety_margin_driver(snapshot, normalized, plan) {
                drivers.push(driver);
            }
        }
        drivers.extend(constraint_impact_drivers(normalized));
        drivers.sort_by(compare_drivers);
        let workload_cost_table = build_workload_cost_table(normalized);
        let action_items = build_action_items(normalized, &drivers, &workload_cost_table);
        let savings_waterfall = compute_savings_waterfall(optimization, &drivers);
        let has_usage_data = normalized
            .workloads
            .iter()
            .any(|w| w.usage.cpu_usage_milli > 0 || w.usage.memory_bytes > 0);
        let pool_summaries_with_emptiability =
            compute_pool_emptiability(&normalized.pool_summaries, normalized);
        let pool_emptiability = pool_summaries_with_emptiability;
        let node_drainability = compute_node_drainability(normalized, optimization);
        let ds_savings =
            compute_daemonset_consolidation_savings(snapshot, normalized, optimization);
        let pdb_impacts = compute_pdb_impacts(snapshot, normalized);
        let vpa_overview = build_vpa_overview(snapshot, normalized, &workload_cost_table);
        let vpa_update_mode_impacts = build_vpa_update_mode_impacts(snapshot, normalized);
        let vpa_policy_gaps = build_vpa_policy_gaps(snapshot);
        let missing_vpa =
            build_missing_vpa_opportunities(snapshot, normalized, &workload_cost_table);
        ExplainabilityReport {
            inflation_drivers: drivers,
            memory_risk: build_memory_risk(snapshot, normalized, optimization),
            has_usage_data,
            pool_emptiability,
            node_drainability,
            daemonset_consolidation_savings: ds_savings,
            pdb_impacts,
            vpa_overview,
            vpa_update_mode_impacts,
            vpa_policy_gaps,
            missing_vpa_opportunities: missing_vpa,
            action_items,
            savings_waterfall,
            workload_cost_table,
        }
    }
}

// @lineage
// reads: report.snapshot, report.normalized, report.optimization
pub fn populate_report(report: &mut AnalysisReport) {
    report.explainability = ExplainabilityBuilder.build(
        &report.snapshot,
        &report.normalized,
        report.optimization.as_ref(),
    );
}

// @lineage
// reads: normalized.nodes.*, normalized.workloads.*, optimization.active_nodes, optimization.recommended_moves
fn build_memory_risk(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
    optimization: Option<&OptimizationPlan>,
) -> MemoryRiskSummary {
    let current = placement_memory_risk(snapshot, normalized, false, optimization);
    let optimized = placement_memory_risk(snapshot, normalized, true, optimization);
    MemoryRiskSummary {
        model: if snapshot
            .pods
            .iter()
            .any(|pod| !pod.memory_history.samples.is_empty())
        {
            "historical_coincidence".to_string()
        } else {
            "observed_peak_proxy".to_string()
        },
        current,
        optimized,
    }
}

// @lineage
// reads: normalized.nodes.*, normalized.workloads.*, optimization.active_nodes, optimization.recommended_moves
fn placement_memory_risk(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
    optimized: bool,
    optimization: Option<&OptimizationPlan>,
) -> PlacementMemoryRisk {
    let mut node_usage = BTreeMap::<String, (i64, i32)>::new();
    let active_nodes = optimization
        .map(|plan| plan.active_nodes.iter().cloned().collect::<BTreeSet<_>>())
        .unwrap_or_default();
    let move_targets = optimization
        .map(|plan| {
            plan.recommended_moves
                .iter()
                .map(|mv| (format!("{}/{}", mv.namespace, mv.pod), mv.to_node.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    for workload in &normalized.workloads {
        let node_name = if optimized {
            let key = format!("{}/{}", workload.namespace, workload.name);
            move_targets
                .get(&key)
                .cloned()
                .filter(|name| !name.is_empty())
                .unwrap_or_else(|| workload.current_node.clone())
        } else {
            workload.current_node.clone()
        };
        if node_name.is_empty() {
            continue;
        }
        let entry = node_usage.entry(node_name).or_insert((0, 0));
        entry.0 += workload.usage.memory_bytes.max(0);
        entry.1 += 1;
    }

    let mut top_nodes = normalized
        .nodes
        .iter()
        .filter(|node| !optimized || active_nodes.is_empty() || active_nodes.contains(&node.name))
        .filter_map(|node| {
            let capacity = node.effective_capacity.memory_bytes.max(0);
            if capacity <= 0 {
                return None;
            }
            let (used, workload_count) = node_usage.get(&node.name).copied().unwrap_or((0, 0));
            let pressure = (used as f64 / capacity as f64) * 100.0;
            let overflow = (pressure - 100.0).max(0.0);
            let overflow_probability_percent = empirical_node_overflow_probability(
                snapshot,
                normalized,
                node.name.as_str(),
                optimized,
                optimization,
            );
            Some(NodeMemoryRisk {
                node_name: node.name.clone(),
                risk: memory_risk_label(pressure).to_string(),
                pressure_percent: pressure,
                overflow_percent: overflow,
                overflow_probability_percent,
                used_memory_bytes: used,
                capacity_memory_bytes: capacity,
                workload_count,
            })
        })
        .collect::<Vec<_>>();

    top_nodes.sort_by(|left, right| {
        right
            .pressure_percent
            .partial_cmp(&left.pressure_percent)
            .unwrap_or(Ordering::Equal)
    });

    let overflow_node_count = top_nodes
        .iter()
        .filter(|node| node.pressure_percent > 100.0)
        .count() as i32;
    let high_risk_node_count = top_nodes
        .iter()
        .filter(|node| node.pressure_percent >= 90.0)
        .count() as i32;
    let max_pressure_percent = top_nodes
        .iter()
        .map(|node| node.pressure_percent)
        .fold(0.0_f64, f64::max);
    let overflow_probability_percent = top_nodes
        .iter()
        .map(|node| node.overflow_probability_percent)
        .fold(0.0_f64, f64::max);
    let risk = memory_risk_label(max_pressure_percent);
    let summary = if optimized {
        format!(
            "Optimized placement reaches {} peak memory pressure with {} overflow nodes, {} high-risk nodes, and up to {} empirical overflow probability under the historical memory model.",
            percent_label(max_pressure_percent),
            overflow_node_count,
            high_risk_node_count,
            percent_label(overflow_probability_percent)
        )
    } else {
        format!(
            "Current placement reaches {} peak memory pressure with {} overflow nodes, {} high-risk nodes, and up to {} empirical overflow probability under the historical memory model.",
            percent_label(max_pressure_percent),
            overflow_node_count,
            high_risk_node_count,
            percent_label(overflow_probability_percent)
        )
    };

    PlacementMemoryRisk {
        risk: risk.to_string(),
        overflow_node_count,
        high_risk_node_count,
        max_pressure_percent,
        overflow_probability_percent,
        summary,
        top_nodes: top_nodes.into_iter().take(10).collect(),
    }
}

fn empirical_node_overflow_probability(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
    node_name: &str,
    optimized: bool,
    optimization: Option<&OptimizationPlan>,
) -> f64 {
    let move_targets = optimization
        .map(|plan| {
            plan.recommended_moves
                .iter()
                .map(|mv| (format!("{}/{}", mv.namespace, mv.pod), mv.to_node.clone()))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let Some(capacity) = normalized
        .nodes
        .iter()
        .find(|node| node.name == node_name)
        .map(|node| node.effective_capacity.memory_bytes.max(0))
    else {
        return 0.0;
    };
    if capacity <= 0 {
        return 0.0;
    }

    let relevant_pods = snapshot
        .pods
        .iter()
        .filter(|pod| {
            let assigned_node = if optimized {
                move_targets
                    .get(&format!("{}/{}", pod.namespace, pod.name))
                    .cloned()
                    .filter(|name| !name.is_empty())
                    .unwrap_or_else(|| pod.node_name.clone())
            } else {
                pod.node_name.clone()
            };
            assigned_node == node_name && !pod.memory_history.samples.is_empty()
        })
        .collect::<Vec<_>>();

    if relevant_pods.is_empty() {
        return 0.0;
    }

    let mut pressure_by_ts = BTreeMap::<i64, i64>::new();
    for pod in relevant_pods {
        for sample in &pod.memory_history.samples {
            *pressure_by_ts.entry(sample.timestamp_unix).or_default() += sample.value.max(0);
        }
    }
    if pressure_by_ts.is_empty() {
        return 0.0;
    }

    let overflow_count = pressure_by_ts
        .values()
        .filter(|value| **value > capacity)
        .count() as f64;
    (overflow_count / pressure_by_ts.len() as f64) * 100.0
}

// @lineage
// reads: plan.resource_summary.autoscaler_blockers
fn autoscaler_drivers(plan: &OptimizationPlan) -> Vec<InflationDriver> {
    plan.resource_summary
        .autoscaler_blockers
        .iter()
        .map(|blocker| InflationDriver {
            key: "safe-to-evict-false".to_string(),
            display_name: "Cluster Autoscaler safe-to-evict=false".to_string(),
            method: "postsolve_blocker".to_string(),
            confidence: "high".to_string(),
            blocked_monthly_cost: blocker.blocked_monthly_cost.clone(),
            affected_workload_count: blocker.blocked_workload_count,
            blocked_node_count: blocker.blocked_node_count,
            summary: format!(
                "{} removable nodes in the solver plan still contain not-safe-to-evict workloads, so autoscaler cannot realize this savings on its own.",
                blocker.blocked_node_count
            ),
            recommended_action: "Review and remove cluster-autoscaler not-safe-to-evict annotations where operationally safe, then retry scale-down.".to_string(),
            top_entities: blocker.blocked_nodes.clone(),
            details: vec![
                format!(
                    "Blocked removable nodes: {}",
                    blocker.blocked_nodes.join(", ")
                ),
                format!(
                    "Blocking workloads on those nodes: {}",
                    blocker.blocked_workload_count
                ),
            ],
        })
        .collect()
}

// @lineage
// reads: normalized.constraint_impacts
fn constraint_impact_drivers(normalized: &NormalizedCluster) -> Vec<InflationDriver> {
    normalized
        .constraint_impacts
        .iter()
        .filter(|impact| impact.estimated_monthly > 0.0)
        .filter_map(|impact| {
            let (key, action) = match impact.name.as_str() {
                "taints/tolerations" => (
                    "taints",
                    "Review taints and tolerations on constrained pools to see whether they are preserving more spread than required.",
                ),
                "required anti-affinity" => (
                    "required-anti-affinity",
                    "Audit hard anti-affinity rules and downgrade non-critical rules to preferences where possible.",
                ),
                "required affinity" => (
                    "required-affinity",
                    "Audit hard affinity rules and remove any rules that unnecessarily pin workloads to specific nodes or pools.",
                ),
                "node selectors" => (
                    "node-selectors",
                    "Check node selectors for stale pool or topology pinning that blocks consolidation.",
                ),
                "volume locality" => (
                    "volume-locality",
                    "Review stateful workloads and storage topology. Zoned storage often anchors expensive pools.",
                ),
                "required node affinity" => (
                    "node-affinity",
                    "Review required node affinity rules. These pin workloads to specific node labels and prevent consolidation.",
                ),
                "topology spread constraints" => (
                    "topology-spread",
                    "Review topology spread constraints. These force pods across failure domains and prevent tighter packing on fewer nodes.",
                ),
                _ => return None,
            };

            let top_entities: Vec<String> = normalized
                .workloads
                .iter()
                .filter(|w| w.reasons.iter().any(|r| r == &impact.name))
                .take(8)
                .map(|w| {
                    format!(
                        "{}/{}",
                        w.namespace,
                        if w.owner_name.is_empty() {
                            w.name.as_str()
                        } else {
                            w.owner_name.as_str()
                        }
                    )
                })
                .collect();

            Some(InflationDriver {
                key: key.to_string(),
                display_name: impact.name.clone(),
                method: "constraint_heuristic".to_string(),
                confidence: "medium".to_string(),
                blocked_monthly_cost: Money {
                    currency: normalized.current_monthly_cost.currency.clone(),
                    monthly: impact.estimated_monthly,
                },
                affected_workload_count: impact.workloads_affected,
                blocked_node_count: 0,
                summary: format!(
                    "{} workloads were affected by {}, with {} modeled constraint hits.",
                    impact.workloads_affected, impact.name, impact.reason_count
                ),
                recommended_action: action.to_string(),
                top_entities,
                details: vec![format!(
                    "Modeled impact estimate: {} across {} reason hits",
                    money_label(impact.estimated_monthly),
                    impact.reason_count
                )],
            })
        })
        .collect()
}

// @lineage
// reads: normalized.workloads.*, plan.resource_summary.optimized
fn requests_driver(
    normalized: &NormalizedCluster,
    plan: &OptimizationPlan,
) -> Option<InflationDriver> {
    let mut affected = 0_i32;
    let mut cpu_saved = 0_i64;
    let mut memory_saved = 0_i64;
    let mut top = normalized
        .workloads
        .iter()
        .filter_map(|workload| {
            let cpu_delta = (workload.current_requests.milli_cpu
                - workload.recommended_requests.milli_cpu)
                .max(0);
            let mem_delta = (workload.current_requests.memory_bytes
                - workload.recommended_requests.memory_bytes)
                .max(0);
            if cpu_delta <= 0 && mem_delta <= 0 {
                return None;
            }
            affected += 1;
            cpu_saved += cpu_delta;
            memory_saved += mem_delta;
            Some((
                mem_delta,
                format!(
                    "{}/{} ({})",
                    workload.namespace,
                    if workload.owner_name.is_empty() {
                        workload.name.as_str()
                    } else {
                        workload.owner_name.as_str()
                    },
                    workload.owner_kind
                ),
            ))
        })
        .collect::<Vec<_>>();

    if affected == 0 {
        return None;
    }

    top.sort_by(|a, b| b.0.cmp(&a.0));
    let top_entities = top
        .into_iter()
        .take(8)
        .map(|(_, name)| name)
        .collect::<Vec<_>>();
    let blocked_monthly_cost = estimate_incremental_reclaim_cost(
        &plan.optimized_monthly_cost,
        &plan.resource_summary.optimized,
        cpu_saved,
        memory_saved,
    );

    Some(InflationDriver {
        key: "requests".to_string(),
        display_name: "Inflated requests".to_string(),
        method: "demand_delta".to_string(),
        confidence: "medium".to_string(),
        blocked_monthly_cost: blocked_monthly_cost.clone(),
        affected_workload_count: affected,
        blocked_node_count: 0,
        summary: format!(
            "{} workloads could shrink by {} CPU and {} memory under the current usage-backed recommendation policy.",
            affected,
            cpu_label(cpu_saved),
            bytes_label(memory_saved)
        ),
        recommended_action: "Review the request recommendation table and apply safe request reductions first to the largest memory-heavy workloads.".to_string(),
        top_entities,
        details: vec![
            format!("Reclaimable CPU requests: {}", cpu_label(cpu_saved)),
            format!("Reclaimable memory requests: {}", bytes_label(memory_saved)),
            format!(
                "Estimated incremental savings beyond current packing plan: {}",
                money_label(blocked_monthly_cost.monthly)
            ),
        ],
    })
}

// @lineage
// reads: snapshot.vpas, normalized.workloads.*, plan.resource_summary.current
fn vpa_driver(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
    plan: &OptimizationPlan,
) -> Option<InflationDriver> {
    if snapshot.vpas.is_empty() {
        return None;
    }

    let workloads_by_owner = normalized.workloads.iter().fold(
        BTreeMap::<(String, String, String), Vec<_>>::new(),
        |mut acc, workload| {
            acc.entry((
                workload.namespace.clone(),
                workload.owner_kind.clone(),
                workload.owner_name.clone(),
            ))
            .or_default()
            .push(workload);
            acc
        },
    );

    let mut affected = 0_i32;
    let mut cpu_saved = 0_i64;
    let mut memory_saved = 0_i64;
    let mut entities = Vec::new();
    let mut details = Vec::new();

    for vpa in &snapshot.vpas {
        let Some(workloads) = workloads_by_owner.get(&(
            vpa.namespace.clone(),
            vpa.target_ref_kind.clone(),
            vpa.target_ref_name.clone(),
        )) else {
            continue;
        };

        let rec = aggregate_vpa_recommendation(vpa);
        if rec.milli_cpu <= 0 && rec.memory_bytes <= 0 {
            continue;
        }

        let mut local_cpu_saved = 0_i64;
        let mut local_mem_saved = 0_i64;
        for workload in workloads {
            local_cpu_saved += (workload.current_requests.milli_cpu - rec.milli_cpu).max(0);
            local_mem_saved += (workload.current_requests.memory_bytes - rec.memory_bytes).max(0);
            affected += 1;
        }
        if local_cpu_saved <= 0 && local_mem_saved <= 0 {
            continue;
        }
        cpu_saved += local_cpu_saved;
        memory_saved += local_mem_saved;
        entities.push(format!(
            "{}/{} ({}) updateMode={}",
            vpa.namespace,
            vpa.name,
            vpa.target_ref_kind,
            non_empty_or_default(&vpa.update_mode, "Auto")
        ));
        details.push(format!(
            "{}/{} -> {} {} target={} cpu / {} mem",
            vpa.namespace,
            vpa.name,
            vpa.target_ref_kind,
            vpa.target_ref_name,
            cpu_label(rec.milli_cpu),
            bytes_label(rec.memory_bytes)
        ));
        if !vpa.container_policies.is_empty() {
            details.extend(vpa.container_policies.iter().take(3).map(|policy| {
                format!(
                    "policy {} resources={} min={} cpu {} mem max={} cpu {} mem",
                    non_empty_or_default(&policy.container_name, "*"),
                    if policy.controlled_resources.is_empty() {
                        "default".to_string()
                    } else {
                        policy.controlled_resources.join("+")
                    },
                    cpu_label(policy.min_allowed.milli_cpu),
                    bytes_label(policy.min_allowed.memory_bytes),
                    cpu_label(policy.max_allowed.milli_cpu),
                    bytes_label(policy.max_allowed.memory_bytes)
                )
            }));
        }
    }

    if entities.is_empty() {
        return None;
    }

    Some(InflationDriver {
        key: "vpa".to_string(),
        display_name: "Vertical Pod Autoscaler policy + recommendations".to_string(),
        method: "vpa_delta".to_string(),
        confidence: "medium".to_string(),
        blocked_monthly_cost: estimate_incremental_reclaim_cost(
            &plan.optimized_monthly_cost,
            &plan.resource_summary.optimized,
            cpu_saved,
            memory_saved,
        ),
        affected_workload_count: affected,
        blocked_node_count: 0,
        summary: format!(
            "{} VPAs were found live. Their current target recommendations imply {} CPU and {} memory of reclaimable request space across {} workloads.",
            snapshot.vpas.len(),
            cpu_label(cpu_saved),
            bytes_label(memory_saved),
            affected
        ),
        recommended_action: "Inspect VPA update modes, min/max resource policies, and recommendation bounds before applying request changes cluster-wide.".to_string(),
        top_entities: entities.into_iter().take(8).collect(),
        details: {
            let mut details = details;
            details.push(format!(
                "Recommender safety margin source={} value={:.0}%",
                non_empty_or_default(&snapshot.vpa_recommender.source, "default"),
                snapshot.vpa_recommender.safety_margin_fraction * 100.0
            ));
            details
        },
    })
}

// @lineage
// reads: snapshot.vpa_recommender.*, snapshot.vpas[].container_recommendations, normalized.workloads, plan.resource_summary.optimized
fn vpa_safety_margin_driver(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
    plan: &OptimizationPlan,
) -> Option<InflationDriver> {
    if snapshot.vpas.is_empty() {
        return None;
    }

    let margin = snapshot.vpa_recommender.safety_margin_fraction;
    if margin <= 0.0 {
        return None;
    }

    let workloads_by_owner = normalized.workloads.iter().fold(
        BTreeMap::<(String, String, String), Vec<_>>::new(),
        |mut acc, workload| {
            acc.entry((
                workload.namespace.clone(),
                workload.owner_kind.clone(),
                workload.owner_name.clone(),
            ))
            .or_default()
            .push(workload);
            acc
        },
    );

    let mut affected = 0_i32;
    let mut cpu_saved = 0_i64;
    let mut memory_saved = 0_i64;
    let mut top_entities = Vec::new();

    for vpa in &snapshot.vpas {
        let Some(workloads) = workloads_by_owner.get(&(
            vpa.namespace.clone(),
            vpa.target_ref_kind.clone(),
            vpa.target_ref_name.clone(),
        )) else {
            continue;
        };
        let rec = aggregate_vpa_recommendation(vpa);
        if rec.milli_cpu <= 0 && rec.memory_bytes <= 0 {
            continue;
        }
        let demargined_cpu = ((rec.milli_cpu as f64) / (1.0 + margin)).round() as i64;
        let demargined_memory = ((rec.memory_bytes as f64) / (1.0 + margin)).round() as i64;
        let cpu_delta = (rec.milli_cpu - demargined_cpu).max(0);
        let memory_delta = (rec.memory_bytes - demargined_memory).max(0);
        if cpu_delta <= 0 && memory_delta <= 0 {
            continue;
        }
        cpu_saved += cpu_delta * workloads.len() as i64;
        memory_saved += memory_delta * workloads.len() as i64;
        affected += workloads.len() as i32;
        top_entities.push(format!(
            "{}/{} margin adds {} CPU and {} memory",
            vpa.namespace,
            vpa.target_ref_name,
            cpu_label(cpu_delta),
            bytes_label(memory_delta)
        ));
    }

    if affected == 0 {
        return None;
    }

    let blocked_monthly_cost = estimate_incremental_reclaim_cost(
        &plan.optimized_monthly_cost,
        &plan.resource_summary.optimized,
        cpu_saved,
        memory_saved,
    );

    Some(InflationDriver {
        key: "vpa-safety-margin".to_string(),
        display_name: "VPA safety margin".to_string(),
        method: "vpa_policy_delta".to_string(),
        confidence: if snapshot.vpa_recommender.found {
            "high".to_string()
        } else {
            "medium".to_string()
        },
        blocked_monthly_cost: blocked_monthly_cost.clone(),
        affected_workload_count: affected,
        blocked_node_count: 0,
        summary: format!(
            "VPA recommender safety margin of {:.0}% is preserving {} CPU and {} memory on top of the de-margined recommendation baseline.",
            margin * 100.0,
            cpu_label(cpu_saved),
            bytes_label(memory_saved)
        ),
        recommended_action: "Decide whether the current VPA recommender safety margin is intentionally conservative for this cluster before treating the remaining request bloat as user-owned waste.".to_string(),
        top_entities: top_entities.into_iter().take(8).collect(),
        details: vec![
            format!(
                "Recommender source: {}",
                non_empty_or_default(&snapshot.vpa_recommender.source, "default")
            ),
            format!("Safety margin: {:.0}%", margin * 100.0),
            format!(
                "Estimated incremental cost preserved by safety margin: {}",
                money_label(blocked_monthly_cost.monthly)
            ),
        ],
    })
}

// @lineage
// reads: normalized.nodes.current_pods, normalized.nodes.allocatable.pods, current_monthly_cost
fn max_pods_driver(
    normalized: &NormalizedCluster,
    plan: &OptimizationPlan,
) -> Option<InflationDriver> {
    let mut saturated_nodes = Vec::new();
    let mut affected_workloads = BTreeSet::new();
    let mut blocked_monthly = 0.0_f64;
    let mut currency = normalized.current_monthly_cost.currency.clone();

    for node in &normalized.nodes {
        if node.allocatable.pods <= 0 || i64::from(node.current_pods) < node.allocatable.pods {
            continue;
        }
        saturated_nodes.push(node.name.clone());
        blocked_monthly += node.price.monthly;
        if currency.is_empty() {
            currency = node.price.currency.clone();
        }
        for workload in &normalized.workloads {
            if workload.current_node == node.name {
                affected_workloads.insert(format!("{}/{}", workload.namespace, workload.name));
            }
        }
    }

    if saturated_nodes.is_empty() {
        return None;
    }

    Some(InflationDriver {
        key: "max-pods-per-node".to_string(),
        display_name: "Max pods per node".to_string(),
        method: "static_heuristic".to_string(),
        confidence: "low".to_string(),
        blocked_monthly_cost: Money {
            currency,
            monthly: blocked_monthly.min(plan.current_monthly_cost.monthly),
        },
        affected_workload_count: affected_workloads.len() as i32,
        blocked_node_count: saturated_nodes.len() as i32,
        summary: format!(
            "{} nodes are already at or above their allocatable pod count, which can block further binpacking even when CPU and memory remain.",
            saturated_nodes.len()
        ),
        recommended_action: "Inspect kubelet max-pods configuration and ENI/IP limits on saturated nodes before chasing more placement savings.".to_string(),
        top_entities: saturated_nodes.into_iter().take(8).collect(),
        details: vec![
            "This is currently a heuristic based on allocatable pod count saturation, not a dedicated max-pods what-if solve.".to_string(),
        ],
    })
}

// @lineage
// reads: optimized_cost, optimized_allocation, reclaimable cpu/memory
fn estimate_incremental_reclaim_cost(
    optimized_cost: &Money,
    allocation: &ResourceAllocation,
    reclaim_cpu_milli: i64,
    reclaim_memory_bytes: i64,
) -> Money {
    let incremental_cpu = (reclaim_cpu_milli - allocation.unallocated_cpu_request_milli).max(0);
    let incremental_memory =
        (reclaim_memory_bytes - allocation.unallocated_memory_request_bytes).max(0);
    let cpu_fraction = if allocation.total_cpu_capacity_milli > 0 {
        incremental_cpu as f64 / allocation.total_cpu_capacity_milli as f64
    } else {
        0.0
    };
    let memory_fraction = if allocation.total_memory_capacity_bytes > 0 {
        incremental_memory as f64 / allocation.total_memory_capacity_bytes as f64
    } else {
        0.0
    };
    let fraction = cpu_fraction.max(memory_fraction).clamp(0.0, 1.0);
    Money {
        currency: optimized_cost.currency.clone(),
        monthly: optimized_cost.monthly * fraction,
    }
}

// @lineage
// reads: vpa.container_recommendations[].target
fn aggregate_vpa_recommendation(vpa: &VerticalPodAutoscaler) -> crate::model::ResourceList {
    vpa.container_recommendations
        .iter()
        .fold(Default::default(), |mut acc, rec| {
            acc.milli_cpu += rec.target.milli_cpu;
            acc.memory_bytes += rec.target.memory_bytes;
            acc.ephemeral_storage += rec.target.ephemeral_storage;
            acc.pods += rec.target.pods;
            acc
        })
}

fn compare_drivers(left: &InflationDriver, right: &InflationDriver) -> Ordering {
    right
        .blocked_monthly_cost
        .monthly
        .partial_cmp(&left.blocked_monthly_cost.monthly)
        .unwrap_or(Ordering::Equal)
        .then_with(|| {
            right
                .affected_workload_count
                .cmp(&left.affected_workload_count)
        })
}

fn non_empty_or_default(value: &str, fallback: &str) -> String {
    if value.is_empty() {
        fallback.to_string()
    } else {
        value.to_string()
    }
}

fn strip_replicaset_hash(name: &str) -> Option<&str> {
    let last_dash = name.rfind('-')?;
    let suffix = &name[last_dash + 1..];
    if suffix.len() >= 7 && suffix.len() <= 10 && suffix.chars().all(|c| c.is_ascii_hexdigit()) {
        Some(&name[..last_dash])
    } else {
        None
    }
}

fn cpu_label(milli: i64) -> String {
    format!("{:.1} cores", milli as f64 / 1000.0)
}

fn bytes_label(bytes: i64) -> String {
    format!("{:.1} GiB", bytes as f64 / 1024_f64.powi(3))
}

fn money_label(monthly: f64) -> String {
    format!("${monthly:.2}/month")
}

fn percent_label(percent: f64) -> String {
    format!("{percent:.1}%")
}

fn memory_risk_label(pressure_percent: f64) -> &'static str {
    if pressure_percent > 100.0 {
        "critical"
    } else if pressure_percent >= 90.0 {
        "high"
    } else if pressure_percent >= 75.0 {
        "medium"
    } else {
        "low"
    }
}

// @lineage
// reads: normalized.workloads.*, normalized.nodes.*
fn build_workload_cost_table(normalized: &NormalizedCluster) -> Vec<WorkloadCostRow> {
    let node_map: BTreeMap<&str, (&crate::model::ResourceList, f64)> = normalized
        .nodes
        .iter()
        .map(|n| (n.name.as_str(), (&n.effective_capacity, n.price.monthly)))
        .collect();

    let mut rows: Vec<WorkloadCostRow> = normalized
        .workloads
        .iter()
        .filter_map(|w| {
            let (cap, node_price) = node_map.get(w.current_node.as_str()).copied()?;
            if cap.milli_cpu <= 0 && cap.memory_bytes <= 0 {
                return None;
            }
            let cpu_frac = if cap.milli_cpu > 0 {
                w.current_requests.milli_cpu as f64 / cap.milli_cpu as f64
            } else {
                0.0
            };
            let mem_frac = if cap.memory_bytes > 0 {
                w.current_requests.memory_bytes as f64 / cap.memory_bytes as f64
            } else {
                0.0
            };
            let monthly_cost = cpu_frac.max(mem_frac) * node_price;

            let rec_cpu_frac = if cap.milli_cpu > 0 {
                w.recommended_requests.milli_cpu as f64 / cap.milli_cpu as f64
            } else {
                0.0
            };
            let rec_mem_frac = if cap.memory_bytes > 0 {
                w.recommended_requests.memory_bytes as f64 / cap.memory_bytes as f64
            } else {
                0.0
            };
            let rightsized_cost = rec_cpu_frac.max(rec_mem_frac) * node_price;
            let savings = (monthly_cost - rightsized_cost).max(0.0);

            let cpu_usage_ratio = if w.current_requests.milli_cpu > 0 {
                w.usage.cpu_usage_milli as f64 / w.current_requests.milli_cpu as f64
            } else {
                0.0
            };
            let memory_usage_ratio = if w.current_requests.memory_bytes > 0 {
                w.usage.memory_bytes as f64 / w.current_requests.memory_bytes as f64
            } else {
                0.0
            };

            Some(WorkloadCostRow {
                namespace: w.namespace.clone(),
                name: w.name.clone(),
                owner_kind: w.owner_kind.clone(),
                owner_name: w.owner_name.clone(),
                current_cpu_milli: w.current_requests.milli_cpu,
                current_memory_bytes: w.current_requests.memory_bytes,
                recommended_cpu_milli: w.recommended_requests.milli_cpu,
                recommended_memory_bytes: w.recommended_requests.memory_bytes,
                usage_cpu_milli: w.usage.cpu_usage_milli,
                usage_memory_bytes: w.usage.memory_bytes,
                cpu_usage_ratio,
                memory_usage_ratio,
                monthly_cost: Money {
                    currency: "USD".to_string(),
                    monthly: monthly_cost,
                },
                savings_if_rightsized: Money {
                    currency: "USD".to_string(),
                    monthly: savings,
                },
                constraints: w.reasons.clone(),
                candidate_levels: w.candidate_levels.clone(),
            })
        })
        .collect();

    rows.sort_by(|a, b| {
        b.savings_if_rightsized
            .monthly
            .partial_cmp(&a.savings_if_rightsized.monthly)
            .unwrap_or(Ordering::Equal)
    });
    rows.truncate(30);
    rows
}

// @lineage
// reads: normalized.workloads.*, drivers.*, workload_cost_table.*
fn build_action_items(
    _normalized: &NormalizedCluster,
    drivers: &[InflationDriver],
    workload_cost_table: &[WorkloadCostRow],
) -> Vec<ActionItem> {
    let mut items = Vec::new();

    let mut rightsizing_by_owner: BTreeMap<String, (WorkloadCostRow, i32)> = BTreeMap::new();
    for row in workload_cost_table {
        if row.savings_if_rightsized.monthly < 0.01 {
            continue;
        }
        let owner_key = format!(
            "{}/{}/{}",
            row.namespace,
            if row.owner_kind.is_empty() {
                "pod"
            } else {
                row.owner_kind.as_str()
            },
            if row.owner_name.is_empty() {
                row.name.as_str()
            } else {
                row.owner_name.as_str()
            }
        );
        rightsizing_by_owner
            .entry(owner_key)
            .and_modify(|e| {
                e.0.savings_if_rightsized.monthly += row.savings_if_rightsized.monthly;
                e.1 += 1;
            })
            .or_insert_with(|| (row.clone(), 1));
    }

    for (row, replica_count) in rightsizing_by_owner.values() {
        let cpu_delta = row.current_cpu_milli - row.recommended_cpu_milli;
        let mem_delta = row.current_memory_bytes - row.recommended_memory_bytes;
        let resource = if mem_delta > 0 && cpu_delta > 0 {
            "cpu+memory"
        } else if mem_delta > 0 {
            "memory"
        } else {
            "cpu"
        };

        let reduction_pct = if resource == "cpu" {
            if row.current_cpu_milli > 0 {
                cpu_delta as f64 / row.current_cpu_milli as f64
            } else {
                0.0
            }
        } else if row.current_memory_bytes > 0 {
            mem_delta as f64 / row.current_memory_bytes as f64
        } else {
            0.0
        };
        let risk = if reduction_pct < 0.3 {
            "low"
        } else if reduction_pct < 0.6 {
            "medium"
        } else {
            "high"
        };

        let current_val = format_resource_pair(row.current_cpu_milli, row.current_memory_bytes);
        let recommended_val =
            format_resource_pair(row.recommended_cpu_milli, row.recommended_memory_bytes);

        let kubectl = generate_kubectl_command(
            &row.namespace,
            &row.owner_kind,
            &row.owner_name,
            row.recommended_cpu_milli,
            row.recommended_memory_bytes,
        );

        let reason = format!(
            "Frees {} per replica ({} replicas), improving binpacking density",
            format_resource_delta(cpu_delta, mem_delta),
            replica_count,
        );

        items.push(ActionItem {
            rank: 0,
            title: format!(
                "Rightsize {} in {}/{}",
                resource,
                row.namespace,
                if row.owner_name.is_empty() {
                    row.name.as_str()
                } else {
                    row.owner_name.as_str()
                }
            ),
            workload: format!("{}/{}", row.namespace, row.owner_name),
            resource: resource.to_string(),
            current_value: current_val,
            recommended_value: recommended_val,
            savings_monthly: row.savings_if_rightsized.clone(),
            risk: risk.to_string(),
            kubectl_command: kubectl,
            reason,
            source: "request_rightsizing".to_string(),
            effort: "quick".to_string(),
        });
    }

    for driver in drivers {
        let key = driver.key.as_str();
        if matches!(
            key,
            "taints"
                | "anti-affinity"
                | "node-selectors"
                | "affinity"
                | "node-affinity"
                | "required-anti-affinity"
                | "volume-locality"
                | "topology-spread"
        ) {
            let blocked = driver.blocked_monthly_cost.monthly;
            if blocked < 0.01 {
                continue;
            }
            items.push(ActionItem {
                rank: 0,
                title: format!("Review {}", driver.display_name.as_str(),),
                workload: driver.top_entities.first().cloned().unwrap_or_default(),
                resource: String::new(),
                current_value: String::new(),
                recommended_value: String::new(),
                savings_monthly: driver.blocked_monthly_cost.clone(),
                risk: "medium".to_string(),
                kubectl_command: String::new(),
                reason: driver.summary.clone(),
                source: "constraint_relaxation".to_string(),
                effort: "review".to_string(),
            });
        }
    }

    items.sort_by(|a, b| {
        b.savings_monthly
            .monthly
            .partial_cmp(&a.savings_monthly.monthly)
            .unwrap_or(Ordering::Equal)
    });
    for (i, item) in items.iter_mut().enumerate() {
        item.rank = (i + 1) as i32;
    }
    items.truncate(10);
    items
}

// @lineage
// reads: optimization.savings_monthly, drivers.[key=requests].blocked_monthly_cost, drivers.[constraint].blocked_monthly_cost
fn compute_savings_waterfall(
    optimization: Option<&OptimizationPlan>,
    drivers: &[InflationDriver],
) -> SavingsWaterfall {
    let placement_savings = optimization
        .map(|o| o.savings_monthly.monthly)
        .unwrap_or(0.0);

    let rightsizing_upside = drivers
        .iter()
        .find(|d| d.key == "requests")
        .map(|d| d.blocked_monthly_cost.monthly)
        .unwrap_or(0.0);

    let constraint_upside: f64 = drivers
        .iter()
        .filter(|d| {
            matches!(
                d.key.as_str(),
                "taints" | "anti-affinity" | "node-selectors" | "affinity"
            )
        })
        .map(|d| d.blocked_monthly_cost.monthly)
        .sum();

    let mut layers = Vec::new();
    let mut cumulative = 0.0;

    if placement_savings > 0.0 {
        cumulative += placement_savings;
        layers.push(SavingsLayer {
            key: "placement".to_string(),
            label: "Placement (strict constraints)".to_string(),
            savings: Money {
                currency: "USD".to_string(),
                monthly: placement_savings,
            },
            cumulative: Money {
                currency: "USD".to_string(),
                monthly: cumulative,
            },
            confidence: "high".to_string(),
        });
    }

    if rightsizing_upside > 0.0 {
        cumulative += rightsizing_upside;
        layers.push(SavingsLayer {
            key: "rightsizing".to_string(),
            label: "Request rightsizing".to_string(),
            savings: Money {
                currency: "USD".to_string(),
                monthly: rightsizing_upside,
            },
            cumulative: Money {
                currency: "USD".to_string(),
                monthly: cumulative,
            },
            confidence: "medium".to_string(),
        });
    }

    if constraint_upside > 0.0 {
        cumulative += constraint_upside;
        layers.push(SavingsLayer {
            key: "constraints".to_string(),
            label: "Constraint relaxation".to_string(),
            savings: Money {
                currency: "USD".to_string(),
                monthly: constraint_upside,
            },
            cumulative: Money {
                currency: "USD".to_string(),
                monthly: cumulative,
            },
            confidence: "low".to_string(),
        });
    }

    SavingsWaterfall {
        layers,
        total: Money {
            currency: "USD".to_string(),
            monthly: cumulative,
        },
    }
}

fn format_resource_pair(cpu_milli: i64, memory_bytes: i64) -> String {
    let cpu = format!("{}m", cpu_milli);
    let mem = format_bytes(memory_bytes);
    format!("{} CPU, {} mem", cpu, mem)
}

fn format_resource_delta(cpu_delta: i64, mem_delta: i64) -> String {
    let mut parts = Vec::new();
    if cpu_delta > 0 {
        parts.push(format!("{}m CPU", cpu_delta));
    }
    if mem_delta > 0 {
        parts.push(format_bytes(mem_delta));
    }
    if parts.is_empty() {
        "no change".to_string()
    } else {
        parts.join(" + ")
    }
}

fn format_bytes(bytes: i64) -> String {
    let gib = 1024 * 1024 * 1024;
    let mib = 1024 * 1024;
    if bytes >= gib {
        format!("{:.1}Gi", bytes as f64 / gib as f64)
    } else {
        format!("{}Mi", bytes / mib)
    }
}

// @lineage
// reads: pool_summaries.*, normalized.nodes.*, normalized.workloads.*
fn compute_pool_emptiability(
    pool_summaries: &[PoolSummary],
    normalized: &NormalizedCluster,
) -> Vec<PoolEmptiability> {
    let mut results = Vec::new();

    for pool in pool_summaries {
        let pool_name = &pool.pool;
        let pool_nodes: BTreeSet<&str> = normalized
            .nodes
            .iter()
            .filter(|n| &n.pool == pool_name)
            .map(|n| n.name.as_str())
            .collect();

        if pool_nodes.is_empty() {
            continue;
        }

        let mut pinned_workloads = Vec::new();
        let mut pin_reasons = Vec::new();

        for workload in &normalized.workloads {
            if !pool_nodes.contains(workload.current_node.as_str()) {
                continue;
            }
            let has_feasible_outside = workload
                .feasible_node_names
                .iter()
                .any(|n| !pool_nodes.contains(n.as_str()));

            if !has_feasible_outside {
                let name = format!("{}/{}", workload.namespace, workload.name);
                let reason = if workload.feasible_node_names.is_empty() {
                    "no feasible nodes at all".to_string()
                } else if workload.pinned_by_volume {
                    "pinned by persistent volume".to_string()
                } else if workload.has_required_affinity {
                    "required node affinity".to_string()
                } else if !workload.reasons.is_empty() {
                    workload.reasons.first().cloned().unwrap_or_default()
                } else {
                    "all feasible nodes are in this pool".to_string()
                };
                if !pin_reasons.contains(&reason) {
                    pin_reasons.push(reason);
                }
                pinned_workloads.push(name);
            }
        }

        let emptiable = pinned_workloads.is_empty();
        results.push(PoolEmptiability {
            pool: pool_name.clone(),
            emptiable,
            monthly_cost: pool.current_monthly_cost.clone(),
            node_count: pool.node_count,
            pinned_workloads,
            pin_reasons,
        });
    }

    results
}

// @lineage
// reads: normalized.nodes.*, normalized.workloads.*, optimization.active_nodes
// @lineage
// reads: snapshot.vpas, snapshot.vpa_recommender, normalized.workloads, workload_cost_table
fn build_vpa_overview(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
    workload_cost_table: &[WorkloadCostRow],
) -> VpaOverview {
    let total_workloads = normalized.workloads.len() as i32;
    let vpa_targets: BTreeSet<(String, String)> = snapshot
        .vpas
        .iter()
        .map(|v| (v.namespace.clone(), v.target_ref_name.clone()))
        .collect();

    let workloads_with_vpa = normalized
        .workloads
        .iter()
        .filter(|w| {
            if vpa_targets.contains(&(w.namespace.clone(), w.owner_name.clone())) {
                return true;
            }
            if w.owner_kind == "ReplicaSet" {
                if let Some(deploy_name) = strip_replicaset_hash(&w.owner_name) {
                    return vpa_targets.contains(&(w.namespace.clone(), deploy_name.to_string()));
                }
            }
            false
        })
        .count() as i32;

    let mut update_mode_counts: BTreeMap<String, i32> = BTreeMap::new();
    for vpa in &snapshot.vpas {
        let mode = if vpa.update_mode.is_empty() {
            "Off".to_string()
        } else {
            vpa.update_mode.clone()
        };
        *update_mode_counts.entry(mode).or_insert(0) += 1;
    }

    let overprovisioned_without_vpa: Vec<&WorkloadCostRow> = workload_cost_table
        .iter()
        .filter(|w| {
            w.savings_if_rightsized.monthly > 0.0
                && !vpa_targets.contains(&(w.namespace.clone(), w.owner_name.clone()))
        })
        .collect();
    let missing_waste: f64 = overprovisioned_without_vpa
        .iter()
        .map(|w| w.savings_if_rightsized.monthly)
        .sum();

    let margin = snapshot.vpa_recommender.safety_margin_fraction;
    let margin_driver_cost = snapshot
        .vpas
        .iter()
        .filter_map(|vpa| {
            let recs = &vpa.container_recommendations;
            if recs.is_empty() {
                return None;
            }
            let mut cost = 0.0_f64;
            for rec in recs {
                let margined_mem = rec.target.memory_bytes as f64;
                let raw_mem = margined_mem / (1.0 + margin);
                let delta = margined_mem - raw_mem;
                cost += delta / (16.0 * 1024.0 * 1024.0 * 1024.0) * 140.0;
            }
            Some(cost)
        })
        .sum::<f64>();

    VpaOverview {
        total_workloads,
        workloads_with_vpa,
        workloads_without_vpa_overprovisioned: overprovisioned_without_vpa.len() as i32,
        missing_vpa_monthly_waste: Money {
            currency: "USD".to_string(),
            monthly: missing_waste,
        },
        update_mode_counts,
        safety_margin_fraction: margin,
        safety_margin_cost: Money {
            currency: "USD".to_string(),
            monthly: margin_driver_cost,
        },
        recommender_args: snapshot.vpa_recommender.args.clone(),
    }
}

// @lineage
// reads: snapshot.vpas, normalized.workloads
fn build_vpa_update_mode_impacts(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
) -> Vec<VpaUpdateModeImpact> {
    snapshot
        .vpas
        .iter()
        .filter(|vpa| vpa.update_mode == "Off" || vpa.update_mode.is_empty())
        .filter_map(|vpa| {
            let matched = normalized
                .workloads
                .iter()
                .find(|w| w.namespace == vpa.namespace && w.owner_name == vpa.target_ref_name)?;
            let current_mem = matched.current_requests.memory_bytes;
            let target_mem: i64 = vpa
                .container_recommendations
                .iter()
                .map(|r| r.target.memory_bytes)
                .sum();
            if target_mem <= 0 || target_mem >= current_mem {
                return None;
            }
            let savings = ((current_mem - target_mem) as f64 / current_mem as f64) * 10.0;
            Some(VpaUpdateModeImpact {
                vpa_name: vpa.name.clone(),
                namespace: vpa.namespace.clone(),
                update_mode: if vpa.update_mode.is_empty() {
                    "Off".to_string()
                } else {
                    vpa.update_mode.clone()
                },
                target_workload: format!("{}/{}", vpa.namespace, vpa.target_ref_name),
                savings_if_auto: Money {
                    currency: "USD".to_string(),
                    monthly: savings,
                },
                kubectl_command: format!(
                    "kubectl patch vpa {} -n {} --type=merge -p '{{\"spec\":{{\"updatePolicy\":{{\"updateMode\":\"Auto\"}}}}}}'",
                    vpa.name, vpa.namespace
                ),
            })
        })
        .collect()
}

// @lineage
// reads: snapshot.vpas
fn build_vpa_policy_gaps(snapshot: &ClusterSnapshot) -> Vec<VpaPolicyGap> {
    let mut gaps = Vec::new();
    for vpa in &snapshot.vpas {
        for (policy, rec) in vpa
            .container_policies
            .iter()
            .zip(vpa.container_recommendations.iter())
        {
            if policy.max_allowed.memory_bytes > 0
                && rec.target.memory_bytes > policy.max_allowed.memory_bytes
            {
                gaps.push(VpaPolicyGap {
                    vpa_name: vpa.name.clone(),
                    namespace: vpa.namespace.clone(),
                    container_name: policy.container_name.clone(),
                    resource: "memory".to_string(),
                    current_request: rec.target.memory_bytes,
                    vpa_target: rec.uncapped_target.memory_bytes,
                    policy_bound: policy.max_allowed.memory_bytes,
                    bound_type: "max".to_string(),
                    gap_cost: Money::default(),
                    summary: format!(
                        "VPA wants {} but maxAllowed is {} — recommendation is capped",
                        format_bytes(rec.uncapped_target.memory_bytes),
                        format_bytes(policy.max_allowed.memory_bytes),
                    ),
                });
            }
            if policy.min_allowed.milli_cpu > 0
                && rec.target.milli_cpu < policy.min_allowed.milli_cpu
            {
                gaps.push(VpaPolicyGap {
                    vpa_name: vpa.name.clone(),
                    namespace: vpa.namespace.clone(),
                    container_name: policy.container_name.clone(),
                    resource: "cpu".to_string(),
                    current_request: policy.min_allowed.milli_cpu,
                    vpa_target: rec.target.milli_cpu,
                    policy_bound: policy.min_allowed.milli_cpu,
                    bound_type: "min".to_string(),
                    gap_cost: Money::default(),
                    summary: format!(
                        "VPA recommends {}m CPU but minAllowed is {}m — floor prevents optimal sizing",
                        rec.target.milli_cpu, policy.min_allowed.milli_cpu,
                    ),
                });
            }
        }
    }
    gaps
}

// @lineage
// reads: snapshot.vpas, normalized.workloads, workload_cost_table
fn build_missing_vpa_opportunities(
    snapshot: &ClusterSnapshot,
    _normalized: &NormalizedCluster,
    workload_cost_table: &[WorkloadCostRow],
) -> MissingVpaOpportunity {
    let vpa_targets: BTreeSet<(String, String)> = snapshot
        .vpas
        .iter()
        .map(|v| (v.namespace.clone(), v.target_ref_name.clone()))
        .collect();

    let without_vpa: Vec<&WorkloadCostRow> = workload_cost_table
        .iter()
        .filter(|w| !vpa_targets.contains(&(w.namespace.clone(), w.owner_name.clone())))
        .collect();
    let overprovisioned: Vec<&&WorkloadCostRow> = without_vpa
        .iter()
        .filter(|w| w.savings_if_rightsized.monthly > 0.0)
        .collect();
    let total_waste: f64 = overprovisioned
        .iter()
        .map(|w| w.savings_if_rightsized.monthly)
        .sum();

    MissingVpaOpportunity {
        workloads_without_vpa: without_vpa.len() as i32,
        overprovisioned_count: overprovisioned.len() as i32,
        total_waste: Money {
            currency: "USD".to_string(),
            monthly: total_waste,
        },
    }
}

// @lineage
// reads: snapshot.daemon_sets, normalized.nodes, optimization.active_nodes
fn compute_daemonset_consolidation_savings(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
    optimization: Option<&OptimizationPlan>,
) -> Money {
    if snapshot.daemon_sets.is_empty() {
        return Money::default();
    }
    let total_nodes = normalized.nodes.len() as f64;
    let active_nodes = optimization
        .map(|o| o.active_nodes.len() as f64)
        .unwrap_or(total_nodes);
    let removed = (total_nodes - active_nodes).max(0.0);
    if removed <= 0.0 {
        return Money::default();
    }

    let avg_price = if total_nodes > 0.0 {
        normalized
            .nodes
            .iter()
            .map(|n| n.price.monthly)
            .sum::<f64>()
            / total_nodes
    } else {
        0.0
    };

    let mut per_node_ds_cost_fraction = 0.0;
    for ds in &snapshot.daemon_sets {
        let avg_cap_cpu = normalized
            .nodes
            .iter()
            .map(|n| n.effective_capacity.milli_cpu as f64)
            .sum::<f64>()
            / total_nodes.max(1.0);
        let avg_cap_mem = normalized
            .nodes
            .iter()
            .map(|n| n.effective_capacity.memory_bytes as f64)
            .sum::<f64>()
            / total_nodes.max(1.0);
        let cpu_frac = if avg_cap_cpu > 0.0 {
            ds.requests.milli_cpu as f64 / avg_cap_cpu
        } else {
            0.0
        };
        let mem_frac = if avg_cap_mem > 0.0 {
            ds.requests.memory_bytes as f64 / avg_cap_mem
        } else {
            0.0
        };
        per_node_ds_cost_fraction += cpu_frac.max(mem_frac);
    }

    Money {
        currency: "USD".to_string(),
        monthly: removed * avg_price * per_node_ds_cost_fraction,
    }
}

// @lineage
// reads: snapshot.pdbs, normalized.workloads
fn compute_pdb_impacts(
    snapshot: &ClusterSnapshot,
    normalized: &NormalizedCluster,
) -> Vec<PdbImpact> {
    let mut impacts = Vec::new();
    for pdb in &snapshot.pdbs {
        let matched: Vec<&str> = normalized
            .workloads
            .iter()
            .filter(|w| w.namespace == pdb.namespace)
            .map(|w| w.name.as_str())
            .collect();
        let matched_count = matched.len() as i32;
        if matched_count == 0 {
            continue;
        }
        let min_avail = pdb.min_available.parse::<i32>().unwrap_or(0);
        let max_unavail = if pdb.max_unavailable.is_empty() {
            matched_count - min_avail
        } else {
            pdb.max_unavailable.parse::<i32>().unwrap_or(1)
        };
        let budget = if min_avail > 0 {
            matched_count - min_avail
        } else {
            max_unavail
        };
        let nodes: BTreeSet<&str> = normalized
            .workloads
            .iter()
            .filter(|w| w.namespace == pdb.namespace)
            .map(|w| w.current_node.as_str())
            .filter(|n| !n.is_empty())
            .collect();
        let summary = if budget <= 0 {
            format!(
                "PDB allows 0 disruptions — all {} pods must remain available. Blocks draining any of {} nodes.",
                matched_count,
                nodes.len()
            )
        } else {
            format!(
                "PDB allows {} simultaneous disruption(s) across {} pods on {} nodes.",
                budget,
                matched_count,
                nodes.len()
            )
        };
        impacts.push(PdbImpact {
            name: pdb.name.clone(),
            namespace: pdb.namespace.clone(),
            min_available: pdb.min_available.clone(),
            max_unavailable: pdb.max_unavailable.clone(),
            matched_pods: matched_count,
            disruption_budget: budget.max(0),
            affected_nodes: nodes.len() as i32,
            summary,
        });
    }
    impacts
}

fn compute_node_drainability(
    normalized: &NormalizedCluster,
    optimization: Option<&OptimizationPlan>,
) -> Vec<NodeDrainability> {
    let active_nodes: BTreeSet<&str> = optimization
        .map(|o| o.active_nodes.iter().map(|n| n.as_str()).collect())
        .unwrap_or_default();

    let mut results = Vec::new();
    for node in &normalized.nodes {
        if !active_nodes.is_empty() && !active_nodes.contains(node.name.as_str()) {
            continue;
        }
        let mut pinned = Vec::new();
        let mut reasons = Vec::new();
        let mut workload_count = 0_i32;

        for workload in &normalized.workloads {
            if workload.current_node != node.name {
                continue;
            }
            workload_count += 1;
            let has_alternative = workload.feasible_node_names.iter().any(|n| {
                n != &node.name && (active_nodes.is_empty() || active_nodes.contains(n.as_str()))
            });
            if !has_alternative {
                let name = format!("{}/{}", workload.namespace, workload.name);
                let reason = if workload.feasible_node_names.len() <= 1 {
                    "single feasible node".to_string()
                } else if workload.pinned_by_volume {
                    "volume topology".to_string()
                } else if workload.has_required_node_affinity {
                    "required node affinity".to_string()
                } else if !workload.reasons.is_empty() {
                    workload.reasons.first().cloned().unwrap_or_default()
                } else {
                    "no alternative nodes".to_string()
                };
                if !reasons.contains(&reason) {
                    reasons.push(reason);
                }
                pinned.push(name);
            }
        }

        results.push(NodeDrainability {
            node: node.name.clone(),
            pool: node.pool.clone(),
            monthly_cost: node.price.monthly,
            drainable: pinned.is_empty(),
            pinned_workloads: pinned,
            pin_reasons: reasons,
            workload_count,
        });
    }

    results.sort_by(|a, b| {
        a.drainable.cmp(&b.drainable).then_with(|| {
            b.monthly_cost
                .partial_cmp(&a.monthly_cost)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    });
    results
}

fn generate_kubectl_command(
    namespace: &str,
    owner_kind: &str,
    owner_name: &str,
    cpu_milli: i64,
    memory_bytes: i64,
) -> String {
    let lowered = owner_kind.to_lowercase();
    let kind = if lowered == "replicaset" {
        "deployment"
    } else {
        lowered.as_str()
    };
    if kind.is_empty() || owner_name.is_empty() {
        return String::new();
    }
    let mem = format_bytes(memory_bytes);
    format!(
        "kubectl set resources {} {} -n {} --requests=cpu={}m,memory={}",
        kind, owner_name, namespace, cpu_milli, mem
    )
}

#[cfg(test)]
mod tests {
    use super::ExplainabilityBuilder;
    use crate::model::{
        AutoscalerBlockerSummary, ClusterSnapshot, Money, NormalizedCluster, NormalizedNode,
        NormalizedWorkload, OptimizationPlan, PodMove, ResourceAllocation, ResourceList,
        ResourceSummary, ResourceUsage,
    };

    #[test]
    fn ranks_autoscaler_and_request_drivers() {
        let snapshot = ClusterSnapshot::default();
        let normalized = NormalizedCluster {
            current_monthly_cost: Money {
                currency: "USD".to_string(),
                monthly: 1000.0,
            },
            nodes: vec![NormalizedNode {
                name: "node-a".to_string(),
                allocatable: ResourceList {
                    pods: 32,
                    ..Default::default()
                },
                current_pods: 32,
                price: Money {
                    currency: "USD".to_string(),
                    monthly: 100.0,
                },
                ..Default::default()
            }],
            workloads: vec![NormalizedWorkload {
                namespace: "default".to_string(),
                name: "app".to_string(),
                owner_kind: "Deployment".to_string(),
                owner_name: "app".to_string(),
                current_node: "node-a".to_string(),
                current_requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 8 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                recommended_requests: ResourceList {
                    milli_cpu: 500,
                    memory_bytes: 4 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let plan = OptimizationPlan {
            current_monthly_cost: Money {
                currency: "USD".to_string(),
                monthly: 1000.0,
            },
            optimized_monthly_cost: Money {
                currency: "USD".to_string(),
                monthly: 800.0,
            },
            resource_summary: ResourceSummary {
                current: ResourceAllocation {
                    allocated_cpu_request_milli: 1000,
                    allocated_memory_request_bytes: 8 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                optimized: ResourceAllocation {
                    total_cpu_capacity_milli: 2000,
                    total_memory_capacity_bytes: 16 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                autoscaler_blockers: vec![AutoscalerBlockerSummary {
                    kind: "cluster autoscaler safe-to-evict=false".to_string(),
                    blocked_monthly_cost: Money {
                        currency: "USD".to_string(),
                        monthly: 400.0,
                    },
                    blocked_node_count: 2,
                    blocked_workload_count: 3,
                    blocked_nodes: vec!["node-a".to_string()],
                }],
                ..Default::default()
            },
            ..Default::default()
        };

        let report = ExplainabilityBuilder::default().build(&snapshot, &normalized, Some(&plan));

        assert_eq!(report.inflation_drivers.len(), 3);
        assert!(report
            .inflation_drivers
            .iter()
            .any(|driver| driver.key == "safe-to-evict-false"));
        assert!(report
            .inflation_drivers
            .iter()
            .any(|driver| driver.key == "requests"));
        assert_eq!(report.memory_risk.model, "observed_peak_proxy");
    }

    #[test]
    fn computes_current_and_optimized_memory_risk() {
        let snapshot = ClusterSnapshot::default();
        let normalized = NormalizedCluster {
            nodes: vec![
                NormalizedNode {
                    name: "node-a".to_string(),
                    effective_capacity: ResourceList {
                        memory_bytes: 100,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                NormalizedNode {
                    name: "node-b".to_string(),
                    effective_capacity: ResourceList {
                        memory_bytes: 100,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            workloads: vec![
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "a".to_string(),
                    current_node: "node-a".to_string(),
                    usage: ResourceUsage {
                        memory_bytes: 60,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "b".to_string(),
                    current_node: "node-b".to_string(),
                    usage: ResourceUsage {
                        memory_bytes: 50,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let plan = OptimizationPlan {
            active_nodes: vec!["node-a".to_string()],
            recommended_moves: vec![PodMove {
                namespace: "ns".to_string(),
                pod: "b".to_string(),
                from_node: "node-b".to_string(),
                to_node: "node-a".to_string(),
                reason: String::new(),
            }],
            ..Default::default()
        };

        let report = ExplainabilityBuilder::default().build(&snapshot, &normalized, Some(&plan));

        assert!((report.memory_risk.current.max_pressure_percent - 60.0).abs() < 0.001);
        assert_eq!(report.memory_risk.current.risk, "low");
        assert!((report.memory_risk.optimized.max_pressure_percent - 110.0).abs() < 0.001);
        assert_eq!(report.memory_risk.optimized.risk, "critical");
        assert_eq!(report.memory_risk.optimized.overflow_node_count, 1);
    }

    #[test]
    fn node_drainability_detects_pinned_workloads() {
        let normalized = NormalizedCluster {
            nodes: vec![
                NormalizedNode {
                    name: "node-a".to_string(),
                    price: Money {
                        currency: "USD".to_string(),
                        monthly: 100.0,
                    },
                    effective_capacity: ResourceList {
                        milli_cpu: 4000,
                        memory_bytes: 8192,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                NormalizedNode {
                    name: "node-b".to_string(),
                    price: Money {
                        currency: "USD".to_string(),
                        monthly: 100.0,
                    },
                    effective_capacity: ResourceList {
                        milli_cpu: 4000,
                        memory_bytes: 8192,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            workloads: vec![
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "movable".to_string(),
                    current_node: "node-a".to_string(),
                    feasible_node_names: vec!["node-a".to_string(), "node-b".to_string()],
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "pinned".to_string(),
                    current_node: "node-a".to_string(),
                    feasible_node_names: vec!["node-a".to_string()],
                    reasons: vec!["node selector mismatch".to_string()],
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let plan = OptimizationPlan {
            active_nodes: vec!["node-a".to_string(), "node-b".to_string()],
            ..Default::default()
        };
        let report = ExplainabilityBuilder::default().build(
            &ClusterSnapshot::default(),
            &normalized,
            Some(&plan),
        );
        assert_eq!(report.node_drainability.len(), 2);
        let node_a = report
            .node_drainability
            .iter()
            .find(|n| n.node == "node-a")
            .unwrap();
        assert!(!node_a.drainable);
        assert_eq!(node_a.pinned_workloads.len(), 1);
        assert_eq!(node_a.pinned_workloads[0], "ns/pinned");
        let node_b = report
            .node_drainability
            .iter()
            .find(|n| n.node == "node-b")
            .unwrap();
        assert!(node_b.drainable);
    }

    #[test]
    fn pdb_impact_computes_disruption_budget() {
        let snapshot = ClusterSnapshot {
            pdbs: vec![crate::model::DisruptionBudget {
                namespace: "ns".to_string(),
                name: "test-pdb".to_string(),
                min_available: "2".to_string(),
                max_unavailable: String::new(),
            }],
            ..Default::default()
        };
        let normalized = NormalizedCluster {
            workloads: vec![
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "pod-1".to_string(),
                    current_node: "n1".to_string(),
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "pod-2".to_string(),
                    current_node: "n2".to_string(),
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "pod-3".to_string(),
                    current_node: "n3".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let report = ExplainabilityBuilder::default().build(&snapshot, &normalized, None);
        assert_eq!(report.pdb_impacts.len(), 1);
        assert_eq!(report.pdb_impacts[0].matched_pods, 3);
        assert_eq!(report.pdb_impacts[0].disruption_budget, 1);
        assert_eq!(report.pdb_impacts[0].affected_nodes, 3);
    }

    #[test]
    fn workload_cost_table_deduplicates_by_owner() {
        let normalized = NormalizedCluster {
            nodes: vec![NormalizedNode {
                name: "n1".to_string(),
                price: Money {
                    currency: "USD".to_string(),
                    monthly: 100.0,
                },
                effective_capacity: ResourceList {
                    milli_cpu: 4000,
                    memory_bytes: 16 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            }],
            workloads: vec![NormalizedWorkload {
                namespace: "ns".to_string(),
                name: "app-abc".to_string(),
                owner_kind: "ReplicaSet".to_string(),
                owner_name: "app-abc".to_string(),
                current_node: "n1".to_string(),
                current_requests: ResourceList {
                    milli_cpu: 500,
                    memory_bytes: 1024 * 1024 * 1024,
                    ..Default::default()
                },
                recommended_requests: ResourceList {
                    milli_cpu: 500,
                    memory_bytes: 512 * 1024 * 1024,
                    ..Default::default()
                },
                requests: ResourceList {
                    milli_cpu: 500,
                    memory_bytes: 1024 * 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let report =
            ExplainabilityBuilder::default().build(&ClusterSnapshot::default(), &normalized, None);
        assert!(!report.workload_cost_table.is_empty());
        let row = &report.workload_cost_table[0];
        assert_eq!(row.current_memory_bytes, 1024 * 1024 * 1024);
        assert_eq!(row.recommended_memory_bytes, 512 * 1024 * 1024);
        assert!(
            row.monthly_cost.monthly > 0.0,
            "monthly cost should be positive: {}",
            row.monthly_cost.monthly
        );
        assert!(row.savings_if_rightsized.monthly >= 0.0);
    }

    #[test]
    fn savings_waterfall_includes_all_layers() {
        let normalized = NormalizedCluster {
            constraint_impacts: vec![crate::model::ConstraintImpact {
                name: "taints/tolerations".to_string(),
                workloads_affected: 5,
                reason_count: 5,
                estimated_monthly: 50.0,
            }],
            ..Default::default()
        };
        let plan = OptimizationPlan {
            savings_monthly: Money {
                currency: "USD".to_string(),
                monthly: 200.0,
            },
            ..Default::default()
        };
        let report = ExplainabilityBuilder::default().build(
            &ClusterSnapshot::default(),
            &normalized,
            Some(&plan),
        );
        assert!(report.savings_waterfall.layers.len() >= 2);
        assert_eq!(report.savings_waterfall.layers[0].key, "placement");
        assert_eq!(report.savings_waterfall.layers[0].savings.monthly, 200.0);
        let constraint_layer = report
            .savings_waterfall
            .layers
            .iter()
            .find(|l| l.key == "constraints");
        assert!(constraint_layer.is_some());
        assert_eq!(constraint_layer.unwrap().savings.monthly, 50.0);
    }

    #[test]
    fn vpa_overview_counts_coverage_and_modes() {
        let snapshot = ClusterSnapshot {
            vpas: vec![
                crate::model::VerticalPodAutoscaler {
                    namespace: "ns".to_string(),
                    name: "vpa-1".to_string(),
                    target_ref_name: "app-a".to_string(),
                    update_mode: "Auto".to_string(),
                    ..Default::default()
                },
                crate::model::VerticalPodAutoscaler {
                    namespace: "ns".to_string(),
                    name: "vpa-2".to_string(),
                    target_ref_name: "app-b".to_string(),
                    update_mode: "Off".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let normalized = NormalizedCluster {
            workloads: vec![
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "app-a-xyz".to_string(),
                    owner_name: "app-a".to_string(),
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "app-b-xyz".to_string(),
                    owner_name: "app-b".to_string(),
                    ..Default::default()
                },
                NormalizedWorkload {
                    namespace: "ns".to_string(),
                    name: "app-c-xyz".to_string(),
                    owner_name: "app-c".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let report = ExplainabilityBuilder::default().build(&snapshot, &normalized, None);
        assert_eq!(report.vpa_overview.total_workloads, 3);
        assert_eq!(report.vpa_overview.workloads_with_vpa, 2);
        assert_eq!(report.vpa_overview.update_mode_counts.get("Auto"), Some(&1));
        assert_eq!(report.vpa_overview.update_mode_counts.get("Off"), Some(&1));
    }

    #[test]
    fn vpa_update_mode_detects_off_vpas() {
        let snapshot = ClusterSnapshot {
            vpas: vec![crate::model::VerticalPodAutoscaler {
                namespace: "ns".to_string(),
                name: "vpa-off".to_string(),
                target_ref_name: "worker".to_string(),
                update_mode: "Off".to_string(),
                container_recommendations: vec![crate::model::VpaContainerRecommendation {
                    target: ResourceList {
                        milli_cpu: 200,
                        memory_bytes: 512 * 1024 * 1024,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let normalized = NormalizedCluster {
            workloads: vec![NormalizedWorkload {
                namespace: "ns".to_string(),
                name: "worker-abc".to_string(),
                owner_name: "worker".to_string(),
                current_requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 2 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let report = ExplainabilityBuilder::default().build(&snapshot, &normalized, None);
        assert_eq!(report.vpa_update_mode_impacts.len(), 1);
        assert_eq!(report.vpa_update_mode_impacts[0].update_mode, "Off");
        assert!(report.vpa_update_mode_impacts[0]
            .kubectl_command
            .contains("Auto"));
    }

    #[test]
    fn vpa_policy_gap_detects_capped_recommendations() {
        let snapshot = ClusterSnapshot {
            vpas: vec![crate::model::VerticalPodAutoscaler {
                namespace: "ns".to_string(),
                name: "vpa-capped".to_string(),
                container_policies: vec![crate::model::VpaContainerPolicy {
                    container_name: "main".to_string(),
                    max_allowed: ResourceList {
                        memory_bytes: 1024 * 1024 * 1024,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                container_recommendations: vec![crate::model::VpaContainerRecommendation {
                    target: ResourceList {
                        memory_bytes: 2 * 1024 * 1024 * 1024,
                        ..Default::default()
                    },
                    uncapped_target: ResourceList {
                        memory_bytes: 4 * 1024 * 1024 * 1024,
                        ..Default::default()
                    },
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };
        let report =
            ExplainabilityBuilder::default().build(&snapshot, &NormalizedCluster::default(), None);
        assert_eq!(report.vpa_policy_gaps.len(), 1);
        assert_eq!(report.vpa_policy_gaps[0].bound_type, "max");
        assert!(report.vpa_policy_gaps[0].summary.contains("capped"));
    }

    #[test]
    fn missing_vpa_detects_overprovisioned_without_vpa() {
        let snapshot = ClusterSnapshot::default();
        let normalized = NormalizedCluster {
            nodes: vec![NormalizedNode {
                name: "n1".to_string(),
                price: Money {
                    currency: "USD".to_string(),
                    monthly: 100.0,
                },
                effective_capacity: ResourceList {
                    milli_cpu: 4000,
                    memory_bytes: 16 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            }],
            workloads: vec![NormalizedWorkload {
                namespace: "ns".to_string(),
                name: "bloated".to_string(),
                owner_name: "bloated-deploy".to_string(),
                current_node: "n1".to_string(),
                current_requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                recommended_requests: ResourceList {
                    milli_cpu: 500,
                    memory_bytes: 1024 * 1024 * 1024,
                    ..Default::default()
                },
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 4 * 1024 * 1024 * 1024,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let report = ExplainabilityBuilder::default().build(&snapshot, &normalized, None);
        assert_eq!(report.missing_vpa_opportunities.overprovisioned_count, 1);
        assert!(report.missing_vpa_opportunities.total_waste.monthly > 0.0);
    }
}
