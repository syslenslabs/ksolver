use crate::model::{OptimizationInput, OptimizationSolution, ScenarioConfig, SolverInfo};

#[cfg(feature = "rust-cp-sat")]
mod enabled {
    use super::{OptimizationInput, OptimizationSolution, ScenarioConfig, SolverInfo};
    use anyhow::{bail, Result};
    use cp_sat::builder::{BoolVar, CpModelBuilder, IntVar, LinearExpr};
    use cp_sat::ffi::cp_solver_response_stats;
    use cp_sat::proto::{CpSolverStatus, SatParameters};
    use std::collections::{HashMap, HashSet};

    pub fn solver_info() -> SolverInfo {
        SolverInfo {
            name: "cp-sat-rust".to_string(),
            available: true,
            status: "available via cp_sat crate".to_string(),
        }
    }

    pub fn solve(
        input: &OptimizationInput,
        scenario: &ScenarioConfig,
    ) -> Result<(OptimizationSolution, SolverInfo)> {
        if scenario.partial_admission && scenario.enable_joint_rightsizing {
            bail!("partial_admission is incompatible with enable_joint_rightsizing");
        }
        let mut model = CpModelBuilder::default();
        let mut y_vars = HashMap::new();
        let mut x_vars = HashMap::new();
        let mut cpu_slack_vars: HashMap<String, IntVar> = HashMap::new();
        let mut mem_slack_vars: HashMap<String, IntVar> = HashMap::new();
        let mut scalar_slack_vars: HashMap<(String, String), IntVar> = HashMap::new();

        for node in &input.nodes {
            let var = model.new_int_var_with_name(
                [(0, i64::from(node.count))],
                format!("y_{}", sanitize(&node.name)),
            );
            y_vars.insert(node.name.clone(), var);
        }

        for workload in &input.workloads {
            if workload.feasible_nodes.is_empty() {
                bail!("workload {} has no feasible nodes", workload.id);
            }
            let upper = i64::from(workload.group_size).max(0);
            for node_name in &workload.feasible_nodes {
                let var = model.new_int_var_with_name(
                    [(0, upper)],
                    format!("x_{}__{}", sanitize(&workload.id), sanitize(node_name)),
                );
                x_vars.insert((workload.id.clone(), node_name.clone()), var);
            }
        }

        // Level selection variables for request quantization
        // level_vars[workload_id] = vec of (level_key, BoolVar) — exactly one selected
        // x_level_vars[(workload_id, level_key, node_name)] = IntVar — replicas at this level on this node
        let mut level_vars: HashMap<String, Vec<(String, BoolVar)>> = HashMap::new();
        let mut x_level_vars: HashMap<(String, String, String), IntVar> = HashMap::new();

        if scenario.enable_joint_rightsizing {
            for workload in &input.workloads {
                if workload.candidate_levels.len() < 2 {
                    continue;
                }
                let upper = i64::from(workload.group_size);
                let mut lvars = Vec::new();
                for level in &workload.candidate_levels {
                    let bv = model.new_bool_var_with_name(format!(
                        "level_{}__{}",
                        sanitize(&workload.id),
                        sanitize(&level.key)
                    ));
                    lvars.push((level.key.clone(), bv));

                    for node_name in &workload.feasible_nodes {
                        let xv = model.new_int_var_with_name(
                            [(0, upper)],
                            format!(
                                "xl_{}__{}_{}",
                                sanitize(&workload.id),
                                sanitize(&level.key),
                                sanitize(node_name)
                            ),
                        );
                        x_level_vars.insert(
                            (workload.id.clone(), level.key.clone(), node_name.clone()),
                            xv,
                        );
                        model.add_le(xv, (upper, bv));
                    }
                }
                let bool_vars: Vec<BoolVar> = lvars.iter().map(|(_, bv)| *bv).collect();
                model.add_exactly_one(bool_vars);
                model.add_hint(lvars[0].1, 1_i64);
                for (_, bv) in &lvars[1..] {
                    model.add_hint(*bv, 0_i64);
                }
                // Link: sum of x_level across levels = x for each node
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    let level_sum: LinearExpr = workload
                        .candidate_levels
                        .iter()
                        .map(|l| {
                            x_level_vars[&(workload.id.clone(), l.key.clone(), node_name.clone())]
                        })
                        .collect();
                    model.add_eq(level_sum, x);
                }
                level_vars.insert(workload.id.clone(), lvars);
            }
        }

        let mut placed_vars: HashMap<String, BoolVar> = HashMap::new();
        for workload in &input.workloads {
            let group_size = i64::from(workload.group_size);
            let sum_expr: LinearExpr = workload
                .feasible_nodes
                .iter()
                .map(|node_name| x_vars[&(workload.id.clone(), node_name.clone())])
                .collect();
            if scenario.partial_admission && group_size > 0 {
                // All-or-nothing admission: sum of replicas == group_size * placed.
                let placed =
                    model.new_bool_var_with_name(format!("placed_{}", sanitize(&workload.id)));
                model.add_eq(sum_expr, (group_size, placed));
                placed_vars.insert(workload.id.clone(), placed);
            } else {
                model.add_eq(sum_expr, group_size);
            }

            for node_name in &workload.feasible_nodes {
                let x = x_vars[&(workload.id.clone(), node_name.clone())];
                let y = y_vars[node_name];
                model.add_le(x, (group_size, y));
            }

            if workload.colocate && group_size > 0 {
                // Single-node co-location: at most one node may hold this gang's
                // replicas, so combined with the latch (sum x = group_size * placed)
                // an admitted gang lands entirely on one node.
                let mut used_sum = LinearExpr::default();
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    let used = model.new_bool_var_with_name(format!(
                        "used_{}__{}",
                        sanitize(&workload.id),
                        sanitize(node_name)
                    ));
                    // x > 0  =>  used = 1
                    model.add_le(x, (group_size, used));
                    used_sum += used;
                }
                if let Some(placed) = placed_vars.get(&workload.id) {
                    model.add_le(used_sum, *placed);
                } else {
                    model.add_le(used_sum, 1_i64);
                }
            }
        }

        if !scenario.relax_required_anti_affinity {
            let anti_affinity_ids: HashSet<&str> = input
                .anti_affinity_pairs
                .iter()
                .map(|(id, _)| id.as_str())
                .collect();
            for workload in &input.workloads {
                if workload.group_size <= 1 {
                    continue;
                }
                let has_anti = anti_affinity_ids.contains(workload.id.as_str())
                    || workload.members.iter().any(|m| {
                        let key = format!("{}/{}", m.namespace, m.name);
                        anti_affinity_ids.contains(key.as_str())
                    });
                if !has_anti {
                    continue;
                }
                for node_name in &workload.feasible_nodes {
                    let x = x_vars[&(workload.id.clone(), node_name.clone())];
                    model.add_le(x, 1_i64);
                }
            }
        }

        let mut hinted_assignments = 0_i64;
        let mut hinted_nodes = HashSet::new();
        for workload in &input.workloads {
            for node_name in &workload.feasible_nodes {
                let count = i64::from(*workload.current_counts.get(node_name).unwrap_or(&0));
                let x = x_vars[&(workload.id.clone(), node_name.clone())];
                model.add_hint(x, count);
                if count > 0 {
                    hinted_assignments += count;
                    hinted_nodes.insert(node_name.clone());
                }
            }
        }

        for node in &input.nodes {
            let total: i64 = input
                .workloads
                .iter()
                .map(|w| i64::from(*w.current_counts.get(&node.name).unwrap_or(&0)))
                .sum();
            let pod_cap = node.effective_capacity.pods;
            let mut active_hint = if hinted_nodes.contains(&node.name) {
                1
            } else {
                0
            };
            if pod_cap > 0 && total > 0 {
                active_hint = std::cmp::min(
                    i64::from(node.count),
                    std::cmp::max(1, ceil_div(total, pod_cap)),
                );
            }
            model.add_hint(y_vars[&node.name], active_hint);
        }

        for node in &input.nodes {
            let y = y_vars[&node.name];
            let mut cpu = LinearExpr::default();
            let mut mem = LinearExpr::default();
            let mut disk = LinearExpr::default();
            let mut pods = LinearExpr::default();

            for workload in &input.workloads {
                if !workload.feasible_nodes.iter().any(|n| n == &node.name) {
                    continue;
                }
                let x = x_vars[&(workload.id.clone(), node.name.clone())];
                pods += x;

                if level_vars.contains_key(&workload.id) {
                    let gs = i64::from(workload.group_size).max(1);
                    for level in &workload.candidate_levels {
                        let xl = x_level_vars
                            [&(workload.id.clone(), level.key.clone(), node.name.clone())];
                        let cpu_per = level.requests.milli_cpu / gs;
                        let mem_per = level.requests.memory_bytes / gs;
                        let disk_per = level.requests.ephemeral_storage / gs;
                        if cpu_per != 0 {
                            cpu += (cpu_per, xl);
                        }
                        if mem_per != 0 {
                            mem += (mem_per, xl);
                        }
                        if disk_per != 0 {
                            disk += (disk_per, xl);
                        }
                    }
                } else {
                    let req = per_replica_requests(workload);
                    if req.milli_cpu != 0 {
                        cpu += (req.milli_cpu, x);
                    }
                    if req.memory_bytes != 0 {
                        mem += (req.memory_bytes, x);
                    }
                    if req.ephemeral_storage != 0 {
                        disk += (req.ephemeral_storage, x);
                    }
                }
            }

            if node.effective_capacity.milli_cpu > 0 {
                model.add_le(cpu.clone(), (node.effective_capacity.milli_cpu, y));
                let slack = model.new_int_var_with_name(
                    [(0, node.effective_capacity.milli_cpu * i64::from(node.count))],
                    format!("cpu_slack_{}", sanitize(&node.name)),
                );
                model.add_eq(cpu + slack, (node.effective_capacity.milli_cpu, y));
                cpu_slack_vars.insert(node.name.clone(), slack);
            }
            if node.effective_capacity.memory_bytes > 0 {
                model.add_le(mem.clone(), (node.effective_capacity.memory_bytes, y));
                let slack = model.new_int_var_with_name(
                    [(
                        0,
                        node.effective_capacity.memory_bytes * i64::from(node.count),
                    )],
                    format!("mem_slack_{}", sanitize(&node.name)),
                );
                model.add_eq(mem + slack, (node.effective_capacity.memory_bytes, y));
                mem_slack_vars.insert(node.name.clone(), slack);
            }
            if node.effective_capacity.ephemeral_storage > 0 {
                model.add_le(disk, (node.effective_capacity.ephemeral_storage, y));
            }
            if node.effective_capacity.pods > 0 {
                model.add_le(pods, (node.effective_capacity.pods, y));
            }
            for (resource_name, capacity) in &node.extended_resources {
                if *capacity <= 0 {
                    continue;
                }
                let mut scalar = LinearExpr::default();
                for workload in &input.workloads {
                    if !workload.feasible_nodes.iter().any(|n| n == &node.name) {
                        continue;
                    }
                    let per_replica_scalar = per_replica_scalar_requests(workload);
                    let Some(req) = per_replica_scalar.get(resource_name) else {
                        continue;
                    };
                    if *req == 0 {
                        continue;
                    }
                    let x = x_vars[&(workload.id.clone(), node.name.clone())];
                    scalar += (*req, x);
                }
                model.add_le(scalar.clone(), (*capacity, y));
                let slack = model.new_int_var_with_name(
                    [(0, *capacity * i64::from(node.count))],
                    format!(
                        "scalar_slack_{}_{}",
                        sanitize(&node.name),
                        sanitize(resource_name)
                    ),
                );
                model.add_eq(scalar + slack, (*capacity, y));
                scalar_slack_vars.insert((node.name.clone(), resource_name.clone()), slack);
            }
        }

        let mut objective = LinearExpr::default();
        for node in &input.nodes {
            let coeff = (node.price.monthly * scenario.cost_weight as f64).round() as i64;
            if coeff != 0 {
                objective += (coeff, y_vars[&node.name]);
            }
            objective += (scenario.active_node_weight, y_vars[&node.name]);
            if let Some(mem_slack) = mem_slack_vars.get(&node.name) {
                objective += (scenario.memory_slack_weight, *mem_slack);
            }
            if let Some(cpu_slack) = cpu_slack_vars.get(&node.name) {
                objective += (scenario.cpu_slack_weight, *cpu_slack);
            }
            for resource_name in node.extended_resources.keys() {
                if let Some(slack) =
                    scalar_slack_vars.get(&(node.name.clone(), resource_name.clone()))
                {
                    objective += (scenario.memory_slack_weight, *slack);
                }
            }
        }
        for workload in &input.workloads {
            for node_name in &workload.feasible_nodes {
                let current_count =
                    i64::from(*workload.current_counts.get(node_name).unwrap_or(&0));
                if current_count > 0 {
                    objective -= (
                        scenario.churn_weight,
                        x_vars[&(workload.id.clone(), node_name.clone())],
                    );
                }
            }
        }
        for (workload_id, lvars) in &level_vars {
            for (level_key, bv) in lvars {
                let workload = input.workloads.iter().find(|w| &w.id == workload_id);
                let risk = workload
                    .and_then(|w| w.candidate_levels.iter().find(|l| &l.key == level_key))
                    .map(|l| l.risk_score)
                    .unwrap_or(0);
                if risk > 0 {
                    objective += (scenario.rightsizing_weight * risk / 100, *bv);
                }
            }
        }
        let effective_admission_weight = if scenario.partial_admission && !placed_vars.is_empty() {
            // Conservative upper bound (i128) of the max magnitude of ALL non-admission
            // objective terms. Node terms use int vars y in [0, node.count], so every
            // per-node term scales by node.count; slack <= capacity * count. Use the
            // absolute value of each weight (weights are not validated nonnegative).
            let mut rest_bound: i128 = 0;
            for node in &input.nodes {
                let count = i128::from(node.count.max(0));
                let cost_coeff =
                    ((node.price.monthly * scenario.cost_weight as f64).round() as i128).abs();
                rest_bound = rest_bound.saturating_add(cost_coeff.saturating_mul(count));
                rest_bound = rest_bound.saturating_add(
                    (scenario.active_node_weight as i128)
                        .abs()
                        .saturating_mul(count),
                );
                rest_bound = rest_bound.saturating_add(
                    (scenario.memory_slack_weight as i128)
                        .abs()
                        .saturating_mul((node.effective_capacity.memory_bytes as i128).max(0))
                        .saturating_mul(count),
                );
                rest_bound = rest_bound.saturating_add(
                    (scenario.cpu_slack_weight as i128)
                        .abs()
                        .saturating_mul((node.effective_capacity.milli_cpu as i128).max(0))
                        .saturating_mul(count),
                );
                for cap in node.extended_resources.values() {
                    rest_bound = rest_bound.saturating_add(
                        (scenario.memory_slack_weight as i128)
                            .abs()
                            .saturating_mul((*cap).max(0) as i128)
                            .saturating_mul(count),
                    );
                }
            }
            // Churn reward: -churn_weight * x on edges with current_count>0; sum of x over
            // nodes == group_size, so per workload the magnitude is <= churn_weight*group_size.
            for workload in &input.workloads {
                let gs = i128::from(workload.group_size.max(0));
                rest_bound = rest_bound
                    .saturating_add((scenario.churn_weight as i128).abs().saturating_mul(gs));
            }
            let w: i128 = if scenario.admission_weight > 0 {
                let explicit = scenario.admission_weight as i128;
                if explicit <= rest_bound {
                    bail!("admission_weight {explicit} does not dominate objective bound {rest_bound}; use 0 for auto or a larger value");
                }
                explicit
            } else {
                // rightsizing terms cannot coexist (guarded above), so rest_bound covers
                // the full objective. saturating_add guards the i128::MAX saturated case.
                rest_bound.saturating_add(1)
            };
            let n = placed_vars.len() as i128;
            let total = w
                .checked_mul(n)
                .and_then(|v| v.checked_add(rest_bound))
                .unwrap_or(i128::MAX);
            if w > i64::MAX as i128 || total > i64::MAX as i128 {
                bail!("partial_admission weight would overflow i64 objective (workloads={n}); reduce scope or set a smaller admission_weight");
            }
            w as i64
        } else {
            0
        };
        for placed in placed_vars.values() {
            // Reward admitting a workload; weight dominates the rest of the objective so
            // the solver maximizes admitted count first, then minimizes cost.
            objective -= (effective_admission_weight, *placed);
        }

        model.minimize(objective);

        let validation = model.validate_cp_model();
        if !validation.is_empty() {
            bail!("cp-sat rust model validation failed: {validation}");
        }

        let worker_count = recommended_worker_count(input);
        let params = SatParameters {
            max_time_in_seconds: Some(600.0),
            num_search_workers: Some(worker_count),
            ..SatParameters::default()
        };
        let response = model.solve_with_parameters(&params);
        let status = response.status();
        let stats = cp_solver_response_stats(&response, true);

        if status != CpSolverStatus::Optimal && status != CpSolverStatus::Feasible {
            bail!("cp-sat rust solve failed with status {status:?}; {stats}");
        }

        let mut solution = OptimizationSolution::default();
        for workload in &input.workloads {
            let mut best_name = String::new();
            let mut best_value = -1_i64;
            let mut counts = HashMap::new();
            for node_name in &workload.feasible_nodes {
                let value =
                    x_vars[&(workload.id.clone(), node_name.clone())].solution_value(&response);
                if value > 0 {
                    counts.insert(node_name.clone(), value as i32);
                }
                if value > best_value {
                    best_value = value;
                    best_name = node_name.clone();
                }
            }
            if !counts.is_empty() {
                solution
                    .assignment_counts
                    .insert(workload.id.clone(), counts);
            }
            if best_value > 0 {
                solution.assignments.insert(workload.id.clone(), best_name);
            }
        }
        for node in &input.nodes {
            solution.active_nodes.insert(
                node.name.clone(),
                y_vars[&node.name].solution_value(&response) as i32,
            );
        }
        for (workload_id, lvars) in &level_vars {
            for (level_key, bv) in lvars {
                if bv.solution_value(&response) {
                    solution
                        .selected_levels
                        .insert(workload_id.clone(), level_key.clone());
                    if level_key != "current" {
                        solution.rightsized_workloads.push(workload_id.clone());
                    }
                }
            }
        }

        Ok((
            solution,
            SolverInfo {
                name: "cp-sat-rust".to_string(),
                available: true,
                status: format!(
                    "status={status:?}; workers={worker_count}; hinted_assignments={hinted_assignments}; hinted_nodes={}; cost_weight={}; active_node_weight={}; memory_slack_weight={}; cpu_slack_weight={}; churn_weight={}; partial_admission={}; admission_weight={effective_admission_weight}; {stats}",
                    hinted_nodes.len(),
                    scenario.cost_weight,
                    scenario.active_node_weight,
                    scenario.memory_slack_weight,
                    scenario.cpu_slack_weight,
                    scenario.churn_weight,
                    scenario.partial_admission
                ),
            },
        ))
    }

    fn sanitize(value: &str) -> String {
        value
            .chars()
            .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '_' })
            .collect()
    }

    fn ceil_div(a: i64, b: i64) -> i64 {
        (a + b - 1) / b
    }

    fn per_replica_requests(
        workload: &crate::model::OptimizationWorkload,
    ) -> crate::model::ResourceList {
        if workload.group_size <= 1 {
            return workload.requests.clone();
        }
        let group_size = i64::from(workload.group_size);
        crate::model::ResourceList {
            milli_cpu: workload.requests.milli_cpu / group_size,
            memory_bytes: workload.requests.memory_bytes / group_size,
            ephemeral_storage: workload.requests.ephemeral_storage / group_size,
            pods: 1,
        }
    }

    fn per_replica_scalar_requests(
        workload: &crate::model::OptimizationWorkload,
    ) -> std::collections::BTreeMap<String, i64> {
        if workload.group_size <= 1 {
            return workload.extended_resource_requests.clone();
        }
        let group_size = i64::from(workload.group_size);
        workload
            .extended_resource_requests
            .iter()
            .map(|(name, value)| (name.clone(), *value / group_size))
            .collect()
    }

    pub(crate) fn recommended_worker_count(input: &OptimizationInput) -> i32 {
        let node_count = input.nodes.len();
        let workload_count = input.workloads.len();
        let assignment_edges: usize = input.workloads.iter().map(|w| w.feasible_nodes.len()).sum();

        if workload_count >= 8_000 || assignment_edges >= 200_000 || node_count >= 96 {
            return 1;
        }
        if workload_count >= 3_000 || assignment_edges >= 75_000 || node_count >= 48 {
            return 2;
        }
        if workload_count >= 1_000 || assignment_edges >= 25_000 {
            return 4;
        }
        8
    }
}

#[cfg(not(feature = "rust-cp-sat"))]
mod enabled {
    use super::{OptimizationInput, OptimizationSolution, ScenarioConfig, SolverInfo};
    use anyhow::{bail, Result};

    pub fn solver_info() -> SolverInfo {
        SolverInfo {
            name: "cp-sat-rust".to_string(),
            available: false,
            status: "unavailable: build with --features rust-cp-sat and provide OR-Tools"
                .to_string(),
        }
    }

    pub fn solve(
        _input: &OptimizationInput,
        _scenario: &ScenarioConfig,
    ) -> Result<(OptimizationSolution, SolverInfo)> {
        bail!("cp-sat-rust unavailable: build with --features rust-cp-sat and provide OR-Tools")
    }
}

pub use enabled::{solve, solver_info};

#[cfg(all(test, feature = "rust-cp-sat"))]
mod tests {
    use super::enabled::recommended_worker_count;
    use crate::model::{OptimizationInput, OptimizationNode, OptimizationWorkload};

    #[test]
    fn large_models_use_single_worker() {
        let input = OptimizationInput {
            nodes: (0..100)
                .map(|i| OptimizationNode {
                    name: format!("n-{i}"),
                    count: 1,
                    ..Default::default()
                })
                .collect(),
            workloads: (0..8_000)
                .map(|i| OptimizationWorkload {
                    id: format!("w-{i}"),
                    feasible_nodes: vec!["n-0".to_string()],
                    ..Default::default()
                })
                .collect(),
            anti_affinity_pairs: Vec::new(),
        };

        assert_eq!(recommended_worker_count(&input), 1);
    }

    fn two_competing_gpu_pods() -> OptimizationInput {
        use crate::model::ResourceList;
        use std::collections::BTreeMap;
        let mut node_ext = BTreeMap::new();
        node_ext.insert("nvidia.com/gpu".to_string(), 1);
        let node = OptimizationNode {
            name: "n1".to_string(),
            count: 1,
            members: vec!["n1".to_string()],
            effective_capacity: ResourceList {
                milli_cpu: 8000,
                memory_bytes: 32 << 30,
                ephemeral_storage: 0,
                pods: 110,
            },
            extended_resources: node_ext,
            ..Default::default()
        };
        let mk = |name: &str| {
            let mut ext = BTreeMap::new();
            ext.insert("nvidia.com/gpu".to_string(), 1);
            OptimizationWorkload {
                id: format!("t/{name}"),
                namespace: "t".to_string(),
                name: name.to_string(),
                group_size: 1,
                requests: ResourceList {
                    milli_cpu: 1000,
                    memory_bytes: 1 << 30,
                    ephemeral_storage: 0,
                    pods: 0,
                },
                extended_resource_requests: ext,
                feasible_nodes: vec!["n1".to_string()],
                ..Default::default()
            }
        };
        OptimizationInput {
            nodes: vec![node],
            workloads: vec![mk("a"), mk("b")],
            anti_affinity_pairs: vec![],
        }
    }

    #[test]
    fn partial_admission_places_what_fits() {
        use crate::model::ScenarioConfig;
        let input = two_competing_gpu_pods();
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("solve should succeed, not infeasible");
        // assignment_counts is authoritative: exactly one of the two pods is admitted.
        let admitted = solution
            .assignment_counts
            .values()
            .filter(|counts| counts.values().any(|c| *c > 0))
            .count();
        assert_eq!(
            admitted, 1,
            "expected exactly one admitted; status={}",
            info.status
        );
    }

    fn gang_workload(group_size: i32, total_gpu: i64, feasible: &[&str]) -> OptimizationWorkload {
        use crate::model::{OptimizationWorkloadMember, ResourceList};
        use std::collections::BTreeMap;
        let mut ext = BTreeMap::new();
        ext.insert("nvidia.com/gpu".to_string(), total_gpu); // TOTAL across the gang
        OptimizationWorkload {
            id: "gang:t/job".to_string(),
            namespace: "t".to_string(),
            name: "job".to_string(),
            group_size,
            members: (0..group_size)
                .map(|i| OptimizationWorkloadMember {
                    namespace: "t".to_string(),
                    name: format!("m{i}"),
                    current_node: String::new(),
                })
                .collect(),
            requests: ResourceList {
                milli_cpu: 1000 * i64::from(group_size),
                memory_bytes: (1 << 30) * i64::from(group_size),
                ephemeral_storage: 0,
                pods: i64::from(group_size),
            },
            extended_resource_requests: ext,
            feasible_nodes: feasible.iter().map(|s| s.to_string()).collect(),
            ..Default::default()
        }
    }

    fn gpu_node(name: &str, gpus: i64) -> OptimizationNode {
        use crate::model::ResourceList;
        use std::collections::BTreeMap;
        let mut node_ext = BTreeMap::new();
        node_ext.insert("nvidia.com/gpu".to_string(), gpus);
        OptimizationNode {
            name: name.to_string(),
            count: 1,
            members: vec![name.to_string()],
            effective_capacity: ResourceList {
                milli_cpu: 64000,
                memory_bytes: 256 << 30,
                ephemeral_storage: 0,
                pods: 110,
            },
            extended_resources: node_ext,
            ..Default::default()
        }
    }

    #[test]
    fn gang_of_five_rejected_on_four_gpus() {
        use crate::model::ScenarioConfig;
        // group_size=5, total 5 GPU (1/replica). 4-GPU node can't fit all 5 -> not admitted.
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 4)],
            workloads: vec![gang_workload(5, 5, &["n1"])],
            anti_affinity_pairs: vec![],
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, info) =
            super::enabled::solve(&input, &scenario).expect("solve should succeed");
        assert!(
            !solution.assignment_counts.contains_key("gang:t/job"),
            "5-replica gang must not be admitted on 4 GPUs; status={}",
            info.status
        );
    }

    #[test]
    fn gang_of_five_admitted_on_five_gpus() {
        use crate::model::ScenarioConfig;
        let input = OptimizationInput {
            nodes: vec![gpu_node("n1", 5)],
            workloads: vec![gang_workload(5, 5, &["n1"])],
            anti_affinity_pairs: vec![],
        };
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (solution, _info) =
            super::enabled::solve(&input, &scenario).expect("solve should succeed");
        let total: i64 = solution
            .assignment_counts
            .get("gang:t/job")
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);
        assert_eq!(total, 5, "5-replica gang must be fully admitted on 5 GPUs");
    }

    fn colocate_gang_input(colocate: bool) -> OptimizationInput {
        let mut w = gang_workload(4, 4, &["n1", "n2"]);
        w.colocate = colocate;
        OptimizationInput {
            nodes: vec![gpu_node("n1", 2), gpu_node("n2", 2)],
            workloads: vec![w],
            anti_affinity_pairs: vec![],
        }
    }

    #[test]
    fn colocated_gang_needs_single_node() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (sol, info) =
            super::enabled::solve(&colocate_gang_input(true), &scenario).expect("solve");
        assert!(
            !sol.assignment_counts.contains_key("gang:t/job"),
            "co-located 4-gang must not fit on 2-GPU nodes; status={}",
            info.status
        );
    }

    #[test]
    fn non_colocated_gang_spreads() {
        use crate::model::ScenarioConfig;
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: true,
            ..Default::default()
        };
        let (sol, _info) =
            super::enabled::solve(&colocate_gang_input(false), &scenario).expect("solve");
        let total: i64 = sol
            .assignment_counts
            .get("gang:t/job")
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);
        assert_eq!(
            total, 4,
            "non-co-located 4-gang should spread 2+2 across the nodes"
        );
    }

    #[test]
    fn hard_equality_is_infeasible_when_flag_off() {
        use crate::model::ScenarioConfig;
        let input = two_competing_gpu_pods();
        let scenario = ScenarioConfig {
            solver: "cp-sat-rust".to_string(),
            partial_admission: false,
            ..Default::default()
        };
        // Both pods must place but only one fits -> hard equality makes the model infeasible.
        assert!(super::enabled::solve(&input, &scenario).is_err());
    }
}
