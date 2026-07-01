use crate::model::{OptimizationInput, OptimizationSolution};
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement};
use std::collections::HashMap;

fn pod_key(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

/// Whether a node is a time-sliced (oversubscribed, no-isolation) GPU node, from its labels.
/// If the NVIDIA `nvidia.com/gpu.sharing-strategy` label is present it is authoritative (only
/// `time-slicing` counts — `mps`/`none` do not, and MPS also uses replicas); otherwise fall
/// back to `nvidia.com/gpu.replicas > 1` (legacy time-slicing without the strategy label).
pub(crate) fn is_time_sliced_node(labels: &std::collections::BTreeMap<String, String>) -> bool {
    match labels.get("nvidia.com/gpu.sharing-strategy") {
        Some(s) => s == "time-slicing",
        None => labels
            .get("nvidia.com/gpu.replicas")
            .and_then(|v| v.parse::<i64>().ok())
            .map(|n| n > 1)
            .unwrap_or(false),
    }
}

/// Map the solver's per-gang output back to per-pod placement decisions.
///
/// A gang (`OptimizationWorkload`, possibly `group_size > 1`) is admitted iff its
/// `assignment_counts` sum equals `group_size` (the admission latch guarantees 0 or
/// group_size; a nonzero partial is anomalous and treated as not admitted). For an
/// admitted gang, members are distributed deterministically across the assigned nodes
/// (sorted members filled into sorted nodes by count), so a spread gang reports honest
/// per-member nodes rather than a single "best" node.
#[allow(clippy::too_many_arguments)]
pub fn build_decision_trace(
    sequence: u64,
    pending: &[PendingGpuPod],
    input: &OptimizationInput,
    solution: &OptimizationSolution,
    solver_status: &str,
    solve_ok: bool,
    solve_millis: u64,
    solve_core_millis: u64,
    snapshot_age_millis: u64,
    drop_reasons: &HashMap<String, String>,
    time_sliced_nodes: &std::collections::HashSet<String>,
) -> DecisionTrace {
    // When the solver returned no usable result (Err: timeout/no incumbent/infeasible/
    // backend error), a submitted pod being unresolved does NOT mean it is unschedulable
    // — the solver simply produced nothing. Generic reason; solver_status carries detail.
    let unresolved_reason = |admitted_case: &str| -> String {
        if solve_ok {
            admitted_case.to_string()
        } else {
            "solver produced no usable solution (see solver_status)".to_string()
        }
    };
    // pod "{ns}/{name}" -> resolved placement.
    let mut placement_for: HashMap<String, PodPlacement> = HashMap::new();

    for workload in &input.workloads {
        let group_size = workload.group_size.max(0) as i64;
        let counts = solution.assignment_counts.get(&workload.id);
        let placed_total: i64 = counts
            .map(|c| c.values().map(|v| i64::from(*v)).sum())
            .unwrap_or(0);
        let admitted = group_size > 0 && placed_total == group_size;

        // Deterministic member order.
        let mut members: Vec<&crate::model::OptimizationWorkloadMember> =
            workload.members.iter().collect();
        members.sort_by(|a, b| a.name.cmp(&b.name));

        if admitted {
            // Expand assignment_counts into a per-replica node list (sorted node order).
            let mut nodes: Vec<String> = Vec::with_capacity(placed_total as usize);
            if let Some(counts) = counts {
                let mut keyed: Vec<(&String, &i32)> = counts.iter().collect();
                keyed.sort_by(|a, b| a.0.cmp(b.0));
                for (node, count) in keyed {
                    for _ in 0..(*count).max(0) {
                        nodes.push(node.clone());
                    }
                }
            }
            for (i, m) in members.iter().enumerate() {
                let node = nodes.get(i).cloned().unwrap_or_default();
                let placement = if node.is_empty() {
                    PodPlacement::Unplaced {
                        reason: "gang admitted but replica node unresolved".to_string(),
                    }
                } else {
                    PodPlacement::Placed { node }
                };
                placement_for.insert(pod_key(&m.namespace, &m.name), placement);
            }
        } else {
            let reason = unresolved_reason("gang not admitted (insufficient capacity or quota)");
            for m in &members {
                placement_for.insert(
                    pod_key(&m.namespace, &m.name),
                    PodPlacement::Unplaced {
                        reason: reason.clone(),
                    },
                );
            }
        }
    }

    let mut decisions = Vec::with_capacity(pending.len());
    for p in pending {
        let scope = pod_key(&p.namespace, &p.name);
        let placement = placement_for.get(&scope).cloned().unwrap_or_else(|| {
            // Never submitted to the solver — use the specific input-build drop reason if we
            // recorded one, else a generic fallback.
            let reason = drop_reasons.get(&scope).cloned().unwrap_or_else(|| {
                "not submitted to solver (filtered as unschedulable during input build)".to_string()
            });
            PodPlacement::Unplaced { reason }
        });
        // Disclose time-sliced (shared, no-isolation) GPU placements.
        let mut caveats = p.unmodeled_constraints.clone();
        if let PodPlacement::Placed { node } = &placement {
            if time_sliced_nodes.contains(node) {
                caveats.push("time-sliced GPU: shared, no isolation".to_string());
            }
        }
        decisions.push(PodDecision {
            uid: p.uid.clone(),
            namespace: p.namespace.clone(),
            name: p.name.clone(),
            gpu_request: p.gpu_request,
            placement,
            caveats,
        });
    }

    DecisionTrace {
        sequence,
        observed_pods: pending.len(),
        decisions,
        solver_status: solver_status.to_string(),
        solve_millis,
        solve_core_millis,
        snapshot_age_millis,
        note: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{
        OptimizationInput, OptimizationSolution, OptimizationWorkload, OptimizationWorkloadMember,
    };
    use crate::scheduler::pod_filter::PendingGpuPod;
    use crate::scheduler::trace::PodPlacement;
    use std::collections::{HashMap, HashSet};

    fn ppod(ns: &str, name: &str) -> PendingGpuPod {
        PendingGpuPod {
            uid: format!("uid-{name}"),
            namespace: ns.into(),
            name: name.into(),
            gpu_request: 1,
            gang_key: Some(format!("{ns}/job")),
            colocate: false,
            unmodeled_constraints: vec![],
            anti_affinity_host_selectors: vec![],
            anti_affinity_topology_selectors: vec![],
        }
    }

    fn member(ns: &str, n: &str) -> OptimizationWorkloadMember {
        OptimizationWorkloadMember {
            namespace: ns.into(),
            name: n.into(),
            current_node: String::new(),
        }
    }

    #[test]
    fn gang_members_share_admission() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 2,
            members: vec![member("team", "m0"), member("team", "m1")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 2);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending = vec![ppod("team", "m0"), ppod("team", "m1")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(t
            .decisions
            .iter()
            .all(|d| matches!(&d.placement, PodPlacement::Placed { node } if node == "n1")));
    }

    #[test]
    fn gang_not_admitted_marks_all_members_unplaced() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 2,
            members: vec![member("team", "m0"), member("team", "m1")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        // no assignment_counts entry -> not admitted
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "m0"), ppod("team", "m1")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(t.decisions.iter().all(|d| matches!(
            &d.placement,
            PodPlacement::Unplaced { reason } if reason.contains("gang not admitted")
        )));
    }

    #[test]
    fn spread_gang_reports_per_member_nodes() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 3,
            members: vec![
                member("team", "m0"),
                member("team", "m1"),
                member("team", "m2"),
            ],
            feasible_nodes: vec!["n1".into(), "n2".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 2);
        counts.insert("n2".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending = vec![ppod("team", "m0"), ppod("team", "m1"), ppod("team", "m2")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        // sorted members m0,m1 -> n1 (count 2); m2 -> n2 (count 1)
        let by_name: HashMap<_, _> = t
            .decisions
            .iter()
            .map(|d| (d.name.clone(), d.placement.clone()))
            .collect();
        assert_eq!(by_name["m0"], PodPlacement::Placed { node: "n1".into() });
        assert_eq!(by_name["m1"], PodPlacement::Placed { node: "n1".into() });
        assert_eq!(by_name["m2"], PodPlacement::Placed { node: "n2".into() });
    }

    #[test]
    fn pod_absent_from_input_is_not_submitted() {
        let input = OptimizationInput::default();
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "ghost")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Unplaced { reason } if reason.contains("not submitted")
        ));
    }

    #[test]
    fn caveats_propagate_to_placed_decision() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 1,
            members: vec![member("team", "m0")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let mut p = ppod("team", "m0");
        p.unmodeled_constraints = vec!["pod anti-affinity".to_string()];
        let t = build_decision_trace(
            1,
            &[p],
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Placed { .. }
        ));
        assert_eq!(
            t.decisions[0].caveats,
            vec!["pod anti-affinity".to_string()]
        );
    }

    #[test]
    fn no_solution_reports_solver_reason_not_unschedulable() {
        // Submitted gang, solve_ok=false (empty solution) -> "no usable solution", NOT
        // "gang not admitted"/"no feasible placement".
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 2,
            members: vec![member("team", "m0"), member("team", "m1")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "m0"), ppod("team", "m1")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "no-solution: x",
            false,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(t.decisions.iter().all(|d| matches!(
            &d.placement,
            PodPlacement::Unplaced { reason } if reason.contains("no usable solution")
        )));
    }

    #[test]
    fn not_submitted_stays_not_submitted_even_when_solve_failed() {
        let input = OptimizationInput::default();
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "ghost")];
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "no-solution: x",
            false,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Unplaced { reason } if reason.contains("not submitted")
        ));
    }

    #[test]
    fn drop_reason_is_surfaced_for_never_submitted_pod() {
        let input = OptimizationInput::default();
        let solution = OptimizationSolution::default();
        let pending = vec![ppod("team", "m0")];
        let mut drops = HashMap::new();
        drops.insert(
            "team/m0".to_string(),
            "no feasible node (insufficient residual capacity or excluded by anti-affinity)"
                .to_string(),
        );
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &drops,
            &HashSet::new(),
        );
        assert!(matches!(
            &t.decisions[0].placement,
            PodPlacement::Unplaced { reason } if reason.contains("no feasible node")
        ));
    }

    #[test]
    fn is_time_sliced_node_detection() {
        use std::collections::BTreeMap;
        let l = |pairs: &[(&str, &str)]| -> BTreeMap<String, String> {
            pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect()
        };
        assert!(super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.sharing-strategy",
            "time-slicing"
        )])));
        // MPS (even with replicas) is NOT time-slicing.
        assert!(!super::is_time_sliced_node(&l(&[
            ("nvidia.com/gpu.sharing-strategy", "mps"),
            ("nvidia.com/gpu.replicas", "4"),
        ])));
        assert!(!super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.sharing-strategy",
            "none"
        )])));
        // No strategy label -> replicas fallback.
        assert!(super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.replicas",
            "4"
        )])));
        assert!(!super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.replicas",
            "1"
        )])));
        assert!(!super::is_time_sliced_node(&l(&[(
            "nvidia.com/gpu.replicas",
            "x"
        )])));
        assert!(!super::is_time_sliced_node(&BTreeMap::new()));
    }

    #[test]
    fn placed_pod_on_time_sliced_node_gets_caveat() {
        let gang = OptimizationWorkload {
            id: "gang:team/job".into(),
            namespace: "team".into(),
            name: "m0".into(),
            group_size: 1,
            members: vec![member("team", "m0")],
            feasible_nodes: vec!["n1".into()],
            ..Default::default()
        };
        let input = OptimizationInput {
            workloads: vec![gang],
            ..Default::default()
        };
        let mut counts = HashMap::new();
        counts.insert("n1".to_string(), 1);
        let mut assignment_counts = HashMap::new();
        assignment_counts.insert("gang:team/job".to_string(), counts);
        let solution = OptimizationSolution {
            assignment_counts,
            ..Default::default()
        };
        let pending = vec![ppod("team", "m0")];
        let time_sliced: HashSet<String> = ["n1".to_string()].into_iter().collect();
        let t = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &time_sliced,
        );
        assert!(t.decisions[0]
            .caveats
            .iter()
            .any(|c| c.contains("time-sliced GPU")));

        // Same pod on a NON-time-sliced node: no caveat.
        let t2 = build_decision_trace(
            1,
            &pending,
            &input,
            &solution,
            "OPTIMAL",
            true,
            5,
            5,
            1,
            &HashMap::new(),
            &HashSet::new(),
        );
        assert!(!t2.decisions[0]
            .caveats
            .iter()
            .any(|c| c.contains("time-sliced GPU")));
    }
}
