use crate::model::{OptimizationInput, OptimizationSolution};
use crate::scheduler::pod_filter::PendingGpuPod;
use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement};
use std::collections::HashSet;

/// The strict-mode workload id for a pod ("{namespace}/{name}").
fn workload_id(p: &PendingGpuPod) -> String {
    format!("{}/{}", p.namespace, p.name)
}

/// Map the solver's output back to per-pod placement decisions, with honest reasons.
#[allow(clippy::too_many_arguments)]
pub fn build_decision_trace(
    sequence: u64,
    pending: &[PendingGpuPod],
    input: &OptimizationInput,
    solution: &OptimizationSolution,
    solver_status: &str,
    solve_millis: u64,
    snapshot_age_millis: u64,
) -> DecisionTrace {
    let submitted: HashSet<&str> = input.workloads.iter().map(|w| w.id.as_str()).collect();
    let mut decisions = Vec::with_capacity(pending.len());
    for p in pending {
        let id = workload_id(p);
        let placement = if !submitted.contains(id.as_str()) {
            PodPlacement::Unplaced {
                reason: "not submitted to solver (filtered as unschedulable during input build)"
                    .to_string(),
            }
        } else {
            match solution.assignments.get(&id) {
                Some(node) if !node.is_empty() => PodPlacement::Placed { node: node.clone() },
                _ => PodPlacement::Unplaced {
                    reason: "no feasible placement found".to_string(),
                },
            }
        };
        decisions.push(PodDecision {
            uid: p.uid.clone(),
            namespace: p.namespace.clone(),
            name: p.name.clone(),
            gpu_request: p.gpu_request,
            placement,
        });
    }
    DecisionTrace {
        sequence,
        observed_pods: pending.len(),
        decisions,
        solver_status: solver_status.to_string(),
        solve_millis,
        snapshot_age_millis,
        note: String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{OptimizationInput, OptimizationSolution, OptimizationWorkload};
    use crate::scheduler::pod_filter::PendingGpuPod;
    use crate::scheduler::trace::PodPlacement;
    use std::collections::HashMap;

    fn pod(ns: &str, name: &str) -> PendingGpuPod {
        PendingGpuPod {
            uid: format!("uid-{name}"),
            namespace: ns.into(),
            name: name.into(),
            gpu_request: 1,
        }
    }

    fn workload(ns: &str, name: &str) -> OptimizationWorkload {
        OptimizationWorkload {
            id: format!("{ns}/{name}"),
            namespace: ns.into(),
            name: name.into(),
            ..Default::default()
        }
    }

    #[test]
    fn placed_unplaced_and_not_submitted() {
        let pending = vec![
            pod("team-a", "placed"),
            pod("team-a", "unplaced"),
            pod("team-a", "ghost"),
        ];
        // Solver saw "placed" and "unplaced"; not "ghost".
        let input = OptimizationInput {
            workloads: vec![workload("team-a", "placed"), workload("team-a", "unplaced")],
            ..Default::default()
        };
        let mut assignments = HashMap::new();
        assignments.insert("team-a/placed".to_string(), "node-1".to_string());
        let solution = OptimizationSolution {
            assignments,
            ..Default::default()
        };

        let t = build_decision_trace(5, &pending, &input, &solution, "OPTIMAL", 20, 4);
        assert_eq!(t.sequence, 5);
        assert_eq!(t.observed_pods, 3);
        assert_eq!(
            t.decisions[0].placement,
            PodPlacement::Placed {
                node: "node-1".into()
            }
        );
        match &t.decisions[1].placement {
            PodPlacement::Unplaced { reason } => assert!(reason.contains("no feasible")),
            _ => panic!("want unplaced"),
        }
        match &t.decisions[2].placement {
            PodPlacement::Unplaced { reason } => assert!(reason.contains("not submitted")),
            _ => panic!("want unplaced"),
        }
    }
}
