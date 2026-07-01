//! Dry-run binding plan: renders the exact Kubernetes `Binding` subresource payloads that a real
//! binder WOULD POST for each placed pod. This module is PURE — it builds data only and never
//! contacts the API server (enforced by `no_mutation_guard`). Shadow mode stays read-only.

use crate::scheduler::trace::{DecisionTrace, PodPlacement};
use serde::{Deserialize, Serialize};

/// One rendered (but never sent) pod→node binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BindingPlanEntry {
    pub namespace: String,
    pub pod_name: String,
    pub pod_uid: String,
    pub node_name: String,
    /// The canonical `pods/binding` subresource POST body a real binder would send (dry-run only).
    pub binding_body: serde_json::Value,
}

/// Render the pod→node bindings implied by a decision trace. Only `Placed` decisions produce an
/// entry; unplaced pods are skipped. No side effects, no API calls.
pub fn render_binding_plan(trace: &DecisionTrace) -> Vec<BindingPlanEntry> {
    trace
        .decisions
        .iter()
        .filter_map(|d| match &d.placement {
            PodPlacement::Placed { node } => Some(BindingPlanEntry {
                namespace: d.namespace.clone(),
                pod_name: d.name.clone(),
                pod_uid: d.uid.clone(),
                node_name: node.clone(),
                binding_body: serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Binding",
                    "metadata": { "name": d.name, "namespace": d.namespace },
                    "target": { "apiVersion": "v1", "kind": "Node", "name": node },
                }),
            }),
            PodPlacement::Unplaced { .. } => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement};

    fn trace_with(decisions: Vec<PodDecision>) -> DecisionTrace {
        DecisionTrace {
            sequence: 1,
            observed_pods: decisions.len(),
            decisions,
            solver_status: "OPTIMAL".into(),
            solve_millis: 1,
            solve_core_millis: 1,
            snapshot_age_millis: 0,
            note: String::new(),
        }
    }

    fn placed(ns: &str, name: &str, uid: &str, node: &str) -> PodDecision {
        PodDecision {
            uid: uid.into(),
            namespace: ns.into(),
            name: name.into(),
            gpu_request: 1,
            placement: PodPlacement::Placed { node: node.into() },
            caveats: vec![],
        }
    }

    #[test]
    fn renders_binding_for_each_placed_pod_only() {
        let t = trace_with(vec![
            placed("team", "a", "uid-a", "node-1"),
            PodDecision {
                uid: "uid-b".into(),
                namespace: "team".into(),
                name: "b".into(),
                gpu_request: 1,
                placement: PodPlacement::Unplaced {
                    reason: "no feasible node".into(),
                },
                caveats: vec![],
            },
        ]);
        let plan = render_binding_plan(&t);
        assert_eq!(plan.len(), 1, "only placed pods yield bindings");
        let e = &plan[0];
        assert_eq!(e.namespace, "team");
        assert_eq!(e.pod_name, "a");
        assert_eq!(e.pod_uid, "uid-a");
        assert_eq!(e.node_name, "node-1");
        assert_eq!(e.binding_body["kind"], "Binding");
        assert_eq!(e.binding_body["apiVersion"], "v1");
        assert_eq!(e.binding_body["metadata"]["name"], "a");
        assert_eq!(e.binding_body["metadata"]["namespace"], "team");
        assert_eq!(e.binding_body["target"]["kind"], "Node");
        assert_eq!(e.binding_body["target"]["name"], "node-1");
    }

    #[test]
    fn empty_when_nothing_placed() {
        let t = trace_with(vec![PodDecision {
            uid: "uid-b".into(),
            namespace: "team".into(),
            name: "b".into(),
            gpu_request: 1,
            placement: PodPlacement::Unplaced { reason: "x".into() },
            caveats: vec![],
        }]);
        assert!(render_binding_plan(&t).is_empty());
    }
}
