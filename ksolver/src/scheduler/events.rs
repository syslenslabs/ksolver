//! Kubernetes Event rendering and optional gated emission for scheduler auditability.
//!
//! Rendering is pure and used by read-only API endpoints. Emission is a separate opt-in path:
//! callers must explicitly pass a kube client after checking scheduler mutation policy.

use crate::scheduler::binder::{BindOutcome, BindResult};
use crate::scheduler::trace::{DecisionTrace, PodDecision, PodPlacement, RepairAction};
use serde::{Deserialize, Serialize};

const REPORTING_CONTROLLER: &str = "ksolver.dev/scheduler";
const MAX_EVENT_NOTE_CHARS: usize = 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EventDraft {
    pub namespace: String,
    pub pod: String,
    #[serde(default)]
    pub pod_uid: String,
    #[serde(default)]
    pub team: String,
    pub reason: String,
    #[serde(rename = "type")]
    pub type_: String,
    pub note: String,
    pub body: serde_json::Value,
}

fn event_reason(result: &BindResult) -> (&'static str, &'static str, &'static str) {
    match result {
        BindResult::Bound { dry_run: false } => ("KsolverBound", "Normal", "BindingApplied"),
        BindResult::Bound { dry_run: true } => {
            ("KsolverBindValidated", "Normal", "BindingDryRunValidated")
        }
        BindResult::Skipped { .. } => ("KsolverBindSkipped", "Normal", "BindingSkipped"),
        BindResult::Failed { .. } => ("KsolverBindFailed", "Warning", "BindingFailed"),
    }
}

fn effective_reporting_instance<'a>(
    scheduler_name: &'a str,
    reporting_instance: &'a str,
) -> &'a str {
    if reporting_instance.is_empty() {
        scheduler_name
    } else {
        reporting_instance
    }
}

fn event_note(outcome: &BindOutcome) -> String {
    let mut note = match &outcome.result {
        BindResult::Bound { dry_run: false } => {
            format!("ksolver bound pod {} to node {}", outcome.pod, outcome.node)
        }
        BindResult::Bound { dry_run: true } => format!(
            "ksolver dry-run validated binding pod {} to node {}",
            outcome.pod, outcome.node
        ),
        BindResult::Skipped { reason } => format!(
            "ksolver skipped binding pod {} to node {}: {}",
            outcome.pod, outcome.node, reason
        ),
        BindResult::Failed { error } => format!(
            "ksolver failed binding pod {} to node {}: {}",
            outcome.pod, outcome.node, error
        ),
    };
    if !outcome.team.trim().is_empty() {
        note.push_str(&format!("; team {}", outcome.team));
    }
    truncate_note(note)
}

fn decision_event_reason(decision: &PodDecision) -> (&'static str, &'static str, &'static str) {
    match &decision.placement {
        PodPlacement::Placed { .. } => (
            "KsolverPlacementRecommended",
            "Normal",
            "PlacementRecommended",
        ),
        PodPlacement::Unplaced { reason }
            if reason.contains("quota exhausted")
                || decision
                    .caveats
                    .iter()
                    .any(|c| c.contains("quota exhausted")) =>
        {
            ("KsolverQuotaThrottled", "Warning", "QuotaThrottled")
        }
        PodPlacement::Unplaced { reason }
            if reason.contains("budget exhausted")
                || decision
                    .caveats
                    .iter()
                    .any(|c| c.contains("budget exhausted")) =>
        {
            ("KsolverBudgetThrottled", "Warning", "BudgetThrottled")
        }
        PodPlacement::Unplaced { .. } => {
            ("KsolverPlacementDeferred", "Warning", "PlacementDeferred")
        }
    }
}

fn decision_event_note(decision: &PodDecision) -> String {
    let mut note = match &decision.placement {
        PodPlacement::Placed { node } => format!(
            "ksolver recommends placing pod {}/{} on node {}",
            decision.namespace, decision.name, node
        ),
        PodPlacement::Unplaced { reason } => format!(
            "ksolver deferred pod {}/{}: {}",
            decision.namespace, decision.name, reason
        ),
    };
    if decision.priority > 0 {
        note.push_str(&format!("; priority {}", decision.priority));
    }
    if decision.queue_wait_seconds > 0 {
        note.push_str(&format!("; queued {}s", decision.queue_wait_seconds));
    }
    if !decision.caveats.is_empty() {
        note.push_str(&format!("; caveats: {}", decision.caveats.join("; ")));
    }
    truncate_note(note)
}

fn repair_event_reason(action: &RepairAction) -> (&'static str, &'static str, &'static str) {
    match action.action.as_str() {
        "migrate" => (
            "KsolverRepairMigrationRecommended",
            "Warning",
            "MigrationRecommended",
        ),
        "preempt" => (
            "KsolverRepairPreemptionRecommended",
            "Warning",
            "PreemptionRecommended",
        ),
        _ => ("KsolverRepairRecommended", "Warning", "RepairRecommended"),
    }
}

fn repair_event_note(plan: &crate::scheduler::trace::RepairPlan, action: &RepairAction) -> String {
    let mut note = match action.action.as_str() {
        "migrate" if !action.to_node.trim().is_empty() => format!(
            "ksolver recommends migrating pod {}/{} from node {} to node {} to repair target {}",
            action.namespace, action.pod, action.node, action.to_node, plan.target
        ),
        "migrate" => format!(
            "ksolver recommends migrating pod {}/{} from node {} to repair target {}",
            action.namespace, action.pod, action.node, plan.target
        ),
        "preempt" => format!(
            "ksolver recommends preempting pod {}/{} on node {} to repair target {}",
            action.namespace, action.pod, action.node, plan.target
        ),
        other => format!(
            "ksolver recommends repair action {} for pod {}/{} on node {} to repair target {}",
            other, action.namespace, action.pod, action.node, plan.target
        ),
    };
    if action.gpu_request > 0 {
        note.push_str(&format!("; frees {} GPU", action.gpu_request));
    }
    if action.disruption_cost > 0 {
        note.push_str(&format!("; disruption cost {}", action.disruption_cost));
    }
    if !action.reason.trim().is_empty() {
        note.push_str(&format!("; {}", action.reason));
    }
    truncate_note(note)
}

fn truncate_note(mut note: String) -> String {
    if note.chars().count() <= MAX_EVENT_NOTE_CHARS {
        return note;
    }
    note = note.chars().take(MAX_EVENT_NOTE_CHARS - 3).collect();
    note.push_str("...");
    note
}

/// Render Kubernetes Event payloads for binding outcomes. The caller supplies `event_time_rfc3339`
/// so tests and replay tooling can be deterministic; pass the current UTC timestamp in production.
pub fn render_binding_events(
    outcomes: &[BindOutcome],
    scheduler_name: &str,
    reporting_instance: &str,
    sequence: u64,
    event_time_rfc3339: &str,
) -> Vec<EventDraft> {
    outcomes
        .iter()
        .map(|outcome| {
            let (reason, type_, action) = event_reason(&outcome.result);
            let note = event_note(outcome);
            let body = serde_json::json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {
                    "namespace": outcome.namespace,
                    "generateName": format!("{}-{}-ksolver-", outcome.pod, sequence),
                },
                "regarding": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "namespace": outcome.namespace,
                    "name": outcome.pod,
                    "uid": outcome.pod_uid,
                },
                "related": {
                    "apiVersion": "v1",
                    "kind": "Node",
                    "name": outcome.node,
                },
                "reason": reason,
                "note": note,
                "type": type_,
                "action": action,
                "eventTime": event_time_rfc3339,
                "reportingController": REPORTING_CONTROLLER,
                "reportingInstance": effective_reporting_instance(scheduler_name, reporting_instance),
                "deprecatedSource": {
                    "component": scheduler_name,
                },
                "series": {
                    "count": 1,
                    "lastObservedTime": event_time_rfc3339,
                }
            });
            EventDraft {
                namespace: outcome.namespace.clone(),
                pod: outcome.pod.clone(),
                pod_uid: outcome.pod_uid.clone(),
                team: outcome.team.clone(),
                reason: reason.to_string(),
                type_: type_.to_string(),
                note,
                body,
            }
        })
        .collect()
}

/// Render Kubernetes Event payloads for the scheduler's shadow decisions. This is intentionally
/// read-only: callers can show, diff, or later POST these payloads from a separately gated emitter.
pub fn render_decision_events(
    trace: &DecisionTrace,
    scheduler_name: &str,
    reporting_instance: &str,
    event_time_rfc3339: &str,
) -> Vec<EventDraft> {
    trace
        .decisions
        .iter()
        .map(|decision| {
            let (reason, type_, action) = decision_event_reason(decision);
            let note = decision_event_note(decision);
            let mut body = serde_json::json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {
                    "namespace": decision.namespace,
                    "generateName": format!("{}-{}-ksolver-decision-", decision.name, trace.sequence),
                },
                "regarding": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "namespace": decision.namespace,
                    "name": decision.name,
                    "uid": decision.uid,
                },
                "reason": reason,
                "note": note,
                "type": type_,
                "action": action,
                "eventTime": event_time_rfc3339,
                "reportingController": REPORTING_CONTROLLER,
                "reportingInstance": effective_reporting_instance(scheduler_name, reporting_instance),
                "deprecatedSource": {
                    "component": scheduler_name,
                },
                "series": {
                    "count": 1,
                    "lastObservedTime": event_time_rfc3339,
                }
            });
            if let PodPlacement::Placed { node } = &decision.placement {
                body["related"] = serde_json::json!({
                    "apiVersion": "v1",
                    "kind": "Node",
                    "name": node,
                });
            }
            EventDraft {
                namespace: decision.namespace.clone(),
                pod: decision.name.clone(),
                pod_uid: decision.uid.clone(),
                team: decision.team.clone(),
                reason: reason.to_string(),
                type_: type_.to_string(),
                note,
                body,
            }
        })
        .collect()
}

/// Render Kubernetes Event payloads for advisory repair actions. This is intentionally read-only:
/// repair plans are recommendations only and must not be confused with real evictions/migrations.
pub fn render_repair_events(
    trace: &DecisionTrace,
    scheduler_name: &str,
    reporting_instance: &str,
    event_time_rfc3339: &str,
) -> Vec<EventDraft> {
    let mut out = Vec::new();
    for plan in &trace.repair_plans {
        for action in &plan.actions {
            let (reason, type_, event_action) = repair_event_reason(action);
            let note = repair_event_note(plan, action);
            let body = serde_json::json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {
                    "namespace": action.namespace,
                    "generateName": format!("{}-{}-ksolver-repair-", action.pod, trace.sequence),
                },
                "regarding": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "namespace": action.namespace,
                    "name": action.pod,
                },
                "related": {
                    "apiVersion": "v1",
                    "kind": "Node",
                    "name": action.node,
                },
                "reason": reason,
                "note": note,
                "type": type_,
                "action": event_action,
                "eventTime": event_time_rfc3339,
                "reportingController": REPORTING_CONTROLLER,
                "reportingInstance": effective_reporting_instance(scheduler_name, reporting_instance),
                "deprecatedSource": {
                    "component": scheduler_name,
                },
                "series": {
                    "count": 1,
                    "lastObservedTime": event_time_rfc3339,
                }
            });
            out.push(EventDraft {
                namespace: action.namespace.clone(),
                pod: action.pod.clone(),
                pod_uid: String::new(),
                team: String::new(),
                reason: reason.to_string(),
                type_: type_.to_string(),
                note,
                body,
            });
        }
    }
    out
}

pub fn event_from_draft(
    draft: &EventDraft,
) -> Result<k8s_openapi::api::events::v1::Event, serde_json::Error> {
    serde_json::from_value(draft.body.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn outcome(result: BindResult) -> BindOutcome {
        BindOutcome {
            namespace: "team".to_string(),
            pod: "train-a".to_string(),
            pod_uid: "uid-a".to_string(),
            team: "research".to_string(),
            node: "gpu-1".to_string(),
            result,
        }
    }

    fn decision(placement: PodPlacement) -> PodDecision {
        PodDecision {
            uid: "uid-a".to_string(),
            namespace: "team".to_string(),
            name: "train-a".to_string(),
            binding_group: String::new(),
            gpu_request: 4,
            priority: 7,
            priority_class_name: "research-high".to_string(),
            team: "research".to_string(),
            queue: "urgent".to_string(),
            queue_score: 100,
            business_value: 0,
            queue_wait_seconds: 300,
            deadline_unix_seconds: 0,
            min_gpus: 0,
            max_gpus: 0,
            preferred_gpus: 0,
            flexible: false,
            predicted_runtime_seconds: 0,
            predicted_peak_vram_bytes: 0,
            deadline_slack_seconds: 0,
            predicted_finish_unix_seconds: 0,
            predicted_deadline_miss: false,
            placement,
            caveats: vec!["deferred below admitted higher-priority work".to_string()],
        }
    }

    fn trace(decisions: Vec<PodDecision>) -> DecisionTrace {
        DecisionTrace {
            sequence: 42,
            observed_pods: decisions.len(),
            decisions,
            solver_status: "status=Optimal".to_string(),
            objective_profile: Default::default(),
            objective_weights: Default::default(),
            solve_millis: 10,
            solve_core_millis: 5,
            snapshot_age_millis: 1,
            note: String::new(),
            repair_plans: Vec::new(),
            repair_notes: Vec::new(),
            repair_metrics: Default::default(),
            deadline_metrics: Default::default(),
            quota_metrics: Default::default(),
            admission_metrics: Default::default(),
            queue_wait_metrics: Default::default(),
            tenant_fairness_metrics: Default::default(),
            gpu_utilization_metrics: Default::default(),
            outcome_summary: Default::default(),
            job_observation_metrics: Default::default(),
            prediction_audit_metrics: Default::default(),
            prediction_audit_details: Vec::new(),
            node_grouping_metrics: Default::default(),
            candidate_quality_metrics: Default::default(),
            binding_reservation_metrics: Default::default(),
            binding_outcome_metrics: Default::default(),
            candidate_node_limit: 0,
            retry_count: 0,
            unpruned_candidate_edges: 0,
            initial_candidate_edges: 0,
            final_candidate_edges: 0,
            candidate_pruned_workloads: 0,
            widening_reason: String::new(),
        }
    }

    fn trace_with_repair(action_name: &str) -> DecisionTrace {
        let mut t = trace(Vec::new());
        t.repair_plans = vec![crate::scheduler::trace::RepairPlan {
            target: "team/urgent".to_string(),
            target_gpu_request: 4,
            target_priority: 10,
            target_business_value: 50,
            target_deadline_unix_seconds: 0,
            target_latest_start_unix_seconds: 0,
            target_queue_wait_seconds: 120,
            node: "gpu-1".to_string(),
            freed_gpu: 1,
            disruption_cost: 12,
            actions: vec![RepairAction {
                action: action_name.to_string(),
                namespace: "team".to_string(),
                pod: "low-priority".to_string(),
                node: "gpu-1".to_string(),
                to_node: "gpu-2".to_string(),
                gpu_request: 1,
                disruption_cost: 12,
                reason: "free 1 GPU on gpu-1 for pending team/urgent".to_string(),
            }],
            skipped_candidates: Vec::new(),
            explanation: "repair explanation".to_string(),
        }];
        t
    }

    #[test]
    fn renders_bound_event_payload() {
        let events = render_binding_events(
            &[outcome(BindResult::Bound { dry_run: false })],
            "ksolver",
            "ksolver-0",
            42,
            "2026-07-02T12:00:00Z",
        );

        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.reason, "KsolverBound");
        assert_eq!(e.type_, "Normal");
        assert_eq!(e.team, "research");
        assert!(e.note.contains("team research"));
        assert_eq!(e.body["apiVersion"], "events.k8s.io/v1");
        assert_eq!(e.body["kind"], "Event");
        assert_eq!(e.body["regarding"]["kind"], "Pod");
        assert_eq!(e.body["regarding"]["name"], "train-a");
        assert_eq!(e.body["regarding"]["uid"], "uid-a");
        assert_eq!(e.body["related"]["kind"], "Node");
        assert_eq!(e.body["related"]["name"], "gpu-1");
        assert_eq!(e.body["action"], "BindingApplied");
        assert_eq!(e.body["reportingController"], REPORTING_CONTROLLER);
        assert_eq!(e.body["reportingInstance"], "ksolver-0");
    }

    #[test]
    fn maps_dry_run_skip_and_failure_to_distinct_reasons() {
        let events = render_binding_events(
            &[
                outcome(BindResult::Bound { dry_run: true }),
                outcome(BindResult::Skipped {
                    reason: "not ready".to_string(),
                }),
                outcome(BindResult::Failed {
                    error: "api error".to_string(),
                }),
            ],
            "ksolver",
            "",
            7,
            "2026-07-02T12:00:00Z",
        );

        assert_eq!(events[0].reason, "KsolverBindValidated");
        assert_eq!(events[0].type_, "Normal");
        assert_eq!(events[1].reason, "KsolverBindSkipped");
        assert_eq!(events[1].type_, "Normal");
        assert!(events[1].note.contains("not ready"));
        assert_eq!(events[2].reason, "KsolverBindFailed");
        assert_eq!(events[2].type_, "Warning");
        assert_eq!(events[2].body["reportingInstance"], "ksolver");
    }

    #[test]
    fn truncates_long_event_notes() {
        let events = render_binding_events(
            &[outcome(BindResult::Failed {
                error: "x".repeat(2000),
            })],
            "ksolver",
            "ksolver-0",
            1,
            "2026-07-02T12:00:00Z",
        );

        assert_eq!(events[0].note.chars().count(), MAX_EVENT_NOTE_CHARS);
        assert!(events[0].note.ends_with("..."));
        assert_eq!(events[0].body["note"], events[0].note);
    }

    #[test]
    fn renders_decision_event_payloads_for_placed_and_deferred_pods() {
        let trace = trace(vec![
            decision(PodPlacement::Placed {
                node: "gpu-1".to_string(),
            }),
            decision(PodPlacement::Unplaced {
                reason: "gang not admitted (insufficient capacity or quota)".to_string(),
            }),
        ]);
        let events = render_decision_events(&trace, "ksolver", "ksolver-0", "2026-07-02T12:00:00Z");

        assert_eq!(events.len(), 2);
        assert_eq!(events[0].reason, "KsolverPlacementRecommended");
        assert_eq!(events[0].type_, "Normal");
        assert_eq!(events[0].team, "research");
        assert_eq!(events[0].body["action"], "PlacementRecommended");
        assert_eq!(events[0].body["related"]["kind"], "Node");
        assert_eq!(events[0].body["related"]["name"], "gpu-1");
        assert!(events[0].note.contains("priority 7"));
        assert!(events[0].note.contains("queued 300s"));

        assert_eq!(events[1].reason, "KsolverPlacementDeferred");
        assert_eq!(events[1].type_, "Warning");
        assert_eq!(events[1].body["action"], "PlacementDeferred");
        assert!(events[1].body.get("related").is_none());
        assert!(events[1].note.contains("insufficient capacity or quota"));
    }

    #[test]
    fn renders_quota_throttled_decision_event_reason() {
        let mut d = decision(PodPlacement::Unplaced {
            reason: "gang not admitted (quota exhausted: 1 selected / 1 allowed for resources nvidia.com/gpu)"
                .to_string(),
        });
        d.caveats = vec![
            "quota exhausted: 1 selected / 1 allowed for resources nvidia.com/gpu".to_string(),
        ];
        let trace = trace(vec![d]);
        let events = render_decision_events(&trace, "ksolver", "ksolver-0", "2026-07-02T12:00:00Z");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].reason, "KsolverQuotaThrottled");
        assert_eq!(events[0].type_, "Warning");
        assert_eq!(events[0].body["action"], "QuotaThrottled");
        assert!(events[0].note.contains("quota exhausted"));
    }

    #[test]
    fn renders_budget_throttled_decision_event_reason() {
        let mut d = decision(PodPlacement::Unplaced {
            reason: "gang not admitted (budget exhausted: 1000000 selected / 1000000 monthly milli-units for tenant research)"
                .to_string(),
        });
        d.caveats = vec![
            "budget exhausted: 1000000 selected / 1000000 monthly milli-units for tenant research"
                .to_string(),
        ];
        let trace = trace(vec![d]);
        let events = render_decision_events(&trace, "ksolver", "ksolver-0", "2026-07-02T12:00:00Z");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].reason, "KsolverBudgetThrottled");
        assert_eq!(events[0].type_, "Warning");
        assert_eq!(events[0].body["action"], "BudgetThrottled");
        assert!(events[0].note.contains("budget exhausted"));
    }

    #[test]
    fn rendered_decision_event_parses_as_kubernetes_event() {
        let trace = trace(vec![decision(PodPlacement::Unplaced {
            reason: "gang not admitted (insufficient capacity)".to_string(),
        })]);
        let events = render_decision_events(&trace, "ksolver", "ksolver-0", "2026-07-02T12:00:00Z");
        let event = event_from_draft(&events[0]).expect("event payload should parse");

        assert_eq!(event.reason.as_deref(), Some("KsolverPlacementDeferred"));
        assert_eq!(
            event.reporting_controller.as_deref(),
            Some("ksolver.dev/scheduler")
        );
        assert_eq!(event.reporting_instance.as_deref(), Some("ksolver-0"));
    }

    #[test]
    fn renders_repair_event_payload_for_migration_recommendation() {
        let trace = trace_with_repair("migrate");
        let events = render_repair_events(&trace, "ksolver", "ksolver-0", "2026-07-02T12:00:00Z");

        assert_eq!(events.len(), 1);
        let e = &events[0];
        assert_eq!(e.reason, "KsolverRepairMigrationRecommended");
        assert_eq!(e.type_, "Warning");
        assert_eq!(e.namespace, "team");
        assert_eq!(e.pod, "low-priority");
        assert!(e.note.contains("migrating pod team/low-priority"));
        assert!(e.note.contains("team/urgent"));
        assert!(e.note.contains("frees 1 GPU"));
        assert_eq!(e.body["action"], "MigrationRecommended");
        assert_eq!(e.body["regarding"]["kind"], "Pod");
        assert_eq!(e.body["regarding"]["name"], "low-priority");
        assert_eq!(e.body["related"]["kind"], "Node");
        assert_eq!(e.body["related"]["name"], "gpu-1");
    }

    #[test]
    fn rendered_repair_event_parses_as_kubernetes_event() {
        let trace = trace_with_repair("preempt");
        let events = render_repair_events(&trace, "ksolver", "", "2026-07-02T12:00:00Z");
        let event = event_from_draft(&events[0]).expect("repair event payload should parse");

        assert_eq!(
            event.reason.as_deref(),
            Some("KsolverRepairPreemptionRecommended")
        );
        assert_eq!(event.action.as_deref(), Some("PreemptionRecommended"));
        assert_eq!(
            event.regarding.as_ref().and_then(|r| r.name.as_deref()),
            Some("low-priority")
        );
        assert_eq!(event.reporting_instance.as_deref(), Some("ksolver"));
    }
}
