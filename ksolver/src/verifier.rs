use crate::collector::build_client;
use crate::model::{
    NormalizedCluster, OptimizationInput, OptimizationPlan, ScenarioConfig, VerificationCheck,
    VerificationReport,
};
use anyhow::{Context, Result};
use k8s_openapi::api::core::v1 as corev1;
use k8s_openapi::api::scheduling::v1 as schedulingv1;
use k8s_openapi::api::storage::v1 as storagev1;
use kube::api::ListParams;
use kube::Api;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use tokio::time::{sleep, Duration, Instant};
use tracing::{info, warn};

const SELECTED_NODE_ANNOTATION: &str = "kube-scheduler-simulator.sigs.k8s.io/selected-node";
const FILTER_RESULT_ANNOTATION: &str = "kube-scheduler-simulator.sigs.k8s.io/filter-result";
const VERIFICATION_TIMEOUT: Duration = Duration::from_secs(10);
const VERIFICATION_POLL_INTERVAL: Duration = Duration::from_millis(350);

#[derive(Default)]
pub struct Verifier;

impl Verifier {
    pub fn new() -> Self {
        Self
    }

    pub async fn verify(
        &self,
        kubeconfig: &str,
        scenario: &ScenarioConfig,
        cluster: &NormalizedCluster,
        input: &OptimizationInput,
        plan: &OptimizationPlan,
    ) -> VerificationReport {
        let requested_backend = normalize_backend_name(&scenario.verification_backend);
        if requested_backend == "scheduler-simulator" {
            if scenario.verification_url.trim().is_empty() {
                let mut report = self.local_precheck(cluster, input, plan);
                report.status = "scheduler-simulator requested but no simulator URL configured; used local precheck".to_string();
                report.confidence = if report.rejected_moves > 0 {
                    "low".to_string()
                } else {
                    "medium".to_string()
                };
                return report;
            }

            match self
                .verify_with_scheduler_simulator(kubeconfig, scenario.verification_url.trim(), plan)
                .await
            {
                Ok(report) => return report,
                Err(err) => {
                    warn!(
                        backend = "scheduler-simulator",
                        url = scenario.verification_url.trim(),
                        error = %err,
                        "scheduler-simulator verification failed, falling back to local precheck"
                    );
                    let mut report = self.local_precheck(cluster, input, plan);
                    report.status = format!(
                        "scheduler-simulator verification failed: {err}; used local precheck"
                    );
                    report.confidence = "medium".to_string();
                    return report;
                }
            }
        }

        self.local_precheck(cluster, input, plan)
    }

    fn local_precheck(
        &self,
        cluster: &NormalizedCluster,
        input: &OptimizationInput,
        plan: &OptimizationPlan,
    ) -> VerificationReport {
        let workload_by_scope: BTreeMap<String, _> = cluster
            .workloads
            .iter()
            .map(|w| (format!("{}/{}", w.namespace, w.name), w))
            .collect();

        let grouped_member_scope_to_workload_id: BTreeMap<String, String> = input
            .workloads
            .iter()
            .flat_map(|w| {
                w.members
                    .iter()
                    .map(|m| (format!("{}/{}", m.namespace, m.name), w.id.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();

        let mut report = VerificationReport {
            backend: "local-precheck".to_string(),
            status: "local feasibility precheck complete; kube-scheduler-simulator backend not configured"
                .to_string(),
            confidence: "high".to_string(),
            blocker_count: plan.blockers.len() as i32,
            ..Default::default()
        };

        for blocker in &plan.blockers {
            report.checks.push(VerificationCheck {
                scope: blocker.scope.clone(),
                kind: "blocker".to_string(),
                status: "blocked".to_string(),
                detail: blocker.message.clone(),
            });
        }

        for mv in &plan.recommended_moves {
            let scope = format!("{}/{}", mv.namespace, mv.pod);
            let detail = if let Some(workload) = workload_by_scope.get(&scope) {
                if workload
                    .feasible_node_names
                    .iter()
                    .any(|n| n == &mv.to_node)
                {
                    report.verified_moves += 1;
                    report.checks.push(VerificationCheck {
                        scope,
                        kind: "move".to_string(),
                        status: "verified".to_string(),
                        detail: format!("target node {} is in normalized feasible set", mv.to_node),
                    });
                    continue;
                }

                format!(
                    "target node {} not in normalized feasible set [{}]",
                    mv.to_node,
                    workload.feasible_node_names.join(",")
                )
            } else if let Some(workload_id) = grouped_member_scope_to_workload_id.get(&scope) {
                format!(
                    "move belongs to grouped workload {}; scheduler-simulator backend needed for stronger verification",
                    workload_id
                )
            } else {
                "workload missing from normalized cluster".to_string()
            };

            report.rejected_moves += 1;
            report.checks.push(VerificationCheck {
                scope,
                kind: "move".to_string(),
                status: "rejected".to_string(),
                detail,
            });
        }

        report.confidence = if report.rejected_moves > 0 {
            "low".to_string()
        } else if report.blocker_count > 0 {
            "medium".to_string()
        } else {
            "high".to_string()
        };

        report
    }

    async fn verify_with_scheduler_simulator(
        &self,
        kubeconfig: &str,
        simulator_url: &str,
        plan: &OptimizationPlan,
    ) -> Result<VerificationReport> {
        let raw = collect_simulator_resources(kubeconfig).await?;
        let payload = prepare_simulator_payload(&raw, plan);
        let client = reqwest::Client::new();
        let base_url = simulator_url.trim_end_matches('/');

        reset_simulator(&client, base_url).await?;
        import_snapshot(&client, base_url, &payload).await?;
        let exported = wait_for_export(&client, base_url, plan).await?;

        Ok(build_simulator_report(plan, exported))
    }
}

#[derive(Clone, Default)]
struct SimulatorResources {
    pods: Vec<corev1::Pod>,
    nodes: Vec<corev1::Node>,
    pvs: Vec<corev1::PersistentVolume>,
    pvcs: Vec<corev1::PersistentVolumeClaim>,
    storage_classes: Vec<storagev1::StorageClass>,
    priority_classes: Vec<schedulingv1::PriorityClass>,
    namespaces: Vec<corev1::Namespace>,
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct SimulatorImportPayload {
    pods: Vec<corev1::Pod>,
    nodes: Vec<corev1::Node>,
    pvs: Vec<corev1::PersistentVolume>,
    pvcs: Vec<corev1::PersistentVolumeClaim>,
    storage_classes: Vec<storagev1::StorageClass>,
    priority_classes: Vec<schedulingv1::PriorityClass>,
    namespaces: Vec<corev1::Namespace>,
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SimulatorExportPayload {
    #[serde(default)]
    pods: Vec<corev1::Pod>,
}

fn normalize_backend_name(value: &str) -> &str {
    match value.trim() {
        "" | "auto" => "scheduler-simulator",
        other => other,
    }
}

async fn collect_simulator_resources(kubeconfig: &str) -> Result<SimulatorResources> {
    let client = build_client(kubeconfig).await?;
    let list_params = ListParams::default();

    let pods_api: Api<corev1::Pod> = Api::all(client.clone());
    let nodes_api: Api<corev1::Node> = Api::all(client.clone());
    let pvs_api: Api<corev1::PersistentVolume> = Api::all(client.clone());
    let pvcs_api: Api<corev1::PersistentVolumeClaim> = Api::all(client.clone());
    let storage_classes_api: Api<storagev1::StorageClass> = Api::all(client.clone());
    let priority_classes_api: Api<schedulingv1::PriorityClass> = Api::all(client.clone());
    let namespaces_api: Api<corev1::Namespace> = Api::all(client);

    let (pods, nodes, pvs, pvcs, storage_classes, priority_classes, namespaces) = tokio::try_join!(
        pods_api.list(&list_params),
        nodes_api.list(&list_params),
        pvs_api.list(&list_params),
        pvcs_api.list(&list_params),
        storage_classes_api.list(&list_params),
        priority_classes_api.list(&list_params),
        namespaces_api.list(&list_params),
    )
    .context("collect raw cluster resources for scheduler-simulator verification")?;

    Ok(SimulatorResources {
        pods: pods
            .items
            .into_iter()
            .filter(|pod| {
                pod.metadata.deletion_timestamp.is_none()
                    && pod
                        .status
                        .as_ref()
                        .and_then(|s| s.phase.as_deref())
                        .map(|phase| phase != "Succeeded" && phase != "Failed")
                        .unwrap_or(true)
            })
            .collect(),
        nodes: nodes.items,
        pvs: pvs.items,
        pvcs: pvcs.items,
        storage_classes: storage_classes.items,
        priority_classes: priority_classes.items,
        namespaces: namespaces.items,
    })
}

fn prepare_simulator_payload(
    raw: &SimulatorResources,
    plan: &OptimizationPlan,
) -> SimulatorImportPayload {
    let moved_scope_set: BTreeSet<String> = plan
        .recommended_moves
        .iter()
        .map(|mv| format!("{}/{}", mv.namespace, mv.pod))
        .collect();

    let pods = raw
        .pods
        .iter()
        .cloned()
        .map(|pod| {
            if moved_scope_set.contains(&pod_scope(&pod)) {
                clone_as_unscheduled_verification_pod(pod)
            } else {
                pod
            }
        })
        .collect();

    SimulatorImportPayload {
        pods,
        nodes: raw.nodes.clone(),
        pvs: raw.pvs.clone(),
        pvcs: raw.pvcs.clone(),
        storage_classes: raw.storage_classes.clone(),
        priority_classes: raw.priority_classes.clone(),
        namespaces: raw.namespaces.clone(),
    }
}

fn clone_as_unscheduled_verification_pod(mut pod: corev1::Pod) -> corev1::Pod {
    if let Some(spec) = pod.spec.as_mut() {
        spec.node_name = None;
    }
    pod.status = None;

    pod.metadata.uid = None;
    pod.metadata.resource_version = None;
    pod.metadata.creation_timestamp = None;
    pod.metadata.managed_fields = None;
    pod.metadata.deletion_timestamp = None;
    pod.metadata.generation = None;
    pod.metadata
        .annotations
        .get_or_insert_with(Default::default)
        .insert(
            "syslens-solver.sigs.k8s.io/verification".to_string(),
            "true".to_string(),
        );
    pod
}

async fn reset_simulator(client: &reqwest::Client, base_url: &str) -> Result<()> {
    let response = client
        .put(format!("{base_url}/api/v1/reset"))
        .send()
        .await
        .context("send scheduler-simulator reset request")?;

    if response.status() != StatusCode::ACCEPTED && response.status() != StatusCode::OK {
        anyhow::bail!(
            "scheduler-simulator reset failed with status {}",
            response.status()
        );
    }

    sleep(Duration::from_millis(250)).await;
    Ok(())
}

async fn import_snapshot(
    client: &reqwest::Client,
    base_url: &str,
    payload: &SimulatorImportPayload,
) -> Result<()> {
    let response = client
        .post(format!("{base_url}/api/v1/import"))
        .json(payload)
        .send()
        .await
        .context("send scheduler-simulator import request")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "scheduler-simulator import failed with status {}",
            response.status()
        );
    }

    Ok(())
}

async fn wait_for_export(
    client: &reqwest::Client,
    base_url: &str,
    plan: &OptimizationPlan,
) -> Result<SimulatorExportPayload> {
    let expected_move_scopes: BTreeSet<String> = plan
        .recommended_moves
        .iter()
        .map(|mv| format!("{}/{}", mv.namespace, mv.pod))
        .collect();
    let deadline = Instant::now() + VERIFICATION_TIMEOUT;
    let mut _latest = SimulatorExportPayload::default();

    loop {
        let response = client
            .get(format!("{base_url}/api/v1/export"))
            .send()
            .await
            .context("send scheduler-simulator export request")?;

        if !response.status().is_success() {
            anyhow::bail!(
                "scheduler-simulator export failed with status {}",
                response.status()
            );
        }

        _latest = response
            .json::<SimulatorExportPayload>()
            .await
            .context("decode scheduler-simulator export response")?;

        let ready_scopes = _latest
            .pods
            .iter()
            .filter_map(|pod| {
                let scope = pod_scope(pod);
                if expected_move_scopes.contains(&scope) && pod_assigned_node(pod).is_some() {
                    Some(scope)
                } else {
                    None
                }
            })
            .collect::<BTreeSet<_>>();

        if ready_scopes.len() == expected_move_scopes.len() || Instant::now() >= deadline {
            return Ok(_latest);
        }

        sleep(VERIFICATION_POLL_INTERVAL).await;
    }
}

fn build_simulator_report(
    plan: &OptimizationPlan,
    exported: SimulatorExportPayload,
) -> VerificationReport {
    let pod_by_scope: BTreeMap<String, corev1::Pod> = exported
        .pods
        .into_iter()
        .map(|pod| (pod_scope(&pod), pod))
        .collect();

    let mut report = VerificationReport {
        backend: "scheduler-simulator".to_string(),
        status: "kube-scheduler-simulator verification complete".to_string(),
        blocker_count: plan.blockers.len() as i32,
        ..Default::default()
    };

    for mv in &plan.recommended_moves {
        let scope = format!("{}/{}", mv.namespace, mv.pod);
        let Some(pod) = pod_by_scope.get(&scope) else {
            report.rejected_moves += 1;
            report.checks.push(VerificationCheck {
                scope,
                kind: "move".to_string(),
                status: "rejected".to_string(),
                detail: "verification pod missing from simulator export".to_string(),
            });
            continue;
        };

        let assigned = pod_assigned_node(pod);
        let detail_suffix = pod_scheduler_detail(pod);
        match assigned {
            Some(node) if node == mv.to_node => {
                report.verified_moves += 1;
                report.checks.push(VerificationCheck {
                    scope,
                    kind: "move".to_string(),
                    status: "verified".to_string(),
                    detail: format!("simulator selected target node {node}{detail_suffix}"),
                });
            }
            Some(node) => {
                report.rejected_moves += 1;
                report.checks.push(VerificationCheck {
                    scope,
                    kind: "move".to_string(),
                    status: "rejected".to_string(),
                    detail: format!(
                        "simulator selected node {node} instead of {}{detail_suffix}",
                        mv.to_node
                    ),
                });
            }
            None => {
                report.rejected_moves += 1;
                report.checks.push(VerificationCheck {
                    scope,
                    kind: "move".to_string(),
                    status: "rejected".to_string(),
                    detail: format!("simulator left pod unscheduled{detail_suffix}"),
                });
            }
        }
    }

    for blocker in &plan.blockers {
        let Some(pod) = pod_by_scope.get(&blocker.scope) else {
            report.checks.push(VerificationCheck {
                scope: blocker.scope.clone(),
                kind: "blocker".to_string(),
                status: "blocked".to_string(),
                detail: blocker.message.clone(),
            });
            continue;
        };

        match pod_assigned_node(pod) {
            Some(node) => report.checks.push(VerificationCheck {
                scope: blocker.scope.clone(),
                kind: "blocker".to_string(),
                status: "rejected".to_string(),
                detail: format!(
                    "simulator scheduled blocker on node {node}{}",
                    pod_scheduler_detail(pod)
                ),
            }),
            None => report.checks.push(VerificationCheck {
                scope: blocker.scope.clone(),
                kind: "blocker".to_string(),
                status: "blocked".to_string(),
                detail: format!(
                    "simulator kept blocker unscheduled{}",
                    pod_scheduler_detail(pod)
                ),
            }),
        }
    }

    report.confidence = if report.rejected_moves > 0 {
        "low".to_string()
    } else if report.verified_moves > 0 {
        "high".to_string()
    } else {
        "medium".to_string()
    };

    info!(
        backend = %report.backend,
        verified_moves = report.verified_moves,
        rejected_moves = report.rejected_moves,
        blocker_count = report.blocker_count,
        "verification complete"
    );

    report
}

fn pod_scope(pod: &corev1::Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.clone().unwrap_or_default(),
        pod.metadata.name.clone().unwrap_or_default()
    )
}

fn pod_assigned_node(pod: &corev1::Pod) -> Option<String> {
    pod.metadata
        .annotations
        .as_ref()
        .and_then(|annotations| annotations.get(SELECTED_NODE_ANNOTATION).cloned())
        .filter(|value| !value.is_empty())
        .or_else(|| {
            pod.spec
                .as_ref()
                .and_then(|spec| spec.node_name.clone())
                .filter(|value| !value.is_empty())
        })
}

fn pod_scheduler_detail(pod: &corev1::Pod) -> String {
    let Some(annotations) = pod.metadata.annotations.as_ref() else {
        return String::new();
    };

    if let Some(filter_result) = annotations.get(FILTER_RESULT_ANNOTATION) {
        return format!("; filter-result={filter_result}");
    }

    String::new()
}

#[cfg(test)]
mod tests {
    use super::Verifier;
    use crate::model::{
        Blocker, NormalizedCluster, NormalizedWorkload, OptimizationInput, OptimizationNode,
        OptimizationPlan, OptimizationWorkload, OptimizationWorkloadMember, PodMove,
        ScenarioConfig,
    };
    use k8s_openapi::api::core::v1 as corev1;
    use std::collections::BTreeMap;

    #[test]
    fn verifies_move_against_normalized_feasible_set() {
        let cluster = NormalizedCluster {
            workloads: vec![NormalizedWorkload {
                namespace: "ns".to_string(),
                name: "pod-a".to_string(),
                feasible_node_names: vec!["node-b".to_string()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let input = OptimizationInput {
            nodes: vec![OptimizationNode {
                name: "ng-001".to_string(),
                members: vec!["node-b".to_string()],
                ..Default::default()
            }],
            workloads: vec![OptimizationWorkload {
                id: "ns/pod-a".to_string(),
                members: vec![OptimizationWorkloadMember {
                    namespace: "ns".to_string(),
                    name: "pod-a".to_string(),
                    current_node: "node-a".to_string(),
                }],
                ..Default::default()
            }],
            anti_affinity_pairs: Vec::new(),
        };
        let plan = OptimizationPlan {
            recommended_moves: vec![crate::model::PodMove {
                namespace: "ns".to_string(),
                pod: "pod-a".to_string(),
                from_node: "node-a".to_string(),
                to_node: "node-b".to_string(),
                reason: String::new(),
            }],
            blockers: vec![Blocker {
                scope: "ns/blocked".to_string(),
                message: "unschedulable".to_string(),
            }],
            ..Default::default()
        };

        let report = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(Verifier::new().verify(
                "",
                &ScenarioConfig::default(),
                &cluster,
                &input,
                &plan,
            ));

        assert_eq!(report.verified_moves, 1);
        assert_eq!(report.rejected_moves, 0);
        assert_eq!(report.blocker_count, 1);
    }

    #[test]
    fn simulator_payload_replaces_moved_pod_with_unscheduled_clone() {
        let raw = super::SimulatorResources {
            pods: vec![corev1::Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some("pod-a".to_string()),
                    namespace: Some("ns".to_string()),
                    resource_version: Some("123".to_string()),
                    ..Default::default()
                },
                spec: Some(corev1::PodSpec {
                    node_name: Some("node-a".to_string()),
                    ..Default::default()
                }),
                status: Some(corev1::PodStatus {
                    phase: Some("Running".to_string()),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };
        let plan = OptimizationPlan {
            recommended_moves: vec![PodMove {
                namespace: "ns".to_string(),
                pod: "pod-a".to_string(),
                from_node: "node-a".to_string(),
                to_node: "node-b".to_string(),
                reason: String::new(),
            }],
            ..Default::default()
        };

        let payload = super::prepare_simulator_payload(&raw, &plan);
        let pod = &payload.pods[0];
        assert_eq!(pod.spec.as_ref().and_then(|s| s.node_name.clone()), None);
        assert!(pod.status.is_none());
        assert!(pod.metadata.resource_version.is_none());
    }

    #[test]
    fn simulator_report_uses_selected_node_annotation() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            super::SELECTED_NODE_ANNOTATION.to_string(),
            "node-b".to_string(),
        );
        let exported = super::SimulatorExportPayload {
            pods: vec![corev1::Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some("pod-a".to_string()),
                    namespace: Some("ns".to_string()),
                    annotations: Some(annotations),
                    ..Default::default()
                },
                ..Default::default()
            }],
        };
        let plan = OptimizationPlan {
            recommended_moves: vec![PodMove {
                namespace: "ns".to_string(),
                pod: "pod-a".to_string(),
                from_node: "node-a".to_string(),
                to_node: "node-b".to_string(),
                reason: String::new(),
            }],
            ..Default::default()
        };

        let report = super::build_simulator_report(&plan, exported);
        assert_eq!(report.verified_moves, 1);
        assert_eq!(report.rejected_moves, 0);
    }
}
