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
use std::future::Future;
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{info, warn};

const SELECTED_NODE_ANNOTATION: &str = "kube-scheduler-simulator.sigs.k8s.io/selected-node";
pub(crate) const FILTER_RESULT_ANNOTATION: &str =
    "kube-scheduler-simulator.sigs.k8s.io/filter-result";
const VERIFICATION_TIMEOUT: Duration = Duration::from_secs(10);
const VERIFICATION_POLL_INTERVAL: Duration = Duration::from_millis(350);
// The post-reset drain is a distinct operation from verification polling: the simulator's embedded
// KWOK re-manages nodes and can be slow to clear objects between scenarios, so it gets its own,
// longer budget. Kept separate from VERIFICATION_TIMEOUT so scheduling-verification patience is
// unchanged (a genuinely-infeasible pod still fails fast at 10s).
const SIMULATOR_RESET_TIMEOUT: Duration = Duration::from_secs(30);
const SIMULATOR_RESET_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SIMULATOR_RESET_STABLE_POLLS: usize = 1;
#[allow(dead_code)]
const SIMULATOR_BATCH_STABLE_POLLS: usize = 3;

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
pub(crate) struct SimulatorResources {
    pub(crate) pods: Vec<corev1::Pod>,
    pub(crate) nodes: Vec<corev1::Node>,
    pub(crate) pvs: Vec<corev1::PersistentVolume>,
    pub(crate) pvcs: Vec<corev1::PersistentVolumeClaim>,
    pub(crate) storage_classes: Vec<storagev1::StorageClass>,
    pub(crate) priority_classes: Vec<schedulingv1::PriorityClass>,
    pub(crate) namespaces: Vec<corev1::Namespace>,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SimulatorImportPayload {
    pub(crate) pods: Vec<corev1::Pod>,
    pub(crate) nodes: Vec<corev1::Node>,
    pub(crate) pvs: Vec<corev1::PersistentVolume>,
    pub(crate) pvcs: Vec<corev1::PersistentVolumeClaim>,
    pub(crate) storage_classes: Vec<storagev1::StorageClass>,
    pub(crate) priority_classes: Vec<schedulingv1::PriorityClass>,
    pub(crate) namespaces: Vec<corev1::Namespace>,
    #[serde(default = "default_scheduler_config")]
    #[serde(skip_serializing_if = "serde_json::Value::is_null")]
    pub(crate) scheduler_config: serde_json::Value,
}

impl Default for SimulatorImportPayload {
    fn default() -> Self {
        Self {
            pods: Vec::new(),
            nodes: Vec::new(),
            pvs: Vec::new(),
            pvcs: Vec::new(),
            storage_classes: Vec::new(),
            priority_classes: Vec::new(),
            namespaces: Vec::new(),
            scheduler_config: default_scheduler_config(),
        }
    }
}

pub(crate) fn default_scheduler_config() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kubescheduler.config.k8s.io/v1",
        "kind": "KubeSchedulerConfiguration",
        "leaderElection": {
            "leaderElect": false
        }
    })
}

/// A KubeSchedulerConfiguration that makes NodeResourcesFit score with `MostAllocated` (bin-packing)
/// instead of the default `LeastAllocated` (spread), weighting GPUs heavily. Used as the *harder*
/// kube-scheduler baseline in the GPU comparison suite.
pub(crate) fn binpack_scheduler_config() -> serde_json::Value {
    serde_json::json!({
        "apiVersion": "kubescheduler.config.k8s.io/v1",
        "kind": "KubeSchedulerConfiguration",
        "leaderElection": {
            "leaderElect": false
        },
        "profiles": [{
            "schedulerName": "default-scheduler",
            "pluginConfig": [{
                "name": "NodeResourcesFit",
                "args": {
                    "apiVersion": "kubescheduler.config.k8s.io/v1",
                    "kind": "NodeResourcesFitArgs",
                    "scoringStrategy": {
                        "type": "MostAllocated",
                        "resources": [
                            {"name": "nvidia.com/gpu", "weight": 100},
                            {"name": "cpu", "weight": 1},
                            {"name": "memory", "weight": 1}
                        ]
                    }
                }
            }]
        }]
    })
}

#[derive(Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SimulatorExportPayload {
    // The simulator returns `null` (not `[]`) for an empty list, and `#[serde(default)]` only
    // covers an ABSENT key — so null must be mapped to the default explicitly.
    #[serde(default, deserialize_with = "null_to_default")]
    pub(crate) pods: Vec<corev1::Pod>,
}

pub(crate) struct SimulatorBatchReport {
    pub(crate) export: SimulatorExportPayload,
    pub(crate) diagnostics: SimulatorBatchDiagnostics,
}

#[derive(Debug)]
pub(crate) struct SimulatorBatchTimeoutError {
    message: String,
    pub(crate) diagnostics: SimulatorBatchDiagnostics,
}

impl SimulatorBatchTimeoutError {
    fn new(message: impl Into<String>, diagnostics: SimulatorBatchDiagnostics) -> Self {
        Self {
            message: message.into(),
            diagnostics,
        }
    }
}

impl std::fmt::Display for SimulatorBatchTimeoutError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.message, self.diagnostics.summary())
    }
}

impl std::error::Error for SimulatorBatchTimeoutError {}

#[derive(Clone, Debug, Default)]
pub(crate) struct SimulatorBatchDiagnostics {
    pub(crate) elapsed_millis: u128,
    pub(crate) phase: String,
    pub(crate) state: SimulatorBatchState,
    pub(crate) stable_polls: usize,
    pub(crate) timed_out: bool,
    pub(crate) phase_timings: Vec<SimulatorPhaseTiming>,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SimulatorPhaseTiming {
    pub(crate) phase: String,
    pub(crate) duration_millis: u64,
    pub(crate) cumulative_millis: u64,
}

impl SimulatorBatchDiagnostics {
    pub(crate) fn summary(&self) -> String {
        format!(
            "phase={}, elapsed={}ms, targets={}, present={}, terminal_present={}, missing={}, stable_polls={}, timed_out={}",
            self.phase,
            self.elapsed_millis,
            self.state.target_count,
            self.state.present_targets,
            self.state.terminal_present_targets,
            self.state.missing_targets(),
            self.stable_polls,
            self.timed_out
        )
    }
}

/// Deserialize helper: treat JSON `null` (and an absent field) as the type's default.
fn null_to_default<'de, D, T>(deserializer: D) -> std::result::Result<T, D::Error>
where
    T: Default + Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Option::<T>::deserialize(deserializer)?.unwrap_or_default())
}

fn normalize_backend_name(value: &str) -> &str {
    match value.trim() {
        "" | "auto" => "scheduler-simulator",
        other => other,
    }
}

pub(crate) async fn collect_simulator_resources(kubeconfig: &str) -> Result<SimulatorResources> {
    let client = build_client(kubeconfig, None).await?;
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
        scheduler_config: default_scheduler_config(),
    }
}

pub(crate) fn clone_as_unscheduled_verification_pod(mut pod: corev1::Pod) -> corev1::Pod {
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

pub(crate) async fn reset_simulator(client: &reqwest::Client, base_url: &str) -> Result<()> {
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

    let deadline = Instant::now() + SIMULATOR_RESET_TIMEOUT;
    let mut empty_polls = 0_usize;
    loop {
        let response = client
            .get(format!("{base_url}/api/v1/export"))
            .send()
            .await
            .context("send scheduler-simulator export request after reset")?;
        if !response.status().is_success() {
            anyhow::bail!(
                "scheduler-simulator export after reset failed with status {}",
                response.status()
            );
        }
        let latest = response
            .json::<serde_json::Value>()
            .await
            .context("decode scheduler-simulator export response after reset")?;
        let pod_count = json_array_len(&latest, "pods");
        let node_count = json_array_len(&latest, "nodes");
        let namespace_count = json_array_len(&latest, "namespaces");
        if pod_count == 0 && node_count == 0 && namespace_count == 0 {
            empty_polls += 1;
            if empty_polls >= SIMULATOR_RESET_STABLE_POLLS {
                return Ok(());
            }
        } else {
            empty_polls = 0;
        }
        if Instant::now() >= deadline {
            anyhow::bail!(
                "scheduler-simulator reset did not drain existing objects within {:?}: pods={}, nodes={}, namespaces={}",
                SIMULATOR_RESET_TIMEOUT,
                pod_count,
                node_count,
                namespace_count
            );
        }
        sleep(SIMULATOR_RESET_POLL_INTERVAL).await;
    }
}

fn json_array_len(value: &serde_json::Value, key: &str) -> usize {
    value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
}

pub(crate) async fn import_snapshot(
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
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|err| format!("<failed to read response body: {err}>"));
        anyhow::bail!("scheduler-simulator import failed with status {status}: {body}",);
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

/// Reset the simulator, import a snapshot ONCE, and poll the export until target pods have
/// resolved (assigned a node or a filter-result annotation), the target set is stable, or the
/// timeout elapses. Returns the final export plus batch diagnostics. This is the batch equivalent
/// of `schedule_snapshot`: one reset+import+poll for the whole scenario instead of one per pod.
/// Missing stable targets are accepted because simulator preemption can remove victim pods from the
/// export.
#[allow(dead_code)]
pub(crate) async fn schedule_all_snapshot_report_with_timeout(
    simulator_url: &str,
    payload: &SimulatorImportPayload,
    target_scopes: &std::collections::BTreeSet<String>,
    timeout: Duration,
) -> Result<SimulatorBatchReport> {
    schedule_all_snapshot_report_with_timeout_and_stable_polls(
        simulator_url,
        payload,
        target_scopes,
        timeout,
        SIMULATOR_BATCH_STABLE_POLLS,
    )
    .await
}

pub(crate) async fn schedule_all_snapshot_report_with_timeout_and_stable_polls(
    simulator_url: &str,
    payload: &SimulatorImportPayload,
    target_scopes: &std::collections::BTreeSet<String>,
    timeout: Duration,
    required_stable_polls: usize,
) -> Result<SimulatorBatchReport> {
    let client = reqwest::Client::new();
    let base_url = simulator_url.trim_end_matches('/');
    let started = Instant::now();
    let deadline = Instant::now() + timeout;
    let mut phase_timings = Vec::new();
    let phase_started = Instant::now();
    with_batch_deadline(
        deadline,
        &started,
        "reset",
        target_scopes.len(),
        reset_simulator(&client, base_url),
    )
    .await?;
    phase_timings.push(simulator_phase_timing("reset", phase_started, &started));
    if !payload.priority_classes.is_empty() {
        // The simulator applies pods before PriorityClasses in a full snapshot, so priority pods
        // can be rejected unless their classes already exist. Keep this import config-free to
        // avoid an extra scheduler restart before the real snapshot import.
        let priority_payload = SimulatorImportPayload {
            priority_classes: payload.priority_classes.clone(),
            ..Default::default()
        };
        let phase_started = Instant::now();
        with_batch_deadline(
            deadline,
            &started,
            "priority class import",
            target_scopes.len(),
            import_snapshot(&client, base_url, &priority_payload),
        )
        .await?;
        phase_timings.push(simulator_phase_timing(
            "priority class import",
            phase_started,
            &started,
        ));
    }
    let phase_started = Instant::now();
    with_batch_deadline(
        deadline,
        &started,
        "snapshot import",
        target_scopes.len(),
        import_snapshot(&client, base_url, payload),
    )
    .await?;
    phase_timings.push(simulator_phase_timing(
        "snapshot import",
        phase_started,
        &started,
    ));

    let mut last_signature = BTreeMap::new();
    let mut stable_polls = 0_usize;
    let mut last_state = SimulatorBatchState {
        target_count: target_scopes.len(),
        ..Default::default()
    };
    loop {
        let now = Instant::now();
        if now >= deadline {
            return Err(SimulatorBatchTimeoutError::new(
                "scheduler-simulator batch timed out before export poll",
                SimulatorBatchDiagnostics {
                    elapsed_millis: started.elapsed().as_millis(),
                    phase: "export".to_string(),
                    state: last_state.clone(),
                    stable_polls,
                    timed_out: true,
                    phase_timings,
                },
            )
            .into());
        }
        let export_started = Instant::now();
        let response = match tokio::time::timeout(
            deadline - now,
            client.get(format!("{base_url}/api/v1/export")).send(),
        )
        .await
        {
            Ok(result) => {
                phase_timings.push(simulator_phase_timing(
                    "export request",
                    export_started,
                    &started,
                ));
                result.context("send scheduler-simulator export request")?
            }
            Err(_) => {
                phase_timings.push(simulator_phase_timing(
                    "export request",
                    export_started,
                    &started,
                ));
                return Err(SimulatorBatchTimeoutError::new(
                    "scheduler-simulator batch timed out during export request",
                    SimulatorBatchDiagnostics {
                        elapsed_millis: started.elapsed().as_millis(),
                        phase: "export".to_string(),
                        state: last_state.clone(),
                        stable_polls,
                        timed_out: true,
                        phase_timings,
                    },
                )
                .into());
            }
        };
        if !response.status().is_success() {
            anyhow::bail!(
                "scheduler-simulator export failed with status {}",
                response.status()
            );
        }
        let now = Instant::now();
        if now >= deadline {
            return Err(SimulatorBatchTimeoutError::new(
                "scheduler-simulator batch timed out before export decode",
                SimulatorBatchDiagnostics {
                    elapsed_millis: started.elapsed().as_millis(),
                    phase: "export-decode".to_string(),
                    state: last_state.clone(),
                    stable_polls,
                    timed_out: true,
                    phase_timings,
                },
            )
            .into());
        }
        let decode_started = Instant::now();
        let latest =
            match tokio::time::timeout(deadline - now, response.json::<SimulatorExportPayload>())
                .await
            {
                Ok(result) => {
                    phase_timings.push(simulator_phase_timing(
                        "export decode",
                        decode_started,
                        &started,
                    ));
                    result.context("decode scheduler-simulator export response")?
                }
                Err(_) => {
                    phase_timings.push(simulator_phase_timing(
                        "export decode",
                        decode_started,
                        &started,
                    ));
                    return Err(SimulatorBatchTimeoutError::new(
                        "scheduler-simulator batch timed out during export decode",
                        SimulatorBatchDiagnostics {
                            elapsed_millis: started.elapsed().as_millis(),
                            phase: "export-decode".to_string(),
                            state: last_state.clone(),
                            stable_polls,
                            timed_out: true,
                            phase_timings,
                        },
                    )
                    .into());
                }
            };

        let stable = simulator_batch_is_stable(
            &latest,
            target_scopes,
            &mut last_signature,
            &mut stable_polls,
            required_stable_polls,
        );

        let state = simulator_batch_state(&latest, target_scopes);
        last_state = state.clone();
        let timed_out = Instant::now() >= deadline;
        if state.all_targets_resolved() || (state.visible_targets_terminal() && stable) {
            return Ok(SimulatorBatchReport {
                export: latest,
                diagnostics: SimulatorBatchDiagnostics {
                    elapsed_millis: started.elapsed().as_millis(),
                    phase: "poll".to_string(),
                    state,
                    stable_polls,
                    timed_out: false,
                    phase_timings,
                },
            });
        }
        if timed_out {
            return Err(SimulatorBatchTimeoutError::new(
                "scheduler-simulator batch timed out during poll",
                SimulatorBatchDiagnostics {
                    elapsed_millis: started.elapsed().as_millis(),
                    phase: "poll".to_string(),
                    state,
                    stable_polls,
                    timed_out: true,
                    phase_timings,
                },
            )
            .into());
        }
        sleep(VERIFICATION_POLL_INTERVAL).await;
    }
}

fn simulator_phase_timing(
    phase: &str,
    phase_started: Instant,
    batch_started: &Instant,
) -> SimulatorPhaseTiming {
    SimulatorPhaseTiming {
        phase: phase.to_string(),
        duration_millis: phase_started.elapsed().as_millis() as u64,
        cumulative_millis: batch_started.elapsed().as_millis() as u64,
    }
}

async fn with_batch_deadline<T, Fut>(
    deadline: Instant,
    started: &Instant,
    phase: &str,
    target_count: usize,
    fut: Fut,
) -> Result<T>
where
    Fut: Future<Output = Result<T>>,
{
    let now = Instant::now();
    if now >= deadline {
        return Err(SimulatorBatchTimeoutError::new(
            format!("scheduler-simulator batch timed out before {phase}"),
            SimulatorBatchDiagnostics {
                elapsed_millis: started.elapsed().as_millis(),
                phase: phase.to_string(),
                state: SimulatorBatchState {
                    target_count,
                    ..Default::default()
                },
                timed_out: true,
                ..Default::default()
            },
        )
        .into());
    }
    match timeout(deadline - now, fut).await {
        Ok(result) => result,
        Err(_) => Err(SimulatorBatchTimeoutError::new(
            format!("scheduler-simulator batch timed out during {phase}"),
            SimulatorBatchDiagnostics {
                elapsed_millis: started.elapsed().as_millis(),
                phase: phase.to_string(),
                state: SimulatorBatchState {
                    target_count,
                    ..Default::default()
                },
                timed_out: true,
                ..Default::default()
            },
        )
        .into()),
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SimulatorBatchState {
    pub(crate) target_count: usize,
    pub(crate) present_targets: usize,
    pub(crate) terminal_present_targets: usize,
}

impl SimulatorBatchState {
    pub(crate) fn missing_targets(&self) -> usize {
        self.target_count.saturating_sub(self.present_targets)
    }

    fn all_targets_resolved(&self) -> bool {
        self.target_count > 0 && self.terminal_present_targets == self.target_count
    }

    fn visible_targets_terminal(&self) -> bool {
        self.present_targets > 0 && self.present_targets == self.terminal_present_targets
    }
}

fn simulator_batch_state(
    latest: &SimulatorExportPayload,
    target_scopes: &BTreeSet<String>,
) -> SimulatorBatchState {
    let mut state = SimulatorBatchState {
        target_count: target_scopes.len(),
        ..Default::default()
    };
    for pod in &latest.pods {
        let scope = pod_scope(pod);
        if !target_scopes.contains(&scope) {
            continue;
        }
        state.present_targets += 1;
        if pod_is_terminal_in_simulator(pod) {
            state.terminal_present_targets += 1;
        }
    }
    state
}

fn pod_is_terminal_in_simulator(pod: &corev1::Pod) -> bool {
    pod_assigned_node(pod).is_some()
        || pod
            .metadata
            .annotations
            .as_ref()
            .map(|a| a.contains_key(FILTER_RESULT_ANNOTATION))
            .unwrap_or(false)
}

fn simulator_batch_is_stable(
    latest: &SimulatorExportPayload,
    target_scopes: &BTreeSet<String>,
    last_signature: &mut BTreeMap<String, (Option<String>, bool)>,
    stable_polls: &mut usize,
    required_stable_polls: usize,
) -> bool {
    let target_signature = simulator_target_signature(latest, target_scopes);
    // Preemption in kube-scheduler-simulator can remove victim pods from the export entirely.
    // For benchmark snapshots, a stable missing target is a terminal "not placed" outcome.
    if target_signature == *last_signature {
        *stable_polls += 1;
    } else {
        *stable_polls = 0;
        *last_signature = target_signature;
    }
    *stable_polls >= required_stable_polls
}

fn simulator_target_signature(
    latest: &SimulatorExportPayload,
    target_scopes: &BTreeSet<String>,
) -> BTreeMap<String, (Option<String>, bool)> {
    latest
        .pods
        .iter()
        .filter_map(|pod| {
            let scope = pod_scope(pod);
            target_scopes.contains(&scope).then(|| {
                let has_filter_result = pod
                    .metadata
                    .annotations
                    .as_ref()
                    .map(|a| a.contains_key(FILTER_RESULT_ANNOTATION))
                    .unwrap_or(false);
                (scope, (pod_assigned_node(pod), has_filter_result))
            })
        })
        .collect()
}

/// Reset the simulator, import a snapshot, and poll the export until the pod at `pod_scope`
/// has either an assigned node or a filter-result annotation (or timeout). Used by the
/// feasibility conformance harness to obtain the scheduler's Filter verdict for one pod.
pub(crate) async fn schedule_snapshot(
    simulator_url: &str,
    payload: &SimulatorImportPayload,
    pod_scope_target: &str,
) -> Result<SimulatorExportPayload> {
    let client = reqwest::Client::new();
    let base_url = simulator_url.trim_end_matches('/');
    reset_simulator(&client, base_url).await?;
    import_snapshot(&client, base_url, payload).await?;

    let deadline = Instant::now() + VERIFICATION_TIMEOUT;
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
        let latest = response
            .json::<SimulatorExportPayload>()
            .await
            .context("decode scheduler-simulator export response")?;

        let resolved = latest.pods.iter().any(|pod| {
            pod_scope(pod) == pod_scope_target
                && (pod_assigned_node(pod).is_some()
                    || pod
                        .metadata
                        .annotations
                        .as_ref()
                        .map(|a| a.contains_key(FILTER_RESULT_ANNOTATION))
                        .unwrap_or(false))
        });
        if resolved || Instant::now() >= deadline {
            return Ok(latest);
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

pub(crate) fn pod_scope(pod: &corev1::Pod) -> String {
    format!(
        "{}/{}",
        pod.metadata.namespace.clone().unwrap_or_default(),
        pod.metadata.name.clone().unwrap_or_default()
    )
}

pub(crate) fn pod_assigned_node(pod: &corev1::Pod) -> Option<String> {
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
    use std::collections::{BTreeMap, BTreeSet};

    #[test]
    fn export_payload_tolerates_null_pods() {
        // The live simulator (v0.4.0) returns `"pods": null` for an empty result; the decoder must
        // treat that as an empty list (regression for "invalid type: null, expected a sequence").
        let p: super::SimulatorExportPayload =
            serde_json::from_str(r#"{"pods":null}"#).expect("null pods decodes");
        assert!(p.pods.is_empty());
        let p2: super::SimulatorExportPayload =
            serde_json::from_str(r#"{}"#).expect("absent pods decodes");
        assert!(p2.pods.is_empty());
    }

    #[test]
    fn simulator_import_payload_includes_valid_default_scheduler_config() {
        let value = serde_json::to_value(super::SimulatorImportPayload::default())
            .expect("serialize default simulator import payload");
        let scheduler_config = value
            .get("schedulerConfig")
            .expect("imports should include a valid scheduler config");
        assert_eq!(
            scheduler_config
                .get("apiVersion")
                .and_then(serde_json::Value::as_str),
            Some("kubescheduler.config.k8s.io/v1")
        );
        assert_eq!(
            scheduler_config
                .get("kind")
                .and_then(serde_json::Value::as_str),
            Some("KubeSchedulerConfiguration")
        );
    }

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
            ..Default::default()
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

    #[test]
    fn simulator_batch_stability_treats_missing_preempted_targets_as_terminal() {
        let mut annotations = BTreeMap::new();
        annotations.insert(
            super::SELECTED_NODE_ANNOTATION.to_string(),
            "node-a".to_string(),
        );
        let exported = super::SimulatorExportPayload {
            pods: vec![corev1::Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some("survivor".to_string()),
                    namespace: Some("bench".to_string()),
                    annotations: Some(annotations),
                    ..Default::default()
                },
                ..Default::default()
            }],
        };
        let target_scopes = BTreeSet::from([
            "bench/survivor".to_string(),
            "bench/preempted-victim".to_string(),
        ]);
        let mut last_signature = BTreeMap::new();
        let mut stable_polls = 0_usize;

        assert!(!super::simulator_batch_is_stable(
            &exported,
            &target_scopes,
            &mut last_signature,
            &mut stable_polls,
            super::SIMULATOR_BATCH_STABLE_POLLS,
        ));
        for _ in 1..super::SIMULATOR_BATCH_STABLE_POLLS {
            assert!(!super::simulator_batch_is_stable(
                &exported,
                &target_scopes,
                &mut last_signature,
                &mut stable_polls,
                super::SIMULATOR_BATCH_STABLE_POLLS,
            ));
        }
        assert!(super::simulator_batch_is_stable(
            &exported,
            &target_scopes,
            &mut last_signature,
            &mut stable_polls,
            super::SIMULATOR_BATCH_STABLE_POLLS,
        ));
    }

    #[test]
    fn simulator_batch_stability_can_use_fewer_polls_for_benchmark_refresh() {
        let exported = super::SimulatorExportPayload {
            pods: vec![corev1::Pod {
                metadata: kube::api::ObjectMeta {
                    name: Some("survivor".to_string()),
                    namespace: Some("bench".to_string()),
                    annotations: Some(BTreeMap::from([(
                        super::SELECTED_NODE_ANNOTATION.to_string(),
                        "node-a".to_string(),
                    )])),
                    ..Default::default()
                },
                ..Default::default()
            }],
        };
        let target_scopes = BTreeSet::from([
            "bench/survivor".to_string(),
            "bench/preempted-victim".to_string(),
        ]);
        let mut last_signature = BTreeMap::new();
        let mut stable_polls = 0_usize;

        assert!(!super::simulator_batch_is_stable(
            &exported,
            &target_scopes,
            &mut last_signature,
            &mut stable_polls,
            1,
        ));
        assert!(super::simulator_batch_is_stable(
            &exported,
            &target_scopes,
            &mut last_signature,
            &mut stable_polls,
            1,
        ));
    }

    #[test]
    fn simulator_batch_state_requires_all_targets_for_full_resolution() {
        let mut selected = BTreeMap::new();
        selected.insert(
            super::SELECTED_NODE_ANNOTATION.to_string(),
            "node-a".to_string(),
        );
        let mut rejected = BTreeMap::new();
        rejected.insert(
            super::FILTER_RESULT_ANNOTATION.to_string(),
            r#"{"node-a":{"NodeResourcesFit":"Insufficient nvidia.com/gpu"}}"#.to_string(),
        );
        let exported = super::SimulatorExportPayload {
            pods: vec![
                corev1::Pod {
                    metadata: kube::api::ObjectMeta {
                        name: Some("placed".to_string()),
                        namespace: Some("bench".to_string()),
                        annotations: Some(selected),
                        ..Default::default()
                    },
                    ..Default::default()
                },
                corev1::Pod {
                    metadata: kube::api::ObjectMeta {
                        name: Some("rejected".to_string()),
                        namespace: Some("bench".to_string()),
                        annotations: Some(rejected),
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
        };

        let complete_targets =
            BTreeSet::from(["bench/placed".to_string(), "bench/rejected".to_string()]);
        let complete = super::simulator_batch_state(&exported, &complete_targets);
        assert_eq!(complete.target_count, 2);
        assert_eq!(complete.present_targets, 2);
        assert_eq!(complete.terminal_present_targets, 2);
        assert!(complete.all_targets_resolved());
        assert!(complete.visible_targets_terminal());

        let partial_targets = BTreeSet::from([
            "bench/placed".to_string(),
            "bench/rejected".to_string(),
            "bench/not-yet-exported".to_string(),
        ]);
        let partial = super::simulator_batch_state(&exported, &partial_targets);
        assert_eq!(partial.target_count, 3);
        assert_eq!(partial.present_targets, 2);
        assert_eq!(partial.terminal_present_targets, 2);
        assert!(!partial.all_targets_resolved());
        assert!(partial.visible_targets_terminal());
    }

    #[test]
    fn simulator_batch_diagnostics_summary_exposes_target_state() {
        let diagnostics = super::SimulatorBatchDiagnostics {
            elapsed_millis: 2500,
            phase: "poll".to_string(),
            state: super::SimulatorBatchState {
                target_count: 6,
                present_targets: 2,
                terminal_present_targets: 2,
            },
            stable_polls: 3,
            timed_out: true,
            phase_timings: vec![super::SimulatorPhaseTiming {
                phase: "snapshot import".to_string(),
                duration_millis: 25,
                cumulative_millis: 100,
            }],
        };

        let summary = diagnostics.summary();

        assert!(summary.contains("phase=poll"));
        assert!(summary.contains("elapsed=2500ms"));
        assert!(summary.contains("targets=6"));
        assert!(summary.contains("present=2"));
        assert!(summary.contains("terminal_present=2"));
        assert!(summary.contains("missing=4"));
        assert!(summary.contains("stable_polls=3"));
        assert!(summary.contains("timed_out=true"));
    }
}
