use crate::model::{
    AffinityTerm, ClusterMetadata, ClusterSnapshot, DaemonSet, DisruptionBudget, Pod, ResourceList,
    ResourceUsage, StorageClass, Taint, Toleration, VerticalPodAutoscaler, VolumeAttachment,
    VpaContainerPolicy, VpaContainerRecommendation, VpaRecommenderConfig,
};
use anyhow::{Context, Error, Result};
use chrono::Utc;
use k8s_openapi::api::apps::v1 as appsv1;
use k8s_openapi::api::core::v1 as corev1;
use k8s_openapi::api::policy::v1 as policyv1;
use k8s_openapi::api::storage::v1 as storagev1;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use kube::api::{ListParams, ObjectList};
use kube::config::{KubeConfigOptions, Kubeconfig};
use kube::core::{ApiResource, DynamicObject, GroupVersionKind};
use kube::{Api, Client, Config};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::future::Future;
use std::time::Duration;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};

const LIST_TIMEOUT: Duration = Duration::from_secs(60);
const LIST_MAX_ATTEMPTS: usize = 3;
const KUBE_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
const KUBE_READ_TIMEOUT: Duration = Duration::from_secs(120);
const KUBE_WRITE_TIMEOUT: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct KubeCollector {
    cluster_name: String,
    client: Client,
}

impl KubeCollector {
    pub async fn new(cluster_name: String, kubeconfig: String) -> Result<Self> {
        info!(
            cluster = %cluster_name,
            kubeconfig = if kubeconfig.is_empty() {
                "<default>"
            } else {
                kubeconfig.as_str()
            },
            "collector initializing"
        );
        let client = build_client(&kubeconfig).await?;
        Ok(Self {
            cluster_name,
            client,
        })
    }

    pub async fn collect(&self) -> Result<ClusterSnapshot> {
        info!(cluster = %self.cluster_name, "snapshot collection starting");
        let nodes_api: Api<corev1::Node> = Api::all(self.client.clone());
        let pods_api: Api<corev1::Pod> = Api::all(self.client.clone());
        let pvcs_api: Api<corev1::PersistentVolumeClaim> = Api::all(self.client.clone());
        let pvs_api: Api<corev1::PersistentVolume> = Api::all(self.client.clone());
        let storage_classes_api: Api<storagev1::StorageClass> = Api::all(self.client.clone());
        let daemon_sets_api: Api<appsv1::DaemonSet> = Api::all(self.client.clone());
        let deployments_api: Api<appsv1::Deployment> = Api::all(self.client.clone());
        let pdbs_api: Api<policyv1::PodDisruptionBudget> = Api::all(self.client.clone());

        let list_params = ListParams::default();
        let list_nodes = async {
            let result = run_list_with_retry("nodes", || async {
                debug!("listing nodes");
                nodes_api.list(&list_params).await.context("list nodes")
            })
            .await?;
            debug!(count = result.items.len(), "listed nodes");
            Ok::<_, Error>(result)
        };

        let list_pods = async {
            let result = run_list_with_retry("pods", || async {
                debug!("listing pods");
                pods_api.list(&list_params).await.context("list pods")
            })
            .await?;
            debug!(count = result.items.len(), "listed pods");
            Ok::<_, Error>(result)
        };

        let list_pvcs = async {
            let result = run_list_with_retry("persistent volume claims", || async {
                debug!("listing persistent volume claims");
                pvcs_api
                    .list(&list_params)
                    .await
                    .context("list persistent volume claims")
            })
            .await?;
            debug!(
                count = result.items.len(),
                "listed persistent volume claims"
            );
            Ok::<_, Error>(result)
        };

        let list_pvs = async {
            let result = run_list_with_retry("persistent volumes", || async {
                debug!("listing persistent volumes");
                pvs_api
                    .list(&list_params)
                    .await
                    .context("list persistent volumes")
            })
            .await?;
            debug!(count = result.items.len(), "listed persistent volumes");
            Ok::<_, Error>(result)
        };

        let list_storage_classes = async {
            let result = run_list_with_retry("storage classes", || async {
                debug!("listing storage classes");
                storage_classes_api
                    .list(&list_params)
                    .await
                    .context("list storage classes")
            })
            .await?;
            debug!(count = result.items.len(), "listed storage classes");
            Ok::<_, Error>(result)
        };

        let list_daemon_sets = async {
            let result = run_list_with_retry("daemonsets", || async {
                debug!("listing daemonsets");
                daemon_sets_api
                    .list(&list_params)
                    .await
                    .context("list daemonsets")
            })
            .await?;
            debug!(count = result.items.len(), "listed daemonsets");
            Ok::<_, Error>(result)
        };

        let list_pdbs = async {
            let result = run_list_with_retry("pod disruption budgets", || async {
                debug!("listing pod disruption budgets");
                pdbs_api
                    .list(&list_params)
                    .await
                    .context("list pod disruption budgets")
            })
            .await?;
            debug!(count = result.items.len(), "listed pod disruption budgets");
            Ok::<_, Error>(result)
        };

        let list_deployments = async {
            let result = run_list_with_retry("deployments", || async {
                debug!("listing deployments");
                deployments_api
                    .list(&list_params)
                    .await
                    .context("list deployments")
            })
            .await?;
            debug!(count = result.items.len(), "listed deployments");
            Ok::<_, Error>(result)
        };

        let list_vpas = async {
            let result = list_vertical_pod_autoscalers(&self.client, &list_params)
                .await
                .unwrap_or_else(|err| {
                    warn!(
                        error = %err,
                        "failed to collect vertical pod autoscalers; continuing without VPA data"
                    );
                    serde_json::from_value::<ObjectList<DynamicObject>>(json!({"items": []}))
                        .unwrap_or_else(|_| panic!("empty object list must deserialize"))
                });
            debug!(
                count = result.items.len(),
                "listed vertical pod autoscalers"
            );
            Ok::<_, Error>(result)
        };

        let (nodes, pods, pvcs, pvs, storage_classes, daemon_sets, deployments, pdbs, vpas) =
            tokio::try_join!(
                list_nodes,
                list_pods,
                list_pvcs,
                list_pvs,
                list_storage_classes,
                list_daemon_sets,
                list_deployments,
                list_pdbs,
                list_vpas,
            )
            .map_err(|err| {
                warn!(
                    error = %err,
                    chain = %format_error_chain(&err),
                    "snapshot collection failed"
                );
                err
            })
            .context("list kubernetes snapshot resources")?;

        let daemonset_count = daemon_sets.items.len();
        let pdb_count = pdbs.items.len();
        let (node_usage_result, pod_usage_result) = tokio::join!(
            fetch_node_metrics(&self.client),
            fetch_pod_metrics(&self.client),
        );
        let node_usage = match node_usage_result {
            Ok(metrics) => metrics,
            Err(err) => {
                warn!(error = %err, "failed to collect node metrics; continuing without live node usage");
                BTreeMap::new()
            }
        };
        let pod_usage = match pod_usage_result {
            Ok(metrics) => metrics,
            Err(err) => {
                warn!(error = %err, "failed to collect pod metrics; continuing without live pod usage");
                BTreeMap::new()
            }
        };

        let pv_by_name: BTreeMap<String, corev1::PersistentVolume> = pvs
            .items
            .into_iter()
            .filter_map(|pv| pv.metadata.name.clone().map(|name| (name, pv)))
            .collect();

        let snapshot = ClusterSnapshot {
            metadata: ClusterMetadata {
                name: self.cluster_name.clone(),
                collected: Some(Utc::now()),
                schema_version: crate::state_cache::snapshot_schema_version(),
            },
            nodes: nodes
                .items
                .into_iter()
                .map(|node| to_model_node(node, &node_usage))
                .collect(),
            pods: pods
                .items
                .into_iter()
                .map(|pod| to_model_pod(pod, &pod_usage))
                .collect(),
            volumes: pvcs
                .items
                .into_iter()
                .map(|pvc| {
                    let pv = pvc
                        .spec
                        .as_ref()
                        .and_then(|spec| spec.volume_name.as_ref())
                        .and_then(|name| pv_by_name.get(name))
                        .cloned();
                    to_volume_attachment(pvc, pv)
                })
                .collect(),
            storage_classes: storage_classes
                .items
                .into_iter()
                .map(to_storage_class)
                .collect(),
            daemon_sets: daemon_sets.items.into_iter().map(to_daemon_set).collect(),
            vpa_recommender: extract_vpa_recommender_config(&deployments.items),
            pdbs: pdbs.items.into_iter().map(to_disruption_budget).collect(),
            vpas: vpas
                .items
                .into_iter()
                .map(to_vertical_pod_autoscaler)
                .collect(),
            warnings: daemonset_warnings(daemonset_count, pdb_count),
        };
        info!(
            cluster = %self.cluster_name,
            nodes = snapshot.nodes.len(),
            pods = snapshot.pods.len(),
            volumes = snapshot.volumes.len(),
            storage_classes = snapshot.storage_classes.len(),
            daemonsets = snapshot.daemon_sets.len(),
            vpa_recommender_found = snapshot.vpa_recommender.found,
            pdbs = snapshot.pdbs.len(),
            vpas = snapshot.vpas.len(),
            warnings = snapshot.warnings.len(),
            "snapshot collection complete"
        );
        Ok(snapshot)
    }
}

fn format_error_chain(err: &Error) -> String {
    err.chain()
        .map(|cause| cause.to_string())
        .collect::<Vec<_>>()
        .join(" | ")
}

async fn run_list_with_retry<T, F, Fut>(resource: &'static str, op: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error = None;

    for attempt in 1..=LIST_MAX_ATTEMPTS {
        match timeout(LIST_TIMEOUT, op()).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(err)) => {
                let transient = is_transient_kube_error(&err);
                if !transient || attempt == LIST_MAX_ATTEMPTS {
                    return Err(err);
                }
                warn!(
                    resource,
                    attempt,
                    max_attempts = LIST_MAX_ATTEMPTS,
                    chain = %format_error_chain(&err),
                    "transient kubernetes list failure, retrying"
                );
                last_error = Some(err);
            }
            Err(_) => {
                let err = anyhow::anyhow!(
                    "list {resource} timed out after {}s",
                    LIST_TIMEOUT.as_secs()
                );
                if attempt == LIST_MAX_ATTEMPTS {
                    return Err(err);
                }
                warn!(
                    resource,
                    attempt,
                    max_attempts = LIST_MAX_ATTEMPTS,
                    timeout_secs = LIST_TIMEOUT.as_secs(),
                    "kubernetes list timed out, retrying"
                );
                last_error = Some(err);
            }
        }

        let backoff_secs = 1_u64 << (attempt - 1);
        sleep(Duration::from_secs(backoff_secs)).await;
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("list {resource} failed")))
}

fn is_transient_kube_error(err: &Error) -> bool {
    let chain = format_error_chain(err).to_ascii_lowercase();
    chain.contains("client error (connect)")
        || chain.contains("deadline has elapsed")
        || chain.contains("timed out")
        || chain.contains("timeout")
        || chain.contains("connection reset")
        || chain.contains("temporarily unavailable")
        || chain.contains("dns error")
}

pub(crate) async fn build_client(kubeconfig: &str) -> Result<Client> {
    if kubeconfig.is_empty() {
        debug!(source = "default", "building kube client");
        let mut config = Config::infer()
            .await
            .context("create default kube config")?;
        apply_timeouts(&mut config);
        return Client::try_from(config).context("create default kube client");
    }

    debug!(
        source = "explicit",
        path = kubeconfig,
        "building kube client"
    );
    let kubeconfig_doc = Kubeconfig::read_from(kubeconfig)
        .with_context(|| format!("read kubeconfig from {kubeconfig}"))?;
    let mut config = Config::from_custom_kubeconfig(kubeconfig_doc, &KubeConfigOptions::default())
        .await
        .context("build kube config from explicit kubeconfig")?;
    apply_timeouts(&mut config);
    Client::try_from(config).context("build kube client from explicit kubeconfig")
}

fn apply_timeouts(config: &mut Config) {
    config.connect_timeout = Some(KUBE_CONNECT_TIMEOUT);
    config.read_timeout = Some(KUBE_READ_TIMEOUT);
    config.write_timeout = Some(KUBE_WRITE_TIMEOUT);
}

fn to_model_node(
    node: corev1::Node,
    usage_by_node: &BTreeMap<String, ResourceUsage>,
) -> crate::model::Node {
    let labels: BTreeMap<String, String> = node
        .metadata
        .labels
        .unwrap_or_default()
        .into_iter()
        .collect();
    let taints = node
        .spec
        .as_ref()
        .and_then(|spec| spec.taints.clone())
        .unwrap_or_default()
        .into_iter()
        .map(|taint| Taint {
            key: taint.key,
            value: taint.value.unwrap_or_default(),
            effect: taint.effect,
        })
        .collect();
    let allocatable = node
        .status
        .as_ref()
        .and_then(|status| status.allocatable.as_ref())
        .map(to_resource_list)
        .unwrap_or_default();
    let extended_resources = node
        .status
        .as_ref()
        .and_then(|status| status.allocatable.as_ref())
        .map(extract_extended_resources)
        .unwrap_or_default();

    let name = node.metadata.name.unwrap_or_default();

    crate::model::Node {
        name: name.clone(),
        pool: labels
            .get("node.kubernetes.io/instance-type")
            .cloned()
            .unwrap_or_default(),
        labels,
        taints,
        allocatable,
        extended_resources,
        usage: usage_by_node.get(&name).cloned().unwrap_or_default(),
        price: Default::default(),
    }
}

fn to_model_pod(pod: corev1::Pod, usage_by_pod: &BTreeMap<String, ResourceUsage>) -> Pod {
    let spec = pod.spec.clone();
    let status = pod.status.clone();
    let namespace = pod.metadata.namespace.clone().unwrap_or_default();
    let name = pod.metadata.name.clone().unwrap_or_default();
    let annotations = pod.metadata.annotations.clone().unwrap_or_default();
    let requests = spec.as_ref().map(sum_pod_requests).unwrap_or_default();
    let pvcs = spec
        .as_ref()
        .and_then(|spec| spec.volumes.as_ref())
        .map(|volumes| {
            volumes
                .iter()
                .filter_map(|vol| {
                    vol.persistent_volume_claim
                        .as_ref()
                        .map(|pvc| pvc.claim_name.clone())
                })
                .collect()
        })
        .unwrap_or_default();
    let owner_refs = pod.metadata.owner_references.clone().unwrap_or_default();
    let labels: BTreeMap<String, String> = pod
        .metadata
        .labels
        .unwrap_or_default()
        .into_iter()
        .collect();
    let node_selector = spec
        .as_ref()
        .and_then(|spec| spec.node_selector.clone())
        .unwrap_or_default()
        .into_iter()
        .collect();
    let tolerations = spec
        .as_ref()
        .and_then(|spec| spec.tolerations.as_ref())
        .map(|vals| to_model_tolerations(vals))
        .unwrap_or_default();
    let affinity = spec.as_ref().and_then(|s| s.affinity.as_ref());
    let qos_class = status
        .as_ref()
        .and_then(|s| s.qos_class.clone())
        .unwrap_or_default();

    Pod {
        namespace: namespace.clone(),
        name: name.clone(),
        node_name: spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default(),
        phase: status.and_then(|s| s.phase).unwrap_or_default(),
        owner_kind: owner_kind(&owner_refs),
        owner_name: owner_name(&owner_refs),
        deleting: pod.metadata.deletion_timestamp.is_some(),
        labels,
        qos_class,
        requests,
        extended_resource_requests: spec
            .as_ref()
            .map(sum_pod_extended_requests)
            .unwrap_or_default(),
        usage: usage_by_pod
            .get(&namespaced_name(&namespace, &name))
            .cloned()
            .unwrap_or_default(),
        memory_history: Default::default(),
        tolerations,
        node_selector,
        required_affinity: to_required_affinity(affinity),
        required_anti: to_required_anti_affinity(affinity),
        required_node_affinity: to_required_node_affinity(affinity),
        topology_spread_constraints: spec
            .as_ref()
            .and_then(|s| s.topology_spread_constraints.as_ref())
            .map(|constraints| constraints.len() as i32)
            .unwrap_or(0),
        topology_spread_rules: spec
            .as_ref()
            .and_then(|s| s.topology_spread_constraints.as_ref())
            .map(|constraints| {
                constraints
                    .iter()
                    .map(|c| crate::model::TopologySpreadRule {
                        max_skew: c.max_skew,
                        topology_key: c.topology_key.clone(),
                        when_unsatisfiable: c.when_unsatisfiable.clone(),
                        selector: c
                            .label_selector
                            .as_ref()
                            .and_then(|s| s.match_labels.clone())
                            .unwrap_or_default()
                            .into_iter()
                            .collect(),
                    })
                    .collect()
            })
            .unwrap_or_default(),
        pvcs,
        disruption_cost: 0,
        autoscaler_not_safe_to_evict: annotations
            .get("cluster-autoscaler.kubernetes.io/safe-to-evict")
            .map(|v| v.eq_ignore_ascii_case("false"))
            .unwrap_or(false),
    }
}

fn owner_kind(refs: &[OwnerReference]) -> String {
    refs.iter()
        .find(|r| r.controller.unwrap_or(false))
        .map(|r| r.kind.clone())
        .or_else(|| refs.first().map(|r| r.kind.clone()))
        .unwrap_or_default()
}

fn owner_name(refs: &[OwnerReference]) -> String {
    refs.iter()
        .find(|r| r.controller.unwrap_or(false))
        .map(|r| r.name.clone())
        .or_else(|| refs.first().map(|r| r.name.clone()))
        .unwrap_or_default()
}

// @lineage
// reads: namespace, name
fn namespaced_name(namespace: &str, name: &str) -> String {
    format!("{namespace}/{name}")
}

fn to_volume_attachment(
    pvc: corev1::PersistentVolumeClaim,
    pv: Option<corev1::PersistentVolume>,
) -> VolumeAttachment {
    let pvc_spec = pvc.spec.as_ref();
    let pvc_sc = pvc_spec.and_then(|spec| spec.storage_class_name.clone());
    let pv_sc = pv.as_ref().and_then(|pv| {
        pv.spec
            .as_ref()
            .and_then(|spec| spec.storage_class_name.clone())
    });

    VolumeAttachment {
        claim_name: pvc.metadata.name.unwrap_or_default(),
        namespace: pvc.metadata.namespace.unwrap_or_default(),
        bound_node_zones: pv.as_ref().map(extract_node_zones).unwrap_or_default(),
        storage_class: pvc_sc.or(pv_sc).unwrap_or_default(),
    }
}

fn to_storage_class(sc: storagev1::StorageClass) -> StorageClass {
    StorageClass {
        name: sc.metadata.name.unwrap_or_default(),
        provisioner: sc.provisioner,
        volume_binding_mode: sc
            .volume_binding_mode
            .map(|m| m.to_string())
            .unwrap_or_default(),
    }
}

fn to_daemon_set(ds: appsv1::DaemonSet) -> DaemonSet {
    let template_spec = ds.spec.as_ref().and_then(|s| s.template.spec.as_ref());
    let requests = template_spec.map(sum_pod_spec_requests).unwrap_or_default();
    DaemonSet {
        namespace: ds.metadata.namespace.unwrap_or_default(),
        name: ds.metadata.name.unwrap_or_default(),
        node_selector: template_spec
            .and_then(|spec| spec.node_selector.clone())
            .unwrap_or_default()
            .into_iter()
            .collect(),
        tolerations: template_spec
            .and_then(|spec| spec.tolerations.as_ref())
            .map(|vals| to_model_tolerations(vals))
            .unwrap_or_default(),
        requests,
    }
}

fn to_disruption_budget(pdb: policyv1::PodDisruptionBudget) -> DisruptionBudget {
    let spec = pdb.spec.as_ref();
    DisruptionBudget {
        namespace: pdb.metadata.namespace.unwrap_or_default(),
        name: pdb.metadata.name.unwrap_or_default(),
        min_available: spec
            .and_then(|s| s.min_available.as_ref())
            .map(int_or_string_to_string)
            .unwrap_or_default(),
        max_unavailable: spec
            .and_then(|s| s.max_unavailable.as_ref())
            .map(int_or_string_to_string)
            .unwrap_or_default(),
    }
}

// @lineage
// reads: deployments[].spec.template.spec.containers[].args
fn extract_vpa_recommender_config(deployments: &[appsv1::Deployment]) -> VpaRecommenderConfig {
    for deployment in deployments {
        let name = deployment.metadata.name.clone().unwrap_or_default();
        let namespace = deployment.metadata.namespace.clone().unwrap_or_default();
        let Some(spec) = deployment.spec.as_ref() else {
            continue;
        };
        let Some(pod_spec) = spec.template.spec.as_ref() else {
            continue;
        };
        for container in &pod_spec.containers {
            let looks_like_recommender = container.name.contains("recommender")
                || name.contains("recommender")
                || name.contains("vpa");
            if !looks_like_recommender {
                continue;
            }
            let args = container.args.clone().unwrap_or_default();
            let safety_margin = args
                .iter()
                .find_map(|arg| parse_vpa_safety_margin_arg(arg))
                .unwrap_or(0.15);
            return VpaRecommenderConfig {
                found: true,
                source: format!("{namespace}/{name}:{}", container.name),
                safety_margin_fraction: safety_margin,
                args,
            };
        }
    }

    VpaRecommenderConfig {
        found: false,
        source: "default".to_string(),
        safety_margin_fraction: 0.15,
        args: Vec::new(),
    }
}

// @lineage
// reads: recommender arg strings
fn parse_vpa_safety_margin_arg(arg: &str) -> Option<f64> {
    let prefixes = [
        "--safetyMarginFraction=",
        "--recommendation-margin-fraction=",
    ];
    prefixes.iter().find_map(|prefix| {
        arg.strip_prefix(prefix)
            .and_then(|value| value.parse::<f64>().ok())
    })
}

// @lineage
// reads: verticalpodautoscaler.spec.*, verticalpodautoscaler.status.recommendation.*
fn to_vertical_pod_autoscaler(vpa: DynamicObject) -> VerticalPodAutoscaler {
    let value = serde_json::to_value(vpa).unwrap_or_default();
    let metadata = value.get("metadata").cloned().unwrap_or_default();
    let spec = value.get("spec").cloned().unwrap_or_default();
    let status = value.get("status").cloned().unwrap_or_default();
    let target_ref = spec.get("targetRef").cloned().unwrap_or_default();
    let update_policy = spec.get("updatePolicy").cloned().unwrap_or_default();
    let resource_policy = spec.get("resourcePolicy").cloned().unwrap_or_default();
    let recommendation = status.get("recommendation").cloned().unwrap_or_default();

    VerticalPodAutoscaler {
        namespace: metadata
            .get("namespace")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        name: metadata
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        target_ref_kind: target_ref
            .get("kind")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        target_ref_name: target_ref
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        update_mode: update_policy
            .get("updateMode")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        container_policies: resource_policy
            .get("containerPolicies")
            .and_then(Value::as_array)
            .map(|policies| policies.iter().map(parse_vpa_container_policy).collect())
            .unwrap_or_default(),
        container_recommendations: recommendation
            .get("containerRecommendations")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .map(parse_vpa_container_recommendation)
                    .collect()
            })
            .unwrap_or_default(),
    }
}

// @lineage
// reads: vpa.spec.resourcePolicy.containerPolicies[]
fn parse_vpa_container_policy(value: &Value) -> VpaContainerPolicy {
    VpaContainerPolicy {
        container_name: value
            .get("containerName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        controlled_resources: value
            .get("controlledResources")
            .and_then(Value::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToString::to_string)
                    .collect()
            })
            .unwrap_or_default(),
        min_allowed: parse_resource_list_value(value.get("minAllowed")),
        max_allowed: parse_resource_list_value(value.get("maxAllowed")),
    }
}

// @lineage
// reads: vpa.status.recommendation.containerRecommendations[]
fn parse_vpa_container_recommendation(value: &Value) -> VpaContainerRecommendation {
    VpaContainerRecommendation {
        container_name: value
            .get("containerName")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string(),
        lower_bound: parse_resource_list_value(value.get("lowerBound")),
        target: parse_resource_list_value(value.get("target")),
        upper_bound: parse_resource_list_value(value.get("upperBound")),
        uncapped_target: parse_resource_list_value(value.get("uncappedTarget")),
    }
}

// @lineage
// reads: resource list json values cpu,memory,ephemeral-storage,pods
fn parse_resource_list_value(value: Option<&Value>) -> ResourceList {
    let Some(Value::Object(map)) = value else {
        return ResourceList::default();
    };
    ResourceList {
        milli_cpu: map
            .get("cpu")
            .and_then(Value::as_str)
            .map(|v| parse_cpu_millis(&Quantity(v.to_string())))
            .unwrap_or(0),
        memory_bytes: map
            .get("memory")
            .and_then(Value::as_str)
            .map(|v| parse_bytes(&Quantity(v.to_string())))
            .unwrap_or(0),
        ephemeral_storage: map
            .get("ephemeral-storage")
            .and_then(Value::as_str)
            .map(|v| parse_bytes(&Quantity(v.to_string())))
            .unwrap_or(0),
        pods: map
            .get("pods")
            .and_then(Value::as_str)
            .map(|v| parse_integer_quantity(&Quantity(v.to_string())))
            .unwrap_or(0),
    }
}

// @lineage
// reads: metrics.k8s.io/v1beta1/nodes
fn node_metrics_api(client: Client) -> Api<DynamicObject> {
    let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "metrics.k8s.io",
        "v1beta1",
        "NodeMetrics",
    ));
    Api::all_with(client, &ar)
}

// @lineage
// reads: metrics.k8s.io/v1beta1/pods
fn pod_metrics_api(client: Client) -> Api<DynamicObject> {
    let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "metrics.k8s.io",
        "v1beta1",
        "PodMetrics",
    ));
    Api::all_with(client, &ar)
}

// @lineage
// reads: metrics.k8s.io/v1beta1/nodes
async fn fetch_node_metrics(client: &Client) -> Result<BTreeMap<String, ResourceUsage>> {
    let list = run_list_with_retry("node metrics", || async {
        node_metrics_api(client.clone())
            .list(&ListParams::default())
            .await
            .context("list node metrics")
    })
    .await?;
    let mut usage = BTreeMap::new();
    for item in list.items {
        let value = serde_json::to_value(&item).context("serialize node metric")?;
        let name = value
            .get("metadata")
            .and_then(|v| v.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if name.is_empty() {
            continue;
        }
        usage.insert(name.to_string(), parse_usage_value(value.get("usage")));
    }
    Ok(usage)
}

// @lineage
// reads: metrics.k8s.io/v1beta1/pods
async fn fetch_pod_metrics(client: &Client) -> Result<BTreeMap<String, ResourceUsage>> {
    let list = run_list_with_retry("pod metrics", || async {
        pod_metrics_api(client.clone())
            .list(&ListParams::default())
            .await
            .context("list pod metrics")
    })
    .await?;
    let mut usage = BTreeMap::new();
    for item in list.items {
        let value = serde_json::to_value(&item).context("serialize pod metric")?;
        let namespace = value
            .get("metadata")
            .and_then(|v| v.get("namespace"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let name = value
            .get("metadata")
            .and_then(|v| v.get("name"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        if namespace.is_empty() || name.is_empty() {
            continue;
        }
        let mut total = ResourceUsage::default();
        if let Some(containers) = value.get("containers").and_then(Value::as_array) {
            for container in containers {
                total = add_usage(&total, &parse_usage_value(container.get("usage")));
            }
        }
        usage.insert(namespaced_name(namespace, name), total);
    }
    Ok(usage)
}

// @lineage
// reads: usage.cpu, usage.memory, usage.ephemeral-storage
fn parse_usage_value(value: Option<&Value>) -> ResourceUsage {
    let Some(value) = value else {
        return ResourceUsage::default();
    };
    ResourceUsage {
        cpu_usage_milli: value
            .get("cpu")
            .and_then(Value::as_str)
            .map(|v| parse_cpu_millis(&Quantity(v.to_string())))
            .unwrap_or(0),
        memory_bytes: value
            .get("memory")
            .and_then(Value::as_str)
            .map(|v| parse_bytes(&Quantity(v.to_string())))
            .unwrap_or(0),
        ephemeral_bytes: value
            .get("ephemeral-storage")
            .and_then(Value::as_str)
            .map(|v| parse_bytes(&Quantity(v.to_string())))
            .unwrap_or(0),
    }
}

// @lineage
// reads: left.*, right.*
fn add_usage(left: &ResourceUsage, right: &ResourceUsage) -> ResourceUsage {
    ResourceUsage {
        cpu_usage_milli: left.cpu_usage_milli + right.cpu_usage_milli,
        memory_bytes: left.memory_bytes + right.memory_bytes,
        ephemeral_bytes: left.ephemeral_bytes + right.ephemeral_bytes,
    }
}

fn to_resource_list(resources: &BTreeMap<String, Quantity>) -> ResourceList {
    ResourceList {
        milli_cpu: resources.get("cpu").map(parse_cpu_millis).unwrap_or(0),
        memory_bytes: resources.get("memory").map(parse_bytes).unwrap_or(0),
        ephemeral_storage: resources
            .get("ephemeral-storage")
            .map(parse_bytes)
            .unwrap_or(0),
        pods: resources
            .get("pods")
            .map(parse_integer_quantity)
            .unwrap_or(0),
    }
}

fn extract_extended_resources(resources: &BTreeMap<String, Quantity>) -> BTreeMap<String, i64> {
    resources
        .iter()
        .filter_map(|(name, quantity)| {
            if is_core_resource(name) {
                None
            } else {
                Some((name.clone(), parse_integer_quantity(quantity)))
            }
        })
        .collect()
}

fn sum_pod_requests(spec: &corev1::PodSpec) -> ResourceList {
    sum_pod_spec_requests(spec)
}

fn sum_pod_spec_requests(spec: &corev1::PodSpec) -> ResourceList {
    let mut regular = ResourceList::default();
    for container in &spec.containers {
        if let Some(resources) = container.resources.as_ref() {
            if let Some(reqs) = resources.requests.as_ref() {
                regular.milli_cpu += reqs.get("cpu").map(parse_cpu_millis).unwrap_or(0);
                regular.memory_bytes += reqs.get("memory").map(parse_bytes).unwrap_or(0);
                regular.ephemeral_storage +=
                    reqs.get("ephemeral-storage").map(parse_bytes).unwrap_or(0);
            }
        }
    }
    if let Some(init_containers) = &spec.init_containers {
        for container in init_containers {
            if let Some(resources) = container.resources.as_ref() {
                if let Some(reqs) = resources.requests.as_ref() {
                    let cpu = reqs.get("cpu").map(parse_cpu_millis).unwrap_or(0);
                    let mem = reqs.get("memory").map(parse_bytes).unwrap_or(0);
                    let eph = reqs.get("ephemeral-storage").map(parse_bytes).unwrap_or(0);
                    regular.milli_cpu = regular.milli_cpu.max(cpu);
                    regular.memory_bytes = regular.memory_bytes.max(mem);
                    regular.ephemeral_storage = regular.ephemeral_storage.max(eph);
                }
            }
        }
    }
    regular.pods += 1;
    regular
}

fn sum_pod_extended_requests(spec: &corev1::PodSpec) -> BTreeMap<String, i64> {
    let mut out = BTreeMap::new();
    for container in &spec.containers {
        if let Some(resources) = container.resources.as_ref() {
            if let Some(reqs) = resources.requests.as_ref() {
                for (name, quantity) in reqs {
                    if is_core_resource(name) {
                        continue;
                    }
                    *out.entry(name.clone()).or_insert(0) += parse_integer_quantity(quantity);
                }
            }
        }
    }
    out
}

fn to_model_tolerations(tolerations: &[corev1::Toleration]) -> Vec<Toleration> {
    tolerations
        .iter()
        .map(|tol| Toleration {
            key: tol.key.clone().unwrap_or_default(),
            operator: tol.operator.clone().unwrap_or_default(),
            value: tol.value.clone().unwrap_or_default(),
            effect: tol.effect.clone().unwrap_or_default(),
        })
        .collect()
}

fn to_required_affinity(affinity: Option<&corev1::Affinity>) -> Vec<AffinityTerm> {
    affinity
        .and_then(|a| a.pod_affinity.as_ref())
        .and_then(|pa| {
            pa.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .map(|terms| {
            terms
                .iter()
                .map(|term| AffinityTerm {
                    topology_key: term.topology_key.clone(),
                    selector: selector_to_map(term.label_selector.as_ref()),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn to_required_anti_affinity(affinity: Option<&corev1::Affinity>) -> Vec<AffinityTerm> {
    affinity
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|pa| {
            pa.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
        .map(|terms| {
            terms
                .iter()
                .map(|term| AffinityTerm {
                    topology_key: term.topology_key.clone(),
                    selector: selector_to_map(term.label_selector.as_ref()),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn selector_to_map(selector: Option<&LabelSelector>) -> BTreeMap<String, String> {
    selector
        .and_then(|s| s.match_labels.clone())
        .unwrap_or_default()
        .into_iter()
        .collect()
}

fn to_required_node_affinity(
    affinity: Option<&corev1::Affinity>,
) -> Vec<crate::model::NodeAffinityTerm> {
    let node_affinity = match affinity.and_then(|a| a.node_affinity.as_ref()) {
        Some(na) => na,
        None => return Vec::new(),
    };
    let required = match node_affinity
        .required_during_scheduling_ignored_during_execution
        .as_ref()
    {
        Some(r) => r,
        None => return Vec::new(),
    };
    let mut terms = Vec::new();
    for selector_term in &required.node_selector_terms {
        if let Some(exprs) = &selector_term.match_expressions {
            for expr in exprs {
                terms.push(crate::model::NodeAffinityTerm {
                    key: expr.key.clone(),
                    operator: expr.operator.clone(),
                    values: expr.values.clone().unwrap_or_default(),
                });
            }
        }
    }
    terms
}

fn extract_node_zones(pv: &corev1::PersistentVolume) -> Vec<String> {
    let mut zones = Vec::new();
    let Some(spec) = pv.spec.as_ref() else {
        return zones;
    };
    let Some(node_affinity) = spec.node_affinity.as_ref() else {
        return zones;
    };
    let Some(required) = node_affinity.required.as_ref() else {
        return zones;
    };

    for term in &required.node_selector_terms {
        if let Some(exprs) = term.match_expressions.as_ref() {
            for expr in exprs {
                if expr.key == "topology.kubernetes.io/zone"
                    || expr.key == "failure-domain.beta.kubernetes.io/zone"
                {
                    zones.extend(expr.values.clone().unwrap_or_default());
                }
            }
        }
    }
    zones.sort();
    zones.dedup();
    zones
}

fn daemonset_warnings(daemonset_count: usize, pdb_count: usize) -> Vec<String> {
    let mut warnings = Vec::new();
    if daemonset_count == 0 {
        warnings.push("no daemonsets found; system overhead may be understated".to_string());
    }
    if pdb_count == 0 {
        warnings.push(
            "no pod disruption budgets found; disruption analysis will be incomplete".to_string(),
        );
    }
    warnings
}

fn int_or_string_to_string(value: &IntOrString) -> String {
    match value {
        IntOrString::Int(v) => v.to_string(),
        IntOrString::String(v) => v.clone(),
    }
}

fn parse_integer_quantity(q: &Quantity) -> i64 {
    q.0.parse::<i64>().unwrap_or(0)
}

fn is_core_resource(name: &str) -> bool {
    matches!(name, "cpu" | "memory" | "ephemeral-storage" | "pods")
        || name.starts_with("hugepages-")
}

fn parse_cpu_millis(q: &Quantity) -> i64 {
    let s = q.0.trim();
    if let Some(value) = s.strip_suffix('m') {
        return value.parse::<f64>().map(|v| v.round() as i64).unwrap_or(0);
    }
    s.parse::<f64>()
        .map(|v| (v * 1000.0).round() as i64)
        .unwrap_or(0)
}

fn parse_bytes(q: &Quantity) -> i64 {
    let s = q.0.trim();
    let (num, suffix) = split_quantity(s);
    let value = num.parse::<f64>().unwrap_or(0.0);
    let factor = match suffix {
        "n" => 1e-9_f64,
        "u" => 1e-6_f64,
        "m" => 1e-3_f64,
        "k" => 1000_f64,
        "Ki" => 1024_f64,
        "Mi" => 1024_f64.powi(2),
        "Gi" => 1024_f64.powi(3),
        "Ti" => 1024_f64.powi(4),
        "Pi" => 1024_f64.powi(5),
        "Ei" => 1024_f64.powi(6),
        "K" => 1000_f64,
        "M" => 1000_f64.powi(2),
        "G" => 1000_f64.powi(3),
        "T" => 1000_f64.powi(4),
        "P" => 1000_f64.powi(5),
        "E" => 1000_f64.powi(6),
        _ => 1.0,
    };
    (value * factor).round() as i64
}

fn split_quantity(value: &str) -> (&str, &str) {
    let idx = value
        .find(|c: char| !(c.is_ascii_digit() || c == '.' || c == '-'))
        .unwrap_or(value.len());
    value.split_at(idx)
}

#[cfg(test)]
mod tests {
    use super::{extract_extended_resources, parse_bytes, parse_vpa_safety_margin_arg};
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use std::collections::BTreeMap;

    #[test]
    fn parses_decimal_kilobyte_suffix() {
        assert_eq!(
            parse_bytes(&Quantity("125251042k".to_string())),
            125_251_042_000
        );
    }

    #[test]
    fn parses_binary_kibibyte_suffix() {
        assert_eq!(
            parse_bytes(&Quantity("130321464Ki".to_string())),
            133_449_179_136
        );
    }

    #[test]
    fn extracts_extended_resources_from_allocatable_map() {
        let mut resources = BTreeMap::new();
        resources.insert("cpu".to_string(), Quantity("4".to_string()));
        resources.insert("memory".to_string(), Quantity("16Gi".to_string()));
        resources.insert("nvidia.com/gpu".to_string(), Quantity("2".to_string()));

        let extended = extract_extended_resources(&resources);

        assert_eq!(extended.get("nvidia.com/gpu"), Some(&2));
        assert!(!extended.contains_key("cpu"));
        assert!(!extended.contains_key("memory"));
    }

    #[test]
    fn parses_vpa_safety_margin_flags() {
        assert_eq!(
            parse_vpa_safety_margin_arg("--safetyMarginFraction=0.15"),
            Some(0.15)
        );
        assert_eq!(
            parse_vpa_safety_margin_arg("--recommendation-margin-fraction=0.2"),
            Some(0.2)
        );
        assert_eq!(parse_vpa_safety_margin_arg("--not-this=1"), None);
    }
}
// @lineage
// reads: autoscaling.k8s.io/v1{,v1beta2}/verticalpodautoscalers
fn vpa_api(client: Client, version: &str) -> Api<DynamicObject> {
    let ar = ApiResource::from_gvk(&GroupVersionKind::gvk(
        "autoscaling.k8s.io",
        version,
        "VerticalPodAutoscaler",
    ));
    Api::all_with(client, &ar)
}

// @lineage
// reads: autoscaling.k8s.io/v1{,v1beta2}/verticalpodautoscalers
async fn list_vertical_pod_autoscalers(
    client: &Client,
    list_params: &ListParams,
) -> Result<ObjectList<DynamicObject>> {
    let versions = ["v1", "v1beta2"];
    let mut last_error = None;
    for version in versions {
        let result = run_list_with_retry("vertical pod autoscalers", || async {
            debug!(version, "listing vertical pod autoscalers");
            vpa_api(client.clone(), version)
                .list(list_params)
                .await
                .with_context(|| format!("list vertical pod autoscalers ({version})"))
        })
        .await;
        match result {
            Ok(items) => return Ok(items),
            Err(err) => {
                last_error = Some(err);
            }
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("list vertical pod autoscalers failed")))
}
