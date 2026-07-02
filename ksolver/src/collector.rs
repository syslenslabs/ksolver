use crate::model::{
    AffinityTerm, ClusterMetadata, ClusterSnapshot, DaemonSet, DisruptionBudget, Pod, ResourceList,
    ResourceUsage, StorageClass, Taint, Toleration, VerticalPodAutoscaler, VolumeAttachment,
    VpaContainerPolicy, VpaContainerRecommendation, VpaRecommenderConfig,
};
use anyhow::{Context, Error, Result};
use chrono::{DateTime, Utc};
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
use sha2::{Digest, Sha256};
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

fn annotation_bool(annotations: &BTreeMap<String, String>, key: &str, default: bool) -> bool {
    annotations
        .get(key)
        .map(|v| match v.trim().to_ascii_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => true,
            "false" | "0" | "no" | "off" => false,
            _ => default,
        })
        .unwrap_or(default)
}

fn annotation_i32(annotations: &BTreeMap<String, String>, key: &str, default: i32) -> i32 {
    annotations
        .get(key)
        .and_then(|v| v.trim().parse::<i32>().ok())
        .unwrap_or(default)
}

fn annotation_i64(annotations: &BTreeMap<String, String>, key: &str, default: i64) -> i64 {
    annotations
        .get(key)
        .and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(default)
}

fn annotation_f64(annotations: &BTreeMap<String, String>, key: &str) -> f64 {
    annotations
        .get(key)
        .and_then(|v| v.trim().parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v > 0.0)
        .unwrap_or(0.0)
}

fn predicted_peak_vram_bytes(annotations: &BTreeMap<String, String>) -> i64 {
    let explicit_bytes = annotation_i64(annotations, "ksolver.dev/predicted-peak-vram-bytes", 0);
    if explicit_bytes > 0 {
        return explicit_bytes;
    }
    let explicit_gib = annotation_f64(annotations, "ksolver.dev/predicted-peak-vram-gib");
    if explicit_gib > 0.0 {
        return (explicit_gib * 1024.0 * 1024.0 * 1024.0).round() as i64;
    }
    0
}

fn annotation_deadline_unix_seconds(annotations: &BTreeMap<String, String>) -> i64 {
    annotations
        .get("ksolver.dev/deadline")
        .and_then(|v| DateTime::parse_from_rfc3339(v.trim()).ok())
        .map(|dt| dt.with_timezone(&Utc).timestamp())
        .unwrap_or(0)
        .max(0)
}

fn normalize_priority(raw: i64) -> i64 {
    if raw <= 0 {
        return 0;
    }
    ((raw + 999) / 1000).clamp(1, 1000)
}

fn pod_priority(annotations: &BTreeMap<String, String>, spec: Option<&corev1::PodSpec>) -> i64 {
    let annotated = annotations
        .get("ksolver.dev/priority")
        .and_then(|v| v.trim().parse::<i64>().ok());
    normalize_priority(
        annotated
            .or_else(|| spec.and_then(|s| s.priority.map(i64::from)))
            .unwrap_or(0),
    )
}

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

        // Namespace labels (for namespaceSelector-scoped anti-affinity, F-CNS-2). Non-fatal:
        // a failure just means no namespace-label-scoped terms are modeled.
        let namespaces: Vec<crate::model::NamespaceMeta> = {
            let api: Api<corev1::Namespace> = Api::all(self.client.clone());
            match api.list(&list_params).await {
                Ok(list) => list
                    .items
                    .into_iter()
                    .map(|ns| crate::model::NamespaceMeta {
                        name: ns.metadata.name.unwrap_or_default(),
                        labels: ns.metadata.labels.unwrap_or_default(),
                    })
                    .collect(),
                Err(err) => {
                    warn!(error = %err, "failed to list namespaces; continuing without namespace labels");
                    Vec::new()
                }
            }
        };

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
            namespaces,
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
        let mut snapshot = snapshot;
        // DRA (Dynamic Resource Allocation) F3a: augment nodes with synthetic per-DeviceClass
        // capacity and pods with per-class demand, so DRA workloads ride the generic extended-
        // resource solver path. Non-fatal: clusters without the resource.k8s.io API are unaffected.
        self.augment_with_dra(&mut snapshot).await;
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

    /// DRA F3a augmentation (non-fatal): list ResourceSlices/DeviceClasses/ResourceClaims/Templates
    /// (resource.k8s.io/v1alpha3), compute per-node per-class availability + per-pod demand via the
    /// pure `crate::dra` module, and fold them into node `extended_resources` and pod
    /// `extended_resource_requests` (keyed `dra.ksolver/<class>`). Any listing error (API absent /
    /// feature-gate off / RBAC) ⇒ skip DRA entirely, leaving the snapshot unchanged.
    async fn augment_with_dra(&self, snapshot: &mut ClusterSnapshot) {
        use k8s_openapi::api::resource::v1alpha3 as dra;
        let slices_api: Api<dra::ResourceSlice> = Api::all(self.client.clone());
        let classes_api: Api<dra::DeviceClass> = Api::all(self.client.clone());
        let claims_api: Api<dra::ResourceClaim> = Api::all(self.client.clone());
        let templates_api: Api<dra::ResourceClaimTemplate> = Api::all(self.client.clone());
        let lp = ListParams::default();
        let (slices, classes, claims, templates) = tokio::join!(
            slices_api.list(&lp),
            classes_api.list(&lp),
            claims_api.list(&lp),
            templates_api.list(&lp),
        );
        let (slices, classes, claims) = match (slices, classes, claims) {
            (Ok(s), Ok(c), Ok(cl)) => (s.items, c.items, cl.items),
            _ => {
                debug!(
                    "DRA API not available (resource.k8s.io/v1alpha3); skipping DRA augmentation"
                );
                return;
            }
        };
        if slices.is_empty() && classes.is_empty() {
            return; // no DRA in use
        }
        // Disclosures accumulated here are folded into snapshot.warnings (the "not silently trusted"
        // contract): anything that makes DRA demand/capacity approximate or incomplete is surfaced.
        let mut dra_warnings: std::collections::BTreeSet<String> =
            std::collections::BTreeSet::new();
        let templates = match templates {
            Ok(t) => t.items,
            Err(_) => {
                dra_warnings.insert(
                    "DRA: could not list ResourceClaimTemplates; template-backed pod demand not modeled"
                        .to_string(),
                );
                Vec::new()
            }
        };

        // Node capacity: synthetic dra.ksolver/<class> = unallocated matching devices.
        let avail = crate::dra::compute_availability(&slices, &classes, &claims);
        for class in &avail.unevaluable_classes {
            dra_warnings.insert(format!(
                "DRA: DeviceClass '{class}' has selectors ksolver cannot evaluate; its devices are not counted"
            ));
        }
        let mut nodes_aug = 0usize;
        let mut total_capacity = 0i64;
        for node in &mut snapshot.nodes {
            let mut touched = false;
            for ((n, class), count) in &avail.by_node_class {
                if n == &node.name && *count > 0 {
                    node.extended_resources
                        .insert(crate::dra::class_resource_key(class), *count);
                    total_capacity += *count;
                    touched = true;
                }
            }
            if touched {
                nodes_aug += 1;
            }
        }
        if avail.overlapping_classes {
            snapshot
                .warnings
                .push("DRA: overlapping DeviceClasses may overestimate node capacity".to_string());
        }

        // Pod demand: resolve each pod's spec.resourceClaims to a ResourceClaim or Template, sum
        // per-class demand, and add as extended requests keyed dra.ksolver/<class>.
        let claim_by_ns_name: BTreeMap<(String, String), &dra::ResourceClaim> = claims
            .iter()
            .filter_map(|c| {
                let ns = c.metadata.namespace.clone()?;
                let name = c.metadata.name.clone()?;
                Some(((ns, name), c))
            })
            .collect();
        let template_by_ns_name: BTreeMap<(String, String), &dra::ResourceClaimTemplate> =
            templates
                .iter()
                .filter_map(|t| {
                    let ns = t.metadata.namespace.clone()?;
                    let name = t.metadata.name.clone()?;
                    Some(((ns, name), t))
                })
                .collect();
        // Raw pod specs (the model Pod drops resourceClaims), keyed by (ns, name). If the pod list
        // fails, DRA pod demand cannot be modeled — disclose it (nodes still carry real capacity,
        // so unmodeled demand would otherwise silently over-admit).
        let raw_pod_claims: BTreeMap<(String, String), Vec<corev1::PodResourceClaim>> = {
            let pods_api: Api<corev1::Pod> = Api::all(self.client.clone());
            match pods_api.list(&lp).await {
                Ok(list) => list
                    .items
                    .into_iter()
                    .filter_map(|p| {
                        let ns = p.metadata.namespace.clone()?;
                        let name = p.metadata.name.clone()?;
                        let rc = p.spec.as_ref().and_then(|s| s.resource_claims.clone())?;
                        Some(((ns, name), rc))
                    })
                    .collect(),
                Err(_) => {
                    dra_warnings.insert(
                        "DRA: could not list pods for claim resolution; DRA pod demand not modeled"
                            .to_string(),
                    );
                    BTreeMap::new()
                }
            }
        };
        let mut pods_aug = 0usize;
        for pod in &mut snapshot.pods {
            let Some(refs) = raw_pod_claims.get(&(pod.namespace.clone(), pod.name.clone())) else {
                continue;
            };
            let mut added = false;
            for pod_claim in refs {
                let demand = if let Some(cn) = pod_claim.resource_claim_name.as_ref() {
                    claim_by_ns_name
                        .get(&(pod.namespace.clone(), cn.clone()))
                        .map(|c| crate::dra::claim_demand(c))
                } else if let Some(tn) = pod_claim.resource_claim_template_name.as_ref() {
                    template_by_ns_name
                        .get(&(pod.namespace.clone(), tn.clone()))
                        .and_then(|t| t.spec.spec.devices.as_ref())
                        .map(crate::dra::demand_from_device_claim)
                } else {
                    None
                };
                match demand {
                    Some(demand) => {
                        for c in demand.caveats {
                            dra_warnings.insert(format!("{}/{}: {c}", pod.namespace, pod.name));
                        }
                        for (class, count) in demand.by_class {
                            if count > 0 {
                                *pod.extended_resource_requests
                                    .entry(crate::dra::class_resource_key(&class))
                                    .or_default() += count;
                                added = true;
                            }
                        }
                    }
                    None => {
                        // Referenced claim/template not found (transient or RBAC) — its demand is
                        // unmodeled, so disclose rather than silently under-count.
                        dra_warnings.insert(format!(
                            "DRA: {}/{} references claim '{}' not found; demand not modeled",
                            pod.namespace, pod.name, pod_claim.name
                        ));
                    }
                }
            }
            if added {
                pods_aug += 1;
            }
        }
        snapshot.warnings.extend(dra_warnings);
        info!(
            slices = slices.len(),
            device_classes = classes.len(),
            claims = claims.len(),
            nodes_augmented = nodes_aug,
            total_dra_capacity = total_capacity,
            pods_with_dra_demand = pods_aug,
            unevaluable_classes = avail.unevaluable_classes.len(),
            "DRA F3a augmentation applied"
        );
    }

    pub async fn refresh_usage(&self, snapshot: &mut ClusterSnapshot) -> bool {
        let (node_usage_result, pod_usage_result) = tokio::join!(
            fetch_node_metrics(&self.client),
            fetch_pod_metrics(&self.client),
        );
        let node_usage = match node_usage_result {
            Ok(m) => m,
            Err(err) => {
                warn!(error = %err, "metrics refresh: failed to fetch node metrics");
                BTreeMap::new()
            }
        };
        let pod_usage = match pod_usage_result {
            Ok(m) => m,
            Err(err) => {
                warn!(error = %err, "metrics refresh: failed to fetch pod metrics");
                BTreeMap::new()
            }
        };
        for node in &mut snapshot.nodes {
            if let Some(u) = node_usage.get(&node.name) {
                node.usage = u.clone();
            }
        }
        for pod in &mut snapshot.pods {
            let key = namespaced_name(&pod.namespace, &pod.name);
            if let Some(u) = pod_usage.get(&key) {
                pod.usage = u.clone();
            }
        }
        let found = snapshot
            .pods
            .iter()
            .any(|p| p.usage.cpu_usage_milli > 0 || p.usage.memory_bytes > 0);
        if found {
            info!(
                pods_with_usage = pod_usage.len(),
                nodes_with_usage = node_usage.len(),
                "metrics refresh: usage data patched into cached snapshot"
            );
        } else {
            info!("metrics refresh: no usage data found from metrics-server");
        }
        found
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
        || chain.contains("service unavailable")
        || chain.contains("no endpoints available")
        || chain.contains("not found")
        || chain.contains("currently unable to handle")
        || chain.contains("try again later")
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
    let container_images = spec.as_ref().map(pod_container_images).unwrap_or_default();
    let command_hash = spec.as_ref().map(pod_command_hash).unwrap_or_default();
    let start_time_unix = status
        .as_ref()
        .and_then(|s| s.start_time.as_ref())
        .map(|t| t.0.timestamp())
        .unwrap_or(0);
    let finish_time_unix = status.as_ref().map(pod_finish_time_unix).unwrap_or(0);

    Pod {
        namespace: namespace.clone(),
        name: name.clone(),
        uid: pod.metadata.uid.clone().unwrap_or_default(),
        node_name: spec
            .as_ref()
            .and_then(|s| s.node_name.clone())
            .unwrap_or_default(),
        phase: status.and_then(|s| s.phase).unwrap_or_default(),
        start_time_unix,
        finish_time_unix,
        owner_kind: owner_kind(&owner_refs),
        owner_name: owner_name(&owner_refs),
        deleting: pod.metadata.deletion_timestamp.is_some(),
        labels,
        team: annotations
            .get("ksolver.dev/team")
            .cloned()
            .unwrap_or_default(),
        container_images,
        command_hash,
        predicted_runtime_seconds: annotation_i64(
            &annotations,
            "ksolver.dev/predicted-runtime-seconds",
            0,
        )
        .max(0),
        predicted_peak_vram_bytes: predicted_peak_vram_bytes(&annotations),
        business_value: annotation_i64(&annotations, "ksolver.dev/business-value", 0).max(0),
        deadline_unix_seconds: annotation_deadline_unix_seconds(&annotations),
        priority: pod_priority(&annotations, spec.as_ref()),
        priority_class_name: spec
            .as_ref()
            .and_then(|s| s.priority_class_name.clone())
            .unwrap_or_default(),
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
        modeled_host_anti_selectors: modeled_host_anti_selectors(affinity),
        anti_affinity_topology_selectors: modeled_topology_anti_selectors(affinity),
        preferred_pod_affinity: modeled_preferred_pod_terms(affinity),
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
                        selector_reqs: c
                            .label_selector
                            .as_ref()
                            .and_then(label_selector_to_reqs)
                            .unwrap_or_default(),
                        max_skew: c.max_skew,
                        topology_key: c.topology_key.clone(),
                        when_unsatisfiable: c.when_unsatisfiable.clone(),
                        min_domains: c.min_domains,
                        node_affinity_policy: c.node_affinity_policy.clone(),
                        node_taints_policy: c.node_taints_policy.clone(),
                        match_label_keys: c.match_label_keys.clone().unwrap_or_default(),
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
        disruption_cost: annotation_i32(&annotations, "ksolver.dev/disruption-cost", 0).max(0),
        migration_allowed: annotation_bool(&annotations, "ksolver.dev/migration-allowed", true),
        preemption_allowed: annotation_bool(&annotations, "ksolver.dev/preemption-allowed", true),
        do_not_disrupt: annotation_bool(&annotations, "ksolver.dev/do-not-disrupt", false),
        checkpoint_age_seconds: annotation_i64(
            &annotations,
            "ksolver.dev/checkpoint-age-seconds",
            0,
        )
        .max(0),
        progress_percent: annotation_i32(&annotations, "ksolver.dev/progress-percent", 0)
            .clamp(0, 100),
        autoscaler_not_safe_to_evict: annotations
            .get("cluster-autoscaler.kubernetes.io/safe-to-evict")
            .map(|v| v.eq_ignore_ascii_case("false"))
            .unwrap_or(false),
    }
}

fn pod_container_images(spec: &corev1::PodSpec) -> Vec<String> {
    spec.containers
        .iter()
        .map(|c| c.image.clone().unwrap_or_default())
        .filter(|image| !image.is_empty())
        .collect()
}

fn pod_command_hash(spec: &corev1::PodSpec) -> String {
    let mut hasher = Sha256::new();
    for c in &spec.containers {
        hasher.update(c.name.as_bytes());
        hasher.update([0]);
        hasher.update(c.image.clone().unwrap_or_default().as_bytes());
        hasher.update([0]);
        for part in c.command.clone().unwrap_or_default() {
            hasher.update(part.as_bytes());
            hasher.update([0]);
        }
        hasher.update([1]);
        for part in c.args.clone().unwrap_or_default() {
            hasher.update(part.as_bytes());
            hasher.update([0]);
        }
        hasher.update([2]);
    }
    let digest = hasher.finalize();
    digest[..12]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect::<String>()
}

fn pod_finish_time_unix(status: &corev1::PodStatus) -> i64 {
    status
        .container_statuses
        .as_deref()
        .unwrap_or_default()
        .iter()
        .filter_map(|s| {
            s.state
                .as_ref()
                .and_then(|state| state.terminated.as_ref())
                .and_then(|terminated| terminated.finished_at.as_ref())
                .map(|t| t.0.timestamp())
        })
        .max()
        .unwrap_or(0)
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
    let (selector, selector_modeled) = spec
        .and_then(|s| s.selector.as_ref())
        .map(pdb_label_selector_to_reqs)
        .unwrap_or_else(|| (Vec::new(), true));
    DisruptionBudget {
        namespace: pdb.metadata.namespace.unwrap_or_default(),
        name: pdb.metadata.name.unwrap_or_default(),
        selector,
        selector_modeled,
        min_available: spec
            .and_then(|s| s.min_available.as_ref())
            .map(int_or_string_to_string)
            .unwrap_or_default(),
        max_unavailable: spec
            .and_then(|s| s.max_unavailable.as_ref())
            .map(int_or_string_to_string)
            .unwrap_or_default(),
        disruptions_allowed: pdb
            .status
            .as_ref()
            .map(|s| s.disruptions_allowed)
            .unwrap_or(0),
    }
}

fn pdb_label_selector_to_reqs(
    ls: &k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector,
) -> (Vec<crate::model::LabelSelectorReq>, bool) {
    let mut reqs = Vec::new();
    if let Some(ml) = ls.match_labels.as_ref() {
        for (k, v) in ml {
            reqs.push(crate::model::LabelSelectorReq {
                key: k.clone(),
                operator: "In".to_string(),
                values: vec![v.clone()],
            });
        }
    }
    if let Some(exprs) = ls.match_expressions.as_ref() {
        for e in exprs {
            match e.operator.as_str() {
                "In" | "NotIn" => {
                    let vals = e.values.clone().unwrap_or_default();
                    if vals.is_empty() {
                        return (reqs, false);
                    }
                    reqs.push(crate::model::LabelSelectorReq {
                        key: e.key.clone(),
                        operator: e.operator.clone(),
                        values: vals,
                    });
                }
                "Exists" | "DoesNotExist" => reqs.push(crate::model::LabelSelectorReq {
                    key: e.key.clone(),
                    operator: e.operator.clone(),
                    values: Vec::new(),
                }),
                _ => return (reqs, false),
            }
        }
    }
    (reqs, true)
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

/// matchLabels of fully-modeled hostname pod-anti-affinity terms (hostname topology,
/// non-empty matchLabels, no matchExpressions, no namespace scoping), read from the raw
/// affinity so lossy conversions cannot broaden the selector. Mirrors the pending-pod
/// rule in scheduler::pod_filter.
/// Lower a raw `LabelSelector` into a modeled requirement list, or `None` if it cannot be
/// fully modeled. `matchLabels {k:v}` becomes `In [v]`; `matchExpressions` are carried for the
/// supported operators (In/NotIn require non-empty values; Exists/DoesNotExist ignore values).
/// Any other operator, or an empty selector (which would match all pods), yields `None` so the
/// term stays unmodeled (and caveated) rather than over-excluding.
pub(crate) fn label_selector_to_reqs(
    ls: &k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector,
) -> Option<Vec<crate::model::LabelSelectorReq>> {
    let mut reqs = Vec::new();
    if let Some(ml) = ls.match_labels.as_ref() {
        for (k, v) in ml {
            reqs.push(crate::model::LabelSelectorReq {
                key: k.clone(),
                operator: "In".to_string(),
                values: vec![v.clone()],
            });
        }
    }
    if let Some(exprs) = ls.match_expressions.as_ref() {
        for e in exprs {
            match e.operator.as_str() {
                "In" | "NotIn" => {
                    let vals = e.values.clone().unwrap_or_default();
                    if vals.is_empty() {
                        return None; // invalid In/NotIn (no values)
                    }
                    reqs.push(crate::model::LabelSelectorReq {
                        key: e.key.clone(),
                        operator: e.operator.clone(),
                        values: vals,
                    });
                }
                "Exists" | "DoesNotExist" => reqs.push(crate::model::LabelSelectorReq {
                    key: e.key.clone(),
                    operator: e.operator.clone(),
                    values: Vec::new(),
                }),
                _ => return None, // unsupported operator ⇒ unmodeled
            }
        }
    }
    if reqs.is_empty() {
        return None; // empty selector matches all ⇒ don't model as anti-affinity
    }
    Some(reqs)
}

/// Lower a raw `namespaceSelector` into modeled requirements (F-CNS-2). Unlike a label selector,
/// an EMPTY selector `{}` is valid and means ALL namespaces ⇒ `Some(vec![])`. Modelable ⇒
/// `Some(reqs)`; an unsupported operator ⇒ `None` (caller skips the term, still caveated).
pub(crate) fn namespace_selector_to_reqs(
    ls: &k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector,
) -> Option<Vec<crate::model::LabelSelectorReq>> {
    let mut reqs = Vec::new();
    if let Some(ml) = ls.match_labels.as_ref() {
        for (k, v) in ml {
            reqs.push(crate::model::LabelSelectorReq {
                key: k.clone(),
                operator: "In".to_string(),
                values: vec![v.clone()],
            });
        }
    }
    if let Some(exprs) = ls.match_expressions.as_ref() {
        for e in exprs {
            match e.operator.as_str() {
                "In" | "NotIn" => {
                    let vals = e.values.clone().unwrap_or_default();
                    if vals.is_empty() {
                        return None;
                    }
                    reqs.push(crate::model::LabelSelectorReq {
                        key: e.key.clone(),
                        operator: e.operator.clone(),
                        values: vals,
                    });
                }
                "Exists" | "DoesNotExist" => reqs.push(crate::model::LabelSelectorReq {
                    key: e.key.clone(),
                    operator: e.operator.clone(),
                    values: Vec::new(),
                }),
                _ => return None, // unsupported operator ⇒ term unmodeled
            }
        }
    }
    Some(reqs) // empty reqs = `{}` = all namespaces (valid for namespaceSelector)
}

fn modeled_anti_selectors_all(
    affinity: Option<&corev1::Affinity>,
) -> Vec<(String, crate::model::AntiAffinitySelector)> {
    let Some(terms) = affinity
        .and_then(|a| a.pod_anti_affinity.as_ref())
        .and_then(|pa| {
            pa.required_during_scheduling_ignored_during_execution
                .as_ref()
        })
    else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for term in terms {
        // namespaceSelector (F-CNS-2): None if absent; Some(reqs) if modelable (empty {} = all);
        // an unmodelable namespaceSelector ⇒ skip the whole term (stays caveated).
        let namespace_selector = match term.namespace_selector.as_ref() {
            None => None,
            Some(ns_ls) => match namespace_selector_to_reqs(ns_ls) {
                Some(reqs) => Some(reqs),
                None => continue,
            },
        };
        let Some(ls) = term.label_selector.as_ref() else {
            continue;
        };
        if let Some(reqs) = label_selector_to_reqs(ls) {
            let namespaces = term.namespaces.clone().unwrap_or_default();
            out.push((
                term.topology_key.clone(),
                crate::model::AntiAffinitySelector {
                    reqs,
                    namespaces,
                    namespace_selector,
                },
            ));
        }
    }
    out
}

/// Preferred (soft) pod affinity + anti-affinity terms from `podAffinity`/`podAntiAffinity`
/// `preferredDuringScheduling…`. weight>0, modelable label + namespace selectors; `anti=true` for
/// anti-affinity; unmodelable selectors skipped. Shared by the collector (running pods, for
/// symmetric soft scoring) and pod_filter (pending pods, forward soft scoring).
pub(crate) fn modeled_preferred_pod_terms(
    affinity: Option<&corev1::Affinity>,
) -> Vec<crate::model::PreferredPodTerm> {
    let Some(aff) = affinity else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let mut consume = |terms: Option<&Vec<corev1::WeightedPodAffinityTerm>>, anti: bool| {
        let Some(terms) = terms else { return };
        for wt in terms {
            if wt.weight <= 0 {
                continue;
            }
            let t = &wt.pod_affinity_term;
            let namespace_selector = match t.namespace_selector.as_ref() {
                None => None,
                Some(ns_ls) => match namespace_selector_to_reqs(ns_ls) {
                    Some(reqs) => Some(reqs),
                    None => continue,
                },
            };
            let Some(ls) = t.label_selector.as_ref() else {
                continue;
            };
            let Some(reqs) = label_selector_to_reqs(ls) else {
                continue;
            };
            out.push(crate::model::PreferredPodTerm {
                weight: i64::from(wt.weight),
                topology_key: t.topology_key.clone(),
                selector: crate::model::AntiAffinitySelector {
                    reqs,
                    namespaces: t.namespaces.clone().unwrap_or_default(),
                    namespace_selector,
                },
                anti,
            });
        }
    };
    consume(
        aff.pod_affinity.as_ref().and_then(|a| {
            a.preferred_during_scheduling_ignored_during_execution
                .as_ref()
        }),
        false,
    );
    consume(
        aff.pod_anti_affinity.as_ref().and_then(|a| {
            a.preferred_during_scheduling_ignored_during_execution
                .as_ref()
        }),
        true,
    );
    out
}

/// Fully-modeled *hostname* anti-affinity selectors (Phase 5e–5h path).
fn modeled_host_anti_selectors(
    affinity: Option<&corev1::Affinity>,
) -> Vec<crate::model::AntiAffinitySelector> {
    modeled_anti_selectors_all(affinity)
        .into_iter()
        .filter(|(k, _)| k == "kubernetes.io/hostname")
        .map(|(_, sel)| sel)
        .collect()
}

/// `(topologyKey, selector)` of fully-modeled *non-hostname* terms (Phase 12).
fn modeled_topology_anti_selectors(
    affinity: Option<&corev1::Affinity>,
) -> Vec<(String, crate::model::AntiAffinitySelector)> {
    modeled_anti_selectors_all(affinity)
        .into_iter()
        .filter(|(k, _)| k != "kubernetes.io/hostname")
        .collect()
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
) -> Vec<crate::model::NodeAffinityGroup> {
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
    // OR-of-terms: one NodeAffinityGroup per nodeSelectorTerm. matchExpressions evaluate against
    // node labels; matchFields (metadata.name) against node fields.
    let map_terms = |reqs: &Option<Vec<corev1::NodeSelectorRequirement>>| {
        reqs.as_ref()
            .map(|list| {
                list.iter()
                    .map(|expr| crate::model::NodeAffinityTerm {
                        key: expr.key.clone(),
                        operator: expr.operator.clone(),
                        values: expr.values.clone().unwrap_or_default(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };
    required
        .node_selector_terms
        .iter()
        .map(|term| crate::model::NodeAffinityGroup {
            match_expressions: map_terms(&term.match_expressions),
            match_fields: map_terms(&term.match_fields),
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::{
        extract_extended_resources, modeled_preferred_pod_terms, parse_bytes,
        parse_vpa_safety_margin_arg, to_model_pod,
    };
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use std::collections::BTreeMap;

    #[test]
    fn collects_running_pod_preferred_pod_terms() {
        use k8s_openapi::api::core::v1 as corev1;
        let aff = corev1::Affinity {
            pod_anti_affinity: Some(corev1::PodAntiAffinity {
                preferred_during_scheduling_ignored_during_execution: Some(vec![
                    corev1::WeightedPodAffinityTerm {
                        weight: 50,
                        pod_affinity_term: corev1::PodAffinityTerm {
                            label_selector: Some(
                                k8s_openapi::apimachinery::pkg::apis::meta::v1::LabelSelector {
                                    match_labels: Some(
                                        [("app".to_string(), "trainer".to_string())].into(),
                                    ),
                                    ..Default::default()
                                },
                            ),
                            topology_key: "kubernetes.io/hostname".to_string(),
                            ..Default::default()
                        },
                    },
                ]),
                ..Default::default()
            }),
            ..Default::default()
        };
        let got = modeled_preferred_pod_terms(Some(&aff));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].weight, 50);
        assert!(got[0].anti);
        assert_eq!(got[0].topology_key, "kubernetes.io/hostname");
        assert_eq!(got[0].selector.reqs.len(), 1);
    }

    #[test]
    fn collects_pod_policy_prediction_hints() {
        use k8s_openapi::api::core::v1 as corev1;

        let pod = corev1::Pod {
            metadata: ObjectMeta {
                namespace: Some("team".to_string()),
                name: Some("trainer".to_string()),
                annotations: Some(BTreeMap::from([
                    ("ksolver.dev/business-value".to_string(), "42".to_string()),
                    (
                        "ksolver.dev/deadline".to_string(),
                        "2027-01-15T12:00:00Z".to_string(),
                    ),
                    (
                        "ksolver.dev/predicted-runtime-seconds".to_string(),
                        "7200".to_string(),
                    ),
                    (
                        "ksolver.dev/predicted-peak-vram-gib".to_string(),
                        "80".to_string(),
                    ),
                ])),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                containers: vec![corev1::Container {
                    name: "main".to_string(),
                    image: Some("pytorch:latest".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            ..Default::default()
        };

        let got = to_model_pod(pod, &BTreeMap::new());

        assert_eq!(got.business_value, 42);
        assert_eq!(got.deadline_unix_seconds, 1_800_014_400);
        assert_eq!(got.predicted_runtime_seconds, 7200);
        assert_eq!(got.predicted_peak_vram_bytes, 80 * 1024 * 1024 * 1024);
    }

    #[test]
    fn collects_topology_spread_match_expression_requirements() {
        use k8s_openapi::api::core::v1 as corev1;
        use k8s_openapi::apimachinery::pkg::apis::meta::v1::{
            LabelSelector, LabelSelectorRequirement,
        };

        let pod = corev1::Pod {
            metadata: ObjectMeta {
                namespace: Some("team".to_string()),
                name: Some("trainer".to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                containers: vec![corev1::Container {
                    name: "main".to_string(),
                    ..Default::default()
                }],
                topology_spread_constraints: Some(vec![corev1::TopologySpreadConstraint {
                    max_skew: 1,
                    topology_key: "topology.kubernetes.io/zone".to_string(),
                    when_unsatisfiable: "DoNotSchedule".to_string(),
                    min_domains: Some(2),
                    node_affinity_policy: Some("Honor".to_string()),
                    node_taints_policy: Some("Ignore".to_string()),
                    match_label_keys: Some(vec!["pod-template-hash".to_string()]),
                    label_selector: Some(LabelSelector {
                        match_expressions: Some(vec![LabelSelectorRequirement {
                            key: "app".to_string(),
                            operator: "In".to_string(),
                            values: Some(vec!["trainer".to_string(), "worker".to_string()]),
                        }]),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        let got = to_model_pod(pod, &BTreeMap::new());

        assert_eq!(got.topology_spread_rules.len(), 1);
        assert!(got.topology_spread_rules[0].selector.is_empty());
        assert_eq!(got.topology_spread_rules[0].selector_reqs.len(), 1);
        assert_eq!(got.topology_spread_rules[0].selector_reqs[0].key, "app");
        assert_eq!(got.topology_spread_rules[0].min_domains, Some(2));
        assert_eq!(
            got.topology_spread_rules[0].node_affinity_policy,
            Some("Honor".to_string())
        );
        assert_eq!(
            got.topology_spread_rules[0].node_taints_policy,
            Some("Ignore".to_string())
        );
        assert_eq!(
            got.topology_spread_rules[0].match_label_keys,
            vec!["pod-template-hash".to_string()]
        );
        assert_eq!(
            got.topology_spread_rules[0].selector_reqs[0].values,
            vec!["trainer".to_string(), "worker".to_string()]
        );
    }

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
