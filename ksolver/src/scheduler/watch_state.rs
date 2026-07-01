use crate::scheduler::config::ShadowConfig;
use crate::scheduler::pod_filter::{classify, PendingGpuPod};
use k8s_openapi::api::core::v1 as corev1;
use kube::runtime::watcher::Event;
use std::collections::BTreeMap;

fn key_of(pod: &corev1::Pod) -> String {
    match pod.metadata.uid.as_deref() {
        Some(u) if !u.is_empty() => u.to_string(),
        _ => format!(
            "{}/{}",
            pod.metadata.namespace.clone().unwrap_or_default(),
            pod.metadata.name.clone().unwrap_or_default()
        ),
    }
}

/// Maintains the set of in-scope pending GPU pods across the watcher lifecycle.
pub struct WatchState {
    observed: BTreeMap<String, PendingGpuPod>,
    init_buffer: Option<BTreeMap<String, PendingGpuPod>>,
}

impl WatchState {
    pub fn new() -> Self {
        Self {
            observed: BTreeMap::new(),
            init_buffer: None,
        }
    }

    pub fn apply(&mut self, event: &Event<corev1::Pod>, cfg: &ShadowConfig) {
        match event {
            Event::Init => {
                self.init_buffer = Some(BTreeMap::new());
            }
            Event::InitApply(pod) => {
                if let Some(p) = classify(pod, cfg) {
                    let buf = self.init_buffer.get_or_insert_with(BTreeMap::new);
                    buf.insert(key_of(pod), p);
                }
            }
            Event::InitDone => {
                if let Some(buf) = self.init_buffer.take() {
                    self.observed = buf;
                }
            }
            Event::Apply(pod) => {
                let key = key_of(pod);
                match classify(pod, cfg) {
                    Some(p) => {
                        self.observed.insert(key, p);
                    }
                    None => {
                        self.observed.remove(&key);
                    }
                }
            }
            Event::Delete(pod) => {
                self.observed.remove(&key_of(pod));
            }
        }
    }

    pub fn snapshot(&self) -> Vec<PendingGpuPod> {
        self.observed.values().cloned().collect()
    }

    pub fn len(&self) -> usize {
        self.observed.len()
    }

    pub fn is_empty(&self) -> bool {
        self.observed.is_empty()
    }
}

impl Default for WatchState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::scheduler::config::ShadowConfig;
    use k8s_openapi::api::core::v1 as corev1;
    use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::runtime::watcher::Event;
    use std::collections::BTreeMap;
    use std::time::Duration;

    fn cfg() -> ShadowConfig {
        ShadowConfig {
            scheduler_name: "ksolver".to_string(),
            batch_window: Duration::from_secs(10),
            namespace_allowlist: vec![],
            gpu_resource_names: vec!["nvidia.com/gpu".to_string()],
            cluster_name: "default".to_string(),
            kubeconfig: String::new(),
            http_addr: "127.0.0.1:8090".to_string(),
            gang_label_key: "scheduling.x-k8s.io/pod-group".to_string(),
        }
    }

    fn gpu_pod(uid: &str, name: &str) -> corev1::Pod {
        let mut req = BTreeMap::new();
        req.insert("nvidia.com/gpu".to_string(), Quantity("1".to_string()));
        corev1::Pod {
            metadata: ObjectMeta {
                name: Some(name.to_string()),
                namespace: Some("team-a".to_string()),
                uid: Some(uid.to_string()),
                ..Default::default()
            },
            spec: Some(corev1::PodSpec {
                scheduler_name: Some("ksolver".to_string()),
                containers: vec![corev1::Container {
                    name: "m".to_string(),
                    resources: Some(corev1::ResourceRequirements {
                        requests: Some(req),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            status: Some(corev1::PodStatus {
                phase: Some("Pending".to_string()),
                ..Default::default()
            }),
        }
    }

    #[test]
    fn apply_adds_matching_pod() {
        let mut s = WatchState::new();
        s.apply(&Event::Apply(gpu_pod("u1", "p1")), &cfg());
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn delete_removes_pod() {
        let mut s = WatchState::new();
        s.apply(&Event::Apply(gpu_pod("u1", "p1")), &cfg());
        s.apply(&Event::Delete(gpu_pod("u1", "p1")), &cfg());
        assert_eq!(s.len(), 0);
    }

    #[test]
    fn relist_drops_pods_absent_after_initdone() {
        let mut s = WatchState::new();
        s.apply(&Event::Apply(gpu_pod("u1", "p1")), &cfg());
        s.apply(&Event::Apply(gpu_pod("u2", "p2")), &cfg());
        // Relist only reports u2 -> u1 must be dropped on InitDone.
        s.apply(&Event::Init, &cfg());
        s.apply(&Event::InitApply(gpu_pod("u2", "p2")), &cfg());
        s.apply(&Event::InitDone, &cfg());
        let snap = s.snapshot();
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].uid, "u2");
    }
}
