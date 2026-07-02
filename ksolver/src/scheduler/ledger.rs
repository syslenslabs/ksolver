//! Pure in-memory reservation ledger for future real binding.
//!
//! The ledger tracks capacity and tenant quota committed by binding plans that have been decided
//! but are not yet reflected in informer state. It does not call Kubernetes and does not mutate the
//! cluster; binder code can transact here before attempting real `Binding` posts.

use crate::scheduler::binding::BindingPlanEntry;
use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

const GPU_RESOURCE: &str = "nvidia.com/gpu";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReservationError {
    InvalidEntry {
        namespace: String,
        pod: String,
        reason: String,
    },
    UnknownNode {
        node: String,
    },
    NodeCapacityExceeded {
        node: String,
        requested: i64,
        available: i64,
    },
    TenantQuotaExceeded {
        tenant: String,
        requested: i64,
        available: i64,
    },
}

#[derive(Debug, Clone)]
struct Reservation {
    entries: Vec<BindingPlanEntry>,
    expires_at: Instant,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileStats {
    pub expired_reservations: usize,
    pub observed_bound_entries: usize,
    pub stale_entries: usize,
    pub active_reservations: usize,
    pub active_entries: usize,
}

#[derive(Debug, Clone)]
pub struct ReservationLedger {
    reservations: HashMap<u64, Reservation>,
    next_id: u64,
}

impl Default for ReservationLedger {
    fn default() -> Self {
        Self::new()
    }
}

impl ReservationLedger {
    pub fn new() -> Self {
        Self {
            reservations: HashMap::new(),
            next_id: 1,
        }
    }

    pub fn reserve(
        &mut self,
        cluster: &crate::model::NormalizedCluster,
        tenant_quotas: &BTreeMap<String, i64>,
        entries: Vec<BindingPlanEntry>,
        ttl: Duration,
        now: Instant,
    ) -> Result<u64, ReservationError> {
        self.expire(now);
        validate_entries(&entries)?;
        validate_capacity(cluster, &self.committed_by_node(), &entries)?;
        validate_quota(
            cluster,
            tenant_quotas,
            &self.committed_by_tenant(),
            &entries,
        )?;

        let id = self.next_id;
        self.next_id = self.next_id.saturating_add(1);
        self.reservations.insert(
            id,
            Reservation {
                entries,
                expires_at: now + ttl,
            },
        );
        Ok(id)
    }

    pub fn release(&mut self, id: u64) -> bool {
        self.reservations.remove(&id).is_some()
    }

    pub fn expire(&mut self, now: Instant) -> usize {
        let before = self.reservations.len();
        self.reservations.retain(|_, r| r.expires_at > now);
        before - self.reservations.len()
    }

    /// Reconcile committed reservations against the latest informer snapshot.
    ///
    /// A reservation entry is released when the expected pod is observed bound to the target node,
    /// or when the pod identity is missing or no longer matches the reservation
    /// (gone/recreated/already bound elsewhere). Same-UID unbound pods remain reserved until they
    /// bind or the TTL expires.
    pub fn reconcile_observed(
        &mut self,
        cluster: &crate::model::NormalizedCluster,
        now: Instant,
    ) -> ReconcileStats {
        let expired_reservations = self.expire(now);
        let mut stats = ReconcileStats {
            expired_reservations,
            ..Default::default()
        };

        for reservation in self.reservations.values_mut() {
            reservation.entries.retain(|entry| {
                match cluster
                    .workloads
                    .iter()
                    .find(|w| w.namespace == entry.namespace && w.name == entry.pod_name)
                {
                    Some(w) if entry.pod_uid.is_empty() || w.uid.is_empty() => {
                        stats.stale_entries += 1;
                        false
                    }
                    Some(w)
                        if !entry.pod_uid.is_empty()
                            && !w.uid.is_empty()
                            && w.uid != entry.pod_uid =>
                    {
                        stats.stale_entries += 1;
                        false
                    }
                    Some(w) if w.current_node == entry.node_name => {
                        stats.observed_bound_entries += 1;
                        false
                    }
                    Some(w) if !w.current_node.is_empty() => {
                        stats.stale_entries += 1;
                        false
                    }
                    Some(_) => true,
                    None => {
                        stats.stale_entries += 1;
                        false
                    }
                }
            });
        }
        self.reservations.retain(|_, r| !r.entries.is_empty());
        stats.active_reservations = self.reservations.len();
        stats.active_entries = self
            .reservations
            .values()
            .map(|r| r.entries.len())
            .sum::<usize>();
        stats
    }

    pub fn len(&self) -> usize {
        self.reservations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.reservations.is_empty()
    }

    pub fn committed_gpu_by_node(&self) -> BTreeMap<String, i64> {
        self.committed_by_node()
    }

    pub fn committed_gpu_by_tenant(&self) -> BTreeMap<String, i64> {
        self.committed_by_tenant()
    }

    pub fn entry_count(&self) -> usize {
        self.reservations
            .values()
            .map(|r| r.entries.len())
            .sum::<usize>()
    }

    pub fn committed_gpu_total(&self) -> i64 {
        self.committed_by_node().values().sum()
    }

    fn committed_by_node(&self) -> BTreeMap<String, i64> {
        let mut committed = BTreeMap::new();
        for reservation in self.reservations.values() {
            for e in &reservation.entries {
                *committed.entry(e.node_name.clone()).or_insert(0) += e.gpu_request.max(0);
            }
        }
        committed
    }

    fn committed_by_tenant(&self) -> BTreeMap<String, i64> {
        let mut committed = BTreeMap::new();
        for reservation in self.reservations.values() {
            for e in &reservation.entries {
                *committed.entry(e.namespace.clone()).or_insert(0) += e.gpu_request.max(0);
            }
        }
        committed
    }
}

fn validate_entries(entries: &[BindingPlanEntry]) -> Result<(), ReservationError> {
    for e in entries {
        if e.namespace.trim().is_empty() {
            return Err(ReservationError::InvalidEntry {
                namespace: e.namespace.clone(),
                pod: e.pod_name.clone(),
                reason: "missing namespace".to_string(),
            });
        }
        if e.pod_name.trim().is_empty() {
            return Err(ReservationError::InvalidEntry {
                namespace: e.namespace.clone(),
                pod: e.pod_name.clone(),
                reason: "missing pod name".to_string(),
            });
        }
        if e.pod_uid.trim().is_empty() {
            return Err(ReservationError::InvalidEntry {
                namespace: e.namespace.clone(),
                pod: e.pod_name.clone(),
                reason: "missing pod uid".to_string(),
            });
        }
        if e.node_name.trim().is_empty() {
            return Err(ReservationError::InvalidEntry {
                namespace: e.namespace.clone(),
                pod: e.pod_name.clone(),
                reason: "missing target node".to_string(),
            });
        }
        if e.gpu_request <= 0 {
            return Err(ReservationError::InvalidEntry {
                namespace: e.namespace.clone(),
                pod: e.pod_name.clone(),
                reason: format!("non-positive GPU request {}", e.gpu_request),
            });
        }
    }
    Ok(())
}

fn running_gpu_by_node(cluster: &crate::model::NormalizedCluster) -> BTreeMap<String, i64> {
    let mut used = BTreeMap::new();
    for w in &cluster.workloads {
        if w.current_node.is_empty() {
            continue;
        }
        let gpu = w
            .extended_resource_requests
            .iter()
            .filter(|(resource, _)| {
                resource.as_str() == GPU_RESOURCE || resource.starts_with("nvidia.com/mig-")
            })
            .map(|(_, amount)| *amount)
            .sum::<i64>()
            .max(0);
        *used.entry(w.current_node.clone()).or_insert(0) += gpu;
    }
    used
}

fn running_gpu_by_tenant(cluster: &crate::model::NormalizedCluster) -> BTreeMap<String, i64> {
    let mut used = BTreeMap::new();
    for w in &cluster.workloads {
        if w.current_node.is_empty() {
            continue;
        }
        let gpu = w
            .extended_resource_requests
            .iter()
            .filter(|(resource, _)| {
                resource.as_str() == GPU_RESOURCE || resource.starts_with("nvidia.com/mig-")
            })
            .map(|(_, amount)| *amount)
            .sum::<i64>()
            .max(0);
        *used.entry(w.namespace.clone()).or_insert(0) += gpu;
    }
    used
}

fn validate_capacity(
    cluster: &crate::model::NormalizedCluster,
    committed: &BTreeMap<String, i64>,
    entries: &[BindingPlanEntry],
) -> Result<(), ReservationError> {
    let running = running_gpu_by_node(cluster);
    let mut requested = BTreeMap::new();
    for e in entries {
        *requested.entry(e.node_name.clone()).or_insert(0) += e.gpu_request.max(0);
    }
    for (node_name, plan_gpu) in requested {
        let Some(node) = cluster.nodes.iter().find(|n| n.name == node_name) else {
            return Err(ReservationError::UnknownNode { node: node_name });
        };
        let capacity = node
            .extended_resources
            .iter()
            .filter(|(resource, _)| {
                resource.as_str() == GPU_RESOURCE || resource.starts_with("nvidia.com/mig-")
            })
            .map(|(_, amount)| *amount)
            .sum::<i64>()
            .max(0);
        let already_used = running.get(&node_name).copied().unwrap_or(0)
            + committed.get(&node_name).copied().unwrap_or(0);
        let available = capacity - already_used;
        if plan_gpu > available {
            return Err(ReservationError::NodeCapacityExceeded {
                node: node_name,
                requested: plan_gpu,
                available,
            });
        }
    }
    Ok(())
}

fn validate_quota(
    cluster: &crate::model::NormalizedCluster,
    tenant_quotas: &BTreeMap<String, i64>,
    committed: &BTreeMap<String, i64>,
    entries: &[BindingPlanEntry],
) -> Result<(), ReservationError> {
    if tenant_quotas.is_empty() {
        return Ok(());
    }
    let running = running_gpu_by_tenant(cluster);
    let mut requested = BTreeMap::new();
    for e in entries {
        *requested.entry(e.namespace.clone()).or_insert(0) += e.gpu_request.max(0);
    }
    for (tenant, plan_gpu) in requested {
        let Some(limit) = tenant_quotas.get(&tenant) else {
            continue;
        };
        let already_used = running.get(&tenant).copied().unwrap_or(0)
            + committed.get(&tenant).copied().unwrap_or(0);
        let available = *limit - already_used;
        if plan_gpu > available {
            return Err(ReservationError::TenantQuotaExceeded {
                tenant,
                requested: plan_gpu,
                available,
            });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(ns: &str, pod: &str, node: &str, gpu: i64) -> BindingPlanEntry {
        BindingPlanEntry {
            namespace: ns.to_string(),
            pod_name: pod.to_string(),
            pod_uid: format!("uid-{pod}"),
            binding_group: String::new(),
            team: String::new(),
            node_name: node.to_string(),
            gpu_request: gpu,
            binding_body: serde_json::json!({}),
        }
    }

    fn node(name: &str, gpu: i64) -> crate::model::NormalizedNode {
        crate::model::NormalizedNode {
            name: name.to_string(),
            extended_resources: BTreeMap::from([(GPU_RESOURCE.to_string(), gpu)]),
            ..Default::default()
        }
    }

    fn running(ns: &str, name: &str, node: &str, gpu: i64) -> crate::model::NormalizedWorkload {
        crate::model::NormalizedWorkload {
            namespace: ns.to_string(),
            name: name.to_string(),
            current_node: node.to_string(),
            extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), gpu)]),
            ..Default::default()
        }
    }

    fn observed(
        ns: &str,
        name: &str,
        uid: &str,
        node: &str,
        gpu: i64,
    ) -> crate::model::NormalizedWorkload {
        crate::model::NormalizedWorkload {
            namespace: ns.to_string(),
            name: name.to_string(),
            uid: uid.to_string(),
            current_node: node.to_string(),
            extended_resource_requests: BTreeMap::from([(GPU_RESOURCE.to_string(), gpu)]),
            ..Default::default()
        }
    }

    fn cluster() -> crate::model::NormalizedCluster {
        crate::model::NormalizedCluster {
            nodes: vec![node("n1", 4), node("n2", 2)],
            workloads: vec![running("team-a", "running-a", "n1", 1)],
            ..Default::default()
        }
    }

    #[test]
    fn reserves_capacity_and_quota_then_releases() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        let id = ledger
            .reserve(
                &cluster(),
                &BTreeMap::from([("team-a".to_string(), 4)]),
                vec![entry("team-a", "p1", "n1", 2)],
                Duration::from_secs(30),
                now,
            )
            .expect("reservation should fit residual capacity and quota");

        assert_eq!(ledger.committed_gpu_by_node().get("n1"), Some(&2));
        assert_eq!(ledger.committed_gpu_by_tenant().get("team-a"), Some(&2));
        assert!(ledger.release(id));
        assert!(ledger.is_empty());
    }

    #[test]
    fn rejects_node_overcommit_across_existing_and_reserved_capacity() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![entry("team-a", "p1", "n1", 2)],
                Duration::from_secs(30),
                now,
            )
            .expect("first reservation should fit");

        match ledger.reserve(
            &cluster(),
            &BTreeMap::new(),
            vec![entry("team-b", "p2", "n1", 2)],
            Duration::from_secs(30),
            now,
        ) {
            Err(ReservationError::NodeCapacityExceeded {
                node,
                requested,
                available,
            }) => {
                assert_eq!(node, "n1");
                assert_eq!(requested, 2);
                assert_eq!(available, 1);
            }
            other => panic!("expected node overcommit rejection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_tenant_quota_overcommit() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        match ledger.reserve(
            &cluster(),
            &BTreeMap::from([("team-a".to_string(), 2)]),
            vec![entry("team-a", "p1", "n1", 2)],
            Duration::from_secs(30),
            now,
        ) {
            Err(ReservationError::TenantQuotaExceeded {
                tenant,
                requested,
                available,
            }) => {
                assert_eq!(tenant, "team-a");
                assert_eq!(requested, 2);
                assert_eq!(available, 1);
            }
            other => panic!("expected tenant quota rejection, got {other:?}"),
        }
    }

    #[test]
    fn rejects_malformed_reservation_entries_before_reserving() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        let mut invalid = entry("team-a", "p1", "n1", 0);

        match ledger.reserve(
            &cluster(),
            &BTreeMap::new(),
            vec![invalid.clone()],
            Duration::from_secs(30),
            now,
        ) {
            Err(ReservationError::InvalidEntry {
                namespace,
                pod,
                reason,
            }) => {
                assert_eq!(namespace, "team-a");
                assert_eq!(pod, "p1");
                assert!(reason.contains("non-positive GPU request"));
            }
            other => panic!("expected invalid entry rejection, got {other:?}"),
        }
        assert!(ledger.is_empty());

        invalid.gpu_request = 1;
        invalid.pod_uid.clear();
        match ledger.reserve(
            &cluster(),
            &BTreeMap::new(),
            vec![invalid],
            Duration::from_secs(30),
            now,
        ) {
            Err(ReservationError::InvalidEntry { reason, .. }) => {
                assert_eq!(reason, "missing pod uid");
            }
            other => panic!("expected missing uid rejection, got {other:?}"),
        }
        assert!(ledger.is_empty());
    }

    #[test]
    fn expires_old_reservations_before_validating_new_one() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![entry("team-a", "p1", "n1", 3)],
                Duration::from_secs(10),
                now,
            )
            .expect("first reservation should consume residual n1 capacity");

        let later = now + Duration::from_secs(11);
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![entry("team-b", "p2", "n1", 3)],
                Duration::from_secs(10),
                later,
            )
            .expect("expired reservation should no longer consume capacity");
        assert_eq!(ledger.len(), 1);
    }

    #[test]
    fn rejects_unknown_node() {
        let mut ledger = ReservationLedger::new();
        match ledger.reserve(
            &cluster(),
            &BTreeMap::new(),
            vec![entry("team-a", "p1", "missing", 1)],
            Duration::from_secs(30),
            Instant::now(),
        ) {
            Err(ReservationError::UnknownNode { node }) => assert_eq!(node, "missing"),
            other => panic!("expected unknown node rejection, got {other:?}"),
        }
    }

    #[test]
    fn reconcile_releases_observed_bound_entry() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![entry("team-a", "p1", "n1", 1)],
                Duration::from_secs(60),
                now,
            )
            .expect("reservation should fit");

        let mut observed_cluster = cluster();
        observed_cluster
            .workloads
            .push(observed("team-a", "p1", "uid-p1", "n1", 1));
        let stats = ledger.reconcile_observed(&observed_cluster, now);

        assert_eq!(stats.observed_bound_entries, 1);
        assert_eq!(stats.stale_entries, 0);
        assert!(ledger.is_empty());
        assert_eq!(ledger.committed_gpu_by_node().get("n1"), None);
    }

    #[test]
    fn reconcile_keeps_same_uid_unbound_entry_until_ttl() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![entry("team-a", "p1", "n1", 1)],
                Duration::from_secs(60),
                now,
            )
            .expect("reservation should fit");

        let mut observed_cluster = cluster();
        observed_cluster
            .workloads
            .push(observed("team-a", "p1", "uid-p1", "", 1));
        let stats = ledger.reconcile_observed(&observed_cluster, now);

        assert_eq!(stats.observed_bound_entries, 0);
        assert_eq!(stats.stale_entries, 0);
        assert_eq!(stats.active_entries, 1);
        assert_eq!(ledger.committed_gpu_by_node().get("n1"), Some(&1));
    }

    #[test]
    fn reconcile_releases_stale_recreated_or_elsewhere_bound_entries() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![
                    entry("team-a", "recreated", "n1", 1),
                    entry("team-a", "elsewhere", "n1", 1),
                ],
                Duration::from_secs(60),
                now,
            )
            .expect("reservation should fit");

        let mut observed_cluster = cluster();
        observed_cluster
            .workloads
            .push(observed("team-a", "recreated", "new-uid", "", 1));
        observed_cluster
            .workloads
            .push(observed("team-a", "elsewhere", "uid-elsewhere", "n2", 1));
        let stats = ledger.reconcile_observed(&observed_cluster, now);

        assert_eq!(stats.stale_entries, 2);
        assert!(ledger.is_empty());
    }

    #[test]
    fn reconcile_releases_entries_with_missing_identity() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        let mut missing_plan_uid = entry("team-a", "missing-plan-uid", "n1", 1);
        missing_plan_uid.pod_uid.clear();
        ledger.reservations.insert(
            1,
            Reservation {
                entries: vec![
                    missing_plan_uid,
                    entry("team-a", "missing-snapshot-uid", "n1", 1),
                ],
                expires_at: now + Duration::from_secs(60),
            },
        );

        let mut observed_cluster = cluster();
        observed_cluster
            .workloads
            .push(observed("team-a", "missing-plan-uid", "uid-a", "", 1));
        observed_cluster
            .workloads
            .push(observed("team-a", "missing-snapshot-uid", "", "", 1));
        let stats = ledger.reconcile_observed(&observed_cluster, now);

        assert_eq!(stats.stale_entries, 2);
        assert!(ledger.is_empty());
        assert_eq!(ledger.committed_gpu_total(), 0);
    }

    #[test]
    fn reconcile_removes_only_observed_member_of_multi_entry_reservation() {
        let now = Instant::now();
        let mut ledger = ReservationLedger::new();
        ledger
            .reserve(
                &cluster(),
                &BTreeMap::new(),
                vec![
                    entry("team-a", "p1", "n1", 1),
                    entry("team-a", "p2", "n1", 1),
                ],
                Duration::from_secs(60),
                now,
            )
            .expect("reservation should fit");

        let mut observed_cluster = cluster();
        observed_cluster
            .workloads
            .push(observed("team-a", "p1", "uid-p1", "n1", 1));
        observed_cluster
            .workloads
            .push(observed("team-a", "p2", "uid-p2", "", 1));
        let stats = ledger.reconcile_observed(&observed_cluster, now);

        assert_eq!(stats.observed_bound_entries, 1);
        assert_eq!(stats.active_reservations, 1);
        assert_eq!(stats.active_entries, 1);
        assert_eq!(ledger.entry_count(), 1);
        assert_eq!(ledger.committed_gpu_by_node().get("n1"), Some(&1));
    }
}
