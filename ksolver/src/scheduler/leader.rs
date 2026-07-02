use crate::metrics;
use crate::scheduler::config::ShadowConfig;
use anyhow::Result;
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use kube::api::PostParams;
use kube::{Api, Client, Error};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const DEFAULT_LEASE_DURATION_SECONDS: i32 = 15;
const DEFAULT_RENEW_INTERVAL_SECONDS: u64 = 5;

#[derive(Debug, Clone, PartialEq, Eq)]
enum LeaseAction {
    Acquire { transitions: i32 },
    Renew { transitions: i32 },
    Wait { holder: String },
}

#[derive(Clone)]
pub struct LeaderElector {
    is_leader: Arc<AtomicBool>,
}

impl LeaderElector {
    pub fn disabled() -> Self {
        metrics::set_shadow_leader(true);
        Self {
            is_leader: Arc::new(AtomicBool::new(true)),
        }
    }

    #[cfg(test)]
    pub fn for_test(is_leader: bool) -> Self {
        Self {
            is_leader: Arc::new(AtomicBool::new(is_leader)),
        }
    }

    pub fn spawn(client: Client, cfg: ShadowConfig) -> Result<Self> {
        let elector = Self {
            is_leader: Arc::new(AtomicBool::new(false)),
        };
        let is_leader = elector.is_leader.clone();
        tokio::spawn(async move {
            let leases: Api<Lease> = Api::namespaced(client, &cfg.leader_election_namespace);
            loop {
                let held = match try_acquire_or_renew(&leases, &cfg).await {
                    Ok(held) => held,
                    Err(err) => {
                        warn!(
                            error = %err,
                            namespace = %cfg.leader_election_namespace,
                            lease = %cfg.leader_election_lease_name,
                            identity = %cfg.leader_election_identity,
                            "leader election renewal failed"
                        );
                        metrics::inc_shadow_leader_renew_errors();
                        false
                    }
                };
                metrics::set_shadow_leader(held);
                is_leader.store(held, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_secs(DEFAULT_RENEW_INTERVAL_SECONDS)).await;
            }
        });
        Ok(elector)
    }

    pub fn is_leader(&self) -> bool {
        self.is_leader.load(Ordering::SeqCst)
    }
}

async fn try_acquire_or_renew(leases: &Api<Lease>, cfg: &ShadowConfig) -> Result<bool> {
    let now = chrono::Utc::now();
    match leases.get(&cfg.leader_election_lease_name).await {
        Ok(existing) => match action_for_lease(
            existing.spec.as_ref(),
            &cfg.leader_election_identity,
            now,
            DEFAULT_LEASE_DURATION_SECONDS,
        ) {
            LeaseAction::Acquire { transitions } => {
                let lease = lease_object(
                    &cfg.leader_election_lease_name,
                    &cfg.leader_election_identity,
                    now,
                    transitions,
                    existing.metadata.resource_version.clone(),
                    None,
                );
                match leases
                    .replace(
                        &cfg.leader_election_lease_name,
                        &PostParams::default(),
                        &lease,
                    )
                    .await
                {
                    Ok(_) => {
                        metrics::inc_shadow_leader_acquired();
                        info!(
                            namespace = %cfg.leader_election_namespace,
                            lease = %cfg.leader_election_lease_name,
                            identity = %cfg.leader_election_identity,
                            "leader election lease acquired"
                        );
                        Ok(true)
                    }
                    Err(Error::Api(ae)) if ae.code == 409 => Ok(false),
                    Err(err) => Err(err.into()),
                }
            }
            LeaseAction::Renew { transitions } => {
                let lease = lease_object(
                    &cfg.leader_election_lease_name,
                    &cfg.leader_election_identity,
                    now,
                    transitions,
                    existing.metadata.resource_version.clone(),
                    existing.spec.as_ref().and_then(|s| s.acquire_time.clone()),
                );
                match leases
                    .replace(
                        &cfg.leader_election_lease_name,
                        &PostParams::default(),
                        &lease,
                    )
                    .await
                {
                    Ok(_) => {
                        metrics::inc_shadow_leader_renewed();
                        Ok(true)
                    }
                    Err(Error::Api(ae)) if ae.code == 409 => Ok(false),
                    Err(err) => Err(err.into()),
                }
            }
            LeaseAction::Wait { .. } => {
                metrics::inc_shadow_leader_wait();
                Ok(false)
            }
        },
        Err(Error::Api(ae)) if ae.code == 404 => {
            let lease = lease_object(
                &cfg.leader_election_lease_name,
                &cfg.leader_election_identity,
                now,
                0,
                None,
                None,
            );
            match leases.create(&PostParams::default(), &lease).await {
                Ok(_) => {
                    metrics::inc_shadow_leader_acquired();
                    info!(
                        namespace = %cfg.leader_election_namespace,
                        lease = %cfg.leader_election_lease_name,
                        identity = %cfg.leader_election_identity,
                        "leader election lease created"
                    );
                    Ok(true)
                }
                Err(Error::Api(ae)) if ae.code == 409 => Ok(false),
                Err(err) => Err(err.into()),
            }
        }
        Err(err) => Err(err.into()),
    }
}

fn lease_object(
    name: &str,
    identity: &str,
    now: chrono::DateTime<chrono::Utc>,
    transitions: i32,
    resource_version: Option<String>,
    acquire_time: Option<MicroTime>,
) -> Lease {
    Lease {
        metadata: ObjectMeta {
            name: Some(name.to_string()),
            resource_version,
            ..Default::default()
        },
        spec: Some(LeaseSpec {
            acquire_time: acquire_time.or_else(|| Some(MicroTime(now))),
            holder_identity: Some(identity.to_string()),
            lease_duration_seconds: Some(DEFAULT_LEASE_DURATION_SECONDS),
            lease_transitions: Some(transitions),
            renew_time: Some(MicroTime(now)),
            ..Default::default()
        }),
    }
}

fn action_for_lease(
    spec: Option<&LeaseSpec>,
    identity: &str,
    now: chrono::DateTime<chrono::Utc>,
    default_duration_seconds: i32,
) -> LeaseAction {
    let Some(spec) = spec else {
        return LeaseAction::Acquire { transitions: 0 };
    };
    let holder = spec.holder_identity.clone().unwrap_or_default();
    let transitions = spec.lease_transitions.unwrap_or_default();
    if holder.trim().is_empty() {
        return LeaseAction::Acquire { transitions };
    }
    if holder == identity {
        return LeaseAction::Renew { transitions };
    }
    if lease_expired(spec, now, default_duration_seconds) {
        return LeaseAction::Acquire {
            transitions: transitions.saturating_add(1),
        };
    }
    LeaseAction::Wait { holder }
}

fn lease_expired(
    spec: &LeaseSpec,
    now: chrono::DateTime<chrono::Utc>,
    default_duration_seconds: i32,
) -> bool {
    let duration = spec
        .lease_duration_seconds
        .unwrap_or(default_duration_seconds)
        .max(1);
    let Some(renew_time) = spec.renew_time.as_ref() else {
        return true;
    };
    now.signed_duration_since(renew_time.0) > chrono::Duration::seconds(duration.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(holder: &str, renewed_seconds_ago: i64, transitions: i32) -> LeaseSpec {
        LeaseSpec {
            holder_identity: Some(holder.to_string()),
            lease_duration_seconds: Some(DEFAULT_LEASE_DURATION_SECONDS),
            lease_transitions: Some(transitions),
            renew_time: Some(MicroTime(
                chrono::Utc::now() - chrono::Duration::seconds(renewed_seconds_ago),
            )),
            ..Default::default()
        }
    }

    #[test]
    fn current_holder_renews_without_transition() {
        let now = chrono::Utc::now();
        assert_eq!(
            action_for_lease(
                Some(&spec("pod-a", 1, 3)),
                "pod-a",
                now,
                DEFAULT_LEASE_DURATION_SECONDS
            ),
            LeaseAction::Renew { transitions: 3 }
        );
    }

    #[test]
    fn waits_for_unexpired_other_holder() {
        let now = chrono::Utc::now();
        assert_eq!(
            action_for_lease(
                Some(&spec("pod-a", 1, 3)),
                "pod-b",
                now,
                DEFAULT_LEASE_DURATION_SECONDS
            ),
            LeaseAction::Wait {
                holder: "pod-a".to_string()
            }
        );
    }

    #[test]
    fn acquires_expired_lease_and_increments_transition() {
        let now = chrono::Utc::now();
        assert_eq!(
            action_for_lease(
                Some(&spec("pod-a", 30, 3)),
                "pod-b",
                now,
                DEFAULT_LEASE_DURATION_SECONDS
            ),
            LeaseAction::Acquire { transitions: 4 }
        );
    }

    #[test]
    fn acquires_lease_with_empty_holder_even_when_not_expired() {
        let now = chrono::Utc::now();
        assert_eq!(
            action_for_lease(
                Some(&spec("", 1, 3)),
                "pod-b",
                now,
                DEFAULT_LEASE_DURATION_SECONDS
            ),
            LeaseAction::Acquire { transitions: 3 }
        );
    }

    #[test]
    fn renewal_lease_preserves_original_acquire_time() {
        let now = chrono::Utc::now();
        let acquired = MicroTime(now - chrono::Duration::seconds(60));
        let lease = lease_object(
            "ksolver-scheduler",
            "pod-a",
            now,
            3,
            Some("rv-1".to_string()),
            Some(acquired.clone()),
        );
        let spec = lease.spec.expect("lease spec should be set");

        assert_eq!(spec.acquire_time, Some(acquired));
        assert_eq!(spec.renew_time, Some(MicroTime(now)));
        assert_eq!(spec.holder_identity.as_deref(), Some("pod-a"));
        assert_eq!(spec.lease_transitions, Some(3));
    }

    #[test]
    fn acquired_lease_uses_now_as_acquire_time() {
        let now = chrono::Utc::now();
        let lease = lease_object("ksolver-scheduler", "pod-a", now, 4, None, None);
        let spec = lease.spec.expect("lease spec should be set");

        assert_eq!(spec.acquire_time, Some(MicroTime(now)));
        assert_eq!(spec.renew_time, Some(MicroTime(now)));
        assert_eq!(spec.lease_transitions, Some(4));
    }
}
