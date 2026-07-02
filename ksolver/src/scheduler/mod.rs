//! Online GPU scheduler components. Shadow mode observes and computes placement decisions and, by
//! default, binds nothing. Phase 3 (real binding) is OPT-IN (`ShadowConfig.enable_real_binding`,
//! default off), fail-closed by `binding_kill_switch`, and isolated entirely in `binder.rs`.
//! Optional Kubernetes Event writes are separately gated and isolated in `event_emitter.rs`.
//! Optional Lease-based leader election is isolated in `leader.rs`.
//! `shadow.rs` orchestrates but must never call a cluster-mutating API directly; `binding.rs` is a
//! pure renderers. These invariants are enforced by `no_mutation_guard` below.

pub mod admission;
pub mod bench;
pub mod binder;
pub mod binding;
pub mod config;
pub mod decision;
pub mod event_emitter;
pub mod events;
pub mod gpu_scenarios;
pub mod leader;
pub mod ledger;
pub mod observations;
pub mod pending_input;
pub mod pod_filter;
pub mod prediction;
pub mod repair;
pub mod shadow;
pub mod trace;
pub mod watch_state;

#[cfg(test)]
mod no_mutation_guard {
    // `shadow.rs` orchestrates but must never DIRECTLY call a cluster-mutating API — all mutation
    // is isolated in `binder.rs` and gated behind `enable_real_binding` plus the kill switch. We grep for
    // concrete mutator signatures (not the word "Binding", which now legitimately appears via the
    // lowercase `binder`/`apply_bindings` orchestration).
    const SHADOW: &str = include_str!("shadow.rs");
    const ADMISSION: &str = include_str!("admission.rs");
    const BINDING: &str = include_str!("binding.rs");
    const EVENT_EMITTER: &str = include_str!("event_emitter.rs");
    const EVENTS: &str = include_str!("events.rs");
    const LEADER: &str = include_str!("leader.rs");
    const REPAIR: &str = include_str!("repair.rs");

    #[test]
    fn shadow_has_no_direct_mutation_calls() {
        for needle in [
            "create_subresource",
            "PostParams",
            "DeleteParams",
            "PatchParams",
            ".evict(",
            ".create(",
            ".replace(",
            ".patch(",
            ".delete(",
        ] {
            assert!(
                !SHADOW.contains(needle),
                "shadow.rs must not directly call `{needle}` — mutation belongs in binder.rs"
            );
        }
    }

    #[test]
    fn binding_renderer_never_mutates_or_calls_api() {
        // The dry-run renderer builds payloads only; it must never POST them or touch a kube
        // client. Unambiguous API-call / kube-path signatures (prose-collidable needles like
        // "Client"/"Api<" are intentionally excluded; comments here stay free of "kube::").
        for needle in [
            ".evict(",
            ".create(",
            ".replace(",
            ".patch(",
            ".delete(",
            ".request(",
            "PostParams",
            "DeleteParams",
            "PatchParams",
            "EvictParams",
            "kube::",
        ] {
            assert!(
                !BINDING.contains(needle),
                "binding.rs must render only, never call `{needle}`"
            );
        }
    }

    #[test]
    fn admission_patch_renderer_never_mutates_or_calls_api() {
        for needle in [
            ".evict(",
            ".create(",
            ".replace(",
            ".patch(",
            ".delete(",
            ".request(",
            "create_subresource",
            "PostParams",
            "DeleteParams",
            "PatchParams",
            "EvictParams",
            "kube::",
        ] {
            assert!(
                !ADMISSION.contains(needle),
                "admission.rs must render only, never call `{needle}`"
            );
        }
    }

    #[test]
    fn event_renderer_never_mutates_or_calls_api() {
        for needle in [
            ".evict(",
            ".create(",
            ".replace(",
            ".patch(",
            ".delete(",
            ".request(",
            "create_subresource",
            "PostParams",
            "DeleteParams",
            "PatchParams",
            "EvictParams",
            "kube::",
        ] {
            assert!(
                !EVENTS.contains(needle),
                "events.rs must render only, never call `{needle}`"
            );
        }
    }

    #[test]
    fn repair_advisor_never_mutates_or_calls_api() {
        for needle in [
            ".evict(",
            ".create(",
            ".replace(",
            ".patch(",
            ".delete(",
            ".request(",
            "create_subresource",
            "PostParams",
            "DeleteParams",
            "PatchParams",
            "EvictParams",
            "kube::",
            "api::core::v1::Pod",
            "api::policy::v1::Eviction",
        ] {
            assert!(
                !REPAIR.contains(needle),
                "repair.rs must render advisory plans only, never call `{needle}`"
            );
        }
    }

    #[test]
    fn event_emitter_only_creates_events() {
        for forbidden in [
            ".evict(",
            ".replace(",
            ".patch(",
            ".delete(",
            ".request(",
            "create_subresource",
            "DeleteParams",
            "PatchParams",
            "EvictParams",
            "pods/binding",
        ] {
            assert!(
                !EVENT_EMITTER.contains(forbidden),
                "event_emitter.rs may create Events only, never call `{forbidden}`"
            );
        }
        assert!(
            EVENT_EMITTER.contains("api::events::v1::Event"),
            "event_emitter.rs should target only Kubernetes Event objects"
        );
    }

    #[test]
    fn leader_elector_only_mutates_leases() {
        for forbidden in [
            ".evict(",
            ".patch(",
            ".delete(",
            ".request(",
            "create_subresource",
            "DeleteParams",
            "PatchParams",
            "EvictParams",
            "pods/binding",
            "api::core::v1::Pod",
            "api::events::v1::Event",
        ] {
            assert!(
                !LEADER.contains(forbidden),
                "leader.rs may create/replace Leases only, never call `{forbidden}`"
            );
        }
        assert!(
            LEADER.contains("api::coordination::v1::{Lease, LeaseSpec}"),
            "leader.rs should target only Kubernetes Lease objects"
        );
    }
}
