//! Online GPU scheduler components. Phase 1 = shadow mode only:
//! observe and compute placement decisions; never bind pods.

pub mod bench;
pub mod binding;
pub mod config;
pub mod decision;
pub mod pending_input;
pub mod pod_filter;
pub mod shadow;
pub mod trace;
pub mod watch_state;

#[cfg(test)]
mod no_mutation_guard {
    // This source must never call cluster-mutating APIs in Phase 1 (shadow mode).
    const SHADOW: &str = include_str!("shadow.rs");
    const BINDING: &str = include_str!("binding.rs");

    #[test]
    fn shadow_has_no_binding_or_mutation_calls() {
        for needle in [
            "Binding",
            ".evict(",
            ".create(",
            ".replace(",
            ".patch(",
            ".delete(",
        ] {
            assert!(
                !SHADOW.contains(needle),
                "shadow.rs must not contain `{needle}` in Phase 1"
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
}
