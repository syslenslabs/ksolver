//! Online GPU scheduler components. Shadow mode observes and computes placement decisions and, by
//! default, binds nothing. Phase 3 (real binding) is OPT-IN (`ShadowConfig.enable_real_binding`,
//! default off) and isolated entirely in `binder.rs` — the single sanctioned, gated mutation site.
//! `shadow.rs` orchestrates but must never call a cluster-mutating API directly; `binding.rs` is a
//! pure renderer. Both invariants are enforced by `no_mutation_guard` below.

pub mod bench;
pub mod binder;
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
    // `shadow.rs` orchestrates but must never DIRECTLY call a cluster-mutating API — all mutation
    // is isolated in `binder.rs` and gated behind `enable_real_binding` (default off). We grep for
    // concrete mutator signatures (not the word "Binding", which now legitimately appears via the
    // lowercase `binder`/`apply_bindings` orchestration).
    const SHADOW: &str = include_str!("shadow.rs");
    const BINDING: &str = include_str!("binding.rs");

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
}
