//! Optional Kubernetes Event emission for scheduler auditability.
//!
//! Rendering lives in `events.rs`; this module is the explicit, gated mutation surface that POSTs
//! those rendered drafts to the Kubernetes Events API.

use crate::scheduler::events::{event_from_draft, EventDraft};
use kube::{api::PostParams, Api, Client};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EventEmitStats {
    pub attempted: usize,
    pub created: usize,
    pub failed: usize,
}

fn body_string<'a>(body: &'a serde_json::Value, path: &[&str]) -> Option<&'a str> {
    let mut current = body;
    for key in path {
        current = current.get(*key)?;
    }
    current.as_str()
}

fn draft_identity_matches_body(draft: &EventDraft) -> bool {
    let namespace_and_name_match = body_string(&draft.body, &["metadata", "namespace"])
        .is_some_and(|namespace| namespace == draft.namespace)
        && body_string(&draft.body, &["regarding", "namespace"])
            .is_some_and(|namespace| namespace == draft.namespace)
        && body_string(&draft.body, &["regarding", "name"]).is_some_and(|pod| pod == draft.pod);
    if !namespace_and_name_match {
        return false;
    }
    draft.pod_uid.trim().is_empty()
        || body_string(&draft.body, &["regarding", "uid"]).is_some_and(|uid| uid == draft.pod_uid)
}

/// POST rendered Event drafts to the Kubernetes Events API.
///
/// This is intentionally best-effort: one malformed or rejected Event increments `failed` and does
/// not abort the batch. Callers must guard this with `ShadowConfig::kubernetes_event_writes_enabled`
/// before constructing or passing a mutation-capable client.
pub async fn emit_event_drafts(client: &Client, drafts: &[EventDraft]) -> EventEmitStats {
    let mut stats = EventEmitStats {
        attempted: drafts.len(),
        ..Default::default()
    };
    for draft in drafts {
        if !draft_identity_matches_body(draft) {
            stats.failed += 1;
            continue;
        }
        let Ok(event) = event_from_draft(draft) else {
            stats.failed += 1;
            continue;
        };
        let api: Api<k8s_openapi::api::events::v1::Event> =
            Api::namespaced(client.clone(), &draft.namespace);
        match api.create(&PostParams::default(), &event).await {
            Ok(_) => stats.created += 1,
            Err(_) => stats.failed += 1,
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    fn draft(
        wrapper_ns: &str,
        body_ns: &str,
        regarding_ns: &str,
        regarding_name: &str,
    ) -> EventDraft {
        EventDraft {
            namespace: wrapper_ns.to_string(),
            pod: "train-a".to_string(),
            pod_uid: "uid-a".to_string(),
            team: "research".to_string(),
            reason: "KsolverPlacementRecommended".to_string(),
            type_: "Normal".to_string(),
            note: "note".to_string(),
            body: serde_json::json!({
                "apiVersion": "events.k8s.io/v1",
                "kind": "Event",
                "metadata": {
                    "namespace": body_ns,
                    "generateName": "train-a-ksolver-",
                },
                "regarding": {
                    "apiVersion": "v1",
                    "kind": "Pod",
                    "namespace": regarding_ns,
                    "name": regarding_name,
                    "uid": "uid-a",
                },
                "reason": "KsolverPlacementRecommended",
                "note": "note",
                "type": "Normal",
                "action": "PlacementRecommended",
                "eventTime": "2026-07-02T12:00:00Z",
                "reportingController": "ksolver.dev/scheduler",
                "reportingInstance": "ksolver",
            }),
        }
    }

    #[test]
    fn draft_identity_must_match_event_body_identity() {
        assert!(draft_identity_matches_body(&draft(
            "team-a", "team-a", "team-a", "train-a"
        )));
        assert!(!draft_identity_matches_body(&draft(
            "team-a", "team-b", "team-a", "train-a"
        )));
        assert!(!draft_identity_matches_body(&draft(
            "team-a", "team-a", "team-b", "train-a"
        )));
        assert!(!draft_identity_matches_body(&draft(
            "team-a", "team-a", "team-a", "train-b"
        )));
        let mut wrong_uid = draft("team-a", "team-a", "team-a", "train-a");
        wrong_uid.body["regarding"]["uid"] = serde_json::json!("uid-b");
        assert!(!draft_identity_matches_body(&wrong_uid));

        let mut legacy_without_uid = draft("team-a", "team-a", "team-a", "train-a");
        legacy_without_uid.pod_uid.clear();
        legacy_without_uid.body["regarding"]
            .as_object_mut()
            .expect("regarding object")
            .remove("uid");
        assert!(draft_identity_matches_body(&legacy_without_uid));
    }
}
