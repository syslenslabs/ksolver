#!/usr/bin/env python3
"""Turn a VRAM resolution into a Kubernetes mutating-admission patch + a DRA claim.

- build_admission_patch(pod, resolution) -> JSONPatch ops:
    * always annotate the predicted peak + source + confidence,
    * at high/authoritative confidence, add a nodeAffinity that keeps the pod OFF GPUs
      smaller than the estimate (enforceable today, no DRA driver needed),
    * at advisory/unknown confidence, annotate advisory-only (never constrain on a guess).
- build_resource_claim_template(...) -> a DRA consumable-capacity ResourceClaimTemplate sized
  to the estimate (the DRA-native artifact; live-ready once a GPU DRA driver publishes devices).

The node label read for feasibility is `ksolver.dev/gpu-vram-gib` (integer GiB), matched with
the nodeAffinity `Gt` operator, mirroring the scheduler's own VRAM feasibility check.
"""
from __future__ import annotations

import base64
import json
import math
from typing import Any, Callable

NODE_VRAM_LABEL = "ksolver.dev/gpu-vram-gib"       # ksolver label, GiB
NODE_VRAM_LABEL_MIB = "nvidia.com/gpu.memory"      # NVIDIA GPU-feature-discovery label, MiB
DEFAULT_DEVICE_CLASS = "gpu.ksolver"


def _escape(key: str) -> str:
    # JSON Pointer escaping (RFC 6901): ~ -> ~0, / -> ~1
    return key.replace("~", "~0").replace("/", "~1")


def build_admission_patch(pod: dict[str, Any], resolution: dict[str, Any]) -> list[dict[str, Any]]:
    patches: list[dict[str, Any]] = []
    annotations = (pod.get("metadata") or {}).get("annotations")
    if not annotations:
        patches.append({"op": "add", "path": "/metadata/annotations", "value": {}})

    def set_ann(key: str, value: Any) -> None:
        patches.append({"op": "add", "path": f"/metadata/annotations/{_escape(key)}", "value": str(value)})

    set_ann("ksolver.dev/predicted-peak-vram-source", resolution.get("source"))
    set_ann("ksolver.dev/predicted-peak-vram-confidence", resolution.get("confidence"))
    if resolution.get("explanation"):
        set_ann("ksolver.dev/predicted-peak-vram-explanation", resolution["explanation"])
    vram_gib = resolution.get("vram_gib")
    if vram_gib is not None:
        set_ann("ksolver.dev/predicted-peak-vram-gib", vram_gib)

    if resolution.get("hard") and vram_gib is not None:
        # Require node per-GPU VRAM strictly greater than floor(estimate)-1 GiB, i.e. >= ceil.
        # Two OR'd terms so it matches the ksolver GiB label OR the NVIDIA GFD MiB label.
        floor_gib = max(0, math.floor(vram_gib) - 1)
        floor_mib = floor_gib * 1024
        patches.append(
            {
                "op": "add",
                "path": "/spec/affinity",
                "value": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {"matchExpressions": [
                                    {"key": NODE_VRAM_LABEL, "operator": "Gt", "values": [str(floor_gib)]}
                                ]},
                                {"matchExpressions": [
                                    {"key": NODE_VRAM_LABEL_MIB, "operator": "Gt", "values": [str(floor_mib)]}
                                ]},
                            ]
                        }
                    }
                },
            }
        )
    else:
        set_ann("ksolver.dev/predicted-peak-vram-advisory", "true")

    return patches


def _allow(uid: str, patches: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    response: dict[str, Any] = {"uid": uid, "allowed": True}
    if patches:
        response["patchType"] = "JSONPatch"
        response["patch"] = base64.b64encode(json.dumps(patches).encode()).decode()
    return {"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview", "response": response}


def render_admission_response(
    review: dict[str, Any],
    resolve_fn: Callable[[dict[str, Any]], dict[str, Any]],
) -> dict[str, Any]:
    """Turn an AdmissionReview request into a response with a VRAM-injection JSONPatch.

    FAILS OPEN: any error (bad pod, resolver failure) admits the pod unchanged — the estimator
    must never block workloads.
    """
    request = review.get("request") or {}
    uid = request.get("uid", "")
    try:
        pod = request.get("object") or {}
        if not ((pod.get("spec") or {}).get("containers")):
            return _allow(uid)
        resolution = resolve_fn(pod)
        patches = build_admission_patch(pod, resolution)
        return _allow(uid, patches)
    except Exception:  # noqa: BLE001 — fail open on anything
        return _allow(uid)


def build_resource_claim_pod_refs(
    pod: dict[str, Any], rct_name: str, claim_name: str = "ksolver-vram"
) -> list[dict[str, Any]]:
    """JSONPatch ops that make a pod REFERENCE a DRA ResourceClaimTemplate: add spec.resourceClaims
    and the GPU container's resources.claims. Apply AFTER the ResourceClaimTemplate exists (else the
    pod references a missing claim). Pairs with build_resource_claim_template."""
    ops: list[dict[str, Any]] = []
    spec = pod.get("spec") or {}
    claim_ref = {"name": claim_name, "resourceClaimTemplateName": rct_name}
    if spec.get("resourceClaims"):
        ops.append({"op": "add", "path": "/spec/resourceClaims/-", "value": claim_ref})
    else:
        ops.append({"op": "add", "path": "/spec/resourceClaims", "value": [claim_ref]})

    containers = spec.get("containers") or []
    idx = 0
    for i, c in enumerate(containers):
        resources = c.get("resources") or {}
        keys = list((resources.get("requests") or {})) + list((resources.get("limits") or {}))
        if any("gpu" in k.lower() for k in keys):
            idx = i
            break
    c = containers[idx] if containers else {}
    resources = c.get("resources") or {}
    if not c.get("resources"):
        ops.append({"op": "add", "path": f"/spec/containers/{idx}/resources", "value": {"claims": [{"name": claim_name}]}})
    elif not resources.get("claims"):
        ops.append({"op": "add", "path": f"/spec/containers/{idx}/resources/claims", "value": [{"name": claim_name}]})
    else:
        ops.append({"op": "add", "path": f"/spec/containers/{idx}/resources/claims/-", "value": {"name": claim_name}})
    return ops


# DRA consumable-capacity memory claims are a GA (resource.k8s.io/v1, k8s 1.34+) feature. Older DRA
# API versions (v1alpha3 in 1.31, v1beta1 in 1.32/1.33) don't express per-request consumable
# capacity, so we do NOT emit a DRA claim there — the node-affinity feasibility injection
# (build_admission_patch) already enforces "keep off too-small GPUs" on every version.
GA_DRA_API_VERSION = "resource.k8s.io/v1"


def build_resource_claim_template(
    namespace: str,
    name: str,
    vram_gib: float,
    device_class: str = DEFAULT_DEVICE_CLASS,
    api_version: str = GA_DRA_API_VERSION,
) -> dict[str, Any] | None:
    """A DRA ResourceClaimTemplate requesting GPU memory as consumable capacity, sized to the
    estimate. Returns None for non-GA DRA versions (consumable capacity isn't available there) so the
    caller degrades to the node-affinity feasibility path instead of emitting an unsupported claim."""
    if api_version != GA_DRA_API_VERSION:
        return None
    gib = int(math.ceil(vram_gib))
    return {
        "apiVersion": api_version,
        "kind": "ResourceClaimTemplate",
        "metadata": {"name": name, "namespace": namespace},
        "spec": {
            "spec": {
                "devices": {
                    "requests": [
                        {
                            "name": "gpu",
                            "exactly": {
                                "deviceClassName": device_class,
                                "capacity": {"requests": {"memory": f"{gib}Gi"}},
                            },
                        }
                    ]
                }
            }
        },
    }
