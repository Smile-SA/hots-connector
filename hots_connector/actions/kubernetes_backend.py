from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

from ..config.settings import Settings
from ..domain.models import ActionResult, PlacementMove
from .base import ActionBackend

log = logging.getLogger(__name__)


class KubernetesActionBackend(ActionBackend):
    """v0 Kubernetes action backend: nodeSelector patch + pod eviction.

    Strategy: patch the owning Deployment's pod template with a nodeSelector
    pinning it to `target_host`, then evict the specific pod so its
    ReplicaSet recreates it -- landing on the target node because of the
    nodeSelector. This delegates the actual scheduling/bin-packing to
    Kubernetes; the connector only asserts *where* the next pod must land.
    No image copying, no config cloning -- the cluster already knows how to
    run the workload, which is what makes this a much smaller v0 than the
    Docker migrator.

    Identity mapping (settings.k8s_workload_identity) -- pin this per
    deployment, it is the likeliest source of integration bugs:
      - "pod" (default): `move.workload_id` is the pod name that was
        observed. It is resolved to its owning Deployment by walking
        ownerReferences (Pod -> ReplicaSet -> Deployment). This matches the
        raw telemetry identity (cAdvisor/kube-state-metrics key on `pod`),
        but is fragile if the pod was already replaced by something else
        between observation and the move arriving -- that surfaces as a
        clear "pod not found" failure rather than a silent no-op.
      - "controller": `move.workload_id` IS the Deployment name already.
        The backend then has to guess which of the Deployment's pods to
        evict (the first one not already on target_host) -- fine for the
        common single-replica benchmark case, ambiguous for replicas > 1.

    v1 (not implemented here): per-pod precision via
    Koordinator's PodMigrationJob CRD (already vendored in the benchmark
    platform) or the descheduler eviction API.

    Caveat: the nodeSelector patch is sticky -- it
    persists on the Deployment after the run ends and constrains future
    scheduling for anyone who doesn't know to look. This backend snapshots
    each Deployment's original nodeSelector the first time it patches it
    (in-memory, per process, and to disk when `k8s_node_selector_snapshot_path`
    is set) and exposes `restore_node_selectors()` to put it back; see the
    README's "nodeSelector is sticky" section for how to call it and its
    limitations (best-effort; in-memory-only state is lost on process
    restart unless a snapshot path is configured).
    """

    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings
        self._core: Any = None
        self._apps: Any = None
        self._original_node_selectors: dict[str, dict[str, str]] = {}

        # Load any snapshot left behind by a previous (crashed/restarted)
        # process, so restore_node_selectors() still covers Deployments it
        # patched.
        snapshot_path = settings.k8s_node_selector_snapshot_path
        if snapshot_path and os.path.exists(snapshot_path):
            try:
                with open(snapshot_path) as f:
                    self._original_node_selectors = json.load(f)
                log.info(
                    "Loaded nodeSelector snapshot from %s (%d deployments)",
                    snapshot_path,
                    len(self._original_node_selectors),
                )
            except Exception:
                log.exception(
                    "Failed to load nodeSelector snapshot from %s; starting fresh", snapshot_path
                )

    def _clients(self) -> tuple[Any, Any]:
        if self._core is None or self._apps is None:
            from ..adapters.kubernetes.client import build_k8s_clients

            self._core, self._apps = build_k8s_clients(self.settings)
        return self._core, self._apps

    def validate(self) -> None:
        core, apps = self._clients()
        core.list_namespace(limit=1)
        apps.list_namespaced_deployment(namespace=self.settings.k8s_namespace, limit=1)

    def apply_move(self, move: PlacementMove) -> ActionResult:
        try:
            self._apply_move(move)
            return ActionResult(
                workload_id=move.workload_id,
                success=True,
                metadata={"backend": "kubernetes"},
            )
        except Exception as exc:
            log.exception("Kubernetes move failed for %s", move.workload_id)
            return ActionResult(
                workload_id=move.workload_id,
                success=False,
                message=str(exc),
                metadata={"backend": "kubernetes"},
            )

    def _apply_move(self, move: PlacementMove) -> None:
        core, apps = self._clients()
        ns = self.settings.k8s_namespace

        deployment_name, pod_name = self._resolve_deployment_and_pod(move)
        self._patch_node_selector(apps, deployment_name, ns, move.target_host)

        if pod_name is None:
            log.warning(
                "No specific pod resolved for Deployment %r; nodeSelector patched but "
                "nothing evicted -- Kubernetes will only place the *next* recreation of "
                "this Deployment's pod(s) on %r.",
                deployment_name,
                move.target_host,
            )
            return

        self._evict_pod(core, pod_name, ns)
        self._wait_for_pod_on_target(core, apps, ns, deployment_name, move.target_host)

        log.info(
            "Migration complete: %s -> %s (%s, deployment=%s)",
            move.source_host,
            move.target_host,
            move.workload_id,
            deployment_name,
        )

    def _resolve_deployment_and_pod(self, move: PlacementMove) -> tuple[str, str | None]:
        core, apps = self._clients()
        ns = self.settings.k8s_namespace

        if self.settings.k8s_workload_identity == "controller":
            deployment_name = move.workload_id
            pod_name = self._find_pod_not_on_host(deployment_name, move.target_host)
            return deployment_name, pod_name

        # "pod" identity: workload_id is the pod name.
        pod_name = move.workload_id
        try:
            pod = core.read_namespaced_pod(name=pod_name, namespace=ns)
        except Exception as exc:
            raise RuntimeError(
                f"Pod {pod_name!r} not found in namespace {ns!r}; it may already have been "
                "recreated since this move was decided (see k8s_workload_identity in the README)"
            ) from exc

        deployment_name = self._deployment_name_from_pod(pod, ns)
        return deployment_name, pod_name

    def _deployment_name_from_pod(self, pod: Any, ns: str) -> str:
        _, apps = self._clients()
        owners = pod.metadata.owner_references or []
        rs_owner = next((o for o in owners if o.kind == "ReplicaSet"), None)
        if rs_owner is None:
            raise RuntimeError(
                f"Pod {pod.metadata.name!r} has no ReplicaSet owner; cannot resolve its "
                "Deployment (is it managed by something other than a Deployment?)"
            )

        rs = apps.read_namespaced_replica_set(name=rs_owner.name, namespace=ns)
        rs_owners = rs.metadata.owner_references or []
        deploy_owner = next((o for o in rs_owners if o.kind == "Deployment"), None)
        if deploy_owner is None:
            raise RuntimeError(f"ReplicaSet {rs_owner.name!r} has no Deployment owner")
        return str(deploy_owner.name)

    def _find_pod_not_on_host(self, deployment_name: str, target_host: str) -> str | None:
        core, apps = self._clients()
        ns = self.settings.k8s_namespace
        deployment = apps.read_namespaced_deployment(name=deployment_name, namespace=ns)
        selector = deployment.spec.selector.match_labels or {}
        label_selector = ",".join(f"{k}={v}" for k, v in selector.items())

        pods = core.list_namespaced_pod(namespace=ns, label_selector=label_selector).items
        candidates = [p for p in pods if (p.spec.node_name or "") != target_host]
        return candidates[0].metadata.name if candidates else None

    def _patch_node_selector(
        self, apps: Any, deployment_name: str, ns: str, target_host: str
    ) -> None:
        if deployment_name not in self._original_node_selectors:
            # Snapshot before the first patch this process makes to this
            # Deployment, so restore_node_selectors() can put it back later.
            current = apps.read_namespaced_deployment(name=deployment_name, namespace=ns)
            original = current.spec.template.spec.node_selector or {}
            self._original_node_selectors[deployment_name] = dict(original)
            self._persist_snapshot()

        patch = {
            "spec": {
                "template": {
                    "spec": {"nodeSelector": {self.settings.k8s_node_label_key: target_host}}
                }
            }
        }
        log.info(
            "Patching Deployment %r nodeSelector -> %s=%r",
            deployment_name,
            self.settings.k8s_node_label_key,
            target_host,
        )
        apps.patch_namespaced_deployment(name=deployment_name, namespace=ns, body=patch)

    def _persist_snapshot(self) -> None:
        # Written after every new Deployment is snapshotted (not just once
        # at the end) so a crash before restore_node_selectors() runs still
        # leaves an up-to-date snapshot on disk for the next process.
        snapshot_path = self.settings.k8s_node_selector_snapshot_path
        if not snapshot_path:
            return
        try:
            with open(snapshot_path, "w") as f:
                json.dump(self._original_node_selectors, f)
        except Exception:
            log.exception("Failed to write nodeSelector snapshot to %s", snapshot_path)

    def restore_node_selectors(self) -> dict[str, ActionResult]:
        """Best-effort: reset every Deployment this backend instance has
        patched back to the nodeSelector it had before the first move
        touched it (an empty dict if it had none).

        Tracked in-memory, and persisted to disk when
        `k8s_node_selector_snapshot_path` is set so restore still works after
        a process restart -- otherwise state is lost if the connector process
        restarts, and this only covers Deployments patched within the
        current process's lifetime. Not a durable audit trail; see the
        platform's provenance capture for that. Does not evict pods back --
        restoring the selector only affects where *future* recreations land.
        """
        _, apps = self._clients()
        ns = self.settings.k8s_namespace
        results: dict[str, ActionResult] = {}

        for deployment_name, original in self._original_node_selectors.items():
            try:
                patch = {"spec": {"template": {"spec": {"nodeSelector": original}}}}
                apps.patch_namespaced_deployment(name=deployment_name, namespace=ns, body=patch)
                log.info("Restored nodeSelector for Deployment %r to %r", deployment_name, original)
                results[deployment_name] = ActionResult(workload_id=deployment_name, success=True)
            except Exception as exc:
                log.exception("Failed to restore nodeSelector for %r", deployment_name)
                results[deployment_name] = ActionResult(
                    workload_id=deployment_name, success=False, message=str(exc)
                )

        snapshot_path = self.settings.k8s_node_selector_snapshot_path
        if snapshot_path and results and all(r.success for r in results.values()):
            try:
                os.remove(snapshot_path)
            except FileNotFoundError:
                pass
            except Exception:
                log.exception("Failed to remove nodeSelector snapshot at %s", snapshot_path)

        return results

    def _evict_pod(self, core: Any, pod_name: str, ns: str) -> None:
        from kubernetes.client import V1Eviction, V1ObjectMeta

        log.info("Evicting pod %r in namespace %r", pod_name, ns)
        eviction = V1Eviction(metadata=V1ObjectMeta(name=pod_name, namespace=ns))
        core.create_namespaced_pod_eviction(name=pod_name, namespace=ns, body=eviction)

    def _wait_for_pod_on_target(
        self, core: Any, apps: Any, ns: str, deployment_name: str, target_host: str
    ) -> None:
        deployment = apps.read_namespaced_deployment(name=deployment_name, namespace=ns)
        selector = deployment.spec.selector.match_labels or {}
        label_selector = ",".join(f"{k}={v}" for k, v in selector.items())

        deadline = time.monotonic() + self.settings.k8s_ready_timeout_seconds
        while time.monotonic() < deadline:
            pods = core.list_namespaced_pod(namespace=ns, label_selector=label_selector).items
            for pod in pods:
                on_target = (pod.spec.node_name or "") == target_host
                running = (pod.status.phase or "") == "Running"
                ready = all(c.ready for c in (pod.status.container_statuses or []))
                if on_target and running and ready:
                    return
            time.sleep(self.settings.k8s_poll_interval_seconds)

        raise RuntimeError(
            f"No Ready pod for Deployment {deployment_name!r} landed on node {target_host!r} "
            f"within {self.settings.k8s_ready_timeout_seconds}s"
        )
