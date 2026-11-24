from __future__ import annotations

import threading

from fastapi import APIRouter, Request

from ..services.connector_service import ConnectorService

router = APIRouter()

# Guards start/stop against two rapid `/stream/start` calls racing past the
# is_running check and starting two loops. Not per-app (FastAPI routers are
# shared across app instances), but this process only ever runs one app.
_lifecycle_lock = threading.Lock()


def _service(req: Request) -> ConnectorService:
    # FastAPI/Starlette's app.state is a dynamic bag of attributes (typed as
    # Any); annotating the return here gives mypy a real type to check
    # against instead of Any leaking into every route.
    service: ConnectorService = req.app.state.service
    return service


@router.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/check")
def check(req: Request) -> dict[str, str]:
    return _service(req).validate()


@router.post("/stream/start")
def start_stream(req: Request) -> dict[str, str]:
    svc = _service(req)

    with _lifecycle_lock:
        thread: threading.Thread | None = getattr(req.app.state, "stream_thread", None)
        if thread is not None and thread.is_alive():
            return {"status": "already_running"}

        thread = threading.Thread(target=svc.run_forever, daemon=True)
        req.app.state.stream_thread = thread
        thread.start()

    return {"status": "started"}


@router.post("/stream/stop")
def stop_stream(req: Request) -> dict[str, str]:
    svc = _service(req)
    svc.stop()

    thread: threading.Thread | None = getattr(req.app.state, "stream_thread", None)
    if thread is not None:
        # Bounded join: don't hang the request forever if a tick is stuck
        # (e.g. blocked on a slow broker call), but give the common case
        # (finishing the current tick) a real chance to complete first.
        thread.join(timeout=5.0)
        if thread.is_alive():
            return {"status": "stopping"}

    return {"status": "stopped"}


@router.post("/kubernetes/reset-node-selectors")
def reset_kubernetes_node_selectors(req: Request) -> dict[str, str]:
    """Undo the nodeSelector patches KubernetesActionBackend has made so far
    this process, restoring each touched Deployment to its pre-run selector.

    A no-op {"status": "not_supported"} on any other action backend -- this
    is specific to the Kubernetes v0 strategy's known "sticky nodeSelector"
    side effect (see README).
    """
    actions = _service(req).actions
    restore = getattr(actions, "restore_node_selectors", None)
    if restore is None:
        return {"status": "not_supported"}

    results = restore()
    if all(r.success for r in results.values()):
        return {"status": "ok", "restored": ",".join(results)}
    return {"status": "partial_failure", "restored": ",".join(results)}