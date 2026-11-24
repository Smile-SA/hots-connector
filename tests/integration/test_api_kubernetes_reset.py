from __future__ import annotations

from fastapi.testclient import TestClient

from hots_connector.api.app import create_app
from hots_connector.config.settings import Settings
from hots_connector.domain.models import ActionResult


def test_reset_node_selectors_not_supported_on_non_kubernetes_backend() -> None:
    # Default settings use action_backend=docker, which has no
    # restore_node_selectors() -- the endpoint must say so, not 500.
    app = create_app(settings=Settings())
    client = TestClient(app)

    r = client.post("/kubernetes/reset-node-selectors")

    assert r.status_code == 200
    assert r.json() == {"status": "not_supported"}


def test_reset_node_selectors_reports_ok_when_backend_supports_it() -> None:
    app = create_app(settings=Settings())

    class _FakeKubernetesActions:
        def restore_node_selectors(self) -> dict[str, ActionResult]:
            return {"web": ActionResult(workload_id="web", success=True)}

    app.state.service.actions = _FakeKubernetesActions()
    client = TestClient(app)

    r = client.post("/kubernetes/reset-node-selectors")

    assert r.status_code == 200
    assert r.json() == {"status": "ok", "restored": "web"}


def test_reset_node_selectors_reports_partial_failure() -> None:
    app = create_app(settings=Settings())

    class _FakeKubernetesActions:
        def restore_node_selectors(self) -> dict[str, ActionResult]:
            return {
                "web": ActionResult(workload_id="web", success=True),
                "cart": ActionResult(workload_id="cart", success=False, message="boom"),
            }

    app.state.service.actions = _FakeKubernetesActions()
    client = TestClient(app)

    r = client.post("/kubernetes/reset-node-selectors")

    assert r.status_code == 200
    assert r.json()["status"] == "partial_failure"
