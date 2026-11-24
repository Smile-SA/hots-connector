"""Unit tests for KubernetesActionBackend's nodeSelector snapshot/restore
behavior (README: "nodeSelector is sticky"), using a
fake AppsV1Api so these run without the 'kubernetes' extra or a live cluster."""

from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from hots_connector.actions.kubernetes_backend import KubernetesActionBackend
from hots_connector.config.settings import Settings


def _deployment(node_selector: dict[str, str] | None = None) -> Any:
    return SimpleNamespace(
        spec=SimpleNamespace(
            template=SimpleNamespace(spec=SimpleNamespace(node_selector=node_selector)),
            selector=SimpleNamespace(match_labels={"app": "web"}),
        )
    )


class _FakeApps:
    def __init__(self, deployments: dict[str, Any]) -> None:
        self._deployments = deployments
        self.patches: list[tuple[str, dict]] = []

    def read_namespaced_deployment(self, *, name: str, namespace: str) -> Any:
        return self._deployments[name]

    def patch_namespaced_deployment(self, *, name: str, namespace: str, body: dict) -> None:
        self.patches.append((name, body))
        selector = body["spec"]["template"]["spec"]["nodeSelector"]
        self._deployments[name] = _deployment(selector)


def _backend(deployments: dict[str, Any]) -> tuple[KubernetesActionBackend, _FakeApps]:
    backend = KubernetesActionBackend(settings=Settings())
    apps = _FakeApps(deployments)
    backend._core = object()  # bypass the lazy client import; unused by these tests
    backend._apps = apps
    return backend, apps


def test_patch_node_selector_snapshots_the_original_before_first_patch() -> None:
    backend, apps = _backend({"web": _deployment({"zone": "a"})})

    backend._patch_node_selector(apps, "web", "default", "node-2")

    assert backend._original_node_selectors == {"web": {"zone": "a"}}

    # A second move against the same Deployment must not overwrite the
    # snapshot with the now-patched value -- we want the *pre-run* original.
    backend._patch_node_selector(apps, "web", "default", "node-3")
    assert backend._original_node_selectors == {"web": {"zone": "a"}}


def test_patch_node_selector_snapshots_empty_dict_when_none_was_set() -> None:
    backend, apps = _backend({"web": _deployment(None)})

    backend._patch_node_selector(apps, "web", "default", "node-2")

    assert backend._original_node_selectors == {"web": {}}


def test_restore_node_selectors_puts_back_the_snapshot() -> None:
    backend, apps = _backend({"web": _deployment({"zone": "a"})})
    backend._patch_node_selector(apps, "web", "default", "node-2")

    results = backend.restore_node_selectors()

    assert results["web"].success is True
    assert apps.patches[-1] == (
        "web",
        {"spec": {"template": {"spec": {"nodeSelector": {"zone": "a"}}}}},
    )


def test_restore_node_selectors_reports_failure_without_raising() -> None:
    backend, apps = _backend({"web": _deployment({"zone": "a"})})
    backend._patch_node_selector(apps, "web", "default", "node-2")

    def _boom(**kwargs: Any) -> None:
        raise RuntimeError("api down")

    apps.patch_namespaced_deployment = _boom  # type: ignore[method-assign]

    results = backend.restore_node_selectors()

    assert results["web"].success is False
    assert "api down" in results["web"].message


def test_restore_node_selectors_is_empty_when_nothing_was_patched() -> None:
    backend, _ = _backend({})
    assert backend.restore_node_selectors() == {}


def test_patch_node_selector_persists_snapshot_to_disk_when_path_configured(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    backend = KubernetesActionBackend(
        settings=Settings(k8s_node_selector_snapshot_path=str(snapshot_path))
    )
    apps = _FakeApps({"web": _deployment({"zone": "a"})})
    backend._core = object()
    backend._apps = apps

    backend._patch_node_selector(apps, "web", "default", "node-2")

    assert json.loads(snapshot_path.read_text()) == {"web": {"zone": "a"}}


def test_backend_loads_snapshot_left_by_a_previous_process(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    snapshot_path.write_text(json.dumps({"web": {"zone": "a"}}))

    backend = KubernetesActionBackend(
        settings=Settings(k8s_node_selector_snapshot_path=str(snapshot_path))
    )

    assert backend._original_node_selectors == {"web": {"zone": "a"}}


def test_restore_node_selectors_removes_snapshot_file_on_full_success(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    backend = KubernetesActionBackend(
        settings=Settings(k8s_node_selector_snapshot_path=str(snapshot_path))
    )
    apps = _FakeApps({"web": _deployment({"zone": "a"})})
    backend._core = object()
    backend._apps = apps
    backend._patch_node_selector(apps, "web", "default", "node-2")
    assert snapshot_path.exists()

    results = backend.restore_node_selectors()

    assert results["web"].success is True
    assert not snapshot_path.exists()


def test_restore_node_selectors_keeps_snapshot_file_on_partial_failure(
    tmp_path: Path,
) -> None:
    snapshot_path = tmp_path / "snapshot.json"
    backend = KubernetesActionBackend(
        settings=Settings(k8s_node_selector_snapshot_path=str(snapshot_path))
    )
    apps = _FakeApps({"web": _deployment({"zone": "a"})})
    backend._core = object()
    backend._apps = apps
    backend._patch_node_selector(apps, "web", "default", "node-2")

    def _boom(**kwargs: Any) -> None:
        raise RuntimeError("api down")

    apps.patch_namespaced_deployment = _boom  # type: ignore[method-assign]

    results = backend.restore_node_selectors()

    assert results["web"].success is False
    assert snapshot_path.exists()
