from __future__ import annotations

from typing import Any

from ...config.settings import Settings


def build_k8s_clients(settings: Settings) -> tuple[Any, Any]:
    """Build (CoreV1Api, AppsV1Api) clients from connector settings.

    Uses in-cluster config when `k8s_kubeconfig_path` is unset (the normal
    case when the connector runs as a pod inside the benchmark platform's
    cluster); otherwise loads the given kubeconfig file, optionally selecting
    a context (useful for local dev against a Kind cluster).
    """
    try:
        from kubernetes import client, config
    except ImportError as exc:  # pragma: no cover - exercised without the extra
        raise ImportError(
            "The 'kubernetes' action backend requires the kubernetes client. "
            "Install it with `pip install hots-connector[kubernetes]`."
        ) from exc

    if settings.k8s_kubeconfig_path:
        config.load_kube_config(
            config_file=settings.k8s_kubeconfig_path,
            context=settings.k8s_context,
        )
    else:
        config.load_incluster_config()

    return client.CoreV1Api(), client.AppsV1Api()
