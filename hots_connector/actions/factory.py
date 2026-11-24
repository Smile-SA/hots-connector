from __future__ import annotations

from ..config.settings import Settings
from .base import ActionBackend

# Backend modules are imported lazily, inside each branch below, rather than
# at module top level: importing this factory (which cli.py/api.app do
# unconditionally) must not require docker/kubernetes client libraries to be
# installed unless that specific backend is actually selected.


def build_action_backend(settings: Settings) -> ActionBackend:
    if settings.action_backend == "docker":
        from .docker_backend import DockerActionBackend

        return DockerActionBackend(settings=settings)

    if settings.action_backend == "kubernetes":
        from .kubernetes_backend import KubernetesActionBackend

        return KubernetesActionBackend(settings=settings)

    raise ValueError(f"Unsupported action backend: {settings.action_backend!r}")