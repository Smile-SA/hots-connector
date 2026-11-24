from __future__ import annotations

from ..config.settings import Settings
from .base import ObservationBackend
from .prometheus import PrometheusObservationBackend


def build_observation_backend(settings: Settings) -> ObservationBackend:
    if settings.observation_backend == "prometheus_docker":
        return PrometheusObservationBackend(settings=settings, profile="docker")

    if settings.observation_backend == "prometheus_kubernetes":
        return PrometheusObservationBackend(settings=settings, profile="kubernetes")

    raise ValueError(f"Unsupported observation backend: {settings.observation_backend!r}")