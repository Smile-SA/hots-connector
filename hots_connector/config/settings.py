from __future__ import annotations

import json
import os
from typing import cast

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="HOTSCONNECTOR_", extra="ignore", env_file=".env")

    log_level: str = Field(default="INFO")

    observation_backend: str = Field(default="prometheus_docker")
    transport_backend: str = Field(default="kafka")
    action_backend: str = Field(default="docker")

    prometheus_url: str = Field(default="http://localhost:9090")

    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_topic_data: str = Field(default="hots_data")
    kafka_topic_moves: str = Field(default="hots_moves")
    kafka_group_id: str = Field(default="hots_connector")
    # Optional: malformed moves are published here (raw bytes) before the
    # offset is committed past them, instead of being silently dropped.
    kafka_topic_moves_dlq: str | None = Field(default=None)
    # Max moves drained per `consume_moves` call (replaces the old
    # one-message-per-poll + busy-loop pattern).
    moves_batch_size: int = Field(default=50)

    publish_interval_seconds: int = Field(default=10)

    # Prometheus label/query defaults are environment-profile-dependent (see
    # PrometheusObservationBackend); leave unset to use the profile's default
    # for prometheus_docker vs prometheus_kubernetes, or override explicitly.
    prom_workload_label: str | None = Field(default=None)
    prom_host_label: str | None = Field(default=None)
    prom_query_mem_mb: str | None = Field(default=None)
    prom_query_cpu: str | None = Field(default=None)

    # Kubernetes action backend
    k8s_kubeconfig_path: str | None = Field(
        default=None, description="Path to kubeconfig; unset means in-cluster config."
    )
    k8s_context: str | None = Field(default=None)
    k8s_namespace: str = Field(default="default")
    # "pod" (workload_id is the pod name, matching the raw telemetry identity)
    # or "controller" (workload_id names the owning Deployment directly).
    # See README "identity mapping" and KubernetesActionBackend's docstring.
    k8s_workload_identity: str = Field(default="pod")
    k8s_node_label_key: str = Field(default="kubernetes.io/hostname")
    k8s_ready_timeout_seconds: int = Field(default=120)
    k8s_poll_interval_seconds: float = Field(default=2.0)
    # Where KubernetesActionBackend persists its nodeSelector snapshot so
    # restore_node_selectors() still works after a process restart/crash.
    # Unset means in-memory only (lost on restart, see README "nodeSelector
    # is sticky").
    k8s_node_selector_snapshot_path: str | None = Field(default=None)

    docker_hosts_json: str = Field(
        default="{}",
        description='JSON dict: {"hostA":"tcp://192.0.2.10:2375","hostB":"tcp://192.0.2.11:2375"}',
    )

    docker_default_host: str = Field(default="unix:///var/run/docker.sock")
    docker_tls_verify: bool = Field(default=False)
    docker_tls_ca_cert: str | None = Field(default=None)
    docker_tls_client_cert: str | None = Field(default=None)
    docker_tls_client_key: str | None = Field(default=None)

    migration_strategy: str = Field(default="recreate")
    migration_stop_timeout_seconds: int = Field(default=10)
    migration_remove_source: bool = Field(default=True)
    migration_keep_name: bool = Field(default=True)
    # Log the migration that would happen (image, ports, volumes, restart
    # policy, hosts) without touching Docker. Lets a whole HOTS run be
    # validated against `check`-style dry runs before any container moves.
    migration_dry_run: bool = Field(default=False)
    migration_ready_timeout_seconds: int = Field(default=30)
    migration_ready_poll_seconds: float = Field(default=1.0)

    @field_validator("docker_hosts_json")
    @classmethod
    def _validate_hosts_json(cls, v: str) -> str:
        v = v.strip()
        if not v:
            return "{}"
        json.loads(v)
        return v

    def docker_hosts(self) -> dict[str, str]:
        return cast(dict[str, str], json.loads(self.docker_hosts_json))


def load_settings() -> Settings:
    """Settings for the CLI: environment variables, plus the dotenv file named
    by HOTSCONNECTOR_ENV_FILE (default `.env` in the working directory). An
    empty HOTSCONNECTOR_ENV_FILE reads no file, so a caller that sets every
    variable itself can't pick up a stray `.env` from wherever it runs."""
    env_file = os.environ.get("HOTSCONNECTOR_ENV_FILE", ".env")
    # `_env_file` is BaseSettings' documented per-instance override; mypy only
    # sees the field-derived __init__ signature.
    return Settings(_env_file=env_file or None)  # type: ignore[call-arg]