from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ...config.settings import Settings

if TYPE_CHECKING:
    from docker import DockerClient
    from docker.tls import TLSConfig

log = logging.getLogger(__name__)


def _import_docker() -> Any:
    try:
        import docker
    except ImportError as exc:  # pragma: no cover - exercised without the extra
        raise ImportError(
            "The 'docker' action backend requires the docker SDK. Install it with "
            "`pip install hots-connector[docker]`."
        ) from exc
    return docker


def _build_tls(settings: Settings) -> TLSConfig | None:
    if not settings.docker_tls_verify:
        return None

    if not (
        settings.docker_tls_ca_cert
        and settings.docker_tls_client_cert
        and settings.docker_tls_client_key
    ):
        raise ValueError(
            "TLS verify enabled but missing cert paths. "
            "Set HOTSCONNECTOR_DOCKER_TLS_CA_CERT, HOTSCONNECTOR_DOCKER_TLS_CLIENT_CERT, "
            "HOTSCONNECTOR_DOCKER_TLS_CLIENT_KEY."
        )

    docker = _import_docker()
    return docker.tls.TLSConfig(
        ca_cert=settings.docker_tls_ca_cert,
        client_cert=(settings.docker_tls_client_cert, settings.docker_tls_client_key),
        verify=True,
    )


@dataclass(frozen=True)
class DockerHostRegistry:
    settings: Settings

    def get(self, host_id: str) -> DockerClient:
        hosts = self.settings.docker_hosts()
        base_url = hosts.get(host_id, None)

        if base_url is None:
            # Fall back to default host (useful for local dev)
            base_url = self.settings.docker_default_host
            log.warning(
                "Host id %r not found in docker_hosts_json; using default host %r",
                host_id,
                base_url,
            )

        tls = _build_tls(self.settings)
        docker = _import_docker()
        return docker.DockerClient(base_url=base_url, tls=tls)

    def ping(self, host_id: str) -> bool:
        try:
            self.get(host_id).ping()
            return True
        except Exception:
            log.exception("Docker ping failed for host %r", host_id)
            return False
