from __future__ import annotations

import logging

from ..actions.base import ActionBackend
from ..config.settings import Settings
from ..domain.models import ActionResult, PlacementMove

log = logging.getLogger(__name__)


class DockerActionBackend(ActionBackend):
    def __init__(self, *, settings: Settings) -> None:
        self.settings = settings
        try:
            from ..adapters.docker.migration import DockerMigrator
        except ImportError as exc:
            raise ImportError(
                "The 'docker' action backend requires the docker package. "
                "Install it with `pip install hots-connector[docker]`."
            ) from exc
        self.migrator = DockerMigrator.from_settings(settings)

    def validate(self) -> None:
        hosts = self.settings.docker_hosts()
        if hosts:
            for host_id in hosts:
                if not self.migrator.registry.ping(host_id):
                    raise RuntimeError(f"Docker host is unreachable: {host_id}")
        else:
            if not self.migrator.ping():
                raise RuntimeError("Default Docker host is unreachable")

    def apply_move(self, move: PlacementMove) -> ActionResult:
        try:
            self.migrator.apply(move)
            return ActionResult(
                workload_id=move.workload_id,
                success=True,
                metadata={"backend": "docker"},
            )
        except Exception as exc:
            log.exception("Docker move failed for %s", move.workload_id)
            return ActionResult(
                workload_id=move.workload_id,
                success=False,
                message=str(exc),
                metadata={"backend": "docker"},
            )
