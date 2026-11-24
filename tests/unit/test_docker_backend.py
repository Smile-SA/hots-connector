"""Unit test for DockerActionBackend's handling of an initial placement
(source_host=None), which the 'recreate' migration strategy cannot service."""

from __future__ import annotations

from hots_connector.actions.docker_backend import DockerActionBackend
from hots_connector.config.settings import Settings
from hots_connector.domain.models import PlacementMove


def test_apply_move_with_no_source_host_fails_clearly_instead_of_crashing() -> None:
    backend = DockerActionBackend(settings=Settings())
    move = PlacementMove(workload_id="c_1", source_host=None, target_host="n_2")

    result = backend.apply_move(move)

    assert result.success is False
    assert "source_host" in result.message or "initial placement" in result.message
