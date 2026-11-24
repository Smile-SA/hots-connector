"""Unit tests for ConnectorService's move-handling and lifecycle behavior,
using minimal fakes for the three backend ABCs (no infrastructure needed)."""

from __future__ import annotations

from hots_connector.domain.models import ActionResult, PlacementMove, WorkloadBatch
from hots_connector.services.connector_service import ConnectorService


class _FakeObservation:
    def validate(self) -> None:
        pass

    def collect(self) -> WorkloadBatch:
        return WorkloadBatch(timestamp=0, samples=[])


class _FakeTransport:
    def __init__(self, moves: list[PlacementMove]) -> None:
        self._moves = moves
        self.commit_calls = 0
        self.published: list[WorkloadBatch] = []

    def validate(self) -> None:
        pass

    def publish_telemetry(self, batch: WorkloadBatch) -> None:
        self.published.append(batch)

    def consume_moves(
        self, timeout_seconds: float = 1.0, max_messages: int = 1
    ) -> list[PlacementMove]:
        moves, self._moves = self._moves[:max_messages], self._moves[max_messages:]
        return moves

    def commit(self) -> None:
        self.commit_calls += 1


class _FakeActions:
    def __init__(self, *, fail: bool = False) -> None:
        self.fail = fail
        self.applied: list[PlacementMove] = []

    def validate(self) -> None:
        pass

    def apply_move(self, move: PlacementMove) -> ActionResult:
        raise NotImplementedError

    def apply_moves(self, moves: list[PlacementMove]) -> list[ActionResult]:
        self.applied.extend(moves)
        return [
            ActionResult(
                workload_id=m.workload_id,
                success=not self.fail,
                message="boom" if self.fail else "",
            )
            for m in moves
        ]


def _move(workload_id: str = "c_1") -> PlacementMove:
    return PlacementMove(workload_id=workload_id, source_host="n_1", target_host="n_2")


def test_apply_pending_moves_commits_after_successful_apply() -> None:
    transport = _FakeTransport([_move()])
    service = ConnectorService(
        observation=_FakeObservation(),
        transport=transport,
        actions=_FakeActions(),
        publish_interval_seconds=10,
    )

    count = service.apply_pending_moves()

    assert count == 1
    assert transport.commit_calls == 1


def test_apply_pending_moves_commits_even_when_apply_fails() -> None:
    """A failed *apply* (e.g. an unreachable Docker host) still commits: it
    will fail identically on redelivery, so retrying it forever would just
    wedge the topic instead of recovering anything. See ConnectorService
    docstring/comment for the reasoning."""
    transport = _FakeTransport([_move()])
    service = ConnectorService(
        observation=_FakeObservation(),
        transport=transport,
        actions=_FakeActions(fail=True),
        publish_interval_seconds=10,
    )

    count = service.apply_pending_moves()

    assert count == 1
    assert transport.commit_calls == 1


def test_apply_pending_moves_does_not_commit_when_nothing_to_apply() -> None:
    transport = _FakeTransport([])
    service = ConnectorService(
        observation=_FakeObservation(),
        transport=transport,
        actions=_FakeActions(),
        publish_interval_seconds=10,
    )

    assert service.apply_pending_moves() == 0
    assert transport.commit_calls == 0


def test_stop_clears_is_running_without_needing_a_running_loop() -> None:
    service = ConnectorService(
        observation=_FakeObservation(),
        transport=_FakeTransport([]),
        actions=_FakeActions(),
        publish_interval_seconds=10,
    )

    assert service.is_running is False
    service.stop()
    assert service.is_running is False
