from __future__ import annotations

from abc import ABC, abstractmethod

from ..domain.models import PlacementMove, WorkloadBatch


class DecisionTransport(ABC):
    @abstractmethod
    def validate(self) -> None:
        pass

    @abstractmethod
    def publish_telemetry(self, batch: WorkloadBatch) -> None:
        pass

    @abstractmethod
    def consume_moves(
        self, timeout_seconds: float = 1.0, max_messages: int = 1
    ) -> list[PlacementMove]:
        """Drain up to ``max_messages`` moves within ``timeout_seconds``.

        Returns an empty list both when nothing was waiting and after a
        transient timeout -- those are not distinguishable to the caller by
        design. A backend that cannot reach its broker must raise instead of
        returning ``[]``, so callers can tell "no moves" from "broker error".
        """

    def commit(self) -> None:  # noqa: B027 -- intentional no-op default hook
        """Acknowledge the moves most recently returned by ``consume_moves``.

        Called only after those moves have been (successfully or
        unsuccessfully-but-terminally) applied, so a crash between consume and
        apply leaves them redelivered rather than lost. Backends without a
        meaningful notion of offsets/acks can leave this as a no-op.
        """