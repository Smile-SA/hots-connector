from __future__ import annotations

from abc import ABC, abstractmethod

from ..domain.models import ActionResult, PlacementMove


class ActionBackend(ABC):
    @abstractmethod
    def validate(self) -> None:
        pass

    @abstractmethod
    def apply_move(self, move: PlacementMove) -> ActionResult:
        pass

    def apply_moves(self, moves: list[PlacementMove]) -> list[ActionResult]:
        return [self.apply_move(move) for move in moves]