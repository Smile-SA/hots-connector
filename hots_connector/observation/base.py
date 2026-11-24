from __future__ import annotations

from abc import ABC, abstractmethod

from ..domain.models import WorkloadBatch


class ObservationBackend(ABC):
    @abstractmethod
    def validate(self) -> None:
        pass

    @abstractmethod
    def collect(self) -> WorkloadBatch:
        pass