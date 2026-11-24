from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class WorkloadSample:
    timestamp: int
    workload_id: str
    host_id: str
    cpu: float | None = None
    mem: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_hots_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "timestamp": int(self.timestamp),
            "container_id": self.workload_id,
            "machine_id": self.host_id,
        }
        if self.cpu is not None:
            payload["cpu"] = float(self.cpu)
        if self.mem is not None:
            payload["mem"] = float(self.mem)
        if self.metadata:
            payload.update(self.metadata)
        return payload


@dataclass(frozen=True)
class WorkloadBatch:
    timestamp: int
    samples: list[WorkloadSample]
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class PlacementMove:
    """A single HOTS placement decision.

    ``source_host`` is ``None`` for an initial placement (the workload had no
    prior host) -- see the ``old_host`` field in the shared move contract
    under ``tests/contracts/``.
    """

    workload_id: str
    source_host: str | None
    target_host: str
    timestamp: int | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class ActionResult:
    workload_id: str
    success: bool
    message: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)