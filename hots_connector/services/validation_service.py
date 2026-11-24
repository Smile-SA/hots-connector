from __future__ import annotations

from ..actions.base import ActionBackend
from ..observation.base import ObservationBackend
from ..transport.base import DecisionTransport


class ValidationService:
    def __init__(
        self,
        *,
        observation: ObservationBackend,
        transport: DecisionTransport,
        actions: ActionBackend,
    ) -> None:
        self.observation = observation
        self.transport = transport
        self.actions = actions

    def run(self) -> dict[str, str]:
        report: dict[str, str] = {}

        try:
            self.observation.validate()
            report["observation"] = "ok"
        except Exception as exc:
            report["observation"] = f"error: {exc}"

        try:
            self.transport.validate()
            report["transport"] = "ok"
        except Exception as exc:
            report["transport"] = f"error: {exc}"

        try:
            self.actions.validate()
            report["actions"] = "ok"
        except Exception as exc:
            report["actions"] = f"error: {exc}"

        return report