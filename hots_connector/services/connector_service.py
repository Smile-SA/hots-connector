from __future__ import annotations

import logging
import threading
import time

from ..actions.base import ActionBackend
from ..observation.base import ObservationBackend
from ..transport.base import DecisionTransport
from .scheduler import TickScheduler

log = logging.getLogger(__name__)


class ConnectorService:
    def __init__(
        self,
        *,
        observation: ObservationBackend,
        transport: DecisionTransport,
        actions: ActionBackend,
        publish_interval_seconds: int,
        moves_batch_size: int = 50,
        moves_poll_timeout_seconds: float = 1.0,
    ) -> None:
        self.observation = observation
        self.transport = transport
        self.actions = actions
        self.scheduler = TickScheduler(publish_interval_seconds)
        self.moves_batch_size = moves_batch_size
        self.moves_poll_timeout_seconds = moves_poll_timeout_seconds
        self._running_event = threading.Event()

    def validate(self) -> dict[str, str]:
        report: dict[str, str] = {}

        try:
            self.observation.validate()
            report["observation"] = "ok"
        except Exception as exc:
            log.exception("Observation validation failed")
            report["observation"] = f"error: {exc}"

        try:
            self.transport.validate()
            report["transport"] = "ok"
        except Exception as exc:
            log.exception("Transport validation failed")
            report["transport"] = f"error: {exc}"

        try:
            self.actions.validate()
            report["actions"] = "ok"
        except Exception as exc:
            log.exception("Action backend validation failed")
            report["actions"] = f"error: {exc}"

        return report

    @property
    def is_running(self) -> bool:
        return self._running_event.is_set()

    def stop(self, *, join_timeout_seconds: float | None = None) -> None:
        """Signal the loop to stop after its current tick.

        This does not itself join any thread the loop is running on -- the
        loop may still be mid-tick when this returns. `join_timeout_seconds`
        is accepted for callers (e.g. the API) that own the thread and want
        to wait for it; `ConnectorService` itself doesn't own one.
        """
        self._running_event.clear()

    def publish_once(self) -> int:
        batch = self.observation.collect()
        self.transport.publish_telemetry(batch)
        return len(batch.samples)

    def apply_pending_moves(self, timeout_seconds: float = 1.0) -> int:
        moves = self.transport.consume_moves(
            timeout_seconds=timeout_seconds, max_messages=self.moves_batch_size
        )
        if not moves:
            return 0

        results = self.actions.apply_moves(moves)
        for result in results:
            if result.success:
                log.info("Applied move successfully: %s", result.workload_id)
            else:
                log.warning("Move failed for %s: %s", result.workload_id, result.message)

        # Commit after apply, not before: a crash between consume and apply
        # redelivers these moves instead of losing them. Committing here even
        # when some moves failed is deliberate -- retrying a failed *apply*
        # (e.g. an unreachable Docker host) will fail identically on
        # redelivery and would otherwise wedge the topic; failures are still
        # visible via the WARNING above and the ActionResult metadata.
        self.transport.commit()

        return len(results)

    def run_forever(self) -> None:
        self._running_event.set()
        log.info("Connector loop started")

        while self._running_event.is_set():
            tick_started = time.monotonic()
            try:
                self.publish_once()
                self.apply_pending_moves(timeout_seconds=self.moves_poll_timeout_seconds)
            except Exception:
                log.exception("Connector tick failed")
            finally:
                self.scheduler.sleep_until_next_tick(tick_started)

        log.info("Connector loop stopped")