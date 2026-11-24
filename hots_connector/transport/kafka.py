from __future__ import annotations

import json
import logging
import time
from typing import Any

from ..domain.errors import TransportError
from ..domain.models import PlacementMove, WorkloadBatch
from .base import DecisionTransport

log = logging.getLogger(__name__)

# Schema version stamped on both message types (tests/contracts/fixtures/).
# Bump this and the fixtures together in both hots and hots-connector.
SCHEMA_VERSION = 1

# Keys fixed by the shared wire contract (tests/contracts/fixtures/moves_batch.json).
# `old_host` may be present with a null value for an initial placement.
_MOVE_REQUIRED_KEYS = {"container_name", "old_host", "new_host"}
_MOVE_KNOWN_KEYS = _MOVE_REQUIRED_KEYS | {"time", "v"}


def _encode_batch(batch: WorkloadBatch) -> bytes:
    # Shape pinned by tests/contracts/fixtures/telemetry_batch.json:
    # {"v": 1, "containers": [...]}, no other top-level keys.
    payload: dict[str, Any] = {
        "v": SCHEMA_VERSION,
        "containers": [sample.to_hots_dict() for sample in batch.samples],
    }
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


def _decode_move(raw: bytes | str) -> PlacementMove:
    try:
        data = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Move payload is not valid JSON: {exc}") from exc

    if not isinstance(data, dict):
        raise ValueError(f"Move payload must be a JSON object, got {type(data).__name__}")

    missing = _MOVE_REQUIRED_KEYS - data.keys()
    if missing:
        raise ValueError(f"Move payload missing required keys {sorted(missing)}: {data!r}")

    # "v" is optional on the way in -- tolerate a HOTS build that predates
    # versioning rather than hard-failing, but flag a version we don't
    # recognize so a future bump doesn't silently misparse.
    version = data.get("v")
    if version is not None and version != SCHEMA_VERSION:
        log.warning(
            "Move for %r has schema version %r, expected %r; parsing it anyway",
            data.get("container_name"),
            version,
            SCHEMA_VERSION,
        )

    return PlacementMove(
        workload_id=data["container_name"],
        source_host=data["old_host"],
        target_host=data["new_host"],
        timestamp=data.get("time"),
        metadata={k: v for k, v in data.items() if k not in _MOVE_KNOWN_KEYS},
    )


class KafkaTransport(DecisionTransport):
    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic_data: str,
        topic_moves: str,
        group_id: str,
        topic_moves_dlq: str | None = None,
    ) -> None:
        try:
            from confluent_kafka import Consumer, Producer
        except ImportError as exc:  # pragma: no cover - exercised without the extra
            raise ImportError(
                "The 'kafka' transport requires confluent-kafka. Install it with "
                "`pip install hots-connector[kafka]`."
            ) from exc

        self.topic_data = topic_data
        self.topic_moves = topic_moves
        self.topic_moves_dlq = topic_moves_dlq

        self._producer = Producer({"bootstrap.servers": bootstrap_servers})
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                # Committed manually via `commit()`, after moves have been applied,
                # so a crash between consume and apply redelivers rather than loses.
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([self.topic_moves])
        self._uncommitted: list[Any] = []

    def validate(self) -> None:
        self._producer.poll(0)
        self._consumer.list_topics(timeout=2.0)

    def publish_telemetry(self, batch: WorkloadBatch) -> None:
        self._producer.produce(self.topic_data, value=_encode_batch(batch))
        self._producer.poll(0)

    def consume_moves(
        self, timeout_seconds: float = 1.0, max_messages: int = 1
    ) -> list[PlacementMove]:
        moves: list[PlacementMove] = []
        deadline = time.monotonic() + timeout_seconds

        while len(moves) < max_messages:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break

            msg = self._consumer.poll(timeout=remaining)
            if msg is None:
                break  # timed out waiting: "no more for now", not an error

            if msg.error():
                # A real broker/consumer error, not "no messages" -- must not be
                # swallowed into an empty list, or callers can't tell them apart.
                raise TransportError(f"Kafka consumer error on {self.topic_moves}: {msg.error()}")

            value = msg.value()
            try:
                if value is None:
                    # e.g. a tombstone on a compacted topic -- not a shape
                    # this contract produces, treat like any other malformed
                    # payload rather than crashing _decode_move on None.
                    raise ValueError("Move message has no value (null payload)")
                moves.append(_decode_move(value))
            except ValueError as exc:
                log.error(
                    "Dropping malformed move on %s (offset=%s partition=%s): %s",
                    self.topic_moves,
                    msg.offset(),
                    msg.partition(),
                    exc,
                )
                if value is not None:
                    self._dead_letter(value)
                # Still tracked for commit: a poison message must not be
                # redelivered forever and block the topic.

            self._uncommitted.append(msg)

        return moves

    def commit(self) -> None:
        if not self._uncommitted:
            return
        # confluent-kafka commits are per-partition high-watermarks; committing
        # the newest message we've seen is sufficient to advance past all of them.
        self._consumer.commit(message=self._uncommitted[-1], asynchronous=False)
        self._uncommitted.clear()

    def _dead_letter(self, raw: bytes) -> None:
        if not self.topic_moves_dlq:
            return
        try:
            self._producer.produce(self.topic_moves_dlq, value=raw)
            self._producer.poll(0)
        except Exception:
            log.exception("Failed to publish malformed move to DLQ topic %s", self.topic_moves_dlq)