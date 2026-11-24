"""Unit tests for KafkaTransport's consume/commit behavior, using fake
confluent-kafka Consumer/Producer objects so these run without the 'kafka'
extra or a live broker (mirrors the fake-object pattern HOTS's own contract
tests use for its KafkaConnector)."""

from __future__ import annotations

import json
from typing import Any

import pytest

from hots_connector.domain.errors import TransportError
from hots_connector.transport.kafka import KafkaTransport


class _FakeMsg:
    def __init__(
        self,
        value: bytes | None,
        *,
        error: Any = None,
        offset: int = 0,
        partition: int = 0,
    ):
        self._value = value
        self._error = error
        self._offset = offset
        self._partition = partition

    def value(self) -> bytes | None:
        return self._value

    def error(self) -> Any:
        return self._error

    def offset(self) -> int:
        return self._offset

    def partition(self) -> int:
        return self._partition


class _FakeConsumer:
    def __init__(self, messages: list[_FakeMsg]) -> None:
        self._queue = list(messages)
        self.committed: list[_FakeMsg] = []

    def poll(self, timeout: float) -> _FakeMsg | None:
        return self._queue.pop(0) if self._queue else None

    def commit(self, message: _FakeMsg, asynchronous: bool) -> None:
        self.committed.append(message)


class _FakeProducer:
    def __init__(self) -> None:
        self.published: list[tuple[str, bytes]] = []

    def produce(self, topic: str, value: bytes) -> None:
        self.published.append((topic, value))

    def poll(self, timeout: float) -> None:
        pass


def _move_bytes(**kwargs: Any) -> bytes:
    return json.dumps(kwargs).encode("utf-8")


def _transport(messages: list[_FakeMsg], *, topic_moves_dlq: str | None = None) -> KafkaTransport:
    # Bypass __init__ (which lazily imports confluent_kafka) so this test
    # doesn't need the 'kafka' extra installed to exercise our own logic.
    t = KafkaTransport.__new__(KafkaTransport)
    t.topic_data = "hots_data"
    t.topic_moves = "hots_moves"
    t.topic_moves_dlq = topic_moves_dlq
    t._consumer = _FakeConsumer(messages)  # type: ignore[attr-defined]
    t._producer = _FakeProducer()  # type: ignore[attr-defined]
    t._uncommitted = []
    return t


def test_consume_moves_drains_up_to_max_messages() -> None:
    messages = [
        _FakeMsg(_move_bytes(container_name="c1", old_host="n1", new_host="n2")),
        _FakeMsg(_move_bytes(container_name="c2", old_host="n1", new_host="n2")),
        _FakeMsg(_move_bytes(container_name="c3", old_host="n1", new_host="n2")),
    ]
    t = _transport(messages)

    moves = t.consume_moves(timeout_seconds=1.0, max_messages=2)

    assert [m.workload_id for m in moves] == ["c1", "c2"]
    assert len(t._uncommitted) == 2


def test_consume_moves_returns_empty_on_timeout_not_error() -> None:
    t = _transport([])
    assert t.consume_moves(timeout_seconds=0.05, max_messages=5) == []


def test_consume_moves_raises_on_broker_error_instead_of_returning_empty() -> None:
    t = _transport([_FakeMsg(None, error="boom")])

    with pytest.raises(TransportError):
        t.consume_moves(timeout_seconds=1.0, max_messages=5)


def test_consume_moves_dead_letters_malformed_payload_and_still_advances_offset() -> None:
    messages = [
        _FakeMsg(b"not json"),
        _FakeMsg(_move_bytes(container_name="c2", old_host="n1", new_host="n2")),
    ]
    t = _transport(messages, topic_moves_dlq="hots_moves_dlq")

    moves = t.consume_moves(timeout_seconds=1.0, max_messages=5)

    assert [m.workload_id for m in moves] == ["c2"]
    # The poison message doesn't come back as a move, but it must still be
    # tracked for commit -- otherwise it would be redelivered forever and
    # wedge the topic.
    assert len(t._uncommitted) == 2
    # ...and it's forwarded to the DLQ rather than silently dropped.
    assert t._producer.published == [("hots_moves_dlq", b"not json")]  # type: ignore[attr-defined]


def test_commit_acks_the_last_seen_message_and_clears_pending() -> None:
    t = _transport([])
    msg = _FakeMsg(_move_bytes(container_name="c1", old_host="n1", new_host="n2"))
    t._uncommitted = [msg]

    t.commit()

    assert t._consumer.committed == [msg]  # type: ignore[attr-defined]
    assert t._uncommitted == []


def test_commit_is_a_no_op_when_nothing_pending() -> None:
    t = _transport([])
    t.commit()  # must not raise
    assert t._consumer.committed == []  # type: ignore[attr-defined]
