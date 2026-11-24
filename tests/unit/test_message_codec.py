"""Unit tests for the wire-format codec functions in transport/kafka.py.

These are deliberately decoupled from the golden fixtures in
tests/contracts/ -- this file covers edge cases (malformed input, extra
metadata, null old_host) the codec must handle regardless of what HOTS
actually sends today; the contract tests pin the exact shape HOTS does send.
"""

from __future__ import annotations

import json

import pytest

from hots_connector.domain.models import WorkloadBatch, WorkloadSample
from hots_connector.transport.kafka import SCHEMA_VERSION, _decode_move, _encode_batch


def test_encode_batch_emits_only_v_and_containers_keys() -> None:
    batch = WorkloadBatch(
        timestamp=100,
        samples=[
            WorkloadSample(timestamp=100, workload_id="c_1", host_id="n_1", cpu=1.5),
        ],
    )

    decoded = json.loads(_encode_batch(batch))

    # Pinned by tests/contracts/fixtures/telemetry_batch.json: no top-level
    # "timestamp" or "file" keys, just {"v": 1, "containers": [...]}.
    assert set(decoded) == {"v", "containers"}
    assert decoded["v"] == SCHEMA_VERSION
    assert decoded["containers"] == [
        {"timestamp": 100, "container_id": "c_1", "machine_id": "n_1", "cpu": 1.5}
    ]


def test_encode_batch_omits_missing_metrics_rather_than_defaulting_to_zero() -> None:
    batch = WorkloadBatch(
        timestamp=100,
        samples=[WorkloadSample(timestamp=100, workload_id="c_1", host_id="n_1")],
    )

    decoded = json.loads(_encode_batch(batch))

    assert "cpu" not in decoded["containers"][0]
    assert "mem" not in decoded["containers"][0]


def test_decode_move_valid_payload() -> None:
    payload = json.dumps(
        {"container_name": "c_1", "old_host": "n_1", "new_host": "n_2", "time": 42}
    ).encode("utf-8")

    move = _decode_move(payload)

    assert move.workload_id == "c_1"
    assert move.source_host == "n_1"
    assert move.target_host == "n_2"
    assert move.timestamp == 42
    assert move.metadata == {}


def test_decode_move_tolerates_missing_v_key() -> None:
    """A HOTS build that predates schema versioning must not be rejected."""
    payload = json.dumps(
        {"container_name": "c_1", "old_host": "n_1", "new_host": "n_2"}
    ).encode("utf-8")

    move = _decode_move(payload)

    assert move.workload_id == "c_1"
    assert move.metadata == {}  # "v"'s absence must not itself become metadata


def test_decode_move_accepts_matching_schema_version() -> None:
    payload = json.dumps(
        {"v": SCHEMA_VERSION, "container_name": "c_1", "old_host": "n_1", "new_host": "n_2"}
    ).encode("utf-8")

    move = _decode_move(payload)

    assert move.workload_id == "c_1"
    assert move.metadata == {}  # "v" is a known key, must not leak into metadata


def test_decode_move_warns_but_still_parses_unexpected_schema_version(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payload = json.dumps(
        {"v": 99, "container_name": "c_1", "old_host": "n_1", "new_host": "n_2"}
    ).encode("utf-8")

    with caplog.at_level("WARNING"):
        move = _decode_move(payload)

    assert move.workload_id == "c_1"  # parsed despite the unexpected version
    assert any("schema version" in record.message for record in caplog.records)


def test_decode_move_accepts_null_old_host_for_initial_placement() -> None:
    payload = json.dumps(
        {"container_name": "c_1", "old_host": None, "new_host": "n_2"}
    ).encode("utf-8")

    move = _decode_move(payload)

    assert move.source_host is None
    assert move.target_host == "n_2"


def test_decode_move_preserves_unknown_keys_as_metadata() -> None:
    payload = json.dumps(
        {
            "container_name": "c_1",
            "old_host": "n_1",
            "new_host": "n_2",
            "reason": "overload",
        }
    ).encode("utf-8")

    move = _decode_move(payload)

    assert move.metadata == {"reason": "overload"}


@pytest.mark.parametrize(
    "raw",
    [
        b"not json at all",
        b'{"container_name": "c_1"}',  # missing old_host / new_host
        b'["not", "an", "object"]',
    ],
)
def test_decode_move_rejects_malformed_payloads(raw: bytes) -> None:
    with pytest.raises(ValueError):
        _decode_move(raw)
