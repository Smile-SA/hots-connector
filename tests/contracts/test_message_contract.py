"""Contract tests for the HOTS <-> hots-connector message formats.

The fixtures in fixtures/ are copied byte-identical from hots's
tests/contracts/fixtures/ -- see fixtures/README.md. HOTS's own suite
verifies the same two files from its side (KafkaConnector/FileConnector
parse `moves_batch.json` and emit `telemetry_batch.json`'s shape); this file
is the connector-side verification: hots_connector must be able to *produce*
telemetry_batch.json's messages and *consume* moves_batch.json's.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hots_connector.domain.models import WorkloadBatch, WorkloadSample
from hots_connector.transport.kafka import _decode_move, _encode_batch

FIXTURES = Path(__file__).parent / "fixtures"


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# Fixture self-consistency (also asserted on the HOTS side; duplicated here
# so a fixture that drifted out of sync fails immediately in this repo too,
# not just in whichever repo's suite happens to run first).
# --------------------------------------------------------------------------


def test_telemetry_fixture_shape() -> None:
    telemetry = _load("telemetry_batch.json")
    assert telemetry["direction"] == "connector_to_hots"
    assert telemetry["messages"], "fixture must carry at least one message"

    for msg in telemetry["messages"]:
        assert set(msg) == {"v", "containers"}
        for c in msg["containers"]:
            assert {"timestamp", "container_id", "machine_id"} <= set(c)
            assert isinstance(c["timestamp"], int)
            assert isinstance(c["container_id"], str)
            assert isinstance(c["machine_id"], str)


def test_moves_fixture_shape() -> None:
    moves = _load("moves_batch.json")
    assert moves["direction"] == "hots_to_connector"
    required = set(moves["required_keys"])
    assert required == {"container_name", "old_host", "new_host"}

    for msg in moves["messages"]:
        assert required <= set(msg)
        assert set(msg) <= required | {"time", "v"}


# --------------------------------------------------------------------------
# Producer verification: hots_connector must emit exactly telemetry_batch.json.
# --------------------------------------------------------------------------


def test_encode_batch_produces_the_telemetry_fixture_messages() -> None:
    telemetry = _load("telemetry_batch.json")

    for msg in telemetry["messages"]:
        samples = [
            WorkloadSample(
                timestamp=c["timestamp"],
                workload_id=c["container_id"],
                host_id=c["machine_id"],
                cpu=c.get("cpu"),
                mem=c.get("mem"),
            )
            for c in msg["containers"]
        ]
        batch = WorkloadBatch(timestamp=samples[0].timestamp, samples=samples)

        encoded = json.loads(_encode_batch(batch))

        assert encoded == msg


# --------------------------------------------------------------------------
# Consumer verification: hots_connector must consume moves_batch.json cleanly.
# --------------------------------------------------------------------------


def test_decode_move_consumes_every_message_in_the_moves_fixture() -> None:
    moves = _load("moves_batch.json")

    for msg in moves["messages"]:
        move = _decode_move(json.dumps(msg).encode("utf-8"))

        assert move.workload_id == msg["container_name"]
        assert move.source_host == msg["old_host"]
        assert move.target_host == msg["new_host"]
        assert move.timestamp == msg.get("time")
        # The fixture's messages carry only the four contract keys, so
        # nothing should spill into metadata.
        assert move.metadata == {}
