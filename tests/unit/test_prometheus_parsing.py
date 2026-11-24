"""Unit tests for _parse_vector and PrometheusObservationBackend.collect()
against representative Prometheus instant-query responses (the shape
PrometheusConnect.custom_query() returns: the `data.result` array)."""

from __future__ import annotations

from hots_connector.config.settings import Settings
from hots_connector.observation.prometheus import PrometheusObservationBackend, _parse_vector

_CPU_VECTOR = [
    {
        "metric": {"name": "web-1", "container_label_machine_id": "vm1"},
        "value": [1700000000.123, "0.42"],
    },
    {
        "metric": {"name": "web-2", "container_label_machine_id": "vm2"},
        "value": [1700000000.456, "0.10"],
    },
]


def test_parse_vector_extracts_workload_host_value_and_sample_timestamp() -> None:
    out = _parse_vector(
        _CPU_VECTOR, workload_label="name", host_label="container_label_machine_id"
    )

    assert out == [
        ("web-1", "vm1", 0.42, 1700000000),
        ("web-2", "vm2", 0.10, 1700000000),
    ]


def test_parse_vector_skips_series_missing_workload_or_host_label() -> None:
    vector = [
        {"metric": {"name": "web-1"}, "value": [1700000000.0, "1.0"]},  # no host label
        {
            "metric": {"container_label_machine_id": "vm1"},
            "value": [1700000000.0, "1.0"],
        },  # no workload label
    ]

    out = _parse_vector(vector, workload_label="name", host_label="container_label_machine_id")

    assert out == []


def test_parse_vector_treats_unparseable_value_as_none_not_a_crash() -> None:
    vector = [
        {
            "metric": {"name": "web-1", "container_label_machine_id": "vm1"},
            "value": [1700000000.0, "not-a-number"],
        }
    ]

    out = _parse_vector(vector, workload_label="name", host_label="container_label_machine_id")

    assert out == [("web-1", "vm1", None, 1700000000)]


class _FakeClient:
    def __init__(self, responses: dict[str, list[dict]]) -> None:
        self._responses = responses

    def ping(self) -> bool:
        return True

    def query(self, query: str) -> list[dict]:
        return self._responses[query]


def _backend(mem_vector, cpu_vector, *, profile: str = "docker") -> PrometheusObservationBackend:
    backend = PrometheusObservationBackend(settings=Settings(), profile=profile)
    backend.client = _FakeClient(  # type: ignore[assignment]
        {backend.query_mem_mb: mem_vector, backend.query_cpu: cpu_vector}
    )
    return backend


def test_collect_merges_cpu_and_mem_using_prometheus_sample_timestamp() -> None:
    mem = [
        {
            "metric": {"name": "web-1", "container_label_machine_id": "vm1"},
            "value": [100.0, "512"],
        }
    ]
    cpu = [
        {
            "metric": {"name": "web-1", "container_label_machine_id": "vm1"},
            "value": [101.0, "0.3"],
        }
    ]

    batch = _backend(mem, cpu).collect()

    assert len(batch.samples) == 1
    sample = batch.samples[0]
    assert sample.workload_id == "web-1"
    assert sample.host_id == "vm1"
    assert sample.mem == 512.0
    assert sample.cpu == 0.3
    # Prometheus's own sample timestamp is used, not connector poll time --
    # this is what removes jitter from HOTS's tick-aligned series.
    assert sample.timestamp == 101


def test_collect_falls_back_to_poll_time_when_no_sample_timestamp_available() -> None:
    batch = _backend([], []).collect()
    assert batch.samples == []
    # No series at all -- nothing to assert on sample timestamps, but the
    # batch itself must still carry a timestamp for bookkeeping.
    assert batch.timestamp > 0


def test_kubernetes_profile_defaults_to_pod_and_node_labels() -> None:
    backend = _backend([], [], profile="kubernetes")
    assert backend.workload_label == "pod"
    assert backend.host_label == "node"


def test_docker_profile_defaults_to_name_and_machine_id_labels() -> None:
    backend = _backend([], [], profile="docker")
    assert backend.workload_label == "name"
    assert backend.host_label == "container_label_machine_id"


def test_explicit_settings_override_profile_defaults() -> None:
    settings = Settings(prom_workload_label="custom_workload")
    backend = PrometheusObservationBackend(settings=settings, profile="docker")
    assert backend.workload_label == "custom_workload"
    # Untouched fields still fall back to the profile default.
    assert backend.host_label == "container_label_machine_id"
