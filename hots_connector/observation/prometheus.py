from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from ..adapters.prometheus.client import PrometheusClient
from ..config.settings import Settings
from ..domain.models import WorkloadBatch, WorkloadSample
from ..observation.base import ObservationBackend

# Per-environment defaults, used whenever the corresponding Settings field is
# left unset. Keeps a single generic backend instead of one class per
# environment -- the environment is a settings profile, not a subclass.
_PROFILE_DEFAULTS: dict[str, dict[str, str]] = {
    "docker": {
        "workload_label": "name",
        "host_label": "container_label_machine_id",
        "query_mem_mb": (
            "sum by (name, container_label_machine_id) (container_memory_rss / (1024*1024))"
        ),
        "query_cpu": (
            "sum by (name, container_label_machine_id) "
            "(rate(container_cpu_usage_seconds_total[1m]))"
        ),
    },
    "kubernetes": {
        "workload_label": "pod",
        "host_label": "node",
        "query_mem_mb": (
            'sum by (pod, node) (container_memory_working_set_bytes{container!=""} / (1024*1024))'
        ),
        "query_cpu": (
            'sum by (pod, node) (rate(container_cpu_usage_seconds_total{container!=""}[1m]))'
        ),
    },
}


def _safe_float(x: Any) -> float | None:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _parse_vector(
    results: list[dict],
    *,
    workload_label: str,
    host_label: str,
) -> list[tuple[str, str, float | None, int | None]]:
    """Parse a Prometheus instant-query vector into (workload, host, value, sample_ts).

    `sample_ts` is Prometheus's own timestamp for the sample (`value[0]`),
    not when we happened to poll -- using it removes jitter between
    observation and reality for HOTS's tick-aligned time series.
    """
    out: list[tuple[str, str, float | None, int | None]] = []
    for item in results or []:
        metric = item.get("metric", {})
        raw_ts, value = item.get("value", [None, None])
        workload_id = metric.get(workload_label)
        host_id = metric.get(host_label)

        if not workload_id or not host_id:
            continue

        sample_ts = int(float(raw_ts)) if raw_ts is not None else None
        out.append((str(workload_id), str(host_id), _safe_float(value), sample_ts))
    return out


class PrometheusObservationBackend(ObservationBackend):
    """Generic Prometheus-backed observation, parameterized by environment profile.

    The queries/labels are Docker- or Kubernetes-specific, but the mechanics
    (query, parse, merge cpu+mem by (workload, host)) are identical -- so this
    is one class with a settings-driven profile rather than one class per
    environment. Register new environments by adding a profile to
    `_PROFILE_DEFAULTS` and a factory entry, no new class needed.
    """

    def __init__(self, *, settings: Settings, profile: str) -> None:
        if profile not in _PROFILE_DEFAULTS:
            raise ValueError(f"Unknown Prometheus observation profile: {profile!r}")

        self.settings = settings
        self.profile = profile
        self.client = PrometheusClient(base_url=settings.prometheus_url)

        defaults = _PROFILE_DEFAULTS[profile]
        self.workload_label = settings.prom_workload_label or defaults["workload_label"]
        self.host_label = settings.prom_host_label or defaults["host_label"]
        self.query_mem_mb = settings.prom_query_mem_mb or defaults["query_mem_mb"]
        self.query_cpu = settings.prom_query_cpu or defaults["query_cpu"]

    def validate(self) -> None:
        if not self.client.ping():
            raise RuntimeError(f"Prometheus is unreachable at {self.settings.prometheus_url}")

    def collect(self) -> WorkloadBatch:
        poll_time = int(time.time())

        mem_raw = self.client.query(self.query_mem_mb) or []
        cpu_raw = self.client.query(self.query_cpu) or []

        mem = _parse_vector(mem_raw, workload_label=self.workload_label, host_label=self.host_label)
        cpu = _parse_vector(cpu_raw, workload_label=self.workload_label, host_label=self.host_label)

        merged: dict[tuple[str, str], dict[str, Any]] = defaultdict(
            lambda: {"cpu": None, "mem": None, "ts": None}
        )
        for wid, hid, v, ts in mem:
            merged[(wid, hid)]["mem"] = v
            if ts is not None:
                merged[(wid, hid)]["ts"] = ts
        for wid, hid, v, ts in cpu:
            merged[(wid, hid)]["cpu"] = v
            if ts is not None:
                merged[(wid, hid)]["ts"] = ts

        # NOTE: if a workload migrated between the two queries, it can appear
        # under two (workload, host) pairs within one batch -- harmless for
        # HOTS today (both rows are still valid observations) but worth
        # knowing if you go looking for "duplicate" workload_ids in a batch.
        samples: list[WorkloadSample] = []
        for (wid, hid), vals in merged.items():
            samples.append(
                WorkloadSample(
                    # Prefer Prometheus's own sample timestamp over poll time
                    # to avoid jitter in HOTS's tick-aligned series.
                    timestamp=vals["ts"] if vals["ts"] is not None else poll_time,
                    workload_id=wid,
                    host_id=hid,
                    cpu=vals["cpu"],
                    mem=vals["mem"],
                    metadata={"backend": f"prometheus_{self.profile}"},
                )
            )

        return WorkloadBatch(timestamp=poll_time, samples=samples)
