from __future__ import annotations

import logging
from typing import Any

from prometheus_api_client import PrometheusConnect

log = logging.getLogger(__name__)


class PrometheusClient:
    """
    Thin wrapper around prometheus-api-client.
    Keeps queries.py free of connection logic.
    """

    def __init__(self, *, base_url: str) -> None:
        self.base_url = base_url
        self._client = PrometheusConnect(
            url=base_url,
            disable_ssl=True,
        )

    def ping(self) -> bool:
        try:
            self._client.custom_query(query="up")
            return True
        except Exception:
            log.exception("Prometheus ping failed")
            return False

    def query(self, query: str) -> list[dict[str, Any]]:
        return self._client.custom_query(query=query)  # type: ignore[no-any-return]
