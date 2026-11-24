from __future__ import annotations

from ..config.settings import Settings
from .base import DecisionTransport

# Imported lazily below: confluent-kafka must not be required just to import
# this factory when a non-Kafka transport is selected.


def build_transport(settings: Settings) -> DecisionTransport:
    if settings.transport_backend == "kafka":
        from .kafka import KafkaTransport

        return KafkaTransport(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            topic_data=settings.kafka_topic_data,
            topic_moves=settings.kafka_topic_moves,
            group_id=settings.kafka_group_id,
            topic_moves_dlq=settings.kafka_topic_moves_dlq,
        )

    raise ValueError(f"Unsupported transport backend: {settings.transport_backend!r}")