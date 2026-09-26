"""Kafka client config shared by every service.

Delivery policy (from CLAUDE.md): idempotent producer with acks=all and lz4;
consumers commit manually, use the cooperative-sticky assignor and read committed.
"""

from typing import Any


def producer_config(bootstrap: str, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "client.id": client_id,
        "enable.idempotence": True,
        "acks": "all",
        "compression.type": "lz4",
        "partitioner": "murmur2_random",  # Java-compatible, matches common.partitioning
        "linger.ms": 20,
        "batch.num.messages": 10000,
    }


def consumer_config(bootstrap: str, group_id: str, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "client.id": client_id,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
        "isolation.level": "read_committed",
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    }
