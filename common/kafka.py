"""Kafka client config shared by every service.

Delivery policy (from CLAUDE.md): idempotent producer with acks=all and lz4;
consumers commit manually, use the cooperative-sticky assignor and read committed.
"""

import uuid
from typing import Any

from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition


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
        "message.max.bytes": 16_777_216,  # checkpoints on state.changelog can be several MB
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


def read_to_end(
    bootstrap: str, topic: str, partition: int
) -> list[tuple[bytes | None, bytes | None, int]]:
    """Read one partition from the beginning to its current end as (key, value, offset).

    Used to rebuild state from compacted topics; independent of any consumer group."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"reader-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "isolation.level": "read_committed",
        }
    )
    try:
        tp = TopicPartition(topic, partition, OFFSET_BEGINNING)
        low, high = consumer.get_watermark_offsets(tp, timeout=10)
        records: list[tuple[bytes | None, bytes | None, int]] = []
        if high <= low:
            return records
        consumer.assign([tp])
        idle_polls = 0
        while idle_polls < 10:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                idle_polls += 1
                continue
            offset = msg.offset()
            if offset is None:
                continue
            records.append((msg.key(), msg.value(), offset))
            if offset + 1 >= high:
                break
        return records
    finally:
        consumer.close()
