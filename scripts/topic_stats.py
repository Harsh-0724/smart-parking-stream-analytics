"""Read a topic from the beginning and summarise it as JSON. Used by chaos scripts and demos.

python scripts/topic_stats.py parking.raw
"""

import json
import os
import sys
import uuid
from collections import Counter

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient

from common.schemas import ParkingEvent

LATE_THRESHOLD_S = 15.0


def read_all(topic: str, bootstrap: str) -> list[tuple[int, int, bytes | None, bytes | None]]:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    partitions = list(admin.list_topics(topic, timeout=10).topics[topic].partitions)
    c = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"topic-stats-{uuid.uuid4()}",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    ends = {}
    for p in partitions:
        low, high = c.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        ends[p] = high
    c.assign([TopicPartition(topic, p, 0) for p in partitions])
    out: list[tuple[int, int, bytes | None, bytes | None]] = []
    done = {p for p, high in ends.items() if high == 0}
    while len(done) < len(partitions):
        msg = c.poll(2.0)
        if msg is None or msg.error():
            continue
        out.append((msg.partition(), msg.offset(), msg.key(), msg.value()))
        if msg.offset() + 1 >= ends[msg.partition()]:
            done.add(msg.partition())
    c.close()
    return out


def summarise_raw(rows: list[tuple[int, int, bytes | None, bytes | None]]) -> dict[str, object]:
    ids: Counter[str] = Counter()
    invalid = late = 0
    by_partition: Counter[int] = Counter()
    hb_last: dict[str, float] = {}
    for partition, _, _, value in rows:
        by_partition[partition] += 1
        try:
            e = ParkingEvent.model_validate_json(value or b"")
        except ValueError:
            invalid += 1
            continue
        ids[e.event_id] += 1
        if (e.ingest_ts - e.event_ts).total_seconds() > LATE_THRESHOLD_S:
            late += 1
        if e.event_type == "HEARTBEAT":
            key = f"{e.lot_id}/{e.sensor_id}"
            hb_last[key] = max(hb_last.get(key, 0.0), e.event_ts.timestamp())
    return {
        "messages": len(rows),
        "invalid": invalid,
        "duplicate_messages": sum(n - 1 for n in ids.values() if n > 1),
        "late_messages": late,
        "by_partition": dict(sorted(by_partition.items())),
        "_hb_last": hb_last,
    }


if __name__ == "__main__":
    topic = sys.argv[1]
    bootstrap = os.environ.get("KAFKA_BOOTSTRAP_HOST", "localhost:19092")
    rows = read_all(topic, bootstrap)
    if topic == "parking.raw":
        stats = summarise_raw(rows)
        hb = stats.pop("_hb_last")
        assert isinstance(hb, dict)
        newest = max(hb.values(), default=0.0)
        stats["sensors_silent_over_60s"] = sorted(k for k, v in hb.items() if newest - v > 60)
        print(json.dumps(stats, indent=1))
    else:
        print(json.dumps({"messages": len(rows)}))
