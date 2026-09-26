"""Pipeline introspection: brokers, partition leaders, consumer groups, lag, rebalances.

Everything shown on the Pipeline screen is read from Kafka itself (admin API + watermarks) and,
for latency, from Prometheus. Rebalances are detected by diffing each group's state and member
assignment between polls."""

import json
import time
import urllib.parse
import urllib.request
from collections import deque
from datetime import UTC, datetime

from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient

from api.models import (
    BrokerInfo,
    ConsumerGroupInfo,
    GroupMember,
    GroupPartition,
    PartitionInfo,
    PipelineStats,
    RebalanceEvent,
    TopicInfo,
)
from common import topics

GROUPS = ("occupancy-processor", "timescale-sink", "alerter")
TOPICS = (
    topics.RAW, topics.DLQ, topics.LATE, topics.LOT_METADATA,
    topics.OCCUPANCY_5MIN, topics.ALERTS, topics.CHANGELOG,
)  # fmt: skip
KAFKA_TIMEOUT_S = 5
REBALANCE_HISTORY = 30
LATENCY_QUERY = (
    "histogram_quantile({q}, sum by (le) (rate(parking_ingest_to_emit_seconds_bucket[1m])))"
)


def _now() -> datetime:
    return datetime.now(UTC)


def prometheus_scalar(base_url: str, query: str) -> float | None:
    url = f"{base_url}/api/v1/query?" + urllib.parse.urlencode({"query": query})
    try:
        with urllib.request.urlopen(url, timeout=2) as response:  # noqa: S310
            result = json.load(response)["data"]["result"]
        value = float(result[0]["value"][1]) if result else None
    except (OSError, ValueError, KeyError):
        return None
    return None if value is None or value != value else value  # NaN -> None


class PipelineCollector:
    def __init__(self, bootstrap: str, prometheus_url: str) -> None:
        self._admin = AdminClient({"bootstrap.servers": bootstrap})
        self._consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": "api-gateway-probe",
                "enable.auto.commit": False,
            }
        )
        self._prom = prometheus_url
        self._last_end: tuple[float, int] | None = None
        self._signatures: dict[str, tuple[str, tuple[tuple[str, tuple[str, ...]], ...]]] = {}
        self.rebalances: deque[RebalanceEvent] = deque(maxlen=REBALANCE_HISTORY)

    def collect(self) -> PipelineStats:
        try:
            return self._collect()
        except Exception as exc:  # Kafka unreachable: show that instead of failing the endpoint
            return PipelineStats(
                generated_at=_now(), error=str(exc)[:200], events_per_s=None, latency_p50_s=None,
                latency_p95_s=None, dlq_messages=0, late_messages=0, brokers=[], topics=[],
                groups=[], rebalances=list(self.rebalances),
            )  # fmt: skip

    def _collect(self) -> PipelineStats:
        metadata = self._admin.list_topics(timeout=KAFKA_TIMEOUT_S)
        ends: dict[tuple[str, int], tuple[int, int]] = {}
        topic_infos: list[TopicInfo] = []
        leaders: dict[int, int] = {}
        for name in TOPICS:
            topic = metadata.topics.get(name)
            if topic is None:
                continue
            partitions = []
            total = 0
            for pid, pm in sorted(topic.partitions.items()):
                low, high = self._consumer.get_watermark_offsets(
                    TopicPartition(name, pid), timeout=KAFKA_TIMEOUT_S
                )
                ends[(name, pid)] = (low, high)
                total += high - low
                leaders[pm.leader] = leaders.get(pm.leader, 0) + 1
                partitions.append(
                    PartitionInfo(
                        partition=pid, leader=pm.leader, replicas=list(pm.replicas),
                        isr=list(pm.isrs), end_offset=high,
                    )
                )  # fmt: skip
            topic_infos.append(TopicInfo(name=name, partitions=partitions, total_messages=total))

        brokers = [
            BrokerInfo(
                id=b.id, host=b.host or "", port=b.port or 0, leader_partitions=leaders.get(b.id, 0)
            )
            for b in sorted(metadata.brokers.values(), key=lambda b: b.id)
        ]
        groups = [g for g in (self._group(name, ends) for name in GROUPS) if g is not None]
        for group in groups:
            self._detect_rebalance(group)

        raw_end = sum(high for (t, _), (_, high) in ends.items() if t == topics.RAW)
        rate = None
        now = time.monotonic()
        if self._last_end is not None and now > self._last_end[0]:
            rate = max(0.0, (raw_end - self._last_end[1]) / (now - self._last_end[0]))
        self._last_end = (now, raw_end)

        def count(topic: str) -> int:
            return sum(h - lo for (t, _), (lo, h) in ends.items() if t == topic)

        return PipelineStats(
            generated_at=_now(),
            error=None,
            events_per_s=rate,
            latency_p50_s=prometheus_scalar(self._prom, LATENCY_QUERY.format(q=0.5)),
            latency_p95_s=prometheus_scalar(self._prom, LATENCY_QUERY.format(q=0.95)),
            dlq_messages=count(topics.DLQ),
            late_messages=count(topics.LATE),
            brokers=brokers,
            topics=topic_infos,
            groups=groups,
            rebalances=list(reversed(self.rebalances)),
        )

    def _group(
        self, group_id: str, ends: dict[tuple[str, int], tuple[int, int]]
    ) -> ConsumerGroupInfo | None:
        try:
            desc = self._admin.describe_consumer_groups([group_id])[group_id].result(
                KAFKA_TIMEOUT_S
            )
            committed_result = self._admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(group_id)]
            )[group_id].result(KAFKA_TIMEOUT_S)
        except Exception:
            return None
        state = str(desc.state).split(".")[-1].title().replace("_", "")
        state = {
            "Preparingrebalance": "PreparingRebalance",
            "Completingrebalance": "CompletingRebalance",
        }.get(state, state)
        owner: dict[tuple[str, int], str] = {}
        members = []
        for m in desc.members:
            assigned = m.assignment.topic_partitions if m.assignment else []
            for tp in assigned:
                owner[(tp.topic, tp.partition)] = m.client_id
            members.append(
                GroupMember(
                    client_id=m.client_id,
                    host=m.host,
                    partitions=sorted(f"{tp.topic}[{tp.partition}]" for tp in assigned),
                )
            )
        rows = []
        for tp in committed_result.topic_partitions:
            low, high = ends.get((tp.topic, tp.partition), (0, tp.offset if tp.offset >= 0 else 0))
            committed = tp.offset if tp.offset >= 0 else None
            rows.append(
                GroupPartition(
                    topic=tp.topic, partition=tp.partition, committed=committed, end_offset=high,
                    lag=max(0, high - (committed if committed is not None else low)),
                    member=owner.get((tp.topic, tp.partition)),
                )
            )  # fmt: skip
        rows.sort(key=lambda r: (r.topic, r.partition))
        return ConsumerGroupInfo(
            group_id=group_id,
            state=state,
            rebalancing="Rebalance" in state,
            members=sorted(members, key=lambda m: m.client_id),
            total_lag=sum(r.lag for r in rows),
            partitions=rows,
        )

    def _detect_rebalance(self, group: ConsumerGroupInfo) -> None:
        signature = (
            "rebalancing" if group.rebalancing else "stable",
            tuple((m.client_id, tuple(m.partitions)) for m in group.members),
        )
        previous = self._signatures.get(group.group_id)
        self._signatures[group.group_id] = signature
        if previous is None or previous == signature:
            return
        if group.rebalancing and previous[0] != "rebalancing":
            text = "rebalance started"
        elif previous[0] == "rebalancing" and not group.rebalancing:
            text = f"rebalance finished: {len(group.members)} member(s), partitions {self._layout(group)}"  # noqa: E501
        else:
            text = f"assignment changed: {len(previous[1])} to {len(group.members)} member(s), {self._layout(group)}"  # noqa: E501
        self.rebalances.append(RebalanceEvent(ts=_now(), group_id=group.group_id, description=text))

    @staticmethod
    def _layout(group: ConsumerGroupInfo) -> str:
        return (
            "; ".join(f"{m.client_id[:8]}: {len(m.partitions)}" for m in group.members)
            or "no members"
        )
