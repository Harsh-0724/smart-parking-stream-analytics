"""Live state: Kafka feed thread -> in-memory slot store -> WebSocket fan-out.

The feed uses manual partition assignment (no consumer-group membership), so every API instance
sees every message and never triggers or suffers a rebalance. Slot state is bootstrapped from the
processor's own checkpoints on `state.changelog`, then updated by tailing `parking.raw` from the
checkpoint's resume offset, so there is no gap between snapshot and live deltas."""

import asyncio
import json
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from confluent_kafka import OFFSET_END, Consumer, TopicPartition
from prometheus_client import Counter, Gauge

from common import topics
from common.kafka import read_to_end

log = logging.getLogger("api.live")

FLUSH_INTERVAL_S = 0.1
OUTBOX_LIMIT = 2000

FEED_MESSAGES = Counter(
    "api_feed_messages_total", "Kafka messages consumed by the API feed", ["topic"]
)
WS_CLIENTS = Gauge("api_websocket_clients", "Connected WebSocket clients")

SlotKey = tuple[str, str]


@dataclass
class Batch:
    slots: list[tuple[str, str, bool, float]] = field(default_factory=list)  # lot, slot, occ, ts
    occupancy: list[dict[str, Any]] = field(default_factory=list)
    alerts: list[dict[str, Any]] = field(default_factory=list)

    def __bool__(self) -> bool:
        return bool(self.slots or self.occupancy or self.alerts)


class SlotStore:
    """Latest known state per slot; older events never overwrite newer ones."""

    def __init__(self) -> None:
        self._state: dict[str, dict[str, tuple[float, bool]]] = {}

    def load(self, lot_id: str, slots: dict[str, tuple[float, bool]]) -> None:
        self._state[lot_id] = dict(slots)

    def apply(self, lot_id: str, slot_id: str, occupied: bool, ts: float) -> bool:
        lot = self._state.setdefault(lot_id, {})
        prev = lot.get(slot_id)
        if prev is not None and ts <= prev[0]:
            return False
        lot[slot_id] = (ts, occupied)
        return True

    def lot(self, lot_id: str) -> dict[str, tuple[float, bool]]:
        return self._state.get(lot_id, {})

    def occupied_count(self, lot_id: str) -> int:
        return sum(1 for _, occ in self._state.get(lot_id, {}).values() if occ)

    def known_lots(self) -> list[str]:
        return list(self._state)


class Client:
    """One WebSocket connection: an outbox of messages plus coalesced slot deltas."""

    def __init__(self, ws: Any = None) -> None:
        self.ws = ws
        self.lots: set[str] = set()
        self.outbox: list[dict[str, Any]] = []
        self.slot_pending: dict[str, dict[str, tuple[bool, float]]] = {}

    def push(self, message: dict[str, Any]) -> None:
        if len(self.outbox) >= OUTBOX_LIMIT:
            del self.outbox[: OUTBOX_LIMIT // 2]  # a slow client loses old messages, not memory
        self.outbox.append(message)

    def queue_slot(self, lot_id: str, slot_id: str, occupied: bool, ts: float) -> None:
        self.slot_pending.setdefault(lot_id, {})[slot_id] = (occupied, ts)

    def drain(self) -> list[dict[str, Any]]:
        messages, self.outbox = self.outbox, []
        pending, self.slot_pending = self.slot_pending, {}
        for lot_id, slots in pending.items():
            messages.append(
                {
                    "type": "slots",
                    "lot_id": lot_id,
                    "deltas": [
                        {"slot_id": s, "occupied": occ, "ts": _iso(ts)}
                        for s, (occ, ts) in slots.items()
                    ],
                }
            )
        return messages


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts).astimezone().isoformat(timespec="milliseconds")


class Hub:
    """Owns the slot store and the connected clients. Only touched from the event loop."""

    def __init__(self) -> None:
        self.store = SlotStore()
        self.clients: set[Client] = set()
        self.live_current: dict[str, tuple[int, str]] = {}  # lot -> (occupied, window emitted_at)
        self.feed_ready = False

    def add(self, ws: Any = None) -> Client:
        client = Client(ws)
        self.clients.add(client)
        WS_CLIENTS.set(len(self.clients))
        return client

    def remove(self, client: Client) -> None:
        self.clients.discard(client)
        WS_CLIENTS.set(len(self.clients))

    def subscribe(self, client: Client, lots: list[str]) -> None:
        client.lots = set(lots)
        for lot_id in lots:
            client.push(self.snapshot_message(lot_id))

    def snapshot_message(self, lot_id: str) -> dict[str, Any]:
        return {
            "type": "snapshot",
            "lot_id": lot_id,
            "slots": [
                {"slot_id": s, "occupied": occ, "ts": _iso(ts)}
                for s, (ts, occ) in sorted(self.store.lot(lot_id).items())
            ],
        }

    def load_snapshot(self, lots: dict[str, dict[str, tuple[float, bool]]]) -> None:
        for lot_id, slots in lots.items():
            self.store.load(lot_id, slots)
        self.feed_ready = True

    def handle(self, batch: Batch) -> None:
        for lot_id, slot_id, occupied, ts in batch.slots:
            if self.store.apply(lot_id, slot_id, occupied, ts):
                for client in self.clients:
                    if lot_id in client.lots:
                        client.queue_slot(lot_id, slot_id, occupied, ts)
        for result in batch.occupancy:
            if result.get("current_occupied") is not None:
                self.live_current[result["lot_id"]] = (
                    result["current_occupied"],
                    result["emitted_at"],
                )
            self._broadcast({"type": "occupancy", "data": result})
        for alert in batch.alerts:
            self._broadcast({"type": "alert", "data": alert})

    def broadcast(self, message: dict[str, Any]) -> None:
        self._broadcast(message)

    def _broadcast(self, message: dict[str, Any]) -> None:
        for client in self.clients:
            client.push(message)


class KafkaFeed(threading.Thread):
    """Background thread that tails Kafka and hands batches to the event loop."""

    def __init__(
        self,
        bootstrap: str,
        loop: asyncio.AbstractEventLoop,
        on_snapshot: Callable[[dict[str, dict[str, tuple[float, bool]]]], None],
        on_batch: Callable[[Batch], None],
    ) -> None:
        super().__init__(daemon=True, name="kafka-feed")
        self._servers = bootstrap
        self._loop = loop
        self._on_snapshot = on_snapshot
        self._on_batch = on_batch
        self._halt = threading.Event()

    def stop(self) -> None:
        self._halt.set()

    def run(self) -> None:
        while not self._halt.is_set():
            try:
                self._run_once()
            except Exception:
                log.exception("kafka feed failed; retrying")
                self._halt.wait(3)

    def _restore_state(self) -> tuple[dict[str, dict[str, tuple[float, bool]]], dict[int, int]]:
        lots: dict[str, dict[str, tuple[float, bool]]] = {}
        resume: dict[int, int] = {}
        for partition in range(topics.RAW_PARTITIONS):
            for _key, value, _ in read_to_end(self._servers, topics.CHANGELOG, partition):
                if not value:
                    continue
                checkpoint = json.loads(value)
                lots[checkpoint["lot_id"]] = {
                    slot: (v[0], bool(v[1])) for slot, v in checkpoint["slots"].items()
                }
                offset = checkpoint["resume_offset"]
                resume[partition] = min(resume.get(partition, offset), offset)
        return lots, resume

    def _run_once(self) -> None:
        lots, resume = self._restore_state()
        self._loop.call_soon_threadsafe(self._on_snapshot, lots)

        consumer = Consumer(
            {
                "bootstrap.servers": self._servers,
                "group.id": "api-gateway",  # required by the client; partitions are assigned manually  # noqa: E501
                "enable.auto.commit": False,
                "isolation.level": "read_committed",
            }
        )
        assignment = [
            TopicPartition(topics.RAW, p, resume.get(p, OFFSET_END))
            for p in range(topics.RAW_PARTITIONS)
        ]
        assignment += [TopicPartition(topics.OCCUPANCY_5MIN, p, OFFSET_END) for p in range(3)]
        assignment.append(TopicPartition(topics.ALERTS, 0, OFFSET_END))
        consumer.assign(assignment)
        log.info("kafka feed started: %d lots restored, resume offsets %s", len(lots), resume)
        try:
            while not self._halt.is_set():
                batch = Batch()
                for msg in consumer.consume(500, 0.2):
                    if msg.error():
                        continue
                    value, topic = msg.value(), msg.topic()
                    if not value or topic is None:
                        continue
                    FEED_MESSAGES.labels(topic).inc()
                    if topic == topics.RAW:
                        if b'"OCCUPANCY"' not in value:
                            continue  # heartbeats are the bulk of the traffic; skip cheaply
                        e = json.loads(value)
                        batch.slots.append(
                            (
                                e["lot_id"],
                                e["slot_id"],
                                e["status"] == "OCCUPIED",
                                datetime.fromisoformat(e["event_ts"]).timestamp(),
                            )
                        )
                    elif topic == topics.OCCUPANCY_5MIN:
                        batch.occupancy.append(json.loads(value))
                    else:
                        batch.alerts.append(json.loads(value))
                if batch:
                    self._loop.call_soon_threadsafe(self._on_batch, batch)
        finally:
            consumer.close()


async def flush_loop(hub: Hub) -> None:
    """Every FLUSH_INTERVAL_S, send each client what accumulated (slot deltas are coalesced)."""
    while True:
        await asyncio.sleep(FLUSH_INTERVAL_S)
        for client in list(hub.clients):
            messages = client.drain()
            if not messages:
                continue
            try:
                for message in messages:
                    await client.ws.send_json(message)
            except Exception:  # connection gone; the route's receive loop will clean up
                hub.remove(client)
