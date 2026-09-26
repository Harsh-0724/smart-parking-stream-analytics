"""Kafka wiring for the stream processor.

Per cycle (every CHECKPOINT_INTERVAL_S, and whenever partitions are revoked):
    1. produce outputs   2. flush   3. write checkpoints to state.changelog   4. commit offsets
A crash between any two steps causes the events since the last checkpoint to be reprocessed.
That yields at-least-once output; the sink's upserts on (lot_id, window_start) make the stored
result effectively-once. This is not Kafka exactly-once semantics (see docs/DECISIONS.md).
"""

import signal
import time
from types import FrameType

import structlog
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from prometheus_client import start_http_server

from common import topics
from common.config import Settings
from common.kafka import commit_tolerant, consumer_config, producer_config
from common.schemas import (
    Alert,
    AlertKind,
    AlertState,
    DeadLetter,
    LateEvent,
    LotMetadata,
    ParkingEvent,
    WindowResult,
)
from processor import metrics
from processor.checkpoint import restore_partition, serialise
from processor.lot_state import (
    LATENCY_SAMPLE_EVERY,
    LotState,
    Outcome,
    Params,
    SensorSignal,
    utc,
)
from processor.metadata import load_metadata

POLL_BATCH = 500
POLL_TIMEOUT_S = 0.2
METADATA_REFRESH_MIN_S = 5.0
FLUSH_TIMEOUT_S = 30.0


class ProcessorApp:
    def __init__(self, settings: Settings, instance_id: str, log: structlog.stdlib.BoundLogger):
        self._s = settings
        self._log = log
        self._params = Params(
            window_size_s=settings.window_size_s,
            allowed_lateness_s=settings.allowed_lateness_s,
            grace_s=settings.window_grace_s,
            dedupe_ttl_s=settings.dedupe_ttl_s,
            sensor_offline_after_s=settings.sensor_offline_after_s,
        )
        self._producer = Producer(producer_config(settings.kafka_bootstrap, instance_id))
        self._consumer = Consumer(
            consumer_config(settings.kafka_bootstrap, "occupancy-processor", instance_id)
        )
        self._lots: dict[int, dict[str, LotState]] = {}  # raw partition -> lot -> state
        self._next_offset: dict[int, int] = {}  # raw partition -> next offset to consume
        self._metadata: dict[str, LotMetadata] = {}
        self._metadata_loaded_at = 0.0
        self._delivery_errors = 0
        self._stop = False
        self._seen = 0  # valid events seen, for latency sampling

    # ---- lifecycle ----------------------------------------------------------------------------

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        start_http_server(self._s.metrics_port)
        self._refresh_metadata(force=True)
        self._consumer.subscribe(
            [topics.RAW],
            on_assign=self._on_assign,
            on_revoke=self._on_revoke,
            on_lost=self._on_lost,
        )
        self._log.info("started", group="occupancy-processor", params=self._params.__dict__)

        last_emit = last_checkpoint = time.monotonic()
        while not self._stop:
            for msg in self._consumer.consume(POLL_BATCH, POLL_TIMEOUT_S):
                self._handle(msg)
            self._producer.poll(0)
            now = time.monotonic()
            if now - last_emit >= self._s.emit_interval_s:
                self._emit()
                last_emit = now
            if now - last_checkpoint >= self._s.checkpoint_interval_s:
                self._checkpoint(list(self._lots))
                last_checkpoint = now

        self._log.info("shutting down: final emit, checkpoint, commit")
        self._emit()
        self._checkpoint(list(self._lots))
        self._consumer.close()  # triggers on_revoke, which checkpoints once more
        self._log.info("stopped")

    def _request_stop(self, _signum: int, _frame: FrameType | None) -> None:
        self._stop = True

    # ---- rebalance callbacks ------------------------------------------------------------------

    def _on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        metrics.REBALANCES.labels("assign").inc()
        started = time.monotonic()
        for tp in partitions:
            lots = restore_partition(self._s.kafka_bootstrap, tp.partition, self._params)
            self._lots[tp.partition] = lots
            if lots:
                # Lots in a partition can hold checkpoints from different cycles if a crash
                # interrupted a checkpoint write, so resume at the oldest; each lot skips the
                # events its own state already contains (see `_handle`).
                tp.offset = min(state.resume_offset for state in lots.values())
                self._next_offset[tp.partition] = tp.offset
            self._log.info(
                "partition assigned",
                partition=tp.partition,
                restored_lots=sorted(lots),
                resume_offset=tp.offset if lots else "committed",
            )
        consumer.incremental_assign(partitions)
        metrics.RESTORE_SECONDS.observe(time.monotonic() - started)
        self._update_gauges()

    def _on_revoke(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        metrics.REBALANCES.labels("revoke").inc()
        revoked = [tp.partition for tp in partitions]
        self._log.info("partitions revoked: checkpointing before hand-off", partitions=revoked)
        self._checkpoint(revoked)
        for p in revoked:
            self._lots.pop(p, None)
            self._next_offset.pop(p, None)
        consumer.incremental_unassign(partitions)
        self._update_gauges()

    def _on_lost(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        metrics.REBALANCES.labels("lost").inc()
        self._log.warning(
            "partitions lost: dropping state without checkpoint", partitions=partitions
        )
        for tp in partitions:
            self._lots.pop(tp.partition, None)
            self._next_offset.pop(tp.partition, None)
        consumer.incremental_unassign(partitions)
        self._update_gauges()

    # ---- per-message path ---------------------------------------------------------------------

    def _handle(self, msg: Message) -> None:
        err = msg.error()
        if err is not None:
            if err.code() != KafkaError._PARTITION_EOF:
                raise KafkaException(err)
            return
        partition, offset = msg.partition(), msg.offset()
        if partition is None or offset is None or partition not in self._lots:
            return
        self._next_offset[partition] = offset + 1

        try:
            event = ParkingEvent.model_validate_json(msg.value() or b"")
        except ValueError as exc:
            self._dead_letter(msg, str(exc), partition, offset)
            return

        self._seen += 1
        if self._seen % LATENCY_SAMPLE_EVERY == 0:
            metrics.record_ingest_latency(event.ingest_ts.timestamp(), time.time())

        state = self._lot_state(event.lot_id, partition)
        if offset < state.resume_offset:
            metrics.EVENTS_REPLAY_SKIPPED.inc()
            return

        outcome = state.process(event, event.ingest_ts.timestamp())
        if outcome is Outcome.ACCEPTED:
            metrics.EVENTS_PROCESSED.inc()
        elif outcome is Outcome.DUPLICATE:
            metrics.EVENTS_DUPLICATE.inc()
        else:
            self._route_late(event, state)
        self._publish_closed(state)

    def _lot_state(self, lot_id: str, partition: int) -> LotState:
        lots = self._lots[partition]
        state = lots.get(lot_id)
        if state is None:
            state = lots[lot_id] = LotState(lot_id, self._params)
            self._update_gauges()
        return state

    def _dead_letter(self, msg: Message, reason: str, partition: int, offset: int) -> None:
        metrics.EVENTS_DLQ.inc()
        record = DeadLetter(
            original_hex=(msg.value() or b"").hex(),
            error=reason[:500],
            source_partition=partition,
            source_offset=offset,
            failed_at=utc(time.time()),
        )
        self._produce(topics.DLQ, msg.key(), record.model_dump_json().encode())

    def _route_late(self, event: ParkingEvent, state: LotState) -> None:
        metrics.EVENTS_LATE.inc()
        assert state.watermark is not None
        window_start = (int(event.event_ts.timestamp()) // self._params.window_size_s) * (
            self._params.window_size_s
        )
        record = LateEvent(
            event=event,
            window_start=utc(window_start),
            watermark=utc(state.watermark),
            late_by_s=round(state.watermark - event.event_ts.timestamp(), 3),
            routed_at=utc(time.time()),
        )
        self._produce(topics.LATE, event.lot_id.encode(), record.model_dump_json().encode())

    # ---- outputs --------------------------------------------------------------------------------

    def _publish_closed(self, state: LotState) -> None:
        # A closed result is final, so never emit it with a guessed capacity: look the lot up
        # now, ignoring the refresh throttle, if its metadata has not been seen yet.
        self._sync_capacity(state, force=bool(state.closed) and state.capacity == 0)
        for result in state.drain_closed(time.time()):
            self._produce_result(result)

    def _produce_result(self, result: WindowResult) -> None:
        self._produce(topics.OCCUPANCY_5MIN, result.key.encode(), result.model_dump_json().encode())

    def _emit(self) -> None:
        """Push open-window updates (dashboard freshness) and sensor-offline alerts."""
        now = time.time()
        for lots in self._lots.values():
            for state in lots.values():
                self._sync_capacity(state)
                for result in state.dirty_open_results(now):
                    self._produce_result(result)
                for ingest_ts in state.drain_pending_ingest():
                    metrics.INGEST_TO_EMIT.observe(max(0.0, now - ingest_ts))
                for signal_ in state.sensor_signals():
                    self._produce_alert(state.lot_id, signal_)
        self._update_gauges()

    def _produce_alert(self, lot_id: str, sig: SensorSignal) -> None:
        silent_min = (sig.now - sig.silent_since) / 60
        raised = sig.offline
        alert = Alert(
            alert_id=f"{lot_id}|{sig.sensor_id}|{int(sig.silent_since)}",
            kind=AlertKind.SENSOR_OFFLINE,
            state=AlertState.RAISED if raised else AlertState.CLEARED,
            lot_id=lot_id,
            sensor_id=sig.sensor_id,
            ts=utc(sig.now),
            message=(
                f"Sensor {sig.sensor_id} silent for {silent_min:.0f} min"
                if raised
                else f"Sensor {sig.sensor_id} reporting again"
            ),
        )
        self._produce(topics.ALERTS, lot_id.encode(), alert.model_dump_json().encode())

    def _produce(self, topic: str, key: bytes | None, value: bytes, partition: int = -1) -> None:
        while True:
            try:
                self._producer.produce(
                    topic, key=key, value=value, partition=partition, on_delivery=self._on_delivery
                )
                break
            except BufferError:
                self._producer.poll(0.2)
        metrics.OUTPUTS.labels(topic).inc()

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._delivery_errors += 1
            self._log.error("delivery failed", error=str(err))

    # ---- checkpoint + commit ---------------------------------------------------------------------

    def _checkpoint(self, partitions: list[int]) -> None:
        """produce outputs (already queued) -> flush -> write checkpoints -> commit offsets."""
        started = time.monotonic()
        owned = [p for p in partitions if p in self._lots]
        for p in owned:
            for state in self._lots[p].values():
                self._publish_closed(state)
        self._flush_or_die()

        size = 0
        for p in owned:
            for state in self._lots[p].values():
                state.resume_offset = self._next_offset.get(p, state.resume_offset)
                payload = serialise(state)
                size += len(payload)
                self._produce(topics.CHANGELOG, state.lot_id.encode(), payload, partition=p)
        self._flush_or_die()

        offsets = [
            TopicPartition(topics.RAW, p, self._next_offset[p])
            for p in owned
            if p in self._next_offset
        ]
        if offsets:
            commit_tolerant(self._consumer, offsets, self._log)
        metrics.CHECKPOINT_SECONDS.observe(time.monotonic() - started)
        metrics.CHECKPOINT_BYTES.set(size)

    def _flush_or_die(self) -> None:
        """A checkpoint must never claim outputs that were not delivered: fail and be replayed."""
        remaining = self._producer.flush(FLUSH_TIMEOUT_S)
        if remaining or self._delivery_errors:
            raise RuntimeError(
                f"{remaining} undelivered, {self._delivery_errors} failed: refusing to checkpoint"
            )

    # ---- helpers -------------------------------------------------------------------------------

    def _refresh_metadata(self, force: bool = False) -> None:
        if not force and time.monotonic() - self._metadata_loaded_at < METADATA_REFRESH_MIN_S:
            return
        self._metadata = load_metadata(self._s.kafka_bootstrap)
        self._metadata_loaded_at = time.monotonic()

    def _sync_capacity(self, state: LotState, force: bool = False) -> None:
        if state.capacity == 0:
            self._refresh_metadata(force=force)
        meta = self._metadata.get(state.lot_id)
        if meta is not None:
            state.capacity = meta.capacity

    def _update_gauges(self) -> None:
        metrics.OWNED_PARTITIONS.set(len(self._lots))
        metrics.OWNED_LOTS.set(sum(len(v) for v in self._lots.values()))
        metrics.OPEN_WINDOWS.set(
            sum(len(s.windows) for v in self._lots.values() for s in v.values())
        )
