"""Kafka -> TimescaleDB sink: python -m sink.main

Loop: consume a batch -> one DB transaction of idempotent upserts -> commit offsets.
Offsets are committed only after the DB commit, so a crash or a database outage replays
the batch and the upserts make the replay harmless."""

import signal
import time
from types import FrameType

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, TopicPartition
from prometheus_client import Counter, Histogram, start_http_server

from common import topics
from common.config import Settings
from common.kafka import commit_tolerant, consumer_config
from common.logging import configure_logging
from common.schemas import Alert, LotMetadata, WindowResult
from sink.db import merge_windows, write_batch

BATCH_MAX = 500
BATCH_MAX_WAIT_S = 1.0
DB_RETRY_BACKOFF_S = (1, 2, 5, 10)
METRICS_PORT = 8001

ROWS = Counter("sink_rows_written_total", "Rows sent to the database", ["table"])
BATCHES = Counter("sink_batches_total", "Committed batches")
DB_ERRORS = Counter("sink_db_errors_total", "Failed database attempts")
INVALID = Counter("sink_invalid_messages_total", "Messages that failed validation")
BATCH_SECONDS = Histogram("sink_batch_seconds", "Database time per batch")


class Sink:
    def __init__(self, settings: Settings) -> None:
        self._s = settings
        self._log = configure_logging("sink")
        self._consumer = Consumer(
            consumer_config(settings.kafka_bootstrap, "timescale-sink", "timescale-sink")
        )
        self._conn: psycopg.Connection | None = None
        self._stop = False

    def _connect(self) -> psycopg.Connection:
        return psycopg.connect(self._s.postgres_dsn, connect_timeout=5)

    def _write_with_retry(
        self, windows: list[WindowResult], alerts: list[Alert], lots: list[LotMetadata]
    ) -> None:
        attempt = 0
        while True:
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = self._connect()
                started = time.monotonic()
                write_batch(self._conn, windows, alerts, lots)
                BATCH_SECONDS.observe(time.monotonic() - started)
                return
            except psycopg.Error as exc:
                DB_ERRORS.inc()
                wait = DB_RETRY_BACKOFF_S[min(attempt, len(DB_RETRY_BACKOFF_S) - 1)]
                self._log.warning(
                    "database unavailable, will retry", error=str(exc)[:200], wait_s=wait
                )
                if self._conn is not None:
                    self._conn.close()
                    self._conn = None
                attempt += 1
                time.sleep(wait)
                if self._stop:
                    raise

    def run(self) -> None:
        signal.signal(signal.SIGTERM, self._request_stop)
        signal.signal(signal.SIGINT, self._request_stop)
        start_http_server(METRICS_PORT)
        self._consumer.subscribe([topics.OCCUPANCY_5MIN, topics.ALERTS, topics.LOT_METADATA])
        self._log.info("started", group="timescale-sink")

        while not self._stop:
            msgs = self._consumer.consume(BATCH_MAX, BATCH_MAX_WAIT_S)
            if msgs:
                self._process(msgs)
        self._consumer.close()
        self._log.info("stopped")

    def _request_stop(self, _signum: int, _frame: FrameType | None) -> None:
        self._stop = True

    def _process(self, msgs: list[Message]) -> None:
        windows: list[WindowResult] = []
        alerts: list[Alert] = []
        lots: list[LotMetadata] = []
        offsets: dict[tuple[str, int], int] = {}
        for msg in msgs:
            err = msg.error()
            if err is not None:
                if err.code() != KafkaError._PARTITION_EOF:
                    raise KafkaException(err)
                continue
            topic, partition, offset = msg.topic(), msg.partition(), msg.offset()
            if topic is None or partition is None or offset is None:
                continue
            offsets[(topic, partition)] = offset + 1
            try:
                value = msg.value() or b""
                if topic == topics.OCCUPANCY_5MIN:
                    windows.append(WindowResult.model_validate_json(value))
                elif topic == topics.ALERTS:
                    alerts.append(Alert.model_validate_json(value))
                else:
                    lots.append(LotMetadata.model_validate_json(value))
            except ValueError as exc:
                INVALID.inc()
                self._log.error(
                    "invalid message skipped", topic=topic, offset=offset, error=str(exc)[:200]
                )

        windows = merge_windows(windows)
        self._write_with_retry(windows, alerts, lots)
        ROWS.labels("lot_occupancy_5min").inc(len(windows))
        ROWS.labels("alerts").inc(len(alerts))
        ROWS.labels("lot_metadata").inc(len(lots))
        BATCHES.inc()
        commit_tolerant(
            self._consumer,
            [TopicPartition(t, p, o) for (t, p), o in offsets.items()],
            self._log,
        )


def main() -> None:
    Sink(Settings.from_env()).run()


if __name__ == "__main__":
    main()
