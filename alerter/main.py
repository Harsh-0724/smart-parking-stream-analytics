"""Alerter: python -m alerter.main

Consumes lot.occupancy.5min and raises/clears FULL_LOT alerts (with hysteresis) on lot.alerts.
Consumes lot.alerts (its own and the processor's sensor-offline alerts) and notifies.
Runs as a single instance: hysteresis state is per lot and a lot's results span partitions."""

import signal
from types import FrameType

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer, TopicPartition
from prometheus_client import Counter, start_http_server

from alerter.hysteresis import FullLotDetector
from alerter.notifiers import build_notifiers, dispatch
from common import topics
from common.config import Settings
from common.kafka import commit_tolerant, consumer_config, producer_config, read_to_end
from common.logging import configure_logging
from common.schemas import Alert, WindowResult

BATCH_MAX = 200
POLL_TIMEOUT_S = 0.5
METRICS_PORT = 8002

RAISED = Counter("alerter_alerts_total", "Alerts published by the alerter", ["state"])
NOTIFIED = Counter("alerter_notifications_total", "Alerts handed to notifiers")
NOTIFY_FAILURES = Counter("alerter_notification_failures_total", "Failed channel deliveries")


def main() -> None:
    settings = Settings.from_env()
    log = configure_logging("alerter")
    detector = FullLotDetector(settings.alert_full_threshold, settings.alert_clear_threshold)
    notifiers = build_notifiers(settings, log)

    history = [
        Alert.model_validate_json(value)
        for _, value, _ in read_to_end(settings.kafka_bootstrap, topics.ALERTS, 0)
        if value
    ]
    detector.restore(history)

    producer = Producer(producer_config(settings.kafka_bootstrap, "alerter"))
    consumer = Consumer(consumer_config(settings.kafka_bootstrap, "alerter", "alerter"))
    consumer.subscribe([topics.OCCUPANCY_5MIN, topics.ALERTS])
    start_http_server(METRICS_PORT)

    stop = False

    def _stop(_signum: int, _frame: FrameType | None) -> None:
        nonlocal stop
        stop = True

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    log.info(
        "started",
        channels=[n.name for n in notifiers],
        restored_alerts=len(history),
        raise_at=settings.alert_full_threshold,
        clear_at=settings.alert_clear_threshold,
    )

    while not stop:
        offsets: dict[tuple[str, int], int] = {}
        for msg in consumer.consume(BATCH_MAX, POLL_TIMEOUT_S):
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
                if topic == topics.OCCUPANCY_5MIN:
                    alert = detector.observe(WindowResult.model_validate_json(msg.value() or b""))
                    if alert is not None:
                        producer.produce(
                            topics.ALERTS,
                            key=alert.lot_id.encode(),
                            value=alert.model_dump_json().encode(),
                        )
                        RAISED.labels(alert.state.value).inc()
                else:
                    alert = Alert.model_validate_json(msg.value() or b"")
                    NOTIFY_FAILURES.inc(dispatch(alert, notifiers, log))
                    NOTIFIED.inc()
            except ValueError as exc:
                log.error(
                    "invalid message skipped", topic=topic, offset=offset, error=str(exc)[:200]
                )
        if offsets:
            # Alerts must be durably on lot.alerts before the occupancy offsets that caused them
            # are committed; a crash in between re-derives the alert from restored state.
            if producer.flush(30):
                raise RuntimeError("alerts not delivered; refusing to commit offsets")
            commit_tolerant(
                consumer, [TopicPartition(t, p, o) for (t, p), o in offsets.items()], log
            )
    consumer.close()
    log.info("stopped")


if __name__ == "__main__":
    main()
