"""Prometheus metrics exposed by every processor instance on /metrics."""

from prometheus_client import Counter, Gauge, Histogram

EVENTS_PROCESSED = Counter("parking_events_processed_total", "Valid events accepted into state")
EVENTS_DLQ = Counter("parking_events_dlq_total", "Messages sent to parking.dlq")
EVENTS_LATE = Counter("parking_events_late_total", "Events routed to parking.late")
EVENTS_DUPLICATE = Counter("parking_events_duplicate_total", "Events dropped as duplicates")
EVENTS_REPLAY_SKIPPED = Counter(
    "parking_events_replay_skipped_total", "Events skipped because a restored checkpoint has them"
)
OUTPUTS = Counter("parking_outputs_total", "Messages produced", ["topic"])
REBALANCES = Counter("parking_rebalances_total", "Consumer group rebalance callbacks", ["kind"])
OPEN_WINDOWS = Gauge("parking_open_windows", "Windows currently open on this instance")
OWNED_PARTITIONS = Gauge("parking_owned_partitions", "parking.raw partitions assigned here")
OWNED_LOTS = Gauge("parking_owned_lots", "Lots whose state lives on this instance")
# Raw ingestion latency. The producer stamps `ingest_ts` when it produces an event; this measures
# the time until the processor has the event in hand, with no windowing involved. INGEST_TO_EMIT
# below is different: it also includes waiting for the next 5 s emit. Both use wall-clock time, so
# producer and processor hosts need synchronised clocks (they share a host in the demo).
INGEST_TO_CONSUME = Histogram(
    "parking_ingest_to_consume_seconds",
    "Time from an event being produced (ingest_ts) to the processor consuming it; sampled 1 in 8",
    buckets=(0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
)
INGEST_TO_EMIT = Histogram(
    "parking_ingest_to_emit_seconds",
    "Time from a sampled event being produced to the emit that reflects it (incl. emit wait)",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300),
)
CHECKPOINT_SECONDS = Histogram(
    "parking_checkpoint_seconds",
    "Duration of flush + checkpoint + commit",
    buckets=(0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
CHECKPOINT_BYTES = Gauge("parking_checkpoint_bytes", "Size of the last checkpoint written")
RESTORE_SECONDS = Histogram(
    "parking_restore_seconds",
    "Time to rebuild state for newly assigned partitions",
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
)


def record_ingest_latency(ingest_ts: float, now: float) -> None:
    """Observe produce-to-consume latency; a producer clock slightly ahead of ours clamps to 0."""
    INGEST_TO_CONSUME.observe(max(0.0, now - ingest_ts))
