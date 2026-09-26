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
INGEST_TO_EMIT = Histogram(
    "parking_ingest_to_emit_seconds",
    "Wall-clock time from a sampled event being produced to the emit that reflects it",
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
