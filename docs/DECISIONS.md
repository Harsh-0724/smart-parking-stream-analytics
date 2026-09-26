# Decisions

ADR-style log. Decisions already fixed in `CLAUDE.md` (Python 3.12, confluent-kafka, own windowing,
3-broker KRaft, TimescaleDB) are not repeated here.

## D1. One Python project, one Docker image for all backend services
- **Decision:** a single `pyproject.toml` at the repo root with packages `common`, `simulator`, `processor`, `sink`, `alerter`; one Dockerfile parameterised by the module to run.
- **Alternatives:** a uv workspace with a package per service.
- **Why:** services share `common/` heavily; one lockfile and one CI type-check pass is simpler to explain and faster to build.

## D2. Kafka image `apache/kafka:3.9.0`
- **Why:** official image, KRaft-native, stable. Version is pinned so the demo is reproducible.

## D3. Two listeners per broker
- `INTERNAL` (`kafka-N:9092`) for containers and inter-broker traffic; `EXTERNAL` (`localhost:N9092`) so host tools (simulator, chaos scripts) can connect. Set `KAFKA_BOOTSTRAP_HOST` to use the latter.

## D4. Configuration via a frozen dataclass reading environment variables
- **Alternative:** `pydantic-settings`. **Why not:** one more dependency for ~15 variables.

## D5. Event validation rules go beyond field types
- `OCCUPIED` requires `vehicle_token`, `FREE` forbids it, `HEARTBEAT` carries neither; timestamps must be timezone-aware. Inconsistent events go to the DLQ rather than corrupting state.

## D6. Lot ids are chosen so they hash evenly across the 6 partitions
- **Problem:** the key is `lot_id` (required so one lot's events stay ordered on one partition). With ~12 keys, `LOT-01..LOT-12` hash 4/1/3/0/2/2 under Kafka's murmur2, so one partition would be idle: it looks like a broken partitioning demo.
- **Decision:** `common.partitioning.balanced_lot_ids` picks the first ids of the form `LOT-NN` that fill partitions evenly (12 lots, 2 per partition). The producer uses `partitioner=murmur2_random`, the Java-compatible one, so the calculation is verifiable against Kafka.
- **Trade-off:** with a handful of keys, key-based partitioning is inherently skewed; real datasets (the Birmingham replay, 30 lots) show that skew and it is reported as-is in RESULTS.md.

## D7. Simulator time model
- `--speed N` runs event time N x faster than the wall clock. `event_ts` is event time; `ingest_ts` is always the wall clock at production. So at speed > 1 `event_ts` runs ahead of `ingest_ts`; end-to-end latency is measured from `ingest_ts`, never from `event_ts`.
- Synthetic demand is a per-personality piecewise-linear occupancy target by local hour (Asia/Kolkata, weekday vs weekend). Arrivals are Poisson with rate = Little's-law churn + a term that closes any gap to the target; departures are log-normal dwell times plus forced departures when occupancy is above target (evening drain).
- The first report of each slot is a *sync* event (state initialisation, not an entry). The processor and the simulator's truth tracker both treat it that way.

## D8. Fault flags add to the stream instead of replacing it
- `--dup-pct` and `--malformed-pct` emit *extra* messages, so valid data is unchanged and counts are exact: the DLQ must hold exactly as many messages as `malformed` in the simulator summary. `--late-pct` delays the same event (original `event_ts`, new `ingest_ts`), it does not add one.
- `--burst M` multiplies heartbeat rate and churn by M for `--burst-len` seconds every `--burst-every` seconds. It ramps over up to one heartbeat interval.
- `--sensor-dropout [LOT:]SENSOR` silences a sensor (heartbeats and events) after `--dropout-after` event-time seconds.
- `--truth-file` writes an independent ground-truth table per (lot, window) so processor output can be checked against the ideal answer.

## D9. Replay mode
- Birmingham snapshots (every ~30 min) become per-slot events: |delta| arrivals or departures at jittered times inside the interval. Capacity is scaled by `--replay-scale` (default 0.1) to keep the sensor count manageable; timestamps are shifted so the first snapshot lands at "now".
- The dataset is not committed (`data/` is gitignored). `make fetch-data` downloads it from UCI.

## D10. Delivery semantics: at-least-once processing, effectively-once results
- **What the processor guarantees:** outputs are produced and flushed, then the checkpoint is written to `state.changelog`, then offsets are committed. A crash between any two steps makes the next owner reprocess events since the last checkpoint, so window results can be emitted more than once.
- **What makes the stored result correct anyway:** every output is keyed by `(lot_id, window_start)` and the sink upserts on that key, so a repeated result overwrites itself. The sink never lets a closed window be overwritten by an open one.
- **What we do NOT claim:** Kafka exactly-once semantics (transactions). Outputs to `parking.dlq`, `parking.late` and `lot.alerts` may be duplicated after a crash; alert ids are deterministic (`lot|sensor|silent_since`) so consumers can dedupe.
- **Alternatives:** Kafka transactions (`read_committed` + transactional producer + `send_offsets_to_transaction`). Rejected: the checkpoint would have to be part of the transaction, the changelog is the source of truth for state, and the brief asks for this explicit commit order.

## D11. Checkpoint format and restore
- One JSON message per lot on `state.changelog`, key = `lot_id`, **written to the same partition number as the lot's `parking.raw` partition** (explicit partition). Restoring partition N means reading changelog partition N to the end (compacted, latest per lot wins).
- Each lot checkpoint carries `resume_offset` = next raw offset at checkpoint time. `on_assign` seeks to the *smallest* `resume_offset` in the partition, and each lot skips messages below its own `resume_offset`. This is exact even if a crash interrupted a checkpoint half-way (some lots at cycle N, some at N-1). The dedupe set is then only needed for real producer duplicates, not for replay, so its TTL does not have to cover a replay span.
- A checkpoint is refused (process exits, restarts, replays) if any produced output is undelivered.

## D12. Event-time rules (all configurable)
- Watermark per lot = `max(event_ts seen) - ALLOWED_LATENESS_S`. An OCCUPANCY event is **late** iff `window_end + WINDOW_GRACE_S < watermark`; it goes to `parking.late` and does not touch state. A window is **closed** (final `closed=true` result) when that same inequality holds for it, so an event is accepted exactly while its window is still open.
- Defaults 60 s lateness + 120 s grace on 5-minute windows: a window closes once event time passes `end + 180 s`.
- Dedupe runs before the late check, so a duplicate of a late event is counted as a duplicate, not late twice. `DEDUPE_TTL_S >= lateness + grace` is enforced at startup.
- Heartbeats only advance event time and refresh sensor liveness; they are never "late".
- Event time is per lot. A lot that receives no events at all never advances, so its last window stays open (no wall-clock idle timeout). Sensor-offline detection is likewise in event time.

## D13. Windowed metrics
- **Average occupancy is time-weighted:** the lot's occupied count is integrated over event time into each window (`area / covered seconds`). A late-in-grace event corrects the already-integrated time retroactively, including in later still-open windows, so the result equals what in-order processing would produce.
- **Entries/exits** count real transitions (known FREE to OCCUPIED and back). A slot's first report only initialises state, it is never an entry; the simulator therefore reports every slot (free ones too) at startup, like a sensor booting.
- **Slot-state regression guard:** an event with `event_ts <=` the slot's last event_ts is ignored for state, but its vehicle token still enters the window's HyperLogLog (a sighting is a sighting).
- **min/max occupied** are observed on the forward timeline only; a retroactive correction does not rewrite past extremes.
- **Unique vehicles** use HyperLogLog p=10 (1 KiB per window, 3.25% standard error), serialised as registers inside the checkpoint.

## D14. Metrics
- Latency is measured as wall-clock `ingest_ts -> emit`, not `event_ts -> emit`: with `--speed > 1` or replay, `event_ts` is on the simulated timeline and the difference would be meaningless.
- Metrics are per-instance on `:8000` (Prometheus scrapes each replica); no `lot` label to keep cardinality flat.

## D15. Sensor-offline alerts are produced by the processor
- CLAUDE.md Phase 2 places them there (only the processor holds per-sensor state and knows event time). The alerter (Phase 4) adds the full-lot alerts and owns notification.

## D16. Sink design
- One consumer (group `timescale-sink`) reads `lot.occupancy.5min`, `lot.alerts` and `lot.metadata`; per batch (up to 500 messages or 1 s) it opens **one DB transaction**, commits, and only then commits Kafka offsets. Batches are collapsed to one result per key before writing.
- **Idempotence lives in SQL, not in Kafka.** Windows: `ON CONFLICT (lot_id, window_start) DO UPDATE ... WHERE NOT (existing.closed AND NOT excluded.closed)`, so a replayed open update can never reopen a closed window. Alerts: RAISED is `DO NOTHING` on conflict (a replay cannot resurrect a cleared alert), CLEARED sets `cleared_at` once.
- **Database outage:** the batch is kept in memory and retried with backoff (1, 2, 5, 10 s); offsets are not committed, so if the sink itself dies the batch is replayed from Kafka. A 60 s outage is well inside `max.poll.interval.ms` (5 min).
- Messages that fail validation are skipped and counted (`sink_invalid_messages_total`) instead of crash-looping the sink: these topics are written by our own processor, so a failure means a bug, and stopping the whole sink for one bad record would be worse.
- Compression policy after 7 days and retention after 90 days on the hypertable; an hourly continuous aggregate over closed windows (real-time aggregation on for the most recent hour). Compression is safe with upserts because windows stop changing minutes after they close.
