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
