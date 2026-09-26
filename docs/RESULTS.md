# Measured results

Every number below was produced by a script or command in this repo; the command is recorded next to it.
Environment: MacBook (Apple Silicon, aarch64), Docker Desktop with 8 GB / 10 CPUs.

## Phase 0: cluster bring-up
- `docker compose up -d` from clean images: all three brokers healthy in ~20 s, init container exit 0.
- `kafka-topics --describe`: `parking.raw` leaders spread across brokers 1/2/3 (2 partitions each), ISR = 1,2,3 for every partition, 0 under-replicated partitions.

## Phase 1: simulator
Commands run against the live 3-broker cluster (fresh topics each time).

| Check | Command | Result |
|---|---|---|
| Partition evenness, 12 balanced lots | `python -m simulator.main --duration 30 --speed 60` | 43,552 events: partitions 0-5 = 7206 / 7348 / 7286 / 7216 / 7267 / 7229 (spread +-1%) |
| Same test with plain `LOT-01..LOT-12` ids (before D6) | same | 15,672 / 7,798 / 7,921 / 4,002 / 3,917 / 7,909: partition 0 got 2x, partitions 3-4 half |
| `--malformed-pct 5` | `--speed 20 --malformed-pct 5` | simulator sent 612 malformed; `topic_stats` found 612 that fail validation |
| `--dup-pct 5` | same run | simulator sent 607 duplicates; `topic_stats` found 607 repeated event_ids |
| `--sensor-dropout LOT-01:S-A-014` | same run | 16 messages suppressed; `topic_stats` lists LOT-01/S-A-014 as silent > 60 s |
| `--late-pct 20 --late-max-s 40` | `--speed 1 --duration 60` | simulator held 43 events; 42 seen with `ingest_ts - event_ts` > 15 s (one released by the shutdown flush before 15 s) |
| `--burst 10` | `--speed 1 --burst-every 20 --burst-len 8` | ~24 events/s baseline, 70-74 events/s in burst windows |
| Replay, Birmingham dataset | `--mode replay --lots 30 --speed 1000 --replay-days 1` | 1 day of 30 car parks replayed in 31.5 s wall, 4,093,030 events, 0 delivery errors. Partition load is skewed (392k to 1.14M) because 30 real ids hash unevenly. |

## Phase 2: stream processor
Setup: 2 processor containers (group `occupancy-processor`), 12 synthetic lots x 60 slots, `--speed 60` (5 event-minutes per 5 wall-seconds), simulator ground truth via `--truth-file`, compared by `scripts/verify_windows.py` (closed windows only; the partial first window and windows the watermark had not yet closed are excluded).

| Test | Result |
|---|---|
| Steady state, 45 s run | 96 closed windows compared: 0 missing, 0 mismatches. Time-weighted average within 0.05 slots, entries and exits exactly equal to ground truth. Consumer lag 0 on all 6 partitions. |
| Partition assignment, 2 instances | 3 partitions each (0,2,4 / 1,3,5) |
| **Processor crash**: `docker kill` (SIGKILL) of one of two instances at ~35 s of a 90 s run | Survivor was assigned the dead instance's partitions ~12 s later (session timeout 10 s + rebalance), restored 6 lots from `state.changelog` and resumed at offsets 6768 to 6806. Verifier: **192 windows compared, 0 missing, 0 mismatches**. At most 3 result messages per window key (re-emission after replay), which the sink's upsert collapses to one row. |
| Graceful SIGTERM | Logged `shutting down: final emit, checkpoint, commit`, then `partitions revoked: checkpointing before hand-off`, then `stopped`, all within ~17 ms of the signal; exit code 0. |
| HLL vs exact (windows of a few vehicles) | mean error 0.68%, max 25% (a window with 4 vehicles estimated as 5). Large-cardinality accuracy is measured in Phase 8. |

## Phase 3: TimescaleDB and sink
| Test | Result |
|---|---|
| Schema | `lot_occupancy_5min` hypertable (compression enabled, segmentby `lot_id`), `alerts`, `lot_metadata`, continuous aggregate `lot_occupancy_hourly`, compression/retention/refresh policies created by `infra/timescale/init.sql` on first start. |
| Steady state, 45 s run at `--speed 60` | 96 closed windows compared to ground truth from the database: 0 missing, 0 mismatches. 120 rows, 120 distinct keys (108 closed, 12 still open). |
| **Sink outage**: `docker compose stop timescaledb` at 21:01:27, restart at 21:02:27 (60 s), simulator running throughout (120 s at `--speed 60`, 2 processors) | Sink logged 9 retry attempts while the DB was down and committed no offsets. After restart the group `timescale-sink` drained to **lag 0**. Verifier on the DB: **264 windows compared, 0 missing, 0 mismatches**; 300 rows = 300 distinct `(lot_id, window_start)` keys, i.e. **0 lost, 0 duplicated**. |
| HLL on tiny windows | Over 120 windows one had 2 real vehicles estimated as 1 (both hashed into the same register). Expected for HLL at n=2 (about 0.1% chance per window); at n>=50 the unit test bounds the error to 3 standard errors. |

## Phase 4: alerter
Scenario: `--start 2026-09-28T04:30:00Z` (Monday 10:00 IST) `--speed 60 --duration 170`, i.e. 10:00 to 12:50 simulated, so office lots sit at their ~92% peak and the lunch dip pulls them down; sensor `LOT-01:S-A-014` silenced after 20 event-minutes. Group `alerter` + `sink` + 2 processors, webhook receiver on the host.

| Check | Result |
|---|---|
| FULL_LOT raised | at exactly 54/60 = 90% (e.g. `LOT-11 full: 90% (54/60 slots)`) |
| FULL_LOT cleared | at 51/60 = 85% or below (e.g. `LOT-04 has space again: 85% (51/60 slots)`), never between 85% and 90% |
| Re-raise after clear | `LOT-06` and `LOT-03` raised a second alert with a new `alert_id` after clearing |
| Totals in `alerts` table | 10 FULL_LOT rows (8 cleared, 2 active) + 1 SENSOR_OFFLINE row (`Sensor S-A-014 silent for 6 min`) |
| Webhook deliveries | **19** = 10 RAISED + 8 CLEARED + 1 sensor-offline, matching the table exactly |
| Invalid messages skipped | 0 |

Bug found by running it for real: the first version of the alerter passed `lot_id` and the JSON to `Producer.produce()` positionally, which swapped key and value (the second positional argument is the value). Unit tests could not catch it; the run did (consumer logged `Invalid JSON ... input_value=b'LOT-07'`). Fixed by using keyword arguments everywhere; the Phase 9 integration test asserts alert payloads round-trip.

## Phase 7: metrics
Setup: Prometheus scraping 2 processors + sink + alerter + kafka-exporter (5 targets, all `up`). Scenario A: 90 s at `--speed 5 --malformed-pct 5 --dup-pct 5 --late-pct 10 --late-max-s 400`, clean state.

| Metric vs. what the simulator reported | Result |
|---|---|
| `parking_events_dlq_total` vs simulator `malformed` | **573 = 573** |
| `parking_events_duplicate_total` vs simulator `duplicates` | **550 = 550** |
| `parking_events_late_total` | 24 (81 events were delayed 15-400 s; delays under the 180 s lateness+grace are accepted into their windows, the rest go to `parking.late`; `parking.late` end offset also 24) |
| `kafka_consumergroup_lag` | 0 for `occupancy-processor`, `timescale-sink`, `alerter` after the run |
| `parking_rebalances_total{kind="assign"}` | 2 (one per instance at start) |

Scenario B, 60 s at `--speed 5` (12 lots, ~120 events/s, 2 processors), corrected latency metric:

| Metric | Value |
|---|---|
| Ingest to emit p50 / p95 / p99 | **2.8 s / 6.0 s / 9.2 s** (the 5 s emit interval dominates: p50 is about half of it) |
| Checkpoint duration p95, size | 21 ms, about 200 KB per instance |

A first measurement of ingest-to-emit read p95 = 29 s. That was a flaw in the metric (see D18), not the pipeline. A second early run also showed 737 late events because the processors still held lot state whose event time was two days in the future from a previous scenario: event-time state must be reset (`make reset-topics`) between scenarios that use a different `--start`.
