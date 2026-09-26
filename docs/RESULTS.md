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

## Bugs and flaws found by running things for real
Every item below was found by executing the system (or by reviewing code that had only been unit-tested), not by the spec. They are the honest evidence for CO3 and CO4.

| # | Where | What went wrong | How it was found | Fix and guard |
|---|---|---|---|---|
| 1 | Alerter | `Producer.produce(topic, lot_id, json)` passed key and value positionally, which swapped them (the second positional argument is the value). | Live run: consumer logged `Invalid JSON ... input_value=b'LOT-07'`. Unit tests cannot see it. | Keyword arguments everywhere; the integration test round-trips alert payloads. |
| 2 | Processor | Metadata refresh was rate-limited to 5 s, so a lot whose metadata arrived just after startup was processed with unknown capacity and its **closed** windows were emitted with a guessed capacity, never corrected. | Integration test from a clean stack (a warm stack hid it). | Closed windows force an unthrottled metadata lookup before emission (`ProcessorApp._publish_closed`). |
| 3 | API feed | A malformed message on `parking.raw` made `json.loads` raise inside the feed thread, which would have restarted it in a loop. | Code review while writing the API integration test. | `parse_slot_event` ignores anything malformed; unit test with five malformed shapes; integration test sends one and checks the live feed survives. |
| 4 | Simulator | A patch added `args.skip_weekend` but the matching `add_argument` silently did not apply after auto-formatting; mypy cannot see `Namespace` attributes, so the container crash-looped. | `docker compose` run: `unrecognized arguments: --skip-weekend`. | Added the argument and a test that parses every flag the Compose file passes to the simulator. |
| 5 | API | `threading.Thread` subclass attributes named `_stop` and `_bootstrap` shadowed `Thread` internals (`TypeError: first arg must be callable`). | First container start. | Renamed. |
| 6 | Prod compose | Compose merges list fields, so `web` would have published the dev port 8081 in production. | `docker compose config --format json` on the merged files. | `ports: !override`; the check now prints published ports per service. |
| 7 | CI | Job names containing `: ` made the workflow YAML invalid; GitHub reported a failed run with no jobs. | First push. | Quoted the names; YAML validated locally with PyYAML before pushing. |
| 8 | Measurement | Committed-offset lag saw-tooths up to 10 s of traffic because commits happen only at checkpoints, so drain times were meaningless. | Lag-spike scenario. | Backlog is now end offsets minus events consumed (see "Measurement notes" below). |
| 9 | Measurement | "Graceful stop" of a broker reports a 1.7 s election because the broker hands off leadership before exiting; a crash takes 9 s. | Broker scenario. | The scenario defaults to SIGKILL and keeps the graceful run as a labelled data point. |
| 10 | Measurement | The first latency metric measured the age of a quiet window's last occupancy event (p95 29 s). | Prometheus check. | Per-event ingest-to-emit sampling (D18). |
| 11 | Demo | Rebuilding images mid-run recreated the simulator and processors with a fresh clock; a restart with a different `--start` leaves processors with a stale watermark. | Frontend session. | `make demo-reset`; the first pre-demo step in DEMO.md. |

## Phase 8: chaos runs (appended by scripts/chaos_*.py)

### Processor crash: PASS (2026-09-26 16:00 UTC)
SIGKILL one of 2 processors at ~35 s of a 90 s run (speed 60)

| Result | Expectation | Measured |
|---|---|---|
| PASS | survivor assigned the 3 orphaned partitions within 30 s | 11.5 s |
| PASS | state restored from state.changelog | 6 lots restored |
| PASS | consumer lag drains to 0 | 5 s |
| PASS | no gaps: every closed window is in the database | missing=0 |
| PASS | no wrong values vs ground truth | 204 windows compared, 0 mismatches |
| PASS | no duplicate rows | 228 rows / 228 keys |

- takeover time (kill to partitions assigned): 11.5 s
- simulator events sent: 130499

### Sink outage: PASS (2026-09-26 16:05 UTC)
stop TimescaleDB for 60 s at ~30 s of a 120 s run (speed 60)

| Result | Expectation | Measured |
|---|---|---|
| PASS | sink accumulated lag while the database was down | lag=420 |
| PASS | database reachable again | 1 s |
| PASS | consumer lag drains to 0 after restart | 9 s |
| PASS | 0 lost: every closed window is in the database | missing=0 |
| PASS | 0 wrong values vs ground truth | 276 windows compared, 0 mismatches |
| PASS | 0 duplicated rows | 300 rows / 300 keys |

- sink lag at end of outage: 420
- time to drain after restart: 9 s

### Bad data: PASS (2026-09-26 16:07 UTC)
--malformed-pct 5 for 90 s (speed 60), 2 processors

| Result | Expectation | Measured |
|---|---|---|
| PASS | consumer lag drains to 0 |  |
| PASS | every malformed message is in parking.dlq | sent 6482, DLQ holds 6482 |
| PASS | processor DLQ counter agrees | metric=6482 |
| PASS | DLQ records carry the original bytes and an error reason | 50 sampled |
| PASS | processors never crashed or restarted | restart counts ['0', '0'] |
| PASS | valid data unaffected: windows match ground truth | 192 windows compared, 0 mismatches, 0 missing |

- messages sent / malformed: 136959 / 6482

### Late and duplicate data: PASS (2026-09-26 16:12 UTC)
--late-pct 10 --dup-pct 5, in-grace (A) and beyond-grace (B) delays

| Result | Expectation | Measured |
|---|---|---|
| PASS | A: every duplicate dropped | sent 7314, dropped 7314 |
| PASS | A: nothing late enough for parking.late | late=0, 98 events were delayed |
| PASS | A: in-grace late events land in the right windows (matches ground truth) | 228 windows compared, 0 mismatches, 0 missing |
| PASS | B: every duplicate dropped | sent 7208, dropped 7208 |
| PASS | B: beyond-grace events routed to parking.late | 29 routed of 87 delayed (the rest were inside the grace period) |
| PASS | B: every parking.late record is past its window's close time | 29 records |

- B: worst lateness recorded: 527 s behind the watermark

### Measurement notes from developing the lag-spike scenario
Five earlier runs were discarded because the method, not the pipeline, was wrong. They are recorded because each is a methodology lesson for the paper:
1. **Committed-offset lag is a poor backlog signal.** `kafka-consumer-groups` lag is measured against committed offsets; the processor commits only at 10 s checkpoints, so lag saw-toothed up to ~190,000 events and collapsed at each commit, making drain times meaningless. Backlog is now `raw end offsets - events the processors report consuming`.
2. **The synthetic generator cannot overload one processor.** At 200x and 800x (4,800 and 19,000 events/s) one processor never fell behind (peak backlog about 10,000, half a second of traffic), so "3 processors recover faster" was a 0 s vs 3 s noise result.
3. **A single processor sustains about 90,000 events/s** on the Birmingham replay stream (heartbeat-dominated, so mostly the cheap path), close to the replay generator's own rate, so even replay does not build a backlog while consumers are running. The final scenario therefore produces the burst while consumers are down.

### Lag spike: PASS (2026-09-26 16:31 UTC)
20 s Birmingham replay at 1000x produced with consumers down, then drained by 1 vs 3 processors

| Result | Expectation | Measured |
|---|---|---|
| PASS | backlog rose above 1,000,000 events (consumers down) | 2603042 |
| PASS | same backlog in both runs (within 1%) | 2603042 vs 2601034 |
| PASS | backlog drained to 0 (1 processor) | 31 s |
| PASS | backlog drained to 0 (3 processors) | 24 s |
| PASS | 3 processors drain faster than 1 | 24 s vs 31 s |

- 1 processor(s): backlog before start: 2603042 events
- 1 processor(s): container start to first consumption: 12 s
- 1 processor(s): time to drain the backlog (from start): 31 s
- 1 processor(s): drain rate once consuming: 142134 events/s
- 3 processor(s): backlog before start: 2601034 events
- 3 processor(s): container start to first consumption: 15 s
- 3 processor(s): time to drain the backlog (from start): 24 s
- 3 processor(s): drain rate once consuming: 293497 events/s
- speed-up in end-to-end drain time (includes ~12-15 s container start): 1.28x
- speed-up in drain rate once consuming: 2.06x

### Broker failure (graceful stop): PASS (2026-09-26 16:34 UTC)
stop kafka-2 (SIGTERM, controlled shutdown) at ~30 s of a 120 s run (speed 60), restart ~35 s later

| Result | Expectation | Measured |
|---|---|---|
| PASS | leadership moved off kafka-2 within 30 s | 1.7 s (it led 22 partitions) |
| PASS | pipeline kept producing closed windows during the outage | 60 -> 156 closed rows |
| PASS | producer had no delivery errors (acks=all, RF=3, min.isr=2) | errors=0 |
| PASS | ISR fully restored after restart (0 under-replicated partitions) | 1 s |
| PASS | consumer lag drains to 0 | 2 s |
| PASS | no data loss: every closed window is in the database | missing=0 |
| PASS | no wrong values vs ground truth | 264 windows compared, 0 mismatches |

- leader election time: 1.7 s
- time to full ISR after restart: not meaningful in this run (it was measured after the simulator finished; the SIGKILL run below measures it from the restart command)

### Broker failure (SIGKILL crash): PASS (2026-09-26 16:40 UTC)
SIGKILL kafka-2 at ~30 s of a 120 s run (speed 60), restart ~35 s later

| Result | Expectation | Measured |
|---|---|---|
| PASS | leadership moved off kafka-2 within 30 s | 9.1 s (it led 22 partitions) |
| PASS | pipeline kept producing closed windows during the outage | 60 -> 168 closed rows |
| PASS | ISR fully restored after restart (0 under-replicated partitions) | 3 s after restart |
| PASS | producer had no delivery errors (acks=all, RF=3, min.isr=2) | errors=0 |
| PASS | consumer lag drains to 0 | 5 s |
| PASS | no data loss: every closed window is in the database | missing=0 |
| PASS | no wrong values vs ground truth | 276 windows compared, 0 mismatches |

- leader election time: 9.1 s
- time to full ISR after restart: 3 s

### HLL estimate vs exact distinct count: PASS (2026-09-26 16:41 UTC)
HyperLogLog p=10: 1,024 bytes of registers, theoretical standard error 3.25%. Expectation: worst error under 3 standard errors (9.75%).

| Distinct vehicles | Trials | Mean error | Max error | Exact set memory | HLL memory |
|---|---|---|---|---|---|
| 100 | 20 | 1.83% | 4.12% | 14 KiB | 1 KiB |
| 1,000 | 20 | 2.46% | 5.63% | 88 KiB | 1 KiB |
| 10,000 | 10 | 1.18% | 3.79% | 1,069 KiB | 1 KiB |
| 100,000 | 5 | 3.78% | 5.43% | 9,663 KiB | 1 KiB |
| 1,000,000 | 2 | 4.26% | 7.57% | 88,432 KiB | 1 KiB |

### Load test: ramp at 1, 2 and 3 processors (2026-09-26 18:16 UTC)
`make load-test`. 30 s per step, valid events at an exact offered rate (30% OCCUPANCY, 70% HEARTBEAT, 12 lots x 200 slots, production producer settings), event time = wall time. Generator and stack share one machine (10 CPUs, Docker Desktop), so figures are a lower bound. Backlog = `parking.raw` end offsets minus events consumed. Latency is ingest-to-emit from Prometheus (sampled 1 in 8 events); the 5 s emit interval sets its floor. Charts: `docs/loadtest/load_test.png`, data: `docs/loadtest/load_test.csv`.

| Processors | Offered/s | Produced/s | Consumed/s | p50 | p95 | p99 | Backlog at end | Drain | Processor CPU | |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,000 | 1,001 | 878 | 2.8 s | 5.0 s | 8.8 s | 0 | 1 s | 5% |  |
| 1 | 5,000 | 5,008 | 4,563 | 2.7 s | 4.9 s | 7.9 s | 0 | 1 s | 9% |  |
| 1 | 10,000 | 10,007 | 8,934 | 2.6 s | 4.8 s | 6.6 s | 0 | 1 s | 16% |  |
| 1 | 25,000 | 25,028 | -14,480 | nan s | nan s | nan s | 1,192,332 | n/a | 1% | saturated |
| 2 | 1,000 | 1,002 | 858 | 2.6 s | 4.9 s | 8.4 s | 0 | 1 s | 5% |  |
| 2 | 5,000 | 5,008 | 4,439 | 2.7 s | 4.9 s | 8.4 s | 0 | 1 s | 11% |  |
| 2 | 10,000 | 10,008 | 9,519 | 2.6 s | 4.8 s | 6.5 s | 0 | 1 s | 20% |  |
| 2 | 25,000 | 25,024 | -13,930 | nan s | nan s | nan s | 1,231,261 | n/a | 1% | saturated |
| 3 | 1,000 | 1,001 | 947 | 2.6 s | 4.9 s | 8.0 s | 0 | 1 s | 7% |  |
| 3 | 5,000 | 5,005 | 4,786 | 2.6 s | 4.9 s | 8.5 s | 0 | 1 s | 14% |  |
| 3 | 10,000 | 10,012 | 9,713 | 2.6 s | 4.9 s | 8.3 s | 0 | 1 s | 23% |  |
| 3 | 25,000 | 25,020 | -15,407 | nan s | nan s | nan s | 1,231,140 | n/a | 53% | saturated |

HyperLogLog accuracy and memory (worst error 7.57% at 1M vehicles against a 9.75% limit; 1 KiB per window versus 88 MB for an exact set) is in the section above.
