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
| **Windowed-emit** latency p50 / p95 / p99 (producer timestamp to the emit that reflects the event) | **2.8 s / 6.0 s / 9.2 s** (the 5 s emit interval dominates: p50 is about half of it). Raw ingestion latency is measured separately, see "Ingestion latency versus windowed-emit latency". |
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
| 12 | Processor | Checkpoint size grew with the event rate (every event id, heartbeats included, kept for 300 s and written into each lot's checkpoint) until it exceeded Kafka's 1 MB message limit at 25,000 events/s: the processors refused to checkpoint and crash-looped. | Load test (the chaos drills never reach that rate). | De-duplicate only state-changing events; `max.message.bytes` 16 MB on `state.changelog`; unit test that heartbeat ids are not remembered. |
| 13 | Consumers | When the broker acting as group coordinator was killed and came back, `commit` failed with `UNKNOWN_MEMBER_ID`; the exception was uncaught, so processors (and potentially sink/alerter) **crashed and were restarted by Compose** (restart count 1 each). Recovery still worked through restart plus checkpoint restore, but a broker failure should not crash consumers. | Frontend self-review: `docker inspect` restart counts after the UI broker-kill test. | `common.kafka.commit_tolerant` logs and skips commits that fail because the coordinator is moving (offsets are a monitoring marker; recovery uses checkpoints); unit test; the broker drill now asserts every processor and sink has restart count 0. |
| 14 | API pipeline collector | The long-lived Kafka probe consumer and admin client kept a stale metadata cache after a broker died: every watermark lookup failed with "host resolution failure" and consumer groups vanished from the Pipeline screen, exactly during a broker-kill demo. It also returned a blank result while failing. | Destructive UI test against the live stack. | Fresh admin client and probe consumer every 2 s cycle; on error the last good snapshot is kept and flagged instead of blanking; unit test. |
| 15 | Web UI | The "Rebalancing" banner only showed while a group was mid-rebalance; a rebalance that finishes inside the 2 s poll flashed past unseen. The first version of the fix swallowed the first-ever rebalance when the history was empty at load. | Destructive UI test (passed once by luck, then failed). | Banner persists 15 s after a newly detected rebalance; component tests for both cases. |

## Frontend self-review: full stack, live UI, real failures (2026-09-27)
`make demo-reset` (weekday morning at 20x, 12 lots, 1,800 slots), then the opt-in destructive Playwright spec `web/tests/e2e/recovery.spec.ts` (`E2E_DESTRUCTIVE=1`) against the running stack through Caddy:

| Drill | What the UI showed | Result |
|---|---|---|
| Kill one of two processors (`docker kill`) | Rebalance banner; all 6 partitions owned by the survivor; rebalance log entry; log line `partition assigned ... restored_lots ["LOT-..."] resume_offset ...`; events/s stayed > 0; restoring the second processor moved partitions back | PASS (26.7 s including restore) |
| Kill the broker leading the most partitions | Broker row Down; its partitions re-elected onto the two survivors (leader count unchanged); "Brokers up 2 of 3"; events/s stayed > 0; after restart the row returned to Up | PASS (15.7 s) |
| Restart counts after both drills | `docker inspect` on processors, sink, alerter, API | all 0 |
| Windowed-emit latency during the processor failover | windowed-emit p95 rose to 23.6 s on the Pipeline screen (screenshot `docs/screenshots/pipeline-rebalance.png`) and recovered; this is the real cost of a rebalance | observed |

Running these against the live product found bugs 13, 14 and 15 below, which the scripted drills did not.

## Prod compose verification (2026-09-27)
`docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --wait` run for real on this machine (Docker Desktop, arm64), from a clean state (`down -v`), with a real `.env`: random 32-character secrets generated for `POSTGRES_PASSWORD`, `GRAFANA_PASSWORD` and `SIM_SALT`, a freshly generated `KAFKA_CLUSTER_ID`, `GHCR_OWNER=harsh-0724`, `SITE_ADDRESS=localhost`. Images were pulled from GHCR (the CI-built multi-arch images, public), nothing built locally.

| Check | Command / observation | Result |
|---|---|---|
| Starts and waits for health | `up -d --wait` | all 14 containers up in **31 s**; `kafka-init` exited 0 as designed |
| Runs the CI images | `docker compose ps` | `ghcr.io/harsh-0724/smart-parking-{processor,sink,alerter,api,web}:latest`; the simulator reuses the processor image |
| Stays healthy | 10 minutes running, then `docker inspect` on every container and `logs --since 10m` | **restart count 0 on all 14**, 0 tracebacks or errors in processor/sink/alerter/api logs, live data (631 of 1,800 slots occupied, 12 lots) |
| Only 80/443 published | `lsof -iTCP -sTCP:LISTEN` for Docker processes | `*:443 *:80` only; Kafka, Postgres, Prometheus, Grafana, API show as unpublished (`9092/tcp`, `5432/tcp`, ...) |
| Caddy, HTTP | `curl -sI http://localhost/` | `308 Permanent Redirect` to `https://localhost/` |
| Caddy, frontend over HTTPS | `curl -sk https://localhost/` | `200 text/html`, `<title>Parking Ops</title>`; `/pipeline` returns 200 (SPA fallback); hashed asset returns `cache-control: public, max-age=31536000, immutable`; `x-content-type-options: nosniff`, `x-frame-options: DENY`, `referrer-policy: no-referrer` |
| Caddy, API | `curl -sk https://localhost/api/health` | `{"status":"ok","database":true,"kafka_feed":true,...}` |
| Caddy, Grafana at `/grafana/` | `/grafana/login` 200; `api/search?query=Pipeline` with the **real** password returns the provisioned "Pipeline health" dashboard; with the default `admin:admin` it returns **401** | correct |
| Live data through Caddy over TLS | Playwright smoke suite with `E2E_BASE_URL=https://localhost` (needs `ignoreHTTPSErrors` for Caddy's internal CA, added to `playwright.config.ts`) | 3 of 3 pass, including WebSocket frames over `wss` |

**Refusing default secrets: the check I had claimed did not exist.** The override used `${POSTGRES_PASSWORD:?...}`, which only fails when the variable is **unset or empty**. With `POSTGRES_PASSWORD=change-me` in `.env`, `docker compose config` accepted it and `up -d timescaledb` **started the database with the default password**. So the original prod override did *not* refuse default secrets. Fixed with a one-shot `prod-guard` service (`infra/prod_guard.sh`, run by the prod override) that inspects the actual values (rejects empty, shorter than 12 characters, or a known default such as `change-me*`, `admin`, or the dev cluster id); the brokers, TimescaleDB, Grafana and `web` depend on it completing successfully. Verified on a clean slate, each case starting from the good `.env` and changing one value:

```
A POSTGRES_PASSWORD=change-me   -> exit=1, running=0 | REFUSING TO START: POSTGRES_PASSWORD is empty or shorter than 12 characters
B GRAFANA_PASSWORD=admin        -> exit=1, running=0 | REFUSING TO START: GRAFANA_PASSWORD is empty or shorter than 12 characters
C SIM_SALT=change-me-too        -> exit=1, running=0 | REFUSING TO START: SIM_SALT still has a default value from .env.example
D dev KAFKA_CLUSTER_ID          -> exit=1, running=0 | REFUSING TO START: KAFKA_CLUSTER_ID still has a default value from .env.example
E POSTGRES_PASSWORD=abc123      -> exit=1, running=0 | REFUSING TO START: POSTGRES_PASSWORD is empty or shorter than 12 characters
(SIM_SALT unset)                -> config error: required variable SIM_SALT is missing a value
```
`docker compose ps` after a refused start shows every service as `Created` (never `Up`) and only `prod-guard` as `Exited (1)`. With the good `.env` restored, the same command starts cleanly and `prod-guard` prints `secrets are set and none is a known default`.

## Demo rehearsal (2026-09-27)
`docs/DEMO.md` followed exactly as written (from `make demo-reset`, `SIM_START_HOUR=8`, T0 = reset + 8 min), driven by a script that performed each action at its stated time and read the UI and Kafka. Full timed record: "Rehearsal record" in `docs/DEMO.md`. Screenshots: `docs/screenshots/rehearsal-6-processor-killed.png`, `rehearsal-8-broker-killed.png`, `rehearsal-10-grafana.png`.

| Moment | Measured |
|---|---|
| Processor kill to Rebalanced banner | 9.4 s; survivor holds all 6 partitions; state restored from the changelog (`restored_lots ["LOT-01","LOT-04"]`, `resume_offset 137141`) |
| Restore command to two owners, Stable | 5.1 s; in controlled repeats the survivor revoked only partitions [0, 1, 2] (3 of 6 move) |
| Broker kill (busiest, kafka-1, 8 of 19 leaders) to Down in the UI | 12.3 s; leaders back on survivors by then; ISR degraded on all 7 topics; 1,179 events/s throughout; Prometheus under-replicated partitions 64 |
| Same broker kill, processor group | also rebalanced (the killed broker hosted the group coordinator), recovered without any restart |
| Broker restart to Up | 3.5 s; ISR full within seconds; the restarted broker leads 0 partitions until Kafka's periodic leader rebalance |
| Final state | health ok, group Stable 2 members (3, 3), 3 brokers, no degraded ISR, late 0, DLQ 0, restart counts 0 |

Found by doing it for real: (1) the banner reads "Rebalanced", not "Rebalancing", and its first text is a transitional "holds 0"; (2) the UI shows a broker as Down about 12 s after the kill, not 9 s; (3) a broker kill also rebalances the processor group; (4) after the restart the broker leads nothing, so the next kill must pick the busiest broker again; (5) **one anomaly not explained:** in the timed run `docker compose up -d --scale processor=2 processor` recreated *both* processor containers (both created at that second), so all 6 partitions changed owner. Two controlled repeats (kill, wait 12 s or 90 s, restore) did not reproduce it: the survivor kept its container and the killed one restarted with the same ID. The system was demoable afterwards either way.

## Phase 8: chaos runs (appended by scripts/chaos_*.py)

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

### HLL estimate vs exact distinct count: PASS (2026-09-26 16:41 UTC)
HyperLogLog p=10: 1,024 bytes of registers, theoretical standard error 3.25%. Expectation: worst error under 3 standard errors (9.75%).

| Distinct vehicles | Trials | Mean error | Max error | Exact set memory | HLL memory |
|---|---|---|---|---|---|
| 100 | 20 | 1.83% | 4.12% | 14 KiB | 1 KiB |
| 1,000 | 20 | 2.46% | 5.63% | 88 KiB | 1 KiB |
| 10,000 | 10 | 1.18% | 3.79% | 1,069 KiB | 1 KiB |
| 100,000 | 5 | 3.78% | 5.43% | 9,663 KiB | 1 KiB |
| 1,000,000 | 2 | 4.26% | 7.57% | 88,432 KiB | 1 KiB |

### Load test: ramp at 1, 2 and 3 processors (2026-09-26, `make load-test`)

**Outcome, stated plainly: the planned ramp did not do what it was designed to do.** It was meant to find where a single processor saturates and then show throughput scaling with 2 and 3 processors. It never saturated one: a **single processor sustained about 100,000 events/s with no backlog**, the top of the ramp and about the limit of the load generator on this machine. So this test **cannot demonstrate horizontal scaling on its own**, and **the real per-processor ceiling is still unknown**; the only things established are lower bounds (100,000 events/s live with a 30% state-change mix; 142,000 events/s drained from a backlog in the heartbeat-heavy lag-spike drill). Horizontal scaling is evidenced by a different experiment, the **lag-spike drain-rate drill above: 142,000 events/s with 1 processor versus 293,000 events/s with 3 (2.06x)**. Pushing the generator beyond 100,000 events/s was not attempted, so no number above that is reported.

What the ramp does establish: no data loss or backlog up to 100,000 events/s at any processor count, flat latency (below), and one real defect (a crash at 25,000 events/s in a first run; end of this section).

Method: 30 s per step, valid events at an exact offered rate (30% OCCUPANCY, 70% HEARTBEAT, 12 lots x 200 slots, production producer settings: idempotent, `acks=all`, lz4), event time = wall time. Generator and stack share one 10-CPU machine (Docker Desktop), so figures are a lower bound. Backlog = `parking.raw` end offsets minus events the processors report consuming. Data: `docs/loadtest/load_test.csv`; chart: `docs/loadtest/load_test.png`.

**Which latency this is:** the p50/p95/p99 columns below are **windowed-emit latency**, from the producer's timestamp to the 5-second window emit that reflects the event. They include waiting for the next emit, so they say nothing about how fast an event is *ingested*. Raw ingestion latency (produce to consume, no windowing) is measured separately in the next section: tens to a few hundred milliseconds.

| Processors | Offered/s | Produced/s | Sustained/s | Windowed-emit p50 | p95 | p99 | Backlog at end of hold | Drain after hold | Processor CPU (indicative) |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 1,000 | 1,001 | 969 | 2.7 s | 4.9 s | 8.5 s | 0 | 1 s | 2% |
| 1 | 5,000 | 5,008 | 4,842 | 2.5 s | 4.8 s | 7.0 s | 0 | 1 s | 8% |
| 1 | 10,000 | 10,014 | 9,697 | 2.5 s | 4.8 s | 6.4 s | 0 | 1 s | 11% |
| 1 | 25,000 | 25,023 | 24,202 | 2.5 s | 4.8 s | 5.5 s | 0 | 1 s | 22% |
| 1 | 50,000 | 50,067 | 48,453 | 2.6 s | 4.8 s | 5.7 s | 0 | 1 s | 33% |
| 1 | 100,000 | 100,126 | 97,066 | 2.6 s | 4.8 s | 6.5 s | 0 | 1 s | 45% |
| 2 | 1,000 | 1,001 | 967 | 2.6 s | 4.9 s | 7.8 s | 0 | 1 s | 6% |
| 2 | 5,000 | 5,005 | 4,832 | 2.7 s | 4.9 s | 8.5 s | 0 | 1 s | 10% |
| 2 | 10,000 | 10,010 | 9,665 | 2.7 s | 4.9 s | 7.5 s | 0 | 1 s | 14% |
| 2 | 25,000 | 25,026 | 24,168 | 2.5 s | 4.8 s | 5.3 s | 0 | 1 s | 21% |
| 2 | 50,000 | 50,051 | 48,142 | 2.6 s | 4.8 s | 6.5 s | 0 | 1 s | 30% |
| 2 | 100,000 | 100,107 | 96,667 | 2.5 s | 4.8 s | 5.0 s | 0 | 1 s | 58% |
| 3 | 1,000 | 1,002 | 964 | 2.6 s | 4.9 s | 7.8 s | 0 | 1 s | 7% |
| 3 | 5,000 | 5,006 | 4,800 | 2.7 s | 4.9 s | 8.6 s | 0 | 1 s | 10% |
| 3 | 10,000 | 10,009 | 9,652 | 2.7 s | 4.9 s | 8.5 s | 0 | 1 s | 13% |
| 3 | 25,000 | 25,019 | 24,015 | 2.6 s | 4.8 s | 6.8 s | 0 | 1 s | 20% |
| 3 | 50,000 | 50,053 | 48,175 | 2.5 s | 4.8 s | 5.7 s | 0 | 1 s | 39% |
| 3 | 100,000 | 100,113 | 96,379 | 2.5 s | 4.8 s | 5.0 s | 0 | 1 s | 62% |

What the numbers say, and what they do not:
- **Throughput:** every processor count kept up with every offered rate up to 100,000 events/s (backlog 0 at the end of each 30 s hold, drained in about 1 s). Because nothing saturated, the 1, 2 and 3 processor rows look alike; that is the absence of a bottleneck, not evidence that adding processors does nothing.
- **Windowed-emit latency is flat, about 2.5 s p50 and 4.8 s p95, at every rate.** It is set by the 5 s emit interval (`EMIT_INTERVAL_S`), not by load, and would only rise if the processors fell behind. It is **not** ingestion latency.
- **CPU** is `docker stats` summed over processor containers, sampled once mid-hold; indicative only (one sample; it under-reports short bursts).
- HyperLogLog accuracy and memory (worst error 7.57% at 1,000,000 vehicles against a 9.75% bound; 1 KiB per window versus 88 MB for an exact set) is in the HLL section above and is not repeated here.

**A first run of this test crashed the processors at 25,000 events/s and is not reported as a result.** The processor remembered every event id (heartbeats included) for 300 s of event time and wrote that set into each lot's checkpoint. At 25,000 events/s a lot's checkpoint exceeded Kafka's 1 MB message limit (`MSG_SIZE_TOO_LARGE`); the processor then correctly refused to checkpoint and crash-looped, and consumed counters went backwards. Fixed by de-duplicating only state-changing events (a re-processed heartbeat is idempotent) and raising `max.message.bytes` on `state.changelog` to 16 MB (DECISIONS D27, bug #12 above).


### Ingestion latency versus windowed-emit latency (2026-09-26 20:35 UTC; two different numbers)
`make ingest-latency`: 2 processors, 30 s per rate, valid events from `scripts/loadgen.py`, producer and processors on one host (so wall clocks agree). Quantiles are Prometheus estimates from histogram buckets (1 ms to 60 s), so resolution is the bucket width.

| Offered/s | **Raw ingestion** p50 | p95 | p99 | Windowed emit p50 | p95 | p99 |
|---|---|---|---|---|---|---|
| 1,000 | **134 ms** | **239 ms** | **249 ms** | 2.6 s | 4.9 s | 7.7 s |
| 10,000 | **78 ms** | **225 ms** | **295 ms** | 2.6 s | 4.8 s | 6.8 s |
| 50,000 | **38 ms** | **82 ms** | **329 ms** | 2.5 s | 4.8 s | 6.0 s |
| 100,000 | **36 ms** | **153 ms** | **464 ms** | 2.5 s | 4.9 s | 7.6 s |

How to read this: **raw ingestion latency** (bold) is the time from the producer stamping an event to the processor holding it: **p50 36-134 ms, p95 82-239 ms, p99 249-464 ms** across 1,000 to 100,000 events/s. **Windowed-emit latency** is a different quantity that also includes waiting for the next 5-second window emit: **p50 about 2.5 s, p95 about 4.8 s**. The 4.8 s figure quoted elsewhere is the windowed-emit p95, never the ingestion latency, and the two must not be swapped in the paper or the viva.

Caveats, measured not assumed: (1) raw latency is *higher* at 1,000 events/s (p50 134 ms) than at 50,000 (38 ms). At light load the processor's `consume(500, 0.2)` call waits up to 200 ms to fill a batch, and the producer adds up to its 20 ms linger; at high load batches fill at once. So the low-load figure is dominated by batching windows, not by Kafka. (2) The load generator stamps `ingest_ts` once per 50 ms slice, so a few tens of milliseconds of the measured value are generator-side. (3) Quantiles are Prometheus estimates from histogram buckets, so resolution is the bucket width. (4) Producer and processors share one host, so their wall clocks agree; across hosts the clocks would need synchronising.

### Late and duplicate data: PASS (2026-09-26 18:39 UTC)
--late-pct 10 --dup-pct 5, in-grace (A) and beyond-grace (B) delays

| Result | Expectation | Measured |
|---|---|---|
| PASS | A: every duplicate dropped | sent 29, dropped 29 |
| PASS | A: nothing late enough for parking.late | late=0, 74 events were delayed |
| PASS | A: in-grace late events land in the right windows (matches ground truth) | 216 windows compared, 0 mismatches, 0 missing |
| PASS | B: every duplicate dropped | sent 31, dropped 31 |
| PASS | B: beyond-grace events routed to parking.late | 40 routed of 77 delayed (the rest were inside the grace period) |
| PASS | B: every parking.late record is past its window's close time | 40 records |

- B: worst lateness recorded: 523 s behind the watermark

### Processor crash: PASS (2026-09-26 18:42 UTC)
SIGKILL one of 2 processors at ~35 s of a 90 s run (speed 60)

| Result | Expectation | Measured |
|---|---|---|
| PASS | survivor assigned the 3 orphaned partitions within 30 s | 10.4 s |
| PASS | state restored from state.changelog | 6 lots restored |
| PASS | consumer lag drains to 0 | 5 s |
| PASS | no gaps: every closed window is in the database | missing=0 |
| PASS | no wrong values vs ground truth | 192 windows compared, 0 mismatches |
| PASS | no duplicate rows | 228 rows / 228 keys |

- takeover time (kill to partitions assigned): 10.4 s
- simulator events sent: 130374

### Broker failure (SIGKILL crash): PASS (2026-09-26 19:05 UTC)
SIGKILL kafka-2 at ~30 s of a 120 s run (speed 60), restart ~35 s later

| Result | Expectation | Measured |
|---|---|---|
| PASS | leadership moved off kafka-2 within 30 s | 9.6 s (it led 32 partitions) |
| PASS | pipeline kept producing closed windows during the outage | 60 -> 168 closed rows |
| PASS | ISR fully restored after restart (0 under-replicated partitions) | 3 s after restart |
| PASS | producer had no delivery errors (acks=all, RF=3, min.isr=2) | errors=0 |
| PASS | no consumer crashed or was restarted during the broker failure | restart counts ['0', '0', '0'] |
| PASS | consumer lag drains to 0 | 11 s |
| PASS | no data loss: every closed window is in the database | missing=0 |
| PASS | no wrong values vs ground truth | 264 windows compared, 0 mismatches |

- leader election time: 9.6 s
- time to full ISR after restart: 3 s
