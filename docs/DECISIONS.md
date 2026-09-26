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

## D17. Alerter design
- **Two roles, one process, group `alerter`:** it reads `lot.occupancy.5min` to raise/clear `FULL_LOT` alerts on `lot.alerts`, and reads `lot.alerts` (its own plus the processor's `SENSOR_OFFLINE`) to notify. Every alert reaches the notifier through one path.
- **Hysteresis:** raise at `>= ALERT_FULL_THRESHOLD` (0.90), clear at `<= ALERT_CLEAR_THRESHOLD` (0.85), nothing in between. The occupancy used is the instantaneous `current_occupied / capacity` on open-window results (the 5-minute *average* would lag by minutes), and results older than the last one handled for that lot are ignored (windows of one lot can arrive out of order across the 3 result partitions).
- **Restart safety:** on start the active-alert set is rebuilt from `lot.alerts` (RAISED without a later CLEARED). Alerts are flushed before the occupancy offsets that caused them are committed, so a crash re-derives them from restored state instead of duplicating them.
- **Single instance.** A lot's results span partitions, so hysteresis state cannot be sharded by partition. This is a documented limit, not something the demo needs to scale past.
- **Notification is at-least-once.** Channels: console (always), webhook (`ALERT_WEBHOOK_URL`), Telegram (`TELEGRAM_BOT_TOKEN` + `TELEGRAM_CHAT_ID`). A failing channel is logged and counted but never blocks the others or the offset commit.
- Alert `ts` for full-lot alerts is the wall-clock `emitted_at` of the result that crossed the threshold.

## D18. Prometheus setup
- One Prometheus, `dns_sd_configs` per service so scaling replicas needs no config change; `kafka-exporter` (`danielqsj/kafka-exporter`) for consumer-group lag and offsets rather than re-implementing lag in each service. Grafana provisioning is left to the next session; the metrics are already the contract.
- **Latency is sampled per event, not read from window results.** The first version observed `emitted_at - latest_ingest_ts` of each result, which measured the age of a quiet window's last occupancy event (p95 = 29 s, meaningless). It now records 1 in 8 accepted events' `ingest_ts` and observes `emit_time - ingest_ts` at the next emit, i.e. the real delay a reader of the output topic experiences.

## D19. Chaos harness
- Each scenario (`scripts/chaos_*.py`, `make chaos-<name>` or `make chaos`) resets to a known state (fresh topics, empty tables, N processors), runs the simulator with `--truth-file`, injects the failure, then checks explicit expectations and prints PASS/FAIL. **Correctness is judged against the simulator's independent ground truth**, not against the processor's own output. Measurements are appended to `docs/RESULTS.md` by the script itself.
- **Broker failure is a SIGKILL by default** (`--graceful` for SIGTERM). A graceful stop lets the broker hand off leadership before exiting (1.7 s), which is a planned restart, not a failure; a crash is only noticed after the broker session timeout (9 s).
- **Backlog is not committed-offset lag.** The processor commits only at checkpoints, so `kafka-consumer-groups` lag saw-tooths by up to 10 s of traffic. The lag scenario measures `raw end offsets - events the processors report consuming`, and produces the burst while consumers are down, because one processor sustains about 90,000 events/s and a live burst never builds a backlog (see the measurement notes in RESULTS.md).
- **In-grace late events:** the slot-state rule (ignore events older than the slot's last event) means a late FREE that arrives after a *newer* event for the same slot is dropped for state, which can skew that window's entries/exits. It needs a slot to change state twice within the delay, which the simulator's dwell times (median >= 1.5 h) make vanishingly rare; in the run below there were 0 mismatches over 228 windows. The scenario allows up to 2% of windows to differ so the check is honest rather than tuned.

## D20. API gateway design (Phase 5)
- **Two live inputs, no consumer-group membership.** The WebSocket feed reads Kafka with a *manually assigned* consumer (`group.id=api-gateway` is set only because the client requires one). Every API instance must see every message, which is fan-out, not work sharing, so joining a group would be wrong and would trigger rebalances. Consequence: the Pipeline screen shows only the three real groups (`occupancy-processor`, `timescale-sink`, `alerter`).
- **Snapshot without a gap.** Slot state is bootstrapped from the processor's own checkpoints on `state.changelog` (at most ~10 s old), then `parking.raw` is tailed from the checkpoint's `resume_offset`, so events between the snapshot and "now" are replayed rather than lost. The store ignores events older than a slot's last event, the same rule as the processor. If no checkpoint exists yet, the feed starts at the end and fills as slots change.
- **WebSocket protocol.** Server pushes `occupancy`, `alert` and `pipeline` messages to everyone; a client sends `{"type":"subscribe","lots":[...]}` (max 5) and receives a `snapshot` then coalesced `slots` deltas for those lots only, flushed every 100 ms. Each client has a bounded outbox: a slow client loses old messages, never memory.
- **The feed must survive bad input.** Malformed messages on `parking.raw` (the bad-data scenario) are ignored by `parse_slot_event`; an early version raised inside the feed thread and would have restarted it in a loop (found in review, covered by a unit test and by the API integration test).
- **Approximations are labelled as such.** `unique_vehicles_est` is the HyperLogLog estimate of the newest *closed* window; `dwell_minutes_est` is Little's law (occupied-seconds / entries over 24 h) and underestimates while a lot is still filling.
- **History defaults to the 24 h ending at the lot's newest window, not wall-clock now**, so the demo works while the simulator's clock runs faster than real time. Responses are cached for 2 s (overview) and 10 s (history).

## D21. Frontend design and engineering (Phase 6)
- **Point of view:** Linear's density and restraint plus a transit-authority operations board: an instrument that reports, with text and tabular numbers first. One accent (verdigris, `#0b7a6e` light / `#3cc8b4` dark) used only for live state, the selected nav item, focus rings and the newest data point. IBM Plex Sans + IBM Plex Mono, self-hosted via `@fontsource`. Scale 12/13/16/24/48 px, 4 px spacing grid, 4 px radii, 1 px hairlines, no shadows or gradients (a test fails if any appear).
- **State is never colour alone:** free slot = outlined square, occupied = filled square, warning = triangle, critical = diamond, always with a text label. WCAG AA contrast for every text pairing in both themes is computed from `tokens.css` by a test.
- **Live data path:** one WebSocket, a typed reconnecting client (exponential backoff 0.5 s to 10 s with jitter, silent-connection watchdog, resubscribe on reconnect) in an external store. Slot deltas mutate a normalised map and are announced once per animation frame; slots are memoised so an event re-renders one `<rect>`, not the plan (a 600-slot test enforces this). TanStack Query polls REST every 5 s and WebSocket numbers overlay it, so the page is correct even if the socket is down.
- **No UI kit, no chart library:** SVG sparkline and time-series are hand-built (thin lines, direct end label, muted axes; open window dashed with a hollow marker). Route-level code splitting; initial JS is 96 kB gzipped against a 200 kB budget, enforced in CI.
- **Types come from the API:** `make openapi` exports the FastAPI schema to `web/openapi.json` and generates `src/api/schema.d.ts`.

## D22. Pipeline screen
- Everything on it is read from Kafka (admin API + watermarks) by the API, plus Prometheus for latency. A rebalance is detected by diffing each group's state and member assignment between 2 s polls; the screen shows a banner while a group is rebalancing, highlights partition cells whose owner changed, and keeps a log. A broker that disappears from cluster metadata is remembered and shown as Down with a timestamp.
- **Committed lag is a sawtooth by design** (offsets are committed at 10 s checkpoints). It is labelled as such in the UI and is not the true backlog (see D19).

## D23. One web image, one entry point (Phase 10)
- The `web` image is Caddy with the built frontend: it serves the static app, proxies `/api` and `/ws` to the API and `/grafana` to Grafana, and terminates TLS (`SITE_ADDRESS=:80` for plain HTTP, a domain name for automatic HTTPS). Dev and prod use the same image. This is why nothing but 80/443 is published in production and why Grafana needs no port of its own.
- `docker-compose.prod.yml` is an override: published ports are removed with `!reset`/`!override`, images come from GHCR, required secrets use `${VAR:?message}` so a deploy fails loudly instead of using a default. Validated by `docker compose config`, which shows `web` as the only service publishing ports.
- Backend images are one Dockerfile with a build argument (`RUN_CMD`) so CI can publish `processor`, `sink`, `alerter` and `api` separately; all are multi-arch (amd64 + arm64). The simulator reuses the processor image.

## D24. Demo-friendly simulator options and the reset rule
- `--start-local-hour`, `--skip-weekend` and `--varied-sizes` make the compose demo replay a weekday morning at 20x with lots of 90/120/240 slots. They stay near wall-clock time so retention and "recent" queries keep working. `make load-test`, the chaos scripts and the integration tests do not use them.
- **Event-time state must be reset whenever the simulator restarts with a different clock** (`make demo-reset`); DEMO.md makes it the first pre-demo step.

## D25. Grafana
- Provisioned entirely from files (`infra/grafana/provisioning`, `infra/grafana/dashboards/pipeline-health.json`): a Prometheus datasource and one "Pipeline health" dashboard covering brokers, under-replicated partitions, rebalances, processor instances, consumer lag, throughput, latency percentiles, DLQ/late/duplicate rates, checkpoint duration, sink rows and topic sizes. It is for operators; the product UI is the custom frontend.

## D26. Load-test method
- `scripts/loadgen.py` produces valid events at an exact rate (idempotent, `acks=all`, lz4; 30% OCCUPANCY, 70% HEARTBEAT; event time = wall time) because the synthetic simulator tops out near 19,000 events/s. `scripts/load_test.py` runs each rate for 30 s at 1, 2 and 3 processors and records produced and consumed rate, backlog (end offsets minus events consumed), processor CPU and ingest-to-emit percentiles from Prometheus. Generator and stack share the machine, so results are a lower bound.
