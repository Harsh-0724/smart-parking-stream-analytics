# Claude Code Master Prompt: Smart Parking Stream Analytics

Paste this as your first message in Claude Code (ideally in plan mode), or save it as `CLAUDE.md` in an empty repo so it persists across sessions.

---

## Role and working style

You are a senior data/streaming engineer and product-minded frontend engineer. Build a complete, production-worthy **Smart Parking Occupancy Analytics** system from scratch in this repo. It is a final-year college project (course: Stream Processing and Analytics, AI372IA) that will be graded on a live demo, a viva, and a technical paper, so **everything must run, be demonstrable, and be explainable line by line.**

Work rules:
1. **Plan first.** Before writing code, output a short plan (repo tree, phase breakdown, risks) and wait for my approval.
2. **Build in the phases below, in order.** Finish and verify each phase before starting the next. After each phase: run it for real, show me the evidence (command output, test results, screenshots), and make a git commit with a clear message.
3. **Never claim something works without running it.** If Docker, Kafka, or a dependency fails, debug it. Don't stub it out or fake the data.
4. Prefer boring, well-understood choices. No unnecessary frameworks. Every dependency must earn its place.
5. Write code I can defend in a viva: clear names, small functions, comments only where the *why* isn't obvious. No dead code, no TODO litter, no placeholder text.
6. Keep a running `docs/DECISIONS.md` (short ADR-style entries: decision, alternatives, why). Keep a `docs/RESULTS.md` where you record every measurement (latency, throughput, recovery times, accuracy) as you produce it. These feed my technical paper.

## Hard requirements (from the department circular; all mandatory)

- Pipeline: **Data Source → Kafka → Stream Processing → Storage → Analytics/Dashboard**
- Apache Kafka is the central platform
- At least one producer and one consumer
- At least two topics
- Demonstrate **partitioning and consumer groups** (including a live rebalance)
- At least one **window-based streaming operation**
- At least one **aggregation/summarization algorithm**
- **Fault tolerance / recovery handling**, demonstrable live
- Store results in a suitable database / time-series store
- **Real-time visualization or alert mechanism**

## System overview

```
Simulator (producer)
   -> parking.raw (6 partitions, key = lot_id)
   -> Stream Processor (consumer group "occupancy-processor", 2-3 instances)
        validate -> dedupe -> late-event check -> slot state -> 5-min event-time windows -> HyperLogLog
        -> parking.dlq | parking.late | lot.occupancy.5min | lot.alerts | state.changelog (compacted)
   -> Sink (group "timescale-sink") -> TimescaleDB
   -> Alerter (group "alerter") -> lot.alerts -> notifier
   -> API gateway (FastAPI: REST + WebSocket, reads Kafka + TimescaleDB)
   -> Web frontend (custom, see design brief)
Ops: Prometheus + kafka-exporter + Grafana (pipeline health only)
```

## Tech decisions (already made; don't relitigate)

- **Language:** Python 3.12 for all backend services. `confluent-kafka` client, `pydantic` v2 for schemas, `datasketch` for HyperLogLog, `prometheus_client`, `structlog` for JSON logs. Own windowing logic (no black-box stream framework), so the logic is explainable.
- **Kafka:** `apache/kafka` image, **3 brokers in KRaft mode**, RF=3, `min.insync.replicas=2`. Producer: `enable.idempotence=true`, `acks=all`, lz4. Consumers: `enable.auto.commit=false`, cooperative-sticky assignor, `isolation.level=read_committed`.
- **Storage:** TimescaleDB (hypertables, compression + retention policies, upserts).
- **Frontend:** Vite + React + TypeScript, no UI kit. Details in the design brief below.
- **Infra:** Docker Compose (base + `docker-compose.prod.yml` override), Caddy for HTTPS reverse proxy, GitHub Actions for CI/CD to GHCR.
- **Tooling:** `uv` for Python deps, `ruff` + `mypy` (strict on `common/` and `processor/`), `pytest`, `pnpm` for the frontend, Makefile with a target for every common task.

## Repo layout

```
smart-parking/
├── docker-compose.yml
├── docker-compose.prod.yml
├── Makefile
├── .env.example
├── common/          # pydantic schemas, config loading, kafka helpers, logging
├── simulator/       # producer: replay + synthetic + fault injection
├── processor/       # windowing, slot state, dedupe, HLL, checkpointing, DLQ
├── sink/            # Kafka -> TimescaleDB (idempotent upserts)
├── alerter/         # threshold + sensor-offline alerts, notifier
├── api/             # FastAPI: REST + WebSocket
├── web/             # React frontend
├── forecaster/      # optional, last
├── infra/           # topics init, timescale init.sql, prometheus, grafana provisioning, Caddyfile
├── scripts/         # chaos demos, load test
├── tests/           # unit, integration, e2e
└── docs/            # architecture diagram, DECISIONS.md, RESULTS.md, DEMO.md, paper notes
```

## Data contracts

Event on `parking.raw` (JSON, validated by pydantic in `common/schemas.py`):

```json
{
  "event_id": "uuid",
  "event_type": "OCCUPANCY | HEARTBEAT",
  "sensor_id": "S-A-014",
  "lot_id": "LOT-03",
  "slot_id": "A-014",
  "status": "OCCUPIED | FREE",
  "vehicle_token": "salted hash, only on OCCUPIED",
  "event_ts": "ISO-8601 UTC (when it happened)",
  "ingest_ts": "ISO-8601 UTC (when produced)"
}
```

Topics:

| Topic | Partitions | Notes |
|---|---|---|
| `parking.raw` | 6 | key = `lot_id`, RF=3, min.isr=2, retention 3 days |
| `parking.dlq` | 1 | original bytes + error reason + timestamp |
| `parking.late` | 1 | events beyond the grace period |
| `lot.metadata` | 1 | compacted; capacity, name, coordinates or floor-plan layout |
| `lot.occupancy.5min` | 3 | window results, key = `lot_id|window_start`, `closed` flag |
| `lot.alerts` | 1 | full-lot and sensor-offline alerts |
| `state.changelog` | 6 | compacted, key = `lot_id`, processor checkpoints |

Topics are created by a one-shot `init` container in Compose, not by hand.

## Phases

### Phase 0: Foundation
Repo scaffold, Makefile, lint/type/test config, 3-broker KRaft cluster with healthchecks, `init` container that creates topics, named volumes, per-broker heap caps (`-Xmx512m`).
**Done when:** `make up` starts a healthy cluster from clean; `kafka-topics --describe` shows leaders spread over all 3 brokers with full ISR.

### Phase 1: Simulator (producer)
- **Synthetic mode:** N lots x M slots, realistic daily/weekly demand curves (morning ramp, lunch dip, evening drain, weekend shape), per-lot personality (office, mall, station), realistic dwell time distributions (log-normal). Emit OCCUPANCY events for state changes and a HEARTBEAT per sensor every 30s.
- **Replay mode:** replay the UCI Birmingham Parking occupancy dataset, converting occupancy deltas into per-slot events with jittered timestamps. Speed flag `--speed 1|60|1000`. If the dataset can't be downloaded in this environment, tell me exactly what file to fetch and where to put it, and continue with synthetic mode.
- **Fault injection flags:** `--late-pct`, `--dup-pct`, `--malformed-pct`, `--sensor-dropout <id>`, `--burst <multiplier>`.
- Publish `lot.metadata` at startup.
**Done when:** events spread evenly across the 6 partitions, and each fault flag visibly does what it says.

### Phase 2: Stream processor (the core; do this with the most care)
Per event: **validate -> dedupe -> late check -> slot state -> window aggregate -> emit.**
1. Validate with pydantic; failures go to `parking.dlq` with the original bytes and the error.
2. Dedupe via per-lot TTL set of `event_id` (TTL >= allowed lateness + margin).
3. Watermark per lot: `max(event_ts) - allowed_lateness`. If the event's window has closed (`window_end + grace < watermark`), route to `parking.late`.
4. Slot state map per lot. Ignore events older than that slot's last `event_ts` so out-of-order events cannot regress state.
5. **Tumbling 5-minute event-time windows** per `(lot_id, window_start)`: time-weighted average occupancy, min/max, entries, exits, and a **HyperLogLog** of `vehicle_token` (approximate unique vehicles). Make window size, grace, and lateness configurable via env.
6. Emit updated results for open windows every N seconds (dashboard freshness) and a final `closed=true` result when the watermark passes `window_end + grace`.
7. Sensor-offline detection from heartbeats, emitted to `lot.alerts`.

**Recovery design:**
- Checkpoint (slot map, open windows, HLL registers, watermark, dedupe set) to `state.changelog` every ~10s and in `on_revoke`. Restore in `on_assign` for newly assigned partitions.
- Commit order: **produce outputs -> flush -> write checkpoint -> commit offsets.** A crash between steps causes reprocessing; downstream upserts on `(lot_id, window_start)` make results effectively-once. Document this honestly in `DECISIONS.md` (do not claim Kafka exactly-once semantics that this design doesn't provide).
- Graceful SIGTERM handling: flush, checkpoint, commit, close.
- Prometheus metrics: events processed / DLQ / late / duplicate counters, processing latency histogram (event_ts -> emit), open windows gauge, rebalance counter.

**Done when:** unit tests cover out-of-order within grace, beyond grace, duplicates, slot-state regression, window boundary edges, and HLL error bound; and with 2 instances running, killing one causes the other to take over its partitions with **no gaps or duplicates** in the output.

### Phase 3: Storage and sink
TimescaleDB `init.sql`: hypertable `lot_occupancy_5min` (PK `lot_id, window_start`), alerts table, lot metadata table, compression and retention policies, a continuous aggregate for hourly rollups. Sink batches inserts with `ON CONFLICT DO UPDATE` and commits offsets only after the DB commit.
**Done when:** stopping TimescaleDB for 60s and restarting it results in zero lost or duplicated rows.

### Phase 4: Alerter
Full-lot alert at >= 90% occupancy with hysteresis (clear at 85%), plus sensor-offline alerts. Publishes to `lot.alerts`. Notifier is pluggable: console + webhook, Telegram optional via env.

### Phase 5: API gateway
FastAPI service. REST: lots list/metadata, current occupancy, history (range + resolution), alerts, pipeline stats. **WebSocket** stream pushing live occupancy, slot-state deltas, and alerts (consumes Kafka with its own consumer group; fans out to clients). Sensible caching, pagination, CORS, typed response models, OpenAPI docs.

### Phase 6: Frontend (see design brief)

### Phase 7: Observability
Prometheus + `kafka-exporter` + provisioned Grafana dashboard for **pipeline health only** (consumer lag, throughput, latency percentiles, DLQ/late rates, rebalances). The product UI is the custom frontend, not Grafana.

### Phase 8: Chaos scripts and load test
Scripts in `scripts/`, each printing PASS/FAIL against explicit expectations and appending measurements to `docs/RESULTS.md`:

| Scenario | Action | Expected |
|---|---|---|
| Broker failure | stop `kafka-2` mid-run | leader election, no data loss, pipeline continues |
| Processor crash | kill one processor | rebalance, state restored from changelog, no gaps/dupes in DB |
| Sink outage | stop TimescaleDB 60s | replay after restart, idempotent upserts |
| Bad data | `--malformed-pct 5` | DLQ fills, pipeline unaffected |
| Late/duplicate data | `--late-pct 10 --dup-pct 5` | dupes dropped, in-grace late events correct windows, beyond-grace go to `parking.late` |
| Lag spike | 100x replay | lag rises then drains; scaling to 3 processors speeds recovery |

Load test: ramp synthetic rate (1k -> 5k -> 10k+ events/s) with 1, 2, 3 processors; record throughput, p50/p95/p99 end-to-end latency, and consumer lag. Also measure **HLL estimate vs exact unique count** (error % and memory). Output tables/CSV plus charts for the paper.

### Phase 9: Testing and CI
Unit tests (pytest), integration test with testcontainers (known event set in, exact DB rows out), API tests, frontend type-check + a few component tests + one Playwright smoke test. GitHub Actions: lint, type-check, tests on every push.

### Phase 10: Deployment
- `docker-compose.prod.yml`: no public Kafka/Postgres/Prometheus ports, secrets from `.env`, `restart: unless-stopped`, images from GHCR, Caddy reverse proxy for the frontend + API (automatic HTTPS, or plain HTTP by IP).
- GitHub Actions on `main`: build and push images to GHCR, then SSH deploy (`compose pull && up -d`).
- Nightly `pg_dump` cron script, `retention.ms` on raw topic, all config via env.
- `docs/DEPLOY.md`: exact steps for an Ubuntu VM (Oracle Always Free ARM or 4 vCPU / 8 GB), firewall rules (only 22, 80, 443). Make sure images build for both amd64 and arm64.

### Phase 11 (only if everything above is solid): Forecaster
LightGBM on hour-of-day, day-of-week, lags, rolling means, predicting occupancy 30 min ahead. Runs as a consumer publishing to `lot.forecast`; the frontend overlays it as a dashed line. Report MAE against a naive "same as now" baseline in `RESULTS.md`.

---

## Frontend design brief

**Goal:** a control-room console for parking operators that looks like it was designed by a person with taste, not generated. Think Linear, Vercel dashboard, Bloomberg-terminal density with modern restraint, or a good transit-authority operations screen. **Not** a SaaS landing page, not a template.

### What to build (screens)
1. **Overview:** system-wide occupancy (a single big, calm number), a ranked list of lots with inline sparklines and live occupancy bars, an alerts feed, and a compact "pipeline heartbeat" strip (events/s, end-to-end latency, consumer lag) so the streaming is *visible* in the product.
2. **Lot detail:** a **live floor plan** rendered as an SVG slot grid, where each slot flips state in real time as events arrive (brief, subtle transition on change). Beside it: occupancy time series (last 24h with the 5-min windows, open window shown differently from closed ones), entries/exits, estimated unique vehicles (labelled as an approximation), average dwell time, and the forecast overlay if Phase 11 exists.
3. **Alerts:** a filterable table (full-lot, sensor-offline) with timestamps, duration, and status.
4. **Pipeline:** a read-only view of the stream itself: topic/partition throughput, consumer group membership and lag, and DLQ/late counts, plus a visible indicator when a rebalance happens. This is the screen that wins the viva.

### Aesthetic rules (follow strictly)
- **Palette:** near-neutral surfaces (warm off-white and a genuinely good dark theme, both first-class, following system preference with a manual toggle). **One** accent color, used sparingly for the live/selected state. Status colors (free / occupied / warning / critical) must be distinguishable for color-blind users: pair color with shape, position, or label. Define all colors as CSS variables/tokens.
- **Typography:** one high-quality sans for UI (e.g. Geist, IBM Plex Sans, or Instrument Sans) and a monospace for numbers, IDs, and timestamps. **Tabular numerals everywhere numbers update**, so live values never jitter. Set a modular type scale; no more than 3-4 sizes per screen. Self-host the fonts.
- **Layout:** a strict 4/8px spacing grid, a fixed left rail for navigation, information-dense but with generous alignment discipline. Left-align text and data. Hairline 1px borders instead of heavy shadows. Small radii (4-6px). Cards only where grouping genuinely helps; prefer tables, lists, and whitespace.
- **Data visualization:** hand-built SVG or a minimal library (e.g. visx or uPlot). No default-looking chart library themes. Thin lines, direct labels instead of legends where possible, no chart junk, no gradients under lines. Axes muted, data prominent. Empty, loading, and error states are designed, not afterthoughts.
- **Motion:** functional only: 120-200ms ease-out for slot flips, number changes, and panel transitions. Respect `prefers-reduced-motion`. No bouncing, no parallax, no animated backgrounds.
- **Copy:** real, concise, operator-oriented microcopy ("Lot full since 14:32", "Sensor S-A-014 silent for 6 min"). No marketing language, no emoji, no lorem ipsum.
- **Icons:** one consistent thin-stroke set (e.g. Lucide), used sparingly, always with labels where meaning isn't obvious.

### Anti-"AI slop" checklist (reject your own output if any of these appear)
- Purple/blue-to-pink gradients, glassmorphism, glowing borders, neon on dark
- Rounded-2xl cards with big soft shadows arranged in a symmetric 3-column grid
- Emoji or generic stock icons as decoration; a hero section; "Welcome back!" headers
- Every number in a big gradient-text stat card
- Default shadcn/Material/Chakra look with untouched tokens
- Fake data that never changes, or charts that animate on a loop with no real data behind them
- Inconsistent spacing, more than one accent color, centered body text

### Frontend engineering standards
- TypeScript strict, API types generated from the FastAPI OpenAPI schema.
- State: TanStack Query for REST, a small typed WebSocket client with reconnect + backoff and a visible "reconnecting" state. Keep slot-state deltas in a normalized store; don't re-render the whole floor plan per event (batch updates per animation frame).
- Routing via React Router; every screen deep-linkable.
- Accessibility: keyboard navigable, visible focus rings, semantic HTML, ARIA where needed, contrast >= WCAG AA in both themes.
- Responsive down to tablet; mobile is a read-only overview.
- Performance budget: initial JS < 200 kB gzipped, floor plan smooth with 500+ slots.
- Build the design system first (`web/src/ui/`: tokens, Button, Table, Badge, Sparkline, SlotGrid, Stat) and then compose screens from it. Show me screenshots of both themes before moving on.

---

## Definition of done

- `cp .env.example .env && make up` brings up the entire stack from scratch; the frontend shows live data within a minute.
- Every circular requirement is demonstrable via a script or a screen, and `docs/DEMO.md` gives a timed, click-by-click demo script (10-12 minutes) including the broker-kill and processor-kill moments.
- CI is green; tests pass; `RESULTS.md` contains real measured numbers.
- README has: architecture diagram (Mermaid), quick start, design rationale, and a "how each circular requirement is satisfied" table.
- No secrets committed, no dead code, no unexplained magic numbers.

## Start now

Begin by reading this whole brief, then output your plan (repo tree, phase-by-phase approach, the riskiest parts and how you'll de-risk them). Ask me questions only if something is genuinely ambiguous; otherwise make a sensible decision and record it in `docs/DECISIONS.md`. Wait for my go-ahead before writing code.
