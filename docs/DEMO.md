# Live demo script (11 minutes)

Covers every circular requirement in order. It doubles as the SEE presentation script.
All commands run from the repository root. Screens: **O** Overview `http://localhost:8081/`,
**L** Lot detail `http://localhost:8081/lots/LOT-01`, **A** Alerts `/alerts`, **P** Pipeline `/pipeline`,
**G** Grafana `/grafana/` (admin / `GRAFANA_PASSWORD`).

## Pre-demo checklist (do this 8 minutes before)

- [ ] **Reset event-time state.** Event-time state must not survive between runs that use different
      simulator start times, or every new event is judged against a stale watermark and lands in `parking.late`.
      Run `make demo-reset` (recreates topics, empties the tables, restarts everything).
- [ ] Start the simulated clock near the morning peak so alerts fire during the demo:
      set `SIM_START_HOUR=8` in `.env` **before** the reset (the office lots reach 92% about 5 minutes after start).
- [ ] Never run `docker compose up --build` after the reset: it recreates the simulator and processors with a
      new clock. Only `make demo-reset` changes state.
- [ ] Confirm health (immediately after the reset it was already `ok` in the rehearsal; the Overview strip shows `-` for about 2 s after each page load until the first WebSocket message): `curl -s localhost:8081/api/health` shows `"status":"ok"`; **O** shows "Live", 12 lots and
      a non-zero events/s; **P** shows `occupancy-processor` Stable with 2 members holding 3 partitions each;
      "Late" on **P** is 0.
- [ ] Note the processor container names: `docker compose ps processor` (for example `spa_lab_parking-processor-1`).
- [ ] Terminal 1: `docker compose logs -f --since 1m processor`. Terminal 2 (commands below). Browser tabs O, L, A, P, G.
- [ ] Do a dry run of both kills once, then reset again. The repeatable dry run is
      `cd web && E2E_DESTRUCTIVE=1 pnpm exec playwright test recovery` (kills a processor and the busiest broker and
      checks the UI shows both). After it, restore two processors: `docker compose up -d --scale processor=2 processor`.

## Timeline

| Time | Screen | Say and do | Circular requirement |
|---|---|---|---|
| 0:00 | slide: README diagram | Smart parking occupancy: sensors emit events, Kafka carries them, a stream processor windows them, TimescaleDB stores results, a live UI shows them. Point at the pipeline arrows. | **1** Kafka is the central platform |
| 1:00 | **O** | Live numbers: 1,800 slots in 12 lots, events/s, windowed-emit latency (about 4.8 s p95 because it waits for the 5 s window emit; raw ingestion latency, produce to processor, is tens of milliseconds and is in Grafana and RESULTS.md), brokers 3 of 3. The strip along the top is the pipeline, visible in the product. Terminal: `docker compose logs --tail 3 simulator` shows the producer (idempotent, `acks=all`, lz4). | **2** producer |
| 2:00 | **P**, Topics table | Seven topics created by the `init` container. `parking.raw` has 6 partitions keyed by `lot_id`, replication factor 3, min ISR 2. Say why keying by lot matters (one lot's events stay ordered) and that lot IDs are chosen to hash evenly (DECISIONS D6). | **3** two or more topics, **4** partitioning |
| 3:00 | **P**, Consumer groups | Three groups: `occupancy-processor` (2 instances, 3 partitions each, shown as the partition map), `timescale-sink`, `alerter`. Consumers commit manually; cooperative-sticky assignor. | **2** consumer, **4** consumer groups |
| 4:00 | **L** | Live floor plan: every slot flip is a real event from Kafka. Occupied is a filled square, free an outlined square (never colour alone). Time series: solid = closed 5-minute windows, dashed with hollow marker = the open window. Entries, exits, vehicles (approximate, HyperLogLog), dwell. | **5** window operation, **6** aggregation/summarisation |
| 5:00 | **A** | (Rehearsed: 8 alerts already existed 13 minutes after the reset, all `FULL_LOT`; the newest may be a "has space again" message.) When a lot reaches 90% a full-lot alert is raised; it clears at 85% (hysteresis, so it cannot flap). Sensor-offline alerts come from heartbeats. Overview feed and the DB agree. | **9** alert mechanism |
| 6:00 | **P** | **Kill a processor.** Terminal 2: `docker kill <processor-container>`. | **7** fault tolerance |
| 6:10 | **P** | About 9 to 10 s after the kill (10 s session timeout) the banner appears (rehearsed: 9.4 s). It is labelled **"Rebalanced"**, not "Rebalancing": the rebalance finishes inside the 2 s poll, so the UI catches it afterwards and keeps the banner for 15 s. Its first text is transitional (`<id> holds 0`); about 2 s later the Rebalance log reads `<id> holds 6`, the survivor owns all 6 partitions, and the partition-map cells highlight. Say "the group briefly holds nothing while partitions are reassigned". Terminal 1 shows `partition assigned ... restored_lots [...] resume_offset ...`: state was restored from `state.changelog`. | **4** live rebalance, **7** |
| 7:00 | **L** | The floor plan and chart never stopped. Explain the commit order: produce outputs, flush, write checkpoint, commit offsets. State honestly: **at-least-once processing, effectively-once stored results** through idempotent upserts on `(lot_id, window_start)`. This is not Kafka exactly-once. | **7** |
| 7:30 | **P** | Bring it back: `docker compose up -d --scale processor=2 processor`. A second rebalance about 5 s later (rehearsed: 5.1 s). Cooperative: the survivor gave up only partitions 0, 1, 2 and kept the other three (its log shows `partitions revoked [0, 1, 2]`), so 3 of 6 move. **Observed once in the timed rehearsal:** the restore command recreated *both* containers (both created at that second, so all 6 partitions changed owner). It did not reproduce in two controlled attempts (the survivor kept its container and the killed one restarted with the same ID), and the demo works either way, but do not promise "3 of 6" unless the survivor's container ID is unchanged in `docker ps`. | **4** |
| 8:30 | **P** | **Kill a broker.** Terminal 2: `docker kill kafka-N` where N is the broker with the most partitions in the **Leads** column on **P** (leadership drifts, so check first; killing a broker that leads nothing proves nothing). Brokers table: that broker turns **Down about 12 s after the kill** (rehearsed: 12.3 s: about 9 s for the controller to notice plus the API's 3 s Kafka timeout and the 2 s poll; until then the screen keeps the last good snapshot), and its partitions are already on the survivors (Leads sum back to 19). **Expect a second rebalance banner:** the broker you killed usually hosts the processor group's coordinator, so the group goes to `PreparingRebalancing` (the member list briefly shows duplicate entries, every partition reads "unassigned" and committed lag climbs, see `docs/screenshots/rehearsal-8-broker-killed.png`) and then recovers by itself without any container restarting; say so, it is the same rebalance mechanism as the processor kill. Topics show a "degraded" ISR badge (RF 3, min ISR 2, so writes continue); events/s stays non-zero (rehearsed: 1,179/s). **G**: Brokers up = 2 and under-replicated partitions = 64 (this counts Kafka's internal topics too). | **7** |
| 9:30 | **P** | `docker compose start kafka-N`. The row is Up about 3.5 s later and the ISR is full within seconds (rehearsed). **The restarted broker then leads 0 partitions** (rehearsed: kafka-1=0, kafka-2=13, kafka-3=6); Kafka only rebalances leaders back after about 5 minutes. For any second broker kill, pick the busiest broker again, not the one you just restarted. Numbers from the chaos scripts: leader election 9.1 s, 0 producer errors, 0 lost windows (RESULTS.md). | **7** |
| 10:00 | terminal | Storage: `docker compose exec timescaledb psql -U parking -d parking -c "select lot_id, window_start, avg_occupied, entries, exits, unique_vehicles_est, closed from lot_occupancy_5min order by window_start desc limit 5"`, then `\d+ lot_occupancy_5min` (hypertable, compression) and `select * from lot_occupancy_hourly limit 3` (continuous aggregate). | **8** database / time-series store |
| 10:30 | **G** | Pipeline health dashboard for operators: lag, throughput, latency percentiles, DLQ and late rates, rebalances. The product UI is the custom frontend. | supports **7**, **9** |
| 11:00 | slide: checklist | `docs/CIRCULAR_CHECKLIST.md`: nine requirements, each with a file and function. Six failure drills all PASS in `docs/RESULTS.md`; CI runs unit, integration and browser tests. | all |

## If something goes wrong

- **Rebalance takes longer than 20 s:** say the session timeout is 10 s plus rebalance; keep talking through
  the commit order; check `docker compose ps`.
- **"Late" counter rising on P:** the simulator clock and the processors disagree (stale state). Run
  `make demo-reset` and wait 2 minutes; do not continue the live kill demo on a stale stack.
- **No alerts yet:** the office lots need the morning peak. Show the Alerts screen from a previous run
  (`docs/screenshots/`), or wait; `SIM_START_HOUR=8` shortens this.
- **UI shows "Reconnecting":** the API restarted; it recovers by itself (backoff 0.5 s to 10 s).

## Cheat sheet

```bash
docker compose ps processor                          # container names
docker kill <processor-container>                    # 6:00
docker compose up -d --scale processor=2 processor   # 7:30
docker kill kafka-N                                  # 8:30, N = broker with most Leads
docker compose start kafka-N                         # 9:30
curl -s localhost:8081/api/pipeline | python3 -m json.tool | head -40
make chaos-processor                                 # scripted, self-checking version of the same drill
```

## Rehearsal record (2026-09-27, followed exactly as written)

Run from `make demo-reset` with `SIM_START_HOUR=8`; T0 = 8 minutes after the reset finished; every action performed at the stated time by a script that also read the UI (Playwright) and Kafka, so the times below are measured, not estimated.

| Stated | Observed |
|---|---|
| Pre-demo | reset done; API `ok` immediately; processor group Stable with 2 members holding 3 partitions each; late 0; 3 brokers; 1,196 events/s |
| 1:00 **O** | 1,109 of 1,800 slots, 12 lot rows; simulator log shows 1,206 events/s |
| 2:00 **P** | 7 topics; `parking.raw` 6 partitions, replication 3, ISR 3 |
| 3:00 **P** | 3 groups; partition map 6 cells, 2 distinct owners |
| 4:00 **L** | 120-slot floor plan drawn; occupied 109 to 105 within 15 s (flipping); dashed open window present |
| 5:00 **A** | 8 alerts, all `FULL_LOT` |
| 6:00 kill processor | banner 9.4 s after the kill; survivor holds 6; log shows `restored_lots ["LOT-01","LOT-04"] resume_offset 137141` |
| 7:00 **L** | floor plan still flipping (91 to 92 in 15 s) |
| 7:30 restore | second owner after 5.1 s (in this run both containers were recreated; see the 7:30 row above) |
| 8:30 kill broker | busiest was kafka-1 (8 of 19 leaders); row Down after 12.3 s; leaders back to 19 on survivors; "degraded" ISR badge; 1,179 events/s; Prometheus brokers 2, under-replicated 64; processor group rebalanced (screenshot) |
| 9:30 restart | row Up after 3.5 s; ISR full; leaders now kafka-1=0, kafka-2=13, kafka-3=6 |
| 10:00 psql | 5 rows; `\d+` shows the hypertable and compression settings; hourly aggregate returns 3 rows |
| 10:30 **G** | "Pipeline health", 13 panels; brokers 3, under-replicated 0 |
| 11:00 final | health ok; group Stable, 2 members holding 3 and 3; 3 brokers; no degraded ISR; late 0; DLQ 0; restart counts 0 on processors, sink, alerter, API |

Every step happened at its stated time. What the script corrected in this document: the banner wording, the 12 s (not 9 s) broker-down time, the second rebalance on a broker kill, the restarted broker leading nothing, and the one-off recreation on restore.
