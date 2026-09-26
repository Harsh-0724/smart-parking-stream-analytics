# Syllabus rubric mapping (report, video and viva; not the circular gate)

The circular checklist is in `docs/CIRCULAR_CHECKLIST.md`. This file maps the same work to the course grading scheme.

## 1. Course outcomes (exact wording)

| CO | Outcome | Evidence in this repo |
|---|---|---|
| **CO1** | Describe the need and the application of real time and stream processing in real world applications. | The application is live parking availability. A batch job answers "how full was the lot yesterday"; a driver needs "is there a free slot now". The system shows the difference: ingest-to-emit p95 4.8 s (RESULTS.md, load test) against a batch's hours, and a floor plan that flips slot by slot (`web/`, `docs/screenshots/`). Need and design context: README, DECISIONS D7, D12. |
| **CO2** | Comprehend and apply the various operations like data ingestion, data communication, data analysis and storage for different streaming data applications. | **Ingestion:** `simulator/` (synthetic demand curves and the UCI Birmingham replay) and `scripts/loadgen.py`. **Communication:** Kafka topics, partitioning by `lot_id`, three consumer groups, manual commits (`common/topics.py`, `common/kafka.py`, `infra/create_topics.sh`). **Analysis:** event-time windows, watermark, dedupe, HyperLogLog (`processor/lot_state.py`, `processor/hll.py`). **Storage:** TimescaleDB hypertable, compression, retention and an hourly continuous aggregate (`infra/timescale/init.sql`, `sink/db.py`). |
| **CO3** | Investigate and apply streaming concepts using modern tools to solve problems related to society and industry. | Streaming concepts applied and *investigated*: watermarks and grace, at-least-once versus exactly-once (D10), checkpoint/restore (D11), idempotent sinks (D16), sketches versus exact counting (RESULTS: HLL table). The investigation is recorded as experiments with expected and measured results: six chaos drills, a load test that found a scalability limit (bug 12), and fourteen other defects found by running the system (RESULTS.md, "Bugs and flaws found by running things for real"). Tools: Kafka KRaft, TimescaleDB, Prometheus, Grafana, FastAPI, React, Playwright, GitHub Actions. |
| **CO4** | Demonstrate a prototype application for streaming data using Kafka as a team / individual. | A complete working prototype: `make up`, live console at `:8081`, `docs/DEMO.md` (11-minute script including a live processor kill and broker kill), CI that starts the whole stack and tests it end to end, multi-arch images on GHCR. |
| **CO5** | Demonstrate solutions for societal and environmental concern problems using modern engineering tools through writing effective reports. | Section 2 below: an estimate of driver search time and emissions, with only two measured inputs from this project and every other parameter cited or labelled as an assumption. Report material is organised in section 5. |

Fault tolerance also touches CO5: a parking-availability service that silently shows stale or wrong numbers after a
failure would send drivers to full lots, so the recovery guarantees (broker kill, processor kill, database outage with
zero lost or duplicated rows) are part of the societal claim, not only an engineering nicety.

## 2. CO5 estimate: driver search time and emissions

**This is an estimate, not a measurement.** Two inputs were measured in this project. Everything else is from a cited
source or is an explicit assumption, and the table shows how much the answer moves when they change.

**Measured here**

- One processor sustained about **100,000 events/s** with no backlog (load test; 30% state-changing events, generator on the same machine). At one heartbeat per sensor per 30 s that is on the order of 3 million sensors per processor if traffic were only heartbeats; halving it for state changes and headroom gives **about 1.5 million slots** per processor. The 12-lot demo (1,800 slots) uses well under 1% of one processor, so the pipeline is not what limits a city-scale deployment.
- **Ingest-to-emit p95 = 4.8 s** at every tested rate (set by the 5 s emit interval). In the demo data the average lot changes state about 1.9 times per 5-minute window (0.0065 changes/s; morning ramp, from the `lot_occupancy_5min` table), so the chance that a lot's displayed count is out of date by the time it is shown is roughly 0.0065 x 4.8 = 3%.

**Cited**

- Shoup, "Cruising for parking", *Transport Policy* 13(6), 2006: across 16 studies (1927-2001) the average time to find a curb space was about 8 minutes (range 3.5 to 15.4) and about 30% of traffic was cruising (range 8% to 74%). Studies were done where cruising was expected, so this is selective. [shoup.bol.ucla.edu/Cruising.pdf](http://shoup.bol.ucla.edu/Cruising.pdf)
- Counter-evidence: Millard-Ball, Hampshire and Weinberger (2020), "Parking behavior: the curious lack of cruising for parking in San Francisco", found far less cruising than the older studies. The low scenario below reflects that. [millardball.its.ucla.edu](https://millardball.its.ucla.edu/wp-content/uploads/sites/22/2022/06/Millard-Ball_Hampshire_Weinberger_2020_Curious_lack_of_cruising_for_parking_preprint.pdf)
- EPA/DOT common factor: 8,887 g CO2 per gallon of gasoline. [epa.gov](https://www.epa.gov/energy/greenhouse-gas-equivalencies-calculator-calculations-and-references)
- Idling burns roughly 0.16 to 0.4 gal/h for passenger cars (published EPA idle-reduction material). A cruising car burns more than an idling one, so using 0.2 gal/h understates the saving.

**Assumptions (labelled, not sourced)**

- 1,800 slots (the demo size) with 1.5 arrivals per slot per day = 2,700 arrivals/day.
- Live availability removes 25% of search time (drivers stop trying full lots). This is the least certain number; the result scales linearly with it.

| Scenario | Cruising share | Search time | Cruising arrivals/day | Search time saved | CO2 saved |
|---|---|---|---|---|---|
| Low (San Francisco-like) | 8% | 3.5 min | 216 | 3.1 h/day | 5.6 kg/day, 2.0 t/year |
| Mid (Shoup averages) | 30% | 8 min | 810 | 27 h/day | 48 kg/day, 17.5 t/year |
| High (Shoup upper range) | 74% | 15.4 min | 1,998 | 128 h/day | 228 kg/day, 83 t/year |

Method: saved minutes = arrivals x cruising share x search minutes x 25%; CO2 = saved minutes x (0.2 gal/h / 60) x 8,887 g/gal = 29.6 g per minute. The honest reading: a campus-sized deployment plausibly saves between a few and a few tens of kilograms of CO2 and a few to a few dozen driver-hours per day; the low end is the safer claim.

## 3. CIE: Experiential Learning (case study 10, program-specific requirements 10, video demonstration 20)

**Case study (10).** The parking pipeline is the case: problem statement (README, CO1 row), design choices with alternatives
(`docs/DECISIONS.md`, D1-D27), measured outcomes (`docs/RESULTS.md`), failures and fixes (bug table), societal estimate (section 2).

**Program-specific requirements (10).** These are satisfied by the department circular's nine mandatory pipeline
requirements, each with a file and function: `docs/CIRCULAR_CHECKLIST.md`. (If your program has additional named
requirements beyond the circular, add them here; I have not seen them.)

**Video demonstration (20): outline, about 8 minutes, screen recording with voice-over, cut from `docs/DEMO.md`.**

| Time | Show | Say |
|---|---|---|
| 0:00 | README architecture diagram | The problem and the pipeline in one sentence per arrow |
| 0:45 | Overview screen, live | Numbers moving in real time; the pipeline strip is the stream, visible in the product |
| 1:45 | Pipeline screen | Seven topics, six partitions keyed by lot, three consumer groups, the partition map |
| 2:45 | Lot detail | Floor plan flipping from real events; closed versus open window; approximate vehicles (HyperLogLog) |
| 3:45 | Alerts screen | Full-lot alert at 90%, clears at 85% |
| 4:15 | Kill a processor | Rebalance banner, partitions move in about 10 s, state restored from the changelog; say "at-least-once, effectively-once results" |
| 5:45 | Kill a broker | Broker Down, leaders re-elected in about 9 s, data keeps flowing |
| 7:00 | Terminal: TimescaleDB query, then RESULTS.md chaos table | Storage requirement; every drill printed PASS against explicit expectations |
| 7:40 | GitHub Actions run | Green backend, integration, frontend, Playwright and image jobs |

## 4. SEE Lab rubric (design and development 20, presentation of working model 20, viva 10)

- **Design and development (20):** `docs/DECISIONS.md` (alternatives and reasons), `docs/CIRCULAR_CHECKLIST.md` (traceability), code structure in the README, CI configuration.
- **Presentation of working model (20):** **`docs/DEMO.md` doubles as the presentation script**: timed, click-by-click, states exactly when to kill the processor (6:00) and the broker (8:30), and starts with the pre-demo checklist including `make demo-reset`.
- **Viva (10):** section 6.

## 5. Also required for SEE: poster and written report (mine to produce)

The SEE component expects a **poster** and a **written report**. These are yours to write; the source material is in the repository:

| Section of report or poster | Source |
|---|---|
| Problem, need for streaming (CO1) | README; section 2 above (Shoup and Millard-Ball sources) |
| Architecture and design decisions | README diagram; `docs/DECISIONS.md` D1-D27 |
| Windowing, watermark, HyperLogLog | `docs/DECISIONS.md` D12, D13; `processor/lot_state.py`; RESULTS.md HLL table (worst error 7.57% at 1M vehicles, bound 9.75%; 1 KiB versus 88 MB) |
| Fault tolerance experiments | `docs/RESULTS.md` Phase 8 (six drills, PASS/FAIL against explicit expectations); `scripts/chaos_*.py` |
| Performance | RESULTS.md load test (`docs/loadtest/load_test.png`, `.csv`); Phase 7 latency; lag-spike drain rates |
| **Bugs found and fixed (strong evidence for CO3 and CO4)** | RESULTS.md "Bugs and flaws found by running things for real": the **alerter key/value swap** (bug 1), the **closed-window capacity race** (bug 2), the **checkpoint-size crash found by the load test** (bug 12), the **consumer crash on a coordinator broker kill** (bug 13) and the **blank Pipeline screen during failover** (bug 14), found only by driving the live UI, and the measurement mistakes (bugs 8-10), which show method, not only results |
| UI and design system | `docs/screenshots/` (light and dark), DECISIONS D21, `web/src/ui/` |
| Deployment | `docs/DEPLOY.md` |
| Societal and environmental impact (CO5) | Section 2 above |
| Poster figures | `docs/screenshots/overview-dark.png`, `pipeline-light.png`, `grafana-pipeline-health.png`, `docs/loadtest/load_test.png`; the Mermaid diagram in the README |

## 6. Viva preparation: the hardest questions, with one-line answers

1. **Is this exactly-once?** No. It is at-least-once processing with effectively-once stored results: outputs are produced and flushed, then the checkpoint is written, then offsets are committed, so a crash replays events; the sink's upserts on `(lot_id, window_start)` make the replay harmless, and a closed window can never be reopened. It does not use Kafka transactions (DECISIONS D10, D16).
2. **Why are the lot IDs so oddly numbered?** With few keys, plain `LOT-01..LOT-12` hash 4/1/3/0/2/2 across 6 partitions under Kafka's murmur2, leaving a partition idle; `balanced_lot_ids` picks IDs that fill partitions evenly (D6). The real Birmingham replay shows the natural skew (392k to 1.14M events per partition).
3. **Why HyperLogLog instead of counting distinct vehicles exactly?** Constant 1 KiB per window with about 3.25% standard error, versus an exact set that grows with traffic (88 MB at 1M vehicles); measured worst error 7.57% at 1M against a 9.75% bound. It also stores no per-vehicle identifiers, which helps privacy (D13, RESULTS).
4. **Why can't the alerter be scaled out or made highly available?** It is a single instance by design: hysteresis state is per lot and a lot's results span partitions. This is a deliberate scope decision, not one of the nine circular-mandated fault-tolerance items, which concern the Kafka pipeline itself (broker and processor) and are proven by the chaos drills. Its state is rebuilt from `lot.alerts` on restart, so a restart is safe; HA is out of scope (D17).
5. **What happens to a late event?** If its window is still open (end + grace is not behind the watermark) it lands in its window and corrects the time-weighted average retroactively; otherwise it goes to `parking.late` (D12, D13; drill A: 0 mismatches over 216 windows).
6. **What if the processor crashes between writing the checkpoint and committing offsets?** It restarts from the oldest `resume_offset` in the partition, each lot skips events its own state already contains, and a real duplicate is caught by the dedupe set (D11).
7. **Why commit offsets only at checkpoints, and what does that do to lag?** The commit order is what makes the guarantee above hold; the cost is that committed-offset lag saw-tooths by up to 10 s of traffic, so the UI labels it and true backlog is measured differently (D19, D22).
8. **Why not use Kafka Streams or Flink?** The brief requires explainable own logic; every rule is small enough to defend line by line (`processor/lot_state.py`, tested in `tests/unit/test_lot_state.py`).
9. **How do you know the results are right?** The simulator writes an independent ground truth per window; every drill compares the database with it (hundreds of windows, 0 mismatches), and the integration test asserts hand-derived expected values.
10. **What did the load test show?** One processor sustained about 100,000 events/s with no backlog; latency is flat at 4.8 s p95 because of the 5 s emit interval. It also found a real limit: checkpoints grew with the event rate and exceeded Kafka's 1 MB message limit at 25,000 events/s, fixed by not de-duplicating heartbeats and raising the limit (D27).
11. **Why does the API not join a consumer group?** It must see every message (fan-out, not work sharing), so it uses manual partition assignment and bootstraps slot state from the processor's own checkpoints (D20).
12. **What are the limits of the average-dwell and vehicle counts on the screen?** Both are labelled estimates: HyperLogLog has about 3% error, and Little's law underestimates dwell while a lot is still filling (D20).
