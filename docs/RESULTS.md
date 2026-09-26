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
