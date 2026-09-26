# Measured results

Every number below was produced by a script or command in this repo; the command is recorded next to it.
Environment: MacBook (Apple Silicon, aarch64), Docker Desktop with 8 GB / 10 CPUs.

## Phase 0: cluster bring-up
- `docker compose up -d` from clean images: all three brokers healthy in ~20 s, init container exit 0.
- `kafka-topics --describe`: `parking.raw` leaders spread across brokers 1/2/3 (2 partitions each), ISR = 1,2,3 for every partition, 0 under-replicated partitions.
