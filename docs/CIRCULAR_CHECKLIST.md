# Circular checklist: where each mandatory requirement is satisfied

Status legend: DONE (implemented and verified by running it), PENDING (planned phase).

| # | Requirement | Status | Evidence (file / function / topic / script) |
|---|---|---|---|
| 1 | Kafka is the central platform | PARTIAL | 3-broker KRaft cluster: `docker-compose.yml` (services `kafka-1..3`); topics: `infra/create_topics.sh`. Verified in Phase 0 (see `docs/RESULTS.md`). |
| 2 | At least one producer and one consumer | PARTIAL | Producer DONE: `simulator/main.py` (`Sender.send`, idempotent producer config in `common/kafka.py:producer_config`). Consumers: Phase 2-4 |
| 3 | At least two topics | DONE | 7 topics created by the `init` service via `infra/create_topics.sh` |
| 4 | Partitioning and consumer groups | PARTIAL | Partitioning DONE: `parking.raw` 6 partitions, key = `lot_id` (`simulator/main.py`, `common/partitioning.py:balanced_lot_ids`); measured even spread in RESULTS.md. Groups and live rebalance: Phase 2 / 8 |
| 5 | Window-based streaming operation | PENDING | Phase 2 |
| 6 | Aggregation / summarization algorithm | PENDING | Phase 2 |
| 7 | Fault tolerance / recovery | PENDING | Phase 2 / 8 |
| 8 | Results stored in DB / time-series store | PENDING | Phase 3 |
| 9 | Real-time visualization or alert mechanism | PENDING | Phase 4 (alerts); dashboard next session |
