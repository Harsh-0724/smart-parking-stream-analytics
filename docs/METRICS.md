# Metrics reference

Prometheus (`localhost:9090`) scrapes every 5 s: all processor replicas (`:8000`, discovered via DNS so `--scale processor=N` is picked up), the sink (`:8001`), the alerter (`:8002`) and `kafka-exporter` (`:9308`, consumer-group lag, topic/partition offsets, broker count).

| Metric | Type | Meaning |
|---|---|---|
| `parking_events_processed_total` | counter | valid events accepted into state |
| `parking_events_dlq_total` | counter | messages sent to `parking.dlq` |
| `parking_events_late_total` | counter | events routed to `parking.late` |
| `parking_events_duplicate_total` | counter | events dropped by the dedupe set |
| `parking_events_replay_skipped_total` | counter | events skipped after a restore because the checkpoint already contains them |
| `parking_outputs_total{topic}` | counter | messages produced, per output topic |
| `parking_rebalances_total{kind}` | counter | `assign` / `revoke` / `lost` callbacks |
| `parking_open_windows` | gauge | open windows on this instance |
| `parking_owned_partitions`, `parking_owned_lots` | gauge | what this instance currently owns |
| `parking_ingest_to_consume_seconds` | histogram | **raw ingestion latency**: producer `ingest_ts` to the processor having the event, no windowing (sampled 1 in 8) |
| `parking_ingest_to_emit_seconds` | histogram | **windowed-emit latency**: producer `ingest_ts` to the 5 s emit that reflects the event, so it includes the wait for the next emit (sampled 1 in 8) |
| `parking_checkpoint_seconds`, `parking_checkpoint_bytes` | histogram / gauge | flush + checkpoint + commit duration, checkpoint size |
| `parking_restore_seconds` | histogram | time to rebuild state for newly assigned partitions |
| `sink_rows_written_total{table}`, `sink_batches_total`, `sink_db_errors_total`, `sink_invalid_messages_total`, `sink_batch_seconds` | mixed | sink throughput and database health |
| `alerter_alerts_total{state}`, `alerter_notifications_total`, `alerter_notification_failures_total` | counter | alerter activity |
| `kafka_consumergroup_lag{consumergroup,topic,partition}` | gauge | from kafka-exporter |

Useful queries:

```
sum(rate(parking_events_processed_total[30s]))                                  # events/s
histogram_quantile(0.95, sum by (le) (rate(parking_ingest_to_emit_seconds_bucket[1m])))       # windowed-emit p95
histogram_quantile(0.95, sum by (le) (rate(parking_ingest_to_consume_seconds_bucket[1m])))    # raw ingestion p95
sum by (consumergroup) (kafka_consumergroup_lag)
sum by (kind) (increase(parking_rebalances_total[5m]))
```
