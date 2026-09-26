"""SQL for the sink. Every statement is an idempotent upsert."""

import json
from typing import Any

import psycopg

from common.schemas import Alert, AlertState, LotMetadata, WindowResult

# A closed window is final: a replayed open-window update must never turn it back into open.
UPSERT_WINDOW = """
INSERT INTO lot_occupancy_5min (
    lot_id, window_start, window_end, capacity, avg_occupied, avg_occupancy_pct,
    min_occupied, max_occupied, entries, exits, unique_vehicles_est, closed,
    current_occupied, emitted_at, latest_ingest_ts)
VALUES (%(lot_id)s, %(window_start)s, %(window_end)s, %(capacity)s, %(avg_occupied)s,
    %(avg_occupancy_pct)s, %(min_occupied)s, %(max_occupied)s, %(entries)s, %(exits)s,
    %(unique_vehicles_est)s, %(closed)s, %(current_occupied)s, %(emitted_at)s,
    %(latest_ingest_ts)s)
ON CONFLICT (lot_id, window_start) DO UPDATE SET
    window_end = EXCLUDED.window_end, capacity = EXCLUDED.capacity,
    avg_occupied = EXCLUDED.avg_occupied, avg_occupancy_pct = EXCLUDED.avg_occupancy_pct,
    min_occupied = EXCLUDED.min_occupied, max_occupied = EXCLUDED.max_occupied,
    entries = EXCLUDED.entries, exits = EXCLUDED.exits,
    unique_vehicles_est = EXCLUDED.unique_vehicles_est, closed = EXCLUDED.closed,
    current_occupied = EXCLUDED.current_occupied, emitted_at = EXCLUDED.emitted_at,
    latest_ingest_ts = EXCLUDED.latest_ingest_ts
WHERE NOT (lot_occupancy_5min.closed AND NOT EXCLUDED.closed)
"""

# RAISED never resurrects an alert that was already cleared (a replay can re-deliver it).
RAISE_ALERT = """
INSERT INTO alerts (alert_id, kind, lot_id, sensor_id, status, raised_at, message, occupancy_pct)
VALUES (%(alert_id)s, %(kind)s, %(lot_id)s, %(sensor_id)s, 'active', %(ts)s, %(message)s,
    %(occupancy_pct)s)
ON CONFLICT (alert_id) DO NOTHING
"""

CLEAR_ALERT = """
INSERT INTO alerts (alert_id, kind, lot_id, sensor_id, status, raised_at, cleared_at, message,
    occupancy_pct)
VALUES (%(alert_id)s, %(kind)s, %(lot_id)s, %(sensor_id)s, 'cleared', %(ts)s, %(ts)s,
    %(message)s, %(occupancy_pct)s)
ON CONFLICT (alert_id) DO UPDATE SET
    status = 'cleared',
    cleared_at = COALESCE(alerts.cleared_at, EXCLUDED.cleared_at),
    message = EXCLUDED.message,
    updated_at = now()
"""

UPSERT_LOT = """
INSERT INTO lot_metadata (lot_id, name, personality, capacity, columns, latitude, longitude,
    slot_ids)
VALUES (%(lot_id)s, %(name)s, %(personality)s, %(capacity)s, %(columns)s, %(latitude)s,
    %(longitude)s, %(slot_ids)s::jsonb)
ON CONFLICT (lot_id) DO UPDATE SET
    name = EXCLUDED.name, personality = EXCLUDED.personality, capacity = EXCLUDED.capacity,
    columns = EXCLUDED.columns, latitude = EXCLUDED.latitude, longitude = EXCLUDED.longitude,
    slot_ids = EXCLUDED.slot_ids, updated_at = now()
"""


def merge_windows(results: list[WindowResult]) -> list[WindowResult]:
    """Collapse a batch to one result per key, keeping the last, and never open over closed."""
    latest: dict[str, WindowResult] = {}
    for r in results:
        prev = latest.get(r.key)
        if prev is not None and prev.closed and not r.closed:
            continue
        latest[r.key] = r
    return list(latest.values())


def write_batch(
    conn: psycopg.Connection[Any],
    windows: list[WindowResult],
    alerts: list[Alert],
    lots: list[LotMetadata],
) -> None:
    """One transaction for the whole batch; the caller commits Kafka offsets only after it."""
    with conn.cursor() as cur:
        if lots:
            cur.executemany(
                UPSERT_LOT,
                [
                    {**m.model_dump(exclude={"slot_ids"}), "slot_ids": _json(m.slot_ids)}
                    for m in lots
                ],
            )
        if windows:
            cur.executemany(UPSERT_WINDOW, [r.model_dump() for r in windows])
        for a in alerts:  # order matters: RAISED before CLEARED
            sql = RAISE_ALERT if a.state is AlertState.RAISED else CLEAR_ALERT
            cur.execute(sql, {**a.model_dump(), "kind": a.kind.value})
    conn.commit()


def _json(value: object) -> str:
    return json.dumps(value)
