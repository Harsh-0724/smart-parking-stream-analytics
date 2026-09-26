"""Read queries against TimescaleDB."""

from datetime import datetime, timedelta
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

SPARK_POINTS = 24  # 24 windows = 2 hours


async def fetch(
    pool: AsyncConnectionPool, sql: str, params: tuple[Any, ...] = ()
) -> list[dict[str, Any]]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return list(await cur.fetchall())


async def lots_with_latest(pool: AsyncConnectionPool) -> list[dict[str, Any]]:
    return await fetch(
        pool,
        """
        SELECT m.lot_id, m.name, m.personality, m.capacity, m.columns, m.latitude, m.longitude,
               m.slot_ids, w.window_start AS as_of,
               COALESCE(w.current_occupied, round(w.avg_occupied)::int) AS occupied
        FROM lot_metadata m
        LEFT JOIN LATERAL (
            SELECT window_start, current_occupied, avg_occupied
            FROM lot_occupancy_5min WHERE lot_id = m.lot_id
            ORDER BY window_start DESC LIMIT 1
        ) w ON true
        ORDER BY m.lot_id
        """,
    )


async def sparklines(pool: AsyncConnectionPool) -> dict[str, list[float]]:
    rows = await fetch(
        pool,
        """
        SELECT lot_id, array_agg(avg_occupancy_pct ORDER BY window_start) AS spark
        FROM (
            SELECT lot_id, window_start, avg_occupancy_pct,
                   row_number() OVER (PARTITION BY lot_id ORDER BY window_start DESC) AS rn
            FROM lot_occupancy_5min WHERE window_start > now() - interval '2 days'
        ) t WHERE rn <= %s GROUP BY lot_id
        """,
        (SPARK_POINTS,),
    )
    return {r["lot_id"]: [float(x) for x in r["spark"]] for r in rows}


async def active_alert_count(pool: AsyncConnectionPool) -> int:
    rows = await fetch(pool, "SELECT count(*) AS n FROM alerts WHERE status = 'active'")
    return int(rows[0]["n"])


async def lot_stats(pool: AsyncConnectionPool, lot_id: str) -> dict[str, Any]:
    rows = await fetch(
        pool,
        """
        WITH last AS (SELECT max(window_start) AS t FROM lot_occupancy_5min WHERE lot_id = %(lot)s)
        SELECT coalesce(sum(entries), 0)::int AS entries, coalesce(sum(exits), 0)::int AS exits,
               sum(avg_occupied) * 300 AS occupied_seconds,
               (SELECT unique_vehicles_est FROM lot_occupancy_5min
                 WHERE lot_id = %(lot)s AND closed ORDER BY window_start DESC LIMIT 1) AS unique_now
        FROM lot_occupancy_5min, last
        WHERE lot_id = %(lot)s AND window_start > last.t - interval '24 hours'
        """,
        {"lot": lot_id},  # type: ignore[arg-type]
    )
    return rows[0]


async def full_since(pool: AsyncConnectionPool, lot_id: str) -> datetime | None:
    rows = await fetch(
        pool,
        "SELECT raised_at FROM alerts WHERE lot_id = %s AND kind = 'FULL_LOT' AND status = 'active' "  # noqa: E501
        "ORDER BY raised_at DESC LIMIT 1",
        (lot_id,),
    )
    raised: datetime | None = rows[0]["raised_at"] if rows else None
    return raised


async def latest_window_end(pool: AsyncConnectionPool, lot_id: str) -> datetime | None:
    rows = await fetch(
        pool,
        "SELECT max(window_start) + interval '5 minutes' AS t FROM lot_occupancy_5min WHERE lot_id = %s",  # noqa: E501
        (lot_id,),
    )
    latest: datetime | None = rows[0]["t"]
    return latest


async def history(
    pool: AsyncConnectionPool,
    lot_id: str,
    start: datetime,
    end: datetime,
    resolution: str,
    limit: int,
) -> list[dict[str, Any]]:
    if resolution == "1h":
        sql = """
            SELECT bucket AS window_start, avg_occupied, avg_occupancy_pct, min_occupied,
                   max_occupied, entries::int, exits::int, NULL::float8 AS unique_vehicles_est,
                   true AS closed
            FROM lot_occupancy_hourly
            WHERE lot_id = %s AND bucket >= %s AND bucket < %s ORDER BY bucket LIMIT %s"""
    else:
        sql = """
            SELECT window_start, avg_occupied, avg_occupancy_pct, min_occupied, max_occupied,
                   entries, exits, unique_vehicles_est, closed
            FROM lot_occupancy_5min
            WHERE lot_id = %s AND window_start >= %s AND window_start < %s
            ORDER BY window_start LIMIT %s"""
    return await fetch(pool, sql, (lot_id, start, end, limit))


async def alerts_page(
    pool: AsyncConnectionPool,
    kind: str | None,
    status: str | None,
    lot_id: str | None,
    limit: int,
    offset: int,
) -> tuple[int, list[dict[str, Any]]]:
    where, params = ["true"], []
    for column, value in (("kind", kind), ("status", status), ("lot_id", lot_id)):
        if value:
            where.append(f"{column} = %s")
            params.append(value)
    clause = " AND ".join(where)
    total = (await fetch(pool, f"SELECT count(*) AS n FROM alerts WHERE {clause}", tuple(params)))[
        0
    ]["n"]
    rows = await fetch(
        pool,
        f"""SELECT alert_id, kind, lot_id, sensor_id, status, raised_at, cleared_at, message,
                   occupancy_pct,
                   greatest(0, extract(epoch FROM
                       coalesce(cleared_at, now()) - raised_at)) AS duration_s
            FROM alerts WHERE {clause} ORDER BY raised_at DESC LIMIT %s OFFSET %s""",
        (*params, limit, offset),
    )
    return int(total), rows


def default_range(latest_end: datetime, hours: int = 24) -> tuple[datetime, datetime]:
    return latest_end - timedelta(hours=hours), latest_end
