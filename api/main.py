"""FastAPI gateway: REST + WebSocket. Run: uvicorn api.main:app --port 8080"""

import asyncio
import contextlib
import logging
import os
import time
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app
from psycopg_pool import AsyncConnectionPool

from api import db
from api.live import Batch, Hub, KafkaFeed, flush_loop
from api.models import (
    AlertPage,
    AlertRow,
    Health,
    History,
    HistoryPoint,
    LotDetail,
    LotSummary,
    Overview,
    PipelineStats,
    SlotSnapshot,
    SlotState,
)
from api.pipeline import PipelineCollector
from common.config import Settings

log = logging.getLogger("api")

PIPELINE_INTERVAL_S = 2.0
OVERVIEW_TTL_S = 2.0
HISTORY_TTL_S = 10.0
MAX_HISTORY_POINTS = 2000


class TtlCache:
    """Tiny in-process cache: dashboards poll often, the underlying data changes every ~5 s."""

    def __init__(self) -> None:
        self._items: dict[str, tuple[float, Any]] = {}

    def get(self, key: str, ttl: float) -> Any | None:
        hit = self._items.get(key)
        return hit[1] if hit and time.monotonic() - hit[0] < ttl else None

    def put(self, key: str, value: Any) -> Any:
        self._items[key] = (time.monotonic(), value)
        return value


def _iso_now() -> datetime:
    return datetime.now(UTC)


@contextlib.asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings.from_env()
    loop = asyncio.get_running_loop()
    hub = Hub()
    pool = AsyncConnectionPool(settings.postgres_dsn, min_size=1, max_size=8, open=False)
    await pool.open()
    collector = PipelineCollector(
        settings.kafka_bootstrap, os.environ.get("PROMETHEUS_URL", "http://prometheus:9090")
    )
    feed = KafkaFeed(settings.kafka_bootstrap, loop, hub.load_snapshot, hub.handle)
    feed.start()

    async def pipeline_loop() -> None:
        while True:
            stats = await asyncio.to_thread(collector.collect)
            app.state.pipeline = stats
            hub.broadcast({"type": "pipeline", "data": stats.model_dump(mode="json")})
            await asyncio.sleep(PIPELINE_INTERVAL_S)

    app.state.hub, app.state.pool, app.state.cache = hub, pool, TtlCache()
    app.state.pipeline = None
    tasks = [asyncio.create_task(pipeline_loop()), asyncio.create_task(flush_loop(hub))]
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        feed.stop()
        await pool.close()


app = FastAPI(
    title="Smart Parking Analytics API",
    version="1.0.0",
    description="Live occupancy, history, alerts and pipeline introspection for the parking stream.",  # noqa: E501
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        o for o in os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(",") if o
    ],
    allow_methods=["GET"],
    allow_headers=["*"],
)
app.mount("/metrics", make_asgi_app())


def _pool(app: FastAPI) -> AsyncConnectionPool:
    pool: AsyncConnectionPool = app.state.pool
    return pool


def _pct(occupied: int | None, capacity: int) -> float | None:
    return None if occupied is None or capacity == 0 else round(100 * occupied / capacity, 1)


def _current(hub: Hub, lot_id: str, db_value: int | None) -> int | None:
    live = hub.live_current.get(lot_id)
    return live[0] if live else db_value


@app.get("/api/health", response_model=Health, tags=["ops"])
async def health() -> Health:
    hub: Hub = app.state.hub
    try:
        await db.fetch(_pool(app), "SELECT 1")
        db_ok = True
    except Exception:
        db_ok = False
    return Health(
        status="ok" if db_ok and hub.feed_ready else "degraded",
        database=db_ok,
        kafka_feed=hub.feed_ready,
        websocket_clients=len(hub.clients),
    )


@app.get("/api/overview", response_model=Overview, tags=["lots"])
async def overview() -> Overview:
    cache: TtlCache = app.state.cache
    hub: Hub = app.state.hub
    cached = cache.get("overview", OVERVIEW_TTL_S)
    rows = cached or cache.put(
        "overview",
        (
            await db.lots_with_latest(_pool(app)),
            await db.sparklines(_pool(app)),
            await db.active_alert_count(_pool(app)),
        ),
    )
    lot_rows, sparks, active = rows
    lots = []
    for r in lot_rows:
        occupied = _current(hub, r["lot_id"], r["occupied"])
        lots.append(
            LotSummary(
                lot_id=r["lot_id"], name=r["name"], personality=r["personality"], capacity=r["capacity"],  # noqa: E501
                occupied=occupied, occupancy_pct=_pct(occupied, r["capacity"]), as_of=r["as_of"],
                spark=sparks.get(r["lot_id"], []),
            )
        )  # fmt: skip
    capacity = sum(x.capacity for x in lots)
    occupied_total = sum(x.occupied or 0 for x in lots)
    return Overview(
        total_capacity=capacity,
        total_occupied=occupied_total,
        occupancy_pct=round(100 * occupied_total / capacity, 1) if capacity else 0.0,
        lots=sorted(lots, key=lambda x: -(x.occupancy_pct or 0)),
        active_alerts=active,
        generated_at=_iso_now(),
    )


@app.get("/api/lots", response_model=list[LotSummary], tags=["lots"])
async def list_lots() -> list[LotSummary]:
    return (await overview()).lots


@app.get("/api/lots/{lot_id}", response_model=LotDetail, tags=["lots"])
async def lot_detail(lot_id: str) -> LotDetail:
    hub: Hub = app.state.hub
    rows = [r for r in await db.lots_with_latest(_pool(app)) if r["lot_id"] == lot_id]
    if not rows:
        raise HTTPException(404, f"unknown lot {lot_id}")
    r = rows[0]
    stats = await db.lot_stats(_pool(app), lot_id)
    occupied = _current(hub, lot_id, r["occupied"])
    entries = stats["entries"]
    seconds = stats["occupied_seconds"]
    return LotDetail(
        lot_id=lot_id, name=r["name"], personality=r["personality"], capacity=r["capacity"],
        columns=r["columns"], latitude=r["latitude"], longitude=r["longitude"], slot_ids=r["slot_ids"],  # noqa: E501
        occupied=occupied, occupancy_pct=_pct(occupied, r["capacity"]),
        entries_24h=entries, exits_24h=stats["exits"],
        unique_vehicles_est=stats["unique_now"],
        dwell_minutes_est=round(float(seconds) / entries / 60, 1) if entries and seconds else None,
        full_since=await db.full_since(_pool(app), lot_id),
    )  # fmt: skip


@app.get("/api/lots/{lot_id}/slots", response_model=SlotSnapshot, tags=["lots"])
async def slot_snapshot(lot_id: str) -> SlotSnapshot:
    hub: Hub = app.state.hub
    known = hub.store.lot(lot_id)
    rows = [r for r in await db.lots_with_latest(_pool(app)) if r["lot_id"] == lot_id]
    if not rows:
        raise HTTPException(404, f"unknown lot {lot_id}")
    return SlotSnapshot(
        lot_id=lot_id,
        slots=[
            SlotState(
                slot_id=s,
                occupied=known[s][1] if s in known else None,
                ts=datetime.fromtimestamp(known[s][0], UTC) if s in known else None,
            )
            for s in rows[0]["slot_ids"]
        ],
    )


@app.get("/api/lots/{lot_id}/history", response_model=History, tags=["lots"])
async def lot_history(
    lot_id: str,
    resolution: Literal["5m", "1h"] = "5m",
    start: datetime | None = None,
    end: datetime | None = None,
    limit: Annotated[int, Query(ge=1, le=MAX_HISTORY_POINTS)] = 500,
) -> History:
    """Default range: the 24 h ending at the lot's newest window (not wall-clock now)."""
    cache: TtlCache = app.state.cache
    if end is None:
        latest = await db.latest_window_end(_pool(app), lot_id)
        if latest is None:
            raise HTTPException(404, f"no data for lot {lot_id}")
        end = latest
    if start is None:
        start = end - timedelta(hours=24 if resolution == "5m" else 24 * 7)
    key = f"hist:{lot_id}:{resolution}:{start.isoformat()}:{end.isoformat()}:{limit}"
    result = cache.get(key, HISTORY_TTL_S)
    if result is None:
        rows = await db.history(_pool(app), lot_id, start, end, resolution, limit)
        result = cache.put(key, History(
            lot_id=lot_id, resolution=resolution, range_start=start, range_end=end,
            points=[HistoryPoint(**r) for r in rows],
        ))  # fmt: skip
    return result  # type: ignore[no-any-return]


@app.get("/api/alerts", response_model=AlertPage, tags=["alerts"])
async def alerts(
    kind: Literal["FULL_LOT", "SENSOR_OFFLINE"] | None = None,
    status: Literal["active", "cleared"] | None = None,
    lot_id: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> AlertPage:
    total, rows = await db.alerts_page(_pool(app), kind, status, lot_id, limit, offset)
    return AlertPage(total=total, limit=limit, offset=offset, items=[AlertRow(**r) for r in rows])


@app.get("/api/pipeline", response_model=PipelineStats, tags=["pipeline"])
async def pipeline() -> PipelineStats:
    stats: PipelineStats | None = app.state.pipeline
    if stats is None:
        raise HTTPException(503, "pipeline stats not collected yet")
    return stats


@app.websocket("/ws")
async def websocket(ws: WebSocket) -> None:
    """Server pushes `occupancy`, `alert`, `pipeline` to everyone, and `snapshot` then `slots`
    deltas for the lots a client subscribes to: send {"type":"subscribe","lots":["LOT-01"]}."""
    hub: Hub = app.state.hub
    await ws.accept()
    client = hub.add(ws)
    client.push({"type": "hello", "server_time": _iso_now().isoformat()})
    if app.state.pipeline is not None:
        client.push({"type": "pipeline", "data": app.state.pipeline.model_dump(mode="json")})
    try:
        while True:
            message = await ws.receive_json()
            if message.get("type") == "subscribe" and isinstance(message.get("lots"), list):
                hub.subscribe(client, [str(x) for x in message["lots"]][:5])
    except WebSocketDisconnect:
        pass
    finally:
        hub.remove(client)


__all__ = ["Batch", "app"]
