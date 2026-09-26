"""Event sources: synthetic (demand-curve driven) and replay (Birmingham dataset)."""

import csv
import random
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Protocol
from zoneinfo import ZoneInfo

from common import topics
from common.partitioning import balanced_lot_ids
from common.schemas import LotMetadata
from simulator.emission import Emission, vehicle_token
from simulator.heartbeats import HeartbeatSchedule
from simulator.indexed_set import IndexedSet
from simulator.lotsim import LotSim

LOCAL_TZ = ZoneInfo("Asia/Kolkata")
PERSONALITIES = ("office", "mall", "station")
LOT_NAMES = {"office": "Tech Park", "mall": "Galleria Mall", "station": "Metro Station"}
SLOTS_PER_ROW = 20
SYNC_SPREAD_S = 5.0
BASE_LAT, BASE_LON = 12.9237, 77.4988  # RVCE campus, Bengaluru


class Source(Protocol):
    lots: list[LotMetadata]

    def advance(self, until: float, burst: float = 1.0) -> list[Emission]: ...

    @property
    def exhausted(self) -> bool: ...


def slot_ids(count: int) -> list[str]:
    return [
        f"{chr(ord('A') + i // SLOTS_PER_ROW)}-{i % SLOTS_PER_ROW + 1:03d}" for i in range(count)
    ]


class SyntheticSource:
    def __init__(
        self, n_lots: int, slots_per_lot: int, start_ts: float, seed: int, salt: str
    ) -> None:
        self.lots: list[LotMetadata] = []
        self._sims: list[LotSim] = []
        lot_ids = balanced_lot_ids(n_lots, topics.RAW_PARTITIONS)
        for i, lot_id in enumerate(lot_ids):
            personality = PERSONALITIES[i % len(PERSONALITIES)]
            meta = LotMetadata(
                lot_id=lot_id,
                name=f"{LOT_NAMES[personality]} {i // len(PERSONALITIES) + 1}",
                personality=personality,  # type: ignore[arg-type]
                capacity=slots_per_lot,
                slot_ids=slot_ids(slots_per_lot),
                columns=SLOTS_PER_ROW,
                latitude=BASE_LAT + 0.004 * i,
                longitude=BASE_LON + 0.003 * i,
            )
            self.lots.append(meta)
            self._sims.append(
                LotSim(meta, random.Random(seed * 1000 + i), salt, start_ts, LOCAL_TZ)
            )

    @property
    def exhausted(self) -> bool:
        return False

    def advance(self, until: float, burst: float = 1.0) -> list[Emission]:
        out: list[Emission] = []
        for sim in self._sims:
            out.extend(sim.advance(until, burst))
        return out


class ReplaySource:
    """Turns occupancy snapshots into per-slot events.

    Each snapshot pair (previous, current) yields |delta| arrivals or departures, placed at
    uniformly jittered times inside the interval. Capacity is scaled by `scale` to keep the
    sensor count manageable; timestamps are shifted so the first snapshot lands at `start_ts`.
    """

    def __init__(
        self,
        dataset: Path,
        start_ts: float,
        seed: int,
        salt: str,
        scale: float,
        days: float,
        max_lots: int,
    ) -> None:
        rng = random.Random(seed)
        rows = _load_rows(dataset)
        first = min(ts for series in rows.values() for ts, _, _ in series)
        horizon = first + days * 86400
        codes = sorted(rows)[:max_lots]

        self.lots = []
        events: list[Emission] = []
        heartbeats: list[HeartbeatSchedule] = []
        for i, code in enumerate(codes):
            series = [r for r in rows[code] if r[0] <= horizon]
            if not series:
                continue
            capacity = max(10, round(series[0][1] * scale))
            meta = LotMetadata(
                lot_id=code,
                name=code,
                personality="mall",
                capacity=capacity,
                slot_ids=slot_ids(capacity),
                columns=SLOTS_PER_ROW,
                latitude=52.48 + 0.002 * i,
                longitude=-1.90 + 0.002 * i,
            )
            self.lots.append(meta)
            events.extend(_snapshots_to_events(meta, series, first, start_ts, scale, salt, rng))
            heartbeats.append(HeartbeatSchedule(code, meta.slot_ids, start_ts, rng))
        events.sort(key=lambda e: e.ts)
        self._events = events
        self._cursor = 0
        self._heartbeats = heartbeats

    @property
    def exhausted(self) -> bool:
        return self._cursor >= len(self._events)

    def advance(self, until: float, burst: float = 1.0) -> list[Emission]:
        out: list[Emission] = []
        while self._cursor < len(self._events) and self._events[self._cursor].ts <= until:
            out.append(self._events[self._cursor])
            self._cursor += 1
        if not self.exhausted:
            for schedule in self._heartbeats:
                out.extend(schedule.due(until, burst))
        return out


def _load_rows(dataset: Path) -> dict[str, list[tuple[float, int, int]]]:
    """lot code -> [(epoch_seconds, capacity, occupancy)] sorted by time."""
    rows: dict[str, list[tuple[float, int, int]]] = defaultdict(list)
    with dataset.open() as f:
        for r in csv.DictReader(f):
            ts = datetime.strptime(r["LastUpdated"], "%Y-%m-%d %H:%M:%S").timestamp()
            rows[r["SystemCodeNumber"]].append((ts, int(r["Capacity"]), int(r["Occupancy"])))
    for series in rows.values():
        series.sort()
    return rows


def _snapshots_to_events(
    meta: LotMetadata,
    series: list[tuple[float, int, int]],
    first_ts: float,
    start_ts: float,
    scale: float,
    salt: str,
    rng: random.Random,
) -> list[Emission]:
    free = IndexedSet(meta.slot_ids)
    occupied = IndexedSet()
    events: list[Emission] = []
    prev_ts = series[0][0]
    for i, (ts, _, occ) in enumerate(series):
        want = min(meta.capacity, max(0, round(occ * scale)))
        lo, hi = (prev_ts, ts) if i else (ts - SYNC_SPREAD_S, ts)
        while len(occupied) < want:
            slot = free.pop_random(rng)
            occupied.add(slot)
            token = vehicle_token(salt, meta.lot_id, rng.randrange(meta.capacity * 8))
            when = start_ts + rng.uniform(lo, hi) - first_ts
            events.append(Emission(when, meta.lot_id, slot, False, True, token, sync=i == 0))
        while len(occupied) > want:
            slot = occupied.pop_random(rng)
            free.add(slot)
            when = start_ts + rng.uniform(lo, hi) - first_ts
            events.append(Emission(when, meta.lot_id, slot, False, False))
        prev_ts = ts
        if i == 0:
            for slot in free.items():
                when = start_ts + rng.uniform(lo, hi) - first_ts
                events.append(Emission(when, meta.lot_id, slot, False, False, sync=True))
    return events
