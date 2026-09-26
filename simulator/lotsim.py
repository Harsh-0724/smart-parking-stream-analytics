"""Synthetic lot: arrivals follow the demand curve, dwell times are log-normal."""

import heapq
import math
import random
from datetime import datetime
from zoneinfo import ZoneInfo

from common.schemas import LotMetadata
from simulator.demand import DWELL, mean_dwell_s, target_occupancy
from simulator.emission import Emission, vehicle_token
from simulator.heartbeats import HeartbeatSchedule
from simulator.indexed_set import IndexedSet

STEP_S = 1.0
FILL_TAU_S = 120.0  # how fast arrivals close a gap to the target occupancy
DRAIN_TAU_S = 300.0  # how fast forced departures remove excess over the target
TARGET_TOLERANCE = 0.02
MIN_DWELL_S = 60.0
VEHICLE_POOL_FACTOR = 8  # distinct vehicles per slot; small enough that vehicles repeat
SYNC_SPREAD_S = 5.0


def _poisson(rng: random.Random, lam: float) -> int:
    limit, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rng.random()
        if p <= limit:
            return k
        k += 1


class LotSim:
    def __init__(
        self,
        meta: LotMetadata,
        rng: random.Random,
        salt: str,
        start_ts: float,
        tz: ZoneInfo,
    ) -> None:
        self.meta = meta
        self._rng = rng
        self._salt = salt
        self._tz = tz
        self._t = start_ts
        self._free = IndexedSet(meta.slot_ids)
        self._occupied = IndexedSet()
        self._visit: dict[str, int] = {}  # slot -> current visit id, to void stale departures
        self._departures: list[tuple[float, str, int]] = []
        self._tokens: dict[int, str] = {}
        self._slot_token: dict[str, str] = {}
        self._visit_seq = 0
        self._target_cache: tuple[int, float] = (-1, 0.0)
        self._mean_dwell = mean_dwell_s(meta.personality)
        self._heartbeats = HeartbeatSchedule(meta.lot_id, meta.slot_ids, start_ts, rng)
        self._pending: list[Emission] = self._prefill(start_ts)

    def _target(self, ts: float) -> float:
        minute = int(ts // 60)
        if self._target_cache[0] != minute:
            local = datetime.fromtimestamp(ts, self._tz)
            value = target_occupancy(
                self.meta.personality, local.weekday(), local.hour + local.minute / 60
            )
            self._target_cache = (minute, value)
        return self._target_cache[1]

    def _token(self) -> str:
        vehicle = self._rng.randrange(self.meta.capacity * VEHICLE_POOL_FACTOR)
        return vehicle_token(self._salt, self.meta.lot_id, vehicle)

    def _dwell(self, burst: float) -> float:
        median, sigma = DWELL[self.meta.personality]
        dwell = self._rng.lognormvariate(math.log(median), sigma) / burst
        return max(MIN_DWELL_S, dwell)

    def _park(self, ts: float, dwell: float, sync: bool) -> Emission:
        slot = self._free.pop_random(self._rng)
        self._occupied.add(slot)
        self._visit_seq += 1
        self._visit[slot] = self._visit_seq
        token = self._token()
        heapq.heappush(self._departures, (ts + dwell, slot, self._visit_seq))
        self._slot_token[slot] = token
        return Emission(ts, self.meta.lot_id, slot, False, True, token, sync)

    def _leave(self, ts: float, slot: str) -> Emission:
        self._occupied.remove(slot)
        self._free.add(slot)
        del self._visit[slot]
        return Emission(ts, self.meta.lot_id, slot, False, False)

    def _prefill(self, start_ts: float) -> list[Emission]:
        count = round(self._target(start_ts) * self.meta.capacity)
        out = []
        for _ in range(count):
            residual = self._dwell(1.0) * self._rng.random()
            ts = start_ts - self._rng.random() * SYNC_SPREAD_S
            out.append(self._park(ts, max(residual, MIN_DWELL_S), sync=True))
        return out

    def advance(self, until: float, burst: float = 1.0) -> list[Emission]:
        """Simulate whole steps up to `until`; return events in no particular order."""
        out, self._pending = self._pending, []
        while self._t + STEP_S <= until:
            self._t += STEP_S
            out.extend(self._step(self._t, burst))
        out.extend(self._heartbeats.due(until, burst))
        return out

    def _step(self, t: float, burst: float) -> list[Emission]:
        out: list[Emission] = []
        while self._departures and self._departures[0][0] <= t:
            dep_ts, slot, visit = heapq.heappop(self._departures)
            if self._visit.get(slot) == visit:
                out.append(self._leave(dep_ts, slot))

        cap = self.meta.capacity
        target = self._target(t)
        frac = len(self._occupied) / cap
        if frac > target + TARGET_TOLERANCE:
            rate = cap * (frac - target) / DRAIN_TAU_S * STEP_S
            for _ in range(min(_poisson(self._rng, rate), len(self._occupied))):
                out.append(self._leave(t, self._occupied.choice(self._rng)))

        frac = len(self._occupied) / cap
        if frac < target + TARGET_TOLERANCE:
            churn = burst * cap * target / self._mean_dwell
            fill = cap * max(0.0, target - frac) / FILL_TAU_S
            for _ in range(_poisson(self._rng, (churn + fill) * STEP_S)):
                if not self._free:
                    break
                ts = t - self._rng.random() * STEP_S
                out.append(self._park(ts, self._dwell(burst), sync=False))
        return out
