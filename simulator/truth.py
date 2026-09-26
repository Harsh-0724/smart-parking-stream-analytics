"""Ground truth per (lot, window) computed from what the simulator emitted, by event time.

Independent of the processor's implementation, so agreement between the two is evidence of
correctness. Ignores lateness, drops and duplicates: it is the ideal answer."""

import json
from collections import defaultdict
from pathlib import Path

from simulator.emission import Emission


class TruthTracker:
    def __init__(self, window_s: int) -> None:
        self._w = window_s
        self._count: dict[str, int] = defaultdict(int)
        self._last_ts: dict[str, float] = {}
        self._state: dict[tuple[str, str], bool] = {}
        self._area: dict[tuple[str, int], float] = defaultdict(float)
        self._entries: dict[tuple[str, int], int] = defaultdict(int)
        self._exits: dict[tuple[str, int], int] = defaultdict(int)
        self._tokens: dict[tuple[str, int], set[str]] = defaultdict(set)

    def _integrate(self, lot: str, until: float) -> None:
        last = self._last_ts.get(lot)
        if last is not None and until > last:
            t = last
            while t < until:
                w = int(t // self._w)
                seg_end = min(until, (w + 1) * self._w)
                self._area[(lot, w)] += self._count[lot] * (seg_end - t)
                t = seg_end
        self._last_ts[lot] = until

    def observe(self, events: list[Emission]) -> None:
        """Feed occupancy emissions in event-time order."""
        for e in events:
            if e.heartbeat:
                continue
            lot, w = e.lot_id, int(e.ts // self._w)
            self._integrate(lot, e.ts)
            prev = self._state.get((lot, e.slot_id))
            if e.occupied:
                self._tokens[(lot, w)].add(e.vehicle_token or "")
                if prev is not True:
                    self._count[lot] += 1
                    if not e.sync:
                        self._entries[(lot, w)] += 1
            elif prev is True:
                self._count[lot] -= 1
                self._exits[(lot, w)] += 1
            self._state[(lot, e.slot_id)] = e.occupied

    def write(self, path: Path, end_ts: float) -> None:
        for lot in list(self._last_ts):
            self._integrate(lot, end_ts)
        rows = {
            f"{lot}|{w * self._w}": {
                "avg_occupied": self._area[(lot, w)] / self._w,
                "entries": self._entries[(lot, w)],
                "exits": self._exits[(lot, w)],
                "unique_vehicles": len(self._tokens[(lot, w)]),
            }
            for (lot, w) in self._area
        }
        path.write_text(json.dumps(rows, indent=1))
