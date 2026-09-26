"""Fault injection between the event source and the producer."""

import heapq
import random
from dataclasses import dataclass, field

from simulator.emission import Emission, encode

MALFORMED_KINDS = ("truncated_json", "missing_field", "bad_status", "naive_timestamp", "no_token")


@dataclass
class FaultConfig:
    late_pct: float = 0.0
    late_min_s: float = 15.0
    late_max_s: float = 600.0
    dup_pct: float = 0.0
    malformed_pct: float = 0.0
    dropout_sensors: frozenset[tuple[str | None, str]] = frozenset()  # (lot_id or None, sensor_id)
    dropout_after_s: float = 20.0


@dataclass
class Counters:
    total: int = 0
    late: int = 0
    duplicates: int = 0
    malformed: int = 0
    dropped: int = 0


@dataclass(order=True)
class _Held:
    release_ts: float
    seq: int
    emission: Emission = field(compare=False)


@dataclass(frozen=True)
class Message:
    key: bytes
    value: bytes


class FaultInjector:
    def __init__(self, cfg: FaultConfig, rng: random.Random, start_ts: float) -> None:
        self._cfg = cfg
        self._rng = rng
        self._dropout_from = start_ts + cfg.dropout_after_s
        self._held: list[_Held] = []
        self._seq = 0
        self.counters = Counters()

    def _is_dropped(self, e: Emission) -> bool:
        if e.ts < self._dropout_from:
            return False
        sensors = self._cfg.dropout_sensors
        return (e.lot_id, e.sensor_id) in sensors or (None, e.sensor_id) in sensors

    def process(self, e: Emission, wall_now: float) -> list[Message]:
        """Sim-time ordering is the caller's job; `wall_now` stamps ingest_ts."""
        if self._is_dropped(e):
            self.counters.dropped += 1
            return []
        cfg = self._cfg
        out: list[Message] = []
        if not e.heartbeat and self._rng.random() * 100 < cfg.late_pct:
            delay = self._rng.uniform(cfg.late_min_s, cfg.late_max_s)
            self._seq += 1
            heapq.heappush(self._held, _Held(e.ts + delay, self._seq, e))
            self.counters.late += 1
        else:
            out.append(self._message(e, wall_now))
            if self._rng.random() * 100 < cfg.dup_pct:
                out.append(out[0])
                self.counters.duplicates += 1
        if self._rng.random() * 100 < cfg.malformed_pct:
            out.append(self._malformed(e, wall_now))
            self.counters.malformed += 1
        return out

    def release_due(self, sim_now: float, wall_now: float) -> list[Message]:
        out = []
        while self._held and self._held[0].release_ts <= sim_now:
            out.append(self._message(heapq.heappop(self._held).emission, wall_now))
        return out

    def flush_held(self, wall_now: float) -> list[Message]:
        out = [self._message(h.emission, wall_now) for h in sorted(self._held)]
        self._held.clear()
        return out

    def _message(self, e: Emission, wall_now: float) -> Message:
        self.counters.total += 1
        return Message(e.lot_id.encode(), encode(e, wall_now))

    def _malformed(self, e: Emission, wall_now: float) -> Message:
        good = encode(Emission(e.ts, e.lot_id, e.slot_id, False, True, "tok"), wall_now)
        kind = self._rng.choice(MALFORMED_KINDS)
        if kind == "truncated_json":
            bad = good[: len(good) // 2]
        elif kind == "missing_field":
            bad = good.replace(b'"lot_id"', b'"lot"')
        elif kind == "bad_status":
            bad = good.replace(b'"OCCUPIED"', b'"BROKEN"')
        elif kind == "naive_timestamp":
            bad = good.replace(b"+00:00", b"")
        else:
            bad = good.replace(b'"vehicle_token": "tok"', b'"vehicle_token": null')
        return Message(e.lot_id.encode(), bad)
