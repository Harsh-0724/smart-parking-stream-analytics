"""All streaming state for one parking lot, and the per-event logic that updates it.

Pure logic with no Kafka dependency, so every rule can be unit-tested. One LotState lives
on exactly one processor instance (the one owning the lot's partition of `parking.raw`).

Per event: dedupe -> late check -> advance event time -> slot state -> window aggregate.
"""

from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from common.schemas import EventType, ParkingEvent, SlotStatus, WindowResult
from processor.windows import Window, window_start


@dataclass(frozen=True)
class Params:
    window_size_s: int
    allowed_lateness_s: float
    grace_s: float
    dedupe_ttl_s: float
    sensor_offline_after_s: float

    def __post_init__(self) -> None:
        if self.dedupe_ttl_s < self.allowed_lateness_s + self.grace_s:
            raise ValueError("DEDUPE_TTL_S must be >= ALLOWED_LATENESS_S + WINDOW_GRACE_S")


class Outcome(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    LATE = "late"


@dataclass(frozen=True)
class SensorSignal:
    sensor_id: str
    offline: bool
    silent_since: float  # last event_ts heard before the sensor went quiet (stable alert identity)
    now: float  # lot event time when the signal was raised


def utc(ts: float) -> datetime:
    return datetime.fromtimestamp(ts, UTC)


class LotState:
    def __init__(self, lot_id: str, params: Params, capacity: int = 0) -> None:
        self.lot_id = lot_id
        self.params = params
        self.capacity = capacity
        self.max_event_ts: float | None = None
        self.cursor: float | None = None  # occupancy has been integrated up to here
        self.count = 0  # occupied slots as of `cursor`
        self.slots: dict[str, tuple[float, bool]] = {}  # slot -> (last event_ts, occupied)
        self.windows: dict[int, Window] = {}
        self.closed: list[Window] = []  # closed, awaiting emission by the caller
        self.seen: OrderedDict[str, float] = OrderedDict()  # event_id -> event_ts
        self.sensors: dict[str, float] = {}  # sensor -> newest event_ts heard
        self.offline: dict[str, float] = {}  # sensor -> last_seen when it was flagged
        self.resume_offset = 0  # events below this partition offset are already in this state

    @property
    def watermark(self) -> float | None:
        if self.max_event_ts is None:
            return None
        return self.max_event_ts - self.params.allowed_lateness_s

    # ---- per-event pipeline -------------------------------------------------------------

    def process(self, e: ParkingEvent, ingest_ts: float) -> Outcome:
        ts = e.event_ts.timestamp()
        if e.event_id in self.seen:
            return Outcome.DUPLICATE
        self._remember(e.event_id, ts)

        occupancy = e.event_type is EventType.OCCUPANCY
        if occupancy and self._is_late(ts):
            return Outcome.LATE

        self._advance_time(ts)
        self.sensors[e.sensor_id] = max(self.sensors.get(e.sensor_id, ts), ts)
        if occupancy:
            self._apply_occupancy(e, ts, ingest_ts)
        self._close_ready_windows()
        return Outcome.ACCEPTED

    def _remember(self, event_id: str, ts: float) -> None:
        self.seen[event_id] = ts
        horizon = max(self.max_event_ts or ts, ts) - self.params.dedupe_ttl_s
        while self.seen and next(iter(self.seen.values())) < horizon:
            self.seen.popitem(last=False)

    def _is_late(self, ts: float) -> bool:
        """The event's window has already been closed (end + grace is behind the watermark)."""
        wm = self.watermark
        if wm is None:
            return False
        w = window_start(ts, self.params.window_size_s)
        return w + self.params.window_size_s + self.params.grace_s < wm

    def _advance_time(self, ts: float) -> None:
        """Move event time forward, integrating the current occupancy over the elapsed time."""
        if self.max_event_ts is None or ts > self.max_event_ts:
            self.max_event_ts = ts
        if self.cursor is None:
            self.cursor = ts
            return
        if ts <= self.cursor:
            return
        t = self.cursor
        size = self.params.window_size_s
        while t < ts:
            win = self._window(window_start(t, size))
            seg_end = min(ts, float(win.end))
            win.area += self.count * (seg_end - t)
            win.covered_s += seg_end - t
            win.observe_count(self.count)
            win.dirty = True
            t = seg_end
        self.cursor = ts

    def _window(self, start: int) -> Window:
        win = self.windows.get(start)
        if win is None:
            win = self.windows[start] = Window(start, self.params.window_size_s)
        return win

    def _apply_occupancy(self, e: ParkingEvent, ts: float, ingest_ts: float) -> None:
        win = self._window(window_start(ts, self.params.window_size_s))
        win.dirty = True
        win.latest_ingest_ts = max(win.latest_ingest_ts or ingest_ts, ingest_ts)
        occupied = e.status is SlotStatus.OCCUPIED
        if occupied and e.vehicle_token:
            win.vehicles.add(e.vehicle_token)

        prev = self.slots.get(e.slot_id)
        if prev is not None and ts <= prev[0]:
            return  # older than what we already know for this slot: never regress state
        self.slots[e.slot_id] = (ts, occupied)
        was = prev[1] if prev else None
        if was == occupied or (was is None and not occupied):
            return  # no change in the number of occupied slots

        if was is False and occupied:
            win.entries += 1
        elif was is True and not occupied:
            win.exits += 1
        self._shift_count(ts, 1 if occupied else -1)
        assert self.cursor is not None
        if ts >= self.cursor:
            win.observe_count(self.count)

    def _shift_count(self, ts: float, delta: int) -> None:
        """Apply a change that happened at `ts`. If `ts` is behind the cursor (a late event still
        inside its grace period) the time already integrated is corrected retroactively."""
        assert self.cursor is not None
        if ts < self.cursor:
            t = ts
            size = self.params.window_size_s
            while t < self.cursor:
                w = window_start(t, size)
                seg_end = min(self.cursor, float(w + size))
                win = self.windows.get(w)
                if win is not None:
                    win.area += delta * (seg_end - t)
                    win.dirty = True
                t = seg_end
        self.count += delta

    def _close_ready_windows(self) -> None:
        wm = self.watermark
        if wm is None:
            return
        for start in sorted(self.windows):
            win = self.windows[start]
            if win.end + self.params.grace_s < wm:
                self.closed.append(self.windows.pop(start))
            else:
                break  # later windows end later, so they cannot be ready either

    # ---- outputs ------------------------------------------------------------------------------

    def _result(self, win: Window, closed: bool, now: float) -> WindowResult:
        capacity = max(self.capacity, len(self.slots), 1)
        avg = win.area / win.covered_s if win.covered_s > 0 else float(self.count)
        return WindowResult(
            lot_id=self.lot_id,
            window_start=utc(win.start),
            window_end=utc(win.end),
            capacity=capacity,
            avg_occupied=round(avg, 3),
            avg_occupancy_pct=round(100 * avg / capacity, 2),
            min_occupied=win.min_occupied if win.min_occupied is not None else self.count,
            max_occupied=win.max_occupied if win.max_occupied is not None else self.count,
            entries=win.entries,
            exits=win.exits,
            unique_vehicles_est=round(win.vehicles.estimate(), 1),
            closed=closed,
            current_occupied=None if closed else self.count,
            emitted_at=utc(now),
            latest_ingest_ts=utc(win.latest_ingest_ts) if win.latest_ingest_ts else None,
        )

    def drain_closed(self, now: float) -> list[WindowResult]:
        out = [self._result(w, True, now) for w in self.closed]
        self.closed.clear()
        return out

    def dirty_open_results(self, now: float) -> list[WindowResult]:
        out = []
        for start in sorted(self.windows):
            win = self.windows[start]
            if win.dirty:
                out.append(self._result(win, False, now))
                win.dirty = False
        return out

    def sensor_signals(self) -> list[SensorSignal]:
        """Sensors that just went silent, or came back. Silence is measured in event time."""
        if self.max_event_ts is None:
            return []
        signals = []
        for sensor, last_seen in self.sensors.items():
            silent = self.max_event_ts - last_seen > self.params.sensor_offline_after_s
            if silent and sensor not in self.offline:
                self.offline[sensor] = last_seen
                signals.append(SensorSignal(sensor, True, last_seen, self.max_event_ts))
            elif not silent and sensor in self.offline:
                since = self.offline.pop(sensor)
                signals.append(SensorSignal(sensor, False, since, self.max_event_ts))
        return signals

    # ---- checkpointing ----------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "lot_id": self.lot_id,
            "capacity": self.capacity,
            "max_event_ts": self.max_event_ts,
            "cursor": self.cursor,
            "count": self.count,
            "slots": {s: [ts, occ] for s, (ts, occ) in self.slots.items()},
            "windows": [w.to_dict() for w in self.windows.values()],
            "seen": list(self.seen.items()),
            "sensors": self.sensors,
            "offline": self.offline,
            "resume_offset": self.resume_offset,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any], params: Params) -> "LotState":
        state = cls(d["lot_id"], params, d["capacity"])
        state.max_event_ts = d["max_event_ts"]
        state.cursor = d["cursor"]
        state.count = d["count"]
        state.slots = {s: (v[0], bool(v[1])) for s, v in d["slots"].items()}
        state.windows = {
            w["start"]: Window.from_dict(w, params.window_size_s) for w in d["windows"]
        }
        state.seen = OrderedDict((k, v) for k, v in d["seen"])
        state.sensors = d["sensors"]
        state.offline = dict(d["offline"])
        state.resume_offset = d["resume_offset"]
        return state
