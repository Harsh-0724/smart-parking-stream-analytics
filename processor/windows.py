"""Tumbling event-time window accumulator."""

from dataclasses import dataclass, field
from typing import Any

from processor.hll import UniqueCounter


def window_start(ts: float, size_s: int) -> int:
    """Start of the tumbling window containing `ts`; windows are [start, start + size)."""
    return int(ts // size_s) * size_s


@dataclass
class Window:
    start: int
    size_s: int
    area: float = 0.0  # occupied-slot-seconds accumulated inside this window
    covered_s: float = 0.0  # seconds of this window for which occupancy was known
    min_occupied: int | None = None
    max_occupied: int | None = None
    entries: int = 0
    exits: int = 0
    vehicles: UniqueCounter = field(default_factory=UniqueCounter)
    latest_ingest_ts: float | None = None
    dirty: bool = True  # changed since it was last emitted

    @property
    def end(self) -> int:
        return self.start + self.size_s

    def observe_count(self, count: int) -> None:
        self.min_occupied = count if self.min_occupied is None else min(self.min_occupied, count)
        self.max_occupied = count if self.max_occupied is None else max(self.max_occupied, count)

    def to_dict(self) -> dict[str, Any]:
        return {
            "start": self.start,
            "area": self.area,
            "covered_s": self.covered_s,
            "min": self.min_occupied,
            "max": self.max_occupied,
            "entries": self.entries,
            "exits": self.exits,
            "hll": self.vehicles.to_b64(),
            "latest_ingest_ts": self.latest_ingest_ts,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any], size_s: int) -> "Window":
        return cls(
            start=d["start"],
            size_s=size_s,
            area=d["area"],
            covered_s=d["covered_s"],
            min_occupied=d["min"],
            max_occupied=d["max"],
            entries=d["entries"],
            exits=d["exits"],
            vehicles=UniqueCounter.from_b64(d["hll"]),
            latest_ingest_ts=d["latest_ingest_ts"],
        )
