import json
import random

from common.schemas import ParkingEvent
from simulator.emission import Emission, encode
from simulator.faults import FaultConfig, FaultInjector
from simulator.indexed_set import IndexedSet
from simulator.source import SyntheticSource
from simulator.truth import TruthTracker

START = 1_767_000_000.0  # a Monday-ish instant; only relative behaviour matters


def _events(seconds: int) -> list[Emission]:
    src = SyntheticSource(3, 30, START, seed=1, salt="s")
    return sorted(src.advance(START + seconds), key=lambda e: e.ts)


def test_generated_events_validate_against_schema() -> None:
    for e in _events(600)[:500]:
        ParkingEvent.model_validate_json(encode(e, START))


def test_per_slot_events_alternate_and_never_regress() -> None:
    state: dict[tuple[str, str], bool] = {}
    for e in _events(3600):
        if e.heartbeat:
            continue
        prev = state.get((e.lot_id, e.slot_id))
        assert prev != e.occupied, "consecutive identical states for a slot"
        state[(e.lot_id, e.slot_id)] = e.occupied


def test_same_seed_is_deterministic() -> None:
    assert _events(300) == _events(300)


def test_indexed_set_remove_keeps_positions() -> None:
    s = IndexedSet(["a", "b", "c", "d"])
    s.remove("b")
    s.remove("a")
    assert len(s) == 2 and "c" in s and "d" in s and "a" not in s
    assert s.pop_random(random.Random(0)) in {"c", "d"}


def _inject(cfg: FaultConfig, n: int = 2000) -> tuple[FaultInjector, list[bytes]]:
    inj = FaultInjector(cfg, random.Random(3), START)
    out: list[bytes] = []
    for i in range(n):
        e = Emission(START + i, "LOT-01", "A-001", False, True, "tok")
        out.extend(m.value for m in inj.process(e, START))
    return inj, out


def test_duplicates_repeat_identical_bytes() -> None:
    inj, out = _inject(FaultConfig(dup_pct=10))
    ids = [json.loads(v)["event_id"] for v in out]
    assert len(ids) - len(set(ids)) == inj.counters.duplicates > 0


def test_malformed_messages_fail_validation() -> None:
    inj, out = _inject(FaultConfig(malformed_pct=20))
    bad = 0
    for v in out:
        try:
            ParkingEvent.model_validate_json(v)
        except ValueError:
            bad += 1
    assert bad == inj.counters.malformed > 0


def test_late_events_are_held_then_released_with_original_event_ts() -> None:
    inj = FaultInjector(
        FaultConfig(late_pct=100, late_min_s=30, late_max_s=30), random.Random(1), START
    )
    e = Emission(START + 10, "LOT-01", "A-001", False, True, "tok")
    assert inj.process(e, START) == []
    assert inj.release_due(START + 39, START) == []
    (m,) = inj.release_due(START + 41, START)
    assert ParkingEvent.model_validate_json(m.value).event_ts.timestamp() == e.ts


def test_sensor_dropout_silences_only_that_sensor_after_delay() -> None:
    cfg = FaultConfig(dropout_sensors=frozenset({(None, "S-A-001")}), dropout_after_s=20)
    inj = FaultInjector(cfg, random.Random(1), START)
    hb = lambda slot, dt: Emission(START + dt, "LOT-01", slot, True)  # noqa: E731
    assert inj.process(hb("A-001", 10), START)
    assert inj.process(hb("A-001", 25), START) == []
    assert inj.process(hb("A-002", 25), START)


def test_truth_tracker_time_weighted_average() -> None:
    t = TruthTracker(300)
    base = 300 * 1000.0
    t.observe(
        [
            Emission(base + 0, "L", "A", False, True, "x"),
            Emission(base + 150, "L", "A", False, False),
        ]
    )
    assert t._area[("L", 1000)] == 150.0
