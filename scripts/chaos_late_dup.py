"""Late and duplicate data: --late-pct 10 --dup-pct 5.

Run A (delays 15-150 s, inside the 180 s lateness+grace): duplicates dropped, nothing routed to
parking.late, late events land in the right windows (results match ground truth).
Run B (delays 15-600 s): events beyond the grace period go to parking.late, each one verifiably
past its window's close time."""

import os
import sys
import time

from common import topics
from common.kafka import read_to_end
from common.schemas import LateEvent
from scripts import chaoslib as c

MISMATCH_TOLERANCE = 0.02  # fraction of windows; see the slot-state rule in docs/DECISIONS.md D13


def run_a(r: c.Report) -> None:
    c.reset_environment(processors=2)
    sim = c.start_sim("--late-pct", "10", "--dup-pct", "5", "--late-max-s", "150", duration=100)
    summary = sim.wait()
    c.wait_drained()
    dups = int(c.processor_metric("parking_events_duplicate_total"))
    late = int(c.processor_metric("parking_events_late_total"))
    r.check("A: every duplicate dropped", dups == summary["duplicates"],
            f"sent {summary['duplicates']}, dropped {dups}")  # fmt: skip
    r.check(
        "A: nothing late enough for parking.late",
        late == 0,
        f"late={late}, {summary['late']} events were delayed",
    )
    time.sleep(10)
    v = c.verify_db(sim, summary)
    bad = len(v["mismatches"])
    r.check("A: in-grace late events land in the right windows (matches ground truth)",
            not v["missing"] and v["compared"] > 0 and bad <= MISMATCH_TOLERANCE * v["compared"],
            f"{v['compared']} windows compared, {bad} mismatches, {len(v['missing'])} missing")  # fmt: skip
    if v["mismatches"]:
        r.metric("A: mismatching windows", "; ".join(v["mismatches"][:5]))


def run_b(r: c.Report) -> None:
    c.reset_environment(processors=2)
    sim = c.start_sim("--late-pct", "10", "--dup-pct", "5", "--late-max-s", "600", duration=100)
    summary = sim.wait()
    c.wait_drained()
    dups = int(c.processor_metric("parking_events_duplicate_total"))
    late = int(c.processor_metric("parking_events_late_total"))
    r.check("B: every duplicate dropped", dups == summary["duplicates"],
            f"sent {summary['duplicates']}, dropped {dups}")  # fmt: skip
    r.check("B: beyond-grace events routed to parking.late", 0 < late < summary["late"],
            f"{late} routed of {summary['late']} delayed (the rest were inside the grace period)")  # fmt: skip
    records = [
        LateEvent.model_validate_json(v)
        for _, v, _ in read_to_end(os.environ["KAFKA_BOOTSTRAP_HOST"], topics.LATE, 0)
        if v
    ]
    closed_before = all(
        rec.window_start.timestamp() + 300 + 120 < rec.watermark.timestamp() for rec in records
    )
    r.check("B: every parking.late record is past its window's close time", closed_before and len(records) == late,
            f"{len(records)} records")  # fmt: skip
    r.metric(
        "B: worst lateness recorded",
        f"{max((x.late_by_s for x in records), default=0):.0f} s behind the watermark",
    )


def main() -> int:
    c.load_env()
    r = c.Report(
        "Late and duplicate data",
        "--late-pct 10 --dup-pct 5, in-grace (A) and beyond-grace (B) delays",
    )
    run_a(r)
    run_b(r)
    return r.finish()


if __name__ == "__main__":
    sys.exit(main())
