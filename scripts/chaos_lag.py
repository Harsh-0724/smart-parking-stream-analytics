"""Lag spike: a 1M+ event burst arrives while consumers are down; recover with 1 vs 3 processors.

Expected: the backlog on parking.raw rises far above zero, drains to 0 once processors start,
and 3 processors drain the identical backlog faster than 1.

The burst is produced with the processors stopped because a single processor sustains about
90,000 events/s, faster than the synthetic generator (~19,000/s) and close to the replay
generator, so a live burst never builds a backlog. Backlog is measured as (raw topic end offsets -
events the processors report consuming): committed-offset lag would be coarse because commits
happen only at 10 s checkpoints."""

import sys
import time

from scripts import chaoslib as c

SPEED = 1000
DURATION_S = 20
REPLAY = ("--mode", "replay", "--replay-days", "1")
LOTS = 30
DRAIN_TIMEOUT_S = 300


def recover(processors: int) -> dict[str, float]:
    c.reset_environment(processors=0, wait_ready=False)
    sim = c.start_sim(*REPLAY, duration=DURATION_S, speed=SPEED, lots=LOTS, truth_file=False)
    events = sim.wait()["total"]
    backlog = c.topic_end_offsets("parking.raw")

    started = time.monotonic()
    c.compose("up", "-d", "--scale", f"processor={processors}", "processor")
    first_consumed: float | None = None
    drained: float | None = None
    while drained is None and time.monotonic() - started < DRAIN_TIMEOUT_S:
        left, consumed = c.raw_backlog()
        if first_consumed is None and consumed > 0:
            first_consumed = time.monotonic() - started
        if left == 0:
            drained = time.monotonic() - started
        time.sleep(1)
    working = (drained - (first_consumed or 0)) if drained else -1
    return {
        "processors": processors,
        "events": events,
        "backlog": backlog,
        "startup_s": first_consumed or -1,
        "drain_s": drained or -1,
        "rate": backlog / working if working > 0 else -1,
    }


def main() -> int:
    c.load_env()
    r = c.Report(
        "Lag spike",
        f"{DURATION_S} s Birmingham replay at {SPEED}x produced with consumers down, then drained by 1 vs 3 processors",
    )
    one = recover(1)
    three = recover(3)
    for run in (one, three):
        n = int(run["processors"])
        r.metric(f"{n} processor(s): backlog before start", f"{int(run['backlog'])} events")
        r.metric(
            f"{n} processor(s): container start to first consumption", f"{run['startup_s']:.0f} s"
        )
        r.metric(
            f"{n} processor(s): time to drain the backlog (from start)", f"{run['drain_s']:.0f} s"
        )
        r.metric(f"{n} processor(s): drain rate once consuming", f"{run['rate']:.0f} events/s")
    r.check(
        "backlog rose above 1,000,000 events (consumers down)",
        one["backlog"] > 1_000_000,
        f"{int(one['backlog'])}",
    )
    r.check(
        "same backlog in both runs (within 1%)",
        abs(one["backlog"] - three["backlog"]) < 0.01 * one["backlog"],
        f"{int(one['backlog'])} vs {int(three['backlog'])}",
    )
    r.check("backlog drained to 0 (1 processor)", one["drain_s"] > 0, f"{one['drain_s']:.0f} s")
    r.check(
        "backlog drained to 0 (3 processors)", three["drain_s"] > 0, f"{three['drain_s']:.0f} s"
    )
    r.check(
        "3 processors drain faster than 1",
        0 < three["drain_s"] < one["drain_s"],
        f"{three['drain_s']:.0f} s vs {one['drain_s']:.0f} s",
    )
    r.metric(
        "speed-up in end-to-end drain time (includes ~12-15 s container start)",
        f"{one['drain_s'] / three['drain_s']:.2f}x",
    )
    r.metric("speed-up in drain rate once consuming", f"{three['rate'] / one['rate']:.2f}x")
    return r.finish()


if __name__ == "__main__":
    sys.exit(main())
