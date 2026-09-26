"""Sink outage: stop TimescaleDB for 60 s mid-run.

Expected: the sink retries, commits no offsets while the database is down, replays after the
restart, and idempotent upserts leave zero lost and zero duplicated rows."""

import sys
import time

from scripts import chaoslib as c

OUTAGE_S = 60


def main() -> int:
    c.load_env()
    r = c.Report(
        "Sink outage", f"stop TimescaleDB for {OUTAGE_S} s at ~30 s of a 120 s run (speed 60)"
    )
    c.reset_environment(processors=2)
    sim = c.start_sim(duration=120)
    time.sleep(30)

    c.compose("stop", "timescaledb")
    time.sleep(OUTAGE_S)
    lag_during, _ = c.group_state("timescale-sink")
    c.compose("start", "timescaledb")
    back = c.wait_until(lambda: c.db_scalar("select 1") == 1, 60, 1)

    summary = sim.wait()
    drained = c.wait_drained()
    r.check("sink accumulated lag while the database was down", lag_during > 0, f"lag={lag_during}")
    r.check("database reachable again", back is not None, f"{back:.0f} s" if back else "")
    r.check(
        "consumer lag drains to 0 after restart",
        drained is not None,
        f"{drained:.0f} s" if drained else "",
    )
    time.sleep(10)
    v = c.verify_db(sim, summary)
    r.check(
        "0 lost: every closed window is in the database",
        not v["missing"],
        f"missing={len(v['missing'])}",
    )
    r.check("0 wrong values vs ground truth", not v["mismatches"] and v["compared"] > 0,
            f"{v['compared']} windows compared, {len(v['mismatches'])} mismatches")  # fmt: skip
    r.check(
        "0 duplicated rows",
        v["rows"] == v["distinct_rows"],
        f"{v['rows']} rows / {v['distinct_rows']} keys",
    )
    r.metric("sink lag at end of outage", lag_during)
    r.metric("time to drain after restart", f"{drained:.0f} s" if drained else "n/a")
    return r.finish()


if __name__ == "__main__":
    sys.exit(main())
