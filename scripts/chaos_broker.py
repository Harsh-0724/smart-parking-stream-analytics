"""Broker failure: kill kafka-2 mid-run, then bring it back.

Default is a crash (SIGKILL): the controller must detect the dead broker via its session timeout.
`--graceful` sends SIGTERM instead; the broker then hands off leadership itself (controlled shutdown).

Expected: leaders move off the dead broker, producers and consumers keep going (RF=3,
min.insync.replicas=2 tolerates one broker down), no data is lost, and the ISR is whole again
after the restart."""

import sys
import time

from scripts import chaoslib as c

BROKER = 2
ELECTION_LIMIT_S = 30


def main() -> int:
    c.load_env()
    graceful = "--graceful" in sys.argv
    mode = "graceful stop" if graceful else "SIGKILL crash"
    r = c.Report(
        f"Broker failure ({mode})",
        f"{'stop' if graceful else 'SIGKILL'} kafka-{BROKER} at ~30 s of a 120 s run (speed 60), "
        "restart ~35 s later",
    )
    c.reset_environment(processors=2)
    led_before = c.leaders_on(BROKER)
    sim = c.start_sim(duration=120)
    time.sleep(30)

    rows_at_stop = c.db_scalar("select count(*) from lot_occupancy_5min where closed")
    c.compose("stop" if graceful else "kill", f"kafka-{BROKER}")
    elected = c.wait_until(lambda: c.leaders_on(BROKER) == 0, 60, 0.5)
    r.check(f"leadership moved off kafka-{BROKER} within {ELECTION_LIMIT_S} s",
            elected is not None and elected < ELECTION_LIMIT_S,
            f"{elected:.1f} s (it led {led_before} partitions)" if elected is not None else "never",
    )  # fmt: skip

    time.sleep(35)
    rows_during = c.db_scalar("select count(*) from lot_occupancy_5min where closed")
    r.check("pipeline kept producing closed windows during the outage", rows_during > rows_at_stop,
            f"{rows_at_stop} -> {rows_during} closed rows")  # fmt: skip

    c.compose("start", f"kafka-{BROKER}")
    healed = c.wait_until(lambda: c.under_replicated() == 0, 180, 1)
    r.check(
        "ISR fully restored after restart (0 under-replicated partitions)",
        healed is not None,
        f"{healed:.0f} s after restart" if healed is not None else "still under-replicated",
    )
    summary = sim.wait()
    errors = summary["delivery_errors"]
    r.check(
        "producer had no delivery errors (acks=all, RF=3, min.isr=2)",
        errors == 0,
        f"errors={errors}",
    )
    drained = c.wait_drained(180)
    r.check("consumer lag drains to 0", drained is not None, f"{drained:.0f} s" if drained else "")
    time.sleep(10)
    v = c.verify_db(sim, summary)
    r.check(
        "no data loss: every closed window is in the database",
        not v["missing"],
        f"missing={len(v['missing'])}",
    )
    r.check("no wrong values vs ground truth", not v["mismatches"] and v["compared"] > 0,
            f"{v['compared']} windows compared, {len(v['mismatches'])} mismatches")  # fmt: skip
    r.metric("leader election time", f"{elected:.1f} s" if elected is not None else "n/a")
    r.metric("time to full ISR after restart", f"{healed:.0f} s" if healed is not None else "n/a")
    return r.finish()


if __name__ == "__main__":
    sys.exit(main())
