"""Processor crash: SIGKILL one of two processors mid-run.

Expected: the survivor takes over the dead instance's partitions, restores their state from
state.changelog, and the database ends up with no gaps and no duplicated or wrong windows."""

import json
import sys
import time
from datetime import UTC, datetime

from scripts import chaoslib as c

TAKEOVER_LIMIT_S = 30


def main() -> int:
    c.load_env()
    r = c.Report("Processor crash", "SIGKILL one of 2 processors at ~35 s of a 90 s run (speed 60)")
    c.reset_environment(processors=2)
    sim = c.start_sim(duration=90)
    time.sleep(35)

    victim, survivor = c.container_ids("processor")
    killed_at = time.time()
    c.sh(["docker", "kill", victim])
    since = datetime.fromtimestamp(killed_at, UTC).isoformat()

    def takeover_lines() -> list[dict[str, object]]:
        logs = c.sh(["docker", "logs", "--since", since, survivor], check=False)
        events = [json.loads(x) for x in logs.splitlines() if x.startswith("{")]
        return [e for e in events if e.get("event") == "partition assigned"]

    took = c.wait_until(lambda: len({e["partition"] for e in takeover_lines()}) >= 3, 60, 1)
    assigned = takeover_lines()
    restored = sum(len(e["restored_lots"]) for e in assigned)  # type: ignore[arg-type]
    r.check(
        f"survivor assigned the 3 orphaned partitions within {TAKEOVER_LIMIT_S} s",
        took is not None and took < TAKEOVER_LIMIT_S,
        f"{took:.1f} s" if took is not None else "never",
    )
    r.check("state restored from state.changelog", restored >= 6, f"{restored} lots restored")

    summary = sim.wait()
    drained = c.wait_drained()
    r.check("consumer lag drains to 0", drained is not None, f"{drained:.0f} s" if drained else "")
    time.sleep(10)
    v = c.verify_db(sim, summary)
    r.check(
        "no gaps: every closed window is in the database",
        not v["missing"],
        f"missing={len(v['missing'])}",
    )
    r.check("no wrong values vs ground truth", not v["mismatches"] and v["compared"] > 0,
            f"{v['compared']} windows compared, {len(v['mismatches'])} mismatches")  # fmt: skip
    r.check(
        "no duplicate rows",
        v["rows"] == v["distinct_rows"],
        f"{v['rows']} rows / {v['distinct_rows']} keys",
    )
    r.metric("takeover time (kill to partitions assigned)", f"{took:.1f} s" if took else "n/a")
    r.metric("simulator events sent", summary["total"])
    return r.finish()


if __name__ == "__main__":
    sys.exit(main())
