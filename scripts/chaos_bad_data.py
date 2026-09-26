"""Bad data: 5% malformed messages.

Expected: every malformed message lands in parking.dlq with its original bytes and an error,
the processors never crash, and the valid data is unaffected."""

import json
import os
import sys
import time

from common import topics
from common.kafka import read_to_end
from scripts import chaoslib as c


def main() -> int:
    c.load_env()
    r = c.Report("Bad data", "--malformed-pct 5 for 90 s (speed 60), 2 processors")
    c.reset_environment(processors=2)
    sim = c.start_sim("--malformed-pct", "5", duration=90)
    summary = sim.wait()
    drained = c.wait_drained()
    r.check("consumer lag drains to 0", drained is not None)

    sent = summary["malformed"]
    dlq = c.topic_end_offsets(topics.DLQ)
    r.check(
        "every malformed message is in parking.dlq", dlq == sent, f"sent {sent}, DLQ holds {dlq}"
    )
    counted = int(c.processor_metric("parking_events_dlq_total"))
    r.check("processor DLQ counter agrees", counted == sent, f"metric={counted}")

    sample = [
        json.loads(v)
        for _, v, _ in read_to_end(os.environ["KAFKA_BOOTSTRAP_HOST"], topics.DLQ, 0)[:50]
        if v
    ]
    ok = bool(sample) and all(
        x["error"] and bytes.fromhex(x["original_hex"]) is not None for x in sample
    )
    r.check(
        "DLQ records carry the original bytes and an error reason", ok, f"{len(sample)} sampled"
    )

    restarts = [
        c.sh(["docker", "inspect", cid, "--format", "{{.RestartCount}}"]).strip()
        for cid in c.container_ids("processor")
    ]
    r.check(
        "processors never crashed or restarted",
        set(restarts) == {"0"},
        f"restart counts {restarts}",
    )

    time.sleep(10)
    v = c.verify_db(sim, summary)
    r.check("valid data unaffected: windows match ground truth", not v["missing"] and not v["mismatches"] and v["compared"] > 0,
            f"{v['compared']} windows compared, {len(v['mismatches'])} mismatches, {len(v['missing'])} missing")  # fmt: skip
    r.metric("messages sent / malformed", f"{summary['total'] + sent} / {sent}")
    return r.finish()


if __name__ == "__main__":
    sys.exit(main())
