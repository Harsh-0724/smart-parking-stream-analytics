"""Measure raw ingestion latency (produce -> processor consume, no windowing) next to the windowed
emit latency, at several offered rates.

    make ingest-latency

Both come from Prometheus histograms populated by the processors:
  parking_ingest_to_consume_seconds  producer's ingest_ts -> the processor has the event (no windowing)
  parking_ingest_to_emit_seconds     producer's ingest_ts -> the emit that reflects it (waits for the 5 s emit)
Quantiles are estimated by Prometheus from histogram buckets, so their resolution is the bucket width.
"""

import math
import subprocess
import sys
import time
from datetime import UTC, datetime

from scripts import chaoslib as c
from scripts.load_test import PROM, prom  # noqa: F401  (PROM re-exported for clarity)

RATES = (1_000, 10_000, 50_000, 100_000)
HOLD_S = 30
PROCESSORS = 2


def q(metric: str, quantile: float, window_s: int) -> float:
    return prom(
        f"histogram_quantile({quantile}, sum by (le) (increase({metric}_bucket[{window_s}s])))"
    )


def main() -> int:
    c.load_env()
    c.reset_environment(processors=PROCESSORS)
    c.compose("stop", "api", "web", "grafana", "simulator", "alerter", check=False)
    from scripts.loadgen import publish_metadata

    publish_metadata(c.os.environ["KAFKA_BOOTSTRAP_HOST"])
    time.sleep(5)
    rows = []
    for rate in RATES:
        workers = max(1, math.ceil(rate / 20_000))
        started = time.time()
        subprocess.run(
            [sys.executable, "scripts/loadgen.py", "--rate", str(rate), "--duration", str(HOLD_S), "--workers", str(workers)],
            cwd=c.ROOT, check=True, capture_output=True,
        )  # fmt: skip
        c.wait_until(lambda: c.raw_backlog()[0] == 0, 90, 1)
        time.sleep(7)  # let Prometheus scrape the final counters
        window = int(time.time() - started) + 5
        row = {
            "rate": rate,
            "consume": [
                q("parking_ingest_to_consume_seconds", x, window) for x in (0.5, 0.95, 0.99)
            ],
            "emit": [q("parking_ingest_to_emit_seconds", x, window) for x in (0.5, 0.95, 0.99)],
        }
        rows.append(row)
        print(row)

    ms = lambda v: f"{v * 1000:.0f} ms"  # noqa: E731
    lines = [
        f"\n### Ingestion latency versus windowed-emit latency ({datetime.now(UTC).strftime('%Y-%m-%d %H:%M UTC')})",
        f"`make ingest-latency`: {PROCESSORS} processors, {HOLD_S} s per rate, valid events from `scripts/loadgen.py`, "
        "producer and processors on one host (so wall clocks agree). Quantiles are Prometheus estimates from "
        "histogram buckets (1 ms to 60 s), so resolution is the bucket width.",
        "",
        "| Offered/s | **Raw ingestion** p50 | p95 | p99 | Windowed emit p50 | p95 | p99 |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        a, b = r["consume"], r["emit"]
        lines.append(
            f"| {r['rate']:,} | **{ms(a[0])}** | **{ms(a[1])}** | **{ms(a[2])}** | {b[0]:.1f} s | {b[1]:.1f} s | {b[2]:.1f} s |"
        )
    c.RESULTS.write_text(c.RESULTS.read_text().rstrip("\n") + "\n" + "\n".join(lines) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
