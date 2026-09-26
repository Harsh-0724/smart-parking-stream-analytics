"""Load test: ramp the offered event rate at 1, 2 and 3 processor instances.

    make load-test            (about 20 minutes; resets the stack, so run it on an idle machine)

For each processor count and each offered rate it records: events actually produced per second,
events the processors consumed per second, backlog on parking.raw (raw end offsets minus events the
processors report consuming), processor CPU, and ingest-to-emit latency p50/p95/p99 from Prometheus.
Results go to docs/loadtest/ (CSV + charts) and a table is appended to docs/RESULTS.md.

The load generator runs on the same machine as the stack, so it competes with the processors for CPU;
the numbers are a lower bound on what dedicated hardware would do.
"""

import csv
import json
import math
import re
import subprocess
import sys
import time
import urllib.parse
import urllib.request
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402

from scripts import chaoslib as c  # noqa: E402

RATES = (1_000, 5_000, 10_000, 25_000, 50_000, 100_000)
PROCESSORS = (1, 2, 3)
HOLD_S = 30
WORKER_RATE = 20_000  # events/s one generator process is asked to sustain
DRAIN_TIMEOUT_S = 90
SATURATION_BACKLOG_S = 3  # backlog above this many seconds of offered rate at the end of the hold
OUT = c.ROOT / "docs" / "loadtest"
PROM = "http://localhost:9090"


@dataclass
class Row:
    processors: int
    offered: int
    produced_per_s: float
    consumed_per_s: float
    peak_backlog: int
    end_backlog: int
    drain_s: float
    cpu_pct: float
    p50_s: float
    p95_s: float
    p99_s: float
    saturated: bool


def prom(expr: str) -> float:
    url = f"{PROM}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
    with urllib.request.urlopen(url, timeout=10) as response:  # noqa: S310
        result = json.load(response)["data"]["result"]
    value = float(result[0]["value"][1]) if result else math.nan
    return value


def quantile(q: float, window_s: int) -> float:
    return prom(
        f"histogram_quantile({q}, sum by (le) "
        f"(increase(parking_ingest_to_emit_seconds_bucket[{window_s}s])))"
    )


def processor_cpu() -> float:
    ids = c.container_ids("processor")
    out = c.sh(["docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", *ids])
    return sum(float(x.strip("%")) for x in out.split() if re.fullmatch(r"[\d.]+%", x))


def step(processors: int, offered: int) -> Row:
    workers = max(1, math.ceil(offered / WORKER_RATE))
    before = c.processor_metrics(c.CONSUMED_COUNTERS)
    started = time.monotonic()
    gen = subprocess.Popen(
        [sys.executable, "scripts/loadgen.py", "--rate", str(offered), "--duration", str(HOLD_S), "--workers", str(workers)],
        cwd=c.ROOT, stdout=subprocess.PIPE, text=True,
    )  # fmt: skip
    peak, cpu = 0, []
    while gen.poll() is None:
        backlog, _ = c.raw_backlog()
        peak = max(peak, backlog)
        if time.monotonic() - started > HOLD_S / 2:
            cpu.append(processor_cpu())
        time.sleep(1)
    load_seconds = time.monotonic() - started
    delivered = json.loads((gen.stdout.read() if gen.stdout else "{}").strip().splitlines()[-1])[
        "delivered"
    ]

    consumed_at_end = sum(c.processor_metrics(c.CONSUMED_COUNTERS).values())
    end_backlog, _ = c.raw_backlog()
    consumed_rate = (consumed_at_end - sum(before.values())) / load_seconds

    drain_started = time.monotonic()
    drained = c.wait_until(lambda: c.raw_backlog()[0] == 0, DRAIN_TIMEOUT_S, 1)
    drain_s = drained if drained is not None else -1.0
    _ = drain_started
    time.sleep(6)  # let Prometheus scrape the final counters
    window = int(load_seconds) + 10
    return Row(
        processors=processors,
        offered=offered,
        produced_per_s=delivered / HOLD_S,
        consumed_per_s=consumed_rate,
        peak_backlog=peak,
        end_backlog=end_backlog,
        drain_s=drain_s,
        cpu_pct=sum(cpu) / len(cpu) if cpu else math.nan,
        p50_s=quantile(0.5, window),
        p95_s=quantile(0.95, window),
        p99_s=quantile(0.99, window),
        saturated=end_backlog > SATURATION_BACKLOG_S * offered,
    )


def run_all() -> list[Row]:
    rows: list[Row] = []
    for n in PROCESSORS:
        print(f"\n=== {n} processor(s)")
        c.reset_environment(processors=n)
        c.compose("stop", "api", "web", "grafana", "simulator", "alerter", check=False)
        from scripts.loadgen import publish_metadata

        publish_metadata(c.os.environ["KAFKA_BOOTSTRAP_HOST"])
        time.sleep(5)
        for rate in RATES:
            row = step(n, rate)
            rows.append(row)
            print(
                f"  offered {rate:>7,}/s  produced {row.produced_per_s:>9,.0f}  consumed {row.consumed_per_s:>9,.0f}  "
                f"backlog end {row.end_backlog:>9,}  p95 {row.p95_s:5.1f}s  cpu {row.cpu_pct:5.0f}%"
                f"{'  SATURATED' if row.saturated else ''}"
            )
            if row.saturated:
                break  # higher offered rates only add backlog
    return rows


def write_outputs(rows: list[Row]) -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    with (OUT / "load_test.csv").open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(asdict(rows[0])))
        writer.writeheader()
        writer.writerows(asdict(r) for r in rows)

    colours = {1: "#1b1a17", 2: "#0b7a6e", 3: "#b45309"}
    fig, axes = plt.subplots(1, 3, figsize=(15, 4))
    for n in PROCESSORS:
        rs = [r for r in rows if r.processors == n]
        if not rs:
            continue
        x = [r.offered for r in rs]
        axes[0].plot(
            x,
            [r.consumed_per_s for r in rs],
            marker="o",
            label=f"{n} processor(s)",
            color=colours[n],
        )
        axes[1].plot(
            x, [r.p95_s for r in rs], marker="o", label=f"{n} processor(s)", color=colours[n]
        )
        axes[2].plot(
            x, [r.end_backlog for r in rs], marker="o", label=f"{n} processor(s)", color=colours[n]
        )
    axes[0].plot([RATES[0], RATES[-1]], [RATES[0], RATES[-1]], ls=":", color="grey", label="ideal")
    for ax, title, ylabel in zip(
        axes,
        ("Throughput", "Ingest-to-emit latency p95", "Backlog at end of 30 s hold"),
        ("events/s consumed", "seconds", "messages"),
        strict=True,
    ):
        ax.set_xscale("log")
        ax.set_title(title)
        ax.set_xlabel("offered events/s")
        ax.set_ylabel(ylabel)
        ax.grid(alpha=0.3)
        ax.legend()
    axes[0].set_yscale("log")
    fig.tight_layout()
    fig.savefig(OUT / "load_test.png", dpi=140)

    stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"\n### Load test: ramp at 1, 2 and 3 processors ({stamp})",
        f"`make load-test`. {HOLD_S} s per step, valid events at an exact offered rate (30% OCCUPANCY, 70% HEARTBEAT, 12 lots x 200 "
        "slots, production producer settings), event time = wall time. Generator and stack share one machine (10 CPUs, Docker "
        "Desktop), so figures are a lower bound. Backlog = `parking.raw` end offsets minus events consumed. Latency is "
        "ingest-to-emit from Prometheus (sampled 1 in 8 events); the 5 s emit interval sets its floor. Charts: "
        "`docs/loadtest/load_test.png`, data: `docs/loadtest/load_test.csv`.",
        "",
        "| Processors | Offered/s | Produced/s | Consumed/s | p50 | p95 | p99 | Backlog at end | Drain | Processor CPU | |",
        "|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        lines.append(
            f"| {r.processors} | {r.offered:,} | {r.produced_per_s:,.0f} | {r.consumed_per_s:,.0f} | {r.p50_s:.1f} s | "
            f"{r.p95_s:.1f} s | {r.p99_s:.1f} s | {r.end_backlog:,} | {'n/a' if r.drain_s < 0 else f'{r.drain_s:.0f} s'} | "
            f"{r.cpu_pct:.0f}% | {'saturated' if r.saturated else ''} |"
        )
    lines += [
        "",
        "HyperLogLog accuracy and memory (worst error 7.57% at 1M vehicles against a 9.75% limit; 1 KiB per window versus 88 MB for an exact set) is in the section above.",
    ]
    c.RESULTS.write_text(c.RESULTS.read_text().rstrip("\n") + "\n" + "\n".join(lines) + "\n")


if __name__ == "__main__":
    c.load_env()
    results = run_all()
    write_outputs(results)
    print(
        f"\nwrote {OUT / 'load_test.csv'}, {OUT / 'load_test.png'} and a table in docs/RESULTS.md"
    )
