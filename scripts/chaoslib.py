"""Shared helpers for the chaos scenarios (scripts/chaos_*.py).

Every scenario: reset to a known state, run the simulator with ground truth, inflict a failure,
wait for the pipeline to settle, then check explicit expectations and print PASS/FAIL. Results
are appended to docs/RESULTS.md so the numbers in the paper are the ones the scripts measured.
"""

import json
import os
import re
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psycopg

from common.config import Settings
from scripts.verify_windows import compare, load_results_db

ROOT = Path(__file__).resolve().parent.parent
RESULTS = ROOT / "docs" / "RESULTS.md"
BROKERS_INTERNAL = "kafka-1:9092,kafka-2:9092,kafka-3:9092"
PARTITIONS = 6
GROUPS = ("occupancy-processor", "timescale-sink")


def load_env() -> None:
    """Make host-side tools see the same settings as the containers."""
    env_file = ROOT / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                os.environ.setdefault(key, value)
    os.environ["KAFKA_BOOTSTRAP_HOST"] = "localhost:19092,localhost:29092,localhost:39092"
    os.environ["POSTGRES_HOST"] = "localhost"
    os.environ["PYTHONPATH"] = str(ROOT)


def sh(cmd: list[str], check: bool = True, timeout: float = 300) -> str:
    out = subprocess.run(
        cmd, cwd=ROOT, capture_output=True, text=True, timeout=timeout, check=False
    )
    if check and out.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} failed: {out.stderr.strip()[:500]}")
    return out.stdout + out.stderr


def compose(*args: str, check: bool = True, timeout: float = 300) -> str:
    return sh(["docker", "compose", *args], check=check, timeout=timeout)


def kafka_tool(tool: str, *args: str, check: bool = True) -> str:
    return compose(
        "exec", "-T", "kafka-1", f"/opt/kafka/bin/{tool}.sh",
        "--bootstrap-server", BROKERS_INTERNAL, *args, check=check,
    )  # fmt: skip


def wait_until(
    condition: Callable[[], bool], timeout: float, interval: float = 1.0
) -> float | None:
    """Seconds until `condition()` first held, or None on timeout."""
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        try:
            if condition():
                return time.monotonic() - start
        except (RuntimeError, subprocess.TimeoutExpired, psycopg.Error):
            pass
        time.sleep(interval)
    return None


# ---- Kafka state ----------------------------------------------------------------------------


def group_state(group: str) -> tuple[int, int]:
    """(total lag, partitions of parking.raw/occupancy topics with a live consumer)."""
    text = kafka_tool("kafka-consumer-groups", "--describe", "--group", group, check=False)
    lag = assigned = 0
    for line in text.splitlines():
        parts = line.split()
        # LAG is "-" until the group has an offset to compare against.
        if len(parts) >= 7 and parts[0] == group and (parts[5].isdigit() or parts[5] == "-"):
            lag += int(parts[5]) if parts[5].isdigit() else 0
            if parts[6] != "-":
                assigned += 1
    return lag, assigned


def leaders_on(broker_id: int) -> int:
    text = kafka_tool("kafka-topics", "--describe", check=False)
    return len(re.findall(rf"Leader: {broker_id}\b", text))


def under_replicated() -> int:
    text = kafka_tool("kafka-topics", "--describe", "--under-replicated-partitions", check=False)
    return sum(1 for line in text.splitlines() if "Partition:" in line)


def topic_end_offsets(topic: str) -> int:
    text = kafka_tool("kafka-get-offsets", "--topic", topic)
    return sum(int(line.rsplit(":", 1)[1]) for line in text.splitlines() if line.count(":") == 2)


def processor_metrics(names: list[str]) -> dict[str, float]:
    """Sum counters across all running processor containers (read straight from /metrics)."""
    script = "import urllib.request as u;print(u.urlopen('http://localhost:8000/metrics').read().decode())"
    totals = dict.fromkeys(names, 0.0)
    for cid in container_ids("processor"):
        try:
            text = sh(["docker", "exec", cid, "python", "-c", script])
        except RuntimeError:
            continue  # still starting: counts as nothing consumed yet
        for line in text.splitlines():
            for name in names:
                if line.startswith(name + " ") or line.startswith(name + "{"):
                    totals[name] += float(line.rsplit(" ", 1)[1])
    return totals


def processor_metric(name: str) -> float:
    return processor_metrics([name])[name]


CONSUMED_COUNTERS = [
    "parking_events_processed_total",
    "parking_events_dlq_total",
    "parking_events_duplicate_total",
    "parking_events_late_total",
    "parking_events_replay_skipped_total",
]


def raw_backlog() -> tuple[int, int]:
    """(unprocessed messages on parking.raw, messages consumed so far).

    Committed-offset lag (kafka-consumer-groups) is coarse because the processor commits only at
    checkpoints, so backlog is measured as topic end offsets minus the processors' own counters.
    Valid while the processors were started after the topics were reset."""
    consumed = int(sum(processor_metrics(CONSUMED_COUNTERS).values()))
    return topic_end_offsets("parking.raw") - consumed, consumed


# ---- database ---------------------------------------------------------------------------------


def db_scalar(sql: str) -> Any:
    with psycopg.connect(Settings.from_env().postgres_dsn, connect_timeout=5) as conn:
        return conn.execute(sql).fetchone()[0]  # type: ignore[index]


# ---- environment ------------------------------------------------------------------------------


def reset_environment(processors: int = 2, alerter: bool = False, wait_ready: bool = True) -> None:
    """Fresh topics, empty tables, N processors + sink running with all partitions assigned."""
    compose("up", "-d", "--wait", "kafka-1", "kafka-2", "kafka-3", "timescaledb", timeout=300)
    sh(["make", "reset-topics"])
    compose("exec", "-T", "timescaledb", "psql", "-U", os.environ["POSTGRES_USER"],
            "-d", os.environ["POSTGRES_DB"], "-qc",
            "truncate lot_occupancy_5min, alerts, lot_metadata")  # fmt: skip
    services = ["processor", "sink", *(["alerter"] if alerter else [])]
    compose("up", "-d", "--scale", f"processor={processors}", *services)
    for group in GROUPS if wait_ready else ():
        ready = wait_until(lambda g=group: group_state(g)[1] == PARTITIONS or _is_lazy(g), 90, 2)
        if ready is None:
            raise RuntimeError(f"group {group} never got all partitions assigned")


def _is_lazy(group: str) -> bool:
    """Sink partitions only appear once its topics exist and it has polled; tolerate empty."""
    return group == "timescale-sink" and group_state(group)[1] > 0


def container_ids(service: str) -> list[str]:
    return compose("ps", "-q", service).split()


# ---- simulator ----------------------------------------------------------------------------------


@dataclass
class SimRun:
    proc: subprocess.Popen[bytes]
    truth: Path
    summary: Path
    log: Path
    started_at: float = field(default_factory=time.time)

    def wait(self) -> dict[str, Any]:
        self.proc.wait()
        if self.proc.returncode != 0:
            raise RuntimeError(f"simulator failed, see {self.log}")
        summary: dict[str, Any] = json.loads(self.summary.read_text())
        return summary


def start_sim(
    *flags: str, duration: int = 100, speed: float = 60, lots: int = 12, truth_file: bool = True
) -> SimRun:
    workdir = Path(tempfile.mkdtemp(prefix="chaos-"))
    truth, summary, log = workdir / "truth.json", workdir / "summary.json", workdir / "sim.log"
    cmd = [
        sys.executable, "-m", "simulator.main",
        "--duration", str(duration), "--speed", str(speed), "--lots", str(lots),
        "--summary-file", str(summary), *(["--truth-file", str(truth)] if truth_file else []), *flags,
    ]  # fmt: skip
    with log.open("wb") as log_file:
        proc = subprocess.Popen(cmd, cwd=ROOT, stdout=log_file, stderr=subprocess.STDOUT)
    return SimRun(proc, truth, summary, log)


def wait_drained(timeout: float = 120) -> float | None:
    """Seconds until both consumer groups have zero lag."""
    return wait_until(lambda: all(group_state(g)[0] == 0 for g in GROUPS), timeout, 2)


def verify_db(sim: SimRun, summary: dict[str, Any]) -> dict[str, Any]:
    """Compare the database with the simulator's ground truth (closed, complete windows)."""
    cfg = Settings.from_env()
    truth = json.loads(sim.truth.read_text())
    results, _ = load_results_db(cfg.postgres_dsn)
    report: dict[str, Any] = compare(
        truth,
        results,
        cfg.window_size_s,
        cfg.allowed_lateness_s + cfg.window_grace_s,
        summary["ended_sim_ts"],
    )
    report["rows"] = db_scalar("select count(*) from lot_occupancy_5min")
    report["distinct_rows"] = db_scalar(
        "select count(*) from (select distinct lot_id, window_start from lot_occupancy_5min) t"
    )
    return report


# ---- reporting ----------------------------------------------------------------------------------


class Report:
    def __init__(self, name: str, description: str) -> None:
        self.name, self.description = name, description
        self.checks: list[tuple[bool, str, str]] = []
        self.metrics: list[tuple[str, str]] = []
        print(f"\n=== {name}: {description}")

    def check(self, label: str, ok: bool, detail: str = "") -> bool:
        self.checks.append((ok, label, detail))
        print(f"  {'PASS' if ok else 'FAIL'}  {label}" + (f"  [{detail}]" if detail else ""))
        return ok

    def metric(self, label: str, value: object) -> None:
        self.metrics.append((label, str(value)))
        print(f"  ....  {label}: {value}")

    def finish(self) -> int:
        passed = all(ok for ok, _, _ in self.checks)
        print(f"=== {self.name}: {'PASS' if passed else 'FAIL'}\n")
        self._append_results(passed)
        return 0 if passed else 1

    def _append_results(self, passed: bool) -> None:
        text = RESULTS.read_text()
        heading = "## Phase 8: chaos runs (appended by scripts/chaos_*.py)"
        if heading not in text:
            text += f"\n{heading}\n"
        stamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
        lines = [
            f"\n### {self.name}: {'PASS' if passed else 'FAIL'} ({stamp})",
            self.description,
            "",
        ]
        lines += ["| Result | Expectation | Measured |", "|---|---|---|"]
        lines += [
            f"| {'PASS' if ok else 'FAIL'} | {label} | {detail} |"
            for ok, label, detail in self.checks
        ]
        if self.metrics:
            lines += ["", *[f"- {label}: {value}" for label, value in self.metrics]]
        RESULTS.write_text(text.rstrip("\n") + "\n" + "\n".join(lines) + "\n")
