"""Compare closed windows on lot.occupancy.5min with the simulator's ground truth.

    python scripts/verify_windows.py /tmp/truth.json /tmp/sim.json

Only closed windows that the simulator covered completely are compared (the first window is
partial by construction). Prints a JSON report; exit code 1 if any comparison fails.
"""

import argparse
import json
import os
import sys
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from common import topics
from common.config import Settings
from common.kafka import read_to_end
from common.schemas import WindowResult
from processor.hll import STANDARD_ERROR

AVG_TOLERANCE = 0.05  # occupied slots


def load_results(bootstrap: str) -> tuple[dict[str, WindowResult], Counter[str]]:
    """Latest result per (lot, window) plus how many messages each key received."""
    latest: dict[str, WindowResult] = {}
    per_key: Counter[str] = Counter()
    for partition in range(3):
        for key, value, _ in read_to_end(bootstrap, topics.OCCUPANCY_5MIN, partition):
            if key is None or value is None:
                continue
            result = WindowResult.model_validate_json(value)
            per_key[key.decode()] += 1
            latest[key.decode()] = result
    return latest, per_key


def compare(
    truth: dict[str, dict[str, float]],
    results: dict[str, WindowResult],
    window_s: int,
    close_after_s: float,
    ended_ts: float,
) -> dict[str, object]:
    starts = sorted({int(k.split("|")[1]) for k in truth})
    # Skip the partial first window and any window the watermark had not yet closed at the end.
    complete = {s for s in starts[1:] if s + window_s + close_after_s < ended_ts}
    report: dict[str, object] = {"compared": 0, "missing": [], "mismatches": [], "hll_errors": []}
    for key, expected in truth.items():
        lot, start_s = key.split("|")
        if int(start_s) not in complete:
            continue
        iso = datetime.fromtimestamp(int(start_s), UTC).isoformat()
        got = results.get(f"{lot}|{iso}")
        if got is None or not got.closed:
            report["missing"].append(key)  # type: ignore[attr-defined]
            continue
        report["compared"] += 1  # type: ignore[operator]
        if abs(got.avg_occupied - expected["avg_occupied"]) > AVG_TOLERANCE:
            report["mismatches"].append(  # type: ignore[attr-defined]
                f"{key} avg {got.avg_occupied} != {expected['avg_occupied']:.3f}"
            )
        for field in ("entries", "exits"):
            if getattr(got, field) != expected[field]:
                report["mismatches"].append(  # type: ignore[attr-defined]
                    f"{key} {field} {getattr(got, field)} != {expected[field]}"
                )
        exact = expected["unique_vehicles"]
        if exact:
            report["hll_errors"].append(abs(got.unique_vehicles_est - exact) / exact)  # type: ignore[attr-defined]
    return report


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("truth", type=Path)
    ap.add_argument("summary", type=Path, help="simulator --summary-file")
    args = ap.parse_args()
    truth = json.loads(args.truth.read_text())
    results, per_key = load_results(os.environ.get("KAFKA_BOOTSTRAP_HOST", "localhost:19092"))
    cfg = Settings.from_env()
    summary = json.loads(args.summary.read_text())
    report = compare(
        truth,
        results,
        cfg.window_size_s,
        cfg.allowed_lateness_s + cfg.window_grace_s,
        summary["ended_sim_ts"],
    )
    errs = report.pop("hll_errors")
    assert isinstance(errs, list)
    report["hll_max_error_pct"] = round(100 * max(errs, default=0), 2)
    report["hll_mean_error_pct"] = round(100 * sum(errs) / len(errs), 2) if errs else 0
    report["hll_expected_std_error_pct"] = round(100 * STANDARD_ERROR, 2)
    report["result_messages_per_key_max"] = max(per_key.values(), default=0)
    print(json.dumps(report, indent=1))
    ok = report["compared"] and not report["missing"] and not report["mismatches"]
    sys.exit(0 if ok else 1)
