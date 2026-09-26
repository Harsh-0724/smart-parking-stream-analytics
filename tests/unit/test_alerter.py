import json
import socketserver
import threading
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, HTTPServer

import structlog

from alerter.hysteresis import FullLotDetector
from alerter.notifiers import ConsoleNotifier, WebhookNotifier, dispatch
from common.schemas import Alert, AlertKind, AlertState, WindowResult

T0 = datetime(2026, 1, 5, 10, 0, tzinfo=UTC).timestamp()


def result(
    occupied: int | None, t: float, closed: bool = False, capacity: int = 100
) -> WindowResult:
    at = datetime.fromtimestamp(T0 + t, UTC)
    return WindowResult(
        lot_id="LOT-01",
        window_start=at,
        window_end=at,
        capacity=capacity,
        avg_occupied=1,
        avg_occupancy_pct=1,
        min_occupied=0,
        max_occupied=1,
        entries=0,
        exits=0,
        unique_vehicles_est=0,
        closed=closed,
        current_occupied=occupied,
        emitted_at=at,
    )


def states(detector: FullLotDetector, series: list[int]) -> list[str | None]:
    out = []
    for i, occupied in enumerate(series):
        alert = detector.observe(result(occupied, i))
        out.append(alert.state.value if alert else None)
    return out


def test_raises_at_90_and_clears_only_at_85() -> None:
    d = FullLotDetector(0.90, 0.85)
    out = states(d, [80, 89, 90, 88, 86, 87, 85, 84])
    assert out == [None, None, "RAISED", None, None, None, "CLEARED", None]


def test_hovering_between_thresholds_never_flaps() -> None:
    d = FullLotDetector(0.90, 0.85)
    out = states(d, [90, 89, 90, 88, 91, 86, 90, 87])
    assert out.count("RAISED") == 1 and "CLEARED" not in out


def test_can_raise_again_after_clearing_with_a_new_alert_id() -> None:
    d = FullLotDetector(0.90, 0.85)
    alerts = [a for i, n in enumerate([95, 80, 95]) if (a := d.observe(result(n, i * 10)))]
    assert [a.state for a in alerts] == [AlertState.RAISED, AlertState.CLEARED, AlertState.RAISED]
    assert alerts[0].alert_id == alerts[1].alert_id != alerts[2].alert_id


def test_closed_and_stale_results_are_ignored() -> None:
    d = FullLotDetector(0.90, 0.85)
    assert d.observe(result(None, 0, closed=True)) is None
    assert d.observe(result(95, 10)) is not None
    assert d.observe(result(50, 5)) is None  # older emission arriving late must not clear
    assert d.is_active("LOT-01")


def test_restore_keeps_alert_active_without_reraising() -> None:
    first = FullLotDetector(0.90, 0.85)
    raised = first.observe(result(95, 0))
    assert raised is not None

    restarted = FullLotDetector(0.90, 0.85)
    restarted.restore([raised])
    assert restarted.observe(result(96, 10)) is None  # already active, no duplicate
    cleared = restarted.observe(result(70, 20))
    assert cleared is not None and cleared.alert_id == raised.alert_id


def test_restore_after_clear_is_inactive() -> None:
    d = FullLotDetector(0.90, 0.85)
    raised, cleared = d.observe(result(95, 0)), d.observe(result(70, 5))
    assert raised and cleared
    fresh = FullLotDetector(0.90, 0.85)
    fresh.restore([raised, cleared])
    assert not fresh.is_active("LOT-01")


def _alert() -> Alert:
    return Alert(
        alert_id="a1",
        kind=AlertKind.SENSOR_OFFLINE,
        state=AlertState.RAISED,
        lot_id="LOT-01",
        sensor_id="S-A-014",
        ts=datetime(2026, 1, 5, tzinfo=UTC),
        message="Sensor S-A-014 silent for 6 min",
    )


def test_webhook_receives_alert_json() -> None:
    received: list[dict[str, object]] = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            received.append(json.loads(self.rfile.read(int(self.headers["Content-Length"]))))
            self.send_response(204)
            self.end_headers()

        def log_message(self, *args: object) -> None:
            pass

    class QuickServer(HTTPServer):
        def server_bind(self) -> None:  # HTTPServer's own bind does a slow reverse-DNS lookup
            socketserver.TCPServer.server_bind(self)
            self.server_port = self.socket.getsockname()[1]

    server = QuickServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log = structlog.get_logger()
    failures = dispatch(_alert(), [WebhookNotifier(f"http://127.0.0.1:{server.server_port}/")], log)
    server.shutdown()
    assert failures == 0
    assert received[0]["alert_id"] == "a1" and received[0]["sensor_id"] == "S-A-014"


def test_failing_channel_does_not_block_the_others() -> None:
    log = structlog.get_logger()
    calls: list[str] = []

    class Boom:
        name = "boom"

        def notify(self, alert: Alert) -> None:
            raise OSError("down")

    class Ok:
        name = "ok"

        def notify(self, alert: Alert) -> None:
            calls.append(alert.alert_id)

    assert dispatch(_alert(), [Boom(), Ok(), ConsoleNotifier(log)], log) == 1
    assert calls == ["a1"]
