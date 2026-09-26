"""Pluggable notification channels. A failing channel is logged and never blocks the others."""

import json
import urllib.request
from typing import Protocol

import structlog

from common.config import Settings
from common.schemas import Alert

HTTP_TIMEOUT_S = 5


class Notifier(Protocol):
    name: str

    def notify(self, alert: Alert) -> None: ...


class ConsoleNotifier:
    name = "console"

    def __init__(self, log: structlog.stdlib.BoundLogger) -> None:
        self._log = log

    def notify(self, alert: Alert) -> None:
        self._log.warning(
            "ALERT",
            kind=alert.kind.value,
            state=alert.state.value,
            lot_id=alert.lot_id,
            sensor_id=alert.sensor_id,
            alert_message=alert.message,
        )


def _post_json(url: str, payload: dict[str, object]) -> None:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=HTTP_TIMEOUT_S) as response:  # noqa: S310
        response.read()


class WebhookNotifier:
    name = "webhook"

    def __init__(self, url: str) -> None:
        self._url = url

    def notify(self, alert: Alert) -> None:
        _post_json(self._url, json.loads(alert.model_dump_json()))


class TelegramNotifier:
    name = "telegram"

    def __init__(self, bot_token: str, chat_id: str) -> None:
        self._url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._chat_id = chat_id

    def notify(self, alert: Alert) -> None:
        prefix = "RAISED" if alert.state.value == "RAISED" else "CLEARED"
        _post_json(self._url, {"chat_id": self._chat_id, "text": f"[{prefix}] {alert.message}"})


def build_notifiers(settings: Settings, log: structlog.stdlib.BoundLogger) -> list[Notifier]:
    notifiers: list[Notifier] = [ConsoleNotifier(log)]
    if settings.alert_webhook_url:
        notifiers.append(WebhookNotifier(settings.alert_webhook_url))
    if settings.telegram_bot_token and settings.telegram_chat_id:
        notifiers.append(TelegramNotifier(settings.telegram_bot_token, settings.telegram_chat_id))
    return notifiers


def dispatch(alert: Alert, notifiers: list[Notifier], log: structlog.stdlib.BoundLogger) -> int:
    """Send to every channel; returns how many failed."""
    failures = 0
    for notifier in notifiers:
        try:
            notifier.notify(alert)
        except Exception as exc:  # a broken channel must not stop the others
            failures += 1
            log.error("notification failed", channel=notifier.name, error=str(exc)[:200])
    return failures
