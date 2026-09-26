"""Environment-driven configuration. Every tunable lives in .env.example."""

import os
from dataclasses import dataclass


def _f(name: str, default: str) -> float:
    return float(os.environ.get(name, default))


def _s(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str
    window_size_s: int
    allowed_lateness_s: float
    window_grace_s: float
    dedupe_ttl_s: float
    emit_interval_s: float
    checkpoint_interval_s: float
    sensor_offline_after_s: float
    metrics_port: int
    alert_full_threshold: float
    alert_clear_threshold: float
    alert_webhook_url: str
    telegram_bot_token: str
    telegram_chat_id: str
    postgres_dsn: str

    @classmethod
    def from_env(cls) -> "Settings":
        # Host-side tools (simulator, chaos scripts) set KAFKA_BOOTSTRAP_HOST.
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP_HOST") or _s(
            "KAFKA_BOOTSTRAP", "localhost:19092"
        )
        dsn = (
            f"host={_s('POSTGRES_HOST', 'localhost')} port={_s('POSTGRES_PORT', '5432')} "
            f"dbname={_s('POSTGRES_DB', 'parking')} user={_s('POSTGRES_USER', 'parking')} "
            f"password={_s('POSTGRES_PASSWORD', 'change-me')}"
        )
        return cls(
            kafka_bootstrap=bootstrap,
            window_size_s=int(_f("WINDOW_SIZE_S", "300")),
            allowed_lateness_s=_f("ALLOWED_LATENESS_S", "60"),
            window_grace_s=_f("WINDOW_GRACE_S", "120"),
            dedupe_ttl_s=_f("DEDUPE_TTL_S", "300"),
            emit_interval_s=_f("EMIT_INTERVAL_S", "5"),
            checkpoint_interval_s=_f("CHECKPOINT_INTERVAL_S", "10"),
            sensor_offline_after_s=_f("SENSOR_OFFLINE_AFTER_S", "120"),
            metrics_port=int(_f("PROCESSOR_METRICS_PORT", "8000")),
            alert_full_threshold=_f("ALERT_FULL_THRESHOLD", "0.90"),
            alert_clear_threshold=_f("ALERT_CLEAR_THRESHOLD", "0.85"),
            alert_webhook_url=_s("ALERT_WEBHOOK_URL", ""),
            telegram_bot_token=_s("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=_s("TELEGRAM_CHAT_ID", ""),
            postgres_dsn=dsn,
        )
