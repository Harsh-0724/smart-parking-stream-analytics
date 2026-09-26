"""Brings up the full Compose stack with testcontainers (or reuses one that is already running).

If `make up` is already running the stack, the tests use it and leave it alone. Otherwise the
fixture starts every service, waits for health, and tears everything down (with volumes) at the end.
"""

import os
import shutil
import socket
from collections.abc import Iterator
from pathlib import Path

import pytest
from testcontainers.compose import DockerCompose

ROOT = Path(__file__).resolve().parents[2]
BOOTSTRAP = "localhost:19092,localhost:29092,localhost:39092"
SERVICES = [
    "kafka-1", "kafka-2", "kafka-3", "init", "timescaledb",
    "processor", "sink", "alerter", "api", "kafka-exporter", "prometheus",
]  # fmt: skip


def _reachable(port: int) -> bool:
    try:
        with socket.create_connection(("localhost", port), timeout=1):
            return True
    except OSError:
        return False


def _read_env() -> dict[str, str]:
    env_file = ROOT / ".env"
    if not env_file.exists():
        shutil.copy(ROOT / ".env.example", env_file)
    values = {}
    for line in env_file.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            values[key] = value
    return values


@pytest.fixture(scope="session")
def stack(request: pytest.FixtureRequest) -> Iterator[dict[str, str]]:
    env = _read_env()
    if _reachable(19092) and _reachable(5432):
        yield env
        return
    compose = DockerCompose(
        ROOT,
        compose_file_name="docker-compose.yml",
        env_file=str(ROOT / ".env"),
        services=SERVICES,
        build=True,
        wait=True,
    )
    compose.start()
    try:
        yield env
    finally:
        if request.session.testsfailed:  # the containers are about to be removed: keep the evidence
            stdout, stderr = compose.get_logs()
            print("=== compose logs (tail) ===")
            print("\n".join((stdout + stderr).splitlines()[-400:]))
        compose.stop()  # `down --volumes`


@pytest.fixture(scope="session")
def bootstrap(stack: dict[str, str]) -> str:
    os.environ["KAFKA_BOOTSTRAP_HOST"] = BOOTSTRAP
    return BOOTSTRAP


@pytest.fixture(scope="session")
def dsn(stack: dict[str, str]) -> str:
    return (
        f"host=localhost port=5432 dbname={stack['POSTGRES_DB']} "
        f"user={stack['POSTGRES_USER']} password={stack['POSTGRES_PASSWORD']}"
    )
