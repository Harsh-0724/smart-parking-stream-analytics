.PHONY: up down clean topics lint type test test-integration fmt sync

COMPOSE ?= docker compose

sync:
	uv sync

up:
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --wait --remove-orphans

down:
	$(COMPOSE) down

clean:
	$(COMPOSE) down -v --remove-orphans

topics:
	$(COMPOSE) exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --describe

lint:
	uv run ruff check .
	uv run ruff format --check .

fmt:
	uv run ruff check --fix .
	uv run ruff format .

type:
	uv run mypy common processor simulator sink alerter

test:
	uv run pytest

test-integration:
	uv run pytest -m integration
