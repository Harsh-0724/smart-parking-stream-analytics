.PHONY: up down clean topics lint type test test-integration fmt sync fetch-data reset-topics

COMPOSE ?= docker compose
# Host-side tools reach the brokers through the EXTERNAL listeners.
export KAFKA_BOOTSTRAP_HOST ?= localhost:19092,localhost:29092,localhost:39092
export PYTHONPATH := .

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

fetch-data:
	mkdir -p data
	curl -sL -o data/birmingham.zip https://archive.ics.uci.edu/static/public/482/parking+birmingham.zip
	unzip -o -q data/birmingham.zip -d data

# Delete and recreate every topic (drops all messages, keeps the cluster).
reset-topics:
	@$(COMPOSE) stop processor sink alerter simulator >/dev/null 2>&1 || true
	@for t in parking.raw parking.dlq parking.late lot.metadata lot.occupancy.5min lot.alerts state.changelog; do \
	  $(COMPOSE) exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic $$t >/dev/null 2>&1 || true; done
	@sleep 5
	$(COMPOSE) up init
