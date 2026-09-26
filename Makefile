.PHONY: up down clean topics lint type test test-integration fmt sync fetch-data reset-topics

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

fetch-data:
	mkdir -p data
	curl -sL -o data/birmingham.zip https://archive.ics.uci.edu/static/public/482/parking+birmingham.zip
	unzip -o -q data/birmingham.zip -d data

# Delete and recreate every topic (drops all messages, keeps the cluster).
reset-topics:
	@for t in parking.raw parking.dlq parking.late lot.metadata lot.occupancy.5min lot.alerts state.changelog; do \
	  $(COMPOSE) exec -T kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:9092 --delete --topic $$t >/dev/null 2>&1 || true; done
	@sleep 5
	$(COMPOSE) up init
