.PHONY: up down clean topics lint type test test-integration fmt sync fetch-data reset-topics chaos chaos-broker chaos-processor chaos-sink chaos-bad-data chaos-late-dup chaos-lag hll-accuracy demo-reset openapi

COMPOSE ?= docker compose
# Host-side tools reach the brokers through the EXTERNAL listeners.
export KAFKA_BOOTSTRAP_HOST ?= localhost:19092,localhost:29092,localhost:39092
export PYTHONPATH := .

sync:
	uv sync

up:
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build --wait --remove-orphans

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
	uv run mypy common processor simulator sink alerter api

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

chaos-broker chaos-processor chaos-sink chaos-bad-data chaos-late-dup chaos-lag: chaos-%:
	uv run python scripts/chaos_$(subst -,_,$*).py

chaos: chaos-broker chaos-processor chaos-sink chaos-bad-data chaos-late-dup chaos-lag

hll-accuracy:
	uv run python scripts/hll_accuracy.py

# Event-time state must not survive between runs that use different simulator start times:
# a stale watermark would classify every new event as late. Run this before every demo.
demo-reset:
	$(MAKE) reset-topics
	$(COMPOSE) exec -T timescaledb psql -U $${POSTGRES_USER:-parking} -d $${POSTGRES_DB:-parking} -qc "truncate lot_occupancy_5min, alerts, lot_metadata"
	$(COMPOSE) up -d --build --wait --remove-orphans
	$(COMPOSE) restart api

# Frontend API types are generated from the FastAPI schema (committed: web/openapi.json).
openapi:
	uv run python -c "import json; from api.main import app; print(json.dumps(app.openapi(), indent=1, sort_keys=True))" > web/openapi.json
	cd web && pnpm gen:types
