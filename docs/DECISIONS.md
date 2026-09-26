# Decisions

ADR-style log. Decisions already fixed in `CLAUDE.md` (Python 3.12, confluent-kafka, own windowing,
3-broker KRaft, TimescaleDB) are not repeated here.

## D1. One Python project, one Docker image for all backend services
- **Decision:** a single `pyproject.toml` at the repo root with packages `common`, `simulator`, `processor`, `sink`, `alerter`; one Dockerfile parameterised by the module to run.
- **Alternatives:** a uv workspace with a package per service.
- **Why:** services share `common/` heavily; one lockfile and one CI type-check pass is simpler to explain and faster to build.

## D2. Kafka image `apache/kafka:3.9.0`
- **Why:** official image, KRaft-native, stable. Version is pinned so the demo is reproducible.

## D3. Two listeners per broker
- `INTERNAL` (`kafka-N:9092`) for containers and inter-broker traffic; `EXTERNAL` (`localhost:N9092`) so host tools (simulator, chaos scripts) can connect. Set `KAFKA_BOOTSTRAP_HOST` to use the latter.

## D4. Configuration via a frozen dataclass reading environment variables
- **Alternative:** `pydantic-settings`. **Why not:** one more dependency for ~15 variables.

## D5. Event validation rules go beyond field types
- `OCCUPIED` requires `vehicle_token`, `FREE` forbids it, `HEARTBEAT` carries neither; timestamps must be timezone-aware. Inconsistent events go to the DLQ rather than corrupting state.
