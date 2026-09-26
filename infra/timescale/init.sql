-- Runs once, on first start of an empty data volume.
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE lot_metadata (
    lot_id      text PRIMARY KEY,
    name        text NOT NULL,
    personality text NOT NULL,
    capacity    integer NOT NULL,
    columns     integer NOT NULL,
    latitude    double precision NOT NULL,
    longitude   double precision NOT NULL,
    slot_ids    jsonb NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- One row per (lot, 5-minute window). The stream processor may emit a window many times
-- (open updates, then a closed result, and re-emissions after a recovery); the sink upserts
-- on the primary key, so the table always holds exactly one, latest, row per window.
CREATE TABLE lot_occupancy_5min (
    lot_id              text NOT NULL,
    window_start        timestamptz NOT NULL,
    window_end          timestamptz NOT NULL,
    capacity            integer NOT NULL,
    avg_occupied        double precision NOT NULL,
    avg_occupancy_pct   double precision NOT NULL,
    min_occupied        integer NOT NULL,
    max_occupied        integer NOT NULL,
    entries             integer NOT NULL,
    exits               integer NOT NULL,
    unique_vehicles_est double precision NOT NULL,
    closed              boolean NOT NULL,
    current_occupied    integer,
    emitted_at          timestamptz NOT NULL,
    latest_ingest_ts    timestamptz,
    PRIMARY KEY (lot_id, window_start)
);
SELECT create_hypertable('lot_occupancy_5min', 'window_start', chunk_time_interval => INTERVAL '1 day');

ALTER TABLE lot_occupancy_5min SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'lot_id',
    timescaledb.compress_orderby = 'window_start DESC'
);
SELECT add_compression_policy('lot_occupancy_5min', INTERVAL '7 days');
SELECT add_retention_policy('lot_occupancy_5min', INTERVAL '90 days');

-- One row per alert; RAISED and CLEARED messages share an alert_id.
CREATE TABLE alerts (
    alert_id       text PRIMARY KEY,
    kind           text NOT NULL,
    lot_id         text NOT NULL,
    sensor_id      text,
    status         text NOT NULL CHECK (status IN ('active', 'cleared')),
    raised_at      timestamptz NOT NULL,
    cleared_at     timestamptz,
    message        text NOT NULL,
    occupancy_pct  double precision,
    updated_at     timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX alerts_raised_at_idx ON alerts (raised_at DESC);
CREATE INDEX alerts_lot_idx ON alerts (lot_id, raised_at DESC);
CREATE INDEX alerts_active_idx ON alerts (status) WHERE status = 'active';

-- Hourly rollup of closed windows; the most recent hour is computed at query time.
CREATE MATERIALIZED VIEW lot_occupancy_hourly
WITH (timescaledb.continuous, timescaledb.materialized_only = false) AS
SELECT
    lot_id,
    time_bucket(INTERVAL '1 hour', window_start) AS bucket,
    avg(avg_occupied)      AS avg_occupied,
    avg(avg_occupancy_pct) AS avg_occupancy_pct,
    min(min_occupied)      AS min_occupied,
    max(max_occupied)      AS max_occupied,
    sum(entries)           AS entries,
    sum(exits)             AS exits,
    count(*)               AS windows
FROM lot_occupancy_5min
WHERE closed
GROUP BY lot_id, bucket
WITH NO DATA;

SELECT add_continuous_aggregate_policy(
    'lot_occupancy_hourly',
    start_offset => INTERVAL '3 hours',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '5 minutes'
);
