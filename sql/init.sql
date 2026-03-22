-- Initializes Postgres schema for trial/promo abuse detection

CREATE TABLE IF NOT EXISTS signups (
  id BIGSERIAL PRIMARY KEY,
  event       TEXT NOT NULL,
  user_id     TEXT NOT NULL,
  device_id   TEXT NOT NULL,
  ts          TIMESTAMP NOT NULL,
  label_abusive BOOLEAN,
  raw_json    JSONB NOT NULL,
  ingested_at TIMESTAMP DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_signups_device_ts ON signups(device_id, ts);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGSERIAL PRIMARY KEY,
  device_id     TEXT NOT NULL,
  window_start  TIMESTAMP NOT NULL,
  window_end    TIMESTAMP NOT NULL,
  signup_count  INTEGER NOT NULL,
  threshold     INTEGER NOT NULL,
  first_seen_ts TIMESTAMP,
  last_seen_ts  TIMESTAMP,
  created_at    TIMESTAMP DEFAULT now(),
  -- Deduplication constraint: prevent duplicate alerts for same device + time window
  UNIQUE (device_id, window_start, window_end)
);

CREATE INDEX IF NOT EXISTS idx_alerts_device_ws ON alerts(device_id, window_start, window_end);
