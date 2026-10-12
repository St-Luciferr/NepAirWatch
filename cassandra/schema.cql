-- NepAirWatch — Apache Cassandra schema (write-optimised time-series store for ground sensors)
--
-- Query-first design: the dashboard asks "readings of station S on day D" and "hourly means of
-- station S for the last N days". The partition key bounds partition size (one station-day ≈ 1,440
-- rows); the clustering key keeps rows time-ordered on disk, newest first.

CREATE KEYSPACE IF NOT EXISTS nepairwatch
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};  -- 3 in production

USE nepairwatch;

-- 1-minute readings (speed layer, written by Spark Structured Streaming)
CREATE TABLE IF NOT EXISTS ground_readings (
  station_id text,
  day        date,
  event_ts   timestamp,
  network    text,
  station    text,
  city       text,
  lat        double,
  lon        double,
  pm1        double,
  pm2_5      double,
  pm10       double,
  aqi        int,
  source     text,
  PRIMARY KEY ((station_id, day), event_ts)
) WITH CLUSTERING ORDER BY (event_ts DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy',
                    'compaction_window_unit': 'DAYS', 'compaction_window_size': 1};

-- hourly means per station: written by the stream (event-time window) and re-computed by the
-- Spark batch job from the lake (Lambda: the batch view overwrites the speed view — same key)
CREATE TABLE IF NOT EXISTS ground_hourly (
  station_id text,
  hour       timestamp,
  network    text,
  station    text,
  city       text,
  pm2_5      double,
  pm10       double,
  pm2_5_max  double,
  n_obs      int,
  aqi        int,
  layer      text,          -- 'speed' or 'batch'
  PRIMARY KEY ((station_id), hour)
) WITH CLUSTERING ORDER BY (hour DESC);
