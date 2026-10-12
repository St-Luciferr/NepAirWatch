-- NepAirWatch — Apache Hive: SQL tables over the HDFS data lake (schema-on-read, EXTERNAL tables:
-- dropping a table never deletes the data that Spark / Flume / MapReduce wrote).
SET hive.msck.path.validation=ignore;

CREATE DATABASE IF NOT EXISTS nepairwatch;
USE nepairwatch;

-- Spark streaming: hourly CAMS air quality + weather, Parquet partitioned by city/date
CREATE EXTERNAL TABLE IF NOT EXISTS air_quality (
  province STRING, lat DOUBLE, lon DOUBLE, event_ts TIMESTAMP,
  pm2_5 DOUBLE, pm10 DOUBLE, co DOUBLE, no2 DOUBLE, so2 DOUBLE, o3 DOUBLE, dust DOUBLE,
  temperature DOUBLE, humidity DOUBLE, wind_speed DOUBLE, precipitation DOUBLE,
  boundary_layer_height DOUBLE, aqi INT, category STRING, source STRING)
PARTITIONED BY (city STRING, `date` DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nepairwatch/lake/air_quality';

-- Spark streaming: 1-minute ground measurements (DoEnv + Sensor.Community)
CREATE EXTERNAL TABLE IF NOT EXISTS ground (
  station_id STRING, station STRING, lat DOUBLE, lon DOUBLE, city STRING, event_ts TIMESTAMP,
  pm1 DOUBLE, pm2_5 DOUBLE, pm10 DOUBLE, aqi INT, source STRING)
PARTITIONED BY (network STRING, `date` DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nepairwatch/lake/ground';

-- Spark streaming: FIRMS fire hotspots with nearest city
CREATE EXTERNAL TABLE IF NOT EXISTS fires (
  fire_id STRING, latitude DOUBLE, longitude DOUBLE, acq_ts TIMESTAMP, satellite STRING,
  instrument STRING, confidence STRING, frp DOUBLE, brightness DOUBLE, daynight STRING, source STRING,
  nearest_city STRING, city_km DOUBLE)
PARTITIONED BY (acq_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nepairwatch/lake/fires';

-- Flume HDFS sink: raw FIRMS CSV lines exactly as ingested (OpenCSVSerde reads every column as STRING)
CREATE EXTERNAL TABLE IF NOT EXISTS fires_raw (
  fire_id STRING, latitude STRING, longitude STRING, acq_ts STRING, satellite STRING,
  instrument STRING, confidence STRING, frp STRING, brightness STRING, daynight STRING, source STRING)
PARTITIONED BY (dt STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/nepairwatch/raw/firms';

-- power-loader: 25 years of NASA POWER daily data (JSON lines)
CREATE EXTERNAL TABLE IF NOT EXISTS nasa_power (
  city STRING, province STRING, lat DOUBLE, lon DOUBLE, `date` STRING,
  aod_55 DOUBLE, t2m DOUBLE, rh2m DOUBLE, ws10m DOUBLE, precip DOUBLE)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.JsonSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/nepairwatch/raw/nasa_power';

-- Hadoop MapReduce output (mapreduce/aod_reducer.py), tab-separated
CREATE EXTERNAL TABLE IF NOT EXISTS aod_yearly_mr (
  city STRING, yr INT, days INT, aod_mean DOUBLE, aod_max DOUBLE,
  hazy_days INT, very_hazy_days INT, t2m_mean DOUBLE, precip_total DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/nepairwatch/curated/aod_yearly_mr';

-- Spark batch: curated zone
CREATE EXTERNAL TABLE IF NOT EXISTS exceedance (
  city STRING, province STRING, days BIGINT, annual_mean DOUBLE, days_over_nepal_std BIGINT,
  days_over_who BIGINT, worst_day_pm25 DOUBLE, pct_days_over_nepal_std DOUBLE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nepairwatch/lake/curated/exceedance';

CREATE EXTERNAL TABLE IF NOT EXISTS ground_hourly (
  station_id STRING, network STRING, station STRING, hour TIMESTAMP, pm2_5 DOUBLE, pm10 DOUBLE,
  pm2_5_max DOUBLE, n_obs INT, aqi INT)
PARTITIONED BY (city STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/nepairwatch/lake/curated/ground_hourly';

MSCK REPAIR TABLE air_quality SYNC PARTITIONS;
MSCK REPAIR TABLE ground SYNC PARTITIONS;
MSCK REPAIR TABLE fires SYNC PARTITIONS;
MSCK REPAIR TABLE fires_raw SYNC PARTITIONS;
MSCK REPAIR TABLE ground_hourly SYNC PARTITIONS;
SHOW TABLES;
