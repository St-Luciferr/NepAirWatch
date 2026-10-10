# NepAirWatch — Real-time Air Quality Monitoring & Early Warning for Nepal

A big-data platform built on **Apache Kafka** and **Apache Spark** (Structured Streaming + Spark SQL + MLlib)
that ingests hourly air-quality and weather data for 15 cities across all 7 provinces of Nepal. It computes
AQI and health alerts in real time, stores the history in a partitioned Parquet data lake plus a MongoDB
serving layer, measures exposure against Nepal's national standard and the WHO guideline, and forecasts
PM2.5 six hours ahead.

![architecture](docs/architecture.png)

## 1. Problem

Every winter, Kathmandu Valley and the Terai rank among the most polluted places in the world. PM2.5 often
exceeds Nepal's National Ambient Air Quality Standard (40 µg/m³, 24 h) and is several times the WHO 2021
guideline (15 µg/m³). The causes are brick kilns, vehicle exhaust, open burning, forest fires, cross-border
haze from the Indo-Gangetic Plain, and the valley's bowl shape. Air pollution is one of the leading risk
factors for death in Nepal. Yet:

* Ground monitoring stations are few, mostly in the Kathmandu Valley, and frequently offline.
* Data is published as raw numbers without health advice in Nepali.
* Nothing warns citizens, schools or hospitals **before** a pollution episode.
* Historical data is not aggregated to show *where* and *when* standards are breached, which is the
  evidence policy-makers need.

**NepAirWatch** gives continuous nationwide coverage with streaming ingestion, real-time AQI and alerts,
bilingual advisories, historical analytics and an ML early-warning forecast.

## 2. Data sources

| Source | What | Access |
| --- | --- | --- |
| Open-Meteo Air-Quality API (Copernicus CAMS global model) | PM2.5, PM10, CO, NO₂, SO₂, O₃, dust (hourly) | `air-quality-api.open-meteo.com` (free, no key) |
| Open-Meteo Weather / Historical-Forecast API | temperature, humidity, wind, precipitation, boundary-layer height | `historical-forecast-api.open-meteo.com`, `api.open-meteo.com` |
| *Extension:* DoEnv Nepal (pollution.gov.np) ground stations, IoT low-cost sensors, NASA FIRMS fire hotspots | ground truth / extra features | same Kafka topic, same JSON schema |

Volume used in the demo: 15 cities × 24 h × 366 days ≈ **132,000 messages** (≈ 46 MB Parquet, 5,500+ files).
The design scales horizontally: more Kafka partitions and more Spark executors.

## 3. Big-data components

| Layer | Component | Role |
| --- | --- | --- |
| Ingestion | **Apache Kafka 3.8** (KRaft) | Durable, replayable, partitioned message bus (`nepal.air.raw`, `nepal.air.alerts`) |
| Processing | **Apache Spark 3.5 Structured Streaming** | Parsing, validation, AQI enrichment, event-time windows with watermark, alert detection |
| Processing | **Spark SQL + MLlib** (batch) | Exceedance statistics, seasonality, weather correlation, GBT forecasting model |
| Storage | **Parquet data lake** (HDFS/S3-compatible) | Immutable history partitioned by `city/date` + curated zone |
| Storage | **MongoDB 7** | Low-latency serving layer for the dashboard |
| Presentation | Streamlit + Plotly, Kafka-UI | Dashboard, ops console |

## 4. Project layout

```text
common/            config (cities, thresholds), AQI calculation & bilingual advisories
producer/          aq_producer.py — Kafka producer (backfill | live | replay)
spark_jobs/        stream_processor.py — streaming job; batch_analytics.py — batch + MLlib
dashboard/         app.py — Streamlit dashboard
docker/            container entry scripts (backfill once, batch loop)
scripts/           local-mode scripts, wait_for_lake.py, report/deck/diagram generators
Dockerfile         app image: Python 3.11 + Java 17 + PySpark + Kafka connector
docker-compose.yml the whole platform: infra + producers + Spark jobs + dashboard
docs/              architecture diagram, screenshots, report, slides
data/              (generated) landing zone, Parquet lake, checkpoints, models, logs
```

## 5. Implementation guide

**Prerequisites:** Docker with Compose v2, internet access, about 6 GB free RAM. No Python or Java is needed on
the host, because everything runs in containers.

```bash
docker compose up -d --build      # first time: builds the app image (~3 min), then starts everything
```

On the first start the containers automatically:

1. start Kafka (KRaft), Kafka-UI and MongoDB, and create the topics (`kafka-init`)
2. back-fill 365 days of history for 15 cities into Kafka (`backfill`, one-shot, about 2 min)
3. stream Kafka → Spark → Parquet lake + MongoDB + alerts (`spark-streaming`, Spark UI on :4040)
4. run Spark batch analytics + MLlib training when the lake is ready, then every 6 h (`spark-batch`, ~3 min)
5. poll **live** data every 15 min (`producer-live`)
6. serve the dashboard (`dashboard`)

| URL | What |
| --- | --- |
| <http://localhost:8501> | NepAirWatch dashboard |
| <http://localhost:8080> | Kafka-UI (topics, partitions, messages) |
| <http://localhost:4040> | Spark UI of the streaming job |

The analytics and forecast tabs fill in about 5–7 minutes after the first start. Later starts skip the
back-fill and are ready in under a minute.

```bash
docker compose ps                              # status of all services
docker compose logs -f spark-streaming         # watch Spark micro-batches
docker compose run --rm replay                 # DEMO: re-stream last 2 days, 1 hour every 2 s
REPLAY_SPEED=1 docker compose run --rm replay  # faster replay
docker compose stop                            # stop (data is kept)
docker compose down -v                         # stop and delete all data (next start = fresh back-fill)
```

Settings (environment variables or a `.env` file): `BACKFILL_DAYS` (365), `LIVE_INTERVAL` (900 s),
`BATCH_INTERVAL` (21600 s), `REPLAY_DAYS` (2), `REPLAY_SPEED` (2 s).

### Running without Docker for the app (local development)

With Python 3.11 and Java 17 on the host: run `./scripts/setup.sh`, then `./scripts/run_pipeline.sh`. Stop the
app containers first (`docker compose stop backfill producer-live spark-streaming spark-batch dashboard`) so
that two streaming jobs don't share state.

## 6. Demo script (≈10 min)

1. **Problem.** Show the Analytics tab: days above the Nepal and WHO limits per city, and the seasonality
   heatmap.
2. **Architecture.** Walk through `docs/architecture.png`.
3. **Kafka.** Open Kafka-UI (localhost:8080) and show topic `nepal.air.raw`: 3 partitions, keyed by city,
   with a live message.
4. **Streaming.** Run `docker compose run --rm replay`. On the dashboard's Live tab the throughput bars move
   every 5 s and alerts appear. `docker compose logs -f spark-streaming` shows the micro-batches.
5. **Alerts fan-out.** In Kafka-UI, topic `nepal.air.alerts` fills up. Any SMS or app service can subscribe.
6. **City explorer.** Kathmandu PM2.5 against the standards, and the boundary-layer-height chart showing why
   night-time inversions trap smoke.
7. **Early warning.** The GBT model beats the persistence baseline (RMSE ≈10 vs ≈17 µg/m³). Show the
   6-hour forecast table.
8. **Pipeline health.** Kafka offsets, MongoDB collections, Parquet lake size.

## 7. Key design decisions

* **Kafka keyed by city** keeps per-city ordering inside a partition and lets consumers scale out.
* **Event-time processing.** The daily window uses the observation time, not the arrival time, aligned to
  Nepal time (UTC+5:45). A 3-hour watermark tolerates late sensor data.
* **Idempotent sinks.** MongoDB writes are upserts on `(city, event_ts)`, and the batch job de-duplicates
  the lake, so replays and restarts never double-count. Spark checkpoints give exactly-once offset tracking.
* **AQI as a native Spark expression**, not a Python UDF, so it runs inside the JVM.
* **Time-based train/test split** for the forecaster. A random split would leak future data.

## 8. Limitations and future work

* CAMS is a ~40 km global model. Kathmandu, Lalitpur and Hetauda fall in the same grid cell, so their values
  are identical. Fusing DoEnv ground stations and low-cost sensors would fix this.
* Single-node deployment. In production, Kafka would get 3 brokers with replication factor 3, Spark would
  run on YARN or Kubernetes, and the lake would live on HDFS or S3.
* Planned extensions: SMS/Viber alert service subscribed to `nepal.air.alerts`, NASA FIRMS fire hotspots as
  features, and Parquet compaction for the small-files problem.
