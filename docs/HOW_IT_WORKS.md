# NepAirWatch: how it works, in plain words

## 1. The idea in one paragraph

Nepal's air is measured in many places, but the pieces are scattered and nobody connects them: a
weather model, government stations, citizen sensors, satellites that see fires and haze, and the news.
NepAirWatch **collects all of them continuously, stores everything, reacts instantly to bad air, and studies
the history** to explain *why* the air is bad and *what will happen next*. Then it shows the results on one
dashboard, with advice in English and Nepali.

Think of it as a **newsroom for air quality**:

| Newsroom | NepAirWatch | Tool |
| --- | --- | --- |
| Reporters in the field | data collectors (producers) | Python, Flume |
| The wire that delivers every story | message bus | Kafka |
| Breaking-news desk (fast, rough) | speed layer | Spark Structured Streaming |
| Archive of every story ever filed | data lake | HDFS |
| Investigative team (slow, thorough) | batch layer | Spark batch + MLlib, MapReduce, Hive |
| Fast lookup shelves | serving databases | Cassandra, MongoDB |
| The front page | dashboard | Streamlit |

---

## 2. Where the data comes from

| Source | What it really is | Why we need it | How often |
| --- | --- | --- | --- |
| **Open-Meteo (CAMS)** | a *computer model* of air + weather for 15 cities | covers every city, includes weather (wind, mixing height) | hourly, plus 1 year of history |
| **DoEnv Nepal** (`pollution.gov.np`) | ~30 *real government monitors* | ground truth, every **minute** | live, plus ~3 weeks of history (the API keeps only ~3 weeks) |
| **Sensor.Community** | cheap citizen sensors | extra eyes where there are no stations | ~5 min |
| **NASA FIRMS** | satellites spotting **fires** | forest fires and crop burning cause smoke episodes | every ~3 h |
| **NASA POWER** | satellite **haze** (aerosol optical depth, "AOD") since 2001 | 25-year trend: is it getting worse? | one bulk load |
| **Google News** | headlines | does the public notice? what topics dominate? | every 30 min |

The key tension: **the model covers everywhere but can be wrong; the stations are right but cover few
places.** The project uses the stations to *correct* the model (section 5).

---

## 3. Follow one reading through the system

A PM2.5 value of **85 µg/m³** is measured at the **Pulchowk** station at 08:01.

```text
 pollution.gov.np ──(1) ground_producer.py polls every 10 min, keeps only new minutes
        │
        ▼
 Kafka topic nepal.ground.raw ──(2) stored durably, keyed by station, can be replayed
        │
        ▼
 Spark Structured Streaming ──(3) parse JSON → compute AQI (85 → AQI 172 "Unhealthy")
        ├─► HDFS lake/ground/network=doenv/date=…/     (4) permanent copy (Parquet)
        ├─► Cassandra ground_readings                   (5) fast "show me Pulchowk today"
        ├─► MongoDB ground_latest                        (6) the live map dot turns red
        └─► 1-hour window per station                    (7) when the hour closes, mean ≥ 151?
                 └─► alert → MongoDB alerts + Kafka nepal.air.alerts  (an SMS service could subscribe)

 Every 6 hours, the batch layer:
 HDFS lake ──(8) Spark recomputes the exact hourly means from ALL data (including late arrivals)
           ──(9) compares Pulchowk with the CAMS model for Lalitpur → e.g. "the model reads too low here"
           ──(10) trains a model that corrects CAMS, applies it to all 15 cities
           └─► results to MongoDB / Cassandra → dashboard
```

Steps 1–7 take **seconds**. Steps 8–10 are slower but **complete and correct**. That pairing is the core
idea of the architecture.

---

## 4. The architecture: two paths, one truth (Lambda)

```text
                 ┌──────────── speed layer (seconds, approximate) ────────────┐
 sources → Kafka ┤                                                            ├─► serving DBs → dashboard
                 └─► HDFS (keep everything) → batch layer (hours, exact) ─────┘
```

- **Why two paths?** The fast path must answer *now*, so it can't wait for late or missing data. The slow
  path waits, sees everything, and fixes the fast path's mistakes.
- **How they meet:** both write the *same key*, e.g. `ground_hourly(station, hour)`. The batch value
  simply overwrites the speed value (the `layer` column shows `speed` or `batch`). On the dashboard,
  orange bars are speed values and blue bars are batch values.
- **The lake is the master copy.** Raw data is never edited, so any result can always be recomputed.
- **History skips the fast path.** 25 years of NASA data goes straight into HDFS, because pushing old data
  through a real-time pipe would be pointless.

---

## 5. Each tool: what it does and why it's here

**Kafka**: a durable, ordered log, like a conveyor belt that also records everything it carried.
Producers drop messages on it; Spark picks them up. If Spark crashes, nothing is lost: it resumes where it
left off. Topics: `nepal.air.raw`, `nepal.ground.raw`, `nepal.fires.raw`, `nepal.news.raw`, and
`nepal.air.alerts` (output). It runs in KRaft mode, which is the modern setup without ZooKeeper.

**Flume**: a delivery agent for *files*. The FIRMS fetcher drops a CSV file into a folder (the "spool
dir"). Flume reads it line by line and **sends a copy to two places**: Kafka (for the stream) and HDFS (raw
archive). It's set up the classic way: source → channel (buffer) → sink.

**HDFS**: the warehouse floor. A big distributed file system: the *NameNode* knows where every file lives,
the *DataNode* holds the bytes. Layout:
`raw/` (as received) · `lake/` (clean Parquet written by Spark) · `curated/` (batch results) ·
`checkpoints/` (Spark's bookmarks).

**Spark Structured Streaming**: processes the Kafka streams in small "micro-batches" every 5–60 s.
One program runs 8 queries: AQI and alerts, daily and hourly **event-time windows** (grouped by when the
reading was *measured*, not when it arrived), fire parsing with nearest-city distance, and news tagging.
A *watermark* (2–3 h) tells Spark how long to wait for late data before closing a window.

**Spark batch + MLlib**: the analysts.
- `batch_analytics.py`: days above the Nepal/WHO limits, seasonality, daily cycle, weather drivers, a
  **6-hour PM2.5 forecast** (Gradient-Boosted Trees), and **recommended actions** per audience.
- `fusion_analytics.py`: combines sources:
  - **Model vs reality**: bias and error of CAMS at each station.
  - **Calibration**: a Random Forest, tuned with 3-fold cross-validation, that learns to correct CAMS.
  - **Fires**: fires within 100 km of each city; do more fires mean worse air the next day?
  - **25-year haze trend** per city (real result: Kathmandu AOD went from 0.34 in 2001 to 0.42–0.50 in
    2023–25, and hazy days went from 68 to 108–148 a year).
  - **KMeans**: groups cities with similar pollution patterns (valley vs plains vs hills).
  - **News text mining**: word counts, themes, and LDA topics.

**MapReduce on YARN**: the "original" Hadoop way, shown for comparison with Spark. A Python *mapper* reads
each NASA day and emits `city|year → AOD`; Hadoop sorts by key; a *reducer* sums each city-year into yearly
statistics. YARN is the scheduler that gives the job memory and CPU.

**Hive**: SQL on top of the HDFS files, without moving them. The tables are *external* ("here is a folder,
read it as a table"), so deleting a table never deletes data. You can query Spark's Parquet, Flume's CSV,
NASA's JSON and MapReduce's output with plain SQL.

**Cassandra**: built for *huge numbers of writes* and "give me rows for key X" lookups. It stores the
1-minute station data, organised by `(station, day)`, so "Pulchowk on 12 Oct" is one fast read.

**MongoDB**: flexible documents for everything the dashboard shows (latest values, alerts, forecasts,
analytics results). It can also answer **map questions** ("fires within 100 km of Kathmandu") using a
geospatial index.

**Streamlit dashboard**: nine tabs that only *read* from the databases. It never computes heavy things
itself.

---

## 6. How to run it

**You need:** Docker Desktop (Compose v2), internet, ~8 GB RAM free (~12 GB for the full version).

```bash
cp .env.example .env                           # optional: edit settings / add FIRMS_MAP_KEY
docker compose up -d --build                   # core platform
# or the full Hadoop version (adds YARN + MapReduce and Hive):
docker compose --profile full up -d --build
```

The first build downloads Hadoop + Flume (~1 GB), so expect it to take several minutes.

### What happens after you press enter (first start)

| Roughly when | What is happening behind the scenes |
| --- | --- |
| 0–1 min | Kafka, MongoDB, Cassandra, HDFS start. Init containers create the Kafka topics, the Cassandra tables and the HDFS folders, then exit. |
| 1–5 min | **Back-fills run once:** 1 year of Open-Meteo, ~3 weeks of 1-minute station data, 7 days of fires (through Flume), news, 25 years of NASA POWER into HDFS. A marker file in `/app/data` records that each back-fill is done, so it never repeats. |
| immediately | Spark streaming starts eating Kafka from the beginning. The Live and Ground tabs fill first. |
| ~5–10 min | The batch container waits until ~95 % of the year has landed in the lake, then runs both batch jobs. Analytics, Forecast, Fires & aerosols and News tabs fill. |
| afterwards | Live producers keep polling; batch repeats every 6 h; MapReduce (full profile) runs daily. |

Later restarts skip the back-fills and are ready in about a minute.

### Where to look

| Open | To see |
| --- | --- |
| <http://localhost:8501> | the dashboard |
| <http://localhost:8080> | Kafka topics and live messages |
| <http://localhost:4040> | Spark streaming jobs and micro-batches |
| <http://localhost:9870> | HDFS: *Utilities → Browse* → `/nepairwatch` |
| <http://localhost:8088> | YARN: the MapReduce job *(full profile)* |
| <http://localhost:10002> | Hive server *(full profile)* |

### Handy commands

```bash
docker compose ps                                   # is everything up?
docker compose logs -f spark-streaming              # watch micro-batches
docker compose logs -f producer-ground              # watch station data arrive
docker compose run --rm replay                      # demo: replay 2 days fast so the live map moves
docker compose exec namenode hdfs dfs -ls -R /nepairwatch/raw | head
docker compose exec cassandra cqlsh -e "SELECT * FROM nepairwatch.ground_hourly LIMIT 5"
docker compose exec hive beeline -u jdbc:hive2://localhost:10000/nepairwatch -f /hive/queries.hql
docker compose stop                                 # pause, keep data
docker compose down -v                              # wipe everything, next start is fresh
```

---

## 7. The clever bits (good to mention when presenting)

- **Nothing is counted twice.** Every database write is an *upsert* on a natural key (city + time,
  station + time, fire id). Replays and restarts are safe; alerts go to Kafka only the first time.
- **Late data is not lost.** If a reading arrives after its window closed, the stream ignores it, but it is
  still in the lake, and the batch layer includes it.
- **Nepal time is handled properly.** Nepal is UTC+5:45, so windows are shifted so that "a day" and "an
  hour" match Nepali clocks.
- **AQI runs inside Spark's engine** (native expressions, not Python functions), so it's fast.
- **Honest model testing.** Models train on the past and are tested on the future (time split), never on a
  random mix, which would let them "see the future".
- **Analytics ladder:** *what happened* (exceedance days) → *why* (weather, fires, model bias) → *what
  next* (forecast) → *what to do* (recommended actions for schools, hospitals, municipality).

---

## 8. Where things live in the code

```text
producer/        one file per source (ground_producer, firms_fetcher, news_producer, power_loader, aq_producer)
spark_jobs/      stream_processor (fast path) · batch_analytics + fusion_analytics (slow path)
mapreduce/       mapper / reducer / job script
hive/            table definitions + example queries (the dashboard's Hive tab reads these)
cassandra/       table definitions
flume/           Flume agent configuration
dashboard/       app.py (tabs) · sources.py (Cassandra / Hive / YARN / Flume readers)
docker/hadoop/   the Hadoop + Flume image and its config files
common/          shared settings, AQI maths, distances, HDFS helper
```

---

## 9. Things to know

- **Tested so far:** the data parsers (against real API responses) and the MapReduce mapper/reducer (on
  real NASA data). **Not yet tested:** a full `docker compose up` run. The first run is where small fixes
  may be needed. Likely spots are the Flume/Hadoop library mix and the Hive container settings.
- **Office network:** Zscaler intercepts HTTPS, so the containers may fail to reach the APIs there. Run
  it on a home or mobile network.
- **Fire history:** only 7 days without a free `FIRMS_MAP_KEY`; add one in `.env` for more.
- **Station history grows over time:** the government API keeps only ~3 weeks, so the longer NepAirWatch
  runs, the bigger its own archive, and the better the calibration model gets.
- **Upgrading an old install:** run `docker compose down -v` once, because the storage layout changed.
