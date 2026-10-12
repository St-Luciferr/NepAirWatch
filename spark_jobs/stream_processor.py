"""
NepAirWatch — Spark Structured Streaming job (speed layer of the Lambda architecture).

One Spark application, four Kafka topics, eight streaming queries:

nepal.air.raw     (CAMS model + weather, hourly, 15 cities)
  ├─ Q1 air_lake       Parquet lake  air_quality/  partitioned city / date        [exactly-once file sink]
  ├─ Q2 air_serving    MongoDB readings + latest + alerts → Kafka nepal.air.alerts
  └─ Q3 air_daily      1-day event-time window (Nepal time) + 3 h watermark → MongoDB daily_stats
nepal.ground.raw  (DoEnv stations + Sensor.Community, ~1 min)
  ├─ Q4 ground_lake    Parquet lake  ground/  partitioned network / date
  ├─ Q5 ground_serving Cassandra ground_readings (time-series) + MongoDB ground_latest
  └─ Q6 ground_hourly  1-hour event-time window per station + 2 h watermark
                       → Cassandra ground_hourly (speed view) + measured-AQI alerts
nepal.fires.raw   (NASA FIRMS via Apache Flume, CSV lines)
  └─ Q7 fires          parse CSV, nearest city (location analytics) → Parquet lake fires/
                       + MongoDB fires (GeoJSON, 2dsphere index)
nepal.news.raw    (headlines)
  └─ Q8 news           rule-based theme tagging → Parquet lake news/ + MongoDB news
A StreamingQueryListener records every micro-batch's progress in MongoDB stream_progress.

Run:  python spark_jobs/stream_processor.py [--reset]
"""
import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pymongo import ASCENDING, DESCENDING, GEOSPHERE, MongoClient, ReplaceOne, UpdateOne  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql.streaming import StreamingQueryListener  # noqa: E402

from spark_common import (FIRES_CSV_DDL, GROUND_SCHEMA, NEWS_SCHEMA, NEWS_THEMES, RAW_SCHEMA,  # noqa: E402
                          aqi_expr, build_spark, category_expr, to_cassandra, with_nearest_city)
from common.config import (ALERT_AQI, CHECKPOINT_PATH, KAFKA_BOOTSTRAP, LAKE_PATH,  # noqa: E402
                           MONGO_DB, MONGO_URI, TOPIC_ALERTS, TOPIC_FIRES, TOPIC_GROUND, TOPIC_NEWS,
                           TOPIC_RAW)

_mongo = None
MONGO_COLLECTIONS = ["readings", "latest", "alerts", "daily_stats", "stream_batches", "stream_progress",
                     "stream_totals", "ground_latest", "fires", "news"]


def mongo():
    global _mongo
    if _mongo is None:
        db = MongoClient(MONGO_URI)[MONGO_DB]
        db.readings.create_index([("city", 1), ("event_ts", 1)], unique=True)
        db.readings.create_index([("event_ts", -1)])
        if "city_1_event_ts_1" in db.alerts.index_information():   # pre-v2 alert key
            db.alerts.drop_index("city_1_event_ts_1")
        db.alerts.create_index([("kind", 1), ("site", 1), ("event_ts", 1)], unique=True)
        db.alerts.create_index([("event_ts", -1)])
        db.daily_stats.create_index([("city", 1), ("day", 1)], unique=True)
        db.fires.create_index("fire_id", unique=True)
        db.fires.create_index([("loc", GEOSPHERE)])
        db.fires.create_index("acq_ts", expireAfterSeconds=30 * 86400)   # keep 30 days hot
        db.news.create_index("id", unique=True)
        db.news.create_index([("published", DESCENDING)])
        db.stream_progress.create_index([("query", ASCENDING), ("at", DESCENDING)])
        db.stream_progress.create_index("at", expireAfterSeconds=7 * 86400)
        _mongo = db
    return _mongo


def _naive(d, *keys):
    """MongoDB stores naive datetimes as wall-clock NPT (the Spark session time zone)."""
    for k in keys:
        if d.get(k) is not None and hasattr(d[k], "tzinfo"):
            d[k] = d[k].replace(tzinfo=None)
    return d


def raise_alerts(alerts):
    """Upsert alerts into MongoDB; publish only the *new* ones to Kafka (replays don't re-alert)."""
    if not alerts:
        return 0
    res = mongo().alerts.bulk_write(
        [UpdateOne({"kind": a["kind"], "site": a["site"], "event_ts": a["event_ts"]}, {"$set": a}, upsert=True)
         for a in alerts], ordered=False)
    new = [alerts[i] for i in res.upserted_ids]
    if new:
        spark = _spark
        rows = [(a["site"], json.dumps({k: a[k] for k in ("kind", "site", "city", "pm2_5", "aqi", "category",
                                                          "severity", "message")}
                                       | {"event_ts": a["event_ts"].isoformat()}))
                for a in new]
        (spark.createDataFrame(rows, "key STRING, value STRING").write.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP).option("topic", TOPIC_ALERTS).save())
    return len(new)


def _alert(kind, site, city, event_ts, pm25, aqi, category, what):
    sev = "CRITICAL" if aqi >= 201 else "WARNING"
    return {"kind": kind, "site": site, "city": city, "event_ts": event_ts, "pm2_5": pm25, "aqi": aqi,
            "category": category, "severity": sev,
            "message": f"{category} air {what} {site}: PM2.5 {pm25:.0f} µg/m³ (AQI {aqi}) at {event_ts:%Y-%m-%d %H:%M}"}


# ----------------------------------------------------------------------------- CAMS (air) sinks
def write_serving(batch_df, batch_id):
    """foreachBatch sink: upsert readings, keep `latest` per city, raise alerts."""
    if batch_df.isEmpty():
        return
    db = mongo()
    cols = ["city", "province", "lat", "lon", "event_ts", "pm2_5", "pm10", "co", "no2", "so2",
            "o3", "temperature", "humidity", "wind_speed", "precipitation",
            "boundary_layer_height", "aqi", "category", "source", "processed_at"]
    rows = [_naive(r.asDict(), "event_ts", "processed_at") for r in batch_df.select(*cols).collect()]

    db.readings.bulk_write([ReplaceOne({"city": d["city"], "event_ts": d["event_ts"]}, d, upsert=True)
                            for d in rows], ordered=False)

    # latest reading per city (only move forward in time)
    newest = {}
    for d in rows:
        if d["city"] not in newest or d["event_ts"] > newest[d["city"]]["event_ts"]:
            newest[d["city"]] = d
    for city, d in newest.items():
        cur = db.latest.find_one({"_id": city}, {"event_ts": 1})
        if cur is None or cur["event_ts"] <= d["event_ts"]:
            db.latest.replace_one({"_id": city}, {**d, "_id": city}, upsert=True)

    # alerts: AQI at/above "Unhealthy" (model data)
    alerts = [_alert("model", d["city"], d["city"], d["event_ts"], d["pm2_5"], d["aqi"], d["category"], "in")
              for d in rows if d["aqi"] is not None and d["aqi"] >= ALERT_AQI]
    new_alerts = raise_alerts(alerts)
    db.stream_batches.insert_one({"batch_id": int(batch_id), "records": len(rows), "alerts": len(alerts),
                                  "cities": len(newest), "at": datetime.now()})
    print(f"[air {batch_id}] {len(rows):,} readings → MongoDB, {len(alerts)} alerts ({new_alerts} new)",
          flush=True)


def write_daily(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    ops = []
    for r in batch_df.collect():
        d = r.asDict()
        d["day"] = d.pop("window_start").strftime("%Y-%m-%d")
        ops.append(ReplaceOne({"city": d["city"], "day": d["day"]}, d, upsert=True))
    mongo().daily_stats.bulk_write(ops, ordered=False)


# ----------------------------------------------------------------------------- ground sinks
GROUND_CASSANDRA_COLS = ["station_id", "day", "event_ts", "network", "station", "city", "lat", "lon",
                         "pm1", "pm2_5", "pm10", "aqi", "source"]


def write_ground(batch_df, batch_id):
    """Minute readings → Cassandra (time-series store); newest per station → MongoDB ground_latest."""
    if batch_df.isEmpty():
        return
    batch_df = batch_df.persist()
    to_cassandra(batch_df.withColumnRenamed("date", "day").select(*GROUND_CASSANDRA_COLS), "ground_readings")

    w = Window.partitionBy("station_id").orderBy(F.desc("event_ts"))
    newest = (batch_df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
              .select("station_id", "network", "station", "city", "lat", "lon", "event_ts",
                      "pm1", "pm2_5", "pm10", "aqi", "category", "source").collect())
    db = mongo()
    for r in newest:
        d = _naive(r.asDict(), "event_ts")
        cur = db.ground_latest.find_one({"_id": d["station_id"]}, {"event_ts": 1})
        if cur is None or cur["event_ts"] <= d["event_ts"]:
            db.ground_latest.replace_one({"_id": d["station_id"]}, {**d, "_id": d["station_id"]}, upsert=True)
    n = batch_df.count()
    batch_df.unpersist()
    print(f"[ground {batch_id}] {n:,} readings → Cassandra, {len(newest)} stations updated", flush=True)


def write_ground_hourly(batch_df, batch_id):
    """Speed view of hourly station means → Cassandra; measured-AQI alerts for complete-enough hours."""
    if batch_df.isEmpty():
        return
    batch_df = batch_df.persist()
    to_cassandra(batch_df.withColumn("layer", F.lit("speed")), "ground_hourly")
    alerts = []
    for r in batch_df.filter((F.col("aqi") >= ALERT_AQI) & (F.col("n_obs") >= 30)).collect():
        hour = r["hour"].replace(tzinfo=None)
        alerts.append({**_alert("ground", r["station"], r["city"], hour, r["pm2_5"], r["aqi"],
                                _category(r["aqi"]), "measured at station"),
                       "station_id": r["station_id"], "network": r["network"]})
    new_alerts = raise_alerts(alerts)
    batch_df.unpersist()
    if alerts:
        print(f"[ground-hourly {batch_id}] {len(alerts)} measured alerts ({new_alerts} new)", flush=True)


def _category(aqi):
    from common.aqi import category
    return category(aqi)["category"]


# ----------------------------------------------------------------------------- fires / news sinks
def write_fires(batch_df, batch_id):
    """Parse Flume CSV events, add the nearest city, append to the lake and upsert into MongoDB."""
    fires = (batch_df.select(F.from_csv(F.col("value").cast("string"), FIRES_CSV_DDL).alias("f")).select("f.*")
             .withColumn("acq_ts", F.to_timestamp("acq_time_iso")).drop("acq_time_iso")
             .filter(F.col("fire_id").isNotNull() & F.col("acq_ts").isNotNull())
             .dropDuplicates(["fire_id"]))
    if fires.isEmpty():
        return
    fires = with_nearest_city(fires, _spark).withColumn("acq_date", F.to_date("acq_ts")).persist()
    fires.write.mode("append").partitionBy("acq_date").parquet(f"{LAKE_PATH}/fires")
    ops = []
    for r in fires.drop("acq_date").collect():
        d = _naive(r.asDict(), "acq_ts")
        d["loc"] = {"type": "Point", "coordinates": [d["longitude"], d["latitude"]]}
        ops.append(ReplaceOne({"fire_id": d["fire_id"]}, d, upsert=True))
    mongo().fires.bulk_write(ops, ordered=False)
    fires.unpersist()
    print(f"[fires {batch_id}] {len(ops):,} hotspots → lake + MongoDB", flush=True)


def write_news(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    batch_df = batch_df.dropDuplicates(["id"]).persist()
    batch_df.write.mode("append").partitionBy("date").parquet(f"{LAKE_PATH}/news")
    ops = [ReplaceOne({"id": r["id"]}, _naive(r.asDict(), "published_ts", "processed_at"), upsert=True)
           for r in batch_df.drop("date").collect()]
    mongo().news.bulk_write(ops, ordered=False)
    batch_df.unpersist()
    print(f"[news {batch_id}] {len(ops)} articles", flush=True)


# ----------------------------------------------------------------------------- monitoring
class ProgressToMongo(StreamingQueryListener):
    """Persists streaming progress (rows, rates) per query — pipeline-health tab + batch scheduling."""

    def onQueryStarted(self, event):
        pass

    def onQueryProgress(self, event):
        p = event.progress
        if not p.numInputRows:
            return

        def num(x):
            return float(x) if x is not None and x == x else 0.0   # NaN-safe
        now = datetime.now()
        db = mongo()
        db.stream_progress.insert_one({
            "query": p.name, "batch_id": int(p.batchId), "input_rows": int(p.numInputRows),
            "input_rps": num(p.inputRowsPerSecond), "processed_rps": num(p.processedRowsPerSecond),
            "duration_ms": int((p.durationMs or {}).get("triggerExecution", 0)), "at": now})
        # cumulative counter per query (never expires; scripts/wait_for_lake.py reads it)
        db.stream_totals.update_one({"_id": p.name}, {"$inc": {"rows": int(p.numInputRows), "batches": 1},
                                                      "$set": {"at": now}}, upsert=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def _kafka(spark, topic, max_offsets):
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("maxOffsetsPerTrigger", max_offsets)
            .option("failOnDataLoss", "false")
            .load())


def _delete(spark, path):
    """Recursive delete on any Hadoop file system (local or hdfs://)."""
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    p.getFileSystem(spark._jsc.hadoopConfiguration()).delete(p, True)


_spark = None


def main():
    global _spark
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="delete checkpoints & lake, reprocess topics")
    ap.add_argument("--max-offsets", type=int, default=25000)
    args = ap.parse_args()

    spark = _spark = build_spark("NepAirWatch-Streaming", kafka=True, cassandra=True)
    if args.reset:
        for p in (CHECKPOINT_PATH, LAKE_PATH):
            _delete(spark, p)
        db = MongoClient(MONGO_URI)[MONGO_DB]
        for c in MONGO_COLLECTIONS:
            db.drop_collection(c)
    mongo()
    spark.streams.addListener(ProgressToMongo())
    ck = CHECKPOINT_PATH

    # ------------------------------------------------------------------ CAMS model + weather
    parsed = (_kafka(spark, TOPIC_RAW, args.max_offsets)
              .select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("m"))
              .select("m.*")
              .withColumn("event_ts", F.to_timestamp("event_time"))
              # data quality: drop malformed / physically impossible values
              .filter(F.col("city").isNotNull() & F.col("event_ts").isNotNull())
              .filter(F.col("pm2_5").between(0, 1000))
              .withColumn("aqi", aqi_expr(F.col("pm2_5")))
              .withColumn("category", category_expr(F.col("aqi")))
              .withColumn("date", F.to_date("event_ts"))
              .withColumn("processed_at", F.current_timestamp()))

    queries = [
        parsed.writeStream.format("parquet")
        .option("path", f"{LAKE_PATH}/air_quality").option("checkpointLocation", f"{ck}/lake")
        .partitionBy("city", "date").trigger(processingTime="10 seconds").queryName("lake_sink").start(),

        parsed.writeStream.foreachBatch(write_serving)
        .option("checkpointLocation", f"{ck}/serving").trigger(processingTime="5 seconds")
        .queryName("serving_sink").start(),
    ]
    # event-time windowed aggregation (calendar day in Nepal time, UTC+5:45)
    daily = (parsed.withWatermark("event_ts", "3 hours")
             .groupBy(F.window("event_ts", "1 day", "1 day", "18 hours 15 minutes"), "city")
             .agg(F.round(F.avg("pm2_5"), 1).alias("pm25_avg"),
                  F.round(F.max("pm2_5"), 1).alias("pm25_max"),
                  F.round(F.avg("pm10"), 1).alias("pm10_avg"),
                  F.max("aqi").alias("aqi_max"),
                  F.count("*").alias("n_obs"))
             .select(F.col("window.start").alias("window_start"), "*").drop("window"))
    queries.append(daily.writeStream.outputMode("update").foreachBatch(write_daily)
                   .option("checkpointLocation", f"{ck}/daily").trigger(processingTime="10 seconds")
                   .queryName("daily_window").start())

    # ------------------------------------------------------------------ ground stations
    ground = (_kafka(spark, TOPIC_GROUND, 50000)
              .select(F.from_json(F.col("value").cast("string"), GROUND_SCHEMA).alias("m")).select("m.*")
              .withColumn("event_ts", F.to_timestamp("event_time")).drop("event_time")
              .filter(F.col("station_id").isNotNull() & F.col("event_ts").isNotNull())
              .filter(F.col("pm2_5").between(0, 2000) | F.col("pm10").between(0, 2000))
              .withColumn("aqi", aqi_expr(F.col("pm2_5")))
              .withColumn("category", category_expr(F.col("aqi")))
              .withColumn("date", F.to_date("event_ts"))
              .withColumn("processed_at", F.current_timestamp()))
    queries += [
        ground.writeStream.format("parquet")
        .option("path", f"{LAKE_PATH}/ground").option("checkpointLocation", f"{ck}/ground_lake")
        .partitionBy("network", "date").trigger(processingTime="30 seconds").queryName("ground_lake").start(),

        ground.writeStream.foreachBatch(write_ground)
        .option("checkpointLocation", f"{ck}/ground_serving").trigger(processingTime="10 seconds")
        .queryName("ground_serving").start(),
    ]
    # hourly windows aligned to Nepal clock hours (NPT hh:00 = UTC hh:15 - 6 h → offset 15 min)
    hourly = (ground.filter(F.col("pm2_5").isNotNull()).withWatermark("event_ts", "2 hours")
              .groupBy(F.window("event_ts", "1 hour", "1 hour", "15 minutes"),
                       "station_id", "network", "station", "city")
              .agg(F.round(F.avg("pm2_5"), 1).alias("pm2_5"), F.round(F.avg("pm10"), 1).alias("pm10"),
                   F.round(F.max("pm2_5"), 1).alias("pm2_5_max"), F.count("*").cast("int").alias("n_obs"))
              .withColumn("hour", F.col("window.start")).drop("window")
              .withColumn("aqi", aqi_expr(F.col("pm2_5"))))
    queries.append(hourly.writeStream.outputMode("update").foreachBatch(write_ground_hourly)
                   .option("checkpointLocation", f"{ck}/ground_hourly").trigger(processingTime="30 seconds")
                   .queryName("ground_hourly").start())

    # ------------------------------------------------------------------ fires (via Flume) and news
    queries.append(_kafka(spark, TOPIC_FIRES, 20000).writeStream.foreachBatch(write_fires)
                   .option("checkpointLocation", f"{ck}/fires").trigger(processingTime="30 seconds")
                   .queryName("fires").start())

    themes = [F.when(F.col("title").rlike(rx), F.lit(name)) for name, rx in NEWS_THEMES.items()]
    news = (_kafka(spark, TOPIC_NEWS, 2000)
            .select(F.from_json(F.col("value").cast("string"), NEWS_SCHEMA).alias("m")).select("m.*")
            .filter(F.col("id").isNotNull() & F.col("title").isNotNull())
            .withColumn("published_ts", F.to_timestamp("published")).drop("published")
            .withColumn("themes", F.filter(F.array(*themes), lambda x: x.isNotNull()))
            .withColumn("date", F.to_date("published_ts"))
            .withColumn("processed_at", F.current_timestamp()))
    queries.append(news.writeStream.foreachBatch(write_news)
                   .option("checkpointLocation", f"{ck}/news").trigger(processingTime="60 seconds")
                   .queryName("news").start())

    print("Streaming queries started:", [q.name for q in queries], flush=True)
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
