"""
NepAirWatch — Spark Structured Streaming job (speed layer).

Kafka `nepal.air.raw`  ──►  parse + validate + AQI enrichment
                             │
                             ├─► Q1  Parquet data lake  (partitioned by city / date)   [batch layer input]
                             ├─► Q2  MongoDB `readings` + `latest`  (serving layer, upsert)
                             │        + alerts → MongoDB `alerts` and Kafka `nepal.air.alerts`
                             └─► Q3  1-day tumbling window per city (Nepal time) with
                                     watermark → MongoDB `daily_stats`

Run:  python spark_jobs/stream_processor.py [--reset]
"""
import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pymongo import MongoClient, ReplaceOne, UpdateOne  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_common import RAW_SCHEMA, aqi_expr, build_spark, category_expr  # noqa: E402
from common.config import (ALERT_AQI, CHECKPOINT_PATH, KAFKA_BOOTSTRAP, LAKE_PATH,  # noqa: E402
                           MONGO_DB, MONGO_URI, TOPIC_ALERTS, TOPIC_RAW)

_mongo = None


def mongo():
    global _mongo
    if _mongo is None:
        _mongo = MongoClient(MONGO_URI)[MONGO_DB]
        _mongo.readings.create_index([("city", 1), ("event_ts", 1)], unique=True)
        _mongo.readings.create_index([("event_ts", -1)])
        _mongo.alerts.create_index([("city", 1), ("event_ts", 1)], unique=True)
        _mongo.daily_stats.create_index([("city", 1), ("day", 1)], unique=True)
    return _mongo


def _docs(rows):
    for r in rows:
        d = r.asDict()
        d["event_ts"] = d["event_ts"].replace(tzinfo=None)  # naive NPT wall-clock
        d["processed_at"] = d["processed_at"].replace(tzinfo=None)
        yield d


def write_serving(batch_df, batch_id):
    """foreachBatch sink: upsert readings, keep `latest` per city, raise alerts."""
    if batch_df.isEmpty():
        return
    db = mongo()
    cols = ["city", "province", "lat", "lon", "event_ts", "pm2_5", "pm10", "co", "no2", "so2",
            "o3", "temperature", "humidity", "wind_speed", "precipitation",
            "boundary_layer_height", "aqi", "category", "source", "processed_at"]
    rows = list(_docs(batch_df.select(*cols).collect()))

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

    # alerts: AQI at/above "Unhealthy"
    alerts = [d for d in rows if d["aqi"] is not None and d["aqi"] >= ALERT_AQI]
    if alerts:
        for a in alerts:
            a["severity"] = "CRITICAL" if a["aqi"] >= 201 else "WARNING"
            a["message"] = (f"{a['category']} air in {a['city']}: PM2.5 {a['pm2_5']:.0f} µg/m³ "
                            f"(AQI {a['aqi']}) at {a['event_ts']:%Y-%m-%d %H:%M}")
        db.alerts.bulk_write([UpdateOne({"city": a["city"], "event_ts": a["event_ts"]},
                                        {"$set": a}, upsert=True) for a in alerts], ordered=False)
        # fan the alerts back out to Kafka so other consumers (SMS gateway, apps) can subscribe
        (batch_df.filter(F.col("aqi") >= ALERT_AQI)
         .select(F.col("city").alias("key"),
                 F.to_json(F.struct("city", "event_ts", "pm2_5", "aqi", "category")).alias("value"))
         .write.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
         .option("topic", TOPIC_ALERTS).save())
    db.stream_batches.insert_one({"batch_id": int(batch_id), "records": len(rows), "alerts": len(alerts),
                                  "cities": len(newest), "at": datetime.now()})
    print(f"[batch {batch_id}] {len(rows):,} readings → MongoDB, {len(alerts)} alerts", flush=True)


def write_daily(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    ops = []
    for r in batch_df.collect():
        d = r.asDict()
        d["day"] = d.pop("window_start").strftime("%Y-%m-%d")
        ops.append(ReplaceOne({"city": d["city"], "day": d["day"]}, d, upsert=True))
    mongo().daily_stats.bulk_write(ops, ordered=False)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--reset", action="store_true", help="delete checkpoints & lake, reprocess topic")
    ap.add_argument("--max-offsets", type=int, default=25000)
    args = ap.parse_args()
    if args.reset:
        for p in (CHECKPOINT_PATH, LAKE_PATH):
            shutil.rmtree(p, ignore_errors=True)
        db = MongoClient(MONGO_URI)[MONGO_DB]
        for c in ("readings", "latest", "alerts", "daily_stats", "stream_batches"):
            db.drop_collection(c)

    spark = build_spark("NepAirWatch-Streaming", kafka=True)

    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC_RAW)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", args.max_offsets)
           .option("failOnDataLoss", "false")
           .load())

    parsed = (raw.select(F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("m"),
                         F.col("timestamp").alias("kafka_ts"))
              .select("m.*", "kafka_ts")
              .withColumn("event_ts", F.to_timestamp("event_time"))
              # data quality: drop malformed / physically impossible values
              .filter(F.col("city").isNotNull() & F.col("event_ts").isNotNull())
              .filter(F.col("pm2_5").between(0, 1000))
              .withColumn("aqi", aqi_expr(F.col("pm2_5")))
              .withColumn("category", category_expr(F.col("aqi")))
              .withColumn("date", F.to_date("event_ts"))
              .withColumn("processed_at", F.current_timestamp()))

    # Q1 — immutable, partitioned Parquet data lake
    q1 = (parsed.drop("kafka_ts").writeStream.format("parquet")
          .option("path", f"{LAKE_PATH}/air_quality")
          .option("checkpointLocation", f"{CHECKPOINT_PATH}/lake")
          .partitionBy("city", "date")
          .trigger(processingTime="10 seconds")
          .queryName("lake_sink").start())

    # Q2 — serving layer + alerts
    q2 = (parsed.writeStream.foreachBatch(write_serving)
          .option("checkpointLocation", f"{CHECKPOINT_PATH}/serving")
          .trigger(processingTime="5 seconds")
          .queryName("serving_sink").start())

    # Q3 — event-time windowed aggregation (calendar day in Nepal time, UTC+5:45)
    daily = (parsed.withWatermark("event_ts", "3 hours")
             .groupBy(F.window("event_ts", "1 day", "1 day", "18 hours 15 minutes"), "city")
             .agg(F.round(F.avg("pm2_5"), 1).alias("pm25_avg"),
                  F.round(F.max("pm2_5"), 1).alias("pm25_max"),
                  F.round(F.avg("pm10"), 1).alias("pm10_avg"),
                  F.max("aqi").alias("aqi_max"),
                  F.count("*").alias("n_obs"))
             .select(F.col("window.start").alias("window_start"), "*").drop("window"))
    q3 = (daily.writeStream.outputMode("update").foreachBatch(write_daily)
          .option("checkpointLocation", f"{CHECKPOINT_PATH}/daily")
          .trigger(processingTime="10 seconds")
          .queryName("daily_window").start())

    print("Streaming queries started:", [q.name for q in (q1, q2, q3)])
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
