"""
NepAirWatch — Spark batch analytics + MLlib forecasting (batch layer).

Reads the Parquet data lake written by the streaming job and produces:
  1. monthly_city       — monthly mean PM2.5 per city (seasonality)
  2. hourly_profile     — mean PM2.5 by hour-of-day per city (diurnal cycle)
  3. exceedance         — days above Nepal NAAQS (40) and WHO (15) 24-h PM2.5 limits
  4. province_summary   — population-level view by province
  5. weather_corr       — Pearson correlation of PM2.5 with weather drivers
  6. model_metrics      — GBT regressor forecasting PM2.5 6 hours ahead vs persistence baseline
  7. forecasts          — latest 6-h-ahead forecast per city (early warning)
All outputs → MongoDB (serving) and Parquet (data lake /curated zone).

Run:  python spark_jobs/batch_analytics.py
"""
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pymongo import MongoClient  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.evaluation import RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler  # noqa: E402
from pyspark.ml.regression import GBTRegressor  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_common import aqi_expr, build_spark, category_expr  # noqa: E402
from common.config import (LAKE_PATH, MONGO_DB, MONGO_URI, NEPAL_PM25_24H,  # noqa: E402
                           WHO_PM25_24H, ROOT)

HORIZON = 6  # hours ahead to forecast


def to_mongo(db, name, df):
    docs = [r.asDict(recursive=True) for r in df.collect()]
    for d in docs:
        for k, v in d.items():
            if hasattr(v, "isoformat") and not isinstance(v, datetime):
                d[k] = v.isoformat()
    db[name].delete_many({})
    if docs:
        db[name].insert_many(docs)
    print(f"  → mongo.{name}: {len(docs)} docs")


def main():
    t0 = time.time()
    spark = build_spark("NepAirWatch-Batch")
    db = MongoClient(MONGO_URI)[MONGO_DB]
    curated = f"{LAKE_PATH}/curated"

    lake = spark.read.parquet(f"{LAKE_PATH}/air_quality")
    # replays can re-deliver the same city-hour: deduplicate on the natural key
    df = (lake.dropDuplicates(["city", "event_ts"])
          .withColumn("hour", F.hour("event_ts"))
          .withColumn("month", F.date_format("event_ts", "yyyy-MM"))
          .cache())
    n = df.count()
    print(f"Loaded {n:,} unique city-hours from the lake ({lake.inputFiles().__len__()} files)")

    # 1. Monthly seasonality
    monthly = (df.groupBy("city", "province", "month")
               .agg(F.round(F.avg("pm2_5"), 1).alias("pm25_avg"),
                    F.round(F.avg("pm10"), 1).alias("pm10_avg"))
               .orderBy("city", "month"))
    to_mongo(db, "monthly_city", monthly)

    # 2. Diurnal profile
    hourly = (df.groupBy("city", "hour").agg(F.round(F.avg("pm2_5"), 1).alias("pm25_avg"))
              .orderBy("city", "hour"))
    to_mongo(db, "hourly_profile", hourly)

    # 3. Exceedance days against national & WHO standards
    daily = (df.groupBy("city", "province", "date")
             .agg(F.avg("pm2_5").alias("pm25_24h")))
    exceed = (daily.groupBy("city", "province")
              .agg(F.count("*").alias("days"),
                   F.round(F.avg("pm25_24h"), 1).alias("annual_mean"),
                   F.sum((F.col("pm25_24h") > NEPAL_PM25_24H).cast("int")).alias("days_over_nepal_std"),
                   F.sum((F.col("pm25_24h") > WHO_PM25_24H).cast("int")).alias("days_over_who"),
                   F.round(F.max("pm25_24h"), 1).alias("worst_day_pm25"))
              .withColumn("pct_days_over_nepal_std",
                          F.round(100 * F.col("days_over_nepal_std") / F.col("days"), 1))
              .orderBy(F.desc("annual_mean")))
    to_mongo(db, "exceedance", exceed)
    exceed.show(20, truncate=False)

    # 4. Province summary
    prov = (df.groupBy("province")
            .agg(F.round(F.avg("pm2_5"), 1).alias("pm25_avg"),
                 F.round(F.expr("percentile_approx(pm2_5, 0.95)"), 1).alias("pm25_p95"),
                 F.round(100 * F.avg((F.col("aqi") >= 151).cast("int")), 1).alias("pct_hours_unhealthy"))
            .orderBy(F.desc("pm25_avg")))
    to_mongo(db, "province_summary", prov)

    # 5. Weather drivers (why is the air bad?)
    drivers = ["boundary_layer_height", "wind_speed", "humidity", "temperature", "precipitation"]
    corr_rows = []
    for region, cond in [("All Nepal", F.lit(True)),
                         ("Kathmandu Valley", F.col("city").isin("Kathmandu", "Lalitpur", "Bhaktapur")),
                         ("Terai", F.col("city").isin("Biratnagar", "Janakpur", "Birgunj", "Nepalgunj",
                                                      "Dhangadhi", "Mahendranagar", "Butwal"))]:
        sub = df.filter(cond).na.drop(subset=drivers + ["pm2_5"])
        for d in drivers:
            corr_rows.append({"region": region, "driver": d,
                              "pearson_r": round(sub.stat.corr("pm2_5", d), 3)})
    db.weather_corr.delete_many({})
    db.weather_corr.insert_many(corr_rows)
    print(f"  → mongo.weather_corr: {len(corr_rows)} docs")

    # 6. MLlib: forecast PM2.5 HORIZON hours ahead (early-warning model)
    w = Window.partitionBy("city").orderBy("event_ts")
    feat = df.select("city", "province", "event_ts", "pm2_5", "pm10", "co", "no2", "temperature",
                     "humidity", "wind_speed", "precipitation", "boundary_layer_height", "hour")
    for lag in (1, 3, 6, 24):
        feat = feat.withColumn(f"pm25_lag{lag}", F.lag("pm2_5", lag).over(w))
    feat = (feat
            .withColumn("pm25_roll24", F.avg("pm2_5").over(w.rowsBetween(-23, 0)))
            .withColumn("month_num", F.month("event_ts"))
            .withColumn("label", F.lead("pm2_5", HORIZON).over(w)))
    numeric = ["pm2_5", "pm10", "co", "no2", "temperature", "humidity", "wind_speed",
               "precipitation", "boundary_layer_height", "hour", "month_num",
               "pm25_lag1", "pm25_lag3", "pm25_lag6", "pm25_lag24", "pm25_roll24"]
    feat = feat.na.drop(subset=numeric).cache()
    labelled = feat.filter(F.col("label").isNotNull())

    cutoff = labelled.select(F.expr("percentile_approx(unix_timestamp(event_ts), 0.8)")).first()[0]
    train = labelled.filter(F.unix_timestamp("event_ts") <= cutoff)
    test = labelled.filter(F.unix_timestamp("event_ts") > cutoff)
    print(f"Training GBT on {train.count():,} rows, testing on {test.count():,} (time-based split)")

    pipe = Pipeline(stages=[
        StringIndexer(inputCol="city", outputCol="city_idx", handleInvalid="keep"),
        OneHotEncoder(inputCols=["city_idx"], outputCols=["city_vec"]),
        VectorAssembler(inputCols=numeric + ["city_vec"], outputCol="features"),
        GBTRegressor(featuresCol="features", labelCol="label", maxIter=40, maxDepth=6, seed=42),
    ])
    model = pipe.fit(train)
    pred = model.transform(test).cache()

    ev = RegressionEvaluator(labelCol="label", predictionCol="prediction")
    base = test.withColumn("prediction", F.col("pm2_5"))  # persistence baseline: "same as now"
    metrics = {
        "model": f"GBTRegressor (PM2.5 t+{HORIZON}h)",
        "horizon_hours": HORIZON,
        "train_rows": train.count(), "test_rows": test.count(),
        "rmse": round(ev.evaluate(pred, {ev.metricName: "rmse"}), 2),
        "mae": round(ev.evaluate(pred, {ev.metricName: "mae"}), 2),
        "r2": round(ev.evaluate(pred, {ev.metricName: "r2"}), 3),
        "baseline_rmse": round(ev.evaluate(base, {ev.metricName: "rmse"}), 2),
        "baseline_mae": round(ev.evaluate(base, {ev.metricName: "mae"}), 2),
        "baseline_r2": round(ev.evaluate(base, {ev.metricName: "r2"}), 3),
        "trained_at": datetime.now().isoformat(timespec="seconds"),
    }
    gbt = model.stages[-1]
    names = numeric + [f"city={c}" for c in model.stages[0].labels]
    imp = sorted(zip(names, gbt.featureImportances.toArray()), key=lambda x: -x[1])[:10]
    metrics["feature_importance"] = [{"feature": k, "importance": round(float(v), 4)} for k, v in imp]
    db.model_metrics.delete_many({})
    db.model_metrics.insert_one(metrics)
    print("Model metrics:", {k: v for k, v in metrics.items() if k != "feature_importance"})

    # store a sample of test predictions for the dashboard (actual vs predicted)
    to_mongo(db, "test_predictions",
             pred.filter(F.col("city") == "Kathmandu")
             .select(F.date_format("event_ts", "yyyy-MM-dd HH:mm").alias("event_ts"),
                     "label", F.round("prediction", 1).alias("prediction"))
             .orderBy("event_ts"))

    model.write().overwrite().save(str(ROOT / "data" / "models" / "pm25_gbt"))

    # 7. Forecast from the newest observation of each city
    latest_w = Window.partitionBy("city").orderBy(F.desc("event_ts"))
    newest = feat.withColumn("rn", F.row_number().over(latest_w)).filter("rn = 1").drop("rn")
    fc = (model.transform(newest)
          .select("city", "province", F.col("event_ts").alias("from_ts"),
                  F.round("pm2_5", 1).alias("pm25_now"),
                  F.round("prediction", 1).alias("pm25_forecast"))
          .withColumn("for_ts", F.col("from_ts") + F.expr(f"INTERVAL {HORIZON} HOURS"))
          .withColumn("aqi_forecast", aqi_expr(F.col("pm25_forecast")))
          .withColumn("category_forecast", category_expr(F.col("aqi_forecast"))))
    to_mongo(db, "forecasts", fc.withColumn("from_ts", F.date_format("from_ts", "yyyy-MM-dd HH:mm"))
             .withColumn("for_ts", F.date_format("for_ts", "yyyy-MM-dd HH:mm")))
    fc.show(20, truncate=False)

    # curated zone of the lake (analytics-ready tables)
    monthly.write.mode("overwrite").parquet(f"{curated}/monthly_city")
    exceed.write.mode("overwrite").parquet(f"{curated}/exceedance")
    daily.write.mode("overwrite").partitionBy("city").parquet(f"{curated}/daily_city")

    db.job_runs.insert_one({"job": "batch_analytics", "rows": n,
                            "seconds": round(time.time() - t0, 1), "at": datetime.now()})
    print(f"Batch analytics finished in {time.time() - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
