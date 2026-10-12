"""
NepAirWatch — multi-source batch analytics (batch layer of the Lambda architecture).

Fuses every source in the HDFS data lake / raw zone:
  lake/air_quality  CAMS model + weather (Kafka → Spark)      lake/ground  DoEnv + Sensor.Community
  lake/fires        NASA FIRMS hotspots (Flume → Kafka → Spark) lake/news    headlines
  raw/nasa_power    25 years of daily AOD + climate (bulk load)

  1  ground_hourly     batch view of hourly station means → Cassandra (overwrites the speed view)
                       + curated zone + station_summary                              descriptive
  2  cams_validation   model vs measured PM2.5 per station: bias, RMSE, Pearson r    diagnostic
  3  calibration       MLlib RandomForest + 3-fold CrossValidator: CAMS → measured   predictive
  4  fires             daily hotspots near cities, fire↔PM2.5 lag correlation,
                       0.25° hotspot density grid                                    location analytics
  5  aod               25-year AOD trend per city (regr_slope), monthly climatology,
                       satellite AOD vs model PM2.5                                  descriptive / diagnostic
  6  city_clusters     MLlib KMeans on seasonal + diurnal PM2.5 profiles (k by silhouette)  data mining
  7  news              tokenise → stop words → term counts, themes, LDA topics      text analytics

A section whose input is not there yet is skipped with a message, so the job is safe on day one.
Run:  python spark_jobs/fusion_analytics.py [--only-ground] [--include-demo]
      --only-ground   just section 1 (≈1 min) — used by scripts/demo.sh to show the batch layer live
      --include-demo  keep the demo simulator's stations in the ground views (demo only)
"""
import argparse
import sys
import time
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from pymongo import MongoClient  # noqa: E402
from pyspark.ml import Pipeline  # noqa: E402
from pyspark.ml.clustering import LDA, KMeans  # noqa: E402
from pyspark.ml.evaluation import ClusteringEvaluator, RegressionEvaluator  # noqa: E402
from pyspark.ml.feature import (CountVectorizer, OneHotEncoder, RegexTokenizer, StandardScaler,  # noqa: E402
                                StopWordsRemover, StringIndexer, VectorAssembler)
from pyspark.ml.regression import RandomForestRegressor  # noqa: E402
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder  # noqa: E402
from pyspark.sql import Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

from spark_common import (aqi_expr, build_spark, category_expr, not_demo, path_exists,  # noqa: E402
                          to_cassandra, to_mongo)
from common.config import (FIRE_NEAR_KM, LAKE_PATH, MONGO_DB, MONGO_URI, NEPAL_PM25_24H,  # noqa: E402
                           RAW_PATH, ROOT, WHO_PM25_24H)

CURATED = f"{LAKE_PATH}/curated"
MIN_CALIBRATION_ROWS = 300


def section(title):
    print(f"\n=== {title} ===", flush=True)


def hour_key(ts_col, shift_seconds=0):
    """Nearest-hour join key (epoch hours) — tolerant to sources stamping hours differently."""
    return F.round((F.unix_timestamp(ts_col) + shift_seconds) / 3600).cast("long")


# ----------------------------------------------------------------------------- 1 ground hourly
def ground_views(spark, db, include_demo=False):
    section("1. Ground stations — hourly batch view")
    path = f"{LAKE_PATH}/ground"
    if not path_exists(spark, path):
        print("  no ground data in the lake yet — skipped")
        return None
    g = spark.read.parquet(path).dropDuplicates(["station_id", "event_ts"])
    if not include_demo:
        g = g.filter(F.coalesce(F.col("network"), F.lit("")) != "demo")
    hourly = (g.filter(F.col("pm2_5").isNotNull())
              .withColumn("hour", F.date_trunc("hour", "event_ts"))     # Nepal clock hour (session TZ)
              .groupBy("station_id", "network", "station", "city", "hour")
              .agg(F.round(F.avg("pm2_5"), 1).alias("pm2_5"), F.round(F.avg("pm10"), 1).alias("pm10"),
                   F.round(F.max("pm2_5"), 1).alias("pm2_5_max"), F.count("*").cast("int").alias("n_obs"))
              .withColumn("aqi", aqi_expr(F.col("pm2_5")))
              .cache())
    print(f"  {hourly.count():,} station-hours from {g.select('station_id').distinct().count()} stations")
    to_cassandra(hourly.withColumn("layer", F.lit("batch")), "ground_hourly")
    hourly.write.mode("overwrite").partitionBy("city").parquet(f"{CURATED}/ground_hourly")

    daily = (hourly.withColumn("day", F.to_date("hour"))
             .groupBy("station_id", "day").agg(F.avg("pm2_5").alias("pm25_24h"), F.count("*").alias("hours"))
             .filter("hours >= 18"))          # a valid 24-h mean needs >= 75 % coverage
    summary = (hourly.groupBy("station_id", "network", "station", "city")
               .agg(F.min("hour").alias("first_hour"), F.max("hour").alias("last_hour"),
                    F.count("*").alias("hours"), F.round(F.avg("pm2_5"), 1).alias("pm25_mean"),
                    F.round(F.expr("percentile_approx(pm2_5, 0.95)"), 1).alias("pm25_p95"))
               .join(daily.groupBy("station_id")
                     .agg(F.count("*").alias("valid_days"),
                          F.sum((F.col("pm25_24h") > NEPAL_PM25_24H).cast("int")).alias("days_over_nepal_std"),
                          F.sum((F.col("pm25_24h") > WHO_PM25_24H).cast("int")).alias("days_over_who")),
                     "station_id", "left")
               .join(g.groupBy("station_id").agg(F.first("lat").alias("lat"), F.first("lon").alias("lon")),
                     "station_id")
               .withColumn("first_hour", F.date_format("first_hour", "yyyy-MM-dd HH:mm"))
               .withColumn("last_hour", F.date_format("last_hour", "yyyy-MM-dd HH:mm"))
               .orderBy(F.desc("pm25_mean")))
    to_mongo(db, "station_summary", summary)
    return hourly


# ----------------------------------------------------------------------------- 2+3 validation & calibration
def cams_vs_ground(spark, db, hourly, air):
    section("2. CAMS model vs measured PM2.5")
    if hourly is None:
        print("  no ground data — skipped")
        return
    # ground hour hh:00–hh:59 is centred on hh:30; CAMS values are instantaneous hourly
    gnd = (hourly.filter(F.col("city").isNotNull() & (F.col("n_obs") >= 30))
           .select("station_id", "station", "network", "city", F.col("hour"),
                   F.col("pm2_5").alias("ground_pm25"), hour_key(F.col("hour"), 1800).alias("hk")))
    cams = air.select("city", F.col("pm2_5").alias("cams_pm25"), F.col("pm10").alias("cams_pm10"),
                      "humidity", "temperature", "wind_speed", "boundary_layer_height", "precipitation",
                      hour_key(F.col("event_ts")).alias("hk"))
    j = gnd.join(cams, ["city", "hk"]).na.drop(subset=["cams_pm25", "ground_pm25"]).cache()
    n = j.count()
    print(f"  {n:,} matched station-hours")
    if n == 0:
        return
    val = (j.groupBy("station_id", "station", "network", "city")
           .agg(F.count("*").alias("hours"),
                F.round(F.avg("ground_pm25"), 1).alias("ground_mean"),
                F.round(F.avg("cams_pm25"), 1).alias("cams_mean"),
                F.round(F.avg(F.col("cams_pm25") - F.col("ground_pm25")), 1).alias("bias"),
                F.round(F.sqrt(F.avg(F.pow(F.col("cams_pm25") - F.col("ground_pm25"), 2))), 1).alias("rmse"),
                F.round(F.corr("cams_pm25", "ground_pm25"), 3).alias("pearson_r"))
           .orderBy(F.desc("hours")))
    to_mongo(db, "cams_validation", val)
    val.show(40, truncate=False)

    section("3. MLlib calibration: CAMS → measured PM2.5 (RandomForest + CrossValidator)")
    feats = ["cams_pm25", "cams_pm10", "humidity", "temperature", "wind_speed", "boundary_layer_height",
             "hour_of_day"]
    data = (j.withColumn("hour_of_day", F.hour("hour"))
            .na.drop(subset=feats).withColumnRenamed("ground_pm25", "label"))
    n = data.count()
    if n < MIN_CALIBRATION_ROWS:
        print(f"  only {n} rows (< {MIN_CALIBRATION_ROWS}) — calibration skipped until more ground data lands")
        return
    cutoff = data.select(F.expr("percentile_approx(hk, 0.8)")).first()[0]
    train, test = data.filter(F.col("hk") <= cutoff), data.filter(F.col("hk") > cutoff)

    rf = RandomForestRegressor(featuresCol="features", labelCol="label", seed=42)
    pipe = Pipeline(stages=[
        StringIndexer(inputCol="city", outputCol="city_idx", handleInvalid="keep"),
        OneHotEncoder(inputCols=["city_idx"], outputCols=["city_vec"]),
        VectorAssembler(inputCols=feats + ["city_vec"], outputCol="features"),
        rf,
    ])
    grid = ParamGridBuilder().addGrid(rf.numTrees, [30, 60]).addGrid(rf.maxDepth, [5, 8]).build()
    ev = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    cv = CrossValidator(estimator=pipe, estimatorParamMaps=grid, evaluator=ev, numFolds=3, parallelism=2,
                        seed=42)
    t = time.time()
    cv_model = cv.fit(train)
    best = cv_model.bestModel
    pred = best.transform(test).cache()
    raw = test.withColumn("prediction", F.col("cams_pm25"))
    best_rf = best.stages[-1]
    metrics = {
        "model": "RandomForestRegressor (CAMS → measured PM2.5)",
        "train_rows": train.count(), "test_rows": test.count(),
        "cv_folds": 3, "grid_size": len(grid),
        "cv_rmse": [round(x, 2) for x in cv_model.avgMetrics],
        "best_num_trees": best_rf.getOrDefault("numTrees"), "best_max_depth": best_rf.getOrDefault("maxDepth"),
        "rmse": round(ev.evaluate(pred), 2),
        "mae": round(ev.evaluate(pred, {ev.metricName: "mae"}), 2),
        "r2": round(ev.evaluate(pred, {ev.metricName: "r2"}), 3),
        "raw_rmse": round(ev.evaluate(raw), 2),
        "raw_mae": round(ev.evaluate(raw, {ev.metricName: "mae"}), 2),
        "raw_r2": round(ev.evaluate(raw, {ev.metricName: "r2"}), 3),
        "fit_seconds": round(time.time() - t, 1),
        "trained_at": datetime.now().isoformat(timespec="seconds"),
    }
    names = feats + [f"city={c}" for c in best.stages[0].labels]
    imp = sorted(zip(names, best_rf.featureImportances.toArray()), key=lambda x: -x[1])[:10]
    metrics["feature_importance"] = [{"feature": k, "importance": round(float(v), 4)} for k, v in imp]
    db.calibration_metrics.delete_many({})
    db.calibration_metrics.insert_one(metrics)
    print("  calibration:", {k: v for k, v in metrics.items() if k != "feature_importance"})
    to_mongo(db, "calibration_sample",
             pred.select("station", "city", F.date_format("hour", "yyyy-MM-dd HH:mm").alias("hour"),
                         "cams_pm25", F.col("label").alias("ground_pm25"),
                         F.round("prediction", 1).alias("calibrated_pm25"))
             .orderBy(F.rand(42)).limit(2000))
    best.write().overwrite().save(str(ROOT / "data" / "models" / "pm25_calibration_rf"))

    # apply to the newest CAMS hour of every city (calibrated "now-cast", incl. cities without stations)
    w = Window.partitionBy("city").orderBy(F.desc("event_ts"))
    latest = (air.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
              .select("city", "event_ts", F.col("pm2_5").alias("cams_pm25"), F.col("pm10").alias("cams_pm10"),
                      "humidity", "temperature", "wind_speed", "boundary_layer_height",
                      F.hour("event_ts").alias("hour_of_day"))
              .na.drop(subset=feats))
    cal = (best.transform(latest)
           .select("city", F.date_format("event_ts", "yyyy-MM-dd HH:mm").alias("event_ts"),
                   F.round("cams_pm25", 1).alias("pm25_cams"), F.round("prediction", 1).alias("pm25_calibrated"))
           .withColumn("aqi_cams", aqi_expr(F.col("pm25_cams")))
           .withColumn("aqi_calibrated", aqi_expr(F.col("pm25_calibrated")))
           .withColumn("category_calibrated", category_expr(F.col("aqi_calibrated"))))
    to_mongo(db, "calibrated_latest", cal)


# ----------------------------------------------------------------------------- 4 fires
def fire_analytics(spark, db, air_daily):
    section("4. Fires — location analytics")
    path = f"{LAKE_PATH}/fires"
    if not path_exists(spark, path):
        print("  no fire data in the lake yet — skipped")
        return
    f = spark.read.parquet(path).filter(not_demo()).dropDuplicates(["fire_id"]).cache()
    near = F.col("city_km") <= FIRE_NEAR_KM
    daily = (f.groupBy("acq_date")
             .agg(F.count("*").alias("hotspots_region"), F.sum(near.cast("int")).alias("hotspots_near_cities"),
                  F.round(F.sum("frp"), 0).alias("frp_region_mw"),
                  F.sum((F.col("confidence") == "high").cast("int")).alias("high_confidence"))
             .orderBy("acq_date"))
    to_mongo(db, "fire_daily", daily)

    # fires near each city per day, zero-filled inside the period FIRMS data covers
    lo, hi = f.agg(F.min("acq_date"), F.max("acq_date")).first()
    per_city = (f.filter(near).groupBy(F.col("nearest_city").alias("city"), F.col("acq_date").alias("date"))
                .agg(F.count("*").alias("fires"), F.sum("frp").alias("frp")))
    pm = air_daily.filter(F.col("date").between(lo, hi))
    w = Window.partitionBy("city").orderBy("date")
    x = (pm.join(per_city, ["city", "date"], "left").na.fill(0, ["fires", "frp"])
         .withColumn("fires_prev_day", F.lag("fires", 1).over(w)))
    corr = (x.groupBy("city")
            .agg(F.count("*").alias("days"), F.sum("fires").alias("fires_total"),
                 F.round(F.corr("pm25_24h", "fires"), 3).alias("r_same_day"),
                 F.round(F.corr("pm25_24h", "fires_prev_day"), 3).alias("r_next_day"))
            .orderBy(F.desc("fires_total")))
    to_mongo(db, "fire_pm_corr", corr)
    corr.show(20, truncate=False)

    # spatial binning: 0.25° grid cells, last 7 days (hotspot density map)
    recent = f.filter(F.col("acq_date") >= F.date_sub(F.lit(hi), 6))
    grid = (recent.withColumn("cell_lat", F.floor(F.col("latitude") / 0.25) * 0.25 + 0.125)
            .withColumn("cell_lon", F.floor(F.col("longitude") / 0.25) * 0.25 + 0.125)
            .groupBy("cell_lat", "cell_lon")
            .agg(F.count("*").alias("hotspots"), F.round(F.sum("frp"), 0).alias("frp_mw")))
    to_mongo(db, "fire_grid", grid)


# ----------------------------------------------------------------------------- 5 AOD
def aod_analytics(spark, db, air_daily):
    section("5. NASA POWER — 25-year aerosol optical depth")
    base = f"{RAW_PATH}/nasa_power"
    if not path_exists(spark, f"{base}/_SUCCESS"):
        print("  NASA POWER raw data not loaded yet — skipped")
        return None
    p = (spark.read.json(f"{base}/power_daily_*.jsonl")
         .withColumn("date", F.to_date("date"))
         .withColumn("year", F.year("date")).withColumn("month", F.month("date")).cache())
    yearly = (p.filter(F.col("aod_55").isNotNull()).groupBy("city", "province", "year")
              .agg(F.count("*").alias("days"), F.round(F.avg("aod_55"), 3).alias("aod_mean"),
                   F.sum((F.col("aod_55") >= 0.5).cast("int")).alias("hazy_days"),
                   F.round(F.avg("t2m"), 2).alias("t2m_mean"))
              .orderBy("city", "year"))
    to_mongo(db, "aod_yearly", yearly)
    full = yearly.filter("days >= 300")    # complete years only for the trend
    trend = (full.groupBy("city", "province")
             .agg(F.round(F.expr("regr_slope(aod_mean, year)") * 10, 4).alias("aod_change_per_decade"),
                  F.round(F.expr("regr_r2(aod_mean, year)"), 3).alias("r2"),
                  F.round(F.expr("regr_slope(hazy_days, year)") * 10, 1).alias("hazy_days_change_per_decade"),
                  F.min("year").alias("from_year"), F.max("year").alias("to_year"),
                  F.round(F.avg("aod_mean"), 3).alias("aod_mean"))
             .orderBy(F.desc("aod_change_per_decade")))
    to_mongo(db, "aod_trend", trend)
    trend.show(20, truncate=False)
    to_mongo(db, "aod_monthly", p.groupBy("city", "month").agg(F.round(F.avg("aod_55"), 3).alias("aod_mean"))
             .orderBy("city", "month"))
    # satellite column aerosol vs model surface PM2.5 over the period both exist
    both = p.select("city", "date", "aod_55").join(air_daily, ["city", "date"])
    to_mongo(db, "aod_pm_corr", both.groupBy("city")
             .agg(F.count("*").alias("days"), F.round(F.corr("aod_55", "pm25_24h"), 3).alias("pearson_r"))
             .orderBy(F.desc("pearson_r")))
    return p.groupBy("city").agg(F.avg("aod_55").alias("aod_longterm"))


# ----------------------------------------------------------------------------- 6 KMeans
def city_clusters(spark, db, air, aod_city):
    section("6. MLlib KMeans — pollution regimes")
    month = air.groupBy("city").pivot("month_num", list(range(1, 13))).agg(F.avg("pm2_5"))
    month = month.toDF("city", *[f"m{i:02d}" for i in range(1, 13)])
    hourly = air.groupBy("city", "hour").agg(F.avg("pm2_5").alias("v"))
    mean = air.groupBy("city").agg(F.avg("pm2_5").alias("annual_mean"))
    shape = (hourly.join(mean, "city").withColumn("v", F.col("v") / F.col("annual_mean"))   # diurnal *shape*
             .groupBy("city").pivot("hour", list(range(24))).agg(F.first("v")))
    shape = shape.toDF("city", *[f"h{i:02d}" for i in range(24)])
    feats = [f"m{i:02d}" for i in range(1, 13)] + [f"h{i:02d}" for i in range(24)]
    data = month.join(shape, "city").join(mean, "city")
    if aod_city is not None:
        data = data.join(aod_city, "city", "left")
        feats.append("aod_longterm")
    data = data.na.fill(0.0, feats).cache()
    vec = VectorAssembler(inputCols=feats, outputCol="raw").transform(data)
    vec = StandardScaler(inputCol="raw", outputCol="features", withMean=True, withStd=True).fit(vec).transform(vec)
    ev = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    best = None
    for k in (2, 3, 4):
        model = KMeans(k=k, seed=42, featuresCol="features").fit(vec)
        sil = ev.evaluate(model.transform(vec))
        print(f"  k={k}: silhouette {sil:.3f}")
        if best is None or sil > best[1]:
            best = (k, sil, model)
    k, sil, model = best
    out = model.transform(vec).select("city", F.col("prediction").alias("cluster"),
                                      F.round("annual_mean", 1).alias("annual_mean"))
    # name clusters by their mean pollution level
    rank = (out.groupBy("cluster").agg(F.avg("annual_mean").alias("m"))
            .withColumn("rank", F.row_number().over(Window.orderBy(F.desc("m")))))
    labels = ["most polluted", "intermediate", "cleaner", "cleanest"]
    out = out.join(rank.select("cluster", "rank"), "cluster").withColumn(
        "regime", F.element_at(F.array(*[F.lit(x) for x in labels]), F.col("rank").cast("int")))
    to_mongo(db, "city_clusters", out.orderBy("rank", F.desc("annual_mean")).drop("rank"))
    db.cluster_metrics.delete_many({})
    db.cluster_metrics.insert_one({"k": k, "silhouette": round(sil, 3), "features": len(feats),
                                   "trained_at": datetime.now().isoformat(timespec="seconds")})


# ----------------------------------------------------------------------------- 7 news text analytics
STOP_EXTRA = ["nepal", "nepali", "says", "said", "new", "news", "amid", "day", "today", "year", "years",
              "also", "per", "via", "post", "times", "kathmandu", "km", "one", "two", "will"]


def news_analytics(spark, db, air_daily):
    section("7. News — text analytics")
    path = f"{LAKE_PATH}/news"
    if not path_exists(spark, path):
        print("  no news in the lake yet — skipped")
        return
    news = spark.read.parquet(path).filter(~F.col("id").startswith("demo")).dropDuplicates(["id"]).cache()
    n = news.count()
    print(f"  {n} unique articles")
    tok = RegexTokenizer(inputCol="title", outputCol="tokens", pattern="[^a-zA-Z0-9.]+", minTokenLength=3)
    sw = StopWordsRemover(inputCol="tokens", outputCol="terms",
                          stopWords=StopWordsRemover.loadDefaultStopWords("english") + STOP_EXTRA)
    terms = sw.transform(tok.transform(news)).cache()
    recent = F.col("published_ts") >= F.date_sub(F.current_date(), 7)
    exploded = terms.select("published_ts", F.explode("terms").alias("term"))
    top_all = exploded.groupBy("term").count().orderBy(F.desc("count")).limit(40).withColumn("window", F.lit("all"))
    top_7d = (exploded.filter(recent).groupBy("term").count().orderBy(F.desc("count")).limit(25)
              .withColumn("window", F.lit("7d")))
    to_mongo(db, "news_terms", top_all.unionByName(top_7d))
    to_mongo(db, "news_themes", news.select(F.explode("themes").alias("theme")).groupBy("theme").count()
             .orderBy(F.desc("count")))

    # does the news follow the air? daily article count vs national mean PM2.5 (model)
    per_day = news.groupBy("date").agg(F.count("*").alias("articles"))
    national = air_daily.groupBy("date").agg(F.avg("pm25_24h").alias("pm25_national"))
    lo = news.agg(F.min("date")).first()[0]
    j = national.filter(F.col("date") >= lo).join(per_day, "date", "left").na.fill(0, ["articles"])
    to_mongo(db, "news_daily", j.withColumn("pm25_national", F.round("pm25_national", 1)).orderBy("date"))

    if n < 30:
        print("  < 30 articles — LDA topic model skipped")
        return
    cv = CountVectorizer(inputCol="terms", outputCol="tf", vocabSize=600, minDF=2).fit(terms)
    lda = LDA(k=4, maxIter=30, seed=42, featuresCol="tf").fit(cv.transform(terms))
    vocab = cv.vocabulary
    topics = [{"topic": int(r["topic"]), "terms": [vocab[i] for i in r["termIndices"]],
               "weights": [round(float(w), 4) for w in r["termWeights"]]}
              for r in lda.describeTopics(8).collect()]
    to_mongo(db, "news_topics", topics)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only-ground", action="store_true", help="run only the ground-station batch view")
    ap.add_argument("--include-demo", action="store_true", help="include demo-simulator stations")
    args = ap.parse_args()
    t0 = time.time()
    spark = build_spark("NepAirWatch-Fusion", cassandra=True)
    db = MongoClient(MONGO_URI)[MONGO_DB]
    if args.only_ground:
        ground_views(spark, db, args.include_demo)
        print(f"\nGround batch view finished in {time.time() - t0:.1f}s")
        spark.stop()
        return
    air = (spark.read.parquet(f"{LAKE_PATH}/air_quality").filter(not_demo())
           .dropDuplicates(["city", "event_ts"])
           .withColumn("hour", F.hour("event_ts")).withColumn("month_num", F.month("event_ts")).cache())
    air_daily = air.groupBy("city", "date").agg(F.avg("pm2_5").alias("pm25_24h")).cache()

    hourly = ground_views(spark, db, args.include_demo)
    cams_vs_ground(spark, db, hourly, air)
    fire_analytics(spark, db, air_daily)
    aod_city = aod_analytics(spark, db, air_daily)
    city_clusters(spark, db, air, aod_city)
    news_analytics(spark, db, air_daily)

    db.job_runs.insert_one({"job": "fusion_analytics", "seconds": round(time.time() - t0, 1),
                            "at": datetime.now()})
    print(f"\nFusion analytics finished in {time.time() - t0:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()
