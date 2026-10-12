"""Helpers shared by the Spark jobs."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import Column, SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from common.config import (CASSANDRA_HOSTS, CITIES, SPARK_CASSANDRA_PKG,  # noqa: E402
                           SPARK_KAFKA_PKG)

RAW_SCHEMA = T.StructType([
    T.StructField("city", T.StringType()),
    T.StructField("province", T.StringType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("pm2_5", T.DoubleType()),
    T.StructField("pm10", T.DoubleType()),
    T.StructField("co", T.DoubleType()),
    T.StructField("no2", T.DoubleType()),
    T.StructField("so2", T.DoubleType()),
    T.StructField("o3", T.DoubleType()),
    T.StructField("dust", T.DoubleType()),
    T.StructField("temperature", T.DoubleType()),
    T.StructField("humidity", T.DoubleType()),
    T.StructField("wind_speed", T.DoubleType()),
    T.StructField("precipitation", T.DoubleType()),
    T.StructField("boundary_layer_height", T.DoubleType()),
    T.StructField("source", T.StringType()),
    T.StructField("ingest_time", T.StringType()),
])

# US-EPA 2024 PM2.5 breakpoints: (C_lo, C_hi, I_lo, I_hi)
_BP = [(0.0, 9.0, 0, 50), (9.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
       (55.5, 125.4, 151, 200), (125.5, 225.4, 201, 300), (225.5, 325.4, 301, 500)]
_CATS = [(50, "Good"), (100, "Moderate"), (150, "Unhealthy for Sensitive Groups"),
         (200, "Unhealthy"), (300, "Very Unhealthy")]


def aqi_expr(pm: Column) -> Column:
    """Native Spark expression for PM2.5 → AQI (no Python UDF, so it stays in the JVM)."""
    c = F.round(pm, 1)
    expr = F.lit(500)
    for c_lo, c_hi, i_lo, i_hi in reversed(_BP):
        val = (F.lit((i_hi - i_lo) / (c_hi - c_lo)) * (F.greatest(c, F.lit(c_lo)) - c_lo) + i_lo)
        expr = F.when(c <= c_hi, F.round(val).cast("int")).otherwise(expr)
    return F.when(pm.isNull(), None).otherwise(expr)


def category_expr(aqi: Column) -> Column:
    expr = F.lit("Hazardous")
    for hi, name in reversed(_CATS):
        expr = F.when(aqi <= hi, name).otherwise(expr)
    return expr


# nepal.ground.raw — DoEnv stations + Sensor.Community (producer/ground_producer.py)
GROUND_SCHEMA = T.StructType([
    T.StructField("network", T.StringType()),
    T.StructField("station_id", T.StringType()),
    T.StructField("station", T.StringType()),
    T.StructField("lat", T.DoubleType()),
    T.StructField("lon", T.DoubleType()),
    T.StructField("city", T.StringType()),
    T.StructField("city_km", T.DoubleType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("pm1", T.DoubleType()),
    T.StructField("pm2_5", T.DoubleType()),
    T.StructField("pm10", T.DoubleType()),
    T.StructField("source", T.StringType()),
    T.StructField("ingest_time", T.StringType()),
])

# nepal.fires.raw — CSV lines from Flume (producer/firms_fetcher.py COLUMNS)
FIRES_CSV_DDL = ("fire_id STRING, latitude DOUBLE, longitude DOUBLE, acq_time_iso STRING, satellite STRING, "
                 "instrument STRING, confidence STRING, frp DOUBLE, brightness DOUBLE, daynight STRING, "
                 "source STRING")

# nepal.news.raw — headlines (producer/news_producer.py)
NEWS_SCHEMA = T.StructType([
    T.StructField("id", T.StringType()),
    T.StructField("title", T.StringType()),
    T.StructField("link", T.StringType()),
    T.StructField("publisher", T.StringType()),
    T.StructField("published", T.StringType()),
    T.StructField("query", T.StringType()),
    T.StructField("ingest_time", T.StringType()),
])

# Rule-based headline tagging (keyword → theme), evaluated as native Spark expressions.
NEWS_THEMES = {
    "fires": r"(?i)\b(fire|fires|wildfire|forest fire|blaze|burning)\b",
    "smog_haze": r"(?i)\b(smog|haze|hazy|visibility|smoke)\b",
    "health": r"(?i)\b(health|hospital|asthma|respiratory|lung|mask|deaths?)\b",
    "transport": r"(?i)\b(vehicle|traffic|diesel|emission|electric)\b",
    "industry": r"(?i)\b(brick|kiln|factory|industr\w*|cement)\b",
    "policy": r"(?i)\b(government|ministry|policy|ban|standard|court|closure|closed|schools?)\b",
    "cross_border": r"(?i)\b(india|indo-gangetic|stubble|punjab|bihar|cross-border|transboundary)\b",
}


def not_demo(col="source") -> Column:
    """True for real data. Demo-simulator rows (source "demo/…") are kept out of every batch result."""
    return ~F.coalesce(F.col(col), F.lit("")).startswith("demo")


def haversine_expr(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """Great-circle distance in km as a native Spark expression (location analytics)."""
    dlat = F.radians(lat2 - lat1)
    dlon = F.radians(lon2 - lon1)
    a = (F.pow(F.sin(dlat / 2), 2)
         + F.cos(F.radians(lat1)) * F.cos(F.radians(lat2)) * F.pow(F.sin(dlon / 2), 2))
    return F.lit(2 * 6371.0) * F.asin(F.sqrt(a))


def cities_df(spark):
    return spark.createDataFrame([(c, p, la, lo) for c, p, la, lo in CITIES],
                                 "c_city STRING, c_province STRING, c_lat DOUBLE, c_lon DOUBLE")


def with_nearest_city(df, spark, lat="latitude", lon="longitude"):
    """Attach nearest_city / city_km to every row (broadcast cross-join with the 15 cities)."""
    key = "_row_id"
    d = df.withColumn(key, F.monotonically_increasing_id())
    j = (d.crossJoin(F.broadcast(cities_df(spark)))
         .withColumn("_km", haversine_expr(F.col(lat), F.col(lon), F.col("c_lat"), F.col("c_lon"))))
    w = Window.partitionBy(key).orderBy("_km")
    return (j.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
            .withColumn("nearest_city", F.col("c_city"))
            .withColumn("city_km", F.round("_km", 1))
            .drop("_rn", "_km", key, "c_city", "c_province", "c_lat", "c_lon"))


def build_spark(app_name: str, kafka: bool = False, cassandra: bool = False) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    b = (SparkSession.builder.appName(app_name)
         .master(os.getenv("SPARK_MASTER", "local[*]"))
         .config("spark.sql.session.timeZone", "Asia/Kathmandu")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
         .config("spark.ui.showConsoleProgress", "false")
         # single-DataNode HDFS in the demo cluster (ignored for the local file system)
         .config("spark.hadoop.dfs.replication", "1"))
    pkgs = ([SPARK_KAFKA_PKG] if kafka else []) + ([SPARK_CASSANDRA_PKG] if cassandra else [])
    if pkgs:
        b = b.config("spark.jars.packages", ",".join(pkgs))
    if cassandra:
        b = (b.config("spark.cassandra.connection.host", ",".join(CASSANDRA_HOSTS))
             .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions"))
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def to_cassandra(df, table, keyspace=None):
    from common.config import CASSANDRA_KEYSPACE
    (df.write.format("org.apache.spark.sql.cassandra")
     .options(keyspace=keyspace or CASSANDRA_KEYSPACE, table=table)
     .mode("append").save())


def to_mongo(db, name, df):
    """Replace a MongoDB collection with the rows of a (small, aggregated) DataFrame or list of dicts."""
    from datetime import datetime
    docs = df if isinstance(df, list) else [r.asDict(recursive=True) for r in df.collect()]
    for d in docs:
        for k, v in d.items():
            if hasattr(v, "isoformat") and not isinstance(v, datetime):
                d[k] = v.isoformat()          # date → "YYYY-MM-DD"
            elif isinstance(v, datetime) and v.tzinfo is not None:
                d[k] = v.replace(tzinfo=None)
    db[name].delete_many({})
    if docs:
        db[name].insert_many(docs)
    print(f"  → mongo.{name}: {len(docs)} docs", flush=True)


def path_exists(spark, path):
    jvm = spark._jvm
    p = jvm.org.apache.hadoop.fs.Path(path)
    return p.getFileSystem(spark._jsc.hadoopConfiguration()).exists(p)
