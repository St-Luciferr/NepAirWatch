"""Helpers shared by the Spark jobs."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pyspark.sql import Column, SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from pyspark.sql import types as T  # noqa: E402

from common.config import SPARK_KAFKA_PKG  # noqa: E402

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


def build_spark(app_name: str, kafka: bool = False) -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    b = (SparkSession.builder.appName(app_name)
         .master(os.getenv("SPARK_MASTER", "local[*]"))
         .config("spark.sql.session.timeZone", "Asia/Kathmandu")
         .config("spark.sql.shuffle.partitions", "8")
         .config("spark.driver.memory", "2g")
         .config("spark.ui.showConsoleProgress", "false"))
    if kafka:
        b = b.config("spark.jars.packages", SPARK_KAFKA_PKG)
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
