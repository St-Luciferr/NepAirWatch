"""Shared configuration for NepAirWatch."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# ------------------------------------------------------------------ ingestion (Kafka)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "nepal.air.raw")            # Open-Meteo CAMS model + weather, hourly
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "nepal.air.alerts")   # alert fan-out
TOPIC_GROUND = os.getenv("TOPIC_GROUND", "nepal.ground.raw")   # DoEnv stations + Sensor.Community, ~1 min
TOPIC_FIRES = os.getenv("TOPIC_FIRES", "nepal.fires.raw")      # NASA FIRMS hotspots (CSV lines, via Flume)
TOPIC_NEWS = os.getenv("TOPIC_NEWS", "nepal.news.raw")         # news headlines (text analytics)
ALL_TOPICS = [TOPIC_RAW, TOPIC_ALERTS, TOPIC_GROUND, TOPIC_FIRES, TOPIC_NEWS]

# ------------------------------------------------------------------ storage
MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "nepairwatch")

CASSANDRA_HOSTS = [h for h in os.getenv("CASSANDRA_HOSTS", "localhost").split(",") if h]
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "nepairwatch")

# Data lake. Local paths for development; docker compose sets hdfs://namenode:9000/nepairwatch/...
LAKE_PATH = os.getenv("LAKE_PATH", str(ROOT / "data" / "lake"))              # Spark-written Parquet
RAW_PATH = os.getenv("RAW_PATH", str(ROOT / "data" / "raw"))                 # raw zone (bulk historical files)
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", str(ROOT / "data" / "checkpoints"))
WEBHDFS_URL = os.getenv("WEBHDFS_URL", "http://localhost:9870")             # used when a path is hdfs://
HDFS_USER = os.getenv("HDFS_USER", "root")

# Flume spooling directory (FIRMS files are dropped here, Flume ships them to Kafka + HDFS)
SPOOL_DIR = os.getenv("SPOOL_DIR", str(ROOT / "data" / "spool"))
STATE_DIR = Path(os.getenv("STATE_DIR", str(ROOT / "data" / "state")))       # producers' resume state

# ------------------------------------------------------------------ query / ops endpoints (optional)
HIVE_HOST = os.getenv("HIVE_HOST", "")            # e.g. "hive"; empty = Hive not deployed
HIVE_PORT = int(os.getenv("HIVE_PORT", "10000"))
YARN_URL = os.getenv("YARN_URL", "")              # e.g. http://resourcemanager:8088
FLUME_METRICS_URL = os.getenv("FLUME_METRICS_URL", "")  # e.g. http://flume:34545/metrics

SPARK_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"
SPARK_CASSANDRA_PKG = "com.datastax.spark:spark-cassandra-connector_2.12:3.5.1"

# ------------------------------------------------------------------ external APIs
FIRMS_MAP_KEY = os.getenv("FIRMS_MAP_KEY", "")    # optional: unlocks FIRMS history older than 7 days
# Fire region of interest: Nepal + the Indo-Gangetic Plain upwind of it (W, S, E, N)
FIRE_BBOX = (77.0, 24.0, 92.0, 31.0)

# Monitoring locations: major cities across all 7 provinces of Nepal.
CITIES = [
    # name,          province,        lat,     lon
    ("Kathmandu",    "Bagmati",       27.7172, 85.3240),
    ("Lalitpur",     "Bagmati",       27.6644, 85.3188),
    ("Bhaktapur",    "Bagmati",       27.6710, 85.4298),
    ("Hetauda",      "Bagmati",       27.4287, 85.0322),
    ("Bharatpur",    "Bagmati",       27.6833, 84.4333),
    ("Pokhara",      "Gandaki",       28.2096, 83.9856),
    ("Biratnagar",   "Koshi",         26.4525, 87.2718),
    ("Dharan",       "Koshi",         26.8125, 87.2836),
    ("Janakpur",     "Madhesh",       26.7288, 85.9263),
    ("Birgunj",      "Madhesh",       27.0104, 84.8770),
    ("Butwal",       "Lumbini",       27.7006, 83.4483),
    ("Nepalgunj",    "Lumbini",       28.0500, 81.6167),
    ("Surkhet",      "Karnali",       28.6010, 81.6336),
    ("Dhangadhi",    "Sudurpashchim", 28.6833, 80.6000),
    ("Mahendranagar","Sudurpashchim", 28.9634, 80.1710),
]
KATHMANDU_VALLEY = ["Kathmandu", "Lalitpur", "Bhaktapur"]
TERAI = ["Biratnagar", "Janakpur", "Birgunj", "Nepalgunj", "Dhangadhi", "Mahendranagar", "Butwal"]

# A ground station is matched to a city (for CAMS validation) when it is within this distance.
STATION_CITY_MAX_KM = 15.0
FIRE_NEAR_KM = 100.0   # "fires near a city" radius used in location analytics

# Thresholds (µg/m³) — Nepal National Ambient Air Quality Standard 2012 (24-h PM2.5 = 40)
# and WHO 2021 guideline (24-h PM2.5 = 15).
NEPAL_PM25_24H = 40.0
WHO_PM25_24H = 15.0
ALERT_AQI = 151  # "Unhealthy" and above triggers an alert
