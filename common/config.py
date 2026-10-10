"""Shared configuration for NepAirWatch."""
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = os.getenv("TOPIC_RAW", "nepal.air.raw")
TOPIC_ALERTS = os.getenv("TOPIC_ALERTS", "nepal.air.alerts")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "nepairwatch")

# Data lake (Parquet). Swap for hdfs://namenode:9000/nepairwatch in a cluster.
LAKE_PATH = os.getenv("LAKE_PATH", str(ROOT / "data" / "lake"))
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", str(ROOT / "data" / "checkpoints"))

SPARK_KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"

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

# Thresholds (µg/m³) — Nepal National Ambient Air Quality Standard 2012 (24-h PM2.5 = 40)
# and WHO 2021 guideline (24-h PM2.5 = 15).
NEPAL_PM25_24H = 40.0
WHO_PM25_24H = 15.0
ALERT_AQI = 151  # "Unhealthy" and above triggers an alert
