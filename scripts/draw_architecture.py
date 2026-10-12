"""Render the NepAirWatch block diagram → docs/architecture.png"""
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = Path(__file__).resolve().parent.parent / "docs" / "architecture.png"

fig, ax = plt.subplots(figsize=(20, 12), dpi=150)
ax.set_xlim(0, 200)
ax.set_ylim(-1, 120)
ax.axis("off")

C = {"src": "#e8f1fb", "ing": "#fdecea", "speed": "#fff3e0", "batch": "#fff8e1", "store": "#e8f5e9",
     "serve": "#f3e5f5", "edge": "#37474f"}


def box(x, y, w, h, title, body="", color="#fff", tcolor="#263238", fs=10.5, lw=1.5):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.35,rounding_size=1.4",
                                fc=color, ec=C["edge"], lw=lw))
    ax.text(x + w / 2, y + h - 1.9, title, ha="center", va="top", fontsize=fs, weight="bold", color=tcolor)
    if body:
        ax.text(x + w / 2, y + h - 5.4, body, ha="center", va="top", fontsize=7.9, color="#37474f",
                linespacing=1.3)


def group(x, y, w, h, label, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.3,rounding_size=2",
                                fc=color, ec="#90a4ae", lw=1.2, ls="--", alpha=0.6))
    ax.text(x + 1.5, y + h - 1.0, label, ha="left", va="top", fontsize=10, weight="bold", color="#455a64")


def arrow(x1, y1, x2, y2, label="", color="#37474f", rad=0.0, lx=0, ly=0, ls="-"):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=14, lw=1.6,
                                 color=color, connectionstyle=f"arc3,rad={rad}", linestyle=ls))
    if label:
        ax.text((x1 + x2) / 2 + lx, (y1 + y2) / 2 + ly, label, fontsize=7.6, ha="center", va="center",
                color=color, bbox=dict(fc="white", ec="none", pad=0.5, alpha=0.9))


def route(points, label="", color="#37474f", lpos=None, ls="-"):
    """Polyline through the gaps between groups, arrow head on the last segment."""
    xs, ys = zip(*points)
    ax.plot(xs[:-1], ys[:-1], color=color, lw=1.6, ls=ls, solid_capstyle="round")
    arrow(points[-2][0], points[-2][1], points[-1][0], points[-1][1], color=color, ls=ls)
    if label:
        x, y = lpos or points[1]
        ax.text(x, y, label, fontsize=7.6, ha="center", va="center", color=color,
                bbox=dict(fc="white", ec="none", pad=0.5, alpha=0.9))


ax.text(100, 119, "NepAirWatch — Lambda Architecture on the Hadoop / Spark ecosystem",
        ha="center", va="top", fontsize=16, weight="bold", color="#263238")
ax.text(100, 115.2, "batch layer (complete, recomputable)  +  speed layer (low latency)  →  serving layer",
        ha="center", va="top", fontsize=10.5, color="#546e7a")

# ① sources -------------------------------------------------------------------
group(1, 4, 30, 106, "① DATA SOURCES", C["src"])
src = [
    (91, "Open-Meteo CAMS + weather", "model PM2.5/PM10/NO₂/O₃…, BLH\nhourly · 15 cities · live + 1 y"),
    (76, "DoEnv Nepal AQMS", "pollution.gov.np · ~30 stations\n1-minute PM1/2.5/10 · live + 3 wk"),
    (61, "Sensor.Community", "citizen low-cost SDS011 sensors\n~5-min · live + daily archive"),
    (46, "Google News RSS", "air-pollution headlines\n(unstructured text)"),
    (31, "NASA FIRMS", "VIIRS / MODIS fire hotspots\nNepal + Indo-Gangetic Plain"),
    (16, "NASA POWER", "satellite AOD + climate, daily\n2001 → today (25 years)"),
]
for y, t, b in src:
    box(3, y, 26, 12.5, t, b, "#ffffff", fs=9.5)
ax.text(16, 9.5, "velocity: 1 min → 1 day\nvariety: JSON · CSV · XML · text", ha="center", fontsize=8,
        color="#455a64")

# ② ingestion -----------------------------------------------------------------
group(34, 4, 31, 106, "② INGESTION", C["ing"])
box(36, 88, 27, 16, "Python producers", "aq_producer · ground_producer\nnews_producer\nbackfill / live / replay\n"
    "idempotent, lz4, acks=all", "#ffffff", fs=10)
box(36, 50, 27, 33, "Apache Kafka (KRaft)", "nepal.air.raw\nnepal.ground.raw\nnepal.fires.raw\nnepal.news.raw\n"
    "nepal.air.alerts  (fan-out)\n\n3 partitions · keyed\n7-day replayable log", "#ffffff",
    tcolor="#b71c1c", fs=11.5, lw=2.4)
box(36, 25, 27, 20, "Apache Flume", "spooldir source (FIRMS files)\nreplicating selector\nfile channel → Kafka sink\n"
    "mem channel → HDFS sink", "#ffffff", tcolor="#1a237e", fs=11, lw=2)
box(36, 7, 27, 13, "Bulk loader", "power_loader → WebHDFS\n(history skips the stream)", "#ffffff", fs=9.5)

# ③ processing ----------------------------------------------------------------
group(68, 58, 50, 52, "③a SPEED LAYER", C["speed"])
box(70, 61, 46, 44, "Spark Structured Streaming",
    "one app · 4 topics · 8 queries · event-time + watermarks\n"
    "• air: AQI (native expr) → lake, MongoDB, alerts\n• air: 1-day window → daily_stats\n"
    "• ground: → lake, Cassandra (1-min), ground_latest\n• ground: 1-hour window → Cassandra + measured alerts\n"
    "• fires: CSV parse + nearest city → lake, MongoDB geo\n• news: regex theme tagging → lake, MongoDB\n"
    "• StreamingQueryListener → stream_progress",
    "#ffffff", tcolor="#e65100", fs=11.5, lw=2.4)
group(68, 4, 50, 51, "③b BATCH LAYER", C["batch"])
box(70, 26, 46, 25, "Spark SQL + MLlib",
    "batch_analytics: exceedance, seasonality, GBT t+6 h\n   forecast, prescriptive advisories\n"
    "fusion_analytics: CAMS↔ground validation,\n   RandomForest + CrossValidator calibration,\n"
    "   fire location analytics, AOD trends, KMeans,\n   news tokenise / stop words / LDA",
    "#ffffff", tcolor="#e65100", fs=11, lw=2.2)
box(70, 7, 22, 15, "MapReduce on YARN", "Hadoop Streaming, Python\nHDFS raw → curated TSV\nyearly AOD per city", "#ffffff",
    tcolor="#1a237e", fs=9.8, lw=1.8)
box(94, 7, 22, 15, "Apache Hive", "HiveServer2 · external\ntables over HDFS\nschema-on-read SQL", "#ffffff",
    tcolor="#1a237e", fs=9.8, lw=1.8)

# ④ storage -------------------------------------------------------------------
group(121, 4, 39, 106, "④ STORAGE", C["store"])
box(123, 58, 35, 47, "HDFS data lake",
    "master dataset · NameNode + DataNode\n\nraw/      Flume FIRMS CSV, NASA POWER JSON\n"
    "lake/     Spark Parquet (immutable, append)\n   air_quality  city/date\n   ground       network/date\n"
    "   fires  acq_date · news  date\ncurated/  batch outputs, MapReduce TSV\ncheckpoints/  streaming state",
    "#ffffff", tcolor="#1b5e20", fs=11, lw=2.2)
box(123, 33, 35, 21, "Apache Cassandra",
    "wide-column time series\nground_readings ((station, day), ts)\nground_hourly (station, hour)\n"
    "speed view ← overwritten by batch view", "#ffffff", tcolor="#1b5e20", fs=11)
box(123, 7, 35, 22, "MongoDB (serving)",
    "readings · latest · alerts · daily_stats\nground_latest · fires (2dsphere) · news\n"
    "forecasts · advisories · cams_validation\naod_trend · city_clusters · news_topics", "#ffffff",
    tcolor="#1b5e20", fs=11)

# ⑤ serving -------------------------------------------------------------------
group(163, 4, 36, 106, "⑤ SERVING / USERS", C["serve"])
box(165, 62, 32, 43, "Streamlit dashboard",
    "live AQI map + Nepali advisories\nmeasured stations (Cassandra)\ncity explorer · analytics\n"
    "fires & 25-y aerosols · clusters\nforecast + recommended actions\nnews text analytics\n"
    "Hive SQL console\npipeline health (Kafka, Flume,\nHDFS, YARN, Spark)", "#ffffff", tcolor="#4a148c", fs=11.5)
box(165, 37, 32, 20, "Ops consoles", "Kafka-UI :8080 · Spark :4040\nHDFS :9870 · YARN :8088\nHiveServer2 :10002",
    "#ffffff")
box(165, 7, 32, 25, "Stakeholders", "citizens · schools · hospitals\nMoHP · DoEnv · municipalities\n"
    "SMS / app services subscribe\nto nepal.air.alerts", "#ffffff")

# arrows ----------------------------------------------------------------------
for y, ty in ((97, 99), (82, 96), (67, 93), (52, 90)):
    arrow(29, y, 36, ty)
arrow(29, 37, 36, 35, "files", ly=1.6)
arrow(29, 22, 36, 13)
arrow(49.5, 88, 49.5, 83.6, "produce", lx=6)
arrow(49.5, 45, 49.5, 49.6, "Kafka sink", lx=8)
arrow(63, 72, 70, 82, "subscribe", ly=1.5)
arrow(70, 64, 63, 60, "alerts", color="#b71c1c", ly=-2)
# raw data into HDFS through the corridor between the speed and batch layers
route([(63, 33), (66, 33), (66, 56.5), (121.5, 56.5), (125, 58.2)], "Flume HDFS sink + WebHDFS",
      color="#1a237e", lpos=(93, 56.5))
route([(63, 12), (66, 12), (66, 33)], color="#1a237e")
arrow(116, 92, 123, 92, "Parquet", ly=2)
arrow(116, 70, 123, 47, "upsert", lx=-0.5, ly=3)
route([(116, 64), (119.5, 64), (119.5, 22), (123, 22)], color="#37474f")
arrow(124, 58, 116, 47, "read lake", lx=1.5, ly=-1)
arrow(116, 38, 123, 38, "batch view", ly=2, color="#1b5e20")
arrow(116, 30, 123, 25, "", color="#1b5e20")
arrow(158, 44, 165, 80, "CQL", lx=-1)
arrow(158, 18, 165, 72, "query", lx=-1)
route([(105, 7), (105, 1.8), (161.5, 1.8), (161.5, 66), (165, 66)], "HiveQL", color="#1a237e",
      lpos=(133, 1.8))
arrow(181, 37, 181, 32.5)

fig.savefig(OUT, bbox_inches="tight", facecolor="white")
print("saved", OUT)
