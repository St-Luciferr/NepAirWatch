"""Render the NepAirWatch block diagram → docs/architecture.png"""
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.patches import FancyArrowPatch, FancyBboxPatch

OUT = Path(__file__).resolve().parent.parent / "docs" / "architecture.png"

fig, ax = plt.subplots(figsize=(17, 9.6), dpi=150)
ax.set_xlim(0, 170)
ax.set_ylim(0, 96)
ax.axis("off")

C = {"src": "#e8f1fb", "kafka": "#fdecea", "spark": "#fff3e0", "store": "#e8f5e9",
     "serve": "#f3e5f5", "edge": "#37474f"}


def box(x, y, w, h, title, body="", color="#fff", tcolor="#263238", fs=11, lw=1.6):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.4,rounding_size=1.6",
                                fc=color, ec=C["edge"], lw=lw))
    ax.text(x + w / 2, y + h - 2.6, title, ha="center", va="top", fontsize=fs, weight="bold", color=tcolor)
    if body:
        ax.text(x + w / 2, y + h - 6.6, body, ha="center", va="top", fontsize=8.6, color="#37474f",
                linespacing=1.35)


def group(x, y, w, h, label, color):
    ax.add_patch(FancyBboxPatch((x, y), w, h, boxstyle="round,pad=0.3,rounding_size=2",
                                fc=color, ec="#90a4ae", lw=1.2, ls="--", alpha=0.55))
    ax.text(x + 1.5, y + h - 1.2, label, ha="left", va="top", fontsize=10.5, weight="bold", color="#455a64")


def arrow(x1, y1, x2, y2, label="", color="#37474f", rad=0.0, lx=0, ly=0):
    ax.add_patch(FancyArrowPatch((x1, y1), (x2, y2), arrowstyle="-|>", mutation_scale=16, lw=1.8,
                                 color=color, connectionstyle=f"arc3,rad={rad}"))
    if label:
        ax.text((x1 + x2) / 2 + lx, (y1 + y2) / 2 + ly, label, fontsize=8.2, ha="center", va="center",
                color=color, bbox=dict(fc="white", ec="none", pad=0.6, alpha=0.9))


ax.text(85, 94.5, "NepAirWatch — Big Data Architecture (Kappa-style streaming + batch on one data lake)",
        ha="center", va="top", fontsize=15, weight="bold", color="#263238")

# 1 ── Data sources
group(1, 8, 27, 80, "① DATA SOURCES", C["src"])
box(3, 64, 23, 18, "Air-Quality API",
    "Open-Meteo (CAMS model)\nPM2.5, PM10, CO, NO₂,\nSO₂, O₃, dust (hourly)", "#ffffff")
box(3, 42, 23, 18, "Weather API",
    "Open-Meteo model:\ntemperature, humidity,\nwind, rain, mixing\nheight (hourly)", "#ffffff")
box(3, 21, 23, 17, "Future sources",
    "DoEnv / pollution.gov.np\nground stations, low-cost\nIoT sensors, NASA FIRMS fires", "#ffffff")
box(3, 10, 23, 8.5, "15 cities × 7 provinces", "", "#ffffff", fs=9.5)

# 2 ── Ingestion
group(31, 8, 30, 80, "② INGESTION", C["kafka"])
box(33, 60, 26, 22, "Python Producer",
    "aq_producer.py\nmodes: backfill / live / replay\nmerges AQ + weather → JSON\nidempotent, lz4, acks=all", "#ffffff")
box(33, 25, 26, 30, "Apache Kafka (KRaft)",
    "topic  nepal.air.raw\n3 partitions, key = city\n7-day retention, replayable\n\ntopic  nepal.air.alerts\n(alerts fan-out for SMS /\nmobile / other consumers)",
    "#ffffff", tcolor="#b71c1c", fs=12, lw=2.4)
box(33, 10, 26, 10.5, "Landing zone (raw JSONL)", "immutable source copy", "#ffffff", fs=9.5)

# 3 ── Processing
group(64, 8, 40, 80, "③ PROCESSING — Apache Spark 3.5", C["spark"])
box(66, 48, 36, 34, "Spark Structured Streaming",
    "stream_processor.py (micro-batches)\n• parse JSON with schema, validate\n• PM2.5 → US-EPA AQI + category\n"
    "• Q1  append → Parquet lake\n• Q2  foreachBatch → MongoDB\n      upsert + alert detection\n"
    "• Q3  1-day event-time window\n      + 3 h watermark → daily stats",
    "#ffffff", tcolor="#e65100", fs=12, lw=2.4)
box(66, 10, 36, 34, "Spark Batch + MLlib",
    "batch_analytics.py\n• de-duplicate, aggregate\n• seasonality, diurnal cycle\n"
    "• exceedance vs Nepal NAAQS / WHO\n• weather-driver correlation\n"
    "• GBTRegressor: PM2.5 t+6 h\n   (early-warning forecast)",
    "#ffffff", tcolor="#e65100", fs=12, lw=2.4)

# 4 ── Storage
group(107, 8, 30, 80, "④ STORAGE", C["store"])
box(109, 48, 26, 34, "Parquet Data Lake",
    "data/lake/air_quality\npartitioned city / date\ncolumnar + compressed\n\ncurated/ zone:\nmonthly, daily, exceedance\n(HDFS / S3 in production)",
    "#ffffff", tcolor="#1b5e20", fs=12)
box(109, 10, 26, 34, "MongoDB (serving)",
    "readings · latest · alerts\ndaily_stats · stream_batches\nexceedance · monthly_city\nhourly_profile · forecasts\nmodel_metrics · weather_corr",
    "#ffffff", tcolor="#1b5e20", fs=12)

# 5 ── Serving
group(140, 8, 29, 80, "⑤ SERVING / USERS", C["serve"])
box(142, 50, 25, 32, "Streamlit Dashboard",
    "live map & AQI table\nNepali health advisories\nalert feed, city explorer\nanalytics & forecasts\npipeline health",
    "#ffffff", tcolor="#4a148c", fs=12)
box(142, 10, 25, 16, "Kafka-UI", "ops console: topics,\npartitions, messages", "#ffffff")
box(142, 30, 25, 16, "Stakeholders",
    "Citizens · schools · MoHP\nDoEnv · KMC · hospitals", "#ffffff")

# arrows
arrow(26, 73, 33, 71, "HTTPS/JSON")
arrow(26, 51, 33, 67)
arrow(46, 60, 46, 55.5, "produce", lx=5)
arrow(46, 25, 46, 20.8, "copy", lx=4)
arrow(59, 45, 66, 62, "subscribe", ly=2)
arrow(102, 70, 109, 70, "Q1 Parquet", ly=2.3)
arrow(102, 52, 109, 30, "Q2/Q3\nupsert", lx=-0.3, ly=8)
arrow(66, 50, 59, 30, "alerts", color="#b71c1c", rad=-0.15, lx=-3)
arrow(109, 52, 102, 30, "read\nlake", lx=-0.3, ly=-8)
arrow(102, 20, 109, 20, "results", ly=-2.2)
arrow(135, 30, 142, 66, "query", lx=-1)
arrow(154.5, 50, 154.5, 46.5)

fig.savefig(OUT, bbox_inches="tight", facecolor="white")
print("saved", OUT)
