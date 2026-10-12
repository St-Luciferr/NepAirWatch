"""Builders for demo / simulated messages (demo control panel + demo simulator).

Every record is tagged so it never pollutes real results: source "demo/…", ground network "demo",
station ids "demo-*", fire ids "demo_*", news ids "demo-*". The batch layer filters these out and
`scripts/demo_cleanup.py` removes them from the serving stores.
"""
import csv
import io
import math
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.config import SPOOL_DIR, TOPIC_FIRES
from producer.common import now_utc_iso, publish

NPT = timezone(timedelta(hours=5, minutes=45))

# target AQI used when a category is picked (middle of each band)
LEVELS = {"Good": 30, "Moderate": 75, "Unhealthy for Sensitive Groups": 125, "Unhealthy": 175,
          "Very Unhealthy": 250, "Hazardous": 380}

DEMO_STATIONS = [  # id, name, lat, lon
    ("demo-1", "DEMO Thamel", 27.7154, 85.3123),
    ("demo-2", "DEMO Koteshwor", 27.6788, 85.3493),
    ("demo-3", "DEMO Bhaktapur Durbar", 27.6722, 85.4280),
    ("demo-4", "DEMO Pokhara Lakeside", 28.2090, 83.9590),
    ("demo-5", "DEMO Biratnagar Bazaar", 26.4550, 87.2800),
]


def air_message(city, province, lat, lon, pm25, source="demo/control-panel", when=None):
    """A nepal.air.raw reading with weather that is physically consistent with the PM level."""
    when = when or datetime.now(NPT)
    return {"city": city, "province": province, "lat": lat, "lon": lon,
            "event_time": when.astimezone(NPT).isoformat(timespec="seconds"),
            "pm2_5": round(pm25, 1), "pm10": round(pm25 * 1.5, 1), "co": round(250 + pm25 * 6, 0),
            "no2": round(8 + pm25 * 0.15, 1), "so2": round(2 + pm25 * 0.05, 1),
            "o3": round(max(10.0, 70 - pm25 * 0.2), 1), "dust": round(pm25 * 0.05, 1),
            "temperature": 16.0, "humidity": 60.0, "wind_speed": round(max(0.5, 8 - pm25 / 40), 1),
            "precipitation": 0.0, "boundary_layer_height": round(max(120.0, 1400 - pm25 * 4.2), 0),
            "source": source, "ingest_time": now_utc_iso()}


def ground_message(sid, name, lat, lon, pm25, source="demo/control-panel", when=None):
    """A nepal.ground.raw minute reading for a demo station (city=None keeps it out of calibration)."""
    when = when or datetime.now(timezone.utc)
    return {"network": "demo", "station_id": sid, "station": name, "lat": lat, "lon": lon,
            "city": None, "city_km": None, "event_time": when.astimezone(timezone.utc).isoformat(timespec="seconds"),
            "pm1": round(pm25 * 0.7, 1), "pm2_5": round(pm25, 1), "pm10": round(pm25 * 1.5, 1),
            "source": source, "ingest_time": now_utc_iso()}


def fire_rows(lat0, lon0, n, radius_km, frp=(5, 60), confidence="high", source="demo/firms", seed=None):
    """n FIRMS-layout hotspots scattered within radius_km of a point (rows in firms_fetcher.COLUMNS order)."""
    rng = random.Random(seed)
    now = datetime.now(timezone.utc)
    rows = []
    for i in range(n):
        ang, km = rng.uniform(0, 2 * math.pi), radius_km * math.sqrt(rng.random())
        lat = lat0 + km / 111 * math.sin(ang)
        lon = lon0 + km / (111 * math.cos(math.radians(lat0))) * math.cos(ang)
        rows.append([f"demo_{int(now.timestamp() * 1000)}_{i}", f"{lat:.5f}", f"{lon:.5f}",
                     now.isoformat(timespec="seconds"), "N", "VIIRS", confidence,
                     f"{rng.uniform(*frp):.1f}", f"{rng.uniform(320, 367):.1f}", "D", source])
    return rows


def csv_line(row):
    buf = io.StringIO()
    csv.writer(buf, lineterminator="").writerow(row)
    return buf.getvalue()


def flume_available():
    return (Path(SPOOL_DIR) / "firms").is_dir()


def send_fires(producer, rows, via_flume=True):
    """Drop a FIRMS-format file into the Flume spool dir (→ Kafka + HDFS) or publish straight to Kafka."""
    lines = [csv_line(r) for r in rows]
    if via_flume and flume_available():
        spool = Path(SPOOL_DIR) / "firms"
        name = f"demo_fires_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}"
        (spool / f"{name}.tmp").write_text("\n".join(lines) + "\n")
        (spool / f"{name}.tmp").rename(spool / f"{name}.csv")
        return "Flume spool dir"
    for line in lines:
        publish(producer, TOPIC_FIRES, line.split(",")[3][:10], line)
    producer.flush()
    return "Kafka (direct)"


def news_message(title, publisher="NepAirWatch demo"):
    now = datetime.now(timezone.utc)
    return {"id": f"demo-{int(now.timestamp() * 1000)}", "title": title, "link": "https://example.org/demo",
            "publisher": publisher, "published": now.isoformat(timespec="seconds"), "query": "demo",
            "ingest_time": now_utc_iso()}
