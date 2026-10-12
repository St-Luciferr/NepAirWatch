"""
NepAirWatch — Kafka producer.

Collects hourly air-quality and weather observations for 15 Nepali cities from the
Open-Meteo APIs (CAMS air-quality model + weather model) and publishes one JSON
message per city-hour to the Kafka topic `nepal.air.raw`.

Modes
-----
  backfill  Download N days of history and publish it (bulk historical load).
  live      Poll the APIs every --interval seconds and publish the latest hour.
  replay    Re-stream the last N days from the local landing zone at accelerated
            speed (one simulated hour every --speed seconds) — used for the live demo.

Examples
  python -m producer.aq_producer backfill --days 365
  python -m producer.aq_producer live --interval 600
  python -m producer.aq_producer replay --days 3 --speed 2
"""
import argparse
import json
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from common.config import CITIES, ROOT, TOPIC_RAW
from producer.common import build_producer, http_get, now_utc_iso
from producer.common import publish as _publish

AQ_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
WX_URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
LIVE_WX_URL = "https://api.open-meteo.com/v1/forecast"
AQ_VARS = "pm2_5,pm10,carbon_monoxide,nitrogen_dioxide,sulphur_dioxide,ozone,dust"
WX_VARS = "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation,boundary_layer_height"
TZ = "Asia/Kathmandu"
NPT = timezone(timedelta(hours=5, minutes=45))
LANDING = ROOT / "data" / "landing"

FIELD_MAP = {
    "pm2_5": "pm2_5", "pm10": "pm10", "carbon_monoxide": "co",
    "nitrogen_dioxide": "no2", "sulphur_dioxide": "so2", "ozone": "o3", "dust": "dust",
    "temperature_2m": "temperature", "relative_humidity_2m": "humidity",
    "wind_speed_10m": "wind_speed", "precipitation": "precipitation",
    "boundary_layer_height": "boundary_layer_height",
}


def _get(url, params):
    return http_get(url, params)


def fetch_city(city, lat, lon, start: date, end: date):
    """Return a list of merged hourly records for one city."""
    base = {"latitude": lat, "longitude": lon, "timezone": TZ,
            "start_date": start.isoformat(), "end_date": end.isoformat()}
    aq = _get(AQ_URL, {**base, "hourly": AQ_VARS})["hourly"]
    wx = _get(WX_URL, {**base, "hourly": WX_VARS})["hourly"]
    wx_by_time = {t: i for i, t in enumerate(wx["time"])}
    rows = []
    for i, t in enumerate(aq["time"]):
        rec = {}
        for k, v in aq.items():
            if k != "time":
                rec[FIELD_MAP[k]] = v[i]
        j = wx_by_time.get(t)
        for k, v in wx.items():
            if k != "time":
                rec[FIELD_MAP[k]] = v[j] if j is not None else None
        if rec.get("pm2_5") is None:
            continue
        rec["event_time"] = t + ":00+05:45"
        rows.append(rec)
    return rows


def make_message(city, province, lat, lon, rec, source):
    return {
        "city": city, "province": province, "lat": lat, "lon": lon,
        **rec,
        "source": source,
        "ingest_time": now_utc_iso(),
    }


def publish(producer, msg):
    _publish(producer, TOPIC_RAW, msg["city"], msg)


# ----------------------------------------------------------------------------- modes
def backfill(days: int):
    producer = build_producer()
    LANDING.mkdir(parents=True, exist_ok=True)
    end = datetime.now(NPT).date()
    start = end - timedelta(days=days)
    now = datetime.now(NPT)
    total = 0
    for city, prov, lat, lon in CITIES:
        print(f"[backfill] {city:14s} {start} → {end}")
        rows = [r for r in fetch_city(city, lat, lon, start, end)
                if datetime.fromisoformat(r["event_time"]) <= now]  # drop forecast hours
        # landing zone: raw JSON-lines, one file per city (immutable source copy)
        with open(LANDING / f"{city.lower()}.jsonl", "w") as f:
            for r in rows:
                msg = make_message(city, prov, lat, lon, r, "open-meteo/backfill")
                f.write(json.dumps(msg) + "\n")
                publish(producer, msg)
        total += len(rows)
        producer.flush()
        print(f"           published {len(rows):,} records (total {total:,})")
    print(f"[backfill] done — {total:,} messages → topic '{TOPIC_RAW}'")


def live(interval: int):
    producer = build_producer()
    print(f"[live] polling every {interval}s — Ctrl+C to stop")
    while True:
        for city, prov, lat, lon in CITIES:
            try:
                base = {"latitude": lat, "longitude": lon, "timezone": TZ}
                aq = _get(AQ_URL, {**base, "current": AQ_VARS})["current"]
                wx = _get(LIVE_WX_URL, {**base, "current": WX_VARS.replace(",boundary_layer_height", "")})["current"]
            except RuntimeError as e:
                print(f"  ! {city}: {e}")
                continue
            rec = {FIELD_MAP[k]: v for k, v in {**aq, **wx}.items() if k in FIELD_MAP}
            rec["event_time"] = aq["time"] + ":00+05:45"
            msg = make_message(city, prov, lat, lon, rec, "open-meteo/live")
            publish(producer, msg)
            print(f"  {msg['event_time']} {city:14s} PM2.5={rec.get('pm2_5')}")
        producer.flush()
        time.sleep(interval)


def replay(days: int, speed: float):
    """Stream the most recent `days` of landed data hour-by-hour (demo mode)."""
    producer = build_producer()
    files = sorted(LANDING.glob("*.jsonl"))
    if not files:
        raise SystemExit("No landing data — run `backfill` first.")
    by_hour = {}
    for fp in files:
        for line in fp.read_text().splitlines():
            m = json.loads(line)
            by_hour.setdefault(m["event_time"], []).append(m)
    hours = sorted(by_hour, key=datetime.fromisoformat)[-days * 24:]
    print(f"[replay] {len(hours)} hours × {len(CITIES)} cities, 1 hour every {speed}s")
    for h in hours:
        for m in by_hour[h]:
            m = {**m, "source": "open-meteo/replay", "ingest_time": now_utc_iso()}
            publish(producer, m)
        producer.flush()
        worst = max(by_hour[h], key=lambda x: x["pm2_5"] or 0)
        print(f"  {h}  {len(by_hour[h])} msgs  worst: {worst['city']} PM2.5={worst['pm2_5']}")
        time.sleep(speed)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)
    b = sub.add_parser("backfill"); b.add_argument("--days", type=int, default=365)
    l = sub.add_parser("live"); l.add_argument("--interval", type=int, default=900)
    r = sub.add_parser("replay"); r.add_argument("--days", type=int, default=3)
    r.add_argument("--speed", type=float, default=2.0)
    a = ap.parse_args()
    if a.mode == "backfill":
        backfill(a.days)
    elif a.mode == "live":
        live(a.interval)
    else:
        replay(a.days, a.speed)


if __name__ == "__main__":
    main()
