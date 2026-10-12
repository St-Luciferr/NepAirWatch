"""
NepAirWatch — ground-truth producer (measured, not modelled, air quality).

Networks
--------
  doenv    Department of Environment, Government of Nepal — Air Quality Monitoring Stations
           (pollution.gov.np, ~30 stations in all 7 provinces). 1-minute PM1 / PM2.5 / PM10.
           The public API keeps only a rolling ~2–3 weeks, so the long history is *our* lake.
  sensors  Sensor.Community citizen-science network (SDS011 low-cost sensors in Nepal).
           Live: ~5-min averages; history: one archive CSV per sensor per day.

Every reading becomes one JSON message on Kafka topic `nepal.ground.raw`, keyed by station:
  {network, station_id, station, lat, lon, city, city_km, event_time, pm1, pm2_5, pm10, source, ingest_time}

Modes
  backfill  [--days N] [--network doenv|sensors|all]   bulk historical load
  live      [--interval S]                              poll both networks, publish only new readings

Examples
  python -m producer.ground_producer backfill --days 20
  python -m producer.ground_producer live --interval 600
"""
import argparse
import csv
import io
import time
from datetime import date, datetime, timedelta, timezone

from common.config import STATION_CITY_MAX_KM, TOPIC_GROUND
from common.geo import nearest_city
from producer.common import build_producer, http_get, load_state, now_utc_iso, publish, save_state

DOENV_API = "https://pollution.gov.np/gss/api"
SC_LIVE = "https://data.sensor.community/airrohr/v1/filter/country=NP"
SC_ARCHIVE = "https://archive.sensor.community"

# DoEnv parameter codes (instantaneous = 1-minute values) → our field names
DOENV_PARAMS = {"PM1_I": "pm1", "PM2.5_I": "pm2_5", "PM10_I": "pm10"}
# Sensor.Community value types → field names (P1 = PM10, P2 = PM2.5, P0 = PM1)
SC_VALUES = {"P0": "pm1", "P1": "pm10", "P2": "pm2_5"}
MAX_PM = 2000.0  # µg/m³ — above this a reading is a sensor fault


def _num(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if 0 <= x <= MAX_PM else None


def _message(network, station_id, station, lat, lon, event_time, values, source):
    city, _, km = nearest_city(lat, lon, STATION_CITY_MAX_KM)
    return {"network": network, "station_id": str(station_id), "station": station.strip(),
            "lat": lat, "lon": lon, "city": city, "city_km": km, "event_time": event_time,
            "pm1": values.get("pm1"), "pm2_5": values.get("pm2_5"), "pm10": values.get("pm10"),
            "source": source, "ingest_time": now_utc_iso()}


# ----------------------------------------------------------------------------- DoEnv
def doenv_stations():
    """Stations with the series ids of their 1-minute PM channels."""
    out = []
    for st in http_get(f"{DOENV_API}/station"):
        # parameter_id → our field, from the station's parameter catalogue
        wanted = {p["parameter_id"]: DOENV_PARAMS[p["parameter_code"]]
                  for ds in (st.get("data_source") or []) for p in ds.get("parameters", [])
                  if p.get("parameter_code") in DOENV_PARAMS}
        if not wanted or st.get("latitude") is None:
            continue
        series = http_get(f"{DOENV_API}/station/{st['id']}/data-series") or []
        ids = {wanted[s["parameter_id"]]: s["id"] for s in series
               if s.get("parameter_id") in wanted and s.get("interval") is None}  # raw, not aggregates
        if "pm2_5" in ids or "pm10" in ids:
            out.append({"id": st["id"], "name": st["name"], "lat": st["latitude"], "lon": st["longitude"],
                        "series": ids})
    return out


def doenv_readings(st, date_from: date, date_to: date):
    """Merge the station's PM series on timestamp → {event_time: {field: value}} (UTC timestamps)."""
    merged = {}
    for field, sid in st["series"].items():
        obs = http_get(f"{DOENV_API}/observation",
                       {"series_id": sid, "date_from": date_from.isoformat(), "date_to": date_to.isoformat()},
                       timeout=180)
        for o in (obs or {}).get("data", []) or []:
            v = _num(o.get("value"))
            if v is not None and o.get("datetime"):
                merged.setdefault(o["datetime"], {})[field] = v
    return merged


def publish_doenv(producer, stations, date_from, date_to, last_sent, source):
    total = 0
    for st in stations:
        key = f"doenv-{st['id']}"
        try:
            readings = doenv_readings(st, date_from, date_to)
        except RuntimeError as e:
            print(f"  ! DoEnv {st['name']}: {e}", flush=True)
            continue
        newest = last_sent.get(key, "")
        n = 0
        for ts in sorted(readings):
            # timestamps share one format (YYYY-MM-DDTHH:MM:SS+00:00) so string order = time order
            if ts <= last_sent.get(key, ""):
                continue
            publish(producer, TOPIC_GROUND, key,
                    _message("doenv", key, st["name"], st["lat"], st["lon"], ts, readings[ts], source))
            newest, n = max(newest, ts), n + 1
        last_sent[key] = newest
        total += n
        print(f"  DoEnv {st['name']:28s} {n:6,} readings", flush=True)
    producer.flush()
    return total


# ----------------------------------------------------------------------------- Sensor.Community
def sc_live_readings():
    """Current readings of all Nepali PM sensors: list of (sensor, type, lat, lon, event_time, values)."""
    out = []
    for rec in http_get(SC_LIVE) or []:
        vals = {SC_VALUES[v["value_type"]]: _num(v["value"]) for v in rec.get("sensordatavalues", [])
                if v.get("value_type") in SC_VALUES}
        vals = {k: v for k, v in vals.items() if v is not None}
        if not vals:
            continue
        s, loc = rec["sensor"], rec["location"]
        ts = rec["timestamp"].replace(" ", "T") + "+00:00"   # Sensor.Community time is UTC
        out.append((s["id"], s["sensor_type"]["name"], float(loc["latitude"]), float(loc["longitude"]), ts, vals))
    return out


def sc_archive_day(sensor_id, sensor_type, day: date):
    """Readings from the per-sensor daily archive CSV (semicolon-separated); [] if the file is missing."""
    d = day.isoformat()
    text = http_get(f"{SC_ARCHIVE}/{d}/{d}_{sensor_type.lower()}_sensor_{sensor_id}.csv",
                    as_json=False, ok_404=True, retries=3)
    if not text:
        return []
    rows = []
    for r in csv.DictReader(io.StringIO(text), delimiter=";"):
        vals = {f: _num(r.get(k)) for k, f in SC_VALUES.items() if r.get(k)}
        vals = {k: v for k, v in vals.items() if v is not None}
        if vals and r.get("timestamp"):
            rows.append((float(r["lat"]), float(r["lon"]), r["timestamp"] + "+00:00", vals))
    return rows


def publish_sc_live(producer, last_sent):
    n = 0
    for sid, stype, lat, lon, ts, vals in sc_live_readings():
        key = f"sc-{sid}"
        if ts <= last_sent.get(key, ""):
            continue
        publish(producer, TOPIC_GROUND, key,
                _message("sensor.community", key, f"SC {stype} #{sid}", lat, lon, ts, vals,
                         "sensor.community/live"))
        last_sent[key], n = ts, n + 1
    producer.flush()
    return n


# ----------------------------------------------------------------------------- modes
def backfill(days: int, network: str):
    producer = build_producer()
    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=days)
    last_sent = {}
    if network in ("doenv", "all"):
        stations = doenv_stations()
        print(f"[backfill] DoEnv: {len(stations)} PM stations, {start} → {today}", flush=True)
        n = publish_doenv(producer, stations, start, today, last_sent, "doenv/backfill")
        print(f"[backfill] DoEnv done — {n:,} messages", flush=True)
    if network in ("sensors", "all"):
        sensors = {(sid, stype): (lat, lon) for sid, stype, lat, lon, _, _ in sc_live_readings()}
        print(f"[backfill] Sensor.Community: {len(sensors)} PM sensors, {start} → {today}", flush=True)
        total = 0
        for (sid, stype), _ in sensors.items():
            key, n = f"sc-{sid}", 0
            for i in range(days + 1):
                for lat, lon, ts, vals in sc_archive_day(sid, stype, start + timedelta(days=i)):
                    publish(producer, TOPIC_GROUND, key,
                            _message("sensor.community", key, f"SC {stype} #{sid}", lat, lon, ts, vals,
                                     "sensor.community/backfill"))
                    last_sent[key] = max(last_sent.get(key, ""), ts)
                    n += 1
            producer.flush()
            total += n
            print(f"  SC #{sid:<8} {n:6,} readings", flush=True)
        print(f"[backfill] Sensor.Community done — {total:,} messages", flush=True)
    save_state("ground_live", last_sent)   # live mode resumes from here (no duplicates, no gap)


def live(interval: int):
    producer = build_producer()
    last_sent = load_state("ground_live", {})
    stations, refreshed = [], 0.0
    print(f"[live] polling DoEnv + Sensor.Community every {interval}s — Ctrl+C to stop", flush=True)
    while True:
        if time.time() - refreshed > 6 * 3600:      # station catalogue changes rarely
            try:
                stations, refreshed = doenv_stations(), time.time()
            except RuntimeError as e:
                print(f"  ! DoEnv station list: {e}", flush=True)
        today = datetime.now(timezone.utc).date()
        n1 = publish_doenv(producer, stations, today - timedelta(days=1), today, last_sent, "doenv/live")
        try:
            n2 = publish_sc_live(producer, last_sent)
        except RuntimeError as e:
            print(f"  ! Sensor.Community: {e}", flush=True)
            n2 = 0
        save_state("ground_live", last_sent)
        print(f"[live] {datetime.now():%H:%M:%S}  DoEnv +{n1:,}  Sensor.Community +{n2:,}", flush=True)
        time.sleep(interval)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--days", type=int, default=20)
    b.add_argument("--network", choices=["doenv", "sensors", "all"], default="all")
    lv = sub.add_parser("live")
    lv.add_argument("--interval", type=int, default=600)
    a = ap.parse_args()
    if a.mode == "backfill":
        backfill(a.days, a.network)
    else:
        live(a.interval)


if __name__ == "__main__":
    main()
