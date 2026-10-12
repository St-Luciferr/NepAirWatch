"""
NepAirWatch — live demo simulator: a scripted pollution episode pushed through the REAL pipeline.

Over --minutes (default 10) it plays a story so every dashboard view visibly changes:

  clean morning → smog builds → forest fires → HAZARDOUS peak → rain clears the air

Every --tick seconds (default 10) it sends, with *current* timestamps:
  • nepal.air.raw     a reading for each of the 15 cities (the Live map changes colour)
  • nepal.ground.raw  3 demo stations in the Kathmandu Valley (Ground stations tab, Cassandra)
and at fixed moments:
  • NASA-FIRMS-format fire files into the Flume spool dir (→ Flume → Kafka + HDFS → Fires tab)
  • news headlines on nepal.news.raw (News tab)

All demo records carry source "demo/…" (stations: network "demo", ids "demo-*", news ids "demo-*"):
the batch layer ignores them and `scripts/demo.sh cleanup` removes them from the serving stores.

Run (normally via scripts/demo.sh):  python -m producer.demo_simulator --minutes 10 --tick 10
"""
import argparse
import csv
import io
import json
import math
import random
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.aqi import category, pm25_to_aqi
from common.config import CITIES, SPOOL_DIR, TOPIC_FIRES, TOPIC_GROUND, TOPIC_NEWS, TOPIC_RAW
from producer.common import build_producer, now_utc_iso, publish

NPT = timezone(timedelta(hours=5, minutes=45))

# Kathmandu-Valley PM2.5 storyline: (fraction of the demo, µg/m³, phase label)
STORY = [(0.00, 12, "Clean morning"), (0.12, 22, "Clean morning"), (0.25, 60, "Smog building"),
         (0.40, 120, "Smog building + forest fires"), (0.55, 230, "HAZARDOUS peak"),
         (0.66, 290, "HAZARDOUS peak"), (0.75, 180, "Wind picks up"), (0.85, 70, "Rain clears the air"),
         (1.00, 18, "Clean again")]
# how strongly each region follows the valley storyline, and how late (fraction of the demo)
PROFILE = {"Bagmati": (1.00, 0.00), "Madhesh": (0.80, 0.06), "Koshi": (0.65, 0.10), "Lumbini": (0.70, 0.08),
           "Gandaki": (0.45, 0.04), "Karnali": (0.35, 0.12), "Sudurpashchim": (0.60, 0.14)}
DEMO_STATIONS = [("demo-1", "DEMO Thamel", 27.7154, 85.3123), ("demo-2", "DEMO Koteshwor", 27.6788, 85.3493),
                 ("demo-3", "DEMO Bhaktapur Durbar", 27.6722, 85.4280)]
FIRE_WAVES = [(0.33, 25), (0.50, 45)]                  # (when, how many hotspots)
NEWS = [(0.45, "Smog thickens over Kathmandu Valley as AQI crosses 200"),
        (0.62, "Schools in Kathmandu suspend outdoor activities amid hazardous air"),
        (0.80, "Forest fires near Kathmandu contained; wind expected to disperse smoke"),
        (0.92, "Rain clears the haze; air quality improves across the valley")]


def storyline(frac):
    """Interpolated valley PM2.5 and phase label at a point of the demo (0…1)."""
    frac = min(max(frac, 0.0), 1.0)
    for (f0, v0, label), (f1, v1, _) in zip(STORY, STORY[1:]):
        if frac <= f1:
            w = (frac - f0) / (f1 - f0) if f1 > f0 else 0
            return v0 + (v1 - v0) * w, label
    return STORY[-1][1], STORY[-1][2]


def air_message(city, province, lat, lon, frac, rng):
    scale, lag = PROFILE.get(province, (0.6, 0.1))
    base, _ = storyline(frac - lag)
    pm25 = max(3.0, base * scale * rng.uniform(0.9, 1.1) + 4 * math.sin(frac * 20 + lat))
    rain = 2.5 + rng.random() * 4 if frac >= 0.80 else 0.0
    return {"city": city, "province": province, "lat": lat, "lon": lon,
            "event_time": datetime.now(NPT).isoformat(timespec="seconds"),
            "pm2_5": round(pm25, 1), "pm10": round(pm25 * rng.uniform(1.3, 1.7), 1),
            "co": round(250 + pm25 * 6, 0), "no2": round(8 + pm25 * 0.15, 1), "so2": round(2 + pm25 * 0.05, 1),
            "o3": round(max(10.0, 70 - pm25 * 0.2), 1), "dust": round(pm25 * 0.05, 1),
            "temperature": round(14 + 8 * (1 - frac), 1), "humidity": round(55 + (30 if rain else 0), 0),
            "wind_speed": round(1.5 + (9 if frac >= 0.72 else 0) * rng.uniform(0.7, 1.0), 1),
            "precipitation": round(rain, 1),
            "boundary_layer_height": round(max(120.0, 1400 - pm25 * 4.2), 0),   # low lid ⇔ high PM
            "source": "demo/simulated", "ingest_time": now_utc_iso()}


def ground_message(sid, name, lat, lon, frac, rng, when=None):
    base, _ = storyline(frac)
    pm25 = max(3.0, base * rng.uniform(0.85, 1.2))
    ts = (when or datetime.now(timezone.utc)).isoformat(timespec="seconds")
    return {"network": "demo", "station_id": sid, "station": name, "lat": lat, "lon": lon,
            "city": None, "city_km": None, "event_time": ts,
            "pm1": round(pm25 * 0.7, 1), "pm2_5": round(pm25, 1), "pm10": round(pm25 * 1.5, 1),
            "source": "demo/simulated", "ingest_time": now_utc_iso()}


def fire_rows(n, wave, rng):
    """Hotspots 20–90 km around Kathmandu (Shivapuri, Mahabharat range, towards Chitwan)."""
    rows, now = [], datetime.now(timezone.utc)
    for i in range(n):
        ang, km = rng.uniform(0, 2 * math.pi), rng.uniform(20, 90)
        lat = 27.7172 + km / 111 * math.sin(ang)
        lon = 85.3240 + km / (111 * math.cos(math.radians(27.7))) * math.cos(ang)
        rows.append([f"demo_{wave}_{i}_{int(now.timestamp())}", f"{lat:.5f}", f"{lon:.5f}",
                     now.isoformat(timespec="seconds"), "N", "VIIRS", rng.choice(["nominal", "high", "high"]),
                     f"{rng.uniform(5, 120):.1f}", f"{rng.uniform(320, 367):.1f}", "D", "demo/firms"])
    return rows


def send_fires(producer, rows):
    """Through Flume when its spool dir is there (docker compose), otherwise straight to Kafka."""
    lines = []
    for r in rows:
        buf = io.StringIO()
        csv.writer(buf, lineterminator="").writerow(r)
        lines.append(buf.getvalue())
    spool = Path(SPOOL_DIR) / "firms"
    if spool.is_dir():
        name = f"demo_fires_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}"
        (spool / f"{name}.tmp").write_text("\n".join(lines) + "\n")
        (spool / f"{name}.tmp").rename(spool / f"{name}.csv")
        return "Flume spool dir"
    for line in lines:
        publish(producer, TOPIC_FIRES, line.split(",")[3][:10], line)
    return "Kafka (direct)"


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--minutes", type=float, default=10)
    ap.add_argument("--tick", type=float, default=10, help="seconds between readings")
    a = ap.parse_args()
    rng = random.Random(42)
    producer = build_producer()
    total = a.minutes * 60
    sent = 0

    # 2 h of calm history for the demo stations, so the batch layer has finished hours to recompute
    start = datetime.now(timezone.utc) - timedelta(hours=2)
    for m in range(0, 120, 2):
        for sid, name, lat, lon in DEMO_STATIONS:
            publish(producer, TOPIC_GROUND, sid,
                    ground_message(sid, name, lat, lon, 0.05, rng, start + timedelta(minutes=m)))
            sent += 1
    producer.flush()
    print(f"[demo] seeded 2 h of history for {len(DEMO_STATIONS)} demo stations ({sent} readings)", flush=True)

    fires_done, news_done, t0, last_phase = set(), set(), time.time(), None
    while (elapsed := time.time() - t0) < total:
        frac = elapsed / total
        for c, p, la, lo in CITIES:
            publish(producer, TOPIC_RAW, c, air_message(c, p, la, lo, frac, rng))
        for sid, name, lat, lon in DEMO_STATIONS:
            publish(producer, TOPIC_GROUND, sid, ground_message(sid, name, lat, lon, frac, rng))
        sent += len(CITIES) + len(DEMO_STATIONS)

        for i, (when, n) in enumerate(FIRE_WAVES):
            if frac >= when and i not in fires_done:
                via = send_fires(producer, fire_rows(n, i, rng))
                fires_done.add(i)
                print(f"\n  🔥 {n} fire hotspots detected around Kathmandu → {via}\n", flush=True)
        for i, (when, title) in enumerate(NEWS):
            if frac >= when and i not in news_done:
                now = datetime.now(timezone.utc)
                publish(producer, TOPIC_NEWS, f"demo-{i}", {
                    "id": f"demo-{i}-{int(now.timestamp())}", "title": title, "link": "https://example.org/demo",
                    "publisher": "NepAirWatch demo", "published": now.isoformat(timespec="seconds"),
                    "query": "demo", "ingest_time": now_utc_iso()})
                news_done.add(i)
                print(f"\n  📰 headline: {title}\n", flush=True)
        producer.flush()

        pm, phase = storyline(frac)
        aqi = pm25_to_aqi(pm)
        if phase != last_phase:
            print(f"\n══ {phase.upper()} ══", flush=True)
            last_phase = phase
        print(f"  [{int(elapsed) // 60:02d}:{int(elapsed) % 60:02d}] Kathmandu PM2.5 ≈ {pm:5.0f} µg/m³ → "
              f"AQI {aqi:3d} {category(aqi)['category']:<31s} msgs sent {sent:,}", flush=True)
        time.sleep(a.tick)

    print(json.dumps({"demo": "finished", "messages": sent}), flush=True)
    print("[demo] done — the live producers continue. Run `scripts/demo.sh cleanup` to remove demo data.")


if __name__ == "__main__":
    main()
