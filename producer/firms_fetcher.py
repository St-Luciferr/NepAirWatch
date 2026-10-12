"""
NepAirWatch — NASA FIRMS active-fire hotspots (forest fires, crop-residue burning, brick kilns).

Downloads VIIRS (S-NPP, NOAA-20, NOAA-21) and MODIS hotspots for Nepal + the Indo-Gangetic Plain,
normalises them to one CSV layout and drops each batch of *new* hotspots as a file into the
Apache Flume spooling directory. Flume then fans every line out to
    • Kafka topic `nepal.fires.raw`  (speed layer → Spark Structured Streaming)
    • HDFS  /nepairwatch/raw/firms/   (batch layer master dataset)
With --direct the lines are published straight to Kafka instead (local runs without Flume).

Sources
  live      public "South Asia" CSVs (last 24 h / 7 d, no key needed), refreshed by FIRMS every ~3 h
  backfill  last 7 days from the public files; with FIRMS_MAP_KEY (free key) the FIRMS area API
            is used and any number of days can be loaded

CSV layout (no header):
  fire_id,latitude,longitude,acq_ts,satellite,instrument,confidence,frp,brightness,daynight,source

Examples
  python -m producer.firms_fetcher backfill --days 7
  python -m producer.firms_fetcher live --interval 10800
"""
import argparse
import csv
import io
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.config import FIRE_BBOX, FIRMS_MAP_KEY, SPOOL_DIR, TOPIC_FIRES
from producer.common import build_producer, http_get, load_state, publish, save_state

PUBLIC = "https://firms.modaps.eosdis.nasa.gov/data/active_fire"
PUBLIC_FILES = {  # product → path template ({w} = 24h | 48h | 7d)
    "VIIRS_SNPP": "suomi-npp-viirs-c2/csv/SUOMI_VIIRS_C2_South_Asia_{w}.csv",
    "VIIRS_NOAA20": "noaa-20-viirs-c2/csv/J1_VIIRS_C2_South_Asia_{w}.csv",
    "VIIRS_NOAA21": "noaa-21-viirs-c2/csv/J2_VIIRS_C2_South_Asia_{w}.csv",
    "MODIS": "modis-c6.1/csv/MODIS_C6_1_South_Asia_{w}.csv",
}
AREA_API = "https://firms.modaps.eosdis.nasa.gov/api/area/csv"
AREA_SOURCES = ["VIIRS_SNPP_SP", "VIIRS_SNPP_NRT", "VIIRS_NOAA20_NRT", "MODIS_SP", "MODIS_NRT"]
COLUMNS = ["fire_id", "latitude", "longitude", "acq_ts", "satellite", "instrument", "confidence",
           "frp", "brightness", "daynight", "source"]


def _confidence(raw):
    """VIIRS reports l/n/h (or low/nominal/high), MODIS 0–100 — map both to low/nominal/high."""
    raw = (raw or "").strip().lower()
    if raw.isdigit():
        v = int(raw)
        return "low" if v < 30 else ("high" if v >= 80 else "nominal")
    return {"l": "low", "n": "nominal", "h": "high"}.get(raw, raw or "unknown")


def normalise(text, source):
    """Parse a FIRMS CSV → list of rows in COLUMNS order, filtered to the region of interest."""
    w, s, e, n = FIRE_BBOX
    out = []
    for r in csv.DictReader(io.StringIO(text)):
        try:
            lat, lon = float(r["latitude"]), float(r["longitude"])
        except (KeyError, TypeError, ValueError):
            continue
        if not (s <= lat <= n and w <= lon <= e):
            continue
        hhmm = r.get("acq_time", "0").zfill(4)
        acq_ts = f"{r['acq_date']}T{hhmm[:2]}:{hhmm[2:]}:00+00:00"   # FIRMS times are UTC
        viirs = "bright_ti4" in r
        sat = r.get("satellite", "")
        fire_id = f"{lat:.5f}_{lon:.5f}_{r['acq_date']}_{hhmm}_{sat}"
        out.append([fire_id, f"{lat:.5f}", f"{lon:.5f}", acq_ts, sat, "VIIRS" if viirs else "MODIS",
                    _confidence(r.get("confidence")), r.get("frp", ""),
                    r.get("bright_ti4") if viirs else r.get("brightness", ""), r.get("daynight", ""), source])
    return out


def fetch_public(window):
    rows = []
    for product, path in PUBLIC_FILES.items():
        try:
            text = http_get(f"{PUBLIC}/{path.format(w=window)}", as_json=False, ok_404=True, retries=3)
        except RuntimeError as e:
            print(f"  ! {product}: {e}", flush=True)
            continue
        if text:
            got = normalise(text, f"firms/{product}")
            print(f"  {product:13s} {window:>3s}: {len(got):6,} hotspots in region", flush=True)
            rows += got
    return rows


def fetch_area(days):
    """FIRMS area API (needs FIRMS_MAP_KEY): walk back in 5-day chunks."""
    bbox = ",".join(str(x) for x in FIRE_BBOX)
    end = datetime.now(timezone.utc).date()
    rows = []
    for src in AREA_SOURCES:
        n, d = 0, end - timedelta(days=days)
        while d <= end:
            span = min(5, (end - d).days + 1)
            try:
                text = http_get(f"{AREA_API}/{FIRMS_MAP_KEY}/{src}/{bbox}/{span}/{d.isoformat()}",
                                as_json=False, retries=3, timeout=120)
            except RuntimeError as e:
                print(f"  ! {src} {d}: {e}", flush=True)
                text = ""
            if text and text.startswith("latitude"):
                got = normalise(text, f"firms/{src}")
                rows += got
                n += len(got)
            d += timedelta(days=span)
        print(f"  {src:17s}: {n:7,} hotspots", flush=True)
    return rows


class Sink:
    """Writes new hotspot lines to the Flume spooling directory, or straight to Kafka."""

    def __init__(self, direct):
        self.producer = build_producer() if direct else None
        self.spool = Path(SPOOL_DIR) / "firms"
        self.spool.mkdir(parents=True, exist_ok=True)
        self.seen = load_state("firms_seen", {})   # fire_id → acq date, pruned after 14 days

    def emit(self, rows):
        new = [r for r in rows if r[0] not in self.seen]
        new = list({r[0]: r for r in new}.values())   # same hotspot reported by two products/files
        if new:
            if self.producer:
                for r in new:
                    publish(self.producer, TOPIC_FIRES, r[3][:10], _csv_line(r))
                self.producer.flush()
            else:
                # Flume's spooldir source requires immutable, uniquely named files: write *.tmp, then
                # rename — the source ignores *.tmp (see flume/flume.conf).
                name = f"fires_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%f}"
                tmp = self.spool / f"{name}.tmp"
                tmp.write_text("".join(_csv_line(r) + "\n" for r in new))
                tmp.rename(self.spool / f"{name}.csv")
        for r in new:
            self.seen[r[0]] = r[3][:10]
        cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).date().isoformat()
        self.seen = {k: v for k, v in self.seen.items() if v >= cutoff}
        save_state("firms_seen", self.seen)
        return len(new)


def _csv_line(row):
    buf = io.StringIO()
    csv.writer(buf, lineterminator="").writerow(row)
    return buf.getvalue()


def backfill(days, direct):
    sink = Sink(direct)
    if FIRMS_MAP_KEY:
        print(f"[backfill] FIRMS area API, last {days} days", flush=True)
        rows = fetch_area(days)
    else:
        print("[backfill] no FIRMS_MAP_KEY — using the public 7-day files", flush=True)
        rows = fetch_public("7d")
    print(f"[backfill] {sink.emit(rows):,} new hotspots → {'Kafka' if direct else 'Flume spool'}", flush=True)


def live(interval, direct):
    sink = Sink(direct)
    print(f"[live] polling FIRMS every {interval}s", flush=True)
    while True:
        n = sink.emit(fetch_public("24h"))
        print(f"[live] {datetime.now():%H:%M:%S}  +{n:,} new hotspots", flush=True)
        time.sleep(interval)


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--direct", action="store_true", help="publish to Kafka directly instead of via Flume")
    sub = ap.add_subparsers(dest="mode", required=True)
    b = sub.add_parser("backfill")
    b.add_argument("--days", type=int, default=7)
    lv = sub.add_parser("live")
    lv.add_argument("--interval", type=int, default=10800)
    a = ap.parse_args()
    if a.mode == "backfill":
        backfill(a.days, a.direct)
    else:
        live(a.interval, a.direct)


if __name__ == "__main__":
    main()
