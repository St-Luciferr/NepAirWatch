"""
NepAirWatch — NASA POWER long-term history loader (batch layer only, no Kafka).

Downloads daily satellite / reanalysis data since 2001 for the 15 cities from NASA POWER
(CERES SYN1deg + MERRA-2):
    AOD_55       aerosol optical depth at 550 nm (column haze / smoke / dust)
    T2M, RH2M    2 m temperature, relative humidity
    WS10M        10 m wind speed
    PRECTOTCORR  precipitation (mm/day)
and writes it as JSON lines into the raw zone of the data lake (HDFS in docker compose):
    <RAW_PATH>/nasa_power/power_daily_<City>.jsonl        (one flat directory: easy for Hive / MapReduce)
    <RAW_PATH>/nasa_power/_SUCCESS                       (marker for the MapReduce / Spark jobs)

Bulk historical data like this goes straight into the batch layer's master dataset — there is no
need to push 25 years of history through the speed layer (Lambda architecture).

Run:  python -m producer.power_loader [--start 2001]
"""
import argparse
import json
from datetime import datetime, timedelta, timezone

from common.config import CITIES, RAW_PATH
from common.storage import write_text
from producer.common import http_get

API = "https://power.larc.nasa.gov/api/temporal/daily/point"
PARAMS = {"AOD_55": "aod_55", "T2M": "t2m", "RH2M": "rh2m", "WS10M": "ws10m", "PRECTOTCORR": "precip"}
FILL = -999.0


def fetch_city(lat, lon, start, end):
    data = http_get(API, {"parameters": ",".join(PARAMS), "community": "RE", "latitude": lat, "longitude": lon,
                          "start": start.strftime("%Y%m%d"), "end": end.strftime("%Y%m%d"), "format": "JSON"},
                    timeout=180)
    series = data["properties"]["parameter"]
    days = sorted(next(iter(series.values())).keys())
    for d in days:
        rec = {}
        for api_name, field in PARAMS.items():
            v = series.get(api_name, {}).get(d)
            rec[field] = None if v is None or v == FILL else v
        if any(v is not None for v in rec.values()):
            yield f"{d[:4]}-{d[4:6]}-{d[6:]}", rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", type=int, default=2001, help="first year (AOD is available from 2001)")
    a = ap.parse_args()
    start = datetime(a.start, 1, 1).date()
    end = datetime.now(timezone.utc).date() - timedelta(days=3)   # POWER lags a few days behind
    base = f"{RAW_PATH}/nasa_power"
    total = 0
    for city, prov, lat, lon in CITIES:
        lines = [json.dumps({"city": city, "province": prov, "lat": lat, "lon": lon, "date": d, **rec})
                 for d, rec in fetch_city(lat, lon, start, end)]
        write_text(f"{base}/power_daily_{city}.jsonl", "\n".join(lines) + "\n")
        total += len(lines)
        print(f"[power] {city:14s} {len(lines):6,} days  {start} → {end}", flush=True)
    write_text(f"{base}/_SUCCESS", datetime.now(timezone.utc).isoformat())
    print(f"[power] done — {total:,} city-days → {base}", flush=True)


if __name__ == "__main__":
    main()
