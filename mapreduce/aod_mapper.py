#!/usr/bin/env python3
"""Hadoop Streaming mapper — NASA POWER daily JSON lines → "city|year \t aod \t t2m \t precip".

Input : <RAW>/nasa_power/power_daily_<City>.jsonl   (one JSON object per city-day)
Output: one line per valid day, keyed by city|year so the shuffle groups a city's year together.
"""
import json
import sys

for line in sys.stdin:
    try:
        r = json.loads(line)
    except ValueError:
        continue                              # corrupt record: skip, never fail the task
    aod = r.get("aod_55")
    if aod is None or not r.get("city") or not r.get("date"):
        continue
    t2m = r.get("t2m")
    precip = r.get("precip")
    print(f"{r['city']}|{r['date'][:4]}\t{aod}\t{'' if t2m is None else t2m}\t{'' if precip is None else precip}")
