#!/usr/bin/env python3
"""Hadoop Streaming reducer — yearly aerosol statistics per city.

Input (sorted by key): "city|year \t aod \t t2m \t precip"
Output (TSV, read by the Hive table `aod_yearly_mr`):
  city  year  days  aod_mean  aod_max  hazy_days(AOD >= 0.5)  very_hazy_days(AOD >= 1.0)  t2m_mean  precip_total
"""
import sys

HAZY, VERY_HAZY = 0.5, 1.0


def emit(key, s):
    city, year = key.split("|")
    t2m = f"{s['t2m'] / s['nt']:.2f}" if s["nt"] else ""
    print("\t".join([city, year, str(s["n"]), f"{s['aod'] / s['n']:.4f}", f"{s['max']:.3f}",
                     str(s["hazy"]), str(s["very"]), t2m, f"{s['precip']:.1f}"]))


def empty():
    return {"n": 0, "aod": 0.0, "max": 0.0, "hazy": 0, "very": 0, "t2m": 0.0, "nt": 0, "precip": 0.0}


current, stats = None, empty()
for line in sys.stdin:
    parts = line.rstrip("\n").split("\t")
    if len(parts) != 4:
        continue
    key, aod, t2m, precip = parts
    if key != current:
        if current is not None:
            emit(current, stats)
        current, stats = key, empty()
    a = float(aod)
    stats["n"] += 1
    stats["aod"] += a
    stats["max"] = max(stats["max"], a)
    stats["hazy"] += a >= HAZY
    stats["very"] += a >= VERY_HAZY
    if t2m:
        stats["t2m"] += float(t2m)
        stats["nt"] += 1
    if precip:
        stats["precip"] += float(precip)
if current is not None:
    emit(current, stats)
