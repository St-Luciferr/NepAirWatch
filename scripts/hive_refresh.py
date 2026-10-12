"""Register new lake partitions in the Hive metastore (best effort; no-op when HIVE_HOST is unset).

Spark keeps adding city/date, network/date, ... directories to the lake; Hive only sees a partition
after MSCK REPAIR TABLE. The batch loop calls this after every batch run.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from common.config import HIVE_HOST, HIVE_PORT  # noqa: E402

TABLES = ["air_quality", "ground", "fires", "fires_raw", "ground_hourly"]


def main():
    if not HIVE_HOST:
        print("[hive] HIVE_HOST not set — skipping partition refresh")
        return
    try:
        from pyhive import hive
        conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username="nepairwatch", database="nepairwatch")
        cur = conn.cursor()
        cur.execute("SET hive.msck.path.validation=ignore")
        for t in TABLES:
            cur.execute(f"MSCK REPAIR TABLE {t} SYNC PARTITIONS")
            print(f"[hive] repaired partitions of {t}")
        conn.close()
    except Exception as e:  # noqa: BLE001 — Hive is optional (compose profile "hive")
        print(f"[hive] partition refresh skipped: {e}")


if __name__ == "__main__":
    main()
