"""Remove demo data (demo simulator + demo control panel) from the serving stores and restore the real
`latest` values.

The HDFS lake keeps the demo rows (it is append-only), but every batch job filters them out
(source "demo/…", network "demo", news id "demo-…"), so analytics and models are unaffected.

Run:  python scripts/demo_cleanup.py      (or `scripts/demo.sh cleanup`, or the button in the control panel)
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

if "__file__" in globals():
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pymongo import MongoClient  # noqa: E402

from common.config import CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, MONGO_DB, MONGO_URI  # noqa: E402

FIXED_DEMO_STATIONS = [f"demo-{i}" for i in range(1, 6)]


def cleanup(log=print):
    db = MongoClient(MONGO_URI)[MONGO_DB]
    demo = {"source": {"$regex": "^demo"}}
    station_ids = sorted(set(FIXED_DEMO_STATIONS) | set(db.ground_latest.distinct("_id", {"network": "demo"})))
    log("MongoDB:")
    for coll, q in [("readings", demo), ("alerts", demo), ("fires", demo),
                    ("news", {"id": {"$regex": "^demo-"}}), ("ground_latest", {"network": "demo"})]:
        log(f"  {coll:14s} -{db[coll].delete_many(q).deleted_count:,}")

    # `latest` only moves forward in time, so demo readings (stamped "now") must be rolled back by hand
    restored = 0
    for city in db.latest.distinct("_id"):
        doc = db.readings.find_one({"city": city}, sort=[("event_ts", -1)])
        if doc:
            doc.pop("_id")
            db.latest.replace_one({"_id": city}, {**doc, "_id": city}, upsert=True)
            restored += 1
    log(f"  latest         restored {restored} cities to their newest real reading")

    try:
        from cassandra.cluster import Cluster
        s = Cluster(CASSANDRA_HOSTS, connect_timeout=10).connect(CASSANDRA_KEYSPACE)
        today = datetime.utcnow().date()
        for sid in station_ids:
            for d in range(-1, 3):
                s.execute("DELETE FROM ground_readings WHERE station_id = %s AND day = %s",
                          (sid, today - timedelta(days=d)))
            s.execute("DELETE FROM ground_hourly WHERE station_id = %s", (sid,))
        log(f"Cassandra: removed partitions of {', '.join(station_ids)}")
    except Exception as e:  # noqa: BLE001
        log(f"Cassandra: skipped ({e})")
    log("Demo data removed.")


if __name__ == "__main__":
    cleanup()
