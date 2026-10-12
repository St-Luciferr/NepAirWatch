"""Block until the streaming lake sink has written >= N rows and has stopped growing.

The streaming job's StreamingQueryListener keeps a cumulative row counter per query in MongoDB
`stream_totals`, so this works the same whether the lake lives on HDFS or on the local disk.

Usage:  python scripts/wait_for_lake.py <min_rows> [query_name]   (default query: lake_sink)
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pymongo import MongoClient  # noqa: E402
from pymongo.errors import PyMongoError  # noqa: E402

from common.config import MONGO_DB, MONGO_URI  # noqa: E402


def committed_rows(db, query):
    doc = db.stream_totals.find_one({"_id": query})
    return doc["rows"] if doc else 0


def main():
    target = int(sys.argv[1])
    query = sys.argv[2] if len(sys.argv) > 2 else "lake_sink"
    db = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)[MONGO_DB]
    last = -1
    while True:
        try:
            n = committed_rows(db, query)
        except PyMongoError as e:
            print(f"  MongoDB not ready: {e}", flush=True)
            n = -1
        print(f"  {query}: {n:,} rows written", flush=True)
        if n >= target and n == last:
            return
        last = n
        time.sleep(15)


if __name__ == "__main__":
    main()
