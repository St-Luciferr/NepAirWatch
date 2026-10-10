"""Block until the Parquet lake holds >= N rows and has stopped growing (streaming sink caught up).

Only files committed in Spark's file-sink log (_spark_metadata) are counted, so files that are
still being written (or were left behind by a crashed job) are ignored.
"""
import json
import sys
import time
from pathlib import Path
from urllib.parse import unquote, urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import pyarrow.parquet as pq  # noqa: E402

from common.config import LAKE_PATH  # noqa: E402

LAKE = Path(LAKE_PATH) / "air_quality"
META = LAKE / "_spark_metadata"
_rows = {}  # path -> row count (committed files never change, so cache them)


def committed_files():
    files = set()
    if not META.exists():
        return files
    for log in META.iterdir():
        try:
            for line in log.read_text().splitlines()[1:]:  # first line is the version marker "v1"
                entry = json.loads(line)
                path = unquote(urlparse(entry["path"]).path)
                if entry.get("action") == "delete":
                    files.discard(path)
                else:
                    files.add(path)
        except (OSError, ValueError):
            continue  # log file being written right now; pick it up next round
    return files


def count_rows():
    total = 0
    for f in committed_files():
        if f not in _rows:
            try:
                _rows[f] = pq.ParquetFile(f).metadata.num_rows
            except Exception:  # noqa: BLE001 — unreadable/in-flight file, retry next round
                continue
        total += _rows[f]
    return total


def main():
    target = int(sys.argv[1])
    last = -1
    while True:
        n = count_rows()
        print(f"  lake rows committed: {n:,}", flush=True)
        if n >= target and n == last:
            return
        last = n
        time.sleep(15)


if __name__ == "__main__":
    main()
