#!/usr/bin/env bash
# Wait until the streaming job has written the backfill to the lake, then run the Spark
# batch analytics + MLlib job, and repeat every BATCH_INTERVAL seconds (default 6 h).
set -uo pipefail
DAYS=${BACKFILL_DAYS:-365}
TARGET=$((15 * 24 * DAYS * 95 / 100))
while true; do
  echo "[batch] waiting for lake to contain >= ${TARGET} rows and stop growing..."
  python scripts/wait_for_lake.py "$TARGET"
  python -u spark_jobs/batch_analytics.py 2>&1 | grep -v WARN
  echo "[batch] next run in ${BATCH_INTERVAL:-21600}s"
  sleep "${BATCH_INTERVAL:-21600}"
done
