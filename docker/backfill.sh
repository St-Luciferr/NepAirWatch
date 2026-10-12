#!/usr/bin/env bash
# One-shot: load BACKFILL_DAYS of history into Kafka, only on the very first start.
set -euo pipefail
MARKER=/app/data/.backfill_done
mkdir -p /app/data
if [ -f "$MARKER" ]; then
  echo "[backfill] already done ($(cat $MARKER)) — skipping"
  exit 0
fi
python -u -m producer.aq_producer backfill --days "${BACKFILL_DAYS:-365}"
date -Iseconds > "$MARKER"
