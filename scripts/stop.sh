#!/usr/bin/env bash
# Stop the Spark streaming job, dashboard and producers, then the containers.
cd "$(dirname "$0")/.."
for f in data/stream.pid data/dashboard.pid; do [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null; rm -f "$f"; done
pkill -f "[s]park_jobs/stream_processor.py" 2>/dev/null
pkill -f "[s]treamlit run dashboard/app.py" 2>/dev/null
pkill -f "[p]roducer.aq_producer" 2>/dev/null
docker compose stop
