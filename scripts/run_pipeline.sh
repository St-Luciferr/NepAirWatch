#!/usr/bin/env bash
# Full pipeline from a clean slate: backfill 1 year → streaming job → batch analytics → dashboard.
# Safe to re-run: it stops previous jobs and resets the Kafka topics first.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data
PY=.venv/bin/python
DAYS=${DAYS:-365}

echo "[0/4] Stopping any previous jobs and resetting Kafka topics..."
for f in data/stream.pid data/dashboard.pid; do
  [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
  rm -f "$f"
done
pkill -f "[s]park_jobs/stream_processor.py" 2>/dev/null || true
pkill -f "[s]treamlit run dashboard/app.py" 2>/dev/null || true
pkill -f "[p]roducer.aq_producer" 2>/dev/null || true
while pgrep -f "[s]park_jobs/stream_processor.py" >/dev/null; do sleep 1; done
for t in nepal.air.raw nepal.air.alerts; do
  docker exec nepair-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --if-exists --topic "$t" >/dev/null 2>&1 || true
done
while docker exec nepair-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
      2>/dev/null | grep -q "^nepal.air"; do sleep 2; done

echo "[1/4] Backfilling $DAYS days of history into Kafka..."
$PY -u -m producer.aq_producer backfill --days "$DAYS"

echo "[2/4] Starting Spark Structured Streaming (log: data/stream.log)..."
nohup $PY -u spark_jobs/stream_processor.py --reset > data/stream.log 2>&1 &
echo $! > data/stream.pid
until grep -q "Streaming queries started" data/stream.log; do
  if ! kill -0 "$(cat data/stream.pid)" 2>/dev/null; then echo "Streaming job died — see data/stream.log"; exit 1; fi
  sleep 3
done
echo "     waiting for the stream to drain the topic..."
$PY scripts/wait_for_lake.py $((15*24*DAYS*95/100))

echo "[3/4] Running Spark batch analytics + MLlib training..."
$PY -u spark_jobs/batch_analytics.py 2>&1 | grep -v WARN

echo "[4/4] Starting dashboard → http://localhost:8501"
nohup .venv/bin/streamlit run dashboard/app.py --server.headless true > data/dashboard.log 2>&1 &
echo $! > data/dashboard.pid
echo "Done. For the live demo run:  $PY -m producer.aq_producer replay --days 2 --speed 2"
