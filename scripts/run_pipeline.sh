#!/usr/bin/env bash
# Full pipeline from a clean slate (local mode): backfill → streaming job → batch layer → dashboard.
# Safe to re-run: it stops previous jobs and resets the Kafka topics first.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p data
PY=.venv/bin/python
DAYS=${DAYS:-365}
TOPICS="nepal.air.raw nepal.air.alerts nepal.ground.raw nepal.fires.raw nepal.news.raw"

echo "[0/5] Stopping any previous jobs and resetting Kafka topics..."
for f in data/*.pid; do
  [ -f "$f" ] && kill "$(cat "$f")" 2>/dev/null || true
  rm -f "$f"
done
pkill -f "[s]park_jobs/stream_processor.py" 2>/dev/null || true
pkill -f "[s]treamlit run dashboard/app.py" 2>/dev/null || true
pkill -f "[p]roducer\.(aq_producer|ground_producer|firms_fetcher|news_producer)" 2>/dev/null || true
while pgrep -f "[s]park_jobs/stream_processor.py" >/dev/null; do sleep 1; done
rm -rf data/state
for t in $TOPICS; do
  docker exec nepair-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
    --delete --if-exists --topic "$t" >/dev/null 2>&1 || true
done
while docker exec nepair-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list \
      2>/dev/null | grep -q "^nepal\."; do sleep 2; done

echo "[1/5] Backfilling history into Kafka (Open-Meteo $DAYS days, ground stations, fires, news)..."
$PY -u -m producer.aq_producer backfill --days "$DAYS"
$PY -u -m producer.ground_producer backfill --days "${GROUND_DAYS:-20}"
$PY -u -m producer.firms_fetcher --direct backfill --days 7
$PY -u -m producer.news_producer once
$PY -u -m producer.power_loader

echo "[2/5] Starting Spark Structured Streaming (log: data/stream.log)..."
nohup $PY -u spark_jobs/stream_processor.py --reset > data/stream.log 2>&1 &
echo $! > data/stream.pid
until grep -q "Streaming queries started" data/stream.log; do
  if ! kill -0 "$(cat data/stream.pid)" 2>/dev/null; then echo "Streaming job died — see data/stream.log"; exit 1; fi
  sleep 3
done
echo "     waiting for the stream to drain the topic..."
$PY scripts/wait_for_lake.py $((15*24*DAYS*95/100))

echo "[3/5] Starting live producers (logs: data/producer-*.log)..."
nohup $PY -u -m producer.aq_producer live --interval 900 > data/producer-air.log 2>&1 & echo $! > data/air.pid
nohup $PY -u -m producer.ground_producer live --interval 600 > data/producer-ground.log 2>&1 & echo $! > data/ground.pid
nohup $PY -u -m producer.firms_fetcher --direct live > data/producer-fires.log 2>&1 & echo $! > data/fires.pid
nohup $PY -u -m producer.news_producer live > data/producer-news.log 2>&1 & echo $! > data/news.pid

echo "[4/5] Running the Spark batch layer (batch_analytics + fusion_analytics)..."
$PY -u spark_jobs/batch_analytics.py 2>&1 | grep -v WARN
$PY -u spark_jobs/fusion_analytics.py 2>&1 | grep -v WARN

echo "[5/5] Starting dashboard → http://localhost:8501"
nohup .venv/bin/streamlit run dashboard/app.py --server.headless true > data/dashboard.log 2>&1 &
echo $! > data/dashboard.pid
echo "Done. For the live demo run:  $PY -m producer.aq_producer replay --days 2 --speed 2"
