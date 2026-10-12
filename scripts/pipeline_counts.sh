#!/usr/bin/env bash
# One-shot snapshot of how much data each pipeline stage holds (run under `watch` to see it grow).
#   Kafka offsets  →  Spark streaming totals  →  HDFS lake / raw zone  →  Cassandra  →  MongoDB
cd "$(dirname "$0")/.."
TOPICS="nepal.air.raw nepal.ground.raw nepal.fires.raw nepal.news.raw nepal.air.alerts"

echo "── KAFKA  messages per topic ─────────────────────────────"
docker compose exec -T kafka bash -c "for t in $TOPICS; do
  printf '  %-18s %10s\n' \$t \$(/opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic \$t 2>/dev/null \
    | awk -F: '{s+=\$3} END {print s+0}'); done"

echo "── SPARK STREAMING  rows read per query (all time) ───────"
docker compose exec -T mongodb mongosh nepairwatch --quiet --eval '
  db.stream_totals.find().sort({_id: 1}).forEach(d =>
    print("  " + d._id.padEnd(16) + String(d.rows).padStart(10) + "   last batch " + d.at.toISOString().slice(11, 19)))'

echo "── HDFS  dirs / files / bytes ────────────────────────────"
docker compose exec -T namenode hdfs dfs -count /nepairwatch/raw/* /nepairwatch/lake/* 2>/dev/null \
  | awk '{sub("/nepairwatch/", "", $4); printf "  %-24s %7d files %9.1f MB\n", $4, $2, $3 / 1e6}'

echo "── CASSANDRA  hourly station rows ────────────────────────"
docker compose exec -T cassandra cqlsh -e "SELECT count(*) FROM nepairwatch.ground_hourly" 2>/dev/null \
  | awk 'NR==4 {print "  ground_hourly      " $1}'

echo "── MONGODB  serving collections ──────────────────────────"
docker compose exec -T mongodb mongosh nepairwatch --quiet --eval '
  ["readings","latest","ground_latest","alerts","fires","news","forecasts","cams_validation"].forEach(c =>
    print("  " + c.padEnd(18) + String(db[c].estimatedDocumentCount()).padStart(10)))'
