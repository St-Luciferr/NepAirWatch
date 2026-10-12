#!/usr/bin/env bash
# One-time setup for local development: Python env + the infrastructure containers.
# (Local mode keeps the lake on the local disk; HDFS / YARN / Hive / Flume run only in docker compose.)
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv 2>/dev/null || uv venv -p 3.11 .venv
.venv/bin/pip install -q -r requirements.txt || uv pip install -p .venv/bin/python -r requirements.txt
docker compose up -d kafka kafka-ui mongodb cassandra kafka-init cassandra-init
echo "Waiting for Kafka, MongoDB & Cassandra to become healthy..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' nepair-kafka)" = healthy ] && \
      [ "$(docker inspect -f '{{.State.Health.Status}}' nepair-mongo)" = healthy ] && \
      [ "$(docker inspect -f '{{.State.Health.Status}}' nepair-cassandra)" = healthy ]; do sleep 3; done
until [ "$(docker inspect -f '{{.State.Status}}' nepair-cassandra-init)" = exited ]; do sleep 2; done
echo "Infrastructure ready: Kafka :9092 | Kafka-UI http://localhost:8080 | MongoDB :27017 | Cassandra :9042"
