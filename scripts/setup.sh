#!/usr/bin/env bash
# One-time setup: Python env + infrastructure containers
set -euo pipefail
cd "$(dirname "$0")/.."
python3 -m venv .venv 2>/dev/null || uv venv -p 3.11 .venv
.venv/bin/pip install -q -r requirements.txt || uv pip install -p .venv/bin/python -r requirements.txt
docker compose up -d
echo "Waiting for Kafka & MongoDB to become healthy..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' nepair-kafka)" = healthy ] && \
      [ "$(docker inspect -f '{{.State.Health.Status}}' nepair-mongo)" = healthy ]; do sleep 3; done
echo "Infrastructure ready: Kafka :9092 | Kafka-UI http://localhost:8080 | MongoDB :27017"
