#!/usr/bin/env bash
# Live view of the whole pipeline in one terminal (tmux, 6 panes): data visibly moving through
# producers → Kafka → Flume → Spark streaming → storage → Spark batch.
#
#   scripts/watch_pipeline.sh [topic]      default topic to tail: nepal.air.raw
#   Ctrl-b then arrow keys = move between panes · Ctrl-b z = zoom a pane · Ctrl-b d = detach
#   tmux kill-session -t nepairwatch       close it
set -euo pipefail
cd "$(dirname "$0")/.."
TOPIC=${1:-nepal.air.raw}
S=nepairwatch
tmux kill-session -t $S 2>/dev/null || true

tmux new-session -d -s $S -x 250 -y 65 \
  "docker compose logs -f --tail 3 producer-live producer-ground producer-fires producer-news"
tmux set -t $S remain-on-exit on
tmux set -t $S pane-border-status top
tmux select-pane -t $S:0.0 -T "① PRODUCERS — fetching from the APIs"

tmux split-window -t $S "docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic $TOPIC --property print.key=true --property key.separator=' | '"
tmux select-pane -T "② KAFKA — every new message on $TOPIC"

tmux split-window -t $S "docker compose logs -f --tail 3 flume producer-fires"
tmux select-pane -T "③ FLUME — FIRMS files → Kafka + HDFS"

tmux split-window -t $S "docker compose logs -f --tail 5 spark-streaming"
tmux select-pane -T "④ SPARK STREAMING — micro-batches"

tmux split-window -t $S "watch -n 10 -t scripts/pipeline_counts.sh"
tmux select-pane -T "⑤ STORAGE — counts growing (refresh 10 s)"

tmux split-window -t $S "docker compose logs -f --tail 5 spark-batch"
tmux select-pane -T "⑥ SPARK BATCH — analytics + ML"

tmux select-layout -t $S tiled
tmux attach -t $S
