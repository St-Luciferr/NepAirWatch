#!/usr/bin/env bash
# NepAirWatch — one-command live demo (run while `docker compose up` is running).
#
#   scripts/demo.sh            10-minute demo
#   scripts/demo.sh 15         15-minute demo
#   scripts/demo.sh cleanup    remove the demo data afterwards (restores real "latest" values)
#
# Opens a tmux window with 6 panes:
#   ① SIMULATOR    scripted pollution episode → Kafka / Flume (clean → smog → fires → hazardous → rain)
#   ② CUES         what to show in which UI, and when; triggers the batch layer on schedule
#   ③ KAFKA        every new message on nepal.air.raw
#   ④ SPARK        streaming micro-batches
#   ⑤ STORAGE      Kafka / Spark / HDFS / Cassandra / MongoDB counts (refresh 10 s)
#   ⑥ FLUME        fire files → Kafka + HDFS
# tmux keys: Ctrl-b + arrows = move · Ctrl-b z = zoom pane · Ctrl-b d = detach
#            tmux kill-session -t nepairdemo = close
set -euo pipefail
cd "$(dirname "$0")/.."
SELF="$PWD/scripts/demo.sh"
APP=producer-ground        # any running app container (they share one image) can run the simulator
SESSION=nepairdemo

running() { docker compose ps --status running --services 2>/dev/null | grep -qx "$1"; }

case "${1:-10}" in
  cleanup)
    docker compose exec -T "$APP" python -u - < scripts/demo_cleanup.py
    exit 0 ;;
  _cues)   # internal: the director's timeline (runs inside a tmux pane)
    MIN=$2; START=$3; S=$((MIN * 60))
    at() { local t=$(( START + S * $1 / 100 )); while [ "$(date +%s)" -lt "$t" ]; do sleep 1; done; }
    cue() { printf '\n\033[1;36m[%s] %s\033[0m\n' "$(date +%H:%M:%S)" "$1"; shift; for l in "$@"; do echo "   • $l"; done; }
    batch() {
      cue "BATCH LAYER — Spark recomputes station hours from HDFS (running ~1 min)" \
          "Ground stations tab → DEMO Thamel: past hours turn BLUE (batch view), current hour stays ORANGE (speed)" \
          "Ground stations tab → expand 'Station summary' (batch view) after it finishes"
      docker compose exec -T spark-batch python -u spark_jobs/fusion_analytics.py --only-ground --include-demo 2>&1 \
        | grep -E "===|station-hours|mongo\.|finished" || echo "   (batch run failed — see docker compose logs spark-batch)"
    }
    cue "START — open the dashboard: http://localhost:8501  (Live monitor tab)" \
        "map is GREEN (clean morning); watch 'Latest observation hour' tick every ~10 s" \
        "⚡ Stream throughput bars grow with every micro-batch"
    at 10; cue "INGESTION — Kafka-UI http://localhost:8080 → Topics → nepal.air.raw → Messages" \
               "new messages every 10 s, key = city, source = demo/simulated" \
               "Spark UI http://localhost:4040 → Structured Streaming → input rate per query"
    at 20; cue "SMOG BUILDING — back to the Live tab: cities turn YELLOW → ORANGE" \
               "City explorer → Kathmandu: PM2.5 climbs while the boundary layer (blue) drops"
    at 25; batch
    at 33; cue "FIRES — 🔥 Fires & aerosols tab (Flume → Kafka + HDFS → Spark → MongoDB geo)" \
               "map shows new hotspots around Kathmandu; move the radius slider" \
               "HDFS http://localhost:9870 → Utilities → Browse → /nepairwatch/raw/firms (Flume HDFS sink)"
    at 45; cue "NEWS + HADOOP — 📰 News tab shows the new headline" \
               "Ground stations tab → map: DEMO stations RED; pick 'DEMO Thamel' → 1-minute chart (Cassandra)"
    if running mapreduce; then
      docker compose restart mapreduce >/dev/null
      cue "MAPREDUCE submitted to YARN → http://localhost:8088 (NepAirWatch-AOD-yearly RUNNING → FINISHED)"
    fi
    at 55; cue "HAZARDOUS PEAK — Live tab: map PURPLE/MAROON, 'Unhealthy now' counter jumps" \
               "🚨 CRITICAL model alerts appear (🛰️ measured ones follow once a station's HOURLY mean passes AQI 151)" \
               "Kafka-UI → nepal.air.alerts: the same alerts, ready for an SMS service"
    at 70
    if running hive; then
      docker compose exec -T spark-batch python -u scripts/hive_refresh.py | tail -1
      cue "HIVE — 🐝 Hive SQL tab → 'Ground stations ranked by mean measured PM2.5' → DEMO stations on top"
    fi
    batch
    at 85; cue "RECOVERY — rain + wind: map returns to GREEN; the News tab reports the clean-up" \
               "Pipeline health tab: Kafka offsets, Flume counters, HDFS size, micro-batch chart"
    at 100; cue "DEMO FINISHED — summary: live data flowed producers/Flume → Kafka → Spark → HDFS/Cassandra/MongoDB," \
                "the batch layer recomputed views from HDFS, and every UI followed the episode." \
                "Remove demo data:  scripts/demo.sh cleanup"
    exec bash ;;
esac

MIN=$1
[[ "$MIN" =~ ^[0-9]+$ ]] || { echo "usage: scripts/demo.sh [minutes] | cleanup"; exit 1; }
for s in kafka mongodb spark-streaming dashboard "$APP"; do
  running "$s" || { echo "service '$s' is not running — start the stack first: docker compose up -d"; exit 1; }
done
START=$(date +%s)
SIM="docker compose exec -T $APP python -u - --minutes $MIN --tick 10 < producer/demo_simulator.py"

if ! command -v tmux >/dev/null; then       # no tmux: simulator in front, cues in the background
  echo "tmux not found — running the simulator here and the cues in the background"
  "$SELF" _cues "$MIN" "$START" &
  eval "$SIM"
  exit 0
fi

tmux kill-session -t $SESSION 2>/dev/null || true
tmux new-session -d -s $SESSION -c "$PWD" -x 250 -y 65 "$SIM; echo; echo '[simulator finished]'; exec bash"
tmux set -t $SESSION remain-on-exit on
tmux set -t $SESSION pane-border-status top
tmux select-pane -t $SESSION:0.0 -T "① SIMULATOR — pollution episode → Kafka / Flume"
tmux split-window -t $SESSION -c "$PWD" "$SELF _cues $MIN $START"
tmux select-pane -T "② CUES — what to show, and when (batch runs here)"
tmux split-window -t $SESSION -c "$PWD" "docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic nepal.air.raw --property print.key=true --property key.separator=' | '"
tmux select-pane -T "③ KAFKA — new messages on nepal.air.raw"
tmux split-window -t $SESSION -c "$PWD" "docker compose logs -f --tail 3 spark-streaming"
tmux select-pane -T "④ SPARK STREAMING — micro-batches"
tmux split-window -t $SESSION -c "$PWD" "watch -n 10 -t scripts/pipeline_counts.sh"
tmux select-pane -T "⑤ STORAGE — counts growing (10 s)"
tmux split-window -t $SESSION -c "$PWD" "docker compose logs -f --tail 3 flume"
tmux select-pane -T "⑥ FLUME — fire files → Kafka + HDFS"
tmux select-layout -t $SESSION tiled
tmux attach -t $SESSION
