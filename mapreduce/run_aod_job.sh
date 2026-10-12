#!/usr/bin/env bash
# Submit the AOD MapReduce job to YARN (Hadoop Streaming) once the NASA POWER raw data is in HDFS,
# then repeat every MR_INTERVAL seconds (default: daily). Runs inside the Hadoop image.
set -uo pipefail
IN=/nepairwatch/raw/nasa_power
OUT=/nepairwatch/curated/aod_yearly_mr
JAR=$(ls "$HADOOP_HOME"/share/hadoop/tools/lib/hadoop-streaming-*.jar | head -1)

while true; do
  until hdfs dfs -test -e "$IN/_SUCCESS"; do
    echo "[mapreduce] waiting for $IN/_SUCCESS (power-loader)..."; sleep 30
  done
  hdfs dfs -rm -r -f -skipTrash "$OUT" >/dev/null 2>&1
  echo "[mapreduce] submitting Hadoop Streaming job to YARN: $IN → $OUT"
  hadoop jar "$JAR" \
    -D mapreduce.job.name="NepAirWatch-AOD-yearly" \
    -D mapreduce.job.reduces=2 \
    -D stream.num.map.output.key.fields=1 \
    -files /mapreduce/aod_mapper.py,/mapreduce/aod_reducer.py \
    -mapper "python3 aod_mapper.py" \
    -reducer "python3 aod_reducer.py" \
    -input "$IN/power_daily_*.jsonl" \
    -output "$OUT" \
  && hdfs dfs -cat "$OUT/part-*" | sort | head -20
  echo "[mapreduce] next run in ${MR_INTERVAL:-86400}s"
  sleep "${MR_INTERVAL:-86400}"
done
