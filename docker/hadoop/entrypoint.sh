#!/usr/bin/env bash
# Start one Hadoop daemon in the foreground: namenode | datanode | resourcemanager | nodemanager
# Anything else is executed as a command (e.g. the MapReduce job script).
set -euo pipefail
role=${1:-}
case "$role" in
  namenode)
    if [ ! -f /hadoop/dfs/name/current/VERSION ]; then
      echo "[namenode] formatting a new HDFS file system"
      hdfs namenode -format -force -nonInteractive nepairwatch
    fi
    exec hdfs namenode ;;
  datanode)        exec hdfs datanode ;;
  resourcemanager) exec yarn resourcemanager ;;
  nodemanager)     exec yarn nodemanager ;;
  *)               exec "$@" ;;
esac
