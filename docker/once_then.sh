#!/usr/bin/env bash
# Run a one-time step (e.g. a historical back-fill) on the very first start only, then exec the
# long-running command.   Usage: once_then.sh <marker-name> "<one-time command>" <command...>
set -euo pipefail
name=$1; once=$2; shift 2
MARKER=/app/data/.${name}_done
mkdir -p /app/data
if [ -f "$MARKER" ]; then
  echo "[$name] one-time step already done ($(cat "$MARKER")) — skipping"
else
  bash -c "$once"
  date -Iseconds > "$MARKER"
fi
if [ $# -gt 0 ]; then exec "$@"; fi
