"""Helpers shared by all NepAirWatch producers: Kafka producer, HTTP with retries, resume state."""
import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from common.config import KAFKA_BOOTSTRAP, STATE_DIR

HEADERS = {"User-Agent": "NepAirWatch/2.0 (academic big-data project)"}


def http_get(url, params=None, retries=6, as_json=True, timeout=60, ok_404=False):
    """GET with exponential back-off. Returns parsed JSON (or text). With ok_404, a 404 returns None."""
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=timeout, headers=HEADERS)
            if ok_404 and r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json() if as_json else r.text
        except (requests.RequestException, ValueError) as e:
            wait = min(2 ** attempt * 5, 300)
            print(f"  ! {e} — retrying in {wait}s", flush=True)
            time.sleep(wait)
    raise RuntimeError(f"failed to fetch {url}")


def build_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "linger.ms": 50,
        "compression.type": "lz4",
        "acks": "all",
        "enable.idempotence": True,
        "queue.buffering.max.messages": 500_000,
    })


def _delivery(err, msg):
    if err is not None:
        print(f"  ! delivery failed: {err}", flush=True)


def publish(producer, topic, key, value):
    """Produce one message; `value` is a dict (sent as JSON) or a str (sent as-is, e.g. a CSV line)."""
    payload = value if isinstance(value, str) else json.dumps(value)
    while True:
        try:
            producer.produce(topic, key=str(key).encode(), value=payload.encode(), on_delivery=_delivery)
            break
        except BufferError:          # local queue full during bulk back-fills: let it drain
            producer.poll(0.5)
    producer.poll(0)


def now_utc_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def load_state(name, default):
    p = STATE_DIR / f"{name}.json"
    try:
        return json.loads(p.read_text())
    except (OSError, ValueError):
        return default


def save_state(name, state):
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    tmp = STATE_DIR / f"{name}.json.tmp"
    tmp.write_text(json.dumps(state))
    tmp.replace(STATE_DIR / f"{name}.json")
