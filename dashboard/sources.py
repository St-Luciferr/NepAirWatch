"""Read-side clients for the dashboard: Cassandra, Hive, HDFS, YARN and Flume.

Every function degrades gracefully (returns None / raises a short message) so the dashboard keeps
working when an optional component (Hive, YARN) is not deployed.
"""
import re
from datetime import timedelta
from pathlib import Path

import pandas as pd
import requests
import streamlit as st

from common.config import (CASSANDRA_HOSTS, CASSANDRA_KEYSPACE, FLUME_METRICS_URL, HIVE_HOST,  # noqa: F401
                           HIVE_PORT, YARN_URL)

NPT_OFFSET = timedelta(hours=5, minutes=45)
QUERIES_HQL = Path(__file__).resolve().parent.parent / "hive" / "queries.hql"


# ----------------------------------------------------------------------------- Cassandra
@st.cache_resource(show_spinner=False)
def cassandra():
    from cassandra.cluster import Cluster
    return Cluster(CASSANDRA_HOSTS, connect_timeout=5).connect(CASSANDRA_KEYSPACE)


def cql_frame(query, params=()):
    """Run CQL → DataFrame. Cassandra returns timestamps as naive UTC: shift them to Nepal time."""
    rows = list(cassandra().execute(query, params, timeout=20))
    df = pd.DataFrame([r._asdict() for r in rows])
    for col in ("event_ts", "hour"):
        if col in df:
            df[col] = pd.to_datetime(df[col]) + NPT_OFFSET
    return df


# ----------------------------------------------------------------------------- Hive
def hive_queries():
    """Named example queries from hive/queries.hql ("-- name: ..." blocks)."""
    if not QUERIES_HQL.exists():
        return {}
    blocks = re.split(r"^-- name:\s*", QUERIES_HQL.read_text(), flags=re.M)[1:]
    out = {}
    for b in blocks:
        name, _, sql = b.partition("\n")
        out[name.strip()] = sql.strip().rstrip(";")
    return out


def hive_frame(sql, limit=1000):
    if not HIVE_HOST:
        raise RuntimeError("Hive is not deployed (start with: docker compose --profile hive up -d)")
    from pyhive import hive
    conn = hive.Connection(host=HIVE_HOST, port=HIVE_PORT, username="nepairwatch", database="nepairwatch")
    try:
        cur = conn.cursor()
        cur.execute(sql.strip().rstrip(";"))
        cols = [c[0].split(".")[-1] for c in cur.description or []]
        return pd.DataFrame(cur.fetchmany(limit), columns=cols)
    finally:
        conn.close()


# ----------------------------------------------------------------------------- YARN / Flume
def yarn_apps(limit=10):
    if not YARN_URL:
        return None
    try:
        r = requests.get(f"{YARN_URL}/ws/v1/cluster/apps", params={"limit": limit}, timeout=3)
        apps = ((r.json().get("apps") or {}).get("app")) or []
    except (requests.RequestException, ValueError):
        return None
    df = pd.DataFrame(apps)
    if df.empty:
        return df
    df["started"] = pd.to_datetime(df["startedTime"], unit="ms") + NPT_OFFSET
    df["elapsed_s"] = (df["elapsedTime"] / 1000).round(1)
    return df.sort_values("started", ascending=False)[["name", "applicationType", "state", "finalStatus",
                                                      "started", "elapsed_s"]].head(limit)


def flume_metrics():
    if not FLUME_METRICS_URL:
        return None
    try:
        m = requests.get(FLUME_METRICS_URL, timeout=3).json()
    except (requests.RequestException, ValueError):
        return None
    rows = []
    for comp, vals in m.items():
        kind, _, name = comp.partition(".")
        keep = {"SOURCE": ["EventReceivedCount", "EventAcceptedCount"],
                "CHANNEL": ["ChannelSize", "ChannelFillPercentage", "EventPutSuccessCount"],
                "SINK": ["EventDrainSuccessCount", "ConnectionFailedCount"]}.get(kind, [])
        rows += [{"component": f"{kind.lower()} {name}", "metric": k, "value": vals[k]} for k in keep if k in vals]
    return pd.DataFrame(rows)
