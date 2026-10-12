"""
NepAirWatch — demo control panel (Streamlit, port 8502).

Compose readings at any AQI level for any topic and publish them to Kafka. The running Spark
Structured Streaming job picks them up automatically; the panel then waits until the results land in
MongoDB and shows how long the pipeline took. Open the dashboard (port 8501) side by side to watch.

  🎬 Scenarios       one click → several topics at once (smog episode, fire season, rain, ...)
  🌐 City readings   nepal.air.raw     — 15 cities, AQI level per city, one or many rounds
  🛰️ Ground stations nepal.ground.raw  — demo stations, live minutes or a full past hour (measured alert)
  🔥 Fires           nepal.fires.raw   — hotspots around a city, via Flume (spool dir) or Kafka direct
  📰 News            nepal.news.raw    — a headline

Every record is tagged as demo data (source "demo/…"); the batch layer ignores it and the
"Remove demo data" button cleans the serving stores.

Run:  streamlit run demo_ui/app.py --server.port 8502      (docker: docker compose --profile control up -d)
"""
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import streamlit as st  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from common.aqi import aqi_to_pm25, category, pm25_to_aqi  # noqa: E402
from common.config import (CITIES, KAFKA_BOOTSTRAP, MONGO_DB, MONGO_URI, TOPIC_GROUND,  # noqa: E402
                           TOPIC_NEWS, TOPIC_RAW)
from common.demo import (DEMO_STATIONS, LEVELS, NPT, air_message, flume_available, fire_rows,  # noqa: E402
                         ground_message, news_message, send_fires)
from producer.common import build_producer, publish  # noqa: E402

st.set_page_config(page_title="NepAirWatch demo control", page_icon="🎛️", layout="wide")

CAT_COLORS = {"Good": "#00e400", "Moderate": "#e6d800", "Unhealthy for Sensitive Groups": "#ff7e00",
              "Unhealthy": "#ff0000", "Very Unhealthy": "#8f3f97", "Hazardous": "#7e0023"}
LEVEL_NAMES = list(LEVELS)
CITY_INFO = {c: (p, la, lo) for c, p, la, lo in CITIES}
STATION_INFO = {name: (sid, la, lo) for sid, name, la, lo in DEMO_STATIONS}
HEADLINES = [
    "Smog thickens over Kathmandu Valley as AQI crosses 200",
    "Schools in Kathmandu suspend outdoor activities amid hazardous air",
    "Forest fires in Shivapuri and Makwanpur send smoke over the valley",
    "Haze from the Indo-Gangetic Plain blankets the Terai",
    "Rain clears the haze; air quality improves across Nepal",
]


@st.cache_resource
def producer():
    return build_producer()


@st.cache_resource
def db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[MONGO_DB]


def naive_npt(dt):
    """MongoDB stores event times as naive Nepal wall-clock (the Spark session time zone)."""
    return dt.astimezone(NPT).replace(tzinfo=None, microsecond=0)


def log(topic, n, detail):
    st.session_state.setdefault("history", []).insert(
        0, {"time": datetime.now(NPT).strftime("%H:%M:%S"), "topic": topic, "messages": n, "detail": detail})
    st.session_state.setdefault("sent", Counter())[topic] += n


def wait_for_spark(label, check, timeout):
    """Poll MongoDB until Spark has written the results (check() → (done, total))."""
    t0 = time.time()
    with st.status(f"⏳ Waiting for Spark to process {label}…") as s:
        done, total = 0, 1
        while time.time() - t0 < timeout:
            done, total = check()
            if done >= total:
                s.update(label=f"✅ Spark processed {label}: {done}/{total} in {time.time() - t0:.1f} s "
                               f"→ look at the dashboard", state="complete")
                return True
            time.sleep(1)
        s.update(label=f"⚠️ {label}: {done}/{total} after {timeout} s — is spark-streaming running?",
                 state="error")
        return False


# ----------------------------------------------------------------------------- senders
def send_cities(levels, rounds=1, interval=10, jitter=0.1, show=True):
    """levels: {city: level name}. Publishes `rounds` readings per city with fresh timestamps."""
    rng, p = random.Random(), producer()
    bar = st.progress(0.0, "sending…") if show and rounds > 1 else None
    first = None
    for r in range(rounds):
        when = datetime.now(NPT).replace(microsecond=0)
        first = first or when
        for city, level in levels.items():
            prov, la, lo = CITY_INFO[city]
            pm = aqi_to_pm25(LEVELS[level]) * rng.uniform(1 - jitter, 1 + jitter)
            publish(p, TOPIC_RAW, city, air_message(city, prov, la, lo, pm, when=when))
        p.flush()
        if bar:
            bar.progress((r + 1) / rounds, f"round {r + 1}/{rounds} sent")
        if r < rounds - 1:
            time.sleep(interval)
    log(TOPIC_RAW, rounds * len(levels), f"{len(levels)} cities × {rounds} round(s)")
    if show:
        last = naive_npt(when)
        wait_for_spark(f"{len(levels)} city readings", lambda: (db().latest.count_documents(
            {"_id": {"$in": list(levels)}, "event_ts": {"$gte": last}}), len(levels)), 45)
        n_alerts = db().alerts.count_documents({"source": {"$regex": "^demo"}, "event_ts": {"$gte": naive_npt(first)}})
        if n_alerts:
            st.error(f"🚨 {n_alerts} alert(s) raised by Spark → dashboard Live tab + Kafka nepal.air.alerts")


def send_ground(names, level, mode, rounds=1, interval=10, show=True):
    rng, p = random.Random(), producer()
    pm0 = aqi_to_pm25(LEVELS[level])
    if mode == "hour":   # 60 one-minute readings for the previous Nepal clock hour
        hour = datetime.now(NPT).replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)
        times = [hour + timedelta(minutes=i) for i in range(60)]
        batches = [times]
    else:
        batches = [None] * rounds
    n = 0
    for b, times in enumerate(batches):
        for when in (times or [datetime.now(timezone.utc).replace(microsecond=0)]):
            for name in names:
                sid, la, lo = STATION_INFO[name]
                publish(p, TOPIC_GROUND, sid, ground_message(sid, name, la, lo, pm0 * rng.uniform(0.9, 1.1),
                                                             when=when))
                n += 1
        p.flush()
        last = (times or [datetime.now(timezone.utc)])[-1]
        if mode != "hour" and b < len(batches) - 1:
            time.sleep(interval)
    when_txt = "previous hour" if mode == "hour" else f"{rounds} round(s)"
    log(TOPIC_GROUND, n, f"{len(names)} station(s), {level}, {when_txt}")
    if not show:
        return
    ids = [STATION_INFO[nm][0] for nm in names]
    since = naive_npt(last) - timedelta(seconds=1)
    wait_for_spark(f"{len(names)} station reading(s)", lambda: (db().ground_latest.count_documents(
        {"_id": {"$in": ids}, "event_ts": {"$gte": since}}), len(ids)), 60)
    if mode == "hour" and LEVELS[level] >= 151:
        hour_start = naive_npt(batches[0][0])
        wait_for_spark("the hourly window → 🛰️ measured alerts", lambda: (db().alerts.count_documents(
            {"kind": "ground", "station_id": {"$in": ids}, "event_ts": {"$gte": hour_start}}), len(ids)), 90)


def send_fire_wave(city, count, radius, frp, confidence, via_flume, show=True):
    _, la, lo = CITY_INFO[city]
    rows = fire_rows(la, lo, count, radius, frp, confidence)
    via = send_fires(producer(), rows, via_flume)
    log("nepal.fires.raw", count, f"{count} hotspots ≤{radius} km of {city} via {via}")
    if show:
        st.info(f"🔥 {count} hotspots sent via **{via}**")
        ids = [r[0] for r in rows]
        wait_for_spark(f"{count} hotspots", lambda: (db().fires.count_documents({"fire_id": {"$in": ids}}), count), 120)
    return rows


def send_news(title, show=True):
    msg = news_message(title)
    publish(producer(), TOPIC_NEWS, msg["id"], msg)
    producer().flush()
    log(TOPIC_NEWS, 1, title)
    if show:
        wait_for_spark("the headline", lambda: (db().news.count_documents({"id": msg["id"]}), 1), 120)


def level_map(levels):
    """Preview: the 15 cities coloured by the level that will be sent."""
    df = pd.DataFrame([{"city": c, "lat": CITY_INFO[c][1], "lon": CITY_INFO[c][2], "level": lv,
                        "AQI": LEVELS[lv], "PM2.5": aqi_to_pm25(LEVELS[lv])} for c, lv in levels.items()])
    if df.empty:
        return
    fig = px.scatter_map(df, lat="lat", lon="lon", color="level", hover_name="city", size="AQI", size_max=28,
                         zoom=5.4, height=380, center={"lat": 28.2, "lon": 84.1}, color_discrete_map=CAT_COLORS,
                         hover_data={"AQI": True, "PM2.5": True, "lat": False, "lon": False},
                         map_style="carto-positron")
    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h", y=-0.05))
    st.plotly_chart(fig, use_container_width=True)


# ----------------------------------------------------------------------------- layout
st.title("🎛️ NepAirWatch — demo control panel")
st.caption("Pick a topic, choose AQI levels, press send. Messages go to **Kafka**; **Spark Structured Streaming** "
           "processes them automatically and this page confirms when the results reach MongoDB. Keep the "
           "dashboard (http://localhost:8501) open side by side. All records are tagged as demo data.")

with st.sidebar:
    st.header("Pipeline")
    try:
        md = producer().list_topics(timeout=3)
        st.success(f"Kafka ✓  {KAFKA_BOOTSTRAP} · {len([t for t in md.topics if t.startswith('nepal.')])} topics")
    except Exception as e:  # noqa: BLE001
        st.error(f"Kafka unreachable: {e}")
    try:
        db().command("ping")
        st.success("MongoDB ✓")
    except Exception as e:  # noqa: BLE001
        st.error(f"MongoDB unreachable: {e}")
    st.markdown("**Open alongside**\n- [Dashboard](http://localhost:8501)\n- [Kafka-UI](http://localhost:8080)\n"
                "- [Spark UI](http://localhost:4040)\n- [HDFS](http://localhost:9870)")
    st.header("Sent this session")
    for t, n in st.session_state.get("sent", Counter()).items():
        st.write(f"`{t}` — {n:,}")
    st.divider()
    if st.button("🧹 Remove demo data", help="MongoDB + Cassandra demo rows; restores real latest values"):
        sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
        from demo_cleanup import cleanup
        lines = []
        cleanup(log=lines.append)
        st.code("\n".join(lines))

tab_sc, tab_city, tab_ground, tab_fire, tab_news = st.tabs(
    ["🎬 Scenarios", "🌐 City readings", "🛰️ Ground stations", "🔥 Fires", "📰 News"])

VALLEY = ["Kathmandu", "Lalitpur", "Bhaktapur"]
SCENARIOS = {
    "🟢 Clean country": dict(cities={c: "Good" for c in CITY_INFO}, news=None, fires=None, ground=("Good",)),
    "🌫️ Kathmandu Valley smog": dict(
        cities={c: ("Very Unhealthy" if c in VALLEY else "Unhealthy" if CITY_INFO[c][0] == "Bagmati" else "Moderate")
                for c in CITY_INFO},
        news=HEADLINES[0], fires=None, ground=("Very Unhealthy",)),
    "🔥 Forest-fire season": dict(
        cities={c: ("Unhealthy" if CITY_INFO[c][0] in ("Bagmati", "Gandaki", "Lumbini") else "Unhealthy for Sensitive Groups")
                for c in CITY_INFO},
        news=HEADLINES[2], fires=("Kathmandu", 60, 80), ground=("Unhealthy",)),
    "☠️ Terai haze (cross-border)": dict(
        cities={c: ("Hazardous" if CITY_INFO[c][0] in ("Madhesh", "Sudurpashchim") else
                    "Very Unhealthy" if CITY_INFO[c][0] in ("Koshi", "Lumbini") else "Unhealthy for Sensitive Groups")
                for c in CITY_INFO},
        news=HEADLINES[3], fires=None, ground=None),
    "🌧️ Rain clears the air": dict(cities={c: "Good" for c in CITY_INFO}, news=HEADLINES[4], fires=None,
                                   ground=("Good",)),
}

with tab_sc:
    st.subheader("One click → several topics at once")
    st.caption("Tip for the demo: Clean country → Kathmandu Valley smog → Forest-fire season → Terai haze → Rain.")
    pick = st.radio("Scenario", list(SCENARIOS), horizontal=True)
    sc = SCENARIOS[pick]
    left, right = st.columns([3, 2])
    with left:
        level_map(sc["cities"])
    with right:
        st.markdown("**Will send**")
        st.markdown(f"- `nepal.air.raw`: 15 city readings ({', '.join(sorted(set(sc['cities'].values())))})")
        if sc["ground"]:
            st.markdown(f"- `nepal.ground.raw`: demo stations at **{sc['ground'][0]}**")
        if sc["fires"]:
            st.markdown(f"- `nepal.fires.raw`: {sc['fires'][1]} hotspots ≤{sc['fires'][2]} km of {sc['fires'][0]} (Flume)")
        if sc["news"]:
            st.markdown(f"- `nepal.news.raw`: “{sc['news']}”")
        rounds = st.number_input("Rounds of city readings", 1, 30, 3, key="sc_rounds",
                                 help="several rounds keep the throughput chart moving")
        if st.button("▶ Send scenario", type="primary", use_container_width=True):
            if sc["ground"]:
                send_ground([n for n in STATION_INFO], sc["ground"][0], "live", show=False)
            if sc["fires"]:
                c, n, r = sc["fires"]
                send_fire_wave(c, n, r, (20, 120), "high", True, show=False)
            if sc["news"]:
                send_news(sc["news"], show=False)
            send_cities(sc["cities"], rounds=rounds, interval=5)

with tab_city:
    st.subheader("nepal.air.raw — choose an AQI level per city")
    if "city_ver" not in st.session_state:
        st.session_state.city_ver = 0
        st.session_state.city_table = pd.DataFrame(
            [{"send": True, "city": c, "province": p, "level": "Moderate"} for c, p, _, _ in CITIES])
    a1, a2, a3 = st.columns([2, 1, 1])
    all_level = a2.selectbox("Set every city to", LEVEL_NAMES, index=3)
    if a3.button("Apply to all", use_container_width=True):
        st.session_state.city_table["level"] = all_level
        st.session_state.city_ver += 1
    edited = st.data_editor(
        st.session_state.city_table, key=f"city_editor_{st.session_state.city_ver}", hide_index=True,
        use_container_width=True, height=565,
        column_config={"send": st.column_config.CheckboxColumn("send"),
                       "city": st.column_config.TextColumn(disabled=True),
                       "province": st.column_config.TextColumn(disabled=True),
                       "level": st.column_config.SelectboxColumn("AQI level", options=LEVEL_NAMES, required=True)})
    chosen = {r.city: r.level for r in edited.itertuples() if r.send}
    with a1:
        level_map(chosen)
    o1, o2, o3 = st.columns(3)
    rounds = o1.number_input("Rounds", 1, 60, 1, help="each round = a fresh reading for every selected city")
    interval = o2.number_input("Seconds between rounds", 3, 60, 10)
    jitter = o3.slider("Random variation ±%", 0, 30, 10) / 100
    if st.button(f"▶ Send to Kafka ({len(chosen)} cities × {rounds})", type="primary", disabled=not chosen):
        send_cities(chosen, rounds, interval, jitter)

with tab_ground:
    st.subheader("nepal.ground.raw — demo monitoring stations (stored minute-by-minute in Cassandra)")
    g1, g2 = st.columns(2)
    names = g1.multiselect("Stations", list(STATION_INFO), default=list(STATION_INFO)[:3])
    level = g2.select_slider("AQI level", LEVEL_NAMES, value="Very Unhealthy")
    pm = aqi_to_pm25(LEVELS[level])
    st.markdown(f"<div style='padding:8px;border-radius:8px;background:{CAT_COLORS[level]}55'>"
                f"PM2.5 ≈ <b>{pm} µg/m³</b> → AQI {pm25_to_aqi(pm)} <b>{level}</b> · "
                f"{category(pm25_to_aqi(pm))['advice_en']}</div>", unsafe_allow_html=True)
    mode = st.radio("Timestamps", ["live", "hour"], horizontal=True, format_func=lambda m: {
        "live": "Live: one reading per station per round (now)",
        "hour": "Fill the previous clock hour (60 readings → hourly window → 🛰️ measured alert)"}[m])
    r1, r2 = st.columns(2)
    g_rounds = r1.number_input("Rounds", 1, 60, 5, key="g_rounds", disabled=mode == "hour")
    g_interval = r2.number_input("Seconds between rounds", 3, 60, 10, key="g_int", disabled=mode == "hour")
    if st.button("▶ Send station readings", type="primary", disabled=not names):
        send_ground(names, level, mode, g_rounds, g_interval)
    st.caption("Dashboard → 🛰️ Ground stations: map dot colour; pick the station for the 1-minute chart (Cassandra).")

with tab_fire:
    st.subheader("nepal.fires.raw — fire hotspots (NASA FIRMS format)")
    f1, f2, f3 = st.columns(3)
    fcity = f1.selectbox("Around city", list(CITY_INFO), index=0)
    fcount = f2.slider("Number of hotspots", 1, 200, 40)
    fradius = f3.slider("Within radius (km)", 10, 150, 60)
    f4, f5, f6 = st.columns(3)
    intensity = f4.selectbox("Intensity (FRP)", ["Small (5–20 MW)", "Medium (20–80 MW)", "Large (80–300 MW)"], index=1)
    frp = {"Small": (5, 20), "Medium": (20, 80), "Large": (80, 300)}[intensity.split()[0]]
    conf = f5.selectbox("Confidence", ["high", "nominal", "low"])
    via_flume = f6.radio("Route", ["Flume (spool dir → Kafka + HDFS)", "Kafka direct"],
                         index=0 if flume_available() else 1, disabled=not flume_available()).startswith("Flume")
    if not flume_available():
        st.caption("Flume spool dir not found here — sending straight to Kafka.")
    if st.button(f"▶ Send {fcount} hotspots", type="primary"):
        rows = send_fire_wave(fcity, fcount, fradius, frp, conf, via_flume)
        pts = pd.DataFrame({"lat": [float(r[1]) for r in rows], "lon": [float(r[2]) for r in rows],
                            "frp": [float(r[7]) for r in rows]})
        st.plotly_chart(px.scatter_map(pts, lat="lat", lon="lon", size="frp", size_max=14, zoom=7, height=360,
                                       color_discrete_sequence=["#e65100"], map_style="carto-positron"),
                        use_container_width=True)
    st.caption("Dashboard → 🔥 Fires & aerosols: the hotspots on the map and in the radius count.")

with tab_news:
    st.subheader("nepal.news.raw — a headline (themes are tagged by Spark in the stream)")
    preset = st.selectbox("Preset", HEADLINES)
    title = st.text_input("Headline", preset)
    if st.button("▶ Publish headline", type="primary", disabled=not title.strip()):
        send_news(title.strip())
    st.caption("Dashboard → 📰 News. The news query triggers every 60 s, so allow up to a minute.")

hist = st.session_state.get("history")
if hist:
    with st.expander(f"Sent this session ({len(hist)} sends)", expanded=False):
        st.dataframe(pd.DataFrame(hist), hide_index=True, use_container_width=True)
