"""
NepAirWatch — Streamlit dashboard (presentation layer).

Reads only from the MongoDB serving layer, which is fed by
  * the Spark Structured Streaming job (readings, latest, alerts, daily_stats, stream_batches)
  * the Spark batch / MLlib job (exceedance, monthly_city, hourly_profile, weather_corr,
    province_summary, model_metrics, forecasts, test_predictions)

Run:  streamlit run dashboard/app.py
"""
import sys
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd  # noqa: E402
import plotly.express as px  # noqa: E402
import plotly.graph_objects as go  # noqa: E402
import streamlit as st  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from common.aqi import category  # noqa: E402
from common.config import (KAFKA_BOOTSTRAP, LAKE_PATH, MONGO_DB, MONGO_URI,  # noqa: E402
                           NEPAL_PM25_24H, TOPIC_ALERTS, TOPIC_RAW, WHO_PM25_24H)

st.set_page_config(page_title="NepAirWatch", page_icon="🏔️", layout="wide")

CAT_COLORS = {"Good": "#00e400", "Moderate": "#e6d800", "Unhealthy for Sensitive Groups": "#ff7e00",
              "Unhealthy": "#ff0000", "Very Unhealthy": "#8f3f97", "Hazardous": "#7e0023"}


@st.cache_resource
def db():
    return MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000)[MONGO_DB]


def frame(coll, query=None, proj=None, sort=None, limit=0):
    cur = db()[coll].find(query or {}, {"_id": 0, **(proj or {})})
    if sort:
        cur = cur.sort(sort)
    if limit:
        cur = cur.limit(limit)
    return pd.DataFrame(list(cur))


st.title("🏔️ NepAirWatch — Real-time Air Quality Monitoring & Early Warning for Nepal")
st.caption("Kafka → Spark Structured Streaming / Spark MLlib → Parquet data lake + MongoDB → Dashboard  •  "
           "Data: Open-Meteo (Copernicus CAMS air quality + weather model), 15 cities across all 7 provinces")

tab_live, tab_city, tab_analytics, tab_forecast, tab_pipeline = st.tabs(
    ["📡 Live monitor", "🏙️ City explorer", "📊 Analytics (batch)", "🔮 Early warning (ML)", "⚙️ Pipeline health"])


# --------------------------------------------------------------------------- LIVE
with tab_live:
    @st.fragment(run_every=5)
    def live_panel():
        latest = frame("latest")
        if latest.empty:
            st.warning("No data yet — start the producer and the Spark streaming job.")
            return
        latest = latest.sort_values("aqi", ascending=False)
        newest_ts = latest["event_ts"].max()
        since = newest_ts - timedelta(hours=24)
        alerts_24h = db().alerts.count_documents({"event_ts": {"$gte": since}})
        unhealthy = int((latest["aqi"] >= 151).sum())

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Cities monitored", len(latest))
        c2.metric("Unhealthy now (AQI ≥151)", unhealthy)
        c3.metric("Worst city", latest.iloc[0]["city"], f"AQI {latest.iloc[0]['aqi']}", delta_color="inverse")
        c4.metric("Alerts (last 24 h)", f"{alerts_24h:,}")
        c5.metric("Readings stored", f"{db().readings.estimated_document_count():,}")
        st.caption(f"Latest observation hour: **{newest_ts:%Y-%m-%d %H:%M} NPT** · auto-refresh every 5 s")

        left, right = st.columns([3, 2])
        with left:
            fig = px.scatter_map(latest, lat="lat", lon="lon", color="category", size="pm2_5",
                                 size_max=40, hover_name="city", zoom=5.7, height=470,
                                 center={"lat": 28.2, "lon": 84.1}, color_discrete_map=CAT_COLORS,
                                 hover_data={"aqi": True, "pm2_5": ":.1f", "province": True,
                                             "lat": False, "lon": False},
                                 map_style="carto-positron")
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h", y=-0.05))
            st.plotly_chart(fig, use_container_width=True)
        with right:
            show = latest[["city", "province", "aqi", "category", "pm2_5", "pm10", "wind_speed"]].copy()
            show["advice (नेपाली)"] = show["aqi"].map(lambda a: category(a)["advice_ne"])
            st.dataframe(
                show.style.apply(lambda r: [f"background-color: {CAT_COLORS.get(r['category'], '')}55"] * len(r),
                                 axis=1).format({"pm2_5": "{:.1f}", "pm10": "{:.1f}", "wind_speed": "{:.1f}"}),
                hide_index=True, height=470, use_container_width=True)

        a1, a2 = st.columns([3, 2])
        with a1:
            st.subheader("🚨 Latest alerts")
            al = frame("alerts", proj={"severity": 1, "message": 1, "event_ts": 1, "city": 1},
                       sort=[("event_ts", -1)], limit=12)
            for _, r in al.iterrows():
                (st.error if r["severity"] == "CRITICAL" else st.warning)(f"**{r['severity']}** — {r['message']}")
        with a2:
            st.subheader("⚡ Stream throughput")
            sb = frame("stream_batches", sort=[("at", -1)], limit=60)
            if not sb.empty:
                fig = px.bar(sb.sort_values("at"), x="at", y="records", height=330,
                             labels={"at": "micro-batch time", "records": "records / micro-batch"})
                fig.update_layout(margin=dict(l=0, r=0, t=10, b=0))
                st.plotly_chart(fig, use_container_width=True)
                st.caption(f"Last micro-batch #{sb.iloc[0]['batch_id']} at {sb.iloc[0]['at']:%H:%M:%S} "
                           f"→ {sb.iloc[0]['records']} records, {sb.iloc[0]['alerts']} alerts")
    live_panel()


# --------------------------------------------------------------------------- CITY
with tab_city:
    cities = sorted(db().latest.distinct("_id"))
    if cities:
        col_a, col_b = st.columns([1, 3])
        city = col_a.selectbox("City", cities, index=cities.index("Kathmandu") if "Kathmandu" in cities else 0)
        days = col_a.slider("Days of history", 1, 90, 14)
        cur = db().latest.find_one({"_id": city})
        cat = category(cur["aqi"])
        col_a.markdown(
            f"<div style='padding:14px;border-radius:10px;background:{cat['color']}55'>"
            f"<h2 style='margin:0'>AQI {cur['aqi']}</h2><b>{cat['category']}</b><br>"
            f"PM2.5 {cur['pm2_5']:.1f} µg/m³<br><small>{cat['advice_en']}<br>{cat['advice_ne']}</small></div>",
            unsafe_allow_html=True)
        since = cur["event_ts"] - timedelta(days=days)
        hist = frame("readings", {"city": city, "event_ts": {"$gte": since}}, sort=[("event_ts", 1)])
        with col_b:
            fig = go.Figure()
            fig.add_scatter(x=hist["event_ts"], y=hist["pm2_5"], name="PM2.5", line=dict(color="#c0392b"))
            fig.add_scatter(x=hist["event_ts"], y=hist["pm10"], name="PM10", line=dict(color="#7f8c8d"))
            fig.add_hline(y=NEPAL_PM25_24H, line_dash="dash", annotation_text="Nepal NAAQS 24-h PM2.5 (40)")
            fig.add_hline(y=WHO_PM25_24H, line_dash="dot", line_color="green", annotation_text="WHO 2021 (15)")
            fig.update_layout(height=380, yaxis_title="µg/m³", margin=dict(l=0, r=0, t=30, b=0),
                              title=f"{city}: hourly particulate matter")
            st.plotly_chart(fig, use_container_width=True)
            fig2 = go.Figure()
            fig2.add_scatter(x=hist["event_ts"], y=hist["boundary_layer_height"], name="Boundary-layer height (m)",
                             fill="tozeroy", line=dict(color="#3498db"))
            fig2.add_scatter(x=hist["event_ts"], y=hist["pm2_5"], name="PM2.5", yaxis="y2",
                             line=dict(color="#c0392b"))
            fig2.update_layout(height=300, margin=dict(l=0, r=0, t=30, b=0),
                               title="Low mixing height traps pollution (temperature inversion)",
                               yaxis=dict(title="m"), yaxis2=dict(title="µg/m³", overlaying="y", side="right"))
            st.plotly_chart(fig2, use_container_width=True)
        ds = frame("daily_stats", {"city": city}, sort=[("day", 1)])
        if not ds.empty:
            fig3 = px.bar(ds.tail(120), x="day", y="pm25_avg", color="pm25_avg", height=300,
                          color_continuous_scale="RdYlGn_r", title="Daily mean PM2.5 (Spark streaming window)")
            fig3.add_hline(y=NEPAL_PM25_24H, line_dash="dash")
            st.plotly_chart(fig3, use_container_width=True)


# --------------------------------------------------------------------------- ANALYTICS
with tab_analytics:
    ex = frame("exceedance")
    if ex.empty:
        st.info("Run `python spark_jobs/batch_analytics.py` to populate batch analytics.")
    else:
        st.subheader("How many days in the past year breached air-quality standards?")
        exm = ex.melt(id_vars="city", value_vars=["days_over_nepal_std", "days_over_who"],
                      var_name="standard", value_name="n_days").replace(
            {"days_over_nepal_std": "Nepal NAAQS (40 µg/m³)", "days_over_who": "WHO 2021 (15 µg/m³)"})
        fig = px.bar(exm, x="city", y="n_days", color="standard", barmode="group", height=380,
                     color_discrete_sequence=["#e74c3c", "#f5b041"])
        st.plotly_chart(fig, use_container_width=True)

        c1, c2 = st.columns(2)
        with c1:
            mo = frame("monthly_city")
            piv = mo.pivot(index="city", columns="month", values="pm25_avg")
            fig = px.imshow(piv, color_continuous_scale="RdYlGn_r", aspect="auto", height=460,
                            title="Seasonality: monthly mean PM2.5 (µg/m³)", text_auto=".0f")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            hp = frame("hourly_profile")
            sel = st.multiselect("Diurnal profile cities", sorted(hp["city"].unique()),
                                 default=["Kathmandu", "Biratnagar", "Pokhara", "Nepalgunj"])
            fig = px.line(hp[hp["city"].isin(sel)], x="hour", y="pm25_avg", color="city", markers=True,
                          height=400, title="Diurnal cycle: mean PM2.5 by hour of day (NPT)")
            st.plotly_chart(fig, use_container_width=True)

        c3, c4 = st.columns(2)
        with c3:
            pv = frame("province_summary")
            fig = px.bar(pv, x="province", y="pm25_avg", color="pct_hours_unhealthy", height=380,
                         color_continuous_scale="Reds", title="Province ranking (colour = % hours 'Unhealthy')")
            st.plotly_chart(fig, use_container_width=True)
        with c4:
            wc = frame("weather_corr")
            fig = px.bar(wc, x="driver", y="pearson_r", color="region", barmode="group", height=380,
                         title="Weather drivers: Pearson r with PM2.5")
            st.plotly_chart(fig, use_container_width=True)
        st.dataframe(ex, hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- FORECAST
with tab_forecast:
    mm = db().model_metrics.find_one({}, {"_id": 0})
    if not mm:
        st.info("Run the batch job to train the forecasting model.")
    else:
        st.subheader(f"Spark MLlib {mm['model']} — {mm['horizon_hours']}-hour early warning")
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("RMSE (µg/m³)", mm["rmse"], f"{mm['rmse'] - mm['baseline_rmse']:+.2f} vs persistence",
                  delta_color="inverse")
        m2.metric("MAE (µg/m³)", mm["mae"], f"{mm['mae'] - mm['baseline_mae']:+.2f} vs persistence",
                  delta_color="inverse")
        m3.metric("R²", mm["r2"], f"{mm['r2'] - mm['baseline_r2']:+.3f} vs persistence")
        m4.metric("Train / test rows", f"{mm['train_rows']:,} / {mm['test_rows']:,}")

        fc = frame("forecasts").sort_values("aqi_forecast", ascending=False)
        st.markdown(f"**Forecast for {fc['for_ts'].iloc[0]} NPT** (from observations at {fc['from_ts'].iloc[0]})")
        fc["trend"] = fc.apply(lambda r: "⬆️ worsening" if r["pm25_forecast"] > r["pm25_now"] * 1.05
                               else ("⬇️ improving" if r["pm25_forecast"] < r["pm25_now"] * 0.95 else "➡️ stable"),
                               axis=1)
        st.dataframe(fc[["city", "province", "pm25_now", "pm25_forecast", "aqi_forecast",
                         "category_forecast", "trend"]], hide_index=True, use_container_width=True)
        c1, c2 = st.columns([2, 1])
        with c1:
            tp = frame("test_predictions")
            fig = go.Figure()
            fig.add_scatter(x=tp["event_ts"], y=tp["label"], name="actual t+6h", line=dict(color="#2c3e50"))
            fig.add_scatter(x=tp["event_ts"], y=tp["prediction"], name="GBT forecast", line=dict(color="#e67e22"))
            fig.update_layout(height=380, title="Kathmandu hold-out period: actual vs forecast",
                              margin=dict(l=0, r=0, t=40, b=0))
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            fi = pd.DataFrame(mm["feature_importance"])
            fig = px.bar(fi.sort_values("importance"), x="importance", y="feature", orientation="h",
                         height=380, title="Top feature importances")
            st.plotly_chart(fig, use_container_width=True)


# --------------------------------------------------------------------------- PIPELINE
with tab_pipeline:
    st.subheader("Big-data components")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.markdown("**Apache Kafka** (ingestion / message bus)")
        try:
            from confluent_kafka import Consumer, TopicPartition
            c = Consumer({"bootstrap.servers": KAFKA_BOOTSTRAP, "group.id": "dashboard-probe"})
            rows = []
            for topic in (TOPIC_RAW, TOPIC_ALERTS):
                md = c.list_topics(topic, timeout=3).topics[topic]
                total = 0
                for p in md.partitions:
                    lo, hi = c.get_watermark_offsets(TopicPartition(topic, p), timeout=3)
                    total += hi - lo
                rows.append({"topic": topic, "partitions": len(md.partitions), "messages": total})
            c.close()
            st.dataframe(pd.DataFrame(rows), hide_index=True)
        except Exception as e:  # noqa: BLE001
            st.error(f"Kafka unreachable: {e}")
    with c2:
        st.markdown("**MongoDB** (serving layer)")
        names = sorted(db().list_collection_names())
        st.dataframe(pd.DataFrame([{"collection": n, "documents": db()[n].estimated_document_count()}
                                   for n in names]), hide_index=True)
    with c3:
        st.markdown("**Parquet data lake** (Spark output, HDFS-compatible)")
        lake = Path(LAKE_PATH)
        files = list(lake.rglob("*.parquet"))
        size = sum(f.stat().st_size for f in files) / 1e6
        st.metric("Parquet files", f"{len(files):,}")
        st.metric("Lake size", f"{size:.1f} MB")
        st.caption("Partitioned by city / date — partition pruning keeps per-city queries fast.")
    arch = Path(__file__).resolve().parent.parent / "docs" / "architecture.png"
    if arch.exists():
        st.image(str(arch), caption="NepAirWatch architecture")
