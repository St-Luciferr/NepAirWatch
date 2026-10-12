"""
NepAirWatch — Streamlit dashboard (presentation layer).

Serving layer of the Lambda architecture:
  * MongoDB   — speed views (readings, latest, alerts, daily_stats, ground_latest, fires, news) and
                batch views (exceedance, forecasts, advisories, cams_validation, aod_trend, ...)
  * Cassandra — 1-minute ground-station time series (ground_readings, ground_hourly)
  * Hive      — ad-hoc SQL over the HDFS data lake (optional, profile "hive")
  * HDFS / YARN / Flume / Kafka — pipeline health

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
from common.config import (ALL_TOPICS, CITIES, FIRE_NEAR_KM, KAFKA_BOOTSTRAP, LAKE_PATH,  # noqa: E402
                           MONGO_DB, MONGO_URI, NEPAL_PM25_24H, RAW_PATH, WHO_PM25_24H)
from common.storage import summary as storage_summary  # noqa: E402
from dashboard.sources import (cql_frame, flume_metrics, hive_frame, hive_queries,  # noqa: E402
                               yarn_apps)

st.set_page_config(page_title="NepAirWatch", page_icon="🏔️", layout="wide")

CAT_COLORS = {"Good": "#00e400", "Moderate": "#e6d800", "Unhealthy for Sensitive Groups": "#ff7e00",
              "Unhealthy": "#ff0000", "Very Unhealthy": "#8f3f97", "Hazardous": "#7e0023"}


def now_npt():
    """Wall-clock Nepal time, matching how event times are stored (naive NPT, the Spark session zone)."""
    return datetime.utcnow() + timedelta(hours=5, minutes=45)


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
st.caption("Flume / Kafka → Spark Structured Streaming + Spark SQL / MLlib + Hadoop MapReduce → HDFS lake, "
           "Cassandra, MongoDB, Hive → Dashboard  •  Data: Open-Meteo CAMS model + weather, DoEnv Nepal "
           "stations, Sensor.Community, NASA FIRMS fires, NASA POWER (2001–), news headlines")

(tab_live, tab_ground, tab_city, tab_analytics, tab_fires, tab_forecast, tab_news, tab_hive,
 tab_pipeline) = st.tabs(["📡 Live monitor", "🛰️ Ground stations", "🏙️ City explorer", "📊 Analytics (batch)",
                          "🔥 Fires & aerosols", "🔮 Early warning (ML)", "📰 News", "🐝 Hive SQL",
                          "⚙️ Pipeline health"])


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
        c5.metric("Ground stations live", db().ground_latest.count_documents(
            {"event_ts": {"$gte": now_npt() - timedelta(hours=3)}}))
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
            al = frame("alerts", proj={"severity": 1, "message": 1, "event_ts": 1, "city": 1, "kind": 1},
                       sort=[("event_ts", -1)], limit=12)
            for _, r in al.iterrows():
                icon = "🛰️ measured" if r.get("kind") == "ground" else "🌐 model"
                (st.error if r["severity"] == "CRITICAL" else st.warning)(
                    f"**{r['severity']}** ({icon}) — {r['message']}")
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

        st.subheader("🧭 Recommended actions (prescriptive analytics)")
        adv = frame("advisories")
        if adv.empty:
            st.success("No action needed: every city is forecast to stay Good.")
        else:
            adv = adv.sort_values(["aqi_forecast", "priority"], ascending=[False, True])
            for city_name, g in adv.groupby("city", sort=False):
                r0 = g.iloc[0]
                trend = " · ⬆️ worsening" if r0["worsening"] else ""
                with st.expander(f"{city_name} — forecast AQI {r0['aqi_forecast']} "
                                 f"({r0['category_forecast']}) at {r0['for_ts']}{trend}",
                                 expanded=r0["aqi_forecast"] >= 151):
                    st.dataframe(g[["audience", "action"]], hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- GROUND STATIONS
with tab_ground:
    gl = frame("ground_latest")
    if gl.empty:
        st.info("No ground-station data yet — start `producer-ground` and the streaming job.")
    else:
        fresh = gl[gl["event_ts"] >= now_npt() - timedelta(hours=3)]
        worst = gl.sort_values("pm2_5", ascending=False).iloc[0]
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Stations / sensors", len(gl))
        c2.metric("Reporting (last 3 h)", len(fresh))
        c3.metric("Networks", gl["network"].nunique())
        c4.metric("Highest PM2.5 now", worst["station"], f"{worst['pm2_5']:.0f} µg/m³", delta_color="inverse")
        st.caption("DoEnv Nepal AQMS (reference stations, 1-min) + Sensor.Community (low-cost citizen sensors). "
                   "Measured values — compare with the CAMS model on the Live tab.")
        left, right = st.columns([3, 2])
        with left:
            fig = px.scatter_map(gl.dropna(subset=["pm2_5"]), lat="lat", lon="lon", color="category",
                                 size="pm2_5", size_max=30, hover_name="station", zoom=5.7, height=470,
                                 center={"lat": 28.2, "lon": 84.1}, color_discrete_map=CAT_COLORS,
                                 hover_data={"network": True, "pm2_5": ":.1f", "pm10": ":.1f", "event_ts": True,
                                             "lat": False, "lon": False}, map_style="carto-positron")
            fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h", y=-0.05))
            st.plotly_chart(fig, use_container_width=True)
        with right:
            st.dataframe(gl.sort_values("pm2_5", ascending=False)[
                ["station", "network", "city", "pm2_5", "pm10", "aqi", "event_ts"]],
                hide_index=True, height=470, use_container_width=True)

        st.subheader("Station explorer (Apache Cassandra time series)")
        labels = {f"{r['station']} ({r['network']})": r["_id"] for r in
                  db().ground_latest.find({}, {"station": 1, "network": 1}).sort("station", 1)}
        s1, s2, s3 = st.columns([2, 1, 1])
        pick = s1.selectbox("Station", list(labels))
        day = s2.date_input("Day (minute data)", value=gl["event_ts"].max().date())
        ndays = s3.slider("Hourly history (days)", 1, 30, 7)
        sid = labels[pick]
        try:
            minute = cql_frame("SELECT event_ts, pm1, pm2_5, pm10 FROM ground_readings "
                               "WHERE station_id = %s AND day = %s", (sid, day))
            hourly = cql_frame("SELECT hour, pm2_5, pm10, pm2_5_max, n_obs, layer FROM ground_hourly "
                               "WHERE station_id = %s AND hour >= %s",
                               (sid, datetime.utcnow() - timedelta(days=ndays)))
            g1, g2 = st.columns(2)
            with g1:
                fig = go.Figure()
                for col, color in (("pm2_5", "#c0392b"), ("pm10", "#7f8c8d"), ("pm1", "#8e44ad")):
                    if col in minute and minute[col].notna().any():
                        fig.add_scatter(x=minute["event_ts"], y=minute[col], name=col.upper().replace("_", "."),
                                        line=dict(color=color, width=1))
                fig.update_layout(height=340, title=f"1-minute readings on {day} ({len(minute):,} rows)",
                                  yaxis_title="µg/m³", margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, use_container_width=True)
            with g2:
                fig = go.Figure()
                if not hourly.empty:
                    fig.add_bar(x=hourly["hour"], y=hourly["pm2_5"], name="hourly mean PM2.5",
                                marker_color=[("#2e86c1" if lyr == "batch" else "#f5b041") for lyr in hourly["layer"]])
                fig.add_hline(y=NEPAL_PM25_24H, line_dash="dash", annotation_text="Nepal 24-h std")
                fig.update_layout(height=340, title="Hourly means — blue: batch view, orange: speed view",
                                  yaxis_title="µg/m³", margin=dict(l=0, r=0, t=40, b=0))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:  # noqa: BLE001
            st.warning(f"Cassandra unavailable: {e}")

        st.subheader("Is the CAMS model right? Validation against measured PM2.5")
        val = frame("cams_validation")
        cm = db().calibration_metrics.find_one({}, {"_id": 0})
        if val.empty:
            st.info("Validation appears after the batch job has matched model hours with station hours.")
        else:
            v1, v2 = st.columns([3, 2])
            with v1:
                st.dataframe(val, hide_index=True, use_container_width=True)
                st.caption("bias = model − measured (µg/m³). Positive: the 40 km model over-estimates the "
                           "station; negative: it under-estimates.")
            with v2:
                if cm:
                    st.metric("Calibrated RMSE (µg/m³)", cm["rmse"], f"{cm['rmse'] - cm['raw_rmse']:+.1f} vs raw CAMS",
                              delta_color="inverse")
                    st.metric("Calibrated R²", cm["r2"], f"{cm['r2'] - cm['raw_r2']:+.2f} vs raw CAMS")
                    st.caption(f"{cm['model']} · {cm['cv_folds']}-fold CrossValidator over {cm['grid_size']} "
                               f"param sets · best: {cm['best_num_trees']} trees, depth {cm['best_max_depth']} · "
                               f"{cm['train_rows']:,} train / {cm['test_rows']:,} test rows")
            sample = frame("calibration_sample")
            if not sample.empty:
                long = sample.melt(id_vars=["ground_pm25"], value_vars=["cams_pm25", "calibrated_pm25"],
                                   var_name="estimate", value_name="pm25")
                fig = px.scatter(long, x="ground_pm25", y="pm25", color="estimate", opacity=0.5, height=380,
                                 labels={"ground_pm25": "measured PM2.5", "pm25": "estimated PM2.5"},
                                 title="Hold-out hours: raw model vs MLlib-calibrated (dashed = perfect)")
                mx = float(long[["ground_pm25", "pm25"]].max().max())
                fig.add_scatter(x=[0, mx], y=[0, mx], mode="lines", line=dict(dash="dash", color="grey"),
                                showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            cal = frame("calibrated_latest")
            if not cal.empty:
                st.markdown("**Calibrated now-cast for all 15 cities** (model corrected with ground truth)")
                st.dataframe(cal, hide_index=True, use_container_width=True)
        ss = frame("station_summary")
        if not ss.empty:
            with st.expander("Station summary (batch view): coverage and days above standards"):
                st.dataframe(ss, hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- FIRES & AEROSOLS
with tab_fires:
    st.subheader("🔥 Active fires — NASA FIRMS (VIIRS / MODIS), ingested with Apache Flume")
    f1, f2, f3 = st.columns([1, 1, 2])
    hours = f1.slider("Last … hours", 24, 168, 48, step=24)
    city_names = [c[0] for c in CITIES]
    near_city = f2.selectbox("Fires near", city_names, index=city_names.index("Kathmandu"))
    radius = f3.slider("Radius (km)", 25, 300, int(FIRE_NEAR_KM), step=25)
    fires = frame("fires", {"acq_ts": {"$gte": now_npt() - timedelta(hours=hours)}},
                  proj={"loc": 0})
    if fires.empty:
        st.info("No fire hotspots in the selected period (or `producer-fires` / Flume not running yet).")
    else:
        lat0, lon0 = next((la, lo) for c, _, la, lo in CITIES if c == near_city)
        # MongoDB geospatial query (2dsphere index): hotspots within the radius of the city
        near = db().fires.count_documents({
            "acq_ts": {"$gte": now_npt() - timedelta(hours=hours)},
            "loc": {"$geoWithin": {"$centerSphere": [[lon0, lat0], radius / 6378.1]}}})
        m1, m2, m3 = st.columns(3)
        m1.metric("Hotspots in region", f"{len(fires):,}")
        m2.metric(f"Within {radius} km of {near_city}", f"{near:,}")
        m3.metric("High-confidence", f"{(fires['confidence'] == 'high').sum():,}")
        fig = px.scatter_map(fires, lat="latitude", lon="longitude", color="instrument", size="frp", size_max=14,
                             opacity=0.7, zoom=5.2, height=480, center={"lat": 27.8, "lon": 84.2},
                             hover_data={"acq_ts": True, "frp": ":.1f", "confidence": True, "nearest_city": True,
                                         "city_km": True, "latitude": False, "longitude": False},
                             map_style="carto-positron")
        fig.add_scattermap(lat=[c[2] for c in CITIES], lon=[c[3] for c in CITIES], text=city_names,
                           mode="markers+text", marker=dict(size=8, color="black"), name="cities",
                           textposition="top right")
        fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h", y=-0.05))
        st.plotly_chart(fig, use_container_width=True)

    fd, fc_ = frame("fire_daily"), frame("fire_pm_corr")
    if not fd.empty:
        c1, c2 = st.columns([3, 2])
        with c1:
            fig = px.bar(fd, x="acq_date", y=["hotspots_near_cities", "hotspots_region"], barmode="overlay",
                         height=340, title=f"Daily hotspots (near = within {FIRE_NEAR_KM:.0f} km of a city)")
            st.plotly_chart(fig, use_container_width=True)
        with c2:
            st.markdown("**Do fires raise PM2.5?** Pearson r of daily PM2.5 with fires near the city")
            st.dataframe(fc_, hide_index=True, use_container_width=True, height=300)

    st.subheader("🌫️ 25 years of aerosols — NASA POWER AOD (HDFS raw zone → MapReduce / Spark)")
    ay, at = frame("aod_yearly"), frame("aod_trend")
    if ay.empty:
        st.info("Waiting for `power-loader` and the batch job.")
    else:
        a1, a2 = st.columns([3, 2])
        with a1:
            pick = st.multiselect("Cities", sorted(ay["city"].unique()),
                                  default=["Kathmandu", "Biratnagar", "Nepalgunj", "Pokhara"])
            fig = px.line(ay[ay["city"].isin(pick)], x="year", y="aod_mean", color="city", markers=True,
                          height=380, title="Annual mean aerosol optical depth (550 nm)")
            st.plotly_chart(fig, use_container_width=True)
        with a2:
            fig = px.bar(at.sort_values("aod_change_per_decade"), x="aod_change_per_decade", y="city",
                         orientation="h", height=380, color="aod_change_per_decade",
                         color_continuous_scale="RdYlGn_r", title="AOD trend per decade (regr_slope)")
            st.plotly_chart(fig, use_container_width=True)
        am = frame("aod_monthly")
        if not am.empty:
            fig = px.imshow(am.pivot(index="city", columns="month", values="aod_mean"), aspect="auto",
                            color_continuous_scale="YlOrBr", height=420, text_auto=".2f",
                            title="Monthly AOD climatology 2001– (pre-monsoon haze peaks in Mar–May)")
            st.plotly_chart(fig, use_container_width=True)

    cl = frame("city_clusters")
    if not cl.empty:
        km = db().cluster_metrics.find_one({}, {"_id": 0}) or {}
        st.subheader("🧩 Pollution regimes — MLlib KMeans")
        st.caption(f"k = {km.get('k')} chosen by silhouette ({km.get('silhouette')}); features: 12 monthly means, "
                   f"24-hour diurnal shape, long-term AOD ({km.get('features')} dimensions, standardised).")
        st.dataframe(cl, hide_index=True, use_container_width=True)


# --------------------------------------------------------------------------- NEWS
with tab_news:
    news = frame("news", sort=[("published_ts", -1)], limit=200)
    if news.empty:
        st.info("No news yet — start `producer-news`.")
    else:
        n1, n2 = st.columns([3, 2])
        with n1:
            st.subheader("Latest headlines")
            for _, r in news.head(25).iterrows():
                tags = " ".join(f"`{t}`" for t in (r.get("themes") or []))
                st.markdown(f"- [{r['title']}]({r['link']}) — *{r['publisher']}*, "
                            f"{pd.to_datetime(r['published_ts']):%Y-%m-%d %H:%M} {tags}")
        with n2:
            th = frame("news_themes")
            if not th.empty:
                st.plotly_chart(px.bar(th, x="count", y="theme", orientation="h", height=300,
                                       title="Themes (rule-based tagging in the stream)"),
                                use_container_width=True)
            terms = frame("news_terms")
            if not terms.empty:
                win = st.radio("Term window", ["7d", "all"], horizontal=True)
                t = terms[terms["window"] == win].sort_values("count")
                st.plotly_chart(px.bar(t, x="count", y="term", orientation="h", height=480,
                                       title="Top terms (Spark ML tokenizer + stop words)"),
                                use_container_width=True)
        topics = frame("news_topics")
        if not topics.empty:
            st.subheader("LDA topics (Spark MLlib)")
            st.dataframe(pd.DataFrame({"topic": topics["topic"],
                                       "top terms": topics["terms"].map(", ".join)}),
                         hide_index=True, use_container_width=True)
        nd = frame("news_daily")
        if not nd.empty:
            fig = go.Figure()
            fig.add_bar(x=nd["date"], y=nd["articles"], name="articles / day", marker_color="#95a5a6")
            fig.add_scatter(x=nd["date"], y=nd["pm25_national"], name="national mean PM2.5", yaxis="y2",
                            line=dict(color="#c0392b"))
            fig.update_layout(height=320, title="Does coverage follow the air?", margin=dict(l=0, r=0, t=40, b=0),
                              yaxis2=dict(overlaying="y", side="right", title="µg/m³"))
            st.plotly_chart(fig, use_container_width=True)


# --------------------------------------------------------------------------- HIVE
with tab_hive:
    st.subheader("🐝 Apache Hive — SQL over the HDFS data lake")
    st.caption("External tables (schema-on-read) over Parquet written by Spark, CSV written by Flume, JSON from "
               "the NASA POWER loader and TSV output of the Hadoop MapReduce job. HiveServer2 runs the query.")
    qs = hive_queries()
    name = st.selectbox("Example query", list(qs) or ["(no queries found)"])
    sql = st.text_area("HiveQL", qs.get(name, "SHOW TABLES"), height=180)
    if st.button("▶ Run on HiveServer2", type="primary"):
        with st.spinner("Hive is executing the query..."):
            try:
                t0 = datetime.now()
                res = hive_frame(sql)
                st.success(f"{len(res):,} rows in {(datetime.now() - t0).total_seconds():.1f} s")
                st.dataframe(res, hide_index=True, use_container_width=True)
            except Exception as e:  # noqa: BLE001
                st.error(f"Hive query failed: {e}")


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
            for topic in ALL_TOPICS:
                md = c.list_topics(topic, timeout=3).topics[topic]
                total = 0
                for part in md.partitions:
                    lo, hi = c.get_watermark_offsets(TopicPartition(topic, part), timeout=3)
                    total += hi - lo
                rows.append({"topic": topic, "partitions": len(md.partitions), "messages": total})
            c.close()
            st.dataframe(pd.DataFrame(rows), hide_index=True)
        except Exception as e:  # noqa: BLE001
            st.error(f"Kafka unreachable: {e}")
        fm = flume_metrics()
        st.markdown("**Apache Flume** (FIRMS spooldir → Kafka + HDFS)")
        if fm is None or fm.empty:
            st.caption("Flume metrics unavailable.")
        else:
            st.dataframe(fm, hide_index=True)
    with c2:
        st.markdown("**HDFS data lake**")
        rows = []
        for label, path in (("lake (Spark Parquet)", LAKE_PATH), ("raw zone (Flume, loaders)", RAW_PATH)):
            sm = storage_summary(path)
            if sm:
                rows.append({"zone": label, "MB": round(sm[0] / 1e6, 1), "files": sm[1], "dirs": sm[2]})
        if rows:
            st.dataframe(pd.DataFrame(rows), hide_index=True)
        else:
            st.caption("Lake not reachable yet.")
        st.caption(f"`{LAKE_PATH}` — partitioned by city/date, network/date, acq_date.")
        st.markdown("**MongoDB** (serving layer)")
        names = sorted(db().list_collection_names())
        st.dataframe(pd.DataFrame([{"collection": n, "documents": db()[n].estimated_document_count()}
                                   for n in names]), hide_index=True, height=300)
    with c3:
        st.markdown("**Spark Structured Streaming** (rows per query, all time)")
        tot = pd.DataFrame(list(db().stream_totals.find()))
        if not tot.empty:
            st.dataframe(tot.rename(columns={"_id": "query", "rows": "rows_in"})
                         .sort_values("rows_in", ascending=False), hide_index=True)
        st.markdown("**YARN** (MapReduce jobs)")
        apps = yarn_apps()
        if apps is None:
            st.caption("YARN not deployed (docker compose --profile mapreduce up -d).")
        else:
            st.dataframe(apps, hide_index=True)
    sp = frame("stream_progress", {"at": {"$gte": datetime.now() - timedelta(hours=2)}})
    if not sp.empty:
        fig = px.scatter(sp, x="at", y="processed_rps", color="query", size="input_rows", height=320,
                         title="Micro-batches of the last 2 h (size = rows, y = processed rows/s)")
        st.plotly_chart(fig, use_container_width=True)
    arch = Path(__file__).resolve().parent.parent / "docs" / "architecture.png"
    if arch.exists():
        st.image(str(arch), caption="NepAirWatch architecture")
