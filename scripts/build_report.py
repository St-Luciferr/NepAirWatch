"""Build docs/NepAirWatch_Report.docx using live results from MongoDB and the HDFS lake.

Run with the stack up (at least mongodb, namenode and datanode):
    MONGO_URI=mongodb://localhost:27017 python scripts/report_figures.py
    MONGO_URI=mongodb://localhost:27017 python scripts/build_report.py
"""
import os
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests  # noqa: E402
from docx import Document  # noqa: E402
from docx.enum.table import WD_TABLE_ALIGNMENT  # noqa: E402
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_BREAK  # noqa: E402
from docx.oxml import OxmlElement  # noqa: E402
from docx.oxml.ns import qn  # noqa: E402
from docx.shared import Cm, Pt, RGBColor  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from common.config import MONGO_DB, MONGO_URI  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
DOCS = ROOT / "docs"
FIG = DOCS / "figures"
SHOT = DOCS / "screenshots"
WEBHDFS = os.getenv("WEBHDFS_URL", "http://localhost:9870")
db = MongoClient(MONGO_URI)[MONGO_DB]

# ------------------------------------------------------------------ live results
ex = list(db.exceedance.find({}, {"_id": 0}).sort("annual_mean", -1))
mm = db.model_metrics.find_one({}, {"_id": 0})
cm = db.calibration_metrics.find_one({}, {"_id": 0}) or {}
km = db.cluster_metrics.find_one({}, {"_id": 0}) or {}
prov = list(db.province_summary.find({}, {"_id": 0}).sort("pm25_avg", -1))
wc = {(r["region"], r["driver"]): r["pearson_r"] for r in db.weather_corr.find()}
cv = list(db.cams_validation.find({"city": {"$ne": None}}, {"_id": 0}).sort("bias", -1))
clusters = list(db.city_clusters.find({}, {"_id": 0}))
topics = list(db.news_topics.find({}, {"_id": 0}).sort("topic", 1))
themes = list(db.news_themes.find({}, {"_id": 0}).sort("count", -1))
aod_trend = list(db.aod_trend.find({}, {"_id": 0}).sort("aod_change_per_decade", -1))
ktm_aod = {r["year"]: r for r in db.aod_yearly.find({"city": "Kathmandu"}, {"_id": 0})}
totals = {r["_id"]: r["rows"] for r in db.stream_totals.find()}
runs = {r["job"]: r for r in db.job_runs.find().sort("at", 1)}
n_air = totals.get("lake_sink", db.readings.estimated_document_count())
n_ground = totals.get("ground_lake", 0)
n_fires = db.fires.estimated_document_count()
n_news = db.news.estimated_document_count()
n_power = sum(r["days"] for r in db.aod_yearly.find({}, {"days": 1}))
n_alerts = db.alerts.estimated_document_count()
n_stations = db.ground_latest.estimated_document_count()
n_doenv = db.ground_latest.count_documents({"network": "doenv"})
n_streamed = n_air + n_ground + totals.get("fires", 0) + totals.get("news", 0)


def hdfs_summary(path):
    try:
        cs = requests.get(f"{WEBHDFS}/webhdfs/v1{path}?op=GETCONTENTSUMMARY", timeout=5).json()["ContentSummary"]
        return cs["fileCount"], cs["length"] / 1e6
    except (requests.RequestException, KeyError, ValueError):
        return None, None


lake_files, lake_mb = hdfs_summary("/nepairwatch/lake")
raw_files, raw_mb = hdfs_summary("/nepairwatch/raw")
ktm = next(r for r in ex if r["city"] == "Kathmandu")
pkr = next(r for r in ex if r["city"] == "Pokhara")
worst = ex[0]
impr = round(100 * (1 - mm["rmse"] / mm["baseline_rmse"]))
cal_impr = round(100 * (1 - cm["rmse"] / cm["raw_rmse"])) if cm else None
valley = [r for r in cv if r["city"] in ("Kathmandu", "Lalitpur", "Bhaktapur")]

doc = Document()
sec = doc.sections[0]
sec.page_width, sec.page_height = Cm(21), Cm(29.7)
for side in ("left_margin", "right_margin"):
    setattr(sec, side, Cm(2.3))
sec.top_margin = sec.bottom_margin = Cm(2.2)

st = doc.styles["Normal"]
st.font.name = "Calibri"
st.font.size = Pt(11)
st.paragraph_format.space_after = Pt(6)
st.paragraph_format.line_spacing = 1.15
for lvl, size, color in ((1, 16, "1F4E79"), (2, 13, "2E75B6"), (3, 11.5, "2E75B6")):
    h = doc.styles[f"Heading {lvl}"]
    h.font.name = "Calibri"
    h.font.size = Pt(size)
    h.font.color.rgb = RGBColor.from_string(color)
    h.paragraph_format.space_before = Pt(14 if lvl == 1 else 10)
    h.paragraph_format.space_after = Pt(6)


def shade(cell, hex_color):
    tcPr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), hex_color)
    tcPr.append(shd)


def para(text, bold_lead=None, style=None, align=None, size=None, italic=False):
    p = doc.add_paragraph(style=style)
    if bold_lead:
        p.add_run(bold_lead).bold = True
    r = p.add_run(text)
    r.italic = italic
    if size:
        r.font.size = Pt(size)
    if align:
        p.alignment = align
    return p


def bullets(items):
    for it in items:
        if isinstance(it, tuple):
            para(it[1], bold_lead=it[0] + " ", style="List Bullet")
        else:
            para(it, style="List Bullet")


def table(header, rows, widths=None, font=9.5):
    t = doc.add_table(rows=1, cols=len(header))
    t.style = "Table Grid"
    t.alignment = WD_TABLE_ALIGNMENT.CENTER
    for i, h in enumerate(header):
        c = t.rows[0].cells[i]
        c.text = ""
        run = c.paragraphs[0].add_run(str(h))
        run.bold = True
        run.font.size = Pt(font)
        run.font.color.rgb = RGBColor(255, 255, 255)
        shade(c, "1F4E79")
    for ri, row in enumerate(rows):
        cells = t.add_row().cells
        for i, v in enumerate(row):
            cells[i].text = ""
            cells[i].paragraphs[0].add_run(str(v)).font.size = Pt(font)
            if ri % 2:
                shade(cells[i], "EAF1F8")
    if widths:
        t.autofit = False
        for i, w in enumerate(widths):
            t.columns[i].width = Cm(w)
        for row in t.rows:
            for i, w in enumerate(widths):
                row.cells[i].width = Cm(w)
    doc.add_paragraph()
    return t


fig_no = [0]


def figure(path, caption, width=16):
    if not Path(path).exists():
        return
    doc.add_picture(str(path), width=Cm(width))
    doc.paragraphs[-1].alignment = WD_ALIGN_PARAGRAPH.CENTER
    fig_no[0] += 1
    return para(f"Figure {fig_no[0]}: {caption}", align=WD_ALIGN_PARAGRAPH.CENTER, size=9, italic=True)


def code(text):
    p = doc.add_paragraph()
    r = p.add_run(text)
    r.font.name = "Consolas"
    r.font.size = Pt(8.5)
    p.paragraph_format.left_indent = Cm(0.5)


def page_break():
    doc.add_paragraph().add_run().add_break(WD_BREAK.PAGE)


# ------------------------------------------------------------------ title page
for _ in range(5):
    doc.add_paragraph()
p = para("NepAirWatch", align=WD_ALIGN_PARAGRAPH.CENTER, size=34)
p.runs[0].bold = True
p.runs[0].font.color.rgb = RGBColor.from_string("1F4E79")
para("A Real-time Big Data Platform for Air-Quality Monitoring and Early Warning in Nepal",
     align=WD_ALIGN_PARAGRAPH.CENTER, size=15)
para("using Apache Kafka, Apache Spark and the Hadoop ecosystem", align=WD_ALIGN_PARAGRAPH.CENTER, size=13,
     italic=True)
for _ in range(3):
    doc.add_paragraph()
para("Big Data — Implementation Task (Project Report)", align=WD_ALIGN_PARAGRAPH.CENTER, size=12)
doc.add_paragraph()
para("Submitted by:", align=WD_ALIGN_PARAGRAPH.CENTER, size=12).runs[0].bold = True
para("[Student Name 1]  —  Roll No. [____]", align=WD_ALIGN_PARAGRAPH.CENTER, size=12)
para("[Student Name 2]  —  Roll No. [____]", align=WD_ALIGN_PARAGRAPH.CENTER, size=12)
doc.add_paragraph()
para("Submitted to: [Instructor Name], [Department / Institute]", align=WD_ALIGN_PARAGRAPH.CENTER, size=12)
para(date.today().strftime("%B %Y"), align=WD_ALIGN_PARAGRAPH.CENTER, size=12)
page_break()

# ------------------------------------------------------------------ abstract
doc.add_heading("Abstract", 1)
para(
    "Air pollution is one of Nepal's most serious public-health problems. The Kathmandu Valley and the Terai "
    "cities regularly record fine-particulate (PM2.5) levels several times higher than the World Health "
    "Organization guideline, yet monitoring is sparse and citizens get no early warning. This project designs "
    "and implements NepAirWatch, an end-to-end big-data platform built as a Lambda architecture on the "
    "Hadoop/Spark ecosystem. It fuses six real sources: the Copernicus CAMS air-quality model and weather "
    "(hourly, 15 cities in all seven provinces), the Department of Environment's measured ground stations and "
    "citizen sensors (1-minute), NASA FIRMS fire hotspots, 25 years of NASA POWER aerosol data, and news "
    "headlines. Apache Kafka and Apache Flume ingest the data. HDFS stores the immutable master dataset. Spark "
    "Structured Streaming forms the speed layer (8 concurrent queries, real-time AQI and alerts). Spark SQL and "
    "MLlib, a Hadoop MapReduce job on YARN and Hive external tables form the batch layer. Cassandra and MongoDB "
    f"serve the results to a 9-tab dashboard with advice in Nepali. On the first start the platform processed "
    f"{n_streamed:,} streamed records. It found that {worst['city']} breached the national 24-hour PM2.5 "
    f"standard on {worst['days_over_nepal_std']} days of the year and Kathmandu on {ktm['days_over_nepal_std']}. "
    f"A Gradient-Boosted Trees model forecasts PM2.5 six hours ahead with {impr}% lower error than a "
    f"persistence baseline (RMSE {mm['rmse']} vs {mm['baseline_rmse']} µg/m³), and a Random Forest calibrates "
    f"the 40 km model against ground stations (RMSE {cm.get('raw_rmse')} → {cm.get('rmse')} µg/m³).")
para("Keywords: big data, Lambda architecture, Apache Kafka, Apache Spark, HDFS, Flume, Hive, Cassandra, "
     "MapReduce, air quality, PM2.5, Nepal, early warning, MLlib", italic=True)

doc.add_heading("Table of Contents", 1)
for line in ["1. Introduction", "2. Problem Definition (Part a)", "3. Data Sources (Part a)",
             "4. Important Features Considered in the Design (Part a)",
             "5. Data Storage Design (Part b)", "6. Choice of Big Data Ecosystem Components (Part b)",
             "7. System Architecture and Block Diagram (Part b)", "8. Methodology and Implementation",
             "9. Results and Analysis", "10. Demonstration", "11. Implementation Guidelines",
             "12. Limitations and Future Work", "13. Conclusion", "References",
             "Appendix A — Message Schemas", "Appendix B — Work Division"]:
    pf = para(line, size=10.5).paragraph_format
    pf.space_after, pf.line_spacing = Pt(0), 1.0
page_break()

# ------------------------------------------------------------------ 1 intro
doc.add_heading("1. Introduction", 1)
para(
    "Nepal's urban air quality gets markedly worse during the dry season (November–April). The Kathmandu "
    "Valley is a bowl-shaped basin. On winter nights a temperature inversion forms and the atmospheric "
    "boundary layer collapses to a few tens of metres, trapping emissions from vehicles, brick kilns, "
    "refuse burning and construction close to the ground. The southern Terai plains are part of the "
    "Indo-Gangetic Plain, one of the most polluted airsheds in the world, and also suffer from crop-residue "
    "burning and spring forest fires. Long-term exposure to PM2.5 causes respiratory and cardiovascular "
    "disease, and air pollution is consistently ranked among the leading risk factors for premature death "
    "in Nepal.")
para(
    "Air-quality data is a natural big-data problem. It arrives continuously, down to one reading per minute "
    "per station (velocity). It mixes modelled and measured values, satellite files and free text (variety). "
    "It accumulates without limit: a single government network adds about 45,000 readings a day, and the "
    "satellite archive covers 25 years (volume). Its usefulness depends on being processed and acted upon "
    "quickly (value). This project shows how an open-source big-data stack can turn these raw sources into "
    "timely, location-specific public-health information for Nepal.")
doc.add_heading("1.1 Objectives", 2)
bullets([
    "Ingest air-quality, weather, ground-sensor, satellite and news data for 15 cities in all 7 provinces, "
    "both historically (back-fill) and live (polling).",
    "Process the streams in real time to compute AQI, classify health risk and raise alerts.",
    "Keep every source in an immutable HDFS data lake and serve results with low latency.",
    "Validate the CAMS model against measured ground stations and calibrate it with machine learning.",
    "Analyse the history at scale: exceedance of standards, seasonal and daily patterns, weather drivers, "
    "fires and 25-year aerosol trends.",
    "Forecast PM2.5 six hours ahead and turn the forecast into recommended actions per audience.",
    "Present everything on an interactive dashboard with advisories in Nepali, started by one command.",
])

# ------------------------------------------------------------------ 2 problem
doc.add_heading("2. Problem Definition (Part a)", 1)
para("Problem statement. ", bold_lead=None).runs[0].bold = True
para(
    "Nepal has no unified, real-time and nationwide system that (i) monitors air quality continuously across "
    "both hill and Terai cities, (ii) combines the sparse measured stations with model and satellite data, "
    "(iii) translates measurements into understandable health advice in the local language, (iv) warns people "
    "before pollution episodes happen, and (v) gives authorities long-term evidence of where and when "
    "air-quality standards are breached.")
doc.add_heading("2.1 Why this is a real problem in Nepal", 2)
bullets([
    ("Severity:", f"In our year of data, every one of the 15 cities exceeded the WHO 24-h PM2.5 guideline on "
                  f"most days. Even comparatively clean Pokhara exceeded it on {pkr['days_over_who']} days. "
                  f"{worst['city']} averaged {worst['annual_mean']} µg/m³ over the year, more than twelve "
                  f"times the WHO annual guideline of 5 µg/m³ (WHO, 2021)."),
    ("Sparse monitoring:", f"Only about {n_doenv} government reference stations report, concentrated in the "
                           "Kathmandu Valley, and several are often offline. The public API keeps only about "
                           "three weeks of history."),
    ("No early warning:", "Schools, hospitals and outdoor workers learn about bad air after exposure has "
                          "already happened. Episodes are predictable from weather (inversions, low wind) "
                          "but no forecast is published at city level."),
    ("Information gap:", "Raw µg/m³ numbers mean little to most citizens. Advice must be categorical "
                         "(Good … Hazardous) and in Nepali."),
    ("Policy evidence:", "Measures such as brick-kiln regulation, crop-burning controls and vehicle emission "
                         "testing need quantitative, long-term, multi-city evidence."),
])
doc.add_heading("2.2 Stakeholders", 2)
table(["Stakeholder", "Need served by NepAirWatch"], [
    ["Citizens / vulnerable groups", "Current AQI, Nepali health advice, 6-hour outlook"],
    ["Schools & hospitals", "Alerts and ranked actions to reschedule outdoor activity, prepare for respiratory cases"],
    ["Department of Environment (DoEnv), MoFE", "Nationwide view, model-vs-station validation, policy evidence"],
    ["Ministry of Health & Population", "Episode detection for public-health advisories"],
    ["Municipalities (e.g. KMC)", "City-level hotspots, fire activity and seasonal timing for interventions"],
    ["Researchers & media", "Long-term archive in open formats (Parquet on HDFS, SQL through Hive)"],
], widths=[5.5, 11])

# ------------------------------------------------------------------ 3 sources
doc.add_heading("3. Data Sources (Part a)", 1)
para(
    "The platform uses six free, public, real data sources of four kinds: modelled, measured, satellite and "
    "text. Each is fetched historically (back-fill on the first start) and live (polling). They are "
    "complementary: the model covers every city every hour but at coarse resolution; the stations are "
    "accurate but sparse; satellites add fires and a 25-year history; news adds the public context.")
table(["Source", "Type / format", "Content", "History · live", "Ingestion path"], [
    ["Open-Meteo Air-Quality + Weather API (Copernicus CAMS)", "Model, JSON",
     "PM2.5, PM10, CO, NO₂, SO₂, O₃, dust, temperature, humidity, wind, rain, boundary-layer height; hourly, "
     "15 cities", "365 days · hourly", "Producer → Kafka nepal.air.raw"],
    ["DoEnv Nepal AQMS (pollution.gov.np)", "Measured, JSON",
     f"{n_doenv} reference stations, PM1 / PM2.5 / PM10 every minute", "~3 weeks (API limit) · 10 min",
     "Producer → Kafka nepal.ground.raw"],
    ["Sensor.Community (SDS011)", "Measured, JSON + CSV",
     "Low-cost citizen PM2.5 / PM10 sensors in Kathmandu and Madhesh, ~5-min", "Daily archives · 10 min",
     "Producer → Kafka nepal.ground.raw"],
    ["NASA FIRMS (VIIRS, MODIS)", "Satellite, CSV files",
     "Active-fire hotspots over Nepal and the Indo-Gangetic Plain", "7 days · 3 h",
     "Spool dir → Flume → Kafka + HDFS"],
    ["NASA POWER (CERES, MERRA-2)", "Satellite / reanalysis, JSON",
     "Daily aerosol optical depth (AOD 550 nm) and climate per city", "2001 → today · daily",
     "Bulk loader → HDFS raw zone"],
    ["Google News RSS", "Unstructured text, XML", "Headlines on air pollution, haze, fires, brick kilns",
     "Latest ~100 per query · 30 min", "Producer → Kafka nepal.news.raw"],
], widths=[3.4, 2.3, 4.6, 2.8, 3.4], font=8.5)
para("Monitored cities (15, all 7 provinces): ", bold_lead=None)
table(["Province", "Cities"], [
    ["Koshi", "Biratnagar, Dharan"], ["Madhesh", "Janakpur, Birgunj"],
    ["Bagmati", "Kathmandu, Lalitpur, Bhaktapur, Hetauda, Bharatpur"], ["Gandaki", "Pokhara"],
    ["Lumbini", "Butwal, Nepalgunj"], ["Karnali", "Surkhet"], ["Sudurpashchim", "Dhangadhi, Mahendranagar"],
], widths=[4, 12.5])
para(
    f"Volume on the first start: {n_air:,} hourly model records (one year × 15 cities), {n_ground:,} "
    f"one-minute ground readings from {n_stations} stations and sensors (growing by about 45,000 a day), "
    f"{n_power:,} NASA POWER city-days (2001 → today), {n_fires:,} fire hotspots and {n_news:,} news "
    f"articles. The HDFS lake holds {lake_files or 0:,} files ({lake_mb or 0:.1f} MB compressed Parquet) and the "
    f"raw zone {raw_mb or 0:.1f} MB. The design scales horizontally through more Kafka partitions, Spark "
    "executors and HDFS DataNodes.")

# ------------------------------------------------------------------ 4 features
doc.add_heading("4. Important Features Considered in the Design (Part a)", 1)
doc.add_heading("4.1 Data features (attributes)", 2)
table(["Feature", "Why it matters"], [
    ["PM2.5 (µg/m³), modelled and measured", "Main health-relevant pollutant; basis of AQI and Nepal NAAQS (40 µg/m³, 24 h)"],
    ["PM1, PM10, CO, NO₂, SO₂, O₃, dust", "Source fingerprinting: traffic (NO₂, CO), kilns/industry (SO₂), dust storms"],
    ["Boundary-layer (mixing) height", "Captures temperature inversions that trap smoke in the Kathmandu Valley"],
    ["Wind speed, precipitation, humidity, temperature", "Dispersion, wet deposition, season and inversion strength"],
    ["Fire hotspots: location, FRP, confidence", "Forest-fire and crop-burning episodes near cities"],
    ["Aerosol optical depth (AOD)", "Satellite measure of haze over 25 years; long-term trend"],
    ["News title, publisher, time", "Public attention and themes (unstructured text)"],
    ["City, province, station, network, lat/lon", "Spatial analysis, partitioning and geo queries"],
    ["Event time (Nepal time, UTC+5:45)", "Event-time windows, seasonality, diurnal cycle"],
    ["Derived: AQI, category, lags, rolling means", "Alerts and machine-learning features"],
], widths=[5.5, 11])
doc.add_heading("4.2 System design features (non-functional requirements)", 2)
bullets([
    ("Scalability:", "Kafka topics are partitioned (key = city or station), Spark parallelises across "
                     "partitions, and HDFS and Cassandra scale out by adding nodes."),
    ("Low latency / real-time:", "Spark Structured Streaming micro-batches (5–60 s triggers) make new "
                                 "readings visible on the dashboard within seconds."),
    ("Fault tolerance & durability:", "Kafka keeps messages for 7 days (replayable), Flume uses a durable "
                                      "file channel, and Spark checkpoints its offsets and state."),
    ("Exactly-once results / idempotency:", "Serving writes are upserts on natural keys and the batch layer "
                                            "de-duplicates the lake, so restarts and replays never "
                                            "double-count."),
    ("Event-time correctness:", "Daily city windows (3 h watermark) and hourly station windows (2 h "
                                "watermark) use observation time aligned to Nepal time."),
    ("Data quality:", "Schema enforcement on parsing, null and range checks (0–1000 µg/m³ for the model, "
                      "0–2000 for sensors), and at least 30 one-minute readings per station-hour for alerts."),
    ("Storage efficiency:", "Columnar, compressed Parquet partitioned by city/date and network/date allows "
                            "partition pruning; Cassandra time-window compaction suits time series."),
    ("Extensibility:", "New sources publish to a topic; new consumers (SMS gateway, mobile app) subscribe to "
                       "the alerts topic."),
    ("Usability & localisation:", "Colour-coded AQI categories and health advice in English and Nepali."),
    ("Reproducibility:", "Infrastructure as code: one docker compose up starts about 20 services."),
])

# ------------------------------------------------------------------ 5 storage
doc.add_heading("5. Data Storage Design (Part b)", 1)
para(
    "NepAirWatch uses polyglot persistence: each storage tier is chosen for its access pattern. HDFS holds the "
    "immutable master dataset of the Lambda architecture; Cassandra and MongoDB hold the serving views.")
table(["Tier", "Technology", "Content", "Access pattern"], [
    ["1. Message log", "Apache Kafka 3.8 (KRaft)",
     "nepal.air.raw, nepal.ground.raw, nepal.fires.raw, nepal.news.raw, nepal.air.alerts",
     "Append-only, 3 partitions, replayable, 7-day retention"],
    ["2. Raw zone", "HDFS 3.4", "raw/firms (Flume CSV), raw/nasa_power (JSON)",
     "Write-once, as received; audit and re-processing"],
    ["3. Lake (refined)", "HDFS + Parquet", "lake/air_quality/city=…/date=…, lake/ground/network=…/date=…, "
     "lake/fires, lake/news", "Large Spark scans with partition pruning"],
    ["4. Curated zone", "HDFS + Parquet", "monthly_city, daily_city, exceedance, MapReduce AOD output",
     "Analytics-ready tables"],
    ["5. SQL layer", "Apache Hive 4 (external tables)", "air_quality, ground, fires, fires_raw, nasa_power, "
     "aod_yearly_mr, exceedance, ground_hourly", "Schema-on-read SQL over the lake"],
    ["6. Time-series store", "Apache Cassandra 5", "ground_readings, ground_hourly",
     "High-rate writes; single-partition reads per station-day"],
    ["7. Serving layer", "MongoDB 7", "latest, readings, alerts, forecasts, fires (2dsphere), analytics …",
     "Indexed ms queries and geo queries for the dashboard"],
    ["8. Model store", "Spark ML pipeline directories", "pm25_gbt, pm25_calibration_rf", "Load for scoring"],
], widths=[2.9, 3.3, 5.6, 4.7], font=8.8)
doc.add_heading("5.1 HDFS and Parquet: the master dataset", 2)
bullets([
    "Every source lands in HDFS, including the DoEnv readings the public API discards after three weeks, so "
    "the lake becomes the long-term archive.",
    "Three zones: raw (as received), lake (cleaned Parquet) and curated (aggregates). Raw data is never "
    "modified, so any view can be recomputed.",
    "Parquet is columnar and compressed: analytical queries read only the needed columns, and the schema is "
    "stored with the data. Spark, Hive and MapReduce all read the same files.",
    "Partitioning by city/date (model) and network/date (sensors) lets queries skip irrelevant files.",
    "In production, at least three DataNodes give 3× block replication; the same paths work on S3.",
])
doc.add_heading("5.2 Cassandra for 1-minute sensor data", 2)
para(
    "Ground readings arrive at about 45,000 per day and are always queried by station and time range. "
    "Cassandra's log-structured write path suits this append-only load. The schema is query-first:")
code("CREATE TABLE ground_readings (station_id text, day date, event_ts timestamp, pm1 double,\n"
     "  pm2_5 double, pm10 double, aqi int, ...,\n"
     "  PRIMARY KEY ((station_id, day), event_ts)) WITH CLUSTERING ORDER BY (event_ts DESC)\n"
     "  AND compaction = {'class': 'TimeWindowCompactionStrategy', 'compaction_window_unit': 'DAYS'};")
para(
    "The partition key (station_id, day) bounds each partition to about 1,440 rows, so \"station S on day D\" "
    "is a single-partition read. The ground_hourly table is written by the streaming window (speed view) and "
    "recomputed by the batch layer under the same key (batch view), with a layer column recording which.")
doc.add_heading("5.3 MongoDB for serving", 2)
bullets([
    "Flexible documents: every analytics output has a different shape and is written without migrations.",
    "Unique compound indexes on natural keys such as (city, event_ts) support idempotent upserts.",
    "A 2dsphere index on fire locations answers \"hotspots within N km of a city\" with $geoWithin.",
    "Millisecond reads for the dashboard, which never scans the lake.",
])
table(["Collection(s)", "Written by", "Purpose"], [
    ["readings, latest, alerts, daily_stats", "Streaming (air)", "Hourly history, current state, alerts, daily windows"],
    ["ground_latest", "Streaming (ground)", "Latest value per station"],
    ["fires, news", "Streaming", "Hotspots with geo index; tagged headlines"],
    ["stream_batches, stream_progress, stream_totals", "Streaming listener", "Pipeline monitoring"],
    ["exceedance, monthly_city, hourly_profile, province_summary, weather_corr", "Batch", "Historical analytics"],
    ["model_metrics, forecasts, test_predictions, advisories", "Batch MLlib", "Early warning and actions"],
    ["cams_validation, calibration_metrics, calibration_sample, station_summary", "Batch fusion", "Model vs ground"],
    ["fire_daily, fire_pm_corr, aod_yearly, aod_trend, city_clusters, news_topics …", "Batch fusion",
     "Fires, aerosols, clusters, text analytics"],
], widths=[6.5, 3.2, 6.8], font=9)

# ------------------------------------------------------------------ 6 components
doc.add_heading("6. Choice of Big Data Ecosystem Components (Part b)", 1)
para(
    "The assignment requires at least two big-data tools. NepAirWatch is built on two core tools, Apache "
    "Kafka and Apache Spark, and uses further Hadoop-ecosystem components where each fits best. The table "
    "compares every choice with its alternatives.")
table(["Need", "Chosen", "Alternatives", "Reason for choice (and when the alternative wins)"], [
    ["Event ingestion", "Apache Kafka", "RabbitMQ, Pulsar, Kinesis",
     "Durable, partitioned, replayable log; many independent consumers; exactly-once with Spark. RabbitMQ "
     "deletes messages once consumed; Kinesis if fully on AWS."],
    ["File ingestion", "Apache Flume", "NiFi, Logstash, Kafka Connect",
     "Built for file/log ingestion into Hadoop: spool directory, replicating fan-out to Kafka and HDFS by "
     "configuration, durable file channel. NiFi offers a visual UI but is much heavier."],
    ["Stream processing", "Spark Structured Streaming", "Flink, Storm, Kafka Streams",
     "Same engine and API as batch and ML; event-time windows, watermarks, checkpoints. Flink has lower "
     "per-event latency, which air quality does not need."],
    ["Batch analytics / ML", "Spark SQL + MLlib", "MapReduce, scikit-learn",
     "In-memory and distributed; trains where the data lives; Pipelines and CrossValidator."],
    ["Classic batch", "Hadoop MapReduce on YARN", "Spark only",
     "Demonstrates the original Hadoop model on a simple one-pass aggregation over 25 years."],
    ["Data lake", "HDFS + Parquet", "S3 / MinIO, ORC",
     "Hadoop-native, replicated, data locality; object stores win in the cloud (same code, other path)."],
    ["SQL on the lake", "Apache Hive (external tables)", "Trino, Impala, Spark Thrift",
     "Standard metastore and schema-on-read; no data copied. Trino is faster for interactive queries."],
    ["Time-series store", "Apache Cassandra", "InfluxDB, TimescaleDB, HBase",
     "Write-optimised, linear scale-out, no single master, time-window compaction. HBase needs ZooKeeper."],
    ["Serving database", "MongoDB", "PostgreSQL/PostGIS, Elasticsearch",
     "Flexible documents and native geospatial queries; PostGIS wins for relational integrity."],
], widths=[2.6, 3.0, 3.0, 7.9], font=8.5)
doc.add_heading("6.1 Apache Kafka in NepAirWatch", 2)
bullets([
    "Runs in KRaft mode (no ZooKeeper) with five topics of three partitions each: nepal.air.raw, "
    "nepal.ground.raw, nepal.fires.raw, nepal.news.raw and nepal.air.alerts.",
    "Message key = city or station, which keeps per-key ordering and spreads load across partitions.",
    "Producers use acks=all, idempotence and lz4 compression.",
    "nepal.air.alerts is written by Spark so that any alert service (SMS, push, sirens) can subscribe "
    "independently: the publish/subscribe benefit of Kafka.",
])
doc.add_heading("6.2 Apache Spark in NepAirWatch", 2)
bullets([
    "Speed layer: one Structured Streaming application runs eight queries over four topics (Section 8.3).",
    "AQI is a native Spark column expression (no Python UDF), so it is optimised by Catalyst and shared by "
    "the speed and batch layers.",
    "Batch layer: batch_analytics.py (descriptive analytics and the GBT forecast) and fusion_analytics.py "
    "(ground-station validation and calibration, fires, aerosols, KMeans, LDA), re-run every 6 hours.",
])
doc.add_heading("6.3 The Hadoop ecosystem in NepAirWatch", 2)
bullets([
    ("HDFS:", "master dataset in raw, lake and curated zones (NameNode + DataNode)."),
    ("Flume:", "FIRMS fire CSVs are dropped in a spool directory; a replicating selector sends each event to "
               "Kafka (durable file channel) and to HDFS (memory channel)."),
    ("YARN + MapReduce:", "a Hadoop Streaming job (Python mapper and reducer) computes yearly AOD statistics "
                          "per city from 25 years of NASA POWER data."),
    ("Hive:", "external tables over Parquet, CSV, JSON and the MapReduce output; queried from the dashboard "
              "through HiveServer2."),
])

# ------------------------------------------------------------------ 7 architecture
doc.add_heading("7. System Architecture and Block Diagram (Part b)", 1)
figure(DOCS / "architecture.png", "NepAirWatch block diagram — Lambda architecture on the Hadoop/Spark ecosystem",
       width=16.5)
doc.add_heading("7.1 Data flow", 2)
for i, s in enumerate([
    "Collection: Python producers poll the Open-Meteo, DoEnv, Sensor.Community and Google News APIs; the "
    "FIRMS fetcher writes CSV files to the Flume spool directory; the NASA POWER loader bulk-loads 25 years "
    "of history.",
    "Ingestion: events are published to Kafka topics; Flume fans fire files out to Kafka and the HDFS raw "
    "zone; bulk history goes straight into HDFS.",
    "Speed layer: Spark Structured Streaming parses and validates each message, computes AQI, and writes "
    "the HDFS lake, Cassandra and MongoDB in micro-batches. Event-time windows produce daily and hourly "
    "views, and alerts are written to MongoDB and back to Kafka.",
    "Batch layer: Spark reads the whole lake, de-duplicates it, computes analytics, validates and calibrates "
    "the model against ground stations, trains and scores the forecaster, clusters cities and models news "
    "topics. Batch views overwrite speed views under the same key. MapReduce and Hive run over the same "
    "HDFS files.",
    "Serving: the Streamlit dashboard reads MongoDB, Cassandra and HiveServer2; Kafka-UI and the Hadoop web "
    "UIs give operators visibility.",
], 1):
    para(s, bold_lead=f"Step {i}. ")
doc.add_heading("7.2 Architecture style: Lambda", 2)
para(
    "The design is a Lambda architecture. HDFS holds the immutable master dataset. The speed layer (Spark "
    "Structured Streaming) gives low-latency, possibly approximate views. The batch layer (Spark, MapReduce, "
    "Hive) periodically recomputes complete, accurate views from the whole dataset and overwrites the speed "
    "views (a layer = speed | batch field records which). Lambda was chosen over Kappa because 25 years of "
    "bulk history should not be pushed through a stream, and the calibration, hourly station views and "
    "models need the complete dataset to be accurate.")

# ------------------------------------------------------------------ 8 methodology & implementation
doc.add_heading("8. Methodology and Implementation", 1)
doc.add_heading("8.1 Methodology", 2)
para("The method climbs an analytics ladder from what happened to what to do:")
table(["Level", "Question", "Technique in NepAirWatch"], [
    ["Descriptive", "What happened?", "Seasonality, diurnal cycle, exceedance days vs Nepal NAAQS and WHO, provinces"],
    ["Diagnostic", "Why?", "Weather correlations; CAMS vs ground stations (bias, RMSE, r); fires vs PM2.5; AOD vs PM2.5"],
    ["Predictive", "What happens next?", "GBT 6-hour PM2.5 forecast; Random Forest calibration of CAMS to measured values"],
    ["Data mining", "Which cities behave alike?", "KMeans on seasonal and diurnal shapes plus AOD, k by silhouette"],
    ["Text analytics", "What is the public discussing?", "Tokenizer → StopWordsRemover → CountVectorizer → LDA; themes"],
    ["Prescriptive", "What should we do?", "Forecast category + worsening trend → ranked actions per audience"],
], widths=[2.8, 4.0, 9.7], font=9)
para(
    "Evaluation principles: time-based train/test splits (never random, to avoid leaking the future), "
    "comparison with an honest baseline, cross-validation for tuning, a metric (silhouette) for choosing k, "
    "validation of inputs against ground truth, and idempotent writes so restarts never double-count.")
doc.add_heading("8.2 Technology stack", 2)
table(["Layer", "Technology / version"], [
    ["Containers", "Docker Compose with profiles (core, mapreduce, hive, full, demo); about 20 services"],
    ["Ingestion", "Apache Kafka 3.8.0 (KRaft), Kafka-UI, Apache Flume 1.11"],
    ["Storage", "Hadoop HDFS 3.4.1, Apache Cassandra 5.0, MongoDB 7.0, Apache Hive 4.0.1"],
    ["Processing", "Apache Spark 3.5.3 (PySpark: Structured Streaming, SQL, MLlib), Hadoop MapReduce on YARN"],
    ["Language & libraries", "Python 3.11, Java 17, confluent-kafka, pymongo, cassandra-driver, pandas, pyarrow"],
    ["Visualisation", "Streamlit, Plotly, Matplotlib"],
], widths=[4, 12.5])
doc.add_heading("8.3 Source code modules", 2)
table(["File", "Responsibility"], [
    ["docker-compose.yml, Dockerfile, docker/", "All services, images, init jobs and entry scripts"],
    ["common/config.py, aqi.py, geo.py", "Cities and thresholds; AQI, categories, Nepali advice, actions; geo helpers"],
    ["producer/aq_producer.py", "CAMS + weather producer: back-fill, live and replay modes"],
    ["producer/ground_producer.py", "DoEnv and Sensor.Community 1-minute readings"],
    ["producer/firms_fetcher.py, flume/flume.conf", "FIRMS CSVs into the spool dir; Flume agent configuration"],
    ["producer/power_loader.py, news_producer.py", "NASA POWER bulk load into HDFS; Google News RSS producer"],
    ["spark_jobs/stream_processor.py", "Speed layer: 8 streaming queries"],
    ["spark_jobs/batch_analytics.py", "Descriptive analytics, GBT forecast, prescriptive advisories"],
    ["spark_jobs/fusion_analytics.py", "Validation, calibration, fires, aerosols, KMeans, LDA"],
    ["mapreduce/, hive/, cassandra/", "MapReduce job; Hive DDL and queries; Cassandra schema"],
    ["dashboard/app.py, sources.py", "9-tab Streamlit dashboard and its data sources"],
], widths=[5.8, 10.7])
doc.add_heading("8.4 AQI computation", 2)
para(
    "The AQI follows the US EPA (2024) PM2.5 breakpoints, the scale used by most public AQI displays in Nepal. "
    "For concentration C in the breakpoint interval [C_lo, C_hi] with index range [I_lo, I_hi]:")
para("AQI = (I_hi − I_lo) / (C_hi − C_lo) × (C − C_lo) + I_lo", align=WD_ALIGN_PARAGRAPH.CENTER).runs[0].bold = True
table(["PM2.5 (µg/m³)", "AQI", "Category", "Advice (Nepali)"], [
    ["0 – 9.0", "0 – 50", "Good", "हावा स्वच्छ छ।"],
    ["9.1 – 35.4", "51 – 100", "Moderate", "संवेदनशील व्यक्तिले लामो समय बाहिर कडा परिश्रम नगर्नुहोस्।"],
    ["35.5 – 55.4", "101 – 150", "Unhealthy for Sensitive Groups", "बालबालिका, वृद्ध र दम/मुटुरोगीले बाहिरी गतिविधि घटाउनुहोस्।"],
    ["55.5 – 125.4", "151 – 200", "Unhealthy  → ALERT (WARNING)", "सबैले बाहिरी गतिविधि घटाउनुहोस्; N95 मास्क लगाउनुहोस्।"],
    ["125.5 – 225.4", "201 – 300", "Very Unhealthy → ALERT (CRITICAL)", "बाहिर ननिस्कनुहोस्।"],
    ["> 225.5", "301 – 500", "Hazardous → ALERT (CRITICAL)", "स्वास्थ्य आपतकाल: घरभित्रै बस्नुहोस्।"],
], widths=[2.8, 2.2, 4.7, 6.8], font=9)
doc.add_heading("8.5 Speed layer: streaming queries", 2)
table(["Topic", "Query", "Output"], [
    ["nepal.air.raw", "lake_sink", "HDFS Parquet partitioned by city/date (every 10 s)"],
    ["nepal.air.raw", "serving_sink", "MongoDB upserts, latest per city, alerts for AQI ≥ 151 → MongoDB + Kafka"],
    ["nepal.air.raw", "daily_window", "1-day event-time window aligned to Nepal midnight, 3 h watermark"],
    ["nepal.ground.raw", "ground_lake", "HDFS Parquet partitioned by network/date"],
    ["nepal.ground.raw", "ground_serving", "Cassandra ground_readings + MongoDB ground_latest"],
    ["nepal.ground.raw", "ground_hourly", "1-hour window, 2 h watermark → Cassandra; alerts if ≥ 30 readings"],
    ["nepal.fires.raw", "fires", "MongoDB with 2dsphere index; nearest city and distance"],
    ["nepal.news.raw", "news", "Rule-based theme tagging → MongoDB"],
], widths=[3.2, 3.2, 10.1], font=9)
code("parsed = (spark.readStream.format('kafka').option('subscribe', 'nepal.air.raw')\n"
     "   .option('maxOffsetsPerTrigger', ...).load()\n"
     "   .select(from_json(col('value').cast('string'), RAW_SCHEMA).alias('m')).select('m.*')\n"
     "   .filter(col('city').isNotNull() & col('event_ts').isNotNull())\n"
     "   .filter(col('pm2_5').between(0, 1000))\n"
     "   .withColumn('aqi', aqi_expr(col('pm2_5'))).withColumn('category', category_expr(col('aqi'))))\n\n"
     "(parsed.withWatermark('event_ts', '3 hours')\n"
     "   .groupBy(window('event_ts', '1 day', '1 day', '18 hours 15 minutes'), 'city')\n"
     "   .agg(avg('pm2_5'), max('pm2_5'), max('aqi'), count('*'))\n"
     "   .writeStream.outputMode('update').foreachBatch(write_daily))")
para("The window offset of 18 h 15 min aligns the 1-day UTC windows to midnight in Nepal (UTC+5:45).",
     italic=True, size=9.5)
doc.add_heading("8.6 Machine-learning models (Spark MLlib)", 2)
table(["Model", "Training data and features", "Evaluation", "Purpose"], [
    ["GBTRegressor (40 trees, depth 6)",
     "One year of hourly CAMS data for 15 cities, read from the HDFS lake and de-duplicated. Label: PM2.5 "
     "6 h later. Features: pollutants, weather incl. boundary-layer height, hour, month, PM2.5 lags 1/3/6/24 h, "
     "24 h rolling mean, one-hot city",
     "Time-based 80/20 split; persistence baseline; RMSE, MAE, R²; feature importance",
     "6-hour early warning; drives the recommended actions"],
    ["RandomForestRegressor + CrossValidator",
     "Measured hourly PM2.5 at stations (≥ 30 readings) joined hour by hour with CAMS. Features: CAMS PM2.5 "
     "and PM10, weather, hour, city",
     "Time-based split; 3-fold CV over numTrees {30, 60} × maxDepth {5, 8}; baseline = raw CAMS",
     "Correct the 40 km model towards ground truth for every city"],
    ["KMeans", "Per city: 12 monthly means, 24-hour diurnal shape, long-term AOD; standardised",
     "k from 2–4 chosen by silhouette", "Group cities into pollution regimes for regional policy"],
    ["LDA (k = 4)", "Headlines → Tokenizer → StopWordsRemover → CountVectorizer",
     "Top words per topic; skipped below 30 articles", "Discover what the public discussion is about"],
], widths=[3.0, 6.0, 4.0, 3.5], font=8.5)
para(
    "Why CAMS for the forecaster rather than measured data: the DoEnv API keeps only about three weeks, "
    "stations cover only part of the country and have outages, and a forecaster needs a full year to learn "
    "seasonality. CAMS assimilates satellite observations and is complete for every city and hour. Its "
    "limitation (a 40 km average) is addressed by the validation and calibration model, and the HDFS archive "
    "of measured data will allow retraining on ground truth as it grows.")
page_break()

# ------------------------------------------------------------------ 9 results
doc.add_heading("9. Results and Analysis", 1)
doc.add_heading("9.1 Exceedance of air-quality standards", 2)
figure(FIG / "exceedance.png", "Days in the past year on which the 24-h mean PM2.5 exceeded each standard")
table(["City", "Province", "Annual mean PM2.5", "Days > Nepal (40)", "Days > WHO (15)", "Worst day"],
      [[r["city"], r["province"], r["annual_mean"], f"{r['days_over_nepal_std']} ({r['pct_days_over_nepal_std']}%)",
        r["days_over_who"], r["worst_day_pm25"]] for r in ex],
      widths=[2.8, 2.8, 2.6, 3.2, 2.5, 2.6], font=8.8)
para(
    f"The Terai cities of Koshi and Madhesh provinces are the most polluted. {worst['city']} averaged "
    f"{worst['annual_mean']} µg/m³ and breached the national standard on {worst['days_over_nepal_std']} days "
    f"({worst['pct_days_over_nepal_std']}%), consistent with the Indo-Gangetic Plain airshed. Kathmandu "
    f"breached it on {ktm['days_over_nepal_std']} days. Pokhara is the cleanest city ({pkr['annual_mean']} "
    f"µg/m³), yet it still exceeded the WHO guideline on {pkr['days_over_who']} days.")
doc.add_heading("9.2 Seasonal and diurnal patterns", 2)
figure(FIG / "seasonality.png", "Monthly mean PM2.5 per city (Spark batch aggregation)")
para(
    "PM2.5 peaks sharply in the dry season (December to April). The monsoon (June to September) brings the "
    "cleanest air through wet deposition and stronger mixing. Interventions such as kiln operating "
    "restrictions and school schedule changes should therefore target the winter and spring months.")
figure(FIG / "diurnal.png", "Average PM2.5 by hour of the day", width=13)
para(
    "Concentrations rise in the evening and stay high through the night and early morning, when the boundary "
    "layer is shallow. They drop around midday, when solar heating deepens the mixing layer.")
doc.add_heading("9.3 Weather drivers", 2)
figure(FIG / "weather_corr.png", "Pearson correlation of PM2.5 with weather variables", width=13)
bl = wc.get(("Kathmandu Valley", "boundary_layer_height"))
pr = wc.get(("All Nepal", "precipitation"))
para(
    f"PM2.5 is negatively correlated with boundary-layer height (r = {bl} in the Kathmandu Valley): a shallow "
    f"mixing layer traps pollution, which confirms the inversion mechanism. Precipitation is also negatively "
    f"associated (r = {pr} nationally) through wet scavenging. These relationships justify using weather as "
    "inputs for early warning.")
figure(FIG / "kathmandu_daily.png", "Kathmandu daily mean PM2.5 from the streaming event-time window")
doc.add_heading("9.4 Forecasting performance", 2)
table(["Model", "RMSE (µg/m³)", "MAE (µg/m³)", "R²"], [
    ["Persistence baseline", mm["baseline_rmse"], mm["baseline_mae"], mm["baseline_r2"]],
    [f"Spark MLlib GBT (t+{mm['horizon_hours']} h)", mm["rmse"], mm["mae"], mm["r2"]],
], widths=[6, 3.5, 3.5, 3.5])
para(
    f"The GBT model reduces the forecast error by {impr}% compared with persistence. It was trained on "
    f"{mm['train_rows']:,} rows and evaluated on {mm['test_rows']:,} later, unseen rows. The most important "
    f"features are {', '.join(f['feature'] for f in mm['feature_importance'][:4])}. A 6-hour lead time gives "
    "schools and hospitals time to act before an episode peaks, and each forecast category is mapped to "
    "ranked actions for schools, hospitals, the municipality, the public and alert services.")
figure(FIG / "forecast.png", "Actual vs forecast PM2.5 for Kathmandu during the hold-out period")
figure(FIG / "feature_importance.png", "Top-10 feature importances of the GBT model", width=10)
doc.add_heading("9.5 Model vs ground truth, and calibration", 2)
figure(FIG / "cams_bias.png", "Bias of the CAMS model against DoEnv ground stations", width=14)
if valley:
    lo, hi = min(r["bias"] for r in valley), max(r["bias"] for r in valley)
    para(
        f"Matching CAMS with measured station-hours shows that the model mostly over-estimates PM2.5 at the "
        f"stations during the observed period: by up to {hi} µg/m³ in the Kathmandu Valley (bias {lo:+} to "
        f"{hi:+} µg/m³) and more in parts of the Terai, with correlation of only about 0.5. A 40 km grid cannot resolve local conditions, which "
        f"is why the platform validates the model instead of trusting it.")
if cm:
    figure(FIG / "calibration.png", "Raw CAMS vs Random-Forest-calibrated PM2.5 on hold-out station-hours", width=11)
    para(
        f"The Random Forest calibration, tuned by 3-fold cross-validation over {cm['grid_size']} parameter sets "
        f"(best: {cm['best_num_trees']} trees, depth {cm['best_max_depth']}), reduces the error against measured "
        f"PM2.5 from {cm['raw_rmse']} to {cm['rmse']} µg/m³ RMSE ({cal_impr}% lower) on {cm['test_rows']:,} "
        f"hold-out hours. Its R² is still low ({cm['r2']}): with only about three weeks of station data it "
        "mainly removes the systematic bias and captures little hour-to-hour variation. Accuracy will improve "
        "as the HDFS archive of measured data grows.")
doc.add_heading("9.6 Fires, aerosols and pollution regimes", 2)
fd = db.fire_daily.find_one(sort=[("hotspots_region", -1)], projection={"_id": 0})
para(
    f"Flume ingested {n_fires:,} fire hotspots over seven days; the busiest day had {fd['hotspots_region']} "
    f"hotspots in the region and {fd['hotspots_near_cities']} within 100 km of a monitored city. October is "
    "outside the main fire season, so fire–PM2.5 correlations over this short window are not yet meaningful; "
    "they become useful in the spring fire season.")
figure(FIG / "aod_trend.png", "Annual mean aerosol optical depth, 2001–2025 (NASA POWER)")
if ktm_aod and aod_trend:
    recent = [ktm_aod[y]["aod_mean"] for y in range(2015, 2025) if y in ktm_aod]
    top = aod_trend[0]
    ktm_t = next(r for r in aod_trend if r["city"] == "Kathmandu")
    para(
        f"Twenty-five years of satellite data show rising haze everywhere. Kathmandu's annual mean AOD rose from "
        f"{ktm_aod[2001]['aod_mean']} in 2001 to about {min(recent):.2f}–{max(recent):.2f} in 2015–2024 "
        f"(+{ktm_t['aod_change_per_decade']:.3f} per decade), and hazy days roughly doubled. The fastest increase "
        f"is in the eastern Terai ({top['city']}: +{top['aod_change_per_decade']:.3f} per decade, R² "
        f"{top['r2']}).")
if clusters and km:
    groups = {}
    for r in clusters:
        groups.setdefault(r["regime"], []).append(r["city"])
    para(
        f"KMeans chose k = {km['k']} (silhouette {km['silhouette']}) and separated the cities into pollution "
        "regimes: " + "; ".join(f"{k}: {', '.join(v)}" for k, v in groups.items()) +
        ". Each regime needs a different policy, such as crop-burning and cross-border measures in the Terai "
        "and inversion-day traffic and kiln measures in the Valley.")
doc.add_heading("9.7 News and public attention", 2)
if themes:
    para(f"{n_news} headlines were streamed and tagged. The most frequent themes were " +
         ", ".join(f"{t['theme']} ({t['count']})" for t in themes[:4]) + ".")
if topics:
    table(["LDA topic", "Top terms"], [[t["topic"], ", ".join(t["terms"][:8])] for t in topics], widths=[2.5, 14])
doc.add_heading("9.8 Pipeline performance", 2)
bullets([
    f"{n_streamed:,} messages from four Kafka topics were processed by eight streaming queries on the first "
    f"start ({n_air:,} model records, {n_ground:,} ground readings).",
    f"{n_alerts:,} alert events were generated and published to the nepal.air.alerts topic.",
    f"HDFS lake: {lake_files or 0:,} files, {lake_mb or 0:.1f} MB of Parquet; raw zone {raw_mb or 0:.1f} MB.",
    f"Batch layer on a single laptop (Spark local mode): batch_analytics "
    f"{runs.get('batch_analytics', {}).get('seconds', '—')} s, fusion_analytics "
    f"{runs.get('fusion_analytics', {}).get('seconds', '—')} s.",
    "In live mode new readings reach the dashboard within one trigger interval (5–60 s).",
])
page_break()

# ------------------------------------------------------------------ 10 demo
doc.add_heading("10. Demonstration", 1)
para("The dashboard has nine tabs. Screenshots from the running system:")
for f, cap in [("1_live.png", "Live monitor — map, AQI per city with Nepali advice, alerts, stream throughput"),
               ("2_ground.png", "Ground stations — measured data, Cassandra time series, validation and calibration"),
               ("3_city.png", "City explorer — PM2.5 vs standards and boundary-layer height"),
               ("4_analytics.png", "Analytics — exceedance days, seasonality, diurnal cycle, weather drivers"),
               ("5_fires.png", "Fires & aerosols — FIRMS hotspots with a geospatial query, 25-year AOD, KMeans"),
               ("6_early.png", "Early warning — model metrics, 6-hour forecast and recommended actions"),
               ("7_news.png", "News — headlines, themes, top terms and LDA topics"),
               ("8_hive.png", "Hive SQL — queries over the HDFS lake through HiveServer2"),
               ("9_pipeline.png", "Pipeline health — Kafka, Flume, HDFS, MongoDB, Spark queries and YARN")]:
    figure(SHOT / f, cap, width=15.5)
doc.add_heading("10.1 Demo plan", 2)
for i, s in enumerate([
    "Show that one docker compose up started about 20 services (docker compose ps).",
    "Open Kafka-UI and show the five topics, partitions and a sample message.",
    "Show Flume's logs and the HDFS raw and lake zones in the NameNode UI.",
    "Start the replay (docker compose run --rm replay) and watch the Live monitor and alerts update.",
    "Open Ground stations: 1-minute Cassandra data, speed vs batch hourly views, validation and calibration.",
    "Open Early warning: forecast accuracy against the baseline and the recommended actions.",
    "Open Fires & aerosols: change the radius of the $geoWithin query; show the 25-year trend.",
    "Show the MapReduce job in the YARN UI and run a Hive query from the dashboard.",
    "Finish on Pipeline health and the architecture diagram.",
], 1):
    para(s, bold_lead=f"{i}. ")

# ------------------------------------------------------------------ 11 guidelines
doc.add_heading("11. Implementation Guidelines", 1)
para("Prerequisites: Docker with Compose v2 and internet access. RAM: about 8 GB for the core stack and "
     "11–12 GB with --profile full. Everything, including Python, Java, Spark and Hadoop, runs in containers.")
table(["Step", "Command", "What it does"], [
    ["Start (core)", "docker compose up -d --build",
     "Builds the images and starts Kafka, Flume, HDFS, Cassandra, MongoDB, the producers and back-fills, "
     "Spark streaming, Spark batch (every 6 h) and the dashboard."],
    ["Start (full)", "docker compose --profile full up -d --build", "Adds YARN, the MapReduce job and Hive."],
    ["Dashboard", "http://localhost:8501", "Kafka-UI :8080, HDFS :9870, Spark :4040, YARN :8088, Hive :10002."],
    ["Status / logs", "docker compose ps\ndocker compose logs -f spark-streaming", "Service health and micro-batches."],
    ["Demo replay", "docker compose run --rm replay", "Re-streams the last 2 days, 1 hour every 2 s."],
    ["Stop", "docker compose stop", "Stops all services and keeps the data."],
    ["Reset", "docker compose down -v", "Deletes all data; the next start back-fills again."],
], widths=[2.6, 6.4, 7.5], font=9)
para("Configuration is through a .env file (see .env.example): history lengths, polling intervals and an "
     "optional FIRMS_MAP_KEY. The first build downloads Hadoop and Flume (~1 GB) from fast Apache mirrors and "
     "takes about 2–5 minutes. The full source code and README accompany this report.")

# ------------------------------------------------------------------ 12 limitations
doc.add_heading("12. Limitations and Future Work", 1)
bullets([
    ("Model resolution:", "CAMS is a ~40 km model, so Kathmandu, Lalitpur and Hetauda share one grid cell. "
                          "It is validated and calibrated against ground stations, but local hotspots remain "
                          "smoothed."),
    ("Short measured history:", "The DoEnv API keeps only about three weeks, which limits the calibration "
                                "model. The HDFS archive grows daily; the forecaster can later be retrained on "
                                "measured data, and OpenAQ history could be added."),
    ("Single-node deployment:", "The demo runs on one machine. Production would use a 3-broker Kafka cluster "
                                "(replication factor 3), at least three HDFS DataNodes and Cassandra nodes, and "
                                "Spark on YARN or Kubernetes."),
    ("Security:", "No authentication or encryption yet; production needs Kerberos, TLS and Kafka ACLs."),
    ("Alert delivery:", "Alerts are published to Kafka and the dashboard; an SMS/Viber/push consumer is planned."),
    ("Small files:", "Frequent micro-batches create many small Parquet files; a compaction job or Delta "
                     "Lake/Iceberg tables would fix this."),
    ("Richer forecasts:", "Fire counts and AOD (already ingested), traffic and kiln calendars as features, "
                          "sequence models (LSTM) and longer horizons."),
])

# ------------------------------------------------------------------ 13 conclusion
doc.add_heading("13. Conclusion", 1)
para(
    "NepAirWatch shows that an open-source big-data stack can address a pressing real-world problem in Nepal. "
    "Kafka and Flume provide scalable, fault-tolerant, replayable ingestion of streams and files. HDFS keeps "
    "an immutable master dataset of six heterogeneous sources. Spark provides real-time AQI and alerting, "
    "event-time windows, large-scale analytics and distributed machine learning, alongside MapReduce on YARN "
    "and SQL through Hive. Cassandra and MongoDB serve time-series and document views with low latency. The "
    f"analysis shows that PM2.5 standards are breached on most days in the Terai and on {ktm['days_over_nepal_std']} "
    f"days a year in Kathmandu, that haze has risen over 25 years, and that episodes can be forecast six hours "
    f"ahead with {impr}% lower error than persistence. With SMS delivery and a growing archive of measured "
    "data, the platform could serve as a national air-quality early-warning system.")

# ------------------------------------------------------------------ references
doc.add_heading("References", 1)
for r in [
    "World Health Organization (2021). WHO Global Air Quality Guidelines. Geneva: WHO.",
    "Government of Nepal (2012). National Ambient Air Quality Standards for Nepal, Nepal Gazette.",
    "Department of Environment, Nepal. Air Quality Monitoring — https://pollution.gov.np",
    "U.S. EPA (2024). Technical Assistance Document for the Reporting of Daily Air Quality — the AQI.",
    "Copernicus Atmosphere Monitoring Service (CAMS). Global atmospheric composition forecasts.",
    "Open-Meteo. Air Quality API and Weather API documentation — https://open-meteo.com",
    "NASA FIRMS. Fire Information for Resource Management System — https://firms.modaps.eosdis.nasa.gov",
    "NASA POWER. Prediction Of Worldwide Energy Resources — https://power.larc.nasa.gov",
    "Sensor.Community. Open environmental data — https://sensor.community",
    "Apache Software Foundation. Kafka 3.8, Spark 3.5, Hadoop 3.4, Flume 1.11, Hive 4 and Cassandra 5 documentation.",
    "Marz, N. & Warren, J. (2015). Big Data: Principles and best practices of scalable realtime data systems. Manning.",
    "Kleppmann, M. (2017). Designing Data-Intensive Applications. O'Reilly Media.",
    "Health Effects Institute (2024). State of Global Air 2024.",
]:
    para(r, style="List Number")

# ------------------------------------------------------------------ appendices
doc.add_heading("Appendix A — Message Schemas", 1)
para("nepal.air.raw (CAMS model + weather, one message per city-hour):", italic=True)
code('{ "city": "Kathmandu", "province": "Bagmati", "lat": 27.7172, "lon": 85.324,\n'
     '  "event_time": "2026-01-12T08:00:00+05:45",\n'
     '  "pm2_5": 112.4, "pm10": 140.2, "co": 1450.0, "no2": 38.1, "so2": 12.0, "o3": 20.3, "dust": 3.0,\n'
     '  "temperature": 6.2, "humidity": 81.0, "wind_speed": 2.1, "precipitation": 0.0,\n'
     '  "boundary_layer_height": 35.0, "source": "open-meteo/live", "ingest_time": "2026-01-12T02:20:11+00:00" }')
para("nepal.ground.raw (measured, one message per station-minute):", italic=True)
code('{ "network": "doenv", "station_id": "doenv-5", "station": "Pulchowk",\n'
     '  "lat": 27.6826, "lon": 85.3188, "city": "Lalitpur", "city_km": 4.2,\n'
     '  "event_time": "2026-10-12T15:53:00+05:45", "pm1": 20.2, "pm2_5": 25.5, "pm10": 30.2,\n'
     '  "source": "doenv/live", "ingest_time": "2026-10-12T10:15:02+00:00" }')
doc.add_heading("Appendix B — Work Division", 1)
table(["Member", "Responsibilities"], [
    ["[Student Name 1]", "Problem study, data sources, producers, Kafka and Flume ingestion, HDFS, Cassandra and MongoDB design, dashboard"],
    ["[Student Name 2]", "Spark streaming and batch layers, MLlib models, MapReduce and Hive, evaluation"],
    ["Both", "Architecture design, testing, report and presentation"],
], widths=[4, 12.5])

# footer page numbers
for s in doc.sections:
    fp = s.footer.paragraphs[0]
    fp.alignment = WD_ALIGN_PARAGRAPH.CENTER
    run = fp.add_run()
    for tag, text in (("begin", None), (None, "PAGE"), ("end", None)):
        if tag:
            el = OxmlElement("w:fldChar")
            el.set(qn("w:fldCharType"), tag)
        else:
            el = OxmlElement("w:instrText")
            el.set(qn("xml:space"), "preserve")
            el.text = text
        run._r.append(el)

out = DOCS / "NepAirWatch_Report.docx"
doc.save(out)
print("saved", out)
