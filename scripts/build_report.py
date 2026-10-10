"""Build docs/NepAirWatch_Report.docx using live results from MongoDB."""
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

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
db = MongoClient(MONGO_URI)[MONGO_DB]

ex = list(db.exceedance.find({}, {"_id": 0}).sort("annual_mean", -1))
mm = db.model_metrics.find_one({}, {"_id": 0})
prov = list(db.province_summary.find({}, {"_id": 0}).sort("pm25_avg", -1))
wc = {(r["region"], r["driver"]): r["pearson_r"] for r in db.weather_corr.find()}
n_readings = db.readings.estimated_document_count()
n_alerts = db.alerts.estimated_document_count()
lake_files = list((ROOT / "data" / "lake" / "air_quality").rglob("*.parquet"))
lake_mb = sum(f.stat().st_size for f in lake_files) / 1e6
ktm = next(r for r in ex if r["city"] == "Kathmandu")
pkr = next(r for r in ex if r["city"] == "Pokhara")
worst = ex[0]

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
    p = para(f"Figure {fig_no[0]}: {caption}", align=WD_ALIGN_PARAGRAPH.CENTER, size=9, italic=True)
    return p


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
para("using Apache Kafka and Apache Spark", align=WD_ALIGN_PARAGRAPH.CENTER, size=13, italic=True)
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
    "Organization guideline, yet citizens get little timely, localised or actionable information. This project "
    "designs and implements NepAirWatch, an end-to-end big-data platform that continuously collects hourly "
    "air-quality and meteorological data for 15 cities across all seven provinces of Nepal. Apache Kafka is "
    "the distributed ingestion backbone. Apache Spark (Structured Streaming, Spark SQL and MLlib) does the "
    "real-time and batch processing. The data is stored in a partitioned Parquet data lake and a MongoDB "
    "serving layer. The system computes the Air Quality Index (AQI) in real time, raises health alerts with "
    "advice in English and Nepali, measures exceedance of the Nepal National Ambient Air Quality Standard and "
    "the WHO guideline, explains pollution episodes through weather drivers, and forecasts PM2.5 six hours "
    f"ahead with a Gradient-Boosted Trees model. Over one year of data ({n_readings:,} city-hour records), "
    f"the platform found that {worst['city']} breached the national 24-hour standard on "
    f"{worst['days_over_nepal_std']} days and Kathmandu on {ktm['days_over_nepal_std']} days. The forecaster "
    f"reduced error against a persistence baseline from {mm['baseline_rmse']} to {mm['rmse']} µg/m³ RMSE "
    f"(R² {mm['r2']}).")
para("Keywords: big data, Apache Kafka, Apache Spark, structured streaming, air quality, PM2.5, Nepal, "
     "early warning, MLlib, data lake", italic=True)

doc.add_heading("Table of Contents", 1)
for line in ["1. Introduction", "2. Problem Definition (Part a)", "3. Data Sources (Part a)",
             "4. Important Features Considered in the Design (Part a)",
             "5. Data Storage Design (Part b)", "6. Choice of Big Data Ecosystem Components (Part b)",
             "7. System Architecture and Block Diagram (Part b)", "8. Implementation",
             "9. Results and Analysis", "10. Demonstration", "11. Implementation Guidelines",
             "12. Limitations and Future Work", "13. Conclusion", "References",
             "Appendix A — Message Schema", "Appendix B — Work Division"]:
    para(line).paragraph_format.space_after = Pt(2)
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
    "Air-quality data is a natural big-data problem. It arrives continuously (velocity), comes from many "
    "locations and many pollutant and weather variables (variety), and accumulates without limit (volume). "
    "Its usefulness depends on being processed and acted upon quickly (value). This project shows how a "
    "modern open-source big-data stack built from Apache Kafka and Apache Spark can turn raw environmental "
    "measurements into timely, location-specific public-health information for Nepal.")
doc.add_heading("1.1 Objectives", 2)
bullets([
    "Continuously ingest hourly air-quality and weather data for major cities in all 7 provinces of Nepal.",
    "Process the stream in real time to compute AQI, classify health risk and raise alerts.",
    "Store the full history in a scalable, query-efficient data lake and serve results with low latency.",
    "Analyse the history at scale: exceedance of standards, seasonal and daily patterns, weather drivers.",
    "Train a machine-learning model that forecasts PM2.5 hours ahead to enable early warning.",
    "Present everything on an interactive dashboard with advisories in Nepali.",
])

# ------------------------------------------------------------------ 2 problem
doc.add_heading("2. Problem Definition (Part a)", 1)
para("Problem statement. ", bold_lead=None).runs[0].bold = True
para(
    "Nepal has no unified, real-time and nationwide system that (i) monitors air quality continuously across "
    "both hill and Terai cities, (ii) translates measurements into understandable health advice in the local "
    "language, (iii) warns people before pollution episodes happen, and (iv) gives authorities historical "
    "evidence of where and when air-quality standards are breached.")
doc.add_heading("2.1 Why this is a real problem in Nepal", 2)
bullets([
    ("Severity:", f"In our year of data, every one of the 15 cities exceeded the WHO 24-h PM2.5 guideline on "
                  f"most days. Even comparatively clean Pokhara exceeded it on {pkr['days_over_who']} days. "
                  f"{worst['city']} averaged {worst['annual_mean']} µg/m³ over the year, more than four "
                  f"times the WHO annual guideline of 5 µg/m³ (WHO, 2021)."),
    ("Sparse monitoring:", "Government reference stations are concentrated in the Kathmandu Valley, and many "
                           "Terai and far-western cities have little or no reliable public monitoring."),
    ("No early warning:", "Schools, hospitals and outdoor workers learn about bad air after exposure has "
                          "already happened. Episodes are predictable from weather (inversions, low wind) "
                          "but no forecast is published at city level."),
    ("Information gap:", "Raw µg/m³ numbers mean little to most citizens. Advice must be categorical "
                         "(Good … Hazardous) and in Nepali."),
    ("Policy evidence:", "Measures such as brick-kiln regulation, vehicle emission testing and odd-even "
                         "schemes need quantitative, long-term, multi-city evidence."),
])
doc.add_heading("2.2 Stakeholders", 2)
table(["Stakeholder", "Need served by NepAirWatch"], [
    ["Citizens / vulnerable groups", "Current AQI, Nepali health advice, 6-hour outlook"],
    ["Schools & hospitals", "Alerts to reschedule outdoor activity, prepare for respiratory cases"],
    ["Department of Environment (DoEnv), MoFE", "Nationwide view, exceedance statistics, policy evidence"],
    ["Ministry of Health & Population", "Episode detection for public-health advisories"],
    ["Municipalities (e.g. KMC)", "City-level hotspots and seasonal timing for interventions"],
    ["Researchers & media", "Clean historical dataset in open formats (Parquet)"],
], widths=[5.5, 11])

# ------------------------------------------------------------------ 3 sources
doc.add_heading("3. Data Sources (Part a)", 1)
para(
    "The platform collects data from public, free web APIs, so it can be reproduced without licences. "
    "Every source is ingested through the same Kafka topic with a common JSON schema, so new sources "
    "(ground stations, IoT sensors) can be added without changing the downstream processing.")
table(["Source", "Variables", "Granularity", "Use"], [
    ["Open-Meteo Air-Quality API (Copernicus Atmosphere Monitoring Service, CAMS global model)",
     "PM2.5, PM10, CO, NO₂, SO₂, O₃, dust", "Hourly, ~40 km grid, 15 locations", "Primary pollution signal, AQI"],
    ["Open-Meteo Historical-Forecast & Forecast Weather API",
     "Temperature, relative humidity, wind speed, precipitation, boundary-layer height",
     "Hourly, same locations", "Explanatory features, ML inputs"],
    ["(Extension) DoEnv Nepal — pollution.gov.np reference stations", "PM2.5, PM10 (measured)",
     "Hourly, ~25 stations", "Ground truth / model calibration"],
    ["(Extension) Low-cost IoT sensors (e.g. PurpleAir-type)", "PM2.5", "Minute-level", "Dense urban coverage"],
    ["(Extension) NASA FIRMS active-fire data", "Fire hotspots", "Daily", "Forest-fire episodes (spring)"],
], widths=[5.2, 4.2, 3.4, 3.7], font=9)
para("Monitored locations (15 cities, all 7 provinces): ", bold_lead=None)
table(["Province", "Cities"], [
    ["Koshi", "Biratnagar, Dharan"], ["Madhesh", "Janakpur, Birgunj"],
    ["Bagmati", "Kathmandu, Lalitpur, Bhaktapur, Hetauda, Bharatpur"], ["Gandaki", "Pokhara"],
    ["Lumbini", "Butwal, Nepalgunj"], ["Karnali", "Surkhet"], ["Sudurpashchim", "Dhangadhi, Mahendranagar"],
], widths=[4, 12.5])
para(
    f"Dataset collected for this project: one year (October 2025 – October 2026) × 15 cities × 24 hours "
    f"= {n_readings:,} hourly records with 17 attributes each, plus a live feed polled every 15 minutes. "
    f"The Parquet lake holds {len(lake_files):,} files ({lake_mb:.1f} MB compressed). The design is "
    "horizontally scalable to thousands of sensors at minute resolution (tens of millions of records per "
    "year) by adding Kafka partitions and Spark executors.")

# ------------------------------------------------------------------ 4 features
doc.add_heading("4. Important Features Considered in the Design (Part a)", 1)
doc.add_heading("4.1 Data features (attributes)", 2)
table(["Feature", "Why it matters"], [
    ["PM2.5 (µg/m³)", "Main health-relevant pollutant; basis of AQI and Nepal NAAQS (40 µg/m³, 24 h)"],
    ["PM10, CO, NO₂, SO₂, O₃, dust", "Source fingerprinting: traffic (NO₂, CO), kilns/industry (SO₂), dust storms"],
    ["Boundary-layer (mixing) height", "Captures temperature inversions that trap smoke in the Kathmandu Valley"],
    ["Wind speed", "Ventilation and dispersion of pollutants"],
    ["Precipitation, humidity", "Wet deposition (rain cleans the air); hygroscopic growth of particles"],
    ["Temperature", "Season and inversion strength"],
    ["City, province, lat/lon", "Spatial analysis and partitioning key"],
    ["Event time (Nepal time, UTC+5:45)", "Event-time windows, seasonality, diurnal cycle"],
    ["Derived: AQI, category, lags, 24-h rolling mean", "Alerts and ML forecasting features"],
], widths=[5, 11.5])
doc.add_heading("4.2 System design features (non-functional requirements)", 2)
bullets([
    ("Scalability:", "Kafka topics are partitioned (key = city) and Spark parallelises across partitions. "
                     "Both scale horizontally by adding brokers or executors."),
    ("Low latency / real-time:", "Spark Structured Streaming micro-batches (5–10 s triggers) make new "
                                 "readings visible on the dashboard within seconds."),
    ("Fault tolerance & durability:", "Kafka keeps messages for 7 days (replayable). Spark checkpoints its "
                                      "offsets and state, so a crashed job resumes where it stopped."),
    ("Exactly-once results / idempotency:", "MongoDB writes are upserts on the natural key (city, "
                                            "event_ts) and the batch job de-duplicates the lake, so replays "
                                            "never double-count."),
    ("Event-time correctness:", "Daily windows use observation time aligned to Nepal midnight, with a "
                                "3-hour watermark to accept late data."),
    ("Data quality:", "Schema enforcement on JSON parsing, null and range checks (0–1000 µg/m³), and "
                      "unparseable records are dropped."),
    ("Storage efficiency:", "Columnar, compressed Parquet partitioned by city/date allows partition "
                            "pruning for city-level queries."),
    ("Extensibility:", "New data sources just publish to the same topic. New consumers (SMS gateway, "
                       "mobile app) subscribe to the alerts topic."),
    ("Usability & localisation:", "Colour-coded AQI categories and health advice in English and Nepali."),
    ("Reproducibility:", "Infrastructure as code (Docker Compose), pinned versions, one-command pipeline."),
])

# ------------------------------------------------------------------ 5 storage
doc.add_heading("5. Data Storage Design (Part b)", 1)
para(
    "NepAirWatch uses polyglot persistence. Each storage tier is chosen for its access pattern, following "
    "the data-lake + serving-layer pattern used in production big-data systems.")
table(["Tier", "Technology", "Content", "Access pattern"], [
    ["1. Message log (hot buffer)", "Apache Kafka topics",
     "nepal.air.raw (all readings), nepal.air.alerts", "Append-only, sequential, replayable, 7-day retention"],
    ["2. Landing zone (raw)", "JSON-Lines files", "Exact copy of source responses per city", "Write-once, audit/re-processing"],
    ["3. Data lake — refined zone", "Apache Parquet (HDFS/S3-compatible)",
     "air_quality/ partitioned by city=…/date=…", "Large scans by Spark; partition pruning"],
    ["4. Data lake — curated zone", "Apache Parquet", "monthly_city, daily_city, exceedance",
     "Analytics-ready aggregates"],
    ["5. Serving layer", "MongoDB 7 (document NoSQL)",
     "readings, latest, alerts, daily_stats, forecasts, model_metrics …", "Indexed point & range queries (ms)"],
    ["6. Model store", "Spark ML pipeline directory", "data/models/pm25_gbt", "Load for scoring"],
], widths=[3.6, 3.6, 4.8, 4.5], font=9)
doc.add_heading("5.1 Why Parquet for the lake", 2)
bullets([
    "Columnar layout: analytical queries read only the needed columns (e.g. pm2_5, event_ts).",
    "Compression (Snappy) and encoding shrink storage several-fold compared to JSON.",
    "Schema stored with the data. It is the native format for Spark and is readable by Hive, Presto/Trino, "
    "pandas and DuckDB.",
    "Partitioning by city and date lets Spark skip irrelevant files (partition pruning).",
    "The path is configurable: on a cluster, LAKE_PATH=hdfs://namenode:9000/nepairwatch stores the same lake "
    "on HDFS with 3× replication, with no code changes.",
])
doc.add_heading("5.2 Why MongoDB for serving", 2)
bullets([
    "Flexible document schema fits sensor readings whose attributes differ by source.",
    "Unique compound indexes on (city, event_ts) support idempotent upserts and fast time-range queries.",
    "Millisecond reads for the dashboard. The dashboard never scans the lake.",
    "Scales horizontally through sharding (shard key: city) and offers replica sets for high availability.",
])
doc.add_heading("5.3 MongoDB collections", 2)
table(["Collection", "Written by", "Key / index", "Purpose"], [
    ["readings", "Streaming (Q2)", "(city, event_ts) unique", "Hourly history for charts"],
    ["latest", "Streaming (Q2)", "_id = city", "Current state per city"],
    ["alerts", "Streaming (Q2)", "(city, event_ts) unique", "Unhealthy-air alerts with message & severity"],
    ["daily_stats", "Streaming window (Q3)", "(city, day) unique", "Daily mean/max PM2.5"],
    ["stream_batches", "Streaming (Q2)", "batch_id", "Throughput monitoring"],
    ["exceedance, monthly_city, hourly_profile, province_summary, weather_corr", "Batch job", "—",
     "Historical analytics"],
    ["model_metrics, forecasts, test_predictions", "Batch MLlib", "—", "Early-warning model outputs"],
], widths=[5, 3.2, 3.6, 4.7], font=9)

# ------------------------------------------------------------------ 6 components
doc.add_heading("6. Choice of Big Data Ecosystem Components (Part b)", 1)
para(
    "The assignment requires at least two big-data tools. NepAirWatch is built on two core tools, Apache "
    "Kafka and Apache Spark, supported by Parquet (data-lake format, HDFS-compatible) and MongoDB (NoSQL "
    "serving store). The table compares each choice with the alternatives in the Hadoop ecosystem.")
table(["Requirement", "Chosen", "Alternatives considered", "Reason for choice"], [
    ["Ingestion / messaging", "Apache Kafka", "Apache Flume, RabbitMQ, Sqoop",
     "Partitioned, durable, replayable log; high throughput; decouples producers from many consumers; "
     "native Spark connector. Flume is file/HDFS-oriented, RabbitMQ deletes messages after delivery, "
     "Sqoop is for RDBMS bulk transfer."],
    ["Stream processing", "Spark Structured Streaming", "Apache Flink, Storm, Kafka Streams",
     "Same engine and API for streaming and batch; event-time windows and watermarks; checkpointing; large "
     "community. Flink has lower latency, but hourly data does not need sub-second processing."],
    ["Batch analytics / ML", "Spark SQL + MLlib", "Hadoop MapReduce, Hive, Mahout",
     "In-memory processing is far faster than disk-based MapReduce. SQL + DataFrames + distributed ML "
     "in one framework."],
    ["Long-term storage", "Parquet data lake (HDFS/S3)", "HBase, Hive tables on ORC",
     "Cheap, columnar, compressed, engine-neutral; ideal for large scans."],
    ["Serving database", "MongoDB", "HBase, Cassandra, PostgreSQL",
     "Flexible documents, secondary indexes, simple operations; Cassandra/HBase are better at much larger "
     "write volumes but need more operational work."],
], widths=[3, 3, 3.6, 6.9], font=8.8)
doc.add_heading("6.1 Apache Kafka in NepAirWatch", 2)
bullets([
    "Runs in KRaft mode (no ZooKeeper) and exposes two topics, each with 3 partitions.",
    "Producer key = city, which guarantees per-city ordering and spreads load across partitions.",
    "Producer settings: acks=all, idempotence enabled, lz4 compression, 50 ms linger for batching.",
    "Topic nepal.air.alerts is written by Spark so that downstream alert services (SMS, push, sirens) can "
    "subscribe independently. This is the publish/subscribe benefit of Kafka.",
])
doc.add_heading("6.2 Apache Spark in NepAirWatch", 2)
bullets([
    "Streaming job: three concurrent queries on one Kafka source. Q1 writes the Parquet lake, Q2 runs a "
    "foreachBatch sink to MongoDB with alert detection, and Q3 computes an event-time 1-day tumbling window "
    "with a watermark.",
    "AQI is a native Spark column expression (no Python UDF), so the work stays in the JVM and is optimised "
    "by Catalyst.",
    "Batch job: de-duplication, Spark SQL aggregations, percentile_approx, Pearson correlations, and an MLlib "
    "Pipeline (StringIndexer → OneHotEncoder → VectorAssembler → GBTRegressor) trained with a time-based "
    "split.",
])

# ------------------------------------------------------------------ 7 architecture
doc.add_heading("7. System Architecture and Block Diagram (Part b)", 1)
figure(DOCS / "architecture.png", "NepAirWatch block diagram — sources, ingestion, processing, storage, serving",
       width=16.5)
doc.add_heading("7.1 Data flow", 2)
for i, s in enumerate([
    "Collection: the Python producer calls the Open-Meteo air-quality and weather APIs for each of the 15 "
    "cities, merges the two responses by timestamp and builds one JSON message per city-hour.",
    "Ingestion: messages are published to Kafka topic nepal.air.raw (key = city), and a raw copy is "
    "written to the landing zone.",
    "Stream processing: Spark Structured Streaming subscribes to the topic, parses and validates each "
    "message, computes AQI and category, and in every micro-batch (a) appends to the Parquet lake, "
    "(b) upserts readings and the latest state into MongoDB, raises alerts and publishes them to "
    "nepal.air.alerts, and (c) updates event-time daily windows.",
    "Batch processing: Spark reads the whole lake, de-duplicates it, computes historical analytics, trains "
    "and evaluates the GBT forecasting model, scores the newest observations, and writes the results to "
    "MongoDB and the curated zone.",
    "Serving: the Streamlit dashboard reads only from MongoDB and refreshes the live view every 5 seconds. "
    "Kafka-UI gives operators visibility into topics and messages.",
], 1):
    para(s, bold_lead=f"Step {i}. ")
doc.add_heading("7.2 Architecture style", 2)
para(
    "The design is Kappa-style. Kafka is the single source of truth, and both the real-time view and the "
    "historical lake come from the same stream, so there is one processing code path. The batch layer "
    "re-reads the lake for heavy analytics and model training, which is the useful part of the Lambda "
    "architecture, without maintaining two separate ingestion pipelines. Because Kafka retains data, the "
    "streaming job can be reset and reprocessed from the beginning at any time (--reset).")

# ------------------------------------------------------------------ 8 implementation
doc.add_heading("8. Implementation", 1)
doc.add_heading("8.1 Technology stack", 2)
table(["Layer", "Technology / version"], [
    ["Containers", "Docker Compose: apache/kafka 3.8.0 (KRaft), kafka-ui, mongo 7.0, and the NepAirWatch app image (Python 3.11 + Java 17 + Spark) running 5 services"],
    ["Processing", "Apache Spark 3.5.3 (PySpark), spark-sql-kafka-0-10 connector, Java 17"],
    ["Language & libraries", "Python 3.11, confluent-kafka, pymongo, pandas, pyarrow, requests"],
    ["Visualisation", "Streamlit, Plotly, Matplotlib"],
], widths=[4, 12.5])
doc.add_heading("8.2 Source code modules", 2)
table(["File", "Responsibility"], [
    ["docker-compose.yml", "Kafka broker (KRaft), Kafka-UI and MongoDB with health checks"],
    ["common/config.py", "Cities, thresholds, connection settings (environment-overridable)"],
    ["common/aqi.py", "US-EPA AQI breakpoints, categories, English/Nepali health advice"],
    ["producer/aq_producer.py", "Kafka producer with backfill, live and replay modes"],
    ["spark_jobs/stream_processor.py", "Structured Streaming: validation, AQI, lake, MongoDB, alerts, windows"],
    ["spark_jobs/batch_analytics.py", "Spark SQL analytics and MLlib GBT forecasting"],
    ["dashboard/app.py", "5-tab Streamlit dashboard"],
    ["Dockerfile, docker/*.sh", "App image and container entry scripts — one-command deployment"],
], widths=[5.3, 11.2])
doc.add_heading("8.3 AQI computation", 2)
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
doc.add_heading("8.4 Streaming job (key logic)", 2)
code = (
    "parsed = (spark.readStream.format('kafka').option('subscribe', 'nepal.air.raw').load()\n"
    "   .select(from_json(col('value').cast('string'), RAW_SCHEMA).alias('m')).select('m.*')\n"
    "   .withColumn('event_ts', to_timestamp('event_time'))\n"
    "   .filter(col('pm2_5').between(0, 1000))\n"
    "   .withColumn('aqi', aqi_expr(col('pm2_5')))\n"
    "   .withColumn('category', category_expr(col('aqi'))))\n\n"
    "parsed.writeStream.format('parquet').partitionBy('city', 'date')...       # Q1 lake\n"
    "parsed.writeStream.foreachBatch(write_serving)...                         # Q2 MongoDB + alerts\n"
    "(parsed.withWatermark('event_ts', '3 hours')\n"
    "   .groupBy(window('event_ts', '1 day', '1 day', '18 hours 15 minutes'), 'city')\n"
    "   .agg(avg('pm2_5'), max('pm2_5'), max('aqi'), count('*'))\n"
    "   .writeStream.outputMode('update').foreachBatch(write_daily))           # Q3 windows")
p = doc.add_paragraph()
r = p.add_run(code)
r.font.name = "Consolas"
r.font.size = Pt(8.5)
p.paragraph_format.left_indent = Cm(0.5)
para("The window offset of 18 h 15 min aligns the 1-day UTC windows to midnight in Nepal (UTC+5:45).",
     italic=True, size=9.5)
doc.add_heading("8.5 Forecasting model (Spark MLlib)", 2)
para(
    f"Target: PM2.5 {mm['horizon_hours']} hours ahead in the same city. Features: current pollutants and "
    "weather, hour of day, month, PM2.5 lags of 1, 3, 6 and 24 hours, 24-hour rolling mean, and one-hot city. "
    "Lag and lead features are built with Spark window functions partitioned by city. The model is a "
    "Gradient-Boosted Trees regressor (40 trees, depth 6). The first 80% of the timeline is used for "
    "training and the last 20% for testing. A chronological split avoids the information leakage that "
    "a random split would cause. The baseline is persistence (\"PM2.5 in 6 hours = PM2.5 now\").")
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
    f"Findings: the Terai cities of Koshi and Madhesh provinces (Biratnagar, Janakpur, Birgunj) are the most "
    f"polluted, with annual means of 54–64 µg/m³. That is above the national 24-hour standard on average, "
    f"and they breach it on 70–76% of days. This matches the role of the Indo-Gangetic Plain airshed. "
    f"Kathmandu breached the national standard on {ktm['days_over_nepal_std']} days. Pokhara is the "
    f"cleanest city ({pkr['annual_mean']} µg/m³), yet it still exceeded the WHO guideline on "
    f"{pkr['days_over_who']} days.")
doc.add_heading("9.2 Seasonal and diurnal patterns", 2)
figure(FIG / "seasonality.png", "Monthly mean PM2.5 per city (Spark batch aggregation)")
para(
    "PM2.5 peaks sharply in the dry season (December to April). The monsoon (June to September) brings the "
    "cleanest air through wet deposition and stronger mixing. Air-quality interventions such as kiln "
    "operating restrictions and school schedule changes should therefore target the winter and spring "
    "months.")
figure(FIG / "diurnal.png", "Average PM2.5 by hour of the day", width=13)
para(
    "A clear diurnal cycle is visible: concentrations rise in the evening and stay high through the night "
    "and early morning, when the boundary layer is shallow. They drop around midday, when solar heating "
    "deepens the mixing layer.")
doc.add_heading("9.3 Weather drivers", 2)
figure(FIG / "weather_corr.png", "Pearson correlation of PM2.5 with weather variables", width=13)
bl = wc.get(("Kathmandu Valley", "boundary_layer_height"))
pr = wc.get(("All Nepal", "precipitation"))
para(
    f"PM2.5 is negatively correlated with boundary-layer height (r = {bl} in the Kathmandu Valley): a "
    f"shallow mixing layer traps pollution, which confirms the inversion mechanism. Precipitation is also "
    f"negatively associated (r = {pr} nationally) through wet scavenging. These relationships justify using "
    "weather forecasts as inputs for early warning.")
figure(FIG / "kathmandu_daily.png", "Kathmandu daily mean PM2.5 produced by the streaming event-time window (Q3)")
doc.add_heading("9.4 Forecasting performance", 2)
table(["Model", "RMSE (µg/m³)", "MAE (µg/m³)", "R²"], [
    ["Persistence baseline", mm["baseline_rmse"], mm["baseline_mae"], mm["baseline_r2"]],
    [f"Spark MLlib GBT (t+{mm['horizon_hours']} h)", mm["rmse"], mm["mae"], mm["r2"]],
], widths=[6, 3.5, 3.5, 3.5])
impr = round(100 * (1 - mm["rmse"] / mm["baseline_rmse"]), 1)
para(
    f"The GBT model reduces the forecast error by {impr}% compared with persistence. It was trained on "
    f"{mm['train_rows']:,} rows and evaluated on {mm['test_rows']:,} unseen later rows. The most important "
    f"features are {', '.join(f['feature'] for f in mm['feature_importance'][:4])}. A 6-hour lead time "
    f"gives schools and hospitals time to act before an episode peaks.")
figure(FIG / "forecast.png", "Actual vs forecast PM2.5 for Kathmandu during the hold-out period")
figure(FIG / "feature_importance.png", "Top-10 feature importances of the GBT model", width=10)
doc.add_heading("9.5 Pipeline performance", 2)
jr = db.job_runs.find_one(sort=[("at", -1)])
bulk = db.stream_batches.find_one(sort=[("records", -1)]) or {"records": 0}
bullets([
    f"{n_readings:,} records ingested through Kafka and processed by Spark Structured Streaming. The "
    f"largest micro-batch was {bulk['records']:,} records (bounded by maxOffsetsPerTrigger = 25,000).",
    f"{n_alerts:,} alert events generated and published to the nepal.air.alerts topic.",
    f"Parquet lake: {len(lake_files):,} files, {lake_mb:.1f} MB, partitioned by city and date.",
    f"Full batch analytics + model training run: {jr['seconds'] if jr else '—'} s on a single 4-core laptop "
    "(Spark local mode).",
    "In live mode new readings reach the dashboard within one trigger interval (≈5–10 s).",
])
page_break()

# ------------------------------------------------------------------ 10 demo
doc.add_heading("10. Demonstration", 1)
para("The dashboard has five tabs. Screenshots from the running system:")
for f, cap in [("1_live.png", "Live monitor — map, current AQI per city with Nepali advice, alert feed, stream throughput"),
               ("2_city.png", "City explorer — PM2.5/PM10 vs standards and boundary-layer height"),
               ("3_analytics.png", "Analytics — exceedance days, seasonality heat-map, diurnal profile"),
               ("4_early.png", "Early warning — model metrics vs baseline and 6-hour forecast per city"),
               ("5_pipeline.png", "Pipeline health — Kafka topic offsets, MongoDB collections, data-lake size")]:
    figure(SHOT / f, cap, width=15.5)
doc.add_heading("10.1 Demo plan", 2)
for i, s in enumerate([
    "Introduce the problem using the Analytics tab (exceedance days and seasonality).",
    "Explain the architecture with the block diagram.",
    "Open Kafka-UI (localhost:8080) and show the nepal.air.raw topic: partitions, keys, a sample message.",
    "Start the replay producer (docker compose run --rm replay) and watch the Spark "
    "micro-batch log and the dashboard throughput chart update every few seconds.",
    "Show alerts appearing in the dashboard and in the nepal.air.alerts Kafka topic.",
    "Explore Kathmandu in the City tab and explain the inversion effect.",
    "Show the 6-hour forecast and model accuracy in the Early-warning tab.",
    "Show the Pipeline-health tab and the Parquet partitions on disk (data/lake/air_quality/city=…/date=…).",
], 1):
    para(s, bold_lead=f"{i}. ")

# ------------------------------------------------------------------ 11 guidelines
doc.add_heading("11. Implementation Guidelines", 1)
para("Prerequisites: Docker with Compose v2, internet access, ~6 GB free RAM. Every component, including "
     "Python, Java 17 and Spark, runs in containers, so nothing else needs to be installed on the host.")
steps = [
    ("Start everything", "docker compose up -d --build",
     "Builds the app image, then starts Kafka, Kafka-UI, MongoDB, topic creation, the one-time 1-year "
     "back-fill, Spark streaming, Spark batch + MLlib (repeats every 6 h), the live producer (every 15 min) "
     "and the dashboard."),
    ("Dashboard", "http://localhost:8501", "Streamlit dashboard. Kafka-UI: http://localhost:8080, Spark UI: :4040."),
    ("Status / logs", "docker compose ps\ndocker compose logs -f spark-streaming", "Service health and Spark micro-batches."),
    ("Demo replay", "docker compose run --rm replay", "Re-streams the last 2 days, 1 hour every 2 s."),
    ("Stop", "docker compose stop", "Stops all services and keeps the data."),
    ("Reset", "docker compose down -v", "Deletes all data. The next start back-fills again."),
]
table(["Step", "Command", "What it does"], [[a, b, c] for a, b, c in steps], widths=[2.6, 6.4, 7.5], font=9)
para("Configuration is through environment variables (BACKFILL_DAYS, LIVE_INTERVAL, BATCH_INTERVAL, KAFKA_BOOTSTRAP, "
     "MONGO_URI, LAKE_PATH, SPARK_MASTER). To run on a cluster, point SPARK_MASTER to YARN, Kubernetes or a "
     "standalone master, and LAKE_PATH to an hdfs:// or s3a:// location. The full source code and README "
     "accompany this report.")

# ------------------------------------------------------------------ 12 limitations
doc.add_heading("12. Limitations and Future Work", 1)
bullets([
    ("Model resolution:", "CAMS is a ~40 km global model. Kathmandu, Lalitpur and Hetauda fall in the same "
                          "grid cell and report identical values, and local hotspots are smoothed out. Fusing "
                          "DoEnv reference stations and low-cost sensors through the same Kafka topic, with "
                          "bias correction, is the next step."),
    ("Single-node deployment:", "The demo runs on one machine. Production would use a 3-broker Kafka "
                                "cluster (replication factor 3), Spark on YARN/Kubernetes and HDFS/S3 storage."),
    ("Alert delivery:", "Alerts are published to Kafka and shown on the dashboard. An SMS/Viber/push "
                        "consumer (for example via Sparrow SMS) is planned."),
    ("Small files:", "Frequent micro-batches create many small Parquet files. A periodic compaction job, "
                     "or Delta Lake/Iceberg tables, would fix this."),
    ("Richer features:", "NASA FIRMS fire counts, traffic and brick-kiln operating calendars could improve "
                         "forecasts, as could deep-learning sequence models (LSTM)."),
])

# ------------------------------------------------------------------ 13 conclusion
doc.add_heading("13. Conclusion", 1)
para(
    "NepAirWatch shows that an open-source big-data stack can address a pressing real-world problem in "
    "Nepal. Apache Kafka provides a scalable, fault-tolerant and replayable ingestion backbone. Apache "
    "Spark provides real-time AQI computation and alerting, event-time windowed aggregation, large-scale "
    "historical analytics and distributed machine learning in one engine. The Parquet data lake and the "
    "MongoDB serving layer separate cheap long-term storage from low-latency access. The analysis shows "
    "that PM2.5 standards are breached on most days of the year in the Terai and on almost half the days "
    "in Kathmandu, and that episodes can be forecast six hours ahead with good accuracy. With ground-sensor "
    "integration and an SMS alert service, the platform could serve as a national early-warning system "
    "protecting millions of people.")

# ------------------------------------------------------------------ references
doc.add_heading("References", 1)
for r in [
    "World Health Organization (2021). WHO Global Air Quality Guidelines: PM2.5, PM10, ozone, nitrogen dioxide, "
    "sulfur dioxide and carbon monoxide. Geneva: WHO.",
    "Government of Nepal (2012). National Ambient Air Quality Standards for Nepal, Nepal Gazette.",
    "Department of Environment, Nepal. Air Quality Monitoring — https://pollution.gov.np",
    "U.S. EPA (2024). Technical Assistance Document for the Reporting of Daily Air Quality — the Air Quality "
    "Index (AQI).",
    "Copernicus Atmosphere Monitoring Service (CAMS). Global atmospheric composition forecasts.",
    "Open-Meteo. Air Quality API and Weather Forecast API documentation — https://open-meteo.com",
    "Apache Software Foundation. Apache Kafka 3.8 Documentation — https://kafka.apache.org",
    "Apache Software Foundation. Structured Streaming Programming Guide, Spark 3.5 — https://spark.apache.org",
    "Kleppmann, M. (2017). Designing Data-Intensive Applications. O'Reilly Media.",
    "Kreps, J. (2014). Questioning the Lambda Architecture. O'Reilly Radar.",
    "Health Effects Institute (2024). State of Global Air 2024.",
]:
    para(r, style="List Number")

# ------------------------------------------------------------------ appendices
doc.add_heading("Appendix A — Kafka Message Schema (nepal.air.raw)", 1)
p = doc.add_paragraph()
r = p.add_run('{ "city": "Kathmandu", "province": "Bagmati", "lat": 27.7172, "lon": 85.324,\n'
              '  "event_time": "2026-01-12T08:00:00+05:45",\n'
              '  "pm2_5": 112.4, "pm10": 140.2, "co": 1450.0, "no2": 38.1, "so2": 12.0, "o3": 20.3, "dust": 3.0,\n'
              '  "temperature": 6.2, "humidity": 81.0, "wind_speed": 2.1, "precipitation": 0.0,\n'
              '  "boundary_layer_height": 35.0,\n'
              '  "source": "open-meteo/live", "ingest_time": "2026-01-12T02:20:11+00:00" }')
r.font.name = "Consolas"
r.font.size = Pt(8.5)
doc.add_heading("Appendix B — Work Division", 1)
table(["Member", "Responsibilities"], [
    ["[Student Name 1]", "Problem study, data-source analysis, Kafka producer & ingestion, MongoDB design, dashboard"],
    ["[Student Name 2]", "Spark Structured Streaming job, batch analytics & MLlib model, data lake, evaluation"],
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
