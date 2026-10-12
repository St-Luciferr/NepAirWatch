// Build docs/NepAirWatch_Presentation.pptx from docs/deck_data.json + figures.
// Usage: NODE_PATH=<dir with pptxgenjs, react-icons, react, react-dom, sharp> node scripts/build_deck.js
const path = require("path");
const fs = require("fs");
const pptxgen = require("pptxgenjs");
const React = require("react");
const ReactDOMServer = require("react-dom/server");
const sharp = require("sharp");
const fa = require("react-icons/fa");

const ROOT = path.resolve(__dirname, "..");
const DOCS = path.join(ROOT, "docs");
const D = JSON.parse(fs.readFileSync(path.join(DOCS, "deck_data.json"), "utf8"));
const OUT = path.join(DOCS, "NepAirWatch_Presentation.pptx");
const APPLY_THEME = process.env.APPLY_THEME;

const THEME = {
  name: "NepAirWatch Haze",
  headFontFace: "Arial",
  bodyFontFace: "Calibri",
  colors: {
    dk1: "1E2A36", lt1: "FFFFFF", dk2: "4A5A6A", lt2: "F1F4F7",
    accent1: "E07A2F", accent2: "C0392B", accent3: "2E86AB", accent4: "5E8C61",
    accent5: "8E6C8A", accent6: "F2C14E", hlink: "2E86AB", folHlink: "8E6C8A",
  },
};
const HEX = THEME.colors;

async function icon(Comp, color, size = 256) {
  const svg = ReactDOMServer.renderToStaticMarkup(React.createElement(Comp, { color: "#" + color, size }));
  const buf = await sharp(Buffer.from(svg)).png().toBuffer();
  return "image/png;base64," + buf.toString("base64");
}

(async () => {
  const pres = new pptxgen();
  pres.layout = "LAYOUT_WIDE"; // 13.33 x 7.5
  pres.title = "NepAirWatch";
  pres.author = "NepAirWatch project team";
  pres.theme = { headFontFace: THEME.headFontFace, bodyFontFace: THEME.bodyFontFace };
  const C = pres.SchemeColor;

  // ---------------------------------------------------------------- layouts
  pres.defineSlideMaster({
    title: "TITLE_DARK",
    background: { color: C.text1 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: 0.8, y: 2.2, w: 11.7, h: 1.4, fontSize: 54,
        bold: true, color: C.background1, fontFace: THEME.headFontFace, valign: "bottom", align: "left", margin: 0 }, text: "" } },
      { placeholder: { options: { name: "body", type: "body", x: 0.8, y: 3.75, w: 11.7, h: 1.2, fontSize: 22,
        color: C.accent1, valign: "top", margin: 0 }, text: "" } },
    ],
  });
  pres.defineSlideMaster({
    title: "CONTENT",
    background: { color: C.background1 },
    margin: [0.5, 0.6, 0.6, 0.6],
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: 0.6, y: 0.35, w: 12.1, h: 0.85, fontSize: 32,
        bold: true, color: C.text1, fontFace: THEME.headFontFace, valign: "middle", align: "left", margin: 0 }, text: "" } },
      { text: { text: "NepAirWatch  ·  Kafka + Spark for clean-air early warning in Nepal",
        options: { x: 0.6, y: 7.0, w: 8, h: 0.3, fontSize: 10, color: C.text2, margin: 0 } } },
    ],
    slideNumber: { x: 12.2, y: 7.0, w: 0.5, h: 0.3, fontSize: 10, color: C.text2, align: "right" },
  });
  pres.defineSlideMaster({
    title: "CONTENT_DARK",
    background: { color: C.text1 },
    objects: [
      { placeholder: { options: { name: "title", type: "title", x: 0.6, y: 0.35, w: 12.1, h: 0.85, fontSize: 32,
        bold: true, color: C.background1, fontFace: THEME.headFontFace, valign: "middle", align: "left", margin: 0 }, text: "" } },
    ],
    slideNumber: { x: 12.2, y: 7.0, w: 0.5, h: 0.3, fontSize: 10, color: C.background2, align: "right" },
  });

  const ex = D.exceedance;
  const m = D.model;
  const city = (n) => ex.find((r) => r.city === n);
  const ktm = city("Kathmandu"), pkr = city("Pokhara"), worst = ex[0];
  const fmt = (n) => n.toLocaleString("en-US");

  const card = (s, x, y, w, h, name, fill = C.background2) =>
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w, h, rectRadius: 0.12, fill: { color: fill },
      line: { type: "none" }, objectName: name });

  const iconCircle = async (s, Comp, x, y, d, bg, fg, name) => {
    s.addShape(pres.shapes.OVAL, { x, y, w: d, h: d, fill: { color: bg }, line: { type: "none" }, objectName: name + " circle" });
    const pad = d * 0.24;
    s.addImage({ data: await icon(Comp, fg), x: x + pad, y: y + pad, w: d - 2 * pad, h: d - 2 * pad, objectName: name + " icon" });
  };

  // ================================================================ 1 Title
  pres.addSection({ title: "Introduction" });
  let s = pres.addSlide({ masterName: "TITLE_DARK", sectionTitle: "Introduction" });
  s.addText("NepAirWatch", { placeholder: "title" });
  s.addText("Real-time air-quality monitoring & early warning for Nepal\nwith Apache Kafka + Apache Spark",
    { placeholder: "body" });
  await iconCircle(s, fa.FaMountain, 0.8, 0.9, 1.0, HEX.accent1, HEX.dk1, "logo");
  s.addText("[Student Name 1]  ·  [Student Name 2]\nBig Data — Implementation Task  ·  Demo", {
    x: 0.8, y: 5.6, w: 11.7, h: 0.9, fontSize: 16, color: C.background2, margin: 0, isTextBox: true, objectName: "team" });
  s.addNotes("Introduce the team and the one-line pitch: a big-data platform that turns raw air-quality data into real-time alerts and 6-hour forecasts for 15 Nepali cities.");

  // ================================================================ 2 Problem
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Introduction" });
  s.addText("Nepal breathes unsafe air on most days of the year", { placeholder: "title" });
  const stats = [
    [`${worst.days_over_nepal_std}`, `days ${worst.city} broke Nepal's 24-h PM2.5 standard (40 µg/m³)`, C.accent2],
    [`${ktm.days_over_nepal_std}`, `days Kathmandu broke the national standard`, C.accent1],
    [`${pkr.days_over_who}`, `days even \"clean\" Pokhara exceeded the WHO guideline`, C.accent3],
  ];
  stats.forEach(([big, lbl, col], i) => {
    const x = 0.6 + i * 2.0;
    s.addText(big, { x: 0.6, y: 1.45 + i * 1.75, w: 2.1, h: 0.95, fontSize: 54, bold: true, color: col, margin: 0,
      isTextBox: true, objectName: `stat ${i + 1}` });
    s.addText(lbl, { x: 2.75, y: 1.5 + i * 1.75, w: 2.7, h: 0.9, fontSize: 14, color: C.text2, margin: 0,
      valign: "middle", isTextBox: true, objectName: `stat ${i + 1} label` });
  });
  s.addChart(pres.charts.BAR, [{ name: "Days > Nepal NAAQS", labels: ex.map((r) => r.city),
    values: ex.map((r) => r.days_over_nepal_std) }], {
    x: 5.6, y: 1.35, w: 7.2, h: 5.45, barDir: "bar", chartColors: [HEX.accent2],
    showTitle: true, title: "Days above Nepal 24-h PM2.5 standard (past year)", titleFontSize: 13,
    titleColor: HEX.dk1, titleFontFace: "+mn-lt",
    showValue: true, dataLabelPosition: "outEnd", dataLabelFontSize: 10, dataLabelColor: HEX.dk2, dataLabelFontFace: "+mn-lt",
    catAxisLabelColor: HEX.dk2, catAxisLabelFontSize: 10, catAxisLabelFontFace: "+mn-lt", catAxisOrientation: "maxMin",
    valAxisHidden: true, valGridLine: { style: "none" }, catGridLine: { style: "none" }, showLegend: false,
    barGapWidthPct: 45, objectName: "exceedance chart",
  });
  s.addNotes("Problem: winter inversions in the Kathmandu bowl, Indo-Gangetic haze in the Terai, brick kilns, traffic, burning. Few ground stations, no city-level early warning, no Nepali advice. These numbers come from our own Spark analysis of one year of data.");

  // ================================================================ 3 Problem definition
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Introduction" });
  s.addText("Problem definition: what is missing today", { placeholder: "title" });
  const gaps = [
    [fa.FaMapMarkedAlt, "Sparse monitoring", "Reference stations sit mostly in Kathmandu Valley; Terai and far-west cities are largely unmonitored."],
    [fa.FaBell, "No early warning", "Schools, hospitals and outdoor workers learn about bad air only after they are exposed."],
    [fa.FaLanguage, "Numbers, not advice", "µg/m³ means little to citizens. People need colour categories and advice in Nepali."],
    [fa.FaChartLine, "No policy evidence", "Authorities lack long-term, multi-city evidence of when and where standards are breached."],
  ];
  for (let i = 0; i < 4; i++) {
    const [Ic, h, b] = gaps[i];
    const x = 0.6 + (i % 2) * 6.15, y = 1.5 + Math.floor(i / 2) * 2.55;
    card(s, x, y, 5.95, 2.3, `gap ${i + 1} card`);
    await iconCircle(s, Ic, x + 0.35, y + 0.4, 0.9, HEX.accent1, HEX.lt1, `gap ${i + 1}`);
    s.addText(h, { x: x + 1.5, y: y + 0.35, w: 4.2, h: 0.5, fontSize: 20, bold: true, color: C.text1, margin: 0, isTextBox: true, objectName: `gap ${i + 1} head` });
    s.addText(b, { x: x + 1.5, y: y + 0.9, w: 4.2, h: 1.2, fontSize: 15, color: C.text2, margin: 0, valign: "top", isTextBox: true, objectName: `gap ${i + 1} body` });
  }
  s.addNotes("Goal: a unified, real-time, nationwide system that monitors, explains, warns ahead of time and builds policy evidence. Stakeholders: citizens, schools, hospitals, DoEnv, MoHP, municipalities.");

  // ================================================================ 4 Data sources
  pres.addSection({ title: "Design" });
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Design" });
  s.addText("Data sources: six real sources, 15 cities, all 7 provinces", { placeholder: "title" });
  const srcs = [
    [fa.FaSatellite, "Open-Meteo · CAMS model", "EU Copernicus air-quality model + weather: PM2.5, PM10, gases, mixing height · hourly, 15 cities · 1 yr + live → Kafka", HEX.accent2],
    [fa.FaBroadcastTower, "DoEnv Nepal stations", "Government reference monitors, ~30 sites, all 7 provinces · PM1 / PM2.5 / PM10 every minute · ground truth → Kafka", HEX.accent3],
    [fa.FaUsers, "Sensor.Community", "Low-cost citizen sensors (SDS011) in Kathmandu and Madhesh · PM2.5 / PM10 every ~5 min → Kafka", HEX.accent4],
    [fa.FaFire, "NASA FIRMS fires", "VIIRS + MODIS satellite fire hotspots over Nepal and the Indo-Gangetic Plain · CSV files → Flume → Kafka + HDFS", HEX.accent1],
    [fa.FaGlobeAsia, "NASA POWER", "Daily aerosol optical depth (haze) + climate per city, 2001 → today (25 years) · bulk-loaded into HDFS", HEX.accent5],
    [fa.FaNewspaper, "Google News RSS", "Headlines on pollution, haze, fires and brick kilns · unstructured text → Kafka → Spark NLP / LDA", HEX.dk2],
  ];
  for (let i = 0; i < srcs.length; i++) {
    const [Ic, h, b, col] = srcs[i];
    const x = 0.6 + (i % 2) * 3.85, y = 1.45 + Math.floor(i / 2) * 1.5;
    await iconCircle(s, Ic, x, y, 0.7, col, HEX.lt1, `source ${i + 1}`);
    s.addText(h, { x: x + 0.85, y: y, w: 2.9, h: 0.4, fontSize: 15, bold: true, color: C.text1, margin: 0, isTextBox: true, objectName: `source ${i + 1} head` });
    s.addText(b, { x: x + 0.85, y: y + 0.42, w: 2.9, h: 1.0, fontSize: 11, color: C.text2, margin: 0, valign: "top", isTextBox: true, objectName: `source ${i + 1} body` });
  }
  card(s, 8.35, 1.45, 4.35, 4.4, "volume card", C.text1);
  const vol = [["131,700", "hourly CAMS + weather records (1 year)"], ["~700,000", "1-minute ground readings (+~45k a day)"],
    ["141,195", "NASA POWER city-days, 2001 → today"], ["1,275 · 383", "fire hotspots (7 days) · news articles"]];
  vol.forEach(([v, l], i) => {
    s.addText(v, { x: 8.65, y: 1.6 + i * 1.05, w: 3.85, h: 0.55, fontSize: 28, bold: true, color: C.accent1, margin: 0, isTextBox: true, objectName: `volume ${i + 1}` });
    s.addText(l, { x: 8.65, y: 2.13 + i * 1.05, w: 3.85, h: 0.35, fontSize: 12, color: C.background2, margin: 0, isTextBox: true, objectName: `volume ${i + 1} label` });
  });
  s.addText("Koshi: Biratnagar, Dharan · Madhesh: Janakpur, Birgunj · Bagmati: Kathmandu, Lalitpur, Bhaktapur, Hetauda, Bharatpur · Gandaki: Pokhara · Lumbini: Butwal, Nepalgunj · Karnali: Surkhet · Sudurpashchim: Dhangadhi, Mahendranagar",
    { x: 0.6, y: 6.1, w: 12.1, h: 0.7, fontSize: 12, color: C.text2, italic: true, margin: 0, isTextBox: true, objectName: "city list" });
  s.addNotes("Six free, real public sources of four kinds. MODEL: Open-Meteo serves the EU Copernicus CAMS model, which gives hourly PM2.5 and weather (incl. boundary-layer height) for every city, but on a ~40 km grid. MEASURED: DoEnv government stations (1-minute) and Sensor.Community citizen sensors are the ground truth used to validate and calibrate CAMS; the DoEnv API keeps only ~3 weeks, so our HDFS lake becomes the archive. SATELLITE: NASA FIRMS fire hotspots arrive as CSV files, so they go through Flume (fan-out to Kafka and HDFS); NASA POWER gives 25 years of aerosol data, bulk-loaded straight into HDFS. TEXT: Google News headlines for theme tagging and LDA topics. Each source is fetched historically (back-fill) and live (polling). Volume, velocity and variety: ~1M records on the first start, 1-minute streams, and JSON, CSV and text.");

  // ================================================================ 5 Design features
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Design" });
  s.addText("Important features considered in the design", { placeholder: "title" });
  const feats = [
    [fa.FaExpandArrowsAlt, "Scalability", "Kafka partitions (key = city) + Spark executors scale out horizontally"],
    [fa.FaBolt, "Low latency", "Structured Streaming micro-batches every 5–10 s → dashboard in seconds"],
    [fa.FaShieldAlt, "Fault tolerance", "Kafka 7-day retention + Spark checkpoints → resume or replay anytime"],
    [fa.FaRedo, "Idempotent results", "Upserts on (city, event_ts) and lake de-duplication → no double counting"],
    [fa.FaClock, "Event-time correctness", "Daily windows aligned to Nepal time (UTC+5:45), 3 h watermark"],
    [fa.FaCheckCircle, "Data quality", "Schema-on-read, null and range checks (0–1000 µg/m³)"],
  ];
  for (let i = 0; i < 6; i++) {
    const [Ic, h, b] = feats[i];
    const x = 0.6 + (i % 3) * 4.1, y = 1.5 + Math.floor(i / 3) * 2.7;
    card(s, x, y, 3.85, 2.45, `feature ${i + 1} card`);
    await iconCircle(s, Ic, x + 0.3, y + 0.3, 0.75, HEX.dk1, HEX.accent1, `feature ${i + 1}`);
    s.addText(h, { x: x + 1.2, y: y + 0.3, w: 2.5, h: 0.75, fontSize: 18, bold: true, color: C.text1, margin: 0, valign: "middle", isTextBox: true, objectName: `feature ${i + 1} head` });
    s.addText(b, { x: x + 0.3, y: y + 1.2, w: 3.3, h: 1.1, fontSize: 14, color: C.text2, margin: 0, valign: "top", isTextBox: true, objectName: `feature ${i + 1} body` });
  }
  s.addNotes("Data features: PM2.5 (main), PM10/CO/NO2/SO2/O3/dust for source fingerprinting, boundary-layer height for inversions, wind, rain, humidity, temperature, city/province, event time, and derived AQI, lags and rolling means. Plus extensibility and Nepali localisation.");

  // ================================================================ 6 Architecture
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Design" });
  s.addText("Architecture: Kafka ingests, Spark processes", { placeholder: "title" });
  s.addImage({ path: path.join(DOCS, "architecture.png"), x: (13.33 - 5.55 * 2006 / 1138) / 2, y: 1.3, w: 5.55 * 2006 / 1138, h: 5.55, objectName: "architecture diagram" });
  s.addNotes("Walk left to right: sources → producer → Kafka topic nepal.air.raw → Spark Structured Streaming with three queries (lake, MongoDB+alerts, daily windows) → Parquet lake and MongoDB → Streamlit dashboard. Batch Spark + MLlib reads the lake and writes analytics and forecasts. Alerts go back to Kafka for SMS/app consumers. Kappa-style: one stream, replayable.");

  // ================================================================ 7 Component choice
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Design" });
  s.addText("Why Kafka and Spark (vs. other ecosystem tools)", { placeholder: "title" });
  const hdr = (t) => ({ text: t, options: { bold: true, color: C.background1, fill: { color: C.text1 }, fontSize: 14 } });
  const rows = [
    [hdr("Need"), hdr("Chosen"), hdr("Alternatives"), hdr("Why")],
    ["Ingestion", "Apache Kafka", "Flume, RabbitMQ, Sqoop", "Durable, partitioned, replayable log; many independent consumers"],
    ["Stream processing", "Spark Structured Streaming", "Flink, Storm, Kafka Streams", "One engine for stream + batch; event-time windows, watermarks, checkpoints"],
    ["Batch + ML", "Spark SQL + MLlib", "MapReduce, Hive, Mahout", "In-memory, much faster than disk MapReduce; SQL + distributed ML"],
    ["Data lake", "Parquet (HDFS / S3)", "HBase, Hive on ORC", "Columnar, compressed, partition pruning, engine-neutral"],
    ["Serving DB", "MongoDB", "Cassandra, HBase, PostgreSQL", "Flexible documents, secondary indexes, ms reads"],
  ];
  s.addTable(rows, { x: 0.6, y: 1.45, w: 12.1, colW: [2.1, 2.9, 2.8, 4.3], fontSize: 14, color: C.text1,
    border: { type: "solid", pt: 0.75, color: "D5DCE3" }, rowH: 0.78, valign: "middle", margin: 0.08, objectName: "component table" });
  s.addNotes("The two core big-data tools are Kafka and Spark. Parquet/HDFS and MongoDB are the storage components. Flink would give lower latency, but hourly data does not need it, and Spark gives one API for streaming, batch and ML.");

  // ================================================================ 8 Storage
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Design" });
  s.addText("Storage: each tier matches an access pattern", { placeholder: "title" });
  const tiers = [
    ["Kafka log", "nepal.air.raw · nepal.air.alerts", "append-only, 3 partitions, 7-day replay", HEX.accent2],
    ["Landing zone", "raw JSON-Lines per city", "immutable audit copy", HEX.dk2],
    ["Parquet lake", "air_quality/city=…/date=…", "columnar scans by Spark (→ HDFS/S3)", HEX.accent4],
    ["Curated zone", "monthly · daily · exceedance", "analytics-ready tables", HEX.accent4],
    ["MongoDB", "readings · latest · alerts · forecasts …", "indexed ms queries for dashboard", HEX.accent3],
  ];
  tiers.forEach(([n, c, a, col], i) => {
    const y = 1.4 + i * 0.98;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 0.6, y, w: 2.9, h: 0.88, rectRadius: 0.1, fill: { color: col }, line: { type: "none" }, objectName: `tier ${i + 1} chip` });
    s.addText(n, { x: 0.6, y, w: 2.9, h: 0.88, fontSize: 18, bold: true, color: C.background1, align: "center", valign: "middle", margin: 0, isTextBox: true, objectName: `tier ${i + 1} name` });
    s.addText(c, { x: 3.8, y, w: 4.6, h: 0.88, fontSize: 15, color: C.text1, valign: "middle", margin: 0, isTextBox: true, objectName: `tier ${i + 1} content` });
    s.addText(a, { x: 8.6, y, w: 4.1, h: 0.88, fontSize: 14, color: C.text2, italic: true, valign: "middle", margin: 0, isTextBox: true, objectName: `tier ${i + 1} access` });
  });
  s.addText("Change one setting (LAKE_PATH=hdfs://namenode:9000/nepairwatch) to move the lake onto HDFS with 3× replication. No code changes.",
    { x: 0.6, y: 6.35, w: 12.1, h: 0.45, fontSize: 13, color: C.accent1, bold: true, margin: 0, isTextBox: true, objectName: "hdfs note" });
  s.addNotes("Polyglot persistence: Kafka is the hot buffer and single source of truth; Parquet lake for cheap, scalable history; MongoDB for low-latency serving. The dashboard never scans the lake.");

  // ================================================================ 9 Spark streaming flow
  pres.addSection({ title: "Implementation" });
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Implementation" });
  s.addText("Spark Structured Streaming: one source, three queries", { placeholder: "title" });
  const steps = [["Kafka source", "subscribe nepal.air.raw\nmaxOffsetsPerTrigger 25k"],
    ["Parse + validate", "from_json with schema\nrange & null checks"],
    ["Enrich", "PM2.5 → AQI + category\n(native Spark expression)"]];
  steps.forEach(([h, b], i) => {
    const x = 0.6 + i * 2.75;
    card(s, x, 1.6, 2.45, 1.9, `step ${i + 1} card`, C.text1);
    s.addText(h, { x: x + 0.2, y: 1.75, w: 2.05, h: 0.5, fontSize: 17, bold: true, color: C.accent1, margin: 0, isTextBox: true, objectName: `step ${i + 1} head` });
    s.addText(b, { x: x + 0.2, y: 2.3, w: 2.05, h: 1.1, fontSize: 13, color: C.background1, margin: 0, valign: "top", isTextBox: true, objectName: `step ${i + 1} body` });
    if (i < 2) s.addShape(pres.shapes.RIGHT_TRIANGLE, { x: x + 2.5, y: 2.4, w: 0.2, h: 0.3, rotate: 0, fill: { color: C.accent1 }, line: { type: "none" }, flipH: false, objectName: `step ${i + 1} arrow` });
  });
  const qs = [["Q1 · Lake sink", "append Parquet partitioned by city / date (every 10 s)", HEX.accent4],
    ["Q2 · Serving + alerts", "foreachBatch → MongoDB upserts, latest per city, AQI ≥ 151 → alerts (MongoDB + Kafka topic)", HEX.accent3],
    ["Q3 · Event-time window", "1-day tumbling window aligned to Nepal midnight, 3 h watermark → daily_stats", HEX.accent5]];
  qs.forEach(([h, b, col], i) => {
    const y = 1.6 + i * 1.75;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x: 8.95, y, w: 3.75, h: 1.55, rectRadius: 0.1, fill: { color: col }, line: { type: "none" }, objectName: `query ${i + 1} card` });
    s.addText(h, { x: 9.15, y: y + 0.12, w: 3.4, h: 0.45, fontSize: 16, bold: true, color: C.background1, margin: 0, isTextBox: true, objectName: `query ${i + 1} head` });
    s.addText(b, { x: 9.15, y: y + 0.55, w: 3.4, h: 0.95, fontSize: 12, color: C.background1, margin: 0, valign: "top", isTextBox: true, objectName: `query ${i + 1} body` });
  });
  s.addShape(pres.shapes.LINE, { x: 8.05, y: 2.55, w: 0.85, h: 0, line: { color: HEX.accent1, width: 2, endArrowType: "triangle" }, objectName: "fan-out line" });
  s.addText([
    { text: `${fmt(D.readings)}`, options: { bold: true, color: C.accent1, fontSize: 30, breakLine: true } },
    { text: "records streamed from Kafka through Spark into the lake and MongoDB", options: { fontSize: 14, color: C.text2, breakLine: true } },
    { text: `${fmt(D.alerts)}`, options: { bold: true, color: C.accent2, fontSize: 30, breakLine: true } },
    { text: "alert events published to the nepal.air.alerts topic", options: { fontSize: 14, color: C.text2 } },
  ], { x: 0.6, y: 3.95, w: 7.6, h: 2.7, margin: 0, valign: "top", isTextBox: true, objectName: "stream stats" });
  s.addNotes("Idempotent: Q2 upserts on (city, event_ts), so replays don't double count. Checkpoints give exactly-once offset tracking. Alerts are fanned back out to Kafka so an SMS gateway can subscribe.");

  // ================================================================ 10 Results — seasonality
  pres.addSection({ title: "Results" });
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
  s.addText("Pollution is seasonal: dry winter and spring are worst", { placeholder: "title" });
  s.addImage({ path: path.join(DOCS, "figures", "seasonality.png"), x: 0.6, y: 1.35, w: 8.1, h: 8.1 * 800 / 1600, objectName: "seasonality heatmap" });
  s.addText([
    { text: "Dec – Apr", options: { bold: true, fontSize: 26, color: C.accent2, breakLine: true } },
    { text: "Peak PM2.5: inversions, crop and forest fires, brick-kiln season", options: { fontSize: 14, color: C.text2, breakLine: true } },
    { text: " ", options: { fontSize: 10, breakLine: true } },
    { text: "Jun – Sep", options: { bold: true, fontSize: 26, color: C.accent4, breakLine: true } },
    { text: "Monsoon rain washes particles out (wet deposition)", options: { fontSize: 14, color: C.text2, breakLine: true } },
    { text: " ", options: { fontSize: 10, breakLine: true } },
    { text: "Terai > Valley > Hills", options: { bold: true, fontSize: 22, color: C.text1, breakLine: true } },
    { text: "Indo-Gangetic haze makes Biratnagar, Janakpur and Birgunj the worst", options: { fontSize: 14, color: C.text2 } },
  ], { x: 9.0, y: 1.5, w: 3.7, h: 5.2, margin: 0, valign: "top", isTextBox: true, objectName: "seasonality takeaways" });
  s.addNotes("Spark batch aggregation of the full lake by city and month. Policy implication: time interventions such as kiln restrictions and school schedules to the dry season.");

  // ================================================================ 11 Results — drivers
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
  s.addText("Weather explains the episodes", { placeholder: "title" });
  s.addImage({ path: path.join(DOCS, "figures", "diurnal.png"), x: 0.6, y: 1.4, w: 6.0, h: 3.0, objectName: "diurnal chart" });
  s.addImage({ path: path.join(DOCS, "figures", "weather_corr.png"), x: 6.75, y: 1.4, w: 6.0, h: 3.0, objectName: "correlation chart" });
  const bl = D.corr.find((r) => r.region === "Kathmandu Valley" && r.driver === "boundary_layer_height");
  s.addText([
    { text: "Night-time peak, midday dip: ", options: { bold: true, color: C.text1 } },
    { text: "PM2.5 builds up after sunset when the boundary layer collapses and clears when the sun deepens the mixing layer.", options: { color: C.text2, breakLine: true } },
    { text: `Boundary-layer height r = ${bl ? bl.pearson_r : "—"} (Kathmandu Valley): `, options: { bold: true, color: C.text1 } },
    { text: "this confirms the inversion mechanism, so weather forecasts can predict pollution.", options: { color: C.text2 } },
  ], { x: 0.6, y: 4.75, w: 12.1, h: 1.9, fontSize: 15, margin: 0, paraSpaceAfter: 8, valign: "top", isTextBox: true, objectName: "drivers takeaway" });
  s.addNotes("Pearson correlations computed with Spark DataFrame stat.corr across all city-hours, separately for the Valley and the Terai.");

  // ================================================================ 12 ML early warning
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Results" });
  s.addText("6-hour early warning with Spark MLlib", { placeholder: "title" });
  const impr = Math.round(100 * (1 - m.rmse / m.baseline_rmse));
  [[`${impr}%`, "lower RMSE than the\npersistence baseline", C.accent4],
   [`${m.r2}`, `R² on unseen data\n(baseline ${m.baseline_r2})`, C.accent3],
   [`${m.rmse}`, `RMSE µg/m³\n(baseline ${m.baseline_rmse})`, C.accent1]].forEach(([v, l, col], i) => {
    s.addText(v, { x: 0.6, y: 1.4 + i * 1.75, w: 3.2, h: 0.9, fontSize: 48, bold: true, color: col, margin: 0, isTextBox: true, objectName: `ml stat ${i + 1}` });
    s.addText(l, { x: 0.6, y: 2.25 + i * 1.75, w: 3.2, h: 0.75, fontSize: 14, color: C.text2, margin: 0, valign: "top", isTextBox: true, objectName: `ml stat ${i + 1} label` });
  });
  const tp = D.test_predictions;
  const labels = tp.map((r) => r.event_ts.slice(5, 10));
  s.addChart(pres.charts.LINE, [
    { name: "Actual PM2.5 (t+6 h)", labels, values: tp.map((r) => r.label) },
    { name: "GBT forecast", labels, values: tp.map((r) => r.prediction) },
  ], {
    x: 4.1, y: 1.35, w: 8.6, h: 4.5, chartColors: [HEX.dk1, HEX.accent1], lineSize: 1.5, lineDataSymbol: "none",
    showTitle: true, title: "Kathmandu hold-out period: actual vs forecast (µg/m³)", titleFontSize: 13, titleColor: HEX.dk1, titleFontFace: "+mn-lt",
    showLegend: true, legendPos: "b", legendFontSize: 11, legendFontFace: "+mn-lt", legendColor: HEX.dk2,
    catAxisLabelColor: HEX.dk2, valAxisLabelColor: HEX.dk2, catAxisLabelFontSize: 10, valAxisLabelFontSize: 10,
    catAxisLabelFontFace: "+mn-lt", valAxisLabelFontFace: "+mn-lt", catAxisLabelFrequency: 48,
    valGridLine: { color: "E3E8ED", size: 0.5 }, catGridLine: { style: "none" }, objectName: "forecast chart",
  });
  s.addText(`GBTRegressor · ${fmt(m.train_rows)} train / ${fmt(m.test_rows)} test rows · time-based split · features: lags 1/3/6/24 h, 24 h mean, weather, hour, month, city`,
    { x: 4.1, y: 6.0, w: 8.6, h: 0.6, fontSize: 12, color: C.text2, italic: true, margin: 0, isTextBox: true, objectName: "model caption" });
  s.addNotes("Pipeline: StringIndexer → OneHotEncoder → VectorAssembler → GBTRegressor. A chronological split avoids leakage. The newest observation of each city is scored and shown as a 6-hour forecast on the dashboard.");

  // ================================================================ 13 Demo
  pres.addSection({ title: "Demo" });
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Demo" });
  s.addText("Live demo: the NepAirWatch dashboard", { placeholder: "title" });
  s.addImage({ path: path.join(DOCS, "screenshots", "1_live.png"), x: 0.6, y: 1.35, w: 8.0, h: 5.0, sizing: { type: "crop", w: 8.0, h: 5.0 }, objectName: "dashboard screenshot" });
  s.addText([
    { text: "Demo flow", options: { bold: true, fontSize: 18, color: C.text1, breakLine: true } },
    { text: "Kafka-UI: topic, partitions, messages", options: { bullet: { type: "number" }, breakLine: true } },
    { text: "Start replay producer → Spark micro-batches", options: { bullet: { type: "number" }, breakLine: true } },
    { text: "Live map, AQI table, Nepali advice", options: { bullet: { type: "number" }, breakLine: true } },
    { text: "Alerts in dashboard + alerts topic", options: { bullet: { type: "number" }, breakLine: true } },
    { text: "City explorer: inversion effect", options: { bullet: { type: "number" }, breakLine: true } },
    { text: "Analytics and 6-hour forecast", options: { bullet: { type: "number" }, breakLine: true } },
    { text: "Pipeline health: offsets, lake", options: { bullet: { type: "number" } } },
  ], { x: 8.95, y: 1.4, w: 3.75, h: 5.0, fontSize: 14, color: C.text2, paraSpaceAfter: 6, margin: 0, valign: "top", isTextBox: true, objectName: "demo steps" });
  s.addNotes("Commands: docker compose run --rm replay ; docker compose logs -f spark-streaming ; open localhost:8501 (dashboard), localhost:8080 (Kafka-UI), localhost:4040 (Spark UI).");

  // ================================================================ 14 Limitations
  pres.addSection({ title: "Conclusion" });
  s = pres.addSlide({ masterName: "CONTENT", sectionTitle: "Conclusion" });
  s.addText("Limitations and next steps", { placeholder: "title" });
  const lim = [
    [fa.FaMicrochip, "Fuse ground sensors", "CAMS is a ~40 km model, so Kathmandu, Lalitpur and Hetauda share one grid cell. Add DoEnv stations and IoT sensors and apply bias correction."],
    [fa.FaServer, "Scale out", "3-broker Kafka (RF 3), Spark on YARN or Kubernetes, lake on HDFS or S3, Parquet compaction or Delta Lake."],
    [fa.FaSms, "Deliver alerts", "An SMS/Viber/push consumer on nepal.air.alerts for schools, hospitals and citizens."],
    [fa.FaFire, "Richer forecasts", "NASA FIRMS fire counts, kiln calendars and traffic as features; longer horizons."],
  ];
  for (let i = 0; i < 4; i++) {
    const [Ic, h, b] = lim[i];
    const y = 1.45 + i * 1.32;
    await iconCircle(s, Ic, 0.6, y + 0.05, 0.85, HEX.accent3, HEX.lt1, `next ${i + 1}`);
    s.addText(h, { x: 1.75, y, w: 3.4, h: 0.95, fontSize: 18, bold: true, color: C.text1, margin: 0, valign: "middle", isTextBox: true, objectName: `next ${i + 1} head` });
    s.addText(b, { x: 5.2, y, w: 7.5, h: 0.95, fontSize: 14, color: C.text2, margin: 0, valign: "middle", isTextBox: true, objectName: `next ${i + 1} body` });
  }
  s.addNotes("Be honest about the model-resolution limitation; the architecture already supports adding sources through the same topic and schema.");

  // ================================================================ 15 Conclusion
  s = pres.addSlide({ masterName: "CONTENT_DARK", sectionTitle: "Conclusion" });
  s.addText("Conclusion", { placeholder: "title" });
  const concl = [
    ["Real problem", "Unsafe PM2.5 on most days in the Terai and almost half of days in Kathmandu"],
    ["Two big-data tools", "Kafka (ingestion, replay, pub/sub) + Spark (streaming, SQL, MLlib)"],
    ["Real-time + history", "Alerts in seconds; one year analysed from a partitioned data lake"],
    ["Early warning", `6-hour PM2.5 forecast, ${impr}% lower error than persistence`],
  ];
  concl.forEach(([h, b], i) => {
    const x = 0.6 + (i % 2) * 6.15, y = 1.5 + Math.floor(i / 2) * 2.2;
    s.addShape(pres.shapes.ROUNDED_RECTANGLE, { x, y, w: 5.95, h: 1.95, rectRadius: 0.12, fill: { color: C.text2 }, line: { type: "none" }, objectName: `conclusion ${i + 1} card` });
    s.addText(h, { x: x + 0.35, y: y + 0.25, w: 5.3, h: 0.5, fontSize: 20, bold: true, color: C.accent1, margin: 0, isTextBox: true, objectName: `conclusion ${i + 1} head` });
    s.addText(b, { x: x + 0.35, y: y + 0.8, w: 5.3, h: 1.0, fontSize: 15, color: C.background1, margin: 0, valign: "top", isTextBox: true, objectName: `conclusion ${i + 1} body` });
  });
  s.addText("धन्यवाद · Thank you. Questions?", { x: 0.6, y: 6.15, w: 12.1, h: 0.7, fontSize: 26, bold: true, color: C.background1, align: "center", margin: 0, isTextBox: true, objectName: "thanks" });
  s.addNotes("Close with the impact: with ground-sensor fusion and SMS delivery this could serve as a national early-warning system.");

  await pres.writeFile({ fileName: OUT });
  if (APPLY_THEME) {
    const { applyTheme } = require(APPLY_THEME);
    await applyTheme(OUT, THEME);
  }
  console.log("saved", OUT);
})();
