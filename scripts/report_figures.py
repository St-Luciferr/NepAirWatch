"""Render static figures for the report/slides from MongoDB results → docs/figures/*.png"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402
from pymongo import MongoClient  # noqa: E402

from common.config import MONGO_DB, MONGO_URI, NEPAL_PM25_24H, WHO_PM25_24H  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "docs" / "figures"
OUT.mkdir(parents=True, exist_ok=True)
db = MongoClient(MONGO_URI)[MONGO_DB]
plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                     "figure.dpi": 160})


def df(c, q=None):
    return pd.DataFrame(list(db[c].find(q or {}, {"_id": 0})))


# 1 exceedance
ex = df("exceedance").sort_values("annual_mean", ascending=False)
fig, ax = plt.subplots(figsize=(10, 4))
x = range(len(ex))
ax.bar([i - 0.2 for i in x], ex["days_over_who"], 0.4, label="Days > WHO 2021 (15 µg/m³)", color="#f5b041")
ax.bar([i + 0.2 for i in x], ex["days_over_nepal_std"], 0.4, label="Days > Nepal NAAQS (40 µg/m³)", color="#e74c3c")
ax.set_xticks(list(x), ex["city"], rotation=40, ha="right")
ax.set_ylabel("days in past year")
ax.set_title("Days breaching 24-h PM2.5 standards (Oct 2025 – Oct 2026)")
ax.legend(frameon=False)
fig.tight_layout(); fig.savefig(OUT / "exceedance.png"); plt.close(fig)

# 2 seasonality heatmap
mo = df("monthly_city")
piv = mo.pivot(index="city", columns="month", values="pm25_avg")
piv = piv.loc[piv.mean(axis=1).sort_values(ascending=False).index]
fig, ax = plt.subplots(figsize=(10, 5))
im = ax.imshow(piv.values, cmap="RdYlGn_r", aspect="auto")
ax.set_xticks(range(len(piv.columns)), piv.columns, rotation=45, ha="right")
ax.set_yticks(range(len(piv.index)), piv.index)
for i in range(piv.shape[0]):
    for j in range(piv.shape[1]):
        v = piv.values[i, j]
        if pd.notna(v):
            ax.text(j, i, f"{v:.0f}", ha="center", va="center", fontsize=7)
fig.colorbar(im, label="PM2.5 µg/m³")
ax.set_title("Seasonality: monthly mean PM2.5 by city")
fig.tight_layout(); fig.savefig(OUT / "seasonality.png"); plt.close(fig)

# 3 diurnal
hp = df("hourly_profile")
fig, ax = plt.subplots(figsize=(8, 4))
for c in ["Kathmandu", "Biratnagar", "Birgunj", "Pokhara", "Nepalgunj"]:
    s = hp[hp.city == c].sort_values("hour")
    ax.plot(s["hour"], s["pm25_avg"], marker="o", ms=3, label=c)
ax.set_xlabel("hour of day (NPT)"); ax.set_ylabel("mean PM2.5 µg/m³")
ax.set_title("Diurnal cycle of PM2.5")
ax.legend(frameon=False, ncol=3)
fig.tight_layout(); fig.savefig(OUT / "diurnal.png"); plt.close(fig)

# 4 weather correlation
wc = df("weather_corr")
piv = wc.pivot(index="driver", columns="region", values="pearson_r")
fig, ax = plt.subplots(figsize=(8, 4))
piv.plot.barh(ax=ax, color=["#5d6d7e", "#2e86c1", "#e67e22"])
ax.axvline(0, color="black", lw=0.8)
ax.set_xlabel("Pearson r with PM2.5"); ax.set_ylabel("")
ax.set_title("Weather drivers of PM2.5")
ax.legend(frameon=False)
fig.tight_layout(); fig.savefig(OUT / "weather_corr.png"); plt.close(fig)

# 5 forecast vs actual (Kathmandu hold-out)
tp = df("test_predictions")
tp["event_ts"] = pd.to_datetime(tp["event_ts"])
tp = tp.tail(24 * 21)
mm = db.model_metrics.find_one()
fig, ax = plt.subplots(figsize=(10, 3.8))
ax.plot(tp["event_ts"], tp["label"], color="#2c3e50", lw=1.2, label="actual PM2.5 (t+6 h)")
ax.plot(tp["event_ts"], tp["prediction"], color="#e67e22", lw=1.2, label="GBT forecast")
ax.axhline(NEPAL_PM25_24H, ls="--", color="#e74c3c", lw=0.8)
ax.set_ylabel("µg/m³")
ax.set_title(f"Kathmandu hold-out: 6-h forecast (RMSE {mm['rmse']} vs persistence {mm['baseline_rmse']})")
ax.legend(frameon=False)
fig.tight_layout(); fig.savefig(OUT / "forecast.png"); plt.close(fig)

# 6 feature importance
fi = pd.DataFrame(mm["feature_importance"]).sort_values("importance")
fig, ax = plt.subplots(figsize=(6, 4))
ax.barh(fi["feature"], fi["importance"], color="#2e86c1")
ax.set_title("GBT feature importance (top 10)")
fig.tight_layout(); fig.savefig(OUT / "feature_importance.png"); plt.close(fig)

# 7 Kathmandu year time series (daily)
ds = df("daily_stats", {"city": "Kathmandu"}).sort_values("day")
ds["day"] = pd.to_datetime(ds["day"])
fig, ax = plt.subplots(figsize=(10, 3.6))
ax.fill_between(ds["day"], ds["pm25_avg"], color="#c0392b", alpha=0.35)
ax.plot(ds["day"], ds["pm25_avg"], color="#c0392b", lw=1)
ax.axhline(NEPAL_PM25_24H, ls="--", color="black", lw=0.8, label="Nepal NAAQS 40")
ax.axhline(WHO_PM25_24H, ls=":", color="green", lw=1, label="WHO 15")
ax.set_ylabel("daily mean PM2.5 µg/m³")
ax.set_title("Kathmandu: daily mean PM2.5 over the past year (Spark streaming window output)")
ax.legend(frameon=False)
fig.tight_layout(); fig.savefig(OUT / "kathmandu_daily.png"); plt.close(fig)

# 8 CAMS vs measured: bias per ground station
cv = df("cams_validation").dropna(subset=["city"]).sort_values("bias")
if not cv.empty:
    fig, ax = plt.subplots(figsize=(8, 4.2))
    ax.barh(cv["station"] + " (" + cv["city"] + ")", cv["bias"],
            color=["#2e86c1" if b < 0 else "#e67e22" for b in cv["bias"]])
    ax.axvline(0, color="black", lw=0.8)
    ax.set_xlabel("bias = CAMS − measured PM2.5 (µg/m³)")
    ax.set_title("CAMS model vs DoEnv ground stations (positive = model over-estimates)")
    fig.tight_layout(); fig.savefig(OUT / "cams_bias.png"); plt.close(fig)

# 9 calibration: raw CAMS vs MLlib-calibrated on hold-out hours
cs = df("calibration_sample")
cm = db.calibration_metrics.find_one()
if not cs.empty and cm:
    fig, ax = plt.subplots(figsize=(6.2, 5))
    ax.scatter(cs["ground_pm25"], cs["cams_pm25"], s=7, alpha=0.4, color="#e67e22",
               label=f"raw CAMS (RMSE {cm['raw_rmse']})")
    ax.scatter(cs["ground_pm25"], cs["calibrated_pm25"], s=7, alpha=0.4, color="#2e86c1",
               label=f"RF-calibrated (RMSE {cm['rmse']})")
    mx = float(cs[["ground_pm25", "cams_pm25", "calibrated_pm25"]].max().max())
    ax.plot([0, mx], [0, mx], ls="--", color="grey", lw=1)
    ax.set_xlabel("measured PM2.5 (µg/m³)"); ax.set_ylabel("estimated PM2.5 (µg/m³)")
    ax.set_title("Calibration on hold-out station-hours (dashed = perfect)")
    ax.legend(frameon=False)
    fig.tight_layout(); fig.savefig(OUT / "calibration.png"); plt.close(fig)

# 10 25-year aerosol trend (NASA POWER, complete years only)
ay = df("aod_yearly")
if not ay.empty:
    ay = ay[ay["days"] >= 360]
    fig, ax = plt.subplots(figsize=(10, 3.8))
    for c in ["Kathmandu", "Biratnagar", "Nepalgunj", "Pokhara"]:
        s = ay[ay.city == c].sort_values("year")
        ax.plot(s["year"], s["aod_mean"], marker="o", ms=3, label=c)
    ax.set_ylabel("annual mean AOD (550 nm)")
    ax.set_title("25 years of aerosol optical depth (NASA POWER, HDFS raw zone → Spark)")
    ax.legend(frameon=False, ncol=4)
    fig.tight_layout(); fig.savefig(OUT / "aod_trend.png"); plt.close(fig)

print("figures →", OUT)
