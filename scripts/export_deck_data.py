"""Export MongoDB results to docs/deck_data.json for the slide generator."""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pymongo import MongoClient  # noqa: E402

from common.config import MONGO_DB, MONGO_URI  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
db = MongoClient(MONGO_URI)[MONGO_DB]
lake = list((ROOT / "data" / "lake" / "air_quality").rglob("*.parquet"))
tp = list(db.test_predictions.find({}, {"_id": 0}).sort("event_ts", 1))[-24 * 14:]
data = {
    "exceedance": list(db.exceedance.find({}, {"_id": 0}).sort("annual_mean", -1)),
    "model": db.model_metrics.find_one({}, {"_id": 0, "trained_at": 0}),
    "readings": db.readings.estimated_document_count(),
    "alerts": db.alerts.estimated_document_count(),
    "lake_files": len(lake),
    "lake_mb": round(sum(f.stat().st_size for f in lake) / 1e6, 1),
    "corr": list(db.weather_corr.find({}, {"_id": 0})),
    "test_predictions": tp,
}
(ROOT / "docs" / "deck_data.json").write_text(json.dumps(data, default=str, indent=1))
print("exported", {k: (v if not isinstance(v, list) else len(v)) for k, v in data.items() if k != "model"})
