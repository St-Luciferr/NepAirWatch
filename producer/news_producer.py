"""
NepAirWatch — news headline stream (unstructured text for text analytics).

Polls Google News RSS searches about air pollution, haze and forest fires in Nepal and publishes
each new article as a JSON message on Kafka topic `nepal.news.raw`:
  {id, title, link, publisher, published, query, ingest_time}
Spark tags the headlines in the stream and runs tokenisation / term counts / LDA topics in batch.

Examples
  python -m producer.news_producer live --interval 1800
  python -m producer.news_producer once
"""
import argparse
import hashlib
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from common.config import TOPIC_NEWS
from producer.common import build_producer, http_get, load_state, now_utc_iso, publish, save_state

RSS = "https://news.google.com/rss/search"
QUERIES = [
    "Nepal air pollution",
    "Kathmandu air quality",
    "Kathmandu smog OR haze",
    "Nepal forest fire",
    "Nepal brick kiln pollution",
    "Terai air pollution India Nepal",
]


def fetch(query):
    xml = http_get(RSS, {"q": query, "hl": "en-US", "gl": "US", "ceid": "US:en"}, as_json=False, retries=3)
    items = []
    for it in ET.fromstring(xml).iter("item"):
        title = (it.findtext("title") or "").strip()
        link = (it.findtext("link") or "").strip()
        if not title or not link:
            continue
        publisher = (it.findtext("source") or "").strip()
        if publisher and title.endswith(f" - {publisher}"):
            title = title[: -len(publisher) - 3]
        try:
            published = parsedate_to_datetime(it.findtext("pubDate")).astimezone(timezone.utc)
        except (TypeError, ValueError):
            published = datetime.now(timezone.utc)
        items.append({"id": hashlib.sha1(link.encode()).hexdigest()[:16], "title": title, "link": link,
                      "publisher": publisher, "published": published.isoformat(timespec="seconds"),
                      "query": query})
    return items


def poll(producer, seen):
    n = 0
    for q in QUERIES:
        try:
            items = fetch(q)
        except (RuntimeError, ET.ParseError) as e:
            print(f"  ! {q}: {e}", flush=True)
            continue
        for item in items:
            if item["id"] in seen:
                continue
            publish(producer, TOPIC_NEWS, item["id"], {**item, "ingest_time": now_utc_iso()})
            seen[item["id"]] = item["published"][:10]
            n += 1
    producer.flush()
    # keep the de-duplication state bounded
    if len(seen) > 20_000:
        seen = dict(sorted(seen.items(), key=lambda kv: kv[1])[-10_000:])
    save_state("news_seen", seen)
    return n, seen


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="mode", required=True)
    lv = sub.add_parser("live")
    lv.add_argument("--interval", type=int, default=1800)
    sub.add_parser("once")
    a = ap.parse_args()
    producer, seen = build_producer(), load_state("news_seen", {})
    while True:
        n, seen = poll(producer, seen)
        print(f"[news] {datetime.now():%H:%M:%S}  +{n} new articles", flush=True)
        if a.mode == "once":
            break
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
