"""Tiny storage abstraction for Python (non-Spark) code: the same call works on the local
file system and on HDFS (via the WebHDFS REST API, so no native Hadoop client is needed).

Spark itself talks to HDFS natively through the `hdfs://` URI.
"""
from pathlib import Path
from urllib.parse import urlparse

import requests

from common.config import HDFS_USER, WEBHDFS_URL


def _is_hdfs(path: str) -> bool:
    return str(path).startswith("hdfs://")


def _webhdfs(path: str) -> str:
    return f"{WEBHDFS_URL}/webhdfs/v1{urlparse(path).path}"


def write_text(path: str, text: str):
    """Create or overwrite a file. Parent directories are created automatically."""
    if _is_hdfs(path):
        # WebHDFS two-step create: the NameNode answers 307 with the DataNode URL to stream to.
        r = requests.put(_webhdfs(path), params={"op": "CREATE", "overwrite": "true", "user.name": HDFS_USER},
                         allow_redirects=False, timeout=60)
        r.raise_for_status()
        r = requests.put(r.headers["Location"], data=text.encode(), timeout=300)
        r.raise_for_status()
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        Path(path).write_text(text)


def exists(path: str) -> bool:
    if _is_hdfs(path):
        r = requests.get(_webhdfs(path), params={"op": "GETFILESTATUS", "user.name": HDFS_USER}, timeout=30)
        return r.status_code == 200
    return Path(path).exists()


def summary(path: str):
    """(bytes, files, directories) under a path; None if it does not exist / is unreachable."""
    try:
        if _is_hdfs(path):
            r = requests.get(_webhdfs(path), params={"op": "GETCONTENTSUMMARY", "user.name": HDFS_USER},
                             timeout=30)
            if r.status_code != 200:
                return None
            s = r.json()["ContentSummary"]
            return s["length"], s["fileCount"], s["directoryCount"]
        p = Path(path)
        if not p.exists():
            return None
        files = [f for f in p.rglob("*") if f.is_file()]
        dirs = [d for d in p.rglob("*") if d.is_dir()]
        return sum(f.stat().st_size for f in files), len(files), len(dirs)
    except (requests.RequestException, OSError, KeyError, ValueError):
        return None
