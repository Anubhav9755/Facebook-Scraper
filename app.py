"""
app.py — Facebook Viral Reel Harvester Web App
Serves the UI and runs harvests via background threads.
"""

from __future__ import annotations

import os
import uuid
import logging
import threading
from pathlib import Path
from flask import Flask, render_template, request, jsonify, send_from_directory

# Add core to path so imports work
import sys
sys.path.insert(0, str(Path(__file__).parent))

from core.harvester import Harvester
from core.seen_db   import SeenDB

logging.basicConfig(level=logging.INFO,
                    format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("app")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "fb-reel-harvester-2025")

# ── Auto-write cookies from env var (for Railway/Render deployment) ──
# Set FB_COOKIES_CONTENT in Railway Variables with the full fb_cookies.txt content
_cookies_content = os.environ.get("FB_COOKIES_CONTENT", "")
COOKIES_FILE     = os.environ.get("FB_COOKIES_FILE", "fb_cookies.txt")

if _cookies_content and not os.path.exists(COOKIES_FILE):
    try:
        Path(COOKIES_FILE).write_text(_cookies_content, encoding="utf-8")
        log.info("Wrote cookies from FB_COOKIES_CONTENT env var → %s", COOKIES_FILE)
    except Exception as e:
        log.error("Failed to write cookies from env var: %s", e)
# In-memory: { job_id: { status, progress, results, error } }
JOBS: dict[str, dict] = {}
JOBS_LOCK = threading.Lock()

COOKIES_FILE = os.environ.get("FB_COOKIES_FILE", "fb_cookies.txt")
OUTPUT_DIR   = os.environ.get("OUTPUT_DIR", "output")
Path(OUTPUT_DIR).mkdir(exist_ok=True)


# ── Routes ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search():
    data       = request.json or {}
    query      = (data.get("query") or "").strip()
    query_type = (data.get("type")  or "keyword").strip()
    limit      = min(int(data.get("limit", 30)), 100)
    fresh      = bool(data.get("fresh", False))

    if not query:
        return jsonify({"error": "Query is required"}), 400
    if query_type not in ("keyword", "person", "hashtag"):
        return jsonify({"error": "Invalid type"}), 400

    job_id = str(uuid.uuid4())[:8]
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status"  : "running",
            "progress": "Starting harvest...",
            "results" : [],
            "error"   : None,
            "query"   : query,
            "type"    : query_type,
            "limit"   : limit,
        }

    thread = threading.Thread(
        target=_run_harvest,
        args=(job_id, query, query_type, limit, fresh),
        daemon=True,
    )
    thread.start()

    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "status"  : job["status"],
        "progress": job["progress"],
        "count"   : len(job["results"]),
        "error"   : job["error"],
    })


@app.route("/results/<job_id>")
def results(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
    if not job:
        return jsonify({"error": "Job not found"}), 404
    return jsonify({
        "status" : job["status"],
        "query"  : job["query"],
        "type"   : job["type"],
        "results": job["results"],
    })


# ── Background harvest ────────────────────────────────────────────────

def _update(job_id: str, **kwargs):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(kwargs)


def _run_harvest(job_id: str, query: str, query_type: str,
                 limit: int, fresh: bool):
    try:
        _update(job_id, progress="Opening Facebook in background browser...")

        seen_db_path = (
            str(Path(OUTPUT_DIR) / "seen_reels.json")
            if not fresh else "NUL"
        )

        harvester = Harvester(
            cookies_file = COOKIES_FILE,
            enrich       = True,
            deep_enrich  = False,   # keep fast
            headless     = True,
            max_scrolls  = 20,
            yt_workers   = 8,
            seen_db_path = seen_db_path,
        )

        _update(job_id, progress="Scrolling Facebook feeds (this takes ~90 seconds)...")
        session = harvester.harvest(query, query_type, limit=limit)

        _update(job_id, progress="Ranking results...")
        top = session.top[:limit]

        results = []
        for r in top:
            results.append({
                "rank"        : r.rank,
                "url"         : r.url,
                "score"       : round(r.viral_score, 1),
                "views"       : r.views,
                "likes"       : r.likes,
                "comments"    : r.comments,
                "creator"     : r.creator_name or "",
                "title"       : r.title or "",
                "posted_at"   : (r.posted_at or "")[:10],
                "hashtags"    : r.hashtags[:5],
                "source"      : r.source,
            })

        _update(job_id,
                status   = "done",
                progress = f"Found {len(results)} viral reels",
                results  = results)

        log.info("Job %s done: %d results", job_id, len(results))

    except Exception as e:
        log.exception("Job %s failed", job_id)
        _update(job_id,
                status   = "error",
                progress = "Harvest failed",
                error    = str(e))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
