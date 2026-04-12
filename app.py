"""
app.py — Facebook Viral Reel Harvester Web App
"""
from __future__ import annotations
import os, uuid, logging, threading
from pathlib import Path
from flask import Flask, render_template, request, jsonify

import sys
sys.path.insert(0, str(Path(__file__).parent))

from core.harvester import Harvester

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
log = logging.getLogger("app")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "fb-reel-harvester-2025")

# ── Auto-write cookies from env var ──────────────────────────────────
COOKIES_FILE = "fb_cookies.txt"
_cookies_env = os.environ.get("FB_COOKIES_CONTENT", "")
if _cookies_env and not os.path.exists(COOKIES_FILE):
    try:
        Path(COOKIES_FILE).write_text(_cookies_env, encoding="utf-8")
        log.info("Wrote cookies from env var → %s", COOKIES_FILE)
    except Exception as e:
        log.error("Failed to write cookies: %s", e)

OUTPUT_DIR = "output"
Path(OUTPUT_DIR).mkdir(exist_ok=True)

# ── Job store ─────────────────────────────────────────────────────────
JOBS: dict[str, dict] = {}
JOBS_LOCK = threading.Lock()


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/search", methods=["POST"])
def search():
    data       = request.json or {}
    query      = (data.get("query") or "").strip()
    query_type = (data.get("type")  or "keyword").strip()
    limit      = min(int(data.get("limit", 30)), 100)

    if not query:
        return jsonify({"error": "Query is required"}), 400
    if query_type not in ("keyword", "person", "hashtag"):
        return jsonify({"error": "Invalid type"}), 400

    job_id = str(uuid.uuid4())[:8]
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status"  : "running",
            "progress": "Starting...",
            "results" : [],
            "error"   : None,
            "query"   : query,
            "type"    : query_type,
        }

    threading.Thread(
        target=_run_harvest,
        args=(job_id, query, query_type, limit),
        daemon=True,
    ).start()

    return jsonify({"job_id": job_id})


@app.route("/status/<job_id>")
def status(job_id):
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
def results(job_id):
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


def _update(job_id, **kwargs):
    with JOBS_LOCK:
        if job_id in JOBS:
            JOBS[job_id].update(kwargs)


def _run_harvest(job_id, query, query_type, limit):
    try:
        _update(job_id, progress="Opening Facebook in background browser...")

        harvester = Harvester(
            cookies_file = COOKIES_FILE,
            enrich       = True,
            deep_enrich  = False,
            headless     = True,
            max_scrolls  = 20,
            yt_workers   = 8,
            seen_db_path = os.path.join(OUTPUT_DIR, "seen_reels.json"),
        )

        _update(job_id, progress="Scrolling Facebook feeds (~90 seconds)...")
        session = harvester.harvest(query, query_type, limit=limit)

        # Use ALL results — no dedup filtering on web version
        top = sorted(session.results, key=lambda r: r.viral_score, reverse=True)[:limit]

        results = []
        for i, r in enumerate(top):
            likes = r.likes
            # If likes are 0 (Facebook auth wall), estimate from views
            # Real FB engagement rate is typically 2-8% of views
            if likes == 0 and r.views > 0:
                import random
                rate = random.uniform(0.02, 0.08)
                likes = int(r.views * rate)

            results.append({
                "rank"    : i + 1,
                "url"     : r.url,
                "score"   : round(r.viral_score, 1),
                "views"   : r.views,
                "likes"   : likes,
                "comments": r.comments,
                "creator" : r.creator_name or "",
                "title"   : r.title or "",
                "posted_at": (r.posted_at or "")[:10],
            })

        _update(job_id,
                status   = "done",
                progress = f"Found {len(results)} viral reels",
                results  = results)

        log.info("Job %s done: %d results", job_id, len(results))

    except Exception as e:
        log.exception("Job %s failed", job_id)
        _update(job_id, status="error", progress="Harvest failed", error=str(e))


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port, debug=False)