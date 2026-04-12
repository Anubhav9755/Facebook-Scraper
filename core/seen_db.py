"""
core/seen_db.py
───────────────
Persistent deduplication database using a simple JSON file.

Tracks every reel URL ever returned across all runs.
On each new run, already-seen URLs are filtered out so you get
fresh viral reels every time you run the same query.

Storage: output/seen_reels.json  (auto-created, human-readable)

Usage:
    db = SeenDB()
    new_reels = db.filter_new(all_reels)   # removes already-seen
    db.mark_seen(new_reels)                # records them for next time
    db.save()
"""

from __future__ import annotations

import json
import datetime
import logging
from pathlib import Path

log = logging.getLogger(__name__)


class SeenDB:
    """
    Persistent set of reel URLs seen in previous runs.
    Thread-safe for single-process use.
    """

    def __init__(self, db_path: str = "output/seen_reels.json"):
        self._path   = Path(db_path)
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._seen   : dict[str, str] = {}   # url → first_seen ISO timestamp
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                data = json.loads(self._path.read_text(encoding="utf-8"))
                self._seen = data if isinstance(data, dict) else {}
                log.info("SeenDB: loaded %d known reels from %s",
                         len(self._seen), self._path)
            except Exception as e:
                log.warning("SeenDB load error: %s — starting fresh", e)
                self._seen = {}
        else:
            self._seen = {}
            log.info("SeenDB: new database (no previous runs found)")

    def save(self) -> None:
        try:
            self._path.write_text(
                json.dumps(self._seen, indent=2, ensure_ascii=False),
                encoding="utf-8"
            )
            log.info("SeenDB: saved %d total known reels", len(self._seen))
        except Exception as e:
            log.error("SeenDB save error: %s", e)

    def is_new(self, url: str) -> bool:
        return url not in self._seen

    def filter_new(self, reels) -> list:
        """Return only reels whose URL hasn't been seen before."""
        new = [r for r in reels if self.is_new(r.url)]
        skipped = len(reels) - len(new)
        if skipped:
            log.info("SeenDB: filtered %d already-seen reels, %d new",
                     skipped, len(new))
        return new

    def mark_seen(self, reels) -> None:
        now = datetime.datetime.utcnow().isoformat() + "Z"
        for r in reels:
            if r.url not in self._seen:
                self._seen[r.url] = now

    @property
    def total_seen(self) -> int:
        return len(self._seen)

    def purge_older_than_days(self, days: int = 90) -> int:
        """Remove entries older than N days to keep the DB lean."""
        cutoff = (datetime.datetime.utcnow() -
                  datetime.timedelta(days=days)).isoformat()
        before = len(self._seen)
        self._seen = {
            url: ts for url, ts in self._seen.items()
            if ts >= cutoff
        }
        removed = before - len(self._seen)
        if removed:
            log.info("SeenDB: purged %d entries older than %d days", removed, days)
        return removed
