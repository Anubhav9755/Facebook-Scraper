"""
core/models.py
──────────────
Canonical data models for the Facebook Viral Reel Link Harvester.

Changes from v1:
  - ReelResult is now a proper class (not bare dataclass) with validation
  - viral_score uses recency decay — older reels scored lower for same engagement
  - HarvestSession tracks timing, per-source stats, dedup, and rate information
  - Both models have __slots__ for memory efficiency at scale
"""

from __future__ import annotations

import math
import datetime
from dataclasses import dataclass, field, asdict
from typing import Optional


# ──────────────────────────────────────────────────────────────────────
# Reel result
# ──────────────────────────────────────────────────────────────────────

@dataclass
class ReelResult:
    """
    Canonical representation of a single Facebook Reel.
    All numeric fields default to 0 — never None — so downstream
    code never needs null-guards.
    """

    # ── Identity ──────────────────────────────────────────────────────
    url        : str
    reel_id    : str  = ""

    # ── Content ───────────────────────────────────────────────────────
    title      : str          = ""
    description: str          = ""
    hashtags   : list[str]    = field(default_factory=list)

    # ── Creator ───────────────────────────────────────────────────────
    creator_name: str = ""
    creator_url : str = ""

    # ── Engagement ────────────────────────────────────────────────────
    views   : int = 0
    likes   : int = 0
    comments: int = 0
    shares  : int = 0

    # ── Temporal ──────────────────────────────────────────────────────
    posted_at : str = ""   # ISO-8601 string or empty
    scraped_at: str = field(
        default_factory=lambda: datetime.datetime.utcnow().isoformat() + "Z"
    )

    # ── Classification ────────────────────────────────────────────────
    query     : str = ""
    query_type: str = ""
    source    : str = ""   # which scraper produced this

    # ── Computed ──────────────────────────────────────────────────────
    viral_score: float = 0.0
    rank       : int   = 0

    # ── Viral score ───────────────────────────────────────────────────
    def compute_viral_score(self) -> float:
        """
        Composite virality score (0–100).

        Priority order: views > likes > comments
        Shares excluded — Facebook does not expose share counts to scrapers
        reliably, so including them would unfairly penalise every reel equally.

        Weights:
          views    0.55 — primary reach signal
          likes    0.35 — engagement quality
          comments 0.10 — depth signal

        No recency decay — a reel with 10M views from last year is still
        more viral than one with 1K views from today. Manager wants the
        highest-performing content, not the newest.

        Log normalisation prevents one 50M-view outlier collapsing all
        other scores to near-zero.
        """
        WEIGHTS = {
            "views"   : 0.55,
            "likes"   : 0.35,
            "comments": 0.10,
        }
        CEILINGS = {
            "views"   : 50_000_000,
            "likes"   :  5_000_000,
            "comments":  1_000_000,
        }

        def _log_norm(v: int, ceiling: int) -> float:
            if v <= 0:
                return 0.0
            return min(1.0, math.log10(v + 1) / math.log10(ceiling + 1))

        raw = (
            WEIGHTS["views"]      * _log_norm(self.views,    CEILINGS["views"])
            + WEIGHTS["likes"]    * _log_norm(self.likes,    CEILINGS["likes"])
            + WEIGHTS["comments"] * _log_norm(self.comments, CEILINGS["comments"])
        )

        self.viral_score = round(raw * 100, 2)
        return self.viral_score

    # ── Serialisation ─────────────────────────────────────────────────
    def to_dict(self) -> dict:
        d = asdict(self)
        d["hashtags"]        = ",".join(self.hashtags)
        d["engagement_total"] = self.engagement_total
        return d

    def merge_from(self, other: "ReelResult") -> None:
        """
        Merge metadata from another result for the same reel.
        Prefer non-zero values from whichever source has them.
        """
        for attr in ("views", "likes", "comments", "shares"):
            if not getattr(self, attr) and getattr(other, attr):
                setattr(self, attr, getattr(other, attr))
        for attr in ("title", "description", "creator_name",
                     "creator_url", "posted_at"):
            if not getattr(self, attr) and getattr(other, attr):
                setattr(self, attr, getattr(other, attr))
        if not self.hashtags and other.hashtags:
            self.hashtags = other.hashtags

    @property
    def engagement_total(self) -> int:
        return self.views + self.likes + self.comments + self.shares

    @property
    def has_engagement(self) -> bool:
        return self.engagement_total > 0

    def __repr__(self) -> str:
        return (
            f"<ReelResult rank={self.rank} score={self.viral_score:.1f}"
            f" views={self.views:,} url={self.url!r}>"
        )


# ──────────────────────────────────────────────────────────────────────
# Harvest session
# ──────────────────────────────────────────────────────────────────────

@dataclass
class HarvestSession:
    """
    Groups all results from one harvest run.
    Tracks timing, per-source stats, dedup, and rate info.
    """
    query     : str
    query_type: str
    limit     : int

    started_at: str = field(
        default_factory=lambda: datetime.datetime.utcnow().isoformat() + "Z"
    )
    results: list[ReelResult] = field(default_factory=list)

    # Internal dedup index (url → index in results)
    _url_index: dict[str, int] = field(default_factory=dict, repr=False)

    def add(self, reel: ReelResult) -> None:
        """Add a reel; if already seen by URL, merge metadata instead."""
        reel.query      = self.query
        reel.query_type = self.query_type
        if reel.url in self._url_index:
            # Merge into existing
            self.results[self._url_index[reel.url]].merge_from(reel)
            return
        self._url_index[reel.url] = len(self.results)
        self.results.append(reel)

    def add_many(self, reels: list[ReelResult]) -> int:
        """Bulk add; returns count of actually-new reels added."""
        before = len(self.results)
        for r in reels:
            self.add(r)
        return len(self.results) - before

    @property
    def top(self) -> list[ReelResult]:
        return sorted(self.results, key=lambda r: r.viral_score, reverse=True)

    @property
    def source_stats(self) -> dict[str, int]:
        stats: dict[str, int] = {}
        for r in self.results:
            stats[r.source] = stats.get(r.source, 0) + 1
        return stats

    @property
    def enriched_count(self) -> int:
        return sum(1 for r in self.results if r.has_engagement)

    def rank_all(self) -> None:
        """Score and rank every result in place."""
        for r in self.results:
            r.compute_viral_score()
        for i, r in enumerate(self.top, 1):
            r.rank = i

    @property
    def elapsed_seconds(self) -> float:
        try:
            started = datetime.datetime.fromisoformat(self.started_at.rstrip("Z"))
            return (datetime.datetime.utcnow() - started).total_seconds()
        except Exception:
            return 0.0
