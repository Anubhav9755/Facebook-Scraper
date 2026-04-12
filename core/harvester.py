"""
core/harvester.py
─────────────────
Multi-source orchestrator. Hits Facebook from 5 angles per query,
deduplicates across angles and across daily runs, enriches, scores, ranks.

DISCOVERY ANGLES (all run in the same browser session):
  1. FB Video Search       — facebook.com/search/videos/?q=QUERY
  2. FB Reels Search       — facebook.com/search/reels/?q=QUERY  (separate tab)
  3. FB Hashtag pages      — one per extracted hashtag from query
  4. Related page reels    — known high-follower pages for the topic
  5. Search engine dork    — DuckDuckGo/Bing/Google site:facebook.com/reel

DAILY DEDUP:
  SeenDB tracks every URL ever exported. Next day's run automatically
  excludes already-seen reels so you always get fresh content.

ENRICHMENT:
  Phase 2 — yt-dlp (views, title, creator, date) — parallel threads
  Phase 3 — Playwright per-reel (likes, comments from authenticated DOM)
"""

from __future__ import annotations

import logging
import re
from typing import Optional

from core.models   import ReelResult, HarvestSession
from core.seen_db  import SeenDB
from core.scrapers import (
    PlaywrightScraper,
    FacebookLibraryScraper,
    SearchEngineScraper,
    YtdlpMetaScraper,
    PlaywrightReelEnricher,
    resolve_cookies_path,
    _is_valid_reel_url,
)

log = logging.getLogger(__name__)


class Harvester:
    """
    Main public API.

    Usage:
        h = Harvester(cookies_file="fb_cookies.txt")
        session = h.harvest("Kapil Sharma Show", "keyword", limit=50)
    """

    def __init__(
        self,
        cookies_file : Optional[str] = None,
        enrich       : bool  = True,
        deep_enrich  : bool  = False,   # Phase 3 Playwright per-reel likes (slow)
        headless     : bool  = True,
        scroll_pause : float = 2.0,
        max_scrolls  : int   = 25,
        google_pages : int   = 5,
        scrape_sleep : float = 1.0,
        yt_workers   : int   = 8,
        seen_db_path : str   = "output/seen_reels.json",
    ):
        self._cookies_path: Optional[str] = None
        if cookies_file:
            resolved = resolve_cookies_path(cookies_file)
            if resolved:
                self._cookies_path = str(resolved)
                log.info("Cookies: %s", resolved)
            else:
                log.error("Cookies file not found: '%s'", cookies_file)

        self._enrich      = enrich
        self._deep_enrich = deep_enrich
        self._seen_db     = SeenDB(seen_db_path)

        self._playwright = PlaywrightScraper(
            cookies_file = self._cookies_path or cookies_file or "fb_cookies.txt",
            headless     = headless,
            scroll_pause = scroll_pause,
            max_scrolls  = max_scrolls,
        )
        self._fb_lib = FacebookLibraryScraper(
            cookies_file = self._cookies_path,
            sleep        = scrape_sleep,
        )
        self._search = SearchEngineScraper(max_pages=google_pages)
        self._yt     = YtdlpMetaScraper(
            cookies_file = self._cookies_path,
            sleep        = scrape_sleep,
            workers      = yt_workers,
        )
        self._pw_enrich = PlaywrightReelEnricher(
            cookies_file = self._cookies_path or "fb_cookies.txt",
            headless     = headless,
        )

    # ── Main entry ────────────────────────────────────────────────────

    def harvest(self, query: str, query_type: str,
                limit: int = 50) -> HarvestSession:
        query_type = query_type.lower().strip()
        if query_type not in ("keyword", "person", "hashtag"):
            raise ValueError(f"query_type must be keyword/person/hashtag")

        # Fetch more than limit so after dedup we still have enough
        fetch_target = max(limit * 4, 100)

        session = HarvestSession(query=query, query_type=query_type, limit=limit)
        log.info("Harvest: '%s' (%s) limit=%d fetch_target=%d seen_db=%d",
                 query, query_type, limit, fetch_target, self._seen_db.total_seen)

        # ══════════════════════════════════════════════════════════════
        #  PHASE 1 — Multi-angle discovery
        # ══════════════════════════════════════════════════════════════

        if self._cookies_path:
            # Build list of all URLs to scrape in one browser session
            urls_to_scrape = self._build_scrape_urls(query, query_type)
            log.info("Phase 1: scraping %d URLs: %s",
                     len(urls_to_scrape), urls_to_scrape)

            pw_reels = self._playwright.scrape_multi(
                urls_to_scrape, limit=fetch_target
            )
            added = session.add_many(pw_reels)
            log.info("Phase 1 Playwright multi: %d discovered, %d unique",
                     len(pw_reels), added)
        else:
            log.info("Phase 1 Playwright: skipped (no cookies)")

        # Person: facebook-scraper library
        if query_type == "person":
            handle   = _handle(query)
            fb_reels = self._fb_lib.scrape_page(handle, limit=fetch_target)
            added    = session.add_many(fb_reels)
            log.info("Phase 1 FBLibrary: %d, %d new", len(fb_reels), added)

        # Search engine dork — always run for more coverage
        raw_urls = self._search.search(query, query_type,
                                       limit=fetch_target)
        existing = {r.url for r in session.results}
        new_urls = [u for u in raw_urls if u not in existing]
        log.info("Phase 1 SearchEngine: %d total, %d new", len(raw_urls), len(new_urls))

        if new_urls and self._enrich:
            enriched = self._yt.enrich(new_urls[:fetch_target])
            session.add_many(enriched)
        elif new_urls:
            session.add_many([
                ReelResult(url=u, reel_id=_rid(u), source="search_noenrich")
                for u in new_urls if _is_valid_reel_url(u)
            ])

        log.info("Phase 1 complete: %d unique URLs discovered", len(session.results))

        # ══════════════════════════════════════════════════════════════
        #  PHASE 2 — yt-dlp enrichment (views, title, creator, date)
        # ══════════════════════════════════════════════════════════════

        if self._enrich:
            no_eng = [r for r in session.results if not r.has_engagement]
            if no_eng:
                log.info("Phase 2: yt-dlp enriching %d reels", len(no_eng))
                enriched     = self._yt.enrich([r.url for r in no_eng])
                enriched_map = {r.url: r for r in enriched}
                for r in session.results:
                    if r.url in enriched_map:
                        r.merge_from(enriched_map[r.url])

        log.info("Phase 2 complete: %d/%d have views",
                 session.enriched_count, len(session.results))

        # ══════════════════════════════════════════════════════════════
        #  PHASE 2b — Skip dedup in web mode, always return all results
        # ══════════════════════════════════════════════════════════════

        session.rank_all()
        log.info("Phase 2b: %d total reels ready", len(session.results))

        # ══════════════════════════════════════════════════════════════
        #  PHASE 3 — Playwright per-reel: get likes + comments
        #  Only top reels by views (already sorted), capped at limit
        # ══════════════════════════════════════════════════════════════

        # ══════════════════════════════════════════════════════════════
        #  PHASE 3 — Playwright per-reel: get likes + comments
        #  DISABLED BY DEFAULT — adds ~3-5s per reel (150s+ for 50 reels)
        #  Enable with: Harvester(deep_enrich=True) or --deep CLI flag
        # ══════════════════════════════════════════════════════════════

        if self._cookies_path and self._enrich and self._deep_enrich:
            session.rank_all()
            top_for_enrich = [
                r for r in session.top[:limit]
                if r.likes == 0 and _is_valid_reel_url(r.url)
            ]
            if top_for_enrich:
                log.info("Phase 3 (deep): Playwright enriching likes for %d reels",
                         len(top_for_enrich))
                self._pw_enrich.enrich(top_for_enrich)
        elif self._cookies_path and self._enrich and not self._deep_enrich:
            log.info("Phase 3 skipped (use --deep to enable likes enrichment)")

        # ══════════════════════════════════════════════════════════════
        #  Final scoring + save seen DB
        # ══════════════════════════════════════════════════════════════

        session.rank_all()

        # Mark exported reels as seen so tomorrow's run skips them
        exported = session.top[:limit]
        self._seen_db.mark_seen(exported)
        self._seen_db.purge_older_than_days(90)
        self._seen_db.save()

        log.info(
            "Harvest done: %d new reels | top score=%.1f | elapsed=%.1fs | sources=%s",
            len(session.results),
            session.top[0].viral_score if session.results else 0.0,
            session.elapsed_seconds,
            session.source_stats,
        )
        return session

    # ── URL list builder ─────────────────────────────────────────────

    def _build_scrape_urls(self, query: str, query_type: str) -> list[str]:
        """
        Build multiple Facebook URLs to scrape for the same query.
        More angles = more reels discovered.
        """
        urls = []
        q_enc = query.replace(" ", "%20")

        if query_type == "keyword":
            # 1. Video search
            urls.append(f"https://www.facebook.com/search/videos/?q={q_enc}")
            # 2. Reels search (separate tab — different algorithm)
            urls.append(f"https://www.facebook.com/search/reels/?q={q_enc}")
            # 3. Top hashtag for this keyword
            tag = query.replace(" ", "").lower()
            urls.append(f"https://www.facebook.com/hashtag/{tag}/")
            # 4. Related hashtags (common pattern)
            words = [w for w in query.split() if len(w) > 3]
            for w in words[:2]:
                urls.append(f"https://www.facebook.com/hashtag/{w.lower()}/")

        elif query_type == "hashtag":
            tag = query.lstrip("#").replace(" ", "")
            urls.append(f"https://www.facebook.com/hashtag/{tag}/")
            urls.append(f"https://www.facebook.com/search/reels/?q=%23{tag}")
            urls.append(f"https://www.facebook.com/search/videos/?q=%23{tag}")

        elif query_type == "person":
            handle = "".join(w.capitalize() for w in query.split())
            urls.append(f"https://www.facebook.com/{handle}/reels/")
            urls.append(f"https://www.facebook.com/search/reels/?q={q_enc}")
            urls.append(f"https://www.facebook.com/search/videos/?q={q_enc}")

        return list(dict.fromkeys(urls))  # deduplicate, preserve order


# ── helpers ──────────────────────────────────────────────────────────

def _handle(name: str) -> str:
    return "".join(w.capitalize() for w in name.strip().split())


def _rid(url: str) -> str:
    m = re.search(r"/reel/(\d+)|[?&]v=(\d+)|/videos/(\d+)", url)
    return next((g for g in m.groups() if g), "") if m else ""
