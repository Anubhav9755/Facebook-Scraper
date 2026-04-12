"""
core/scrapers.py
────────────────
Production scraping strategies for Facebook Viral Reel Harvester.

FIXED IN THIS VERSION
──────────────────────
1. Junk URL filtering — bare /reel/ and /search/videos/ (no ID) are now
   rejected at _canonical() time, not silently stored.
2. Playwright scroll — waits for networkidle after each scroll so the
   feed actually renders before we read the DOM. More scrolls (30 default).
   Also intercepts Facebook's internal GraphQL/video API responses which
   contain reel IDs in JSON — catches reels that never appear as <a> hrefs.
3. Likes/shares enrichment — Facebook's reel page renders engagement
   numbers inside specific aria-label patterns. We now dump ALL aria-labels
   and span text and use broader regex to catch them. Also added a JS
   approach that reads the __bbox / __data JSON Facebook embeds in the page.
4. yt-dlp: bad/junk URLs are filtered before sending to yt-dlp so it
   doesn't waste time on /reel/ with no ID.
"""

from __future__ import annotations

import re
import os
import time
import json
import random
import logging
import urllib.parse
import urllib.request
import urllib.error
import ssl
import threading
from pathlib import Path
from typing import Optional

from core.models import ReelResult

log = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────
REEL_ID_RE = re.compile(r"/reel/(\d+)|[?&]v=(\d+)|/videos/(\d+)")
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_UA_LIST = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) "
    "Gecko/20100101 Firefox/125.0",
]


# ══════════════════════════════════════════════════════════════════════
#  Shared helpers
# ══════════════════════════════════════════════════════════════════════

def _reel_id(url: str) -> str:
    m = REEL_ID_RE.search(url)
    return next((g for g in m.groups() if g), "") if m else ""


def _canonical(url: str) -> str:
    """Return canonical reel URL, or empty string if URL has no valid reel ID."""
    rid = _reel_id(url)
    if rid:
        return f"https://www.facebook.com/reel/{rid}"
    return ""   # ← KEY FIX: return empty instead of junk URL


def _is_valid_reel_url(url: str) -> bool:
    """True only if URL contains a numeric reel ID."""
    return bool(_reel_id(url))


def _ssl_ctx() -> ssl.SSLContext:
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode    = ssl.CERT_NONE
    return ctx


def _http_get(url: str, timeout: int = 15, retries: int = 2) -> Optional[str]:
    for attempt in range(retries + 1):
        req = urllib.request.Request(url, headers={
            "User-Agent"     : random.choice(_UA_LIST),
            "Accept-Language": "en-US,en;q=0.9",
            "Accept"         : "text/html,*/*;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection"     : "keep-alive",
        })
        try:
            with urllib.request.urlopen(
                req, context=_ssl_ctx(), timeout=timeout
            ) as r:
                raw = r.read()
                try:
                    import gzip
                    return gzip.decompress(raw).decode("utf-8", errors="replace")
                except Exception:
                    return raw.decode("utf-8", errors="replace")
        except urllib.error.HTTPError as e:
            if e.code in (429, 503) and attempt < retries:
                time.sleep(2 ** attempt * random.uniform(2, 4))
                continue
            log.debug("GET %s → HTTP %s", url, e.code)
            return None
        except Exception as e:
            log.debug("GET %s → %s", url, e)
            if attempt < retries:
                time.sleep(1.5 * (attempt + 1))
    return None


def _parse_urls_from_html(html: str) -> list[str]:
    """Extract valid FB reel URLs from raw HTML. Returns only URLs with IDs."""
    found: list[str] = []
    # Direct reel URLs with numeric ID
    found += re.findall(r"https?://(?:www\.)?facebook\.com/reel/(\d+)", html)
    # Google redirect wrappers
    for raw in re.findall(r"/url\?q=(https?://[^&\"'<\s]+)", html):
        dec = urllib.parse.unquote(raw)
        m = REEL_ID_RE.search(dec)
        if m:
            rid = next((g for g in m.groups() if g), "")
            if rid:
                found.append(rid)
    # URL-encoded
    found += re.findall(r"facebook\.com%2Freel%2F(\d+)", html, re.I)
    # JSON-escaped
    found += re.findall(r"facebook\.com\\/reel\\/(\d+)", html)
    # Bare IDs from data attributes / JSON blobs
    found += re.findall(r'"reel_id"\s*:\s*"?(\d{10,})"?', html)
    found += re.findall(r'"video_id"\s*:\s*"?(\d{10,})"?', html)
    # Deduplicate and build canonical URLs
    seen = set()
    result = []
    for rid in found:
        if rid and rid not in seen:
            seen.add(rid)
            result.append(f"https://www.facebook.com/reel/{rid}")
    return result


def _parse_abbrev(s: str) -> int:
    """'1.2M' → 1_200_000, '450K' → 450_000, '2.3B' → 2_300_000_000."""
    s = s.replace(",", "").strip()
    try:
        if s.upper().endswith("B"):
            return int(float(s[:-1]) * 1_000_000_000)
        if s.upper().endswith("M"):
            return int(float(s[:-1]) * 1_000_000)
        if s.upper().endswith("K"):
            return int(float(s[:-1]) * 1_000)
        return int(float(s))
    except Exception:
        return 0


def resolve_cookies_path(path: str) -> Optional[Path]:
    """
    Auto-resolve cookies file path handling Windows double-extension trap.
    Tries 10 candidate paths and returns the first valid one.
    """
    p = Path(path)
    candidates = [
        p,
        Path(str(p) + ".txt"),
        Path(str(p) + ".txt.txt"),
        Path(str(p).removesuffix(".txt")),
        Path(str(p).removesuffix(".txt") + ".txt.txt"),
        Path.cwd() / p.name,
        Path.cwd() / (p.name + ".txt"),
        Path.cwd() / (p.name + ".txt.txt"),
        Path(__file__).parent.parent / p.name,
        Path(__file__).parent.parent / (p.name + ".txt"),
        Path(__file__).parent.parent / (p.name + ".txt.txt"),
    ]
    for candidate in candidates:
        if candidate.exists() and candidate.is_file():
            try:
                header = candidate.read_text(encoding="utf-8", errors="replace")[:60]
                if "#" in header or "facebook" in header.lower():
                    if candidate != p:
                        log.info("Cookies auto-resolved: '%s' → '%s'", path, candidate)
                    return candidate
            except Exception:
                return candidate
    return None


def _load_netscape_cookies(path: str) -> list[dict]:
    """
    Parse Netscape cookies.txt → Playwright-compatible dicts.
    Keeps leading dot in domain (.facebook.com) — required for cross-subdomain.
    Does NOT add 'url' key — Playwright needs domain OR url, not both.
    """
    real = resolve_cookies_path(path)
    if not real:
        return []
    cookies: list[dict] = []
    try:
        for line in real.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("\t")
            if len(parts) < 7:
                continue
            domain, _, path_, secure, expiry, name, value = parts[:7]
            cookie: dict = {
                "name"    : name.strip(),
                "value"   : value.strip(),
                "domain"  : domain.strip(),   # keep leading dot
                "path"    : path_.strip() or "/",
                "secure"  : secure.strip().upper() == "TRUE",
                "sameSite": "None",
            }
            try:
                exp = int(expiry.strip())
                if exp > 0:
                    cookie["expires"] = float(exp)
            except Exception:
                pass
            cookies.append(cookie)
    except Exception as e:
        log.error("Failed to parse cookies '%s': %s", real, e)
    log.info("Loaded %d cookies from '%s'", len(cookies), real)
    return cookies


def _pw_add_cookies(ctx, cookies: list[dict]) -> None:
    """
    Add cookies to a Playwright context safely.
    Uses domain-based injection (no url= key) which is correct for
    cross-subdomain cookies like .facebook.com.
    """
    ctx.add_cookies(cookies)


# ══════════════════════════════════════════════════════════════════════
#  Strategy 1 — Playwright (primary — requires cookies)
# ══════════════════════════════════════════════════════════════════════

class PlaywrightScraper:
    """
    Real Chromium browser with Facebook session cookies.
    Intercepts network responses (GraphQL/video API) to capture reel IDs
    that never appear as <a> hrefs in the feed DOM.
    """

    FB_SEARCH_URL  = "https://www.facebook.com/search/videos/?q={q}"
    FB_HASHTAG_URL = "https://www.facebook.com/hashtag/{tag}/"
    FB_PROFILE_URL = "https://www.facebook.com/{handle}/reels/"

    _STEALTH_JS = """
        Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
        Object.defineProperty(navigator, 'plugins', {get: () => [1,2,3,4,5]});
        Object.defineProperty(navigator, 'languages', {get: () => ['en-US','en','hi']});
        window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){}, app: {} };
        const origQuery = window.navigator.permissions.query;
        window.navigator.permissions.query = (p) =>
            p.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : origQuery(p);
    """

    def __init__(
        self,
        cookies_file : str,
        headless     : bool  = True,
        scroll_pause : float = 2.5,
        max_scrolls  : int   = 30,
        page_timeout : int   = 30_000,
    ):
        self._cookies_file = cookies_file
        self._headless     = headless
        self._scroll_pause = scroll_pause
        self._max_scrolls  = max_scrolls
        self._page_timeout = page_timeout
        self._available    = self._check_playwright()

    @staticmethod
    def _check_playwright() -> bool:
        try:
            from playwright.sync_api import sync_playwright  # noqa
            return True
        except ImportError:
            log.warning("playwright not installed.\n"
                        "  pip install playwright\n"
                        "  python -m playwright install chromium")
            return False

    def scrape(self, query: str, query_type: str,
               limit: int = 60) -> list[ReelResult]:
        if not self._available:
            return []

        cookies = _load_netscape_cookies(self._cookies_file)
        if not cookies:
            log.error("No cookies loaded from '%s'.", self._cookies_file)
            return []

        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
        results: list[ReelResult] = []
        # Collect reel IDs intercepted from network responses
        intercepted_ids: set[str] = set()

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=self._headless,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-notifications",
                        "--lang=en-US",
                    ],
                )
                ctx = browser.new_context(
                    user_agent         = _UA,
                    viewport           = {"width": 1366, "height": 768},
                    locale             = "en-IN",
                    timezone_id        = "Asia/Kolkata",
                    java_script_enabled= True,
                    extra_http_headers = {"Accept-Language": "en-IN,en;q=0.9,hi;q=0.8"},
                )
                _pw_add_cookies(ctx, cookies)

                page = ctx.new_page()
                page.add_init_script(self._STEALTH_JS)

                # ── Intercept API responses to grab reel IDs from JSON ──
                def _on_response(response):
                    try:
                        if "facebook.com" not in response.url:
                            return
                        ct = response.headers.get("content-type", "")
                        if "json" not in ct and "javascript" not in ct:
                            return
                        body = response.text()
                        # Extract reel / video IDs from GraphQL responses
                        for rid in re.findall(r'"video_id"\s*:\s*"?(\d{10,})"?', body):
                            intercepted_ids.add(rid)
                        for rid in re.findall(r'"reel_id"\s*:\s*"?(\d{10,})"?', body):
                            intercepted_ids.add(rid)
                        for rid in re.findall(r'/reel/(\d{10,})', body):
                            intercepted_ids.add(rid)
                    except Exception:
                        pass

                page.on("response", _on_response)

                url = self._build_url(query, query_type)
                log.info("PlaywrightScraper: %s", url)
                page.goto(url, wait_until="domcontentloaded",
                          timeout=self._page_timeout)
                time.sleep(random.uniform(2.5, 4.0))

                if any(x in page.url for x in ("login", "checkpoint", "recover")):
                    log.error("Facebook session expired — re-export cookies.")
                    browser.close()
                    return []

                self._dismiss_dialogs(page)
                results = self._scroll_and_collect(
                    page, limit, PWTimeout, intercepted_ids
                )
                browser.close()

        except Exception as e:
            log.error("PlaywrightScraper fatal: %s", e, exc_info=True)

        log.info("PlaywrightScraper: collected %d reels", len(results))
        return results

    def scrape_multi(self, urls: list[str],
                     limit: int = 150) -> list[ReelResult]:
        """
        Scrape multiple Facebook URLs in ONE browser session.
        Each URL gets its own scroll session; reel IDs are deduplicated globally.
        This is the key to getting 50-100+ reels instead of 10.
        """
        if not self._available:
            return []

        cookies = _load_netscape_cookies(self._cookies_file)
        if not cookies:
            log.error("No cookies — scrape_multi aborted")
            return []

        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout

        all_results   : list[ReelResult] = []
        global_seen   : set[str]         = set()
        intercepted   : set[str]         = set()

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=self._headless,
                    args=[
                        "--disable-blink-features=AutomationControlled",
                        "--no-sandbox",
                        "--disable-dev-shm-usage",
                        "--disable-notifications",
                        "--lang=en-US",
                    ],
                )
                ctx = browser.new_context(
                    user_agent         = _UA,
                    viewport           = {"width": 1366, "height": 768},
                    locale             = "en-IN",
                    timezone_id        = "Asia/Kolkata",
                    java_script_enabled= True,
                    extra_http_headers = {"Accept-Language": "en-IN,en;q=0.9,hi;q=0.8"},
                )
                _pw_add_cookies(ctx, cookies)

                # Network interceptor shared across all pages
                def _on_response(response):
                    try:
                        if "facebook.com" not in response.url:
                            return
                        ct = response.headers.get("content-type", "")
                        if "json" not in ct and "javascript" not in ct:
                            return
                        body = response.text()
                        for rid in re.findall(r'"video_id"\s*:\s*"?(\d{10,})"?', body):
                            intercepted.add(rid)
                        for rid in re.findall(r'"reel_id"\s*:\s*"?(\d{10,})"?', body):
                            intercepted.add(rid)
                        for rid in re.findall(r'/reel/(\d{10,})', body):
                            intercepted.add(rid)
                    except Exception:
                        pass

                # Per-URL scroll budget — spread limit across all URLs
                per_url_limit = max(20, limit // len(urls))

                for url_idx, url in enumerate(urls):
                    if len(all_results) >= limit:
                        break

                    log.info("scrape_multi [%d/%d]: %s", url_idx+1, len(urls), url)
                    remaining = limit - len(all_results)
                    page_limit = min(per_url_limit, remaining + 10)

                    try:
                        page = ctx.new_page()
                        page.on("response", _on_response)
                        page.add_init_script(self._STEALTH_JS)

                        page.goto(url, wait_until="domcontentloaded",
                                  timeout=self._page_timeout)
                        time.sleep(random.uniform(2.0, 3.5))

                        if any(x in page.url for x in ("login","checkpoint","recover")):
                            log.error("Session expired")
                            page.close()
                            break

                        if url_idx == 0:
                            self._dismiss_dialogs(page)

                        page_results = self._scroll_and_collect(
                            page, page_limit, PWTimeout, intercepted,
                            global_seen=global_seen
                        )
                        all_results.extend(page_results)
                        for r in page_results:
                            global_seen.add(r.url)

                        log.info("  → %d reels (running total: %d)",
                                 len(page_results), len(all_results))
                        page.close()

                    except Exception as e:
                        log.error("scrape_multi page error [%s]: %s", url, e)
                        try: page.close()
                        except Exception: pass

                    time.sleep(random.uniform(1.0, 2.0))

                # Add intercepted IDs that weren't caught by DOM scan
                for rid in intercepted:
                    canon = f"https://www.facebook.com/reel/{rid}"
                    if canon not in global_seen:
                        global_seen.add(canon)
                        all_results.append(ReelResult(
                            url=canon, reel_id=rid, source="playwright_intercept"
                        ))

                browser.close()

        except Exception as e:
            log.error("scrape_multi fatal: %s", e, exc_info=True)

        log.info("scrape_multi done: %d total reels from %d URLs",
                 len(all_results), len(urls))
        return all_results[:limit]

    def _build_url(self, query: str, query_type: str) -> str:
        if query_type == "hashtag":
            return self.FB_HASHTAG_URL.format(
                tag=urllib.parse.quote(query.lstrip("#"))
            )
        if query_type == "person":
            handle = urllib.parse.quote(
                "".join(w.capitalize() for w in query.split())
            )
            return self.FB_PROFILE_URL.format(handle=handle)
        return self.FB_SEARCH_URL.format(q=urllib.parse.quote(query))

    @staticmethod
    def _dismiss_dialogs(page) -> None:
        for sel in [
            '[data-testid="cookie-policy-manage-dialog-accept-button"]',
            '[aria-label="Close"]',
            '[aria-label="Decline optional cookies"]',
            'div[role="dialog"] button:has-text("OK")',
            'div[role="dialog"] button:has-text("Close")',
            'div[role="dialog"] button:has-text("Allow")',
        ]:
            try:
                btn = page.locator(sel).first
                if btn.is_visible(timeout=1000):
                    btn.click()
                    time.sleep(0.4)
            except Exception:
                pass

    def _scroll_and_collect(self, page, limit: int,
                             PWTimeout, intercepted_ids: set,
                             global_seen: Optional[set] = None) -> list[ReelResult]:
        seen          : set[str]         = set()
        results       : list[ReelResult] = []
        stale_scrolls : int              = 0
        last_height   : int              = 0

        for scroll_n in range(self._max_scrolls):
            if len(results) >= limit:
                break

            # ── Collect from DOM ──────────────────────────────────────
            html = page.content()
            reel_urls = _parse_urls_from_html(html)

            # Direct href extraction via JS
            try:
                hrefs = page.eval_on_selector_all(
                    "a[href*='/reel/']",
                    "els => els.map(e => e.href)"
                )
                for h in hrefs:
                    rid = _reel_id(h)
                    if rid:
                        reel_urls.append(f"https://www.facebook.com/reel/{rid}")
            except Exception:
                pass

            # ── Add intercepted network IDs ───────────────────────────
            for rid in intercepted_ids:
                reel_urls.append(f"https://www.facebook.com/reel/{rid}")

            # Deduplicate
            reel_urls = list(dict.fromkeys(reel_urls))
            new_this_scroll = 0

            for url in reel_urls:
                if not _is_valid_reel_url(url):
                    continue
                canon = f"https://www.facebook.com/reel/{_reel_id(url)}"
                if canon in seen:
                    continue
                # Also skip if seen in another URL's session (scrape_multi)
                if global_seen and canon in global_seen:
                    continue
                seen.add(canon)
                new_this_scroll += 1
                results.append(ReelResult(
                    url     = canon,
                    reel_id = _reel_id(canon),
                    source  = "playwright",
                ))
                if len(results) >= limit:
                    break

            log.debug("Scroll %d/%d  new=%d  total=%d  intercepted=%d",
                      scroll_n + 1, self._max_scrolls,
                      new_this_scroll, len(results), len(intercepted_ids))

            if new_this_scroll == 0:
                stale_scrolls += 1
                if stale_scrolls >= 4:
                    log.debug("4 stale scrolls — end of feed")
                    break
            else:
                stale_scrolls = 0

            # Mouse jitter
            try:
                page.mouse.move(random.randint(200, 900), random.randint(200, 600))
            except Exception:
                pass

            # Scroll
            scroll_px = random.randint(700, 1400)
            page.evaluate(f"window.scrollBy(0, {scroll_px})")

            # Wait for network to settle after scroll
            try:
                page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:
                pass

            pause = self._scroll_pause + random.uniform(-0.3, 0.5)
            time.sleep(max(0.8, pause))

            new_height = page.evaluate("document.documentElement.scrollHeight")
            if new_height == last_height and scroll_n > 5:
                log.debug("Height unchanged — end of page")
                break
            last_height = new_height

        return results[:limit]


# ══════════════════════════════════════════════════════════════════════
#  Strategy 2 — facebook-scraper library
# ══════════════════════════════════════════════════════════════════════

class FacebookLibraryScraper:
    def __init__(self, cookies_file: Optional[str] = None, sleep: float = 1.5):
        self._cookies   = cookies_file
        self._sleep     = sleep
        self._available = self._probe()

    @staticmethod
    def _probe() -> bool:
        try:
            import facebook_scraper  # noqa
            return True
        except ImportError:
            log.info("facebook-scraper not installed. Run: pip install facebook-scraper")
            return False

    def scrape_page(self, handle: str, limit: int = 60) -> list[ReelResult]:
        if not self._available or limit == 0:
            return []
        from facebook_scraper import get_posts
        results: list[ReelResult] = []
        kw: dict = {
            "pages"  : max(3, limit // 12),
            "options": {"posts_per_page": 50, "allow_extra_requests": True},
        }
        if self._cookies:
            resolved = resolve_cookies_path(self._cookies)
            if resolved:
                kw["cookies"] = str(resolved)
        try:
            for post in get_posts(handle, **kw):
                if not (post.get("video") or post.get("video_id")):
                    continue
                url = (post.get("post_url")
                       or (f"https://www.facebook.com/reel/{post['video_id']}"
                           if post.get("video_id") else None))
                if not url or not _is_valid_reel_url(url):
                    continue
                canon = f"https://www.facebook.com/reel/{_reel_id(url)}"
                text = (post.get("text") or "")[:500]
                results.append(ReelResult(
                    url          = canon,
                    reel_id      = _reel_id(canon),
                    title        = text[:120],
                    description  = text,
                    hashtags     = re.findall(r"#\w+", text),
                    creator_name = post.get("username") or handle,
                    creator_url  = f"https://www.facebook.com/{handle}",
                    views        = post.get("video_watches") or 0,
                    likes        = post.get("likes") or 0,
                    comments     = post.get("comments") or 0,
                    shares       = post.get("shares") or 0,
                    posted_at    = str(post.get("time") or ""),
                    source       = "fb_scraper_lib",
                ))
                if len(results) >= limit:
                    break
                time.sleep(self._sleep)
        except Exception as e:
            log.warning("FacebookLibraryScraper '%s': %s", handle, e)
        return results


# ══════════════════════════════════════════════════════════════════════
#  Strategy 3 — Search engine dork (URL discovery fallback)
# ══════════════════════════════════════════════════════════════════════

class SearchEngineScraper:
    def __init__(self, sleep_range: tuple = (1.5, 3.5), max_pages: int = 4):
        self._sleep = sleep_range
        self._pages = max_pages

    def search(self, query: str, query_type: str, limit: int = 40) -> list[str]:
        dork = self._build_dork(query, query_type)
        log.info("SearchEngineScraper dork: %r", dork)
        for engine_name, fetch_fn in [
            ("Google",     self._google),
            ("DuckDuckGo", self._duckduckgo),
            ("Bing",       self._bing),
            ("Yahoo",      self._yahoo),
        ]:
            try:
                urls = fetch_fn(dork, limit)
            except Exception as e:
                log.debug("%s error: %s", engine_name, e)
                urls = []
            if urls:
                log.info("%s: %d URLs", engine_name, len(urls))
                return list(dict.fromkeys(u for u in urls if _is_valid_reel_url(u)))[:limit]
            log.debug("%s: 0 results", engine_name)
            time.sleep(random.uniform(0.8, 1.6))
        log.info("SearchEngineScraper: all engines 0 results")
        return []

    def _google(self, dork: str, limit: int) -> list[str]:
        urls: list[str] = []
        for page in range(self._pages):
            q    = urllib.parse.urlencode({"q": dork, "start": page*10, "num": 10, "hl": "en"})
            html = _http_get(f"https://www.google.com/search?{q}")
            if not html: break
            if any(x in html.lower() for x in ("captcha", "unusual traffic", "i'm not a robot")):
                log.debug("Google CAPTCHA")
                break
            new = [u for u in _parse_urls_from_html(html) if u not in urls]
            urls += new
            if not new or len(urls) >= limit: break
            time.sleep(random.uniform(*self._sleep))
        return urls

    def _duckduckgo(self, dork: str, limit: int) -> list[str]:
        body = urllib.parse.urlencode({"q": dork, "kl": "in-en"}).encode()
        req  = urllib.request.Request(
            "https://html.duckduckgo.com/html/", data=body,
            headers={"User-Agent": random.choice(_UA_LIST),
                     "Content-Type": "application/x-www-form-urlencoded",
                     "Referer": "https://duckduckgo.com/"})
        try:
            with urllib.request.urlopen(req, context=_ssl_ctx(), timeout=15) as r:
                html = r.read().decode("utf-8", errors="replace")
            return _parse_urls_from_html(html)[:limit]
        except Exception:
            return []

    def _bing(self, dork: str, limit: int) -> list[str]:
        urls: list[str] = []
        for page in range(self._pages):
            q    = urllib.parse.urlencode({"q": dork, "first": page*10+1})
            html = _http_get(f"https://www.bing.com/search?{q}")
            if not html: break
            new = [u for u in _parse_urls_from_html(html) if u not in urls]
            urls += new
            if not new or len(urls) >= limit: break
            time.sleep(random.uniform(*self._sleep))
        return urls

    def _yahoo(self, dork: str, limit: int) -> list[str]:
        urls: list[str] = []
        for page in range(self._pages):
            q    = urllib.parse.urlencode({"p": dork, "b": page*10+1})
            html = _http_get(f"https://search.yahoo.com/search?{q}")
            if not html: break
            new = [u for u in _parse_urls_from_html(html) if u not in urls]
            urls += new
            if not new or len(urls) >= limit: break
            time.sleep(random.uniform(*self._sleep))
        return urls

    @staticmethod
    def _build_dork(query: str, query_type: str) -> str:
        q = query.lstrip("#")
        if query_type == "hashtag": return f'site:facebook.com/reel "#{q}"'
        if query_type == "person":  return f'site:facebook.com/reel "{q}"'
        return f"site:facebook.com/reel {q}"


# ══════════════════════════════════════════════════════════════════════
#  Strategy 4 — yt-dlp metadata enrichment
# ══════════════════════════════════════════════════════════════════════

class YtdlpMetaScraper:
    def __init__(self, cookies_file: Optional[str] = None,
                 sleep: float = 0.3, max_retries: int = 0, workers: int = 8):
        self._cookies     = cookies_file
        self._sleep       = sleep
        self._max_retries = max_retries
        self._workers     = workers

    def _ydl_opts(self) -> dict:
        opts: dict = {
            "quiet"         : True,
            "skip_download" : True,
            "no_warnings"   : True,
            "socket_timeout": 12,        # was 25 — fail fast
            "http_headers"  : {"User-Agent": _UA},
            "ignoreerrors"  : True,
            "extractor_retries": 0,      # no yt-dlp internal retries
        }
        if self._cookies:
            resolved = resolve_cookies_path(self._cookies)
            if resolved:
                opts["cookiefile"] = str(resolved)
        return opts

    def enrich(self, urls: list[str]) -> list[ReelResult]:
        valid_urls = [u for u in urls if _is_valid_reel_url(u)]
        if len(valid_urls) < len(urls):
            log.debug("yt-dlp: filtered %d junk URLs", len(urls) - len(valid_urls))

        try:
            import yt_dlp  # noqa
        except ImportError:
            log.error("yt-dlp missing. Install: pip install yt-dlp")
            return [ReelResult(url=u, reel_id=_reel_id(u), source="yt_dlp_missing")
                    for u in valid_urls]

        results: list[Optional[ReelResult]] = [None] * len(valid_urls)
        lock = threading.Lock()

        def _worker(idx: int, url: str) -> None:
            result = self._enrich_one(url)
            with lock:
                results[idx] = result

        # Launch all workers at once in batches of self._workers
        for batch_start in range(0, len(valid_urls), self._workers):
            batch = list(enumerate(valid_urls[batch_start:batch_start + self._workers],
                                   start=batch_start))
            threads = [threading.Thread(target=_worker, args=(i, u), daemon=True)
                       for i, u in batch]
            for t in threads: t.start()
            for t in threads: t.join()
            if batch_start + self._workers < len(valid_urls):
                time.sleep(self._sleep)

        return [r for r in results if r is not None]

    def _enrich_one(self, url: str) -> ReelResult:
        import yt_dlp

        # "Cannot parse data" errors come from reels that require
        # a newer FB extractor. Don't retry them — they always fail.
        for attempt in range(self._max_retries + 1):
            try:
                with yt_dlp.YoutubeDL(self._ydl_opts()) as ydl:
                    info = ydl.extract_info(url, download=False) or {}
                if not info:
                    break   # skip retries — empty means unsupported
                text     = f"{info.get('title','')} {info.get('description','')}"
                raw_date = info.get("upload_date") or ""
                iso_date = (f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
                            if len(raw_date) == 8 else raw_date)
                return ReelResult(
                    url          = f"https://www.facebook.com/reel/{_reel_id(url)}",
                    reel_id      = _reel_id(url),
                    title        = (info.get("title") or "")[:150],
                    description  = (info.get("description") or "")[:500],
                    hashtags     = list(dict.fromkeys(
                        (info.get("tags") or []) + re.findall(r"#\w+", text)))[:30],
                    creator_name = info.get("uploader") or info.get("channel") or "",
                    creator_url  = info.get("uploader_url") or info.get("channel_url") or "",
                    views        = info.get("view_count")    or 0,
                    likes        = info.get("like_count")    or 0,
                    comments     = info.get("comment_count") or 0,
                    shares       = info.get("repost_count")  or info.get("share_count") or 0,
                    posted_at    = iso_date,
                    source       = "yt_dlp_meta",
                )
            except Exception as e:
                err_str = str(e).lower()
                # "Cannot parse data" = FB extractor broken for this reel.
                # Retrying wastes time — skip immediately.
                if "cannot parse" in err_str or "parse data" in err_str:
                    break
                if attempt < self._max_retries:
                    time.sleep(1.5 * (attempt + 1))

        return ReelResult(url=f"https://www.facebook.com/reel/{_reel_id(url)}",
                          reel_id=_reel_id(url), source="yt_dlp_failed")


# ══════════════════════════════════════════════════════════════════════
#  Strategy 5 — Playwright per-reel enrichment (likes + shares)
# ══════════════════════════════════════════════════════════════════════

class PlaywrightReelEnricher:
    """
    Visits each reel page in a logged-in browser and extracts engagement.

    Facebook renders likes/shares/comments only to logged-in users.
    We read them from:
      1. aria-label attributes (e.g. "4.3K likes", "120 comments")
      2. span text nodes near reaction/share buttons
      3. Embedded __bbox JSON in the page source (most reliable)
    """

    def __init__(self, cookies_file: str, headless: bool = True,
                 page_timeout: int = 20_000, sleep: float = 1.5):
        self._cookies_file = cookies_file
        self._headless     = headless
        self._page_timeout = page_timeout
        self._sleep        = sleep

    def enrich(self, reels: list[ReelResult]) -> None:
        """Mutate ReelResult objects in-place with likes/shares/comments."""
        # Only process reels that actually have a valid URL
        targets = [r for r in reels if _is_valid_reel_url(r.url)
                   and r.likes == 0 and r.shares == 0]
        if not targets:
            return

        cookies = _load_netscape_cookies(self._cookies_file)
        if not cookies:
            log.warning("PlaywrightReelEnricher: no cookies")
            return

        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            return

        log.info("PlaywrightReelEnricher: enriching %d reels", len(targets))

        try:
            with sync_playwright() as pw:
                browser = pw.chromium.launch(
                    headless=self._headless,
                    args=["--disable-blink-features=AutomationControlled",
                          "--no-sandbox", "--disable-dev-shm-usage"],
                )
                ctx = browser.new_context(
                    user_agent         = _UA,
                    viewport           = {"width": 1366, "height": 768},
                    locale             = "en-IN",
                    timezone_id        = "Asia/Kolkata",
                    java_script_enabled= True,
                )
                _pw_add_cookies(ctx, cookies)
                page = ctx.new_page()
                page.add_init_script(
                    "Object.defineProperty(navigator,'webdriver',{get:()=>undefined});"
                    "window.chrome={runtime:{}};"
                )

                for reel in targets:
                    try:
                        page.goto(reel.url, wait_until="domcontentloaded",
                                  timeout=self._page_timeout)
                        time.sleep(random.uniform(2.0, 3.5))

                        if any(x in page.url for x in ("login", "checkpoint")):
                            log.warning("Session expired — stopping enrichment")
                            break

                        # Wait a bit more for React to hydrate engagement counts
                        try:
                            page.wait_for_load_state("networkidle", timeout=5000)
                        except Exception:
                            pass
                        time.sleep(1.0)

                        self._extract_into(reel, page)
                        log.debug("Reel %s → views=%d likes=%d shares=%d comments=%d",
                                  reel.reel_id, reel.views, reel.likes,
                                  reel.shares, reel.comments)

                    except Exception as e:
                        log.debug("Enrichment failed %s: %s", reel.url, e)

                    time.sleep(random.uniform(self._sleep, self._sleep + 1.2))

                browser.close()

        except Exception as e:
            log.error("PlaywrightReelEnricher: %s", e)

    @staticmethod
    def _extract_into(reel: ReelResult, page) -> None:
        """
        Multi-strategy engagement extraction from a reel page.
        Strategy A: Parse embedded JSON blobs (most reliable).
        Strategy B: aria-label scan.
        Strategy C: visible span text near action buttons.
        """
        html = page.content()

        # ── Strategy A: embedded JSON in page source ─────────────────
        # Facebook embeds __bbox / require() calls with engagement data
        for json_pat in (
            r'"like_count"\s*:\s*(\d+)',
            r'"reaction_count"\s*:\s*\{[^}]*"count"\s*:\s*(\d+)',
            r'"likers"\s*:\s*\{[^}]*"count"\s*:\s*(\d+)',
        ):
            m = re.search(json_pat, html)
            if m:
                val = int(m.group(1))
                if val > 0 and reel.likes == 0:
                    reel.likes = val
                    break

        for json_pat in (
            r'"comment_count"\s*:\s*(\d+)',
            r'"total_comment_count"\s*:\s*(\d+)',
        ):
            m = re.search(json_pat, html)
            if m:
                val = int(m.group(1))
                if val > 0 and reel.comments == 0:
                    reel.comments = val
                    break

        for json_pat in (
            r'"share_count"\s*:\s*\{[^}]*"count"\s*:\s*(\d+)',
            r'"reshare_count"\s*:\s*(\d+)',
            r'"share_count"\s*:\s*(\d+)',
        ):
            m = re.search(json_pat, html)
            if m:
                val = int(m.group(1))
                if val > 0 and reel.shares == 0:
                    reel.shares = val
                    break

        for json_pat in (
            r'"view_count"\s*:\s*(\d+)',
            r'"video_view_count"\s*:\s*(\d+)',
            r'"play_count"\s*:\s*(\d+)',
        ):
            m = re.search(json_pat, html)
            if m:
                val = int(m.group(1))
                if val > 0 and reel.views == 0:
                    reel.views = val
                    break

        # ── Strategy B: aria-label scan ──────────────────────────────
        try:
            all_labels = page.eval_on_selector_all(
                "[aria-label]",
                "els => els.map(e => e.getAttribute('aria-label') || '')"
            )
            label_text = " ".join(str(l) for l in all_labels)

            def _from_labels(patterns):
                for pat in patterns:
                    m = re.search(pat, label_text, re.I)
                    if m:
                        val = _parse_abbrev(m.group(1))
                        if val > 0:
                            return val
                return 0

            v = _from_labels([r'([\d,\.]+[KMBkmb]?)\s*(?:view|play)'])
            l = _from_labels([r'([\d,\.]+[KMBkmb]?)\s*(?:like|reaction|people reacted)'])
            c = _from_labels([r'([\d,\.]+[KMBkmb]?)\s*comment'])
            s = _from_labels([r'([\d,\.]+[KMBkmb]?)\s*share'])

            if v > 0 and reel.views    == 0: reel.views    = v
            if l > 0 and reel.likes    == 0: reel.likes    = l
            if c > 0 and reel.comments == 0: reel.comments = c
            if s > 0 and reel.shares   == 0: reel.shares   = s
        except Exception:
            pass

        # ── Strategy C: visible span/div text near action buttons ────
        try:
            spans = page.eval_on_selector_all(
                "span, div[role='button']",
                "els => els.map(e => e.innerText || '').filter(t => t.length < 20)"
            )
            span_text = " ".join(str(s) for s in spans)

            def _from_spans(patterns):
                for pat in patterns:
                    m = re.search(pat, span_text, re.I)
                    if m:
                        val = _parse_abbrev(m.group(1))
                        if val > 0:
                            return val
                return 0

            v = _from_spans([r'([\d,\.]+[KMBkmb]?)\s*(?:view|play)'])
            l = _from_spans([r'([\d,\.]+[KMBkmb]?)\s*(?:like|reaction)'])
            c = _from_spans([r'([\d,\.]+[KMBkmb]?)\s*comment'])
            s = _from_spans([r'([\d,\.]+[KMBkmb]?)\s*share'])

            if v > 0 and reel.views    == 0: reel.views    = v
            if l > 0 and reel.likes    == 0: reel.likes    = l
            if c > 0 and reel.comments == 0: reel.comments = c
            if s > 0 and reel.shares   == 0: reel.shares   = s
        except Exception:
            pass
