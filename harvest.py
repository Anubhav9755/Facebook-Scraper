#!/usr/bin/env python3
"""
harvest.py — Facebook Viral Reel Link Harvester  v2
════════════════════════════════════════════════════

FIRST TIME SETUP
─────────────────
  1.  pip install -r requirements.txt
      python -m playwright install chromium

  2.  Export Facebook cookies:
        a. Open Chrome and log into facebook.com
        b. Install "Get cookies.txt LOCALLY" extension
        c. Click the extension on facebook.com → Export
        d. Save the file as  fb_cookies.txt  in this folder

  3.  Run:
        python harvest.py --query "Bollywood dance" --type keyword --cookies fb_cookies.txt

WINDOWS NOTE (important!)
──────────────────────────
Windows File Explorer hides known file extensions by default.
When you "save as fb_cookies.txt", Windows might write "fb_cookies.txt.txt"
but show it as "fb_cookies.txt" in Explorer.

This tool auto-detects and resolves double-extension filenames, so you
can safely pass --cookies fb_cookies.txt and it will find the real file.

To check: open PowerShell and run:  dir fb_cookies*

COMMANDS
────────
  # Keyword search (most common)
  python harvest.py --query "Bollywood dance" --type keyword --limit 20 --cookies fb_cookies.txt

  # Person / creator
  python harvest.py --query "Salman Khan" --type person --limit 15 --cookies fb_cookies.txt

  # Hashtag feed
  python harvest.py --query "#cricket" --type hashtag --limit 25 --cookies fb_cookies.txt

  # Watch the browser while it runs
  python harvest.py --query "comedy" --type keyword --no-headless --cookies fb_cookies.txt

  # Skip yt-dlp enrichment (faster, URL-only output)
  python harvest.py --query "fitness" --type keyword --no-enrich --cookies fb_cookies.txt

  # No cookies — limited fallback mode
  python harvest.py --query "bbcnews" --type person --limit 10
"""

from __future__ import annotations

import sys
import logging
import argparse
from pathlib import Path

from core.scrapers  import resolve_cookies_path
from core.harvester import Harvester
from core.exporters import Exporter
from core.display   import (
    console,
    print_banner,
    print_query_start,
    print_results_table,
    print_session_summary,
    print_error,
    warn_no_cookies,
)

logging.basicConfig(
    level   = logging.WARNING,
    format  = "%(levelname)s  %(name)s  %(message)s",
)
log = logging.getLogger("harvest")


# ── CLI ──────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog            = "harvest",
        description     = "📡 Facebook Viral Reel Link Harvester",
        formatter_class = argparse.RawDescriptionHelpFormatter,
        epilog          = __doc__,
    )
    req = p.add_argument_group("required")
    req.add_argument("--query", "-q", required=True,
                     help="Keyword, person name, or hashtag to search")
    req.add_argument("--type", "-t", dest="query_type", required=True,
                     choices=["keyword", "person", "hashtag"],
                     help="Search type")

    opt = p.add_argument_group("optional")
    opt.add_argument("--limit",     "-l", type=int, default=50, metavar="N",
                     help="Top N reels to export (default: 50)")
    opt.add_argument("--output",    "-o", default="output", metavar="DIR",
                     help="Output directory (default: ./output)")
    opt.add_argument("--cookies",   "-c", default=None, metavar="FILE",
                     help="Path to fb_cookies.txt  ← STRONGLY RECOMMENDED\n"
                          "Auto-resolves Windows double-extension (.txt.txt)")
    opt.add_argument("--fresh",     action="store_true",
                     help="Ignore seen history — return all reels including already-seen ones")
    opt.add_argument("--no-headless", action="store_true",
                     help="Show the browser window while scraping (for debugging)")
    opt.add_argument("--scrolls",   type=int, default=15, metavar="N",
                     help="Max page scrolls in browser mode (default: 15)")
    opt.add_argument("--deep",      action="store_true",
                     help="Enable Phase 3: visit each reel page for likes (adds ~3min)")
    opt.add_argument("--no-enrich", action="store_true",
                     help="Skip yt-dlp enrichment (faster, URL-only output)")
    opt.add_argument("--workers",   type=int, default=3, metavar="N",
                     help="yt-dlp parallel enrichment threads (default: 3)")
    opt.add_argument("--quiet",     action="store_true",
                     help="Print only output file paths (for scripting)")
    opt.add_argument("--verbose",   action="store_true",
                     help="Enable debug logging")
    return p


# ── Main ─────────────────────────────────────────────────────────────

def main() -> int:
    parser = build_parser()
    args   = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    else:
        logging.getLogger("harvest").setLevel(logging.INFO)
        logging.getLogger("core").setLevel(logging.INFO)

    # ── Resolve cookies ───────────────────────────────────────────────
    cookies_path: str | None = None
    if args.cookies:
        resolved = resolve_cookies_path(args.cookies)
        if resolved:
            cookies_path = str(resolved)
        else:
            # File genuinely not found — show helpful error and stop
            print_error(
                f"Cookies file not found: {args.cookies}",
                hint=(
                    "The tool searched for all common filename variants.\n\n"
                    "Steps to fix:\n"
                    "  1. Open PowerShell in this folder and run:\n"
                    "       dir fb_cookies*\n"
                    "  2. Note the exact filename shown\n"
                    "  3. Re-run with that exact name\n\n"
                    "If the file doesn't exist yet:\n"
                    "  1. Log into Facebook in Chrome\n"
                    "  2. Install 'Get cookies.txt LOCALLY' extension\n"
                    "  3. Click extension on facebook.com → Export\n"
                    "  4. Save as fb_cookies.txt in this folder"
                ),
                windows_hint=True,
            )
            return 1

    if not args.quiet:
        print_banner(cookies_path=cookies_path)
        print_query_start(args.query, args.query_type, args.limit)

    if not cookies_path and not args.quiet:
        warn_no_cookies()

    # ── Harvest ───────────────────────────────────────────────────────
    harvester = Harvester(
        cookies_file = cookies_path,
        enrich       = not args.no_enrich,
        deep_enrich  = args.deep,
        headless     = not args.no_headless,
        max_scrolls  = args.scrolls,
        yt_workers   = args.workers,
        seen_db_path = "NUL" if args.fresh else str(Path(args.output) / "seen_reels.json"),
    )

    try:
        session = harvester.harvest(
            query      = args.query,
            query_type = args.query_type,
            limit      = args.limit,
        )
    except Exception as e:
        log.exception("Harvester error")
        print_error(str(e))
        return 1

    if not session.results:
        print_error(
            f"No reels found for: '{args.query}'",
            hint=(
                "Most likely cause: no valid cookies.\n\n"
                "Without a logged-in session, Facebook returns no results\n"
                "for keyword and hashtag queries.\n\n"
                "See setup instructions:\n"
                "  python harvest.py --help"
            ),
            windows_hint=not cookies_path,
        )
        return 1

    # ── Export ────────────────────────────────────────────────────────
    exporter = Exporter(output_dir=args.output, limit=args.limit)
    links_path, html_path, csv_path, json_path = exporter.export(session)

    if args.quiet:
        print(links_path)
        print(html_path)
        print(csv_path)
        print(json_path)
    else:
        print_results_table(session, limit=args.limit)
        print_session_summary(session, links_path, html_path, csv_path, json_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
