"""
core/exporters.py
─────────────────
4 output formats every run:

  1. _links.txt   — plain numbered list, one URL per line. Copy-paste ready.
  2. _report.html — click-to-open HTML. Send to manager, open in Chrome.
  3. _data.csv    — full data for Excel / analysts.
  4. _data.json   — full data for developers.
"""

from __future__ import annotations

import csv
import json
import re
import datetime
import logging
from pathlib import Path

from core.models import HarvestSession

log = logging.getLogger(__name__)

CSV_COLUMNS = [
    "rank", "viral_score", "url", "reel_id", "title",
    "creator_name", "creator_url", "views", "likes", "comments", "shares",
    "engagement_total", "posted_at", "scraped_at",
    "hashtags", "description", "query", "query_type", "source",
]


def _slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s-]+", "_", text)
    return text[:40]


def _ts() -> str:
    return datetime.datetime.utcnow().strftime("%Y%m%d_%H%M%S")


def _fmt(n: int) -> str:
    if n >= 1_000_000: return f"{n/1_000_000:.1f}M"
    if n >= 1_000:     return f"{n/1_000:.1f}K"
    return str(n)


def _esc(s) -> str:
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;").replace('"','&quot;')


class Exporter:
    def __init__(self, output_dir: str = "output", limit: int = 0):
        self._dir   = Path(output_dir)
        self._limit = limit
        self._dir.mkdir(parents=True, exist_ok=True)

    def export(self, session: HarvestSession) -> tuple[Path, Path, Path, Path]:
        rows  = self._rows(session)
        stem  = f"{_slugify(session.query)}_{_ts()}_top{len(rows)}"
        return (
            self._write_links(rows, stem, session),
            self._write_html(rows, stem, session),
            self._write_csv(rows, stem),
            self._write_json(rows, stem, session),
        )

    # ── 1. Plain links.txt ───────────────────────────────────────────

    def _write_links(self, rows, stem, session) -> Path:
        path = self._dir / f"{stem}_links.txt"
        now  = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
        out  = [
            f"FACEBOOK VIRAL REELS — {session.query.upper()}",
            f"Generated : {now}",
            f"Reels     : {len(rows)}",
            "=" * 60,
            "",
        ]
        for r in rows:
            views   = _fmt(r.get("views", 0))
            likes   = _fmt(r.get("likes", 0))
            creator = (r.get("creator_name") or "").strip()[:35]
            title   = (r.get("title") or "").strip()[:65]
            score   = r.get("viral_score", 0)
            out.append(f"#{r['rank']}  Score:{score:.0f}  Views:{views}  Likes:{likes}  Creator:{creator}")
            if title:
                out.append(f"    {title}")
            out.append(f"    {r['url']}")
            out.append("")
        path.write_text("\n".join(out), encoding="utf-8")
        log.info("Links TXT → %s", path)
        return path

    # ── 2. HTML report ───────────────────────────────────────────────

    def _write_html(self, rows, stem, session) -> Path:
        path = self._dir / f"{stem}_report.html"
        now  = datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        def color(score):
            if score >= 60: return "#22c55e"
            if score >= 35: return "#f59e0b"
            if score >= 15: return "#fb923c"
            return "#94a3b8"

        tbody = ""
        for r in rows:
            score    = r.get("viral_score", 0)
            views    = _fmt(r.get("views", 0))
            likes    = _fmt(r.get("likes", 0))
            comments = _fmt(r.get("comments", 0))
            creator  = _esc((r.get("creator_name") or "—")[:40])
            date     = (r.get("posted_at") or "")[:10] or "—"
            title    = _esc((r.get("title") or "—")[:80])
            url      = _esc(r["url"])
            c        = color(score)
            tbody += f"""<tr>
<td class=rk>{r['rank']}</td>
<td><b style="color:{c};font-size:17px">{score:.0f}</b></td>
<td class=n>{views}</td><td class=n>{likes}</td><td class=n>{comments}</td>
<td class=cr>{creator}</td><td class=dt>{date}</td>
<td class=ti title="{url}">{title}</td>
<td><a href="{url}" target=_blank class=btn>▶ Open</a><br><span class=lnk>{url}</span></td>
</tr>\n"""

        total_views = _fmt(sum(r.get("views",0) for r in rows))
        total_likes = _fmt(sum(r.get("likes",0) for r in rows))
        top_score   = f"{rows[0]['viral_score']:.0f}" if rows else "0"

        html = f"""<!DOCTYPE html><html lang=en><head>
<meta charset=UTF-8><meta name=viewport content="width=device-width,initial-scale=1">
<title>Viral Reels — {_esc(session.query)}</title>
<style>
*{{box-sizing:border-box;margin:0;padding:0}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#0f172a;color:#e2e8f0;padding:20px}}
.hdr{{max-width:1400px;margin:0 auto 20px;background:#1e293b;border-radius:12px;padding:24px 28px;border-left:4px solid #3b82f6}}
.hdr h1{{font-size:20px;color:#f1f5f9;margin-bottom:6px}}
.hdr h1 em{{color:#60a5fa;font-style:normal}}
.meta{{color:#94a3b8;font-size:12px;margin-bottom:16px}}
.stats{{display:flex;gap:16px;flex-wrap:wrap}}
.stat{{background:#0f172a;border-radius:8px;padding:10px 18px}}
.sl{{font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.05em}}
.sv{{font-size:20px;font-weight:700;color:#f1f5f9;margin-top:2px}}
.wrap{{max-width:1400px;margin:0 auto;background:#1e293b;border-radius:12px;overflow:auto}}
table{{width:100%;border-collapse:collapse;font-size:13px}}
thead tr{{background:#0f172a}}
th{{padding:11px 13px;text-align:left;font-size:10px;color:#64748b;text-transform:uppercase;letter-spacing:.05em;border-bottom:1px solid #334155;white-space:nowrap}}
td{{padding:10px 13px;border-bottom:1px solid #1a2744;vertical-align:middle}}
tr:hover td{{background:#263346}}
.rk{{color:#64748b;font-weight:600;width:32px}}
.n{{font-family:monospace;color:#cbd5e1;white-space:nowrap}}
.cr{{color:#93c5fd;max-width:130px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}}
.dt{{color:#64748b;white-space:nowrap;font-size:12px}}
.ti{{color:#94a3b8;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px}}
.btn{{display:inline-block;background:#3b82f6;color:#fff;padding:5px 11px;border-radius:6px;text-decoration:none;font-size:12px;font-weight:600;white-space:nowrap}}
.btn:hover{{background:#2563eb}}
.lnk{{font-family:monospace;font-size:10px;color:#475569;user-select:all;word-break:break-all}}
.foot{{max-width:1400px;margin:14px auto 0;color:#475569;font-size:11px;text-align:center}}
</style></head><body>
<div class=hdr>
  <h1>📡 Viral Reels — <em>{_esc(session.query)}</em></h1>
  <div class=meta>{now} &nbsp;·&nbsp; {_esc(session.query_type)} search &nbsp;·&nbsp; ranked by Views × Likes</div>
  <div class=stats>
    <div class=stat><div class=sl>Reels</div><div class=sv>{len(rows)}</div></div>
    <div class=stat><div class=sl>Total Views</div><div class=sv>{total_views}</div></div>
    <div class=stat><div class=sl>Total Likes</div><div class=sv>{total_likes}</div></div>
    <div class=stat><div class=sl>Top Score</div><div class=sv>{top_score}</div></div>
  </div>
</div>
<div class=wrap>
<table>
<thead><tr><th>#</th><th>Score</th><th>Views</th><th>Likes</th><th>Comments</th>
<th>Creator</th><th>Date</th><th>Title</th><th>Link (click to open)</th></tr></thead>
<tbody>{tbody}</tbody>
</table>
</div>
<div class=foot>Facebook Viral Reel Harvester &nbsp;·&nbsp; {now}</div>
</body></html>"""

        path.write_text(html, encoding="utf-8")
        log.info("HTML report → %s", path)
        return path

    # ── 3. CSV ───────────────────────────────────────────────────────

    def _write_csv(self, rows, stem) -> Path:
        path = self._dir / f"{stem}_data.csv"
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=CSV_COLUMNS,
                               extrasaction="ignore", lineterminator="\n")
            w.writeheader()
            w.writerows(rows)
        log.info("CSV → %s", path)
        return path

    # ── 4. JSON ──────────────────────────────────────────────────────

    def _write_json(self, rows, stem, session) -> Path:
        path = self._dir / f"{stem}_data.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump({
                "meta": {
                    "query"      : session.query,
                    "query_type" : session.query_type,
                    "total_found": len(session.results),
                    "exported"   : len(rows),
                    "exported_at": datetime.datetime.utcnow().isoformat() + "Z",
                },
                "reels": rows,
            }, fh, ensure_ascii=False, indent=2)
        log.info("JSON → %s", path)
        return path

    # ── Helpers ──────────────────────────────────────────────────────

    def _rows(self, session: HarvestSession) -> list[dict]:
        ranked = session.top
        if self._limit and self._limit > 0:
            ranked = ranked[:self._limit]
        out = []
        for r in ranked:
            d = r.to_dict()
            d["engagement_total"] = r.engagement_total
            out.append(d)
        return out
