"""
core/display.py
───────────────
Rich-powered terminal UI.
Zero coupling to scraping logic.

New in v2:
  - Live progress spinner during harvest
  - Source breakdown panel
  - Better score colouring thresholds
  - Cookies status shown in banner
  - print_error shows Windows-specific hints for common failures
"""

from __future__ import annotations

from pathlib import Path
from typing  import Optional

from rich.console    import Console
from rich.table      import Table
from rich.panel      import Panel
from rich.text       import Text
from rich.rule       import Rule
from rich.live       import Live
from rich.spinner    import Spinner
from rich.columns    import Columns
from rich            import box

from core.models import HarvestSession

console = Console()


# ── Colouring ────────────────────────────────────────────────────────

def _score_colour(score: float) -> str:
    if score >= 70: return "bold green"
    if score >= 45: return "green"
    if score >= 25: return "yellow"
    if score >= 10: return "orange3"
    return "dim red"


# ── Panels ───────────────────────────────────────────────────────────

def print_banner(cookies_path: Optional[str] = None) -> None:
    cookie_line = (
        f"[green]✓  Cookies:[/green] [dim]{cookies_path}[/dim]"
        if cookies_path
        else "[yellow]⚠  No cookies — running in limited fallback mode[/yellow]"
    )
    console.print(Panel.fit(
        "[bold cyan]📡 Facebook Viral Reel Link Harvester[/bold cyan]\n"
        "[dim]Keyword · Person · Hashtag  →  Ranked Links  →  CSV + JSON[/dim]\n"
        + cookie_line,
        border_style = "cyan",
        padding      = (0, 4),
    ))


def print_query_start(query: str, query_type: str, limit: int) -> None:
    console.print()
    console.print(Rule(
        f"[bold]Query:[/bold] [cyan]{query}[/cyan]  "
        f"[dim]({query_type})[/dim]  ·  top [yellow]{limit}[/yellow]"
    ))


def print_results_table(session: HarvestSession,
                         limit: Optional[int] = None) -> None:
    ranked = session.top[:limit] if limit else session.top

    t = Table(
        title        = f"Top {len(ranked)} Viral Reels — \"{session.query}\"",
        box          = box.ROUNDED,
        show_header  = True,
        header_style = "bold magenta",
        expand       = False,
        min_width    = 90,
    )
    t.add_column("#",       style="dim",    width=4,  justify="right")
    t.add_column("Score",                   width=7,  justify="right")
    t.add_column("Views",   style="yellow", width=11, justify="right")
    t.add_column("Likes",   style="green",  width=9,  justify="right")
    t.add_column("Shares",  style="cyan",   width=9,  justify="right")
    t.add_column("Creator", style="cyan",   max_width=22)
    t.add_column("Posted",  style="dim",    width=11)
    t.add_column("Source",  style="dim",    width=14)
    t.add_column("Reel URL",style="blue",   min_width=42)

    for r in ranked:
        colour  = _score_colour(r.viral_score)
        score_t = Text(f"{r.viral_score:.1f}", style=colour)
        date    = r.posted_at[:10] if r.posted_at else "—"

        t.add_row(
            str(r.rank),
            score_t,
            f"{r.views:,}",
            f"{r.likes:,}",
            f"{r.shares:,}",
            (r.creator_name or "—")[:22],
            date,
            r.source[:14],
            r.url,
        )

    console.print(t)


def print_session_summary(session: HarvestSession,
                           links_path: Path,
                           html_path: Path,
                           csv_path: Path,
                           json_path: Path) -> None:
    total    = len(session.results)
    exported = min(session.limit, total)
    top      = session.top[0] if session.results else None

    top_line = (
        f"[bold]#1 reel :[/bold] {top.url}\n"
        f"          score [green]{top.viral_score:.1f}[/green]  "
        f"views [yellow]{top.views:,}[/yellow]  "
        f"likes [green]{top.likes:,}[/green]"
        if top else "No reels found."
    )

    src_lines = "  ".join(
        f"[dim]{src}[/dim]=[bold]{cnt}[/bold]"
        for src, cnt in sorted(session.source_stats.items(), key=lambda x: -x[1])
    )

    console.print()
    console.print(Panel(
        f"[bold green]✓  Harvest complete[/bold green]\n\n"
        f"[bold]Query    :[/bold] {session.query}  [dim]({session.query_type})[/dim]\n"
        f"[bold]Found    :[/bold] {total} candidates  →  exported top {exported}\n"
        f"[bold]Enriched :[/bold] {session.enriched_count}/{total} with engagement data\n"
        f"[bold]Elapsed  :[/bold] {session.elapsed_seconds:.1f}s\n"
        f"[bold]Sources  :[/bold] {src_lines}\n\n"
        f"{top_line}\n\n"
        f"[bold cyan]📋 Links (share with manager):[/bold cyan]\n"
        f"   [dim]{links_path}[/dim]\n\n"
        f"[bold cyan]🌐 HTML report (open in Chrome):[/bold cyan]\n"
        f"   [dim]{html_path}[/dim]\n\n"
        f"[dim]CSV  → {csv_path}[/dim]\n"
        f"[dim]JSON → {json_path}[/dim]",
        title        = "Session Summary",
        border_style = "green",
        expand       = False,
    ))


def print_error(message: str, hint: str = "",
                windows_hint: bool = False) -> None:
    body = f"[red]{message}[/red]"
    if hint:
        body += f"\n\n[dim]{hint}[/dim]"
    if windows_hint:
        body += (
            "\n\n[yellow]Windows tip:[/yellow] File Explorer hides extensions by default.\n"
            "Your file might be named [bold]fb_cookies.txt.txt[/bold] on disk.\n"
            "In PowerShell, run:\n"
            "  [cyan]dir fb_cookies*[/cyan]\n"
            "to see the real filename.\n\n"
            "The tool will auto-detect this on next run — "
            "just pass the name you see in Explorer."
        )
    console.print(Panel(
        body,
        title        = "[red]Error[/red]",
        border_style = "red",
        expand       = False,
    ))


def warn_no_cookies() -> None:
    console.print(Panel(
        "[yellow]⚠  No cookies file provided — running in fallback mode[/yellow]\n\n"
        "Results will be [bold]severely limited[/bold]. Facebook requires login "
        "for keyword and hashtag searches.\n\n"
        "[bold]Full setup (5 min):[/bold]\n"
        "  1. Open Chrome and log into [cyan]facebook.com[/cyan]\n"
        "  2. Install [bold]'Get cookies.txt LOCALLY'[/bold] extension\n"
        "  3. Click the extension icon on facebook.com\n"
        "  4. Click [bold]Export[/bold] → save as [bold cyan]fb_cookies.txt[/bold cyan]\n"
        "  5. Re-run with [bold cyan]--cookies fb_cookies.txt[/bold cyan]",
        title        = "[yellow]Cookie Setup[/yellow]",
        border_style = "yellow",
        expand       = False,
    ))
