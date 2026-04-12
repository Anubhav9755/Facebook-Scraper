# 📡 Facebook Viral Reel Link Harvester

Discover and rank the most viral Facebook Reels for any keyword, person, or hashtag.
Outputs clean, ranked **links + engagement metadata** to **CSV and JSON** — no video downloads.

---

## Architecture

```
                ┌────────────────────────────────────────────┐
                │               harvest.py  (CLI)            │
                └──────────────────┬─────────────────────────┘
                                   │
                        ┌──────────▼───────────┐
                        │    Harvester          │  orchestrator
                        └──┬────────────────┬──┘
                           │                │
              ┌────────────▼──┐        ┌────▼────────────────┐
              │ GoogleDork    │        │ FacebookLibrary      │
              │ Scraper       │        │ Scraper              │
              │ (discover)    │        │ (person queries)     │
              └────────────┬──┘        └────────────────┬────┘
                           │                            │
                    ┌──────▼────────────────────────────▼──┐
                    │        YtdlpMeta Scraper              │
                    │    (enrich with engagement data)      │
                    └─────────────────┬─────────────────────┘
                                      │
                         ┌────────────▼────────────┐
                         │  Viral Score + Ranking  │
                         │  (log-normalised, 0–100)│
                         └────────────┬────────────┘
                                      │
                          ┌───────────▼──────────┐
                          │   CSV  +  JSON export │
                          └──────────────────────┘
```

---

## Quickstart

```bash
# Install
pip install -r requirements.txt

# Keyword search
python harvest.py --query "Bollywood dance" --type keyword --limit 20

# Person / creator
python harvest.py --query "Salman Khan" --type person --limit 15

# Hashtag
python harvest.py --query "#fitness" --type hashtag --limit 25

# Authenticated (more results for some pages)
python harvest.py --query "cricket" --type keyword --cookies fb_cookies.txt
```

---

## Output

Every run produces two files in `./output/`:

```
output/
  bollywood_dance_20250411_143022_top20.csv
  bollywood_dance_20250411_143022_top20.json
```

### CSV columns
| Column | Description |
|--------|-------------|
| rank | 1 = most viral |
| viral_score | Composite 0–100 score |
| url | Direct Facebook reel link |
| creator_name | Page / profile name |
| views / likes / comments / shares | Raw engagement |
| engagement_total | Sum of all engagement |
| posted_at | Upload date |
| hashtags | Comma-separated tags |

### JSON structure
```json
{
  "meta": { "query": "...", "query_type": "...", "total_found": 48, ... },
  "reels": [ { "rank": 1, "viral_score": 82.4, "url": "...", ... }, ... ]
}
```

---

## Viral Score Formula

```
score = 0.35 × log_norm(views, 50M)
      + 0.25 × log_norm(shares, 2M)
      + 0.25 × log_norm(likes, 5M)
      + 0.15 × log_norm(comments, 1M)

      × 100   (scaled to 0–100)
```

Log-normalisation prevents a single 100M-view outlier from collapsing
all other scores to near-zero.

---

## Facebook Cookies Setup (Optional)

For better access to content that's partially behind a login wall:

1. Install **"Get cookies.txt LOCALLY"** browser extension
2. Log into Facebook in your browser
3. Click the extension → Export cookies for `facebook.com`
4. Save as `fb_cookies.txt` in this folder
5. Pass `--cookies fb_cookies.txt` to any command

---

## CLI Reference

```
usage: harvest [-h] --query QUERY --type {keyword,person,hashtag}
               [--limit N] [--output DIR] [--cookies FILE]
               [--google-pages N] [--no-enrich] [--quiet] [--verbose]

Required:
  --query,   -q   Search term
  --type,    -t   keyword | person | hashtag

Optional:
  --limit,   -l   Top N reels to export (default: 20)
  --output,  -o   Output directory (default: ./output)
  --cookies, -c   Path to fb_cookies.txt
  --google-pages  Google result pages to scan (default: 5)
  --no-enrich     Skip yt-dlp metadata fetch (faster, URL-only)
  --quiet         Print only output file paths (for scripting)
  --verbose       Enable debug logging
```
# Facebook-Scraper
