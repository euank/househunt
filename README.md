# Househunt Pages

This repo stores a local SUUMO scraping pipeline plus a GitHub Pages-friendly static site.

## Repo layout

- `scripts/` scraper code
- `data/` SQLite database with raw scraped rows
- `output/` markdown and JSON shortlist outputs
- `docs/` static GitHub Pages site
- `docs/YYYY-MM-DD/` archived site snapshots by run date

## Refresh the data

Use the combined scraper:

```bash
nix-shell -p 'python313.withPackages (ps: [ ps.requests ps.beautifulsoup4 ])' sqlite --run 'python scripts/scrape_suumo_both.py'
```

This updates:

- `data/suumo_listings.sqlite3`
- `output/top10_mansions.*`
- `output/top10_houses.*`
- `docs/data/*.json`
- `docs/YYYY-MM-DD/data/*.json` for the current run date

## GitHub Pages

Use the `docs/` directory as the Pages source.

The site entrypoint is:

- `docs/index.html`

The site reads static JSON from:

- `docs/data/mansions.json`
- `docs/data/houses.json`
- `docs/data/site.json`

Archived runs are also published under dated paths, for example:

- `docs/2026-04-15/index.html`
