# AGENTS

## Purpose

This repo's main executable task is the housing scraper/ranker in `scripts/scrape_suumo_both.py`.

It currently aggregates listings from:

- SUUMO
- KEN Corporation buy listings

## How To Run

Use the Nix flake dev shell defined in `flake.nix`.

Standard invocation:

```bash
nix develop -c python scripts/scrape_suumo_both.py
```

Rescore-only invocation:

```bash
nix develop -c python scripts/scrape_suumo_both.py --rescore-only
```

This enters the default dev shell and runs the scraper with the Python environment declared by the flake:

- `python3.13`
- `requests`
- `beautifulsoup4`
- `sqlite`

The script performs the full pipeline:

- scrape source listings
- enrich listing details
- score and rank candidates
- update `data/`, `output/`, and `docs/`

`--rescore-only` skips source scraping and reuses the persisted SQLite data in `data/suumo_listings.sqlite3`.
Use it when only the scoring/filtering logic changed and the underlying scraped listings do not need to be refreshed.

In rescore-only mode, the script still:

- recomputes scores from the DB-backed listing set
- rebuilds shortlist outputs
- refreshes shortlist preview images used in `output/` and `docs/`

## Sandbox Note

In restricted environments, `nix develop` may fail if Nix cannot write to its default cache directory under `~/.cache/nix`.

If that happens, run with a writable cache override:

```bash
XDG_CACHE_HOME=/tmp/nix-cache-househunt nix develop -c python scripts/scrape_suumo_both.py
```

The same cache override works for rescore-only mode:

```bash
XDG_CACHE_HOME=/tmp/nix-cache-househunt nix develop -c python scripts/scrape_suumo_both.py --rescore-only
```

## Outputs

Running the script updates these repo paths:

- `data/suumo_listings.sqlite3`
- `output/top15_mansions.*`
- `output/top15_houses.*`
- `output/mansion_images/`
- `output/house_images/`
- `docs/data/*.json`
- `docs/data/images/**`
- `docs/YYYY-MM-DD/data/*.json`
- `docs/YYYY-MM-DD/assets/`
- `docs/YYYY-MM-DD/index.html`

## Agent Guidance

- Prefer `nix develop -c ...` over invoking the system Python directly.
- Do not assume Python dependencies are installed outside the flake dev shell.
- If verifying the script without waiting for a full scrape, `nix develop -c python -m py_compile scripts/scrape_suumo_both.py` is a quick sanity check.
- If `nix develop` fails due to cache permissions, use the `XDG_CACHE_HOME=/tmp/nix-cache-househunt` prefix shown above.
- Expect a full run to take several minutes because the pipeline fetches detail pages and localizes preview images for published outputs.
- Prefer `--rescore-only` after scorer changes when a full rescrape is unnecessary; it is materially cheaper than a full source refresh.
