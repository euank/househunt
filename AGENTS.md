# AGENTS

## Purpose

This repo's main executable task is the SUUMO scraper/ranker in `scripts/scrape_suumo_both.py`.

## How To Run

Use the Nix flake dev shell defined in `flake.nix`.

Standard invocation:

```bash
nix develop -c python scripts/scrape_suumo_both.py
```

This enters the default dev shell and runs the scraper with the Python environment declared by the flake:

- `python3.13`
- `requests`
- `beautifulsoup4`
- `sqlite`

## Sandbox Note

In restricted environments, `nix develop` may fail if Nix cannot write to its default cache directory under `~/.cache/nix`.

If that happens, run with a writable cache override:

```bash
XDG_CACHE_HOME=/tmp/nix-cache-househunt nix develop -c python scripts/scrape_suumo_both.py
```

## Outputs

Running the script updates these repo paths:

- `data/suumo_listings.sqlite3`
- `output/top15_mansions.*`
- `output/top15_houses.*`
- `docs/data/*.json`
- `docs/YYYY-MM-DD/data/*.json`

## Agent Guidance

- Prefer `nix develop -c ...` over invoking the system Python directly.
- Do not assume Python dependencies are installed outside the flake dev shell.
- If verifying the script without waiting for a full scrape, `nix develop -c python -m py_compile scripts/scrape_suumo_both.py` is a quick sanity check.
