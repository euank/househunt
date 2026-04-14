#!/usr/bin/env python3

from __future__ import annotations

import json
import math
import re
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://suumo.jp"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)


@dataclass(frozen=True)
class StationSeed:
    name: str
    code: str
    priority: str
    note: str


SEEDS = [
    StationSeed("中野", "27280", "exact", "exact target"),
    StationSeed("下北沢", "18010", "exact", "exact target"),
    StationSeed("代々木公園", "41300", "exact", "exact target"),
    StationSeed("代々木上原", "41290", "exact", "exact target"),
    StationSeed("代官山", "21850", "exact", "exact target"),
    StationSeed("中目黒", "27580", "exact", "user-added exact target"),
    StationSeed("池ノ上", "02030", "exact", "user-added exact target"),
    StationSeed("学芸大学", "07660", "exact", "user-added exact target"),
    StationSeed("梅ヶ丘", "04590", "exact", "user-added exact target"),
    StationSeed("渋谷", "17640", "exact", "user-added exact target"),
    StationSeed("祐天寺", "40640", "exact", "user-added exact target"),
    StationSeed("三軒茶屋", "16720", "exact", "user-added exact target"),
    StationSeed("代々木八幡", "41310", "nearby", "adjacent to 代々木公園/代々木上原"),
    StationSeed("恵比寿", "05050", "nearby", "adjacent to 代官山"),
]

BRIGHTNESS_KEYWORDS = [
    "南向き",
    "南東向き",
    "南西向き",
    "陽当り良好",
    "日当たり良好",
    "採光",
    "眺望良好",
    "通風良好",
    "ワイドスパン",
    "角住戸",
    "三方角住戸",
    "三面採光",
    "二面採光",
    "大きな窓",
]
CEILING_WINDOW_KEYWORDS = [
    "天井高",
    "ハイサッシ",
    "吹抜け",
    "勾配天井",
    "折上天井",
    "ワイドサッシ",
]
DISHWASHER_KEYWORDS = [
    "食器洗乾燥機",
    "食洗機",
    "食器洗浄乾燥機",
]


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def ensure_dirs() -> tuple[Path, Path]:
    data_dir = Path("data")
    output_dir = Path("output")
    data_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)
    return data_dir, output_dir


def connect_db(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript(
        """
        PRAGMA journal_mode = WAL;

        CREATE TABLE IF NOT EXISTS listings (
          listing_id TEXT PRIMARY KEY,
          url TEXT NOT NULL,
          title TEXT,
          property_name TEXT,
          address TEXT,
          access_text TEXT,
          price_man REAL,
          area_sqm REAL,
          layout TEXT,
          balcony_sqm REAL,
          walk_min INTEGER,
          built_year INTEGER,
          built_text TEXT,
          list_blurb TEXT,
          detail_summary TEXT,
          feature_tags_json TEXT,
          overview_json TEXT,
          exact_station_hits_json TEXT,
          nearby_station_hits_json TEXT,
          dishwasher INTEGER,
          brightness_hits_json TEXT,
          ceiling_hits_json TEXT,
          score REAL,
          criteria_notes_json TEXT,
          scraped_at TEXT NOT NULL,
          detail_scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS listing_station_hits (
          listing_id TEXT NOT NULL,
          station_name TEXT NOT NULL,
          station_code TEXT NOT NULL,
          priority TEXT NOT NULL,
          note TEXT,
          source_url TEXT NOT NULL,
          scraped_at TEXT NOT NULL,
          PRIMARY KEY (listing_id, station_code)
        );
        """
    )
    return conn


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch(session: requests.Session, url: str, *, sleep_s: float = 0.15) -> str:
    last_exc: Exception | None = None
    for attempt in range(5):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
            if sleep_s:
                time.sleep(sleep_s)
            return response.text
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            time.sleep(0.5 * (attempt + 1))
    raise RuntimeError(f"failed to fetch {url}: {last_exc}") from last_exc


def soup_for(session: requests.Session, url: str) -> BeautifulSoup:
    return BeautifulSoup(fetch(session, url), "html.parser")


def parse_float(text: str | None) -> float | None:
    if not text:
        return None
    match = re.search(r"([\d.]+)", text.replace(",", ""))
    return float(match.group(1)) if match else None


def parse_price_man(text: str | None) -> float | None:
    if not text:
        return None
    compact = text.replace(",", "").strip()
    if "億" in compact:
        match = re.match(r"(?:(\d+(?:\.\d+)?)億)?(?:(\d+)万円)?", compact)
        if not match:
            return None
        oku = float(match.group(1) or 0)
        man = float(match.group(2) or 0)
        return oku * 10000 + man
    match = re.search(r"(\d+(?:\.\d+)?)", compact)
    return float(match.group(1)) if match else None


def parse_year(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(r"(\d{4})年", text)
    return int(match.group(1)) if match else None


def parse_walk_min(text: str | None) -> int | None:
    if not text:
        return None
    values = [int(n) for n in re.findall(r"歩(\d+)分", text)]
    return min(values) if values else None


def normalize_layout(text: str | None) -> str:
    if not text:
        return ""
    return (
        text.strip()
        .upper()
        .replace("＋", "+")
        .replace("Ｓ", "S")
        .replace("Ｌ", "L")
        .replace("Ｄ", "D")
        .replace("Ｋ", "K")
    )


def layout_room_count(layout: str) -> int | None:
    match = re.match(r"(\d+)", layout)
    return int(match.group(1)) if match else None


def is_layout_preferred(layout: str) -> bool:
    if not layout:
        return False
    rooms = layout_room_count(layout)
    if rooms is None or rooms < 2:
        return False
    return "LDK" in layout or "SLDK" in layout or "+S" in layout


def extract_rows_from_listing(item: BeautifulSoup) -> dict[str, str]:
    rows: dict[str, str] = {}
    for line in item.select("div.dottable-line"):
        cells = [cell.get_text(" ", strip=True) for cell in line.select("dt,dd")]
        for idx in range(0, len(cells) - 1, 2):
            rows[cells[idx]] = cells[idx + 1]
    return rows


def listing_id_from_url(url: str) -> str:
    match = re.search(r"/nc_(\d+)/", url)
    if not match:
        raise ValueError(f"could not parse listing id from {url}")
    return match.group(1)


def page_urls_for_seed(session: requests.Session, seed: StationSeed) -> list[str]:
    base = f"{BASE_URL}/ms/chuko/tokyo/ek_{seed.code}/"
    first = soup_for(session, base)
    pages = {1}
    for anchor in first.select("a[href]"):
        href = anchor.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            pages.add(int(match.group(1)))
    return [base if page == 1 else f"{base}?page={page}&rn=0305" for page in range(1, max(pages) + 1)]


def collect_listings(session: requests.Session) -> dict[str, dict]:
    collected: dict[str, dict] = {}
    for seed in SEEDS:
        for page_url in page_urls_for_seed(session, seed):
            soup = soup_for(session, page_url)
            for item in soup.select("div.property_unit"):
                title_anchor = item.select_one("h2 a[href]")
                if not title_anchor:
                    continue
                detail_url = urljoin(BASE_URL, title_anchor["href"])
                listing_id = listing_id_from_url(detail_url)
                rows = extract_rows_from_listing(item)
                record = collected.setdefault(
                    listing_id,
                    {
                        "listing_id": listing_id,
                        "url": detail_url,
                        "title": title_anchor.get_text(" ", strip=True),
                        "property_name": rows.get("物件名", ""),
                        "address": rows.get("所在地", ""),
                        "access_text": rows.get("沿線・駅", ""),
                        "price_man": parse_price_man(rows.get("販売価格")),
                        "area_sqm": parse_float(rows.get("専有面積")),
                        "layout": normalize_layout(rows.get("間取り")),
                        "balcony_sqm": parse_float(rows.get("バルコニー")),
                        "walk_min": parse_walk_min(rows.get("沿線・駅")),
                        "built_year": parse_year(rows.get("築年月")),
                        "built_text": rows.get("築年月", ""),
                        "list_blurb": item.select_one("div.storecomment-txt").get_text(" ", strip=True)
                        if item.select_one("div.storecomment-txt")
                        else "",
                        "station_hits": [],
                    },
                )
                if not record.get("list_blurb") and item.select_one("div.moreinfo"):
                    record["list_blurb"] = item.select_one("div.moreinfo").get_text(" ", strip=True)
                record["station_hits"].append(
                    {
                        "station_name": seed.name,
                        "station_code": seed.code,
                        "priority": seed.priority,
                        "note": seed.note,
                        "source_url": page_url,
                    }
                )
    return collected


def listing_prefilter(record: dict) -> bool:
    price = record.get("price_man") or 0
    area = record.get("area_sqm") or 0
    walk = record.get("walk_min") or 999
    year = record.get("built_year") or 0
    layout = record.get("layout") or ""
    rooms = layout_room_count(layout) or 0
    return (
        8000 <= price <= 18000
        and area >= 60
        and walk <= 12
        and year >= 1995
        and rooms >= 2
    )


def extract_detail_summary(soup: BeautifulSoup) -> tuple[str, list[str]]:
    feature_header = None
    for heading in soup.find_all(["h2", "h3"]):
        text = heading.get_text(" ", strip=True)
        if text == "物件の特徴":
            feature_header = heading
            break
    if not feature_header:
        return "", []

    section = feature_header.parent
    raw = section.get_text("\n", strip=True)
    raw = raw.split("イベント情報", 1)[0]
    raw = raw.split("物件詳細情報", 1)[0]
    raw = raw.replace("物件の特徴", "", 1).strip()
    lines = [line.strip(" -/") for line in raw.splitlines()]
    lines = [line for line in lines if line and line != "特徴ピックアップ"]

    tags: list[str] = []
    for line in lines:
        if "/" in line:
            tags.extend([part.strip() for part in line.split("/") if part.strip()])
    summary_lines = [line for line in lines if "/" not in line][:8]
    return " ".join(summary_lines).strip(), tags


def extract_overview(soup: BeautifulSoup) -> dict[str, str]:
    overview: dict[str, str] = {}
    for table in soup.select("table"):
        rows = []
        for tr in table.select("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in tr.select("th,td")]
            if cells:
                rows.append(cells)
        for row in rows:
            if len(row) >= 2:
                overview[row[0].replace(" ヒント", "")] = row[1]
            if len(row) >= 4:
                overview[row[2].replace(" ヒント", "")] = row[3]
    return overview


def enrich_details(session: requests.Session, listings: dict[str, dict]) -> None:
    for record in listings.values():
        if not listing_prefilter(record):
            continue
        soup = soup_for(session, record["url"])
        page_text = soup.get_text(" ", strip=True)
        summary, tags = extract_detail_summary(soup)
        overview = extract_overview(soup)
        record["detail_summary"] = summary
        record["feature_tags"] = sorted(dict.fromkeys(tags))
        record["overview"] = overview
        record["dishwasher_hits"] = [kw for kw in DISHWASHER_KEYWORDS if kw in page_text]
        record["brightness_hits"] = [kw for kw in BRIGHTNESS_KEYWORDS if kw in page_text]
        record["ceiling_hits"] = [kw for kw in CEILING_WINDOW_KEYWORDS if kw in page_text]
        if not record.get("access_text") and overview.get("交通"):
            record["access_text"] = overview["交通"]
        if not record.get("address") and overview.get("所在地"):
            record["address"] = overview["所在地"]
        if not record.get("walk_min"):
            record["walk_min"] = parse_walk_min(record.get("access_text"))
        if not record.get("built_year"):
            record["built_year"] = parse_year(overview.get("完成時期(築年月)") or overview.get("完成時期（築年月）"))
        if not record.get("layout"):
            record["layout"] = normalize_layout(overview.get("間取り"))
        if not record.get("area_sqm"):
            record["area_sqm"] = parse_float(overview.get("専有面積"))


def station_groups(record: dict) -> tuple[list[str], list[str]]:
    exact = sorted({hit["station_name"] for hit in record["station_hits"] if hit["priority"] == "exact"})
    nearby = sorted({hit["station_name"] for hit in record["station_hits"] if hit["priority"] != "exact"})
    return exact, nearby


def price_score(price_man: float | None) -> float:
    if not price_man:
        return -6
    if 10000 <= price_man <= 15000:
        return max(6, 16 - abs(price_man - 13000) / 250)
    if 9000 <= price_man < 10000 or 15000 < price_man <= 16000:
        return 2
    return -8


def area_score(area_sqm: float | None) -> float:
    if not area_sqm:
        return -6
    if area_sqm >= 75:
        return 18
    if area_sqm >= 70:
        return 16
    if area_sqm >= 65:
        return 12
    if area_sqm >= 60:
        return 3
    return -10


def layout_score(layout: str) -> float:
    if not layout:
        return -8
    rooms = layout_room_count(layout)
    if not rooms:
        return -8
    if ("SLDK" in layout or "+S" in layout) and rooms >= 2:
        return 16
    if "LDK" in layout and rooms >= 3:
        return 15
    if "LDK" in layout and rooms >= 2:
        return 12
    return -8


def walk_score(walk_min: int | None) -> float:
    if walk_min is None:
        return -4
    if walk_min <= 5:
        return 14
    if walk_min <= 10:
        return 12
    if walk_min <= 12:
        return 4
    return -10


def year_score(year: int | None) -> float:
    if not year:
        return -4
    if year >= 2015:
        return 13
    if year >= 2005:
        return 11
    if year >= 2000:
        return 9
    if year >= 1995:
        return 2
    return -8


def station_score(record: dict) -> float:
    exact, nearby = station_groups(record)
    if exact:
        return 14 + min(4, len(exact))
    if nearby:
        return 7 + min(3, len(nearby))
    return 0


def keyword_score(record: dict) -> float:
    dishwasher = 10 if record.get("dishwasher_hits") else -3
    bright = min(8, len(record.get("brightness_hits", [])) * 1.5)
    ceiling = min(5, len(record.get("ceiling_hits", [])) * 2)
    return dishwasher + bright + ceiling


def build_notes(record: dict) -> list[str]:
    notes: list[str] = []
    exact, nearby = station_groups(record)
    if exact:
        notes.append(f"exact target station match: {', '.join(exact)}")
    elif nearby:
        notes.append(f"nearby target-area station match: {', '.join(nearby)}")

    area = record.get("area_sqm")
    if area:
        if area >= 70:
            notes.append(f"size clears ideal threshold at {area:.2f} sqm")
        elif area >= 65:
            notes.append(f"size clears hard threshold at {area:.2f} sqm")
        else:
            notes.append(f"size is below target at {area:.2f} sqm")

    layout = record.get("layout") or ""
    if layout:
        notes.append(f"layout: {layout}")

    price_man = record.get("price_man")
    if price_man:
        if 10000 <= price_man <= 15000:
            notes.append(f"price is within budget at {price_man:.0f}万円")
        else:
            notes.append(f"price is outside target budget at {price_man:.0f}万円")

    walk = record.get("walk_min")
    if walk is not None:
        if walk <= 10:
            notes.append(f"walk time meets target at {walk} min")
        else:
            notes.append(f"walk time misses target at {walk} min")

    year = record.get("built_year")
    if year:
        if year >= 2000:
            notes.append(f"built in {year}")
        else:
            notes.append(f"older build year: {year}")

    if record.get("dishwasher_hits"):
        notes.append("dishwasher mentioned in listing")
    else:
        notes.append("dishwasher not explicitly confirmed")

    if record.get("brightness_hits"):
        notes.append("brightness/window positives: " + ", ".join(record["brightness_hits"][:4]))
    if record.get("ceiling_hits"):
        notes.append("ceiling/window-height positives: " + ", ".join(record["ceiling_hits"][:3]))
    return notes


def score_listing(record: dict) -> float:
    score = 0.0
    score += station_score(record)
    score += area_score(record.get("area_sqm"))
    score += layout_score(record.get("layout", ""))
    score += price_score(record.get("price_man"))
    score += walk_score(record.get("walk_min"))
    score += year_score(record.get("built_year"))
    score += keyword_score(record)
    if not record.get("detail_summary"):
        score -= 2
    record["criteria_notes"] = build_notes(record)
    record["score"] = round(score, 2)
    return record["score"]


def persist(conn: sqlite3.Connection, listings: dict[str, dict]) -> None:
    scraped_at = now_iso()
    for record in listings.values():
        exact_hits, nearby_hits = station_groups(record)
        conn.execute(
            """
            INSERT INTO listings (
              listing_id, url, title, property_name, address, access_text,
              price_man, area_sqm, layout, balcony_sqm, walk_min, built_year,
              built_text, list_blurb, detail_summary, feature_tags_json, overview_json,
              exact_station_hits_json, nearby_station_hits_json, dishwasher,
              brightness_hits_json, ceiling_hits_json, score, criteria_notes_json,
              scraped_at, detail_scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
              url=excluded.url,
              title=excluded.title,
              property_name=excluded.property_name,
              address=excluded.address,
              access_text=excluded.access_text,
              price_man=excluded.price_man,
              area_sqm=excluded.area_sqm,
              layout=excluded.layout,
              balcony_sqm=excluded.balcony_sqm,
              walk_min=excluded.walk_min,
              built_year=excluded.built_year,
              built_text=excluded.built_text,
              list_blurb=excluded.list_blurb,
              detail_summary=excluded.detail_summary,
              feature_tags_json=excluded.feature_tags_json,
              overview_json=excluded.overview_json,
              exact_station_hits_json=excluded.exact_station_hits_json,
              nearby_station_hits_json=excluded.nearby_station_hits_json,
              dishwasher=excluded.dishwasher,
              brightness_hits_json=excluded.brightness_hits_json,
              ceiling_hits_json=excluded.ceiling_hits_json,
              score=excluded.score,
              criteria_notes_json=excluded.criteria_notes_json,
              scraped_at=excluded.scraped_at,
              detail_scraped_at=excluded.detail_scraped_at
            """,
            (
                record["listing_id"],
                record["url"],
                record.get("title"),
                record.get("property_name"),
                record.get("address"),
                record.get("access_text"),
                record.get("price_man"),
                record.get("area_sqm"),
                record.get("layout"),
                record.get("balcony_sqm"),
                record.get("walk_min"),
                record.get("built_year"),
                record.get("built_text"),
                record.get("list_blurb"),
                record.get("detail_summary", ""),
                json.dumps(record.get("feature_tags", []), ensure_ascii=False),
                json.dumps(record.get("overview", {}), ensure_ascii=False),
                json.dumps(exact_hits, ensure_ascii=False),
                json.dumps(nearby_hits, ensure_ascii=False),
                1 if record.get("dishwasher_hits") else 0,
                json.dumps(record.get("brightness_hits", []), ensure_ascii=False),
                json.dumps(record.get("ceiling_hits", []), ensure_ascii=False),
                record.get("score"),
                json.dumps(record.get("criteria_notes", []), ensure_ascii=False),
                scraped_at,
                scraped_at if record.get("detail_summary") else None,
            ),
        )
        for hit in record["station_hits"]:
            conn.execute(
                """
                INSERT INTO listing_station_hits (
                  listing_id, station_name, station_code, priority, note, source_url, scraped_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(listing_id, station_code) DO UPDATE SET
                  station_name=excluded.station_name,
                  priority=excluded.priority,
                  note=excluded.note,
                  source_url=excluded.source_url,
                  scraped_at=excluded.scraped_at
                """,
                (
                    record["listing_id"],
                    hit["station_name"],
                    hit["station_code"],
                    hit["priority"],
                    hit["note"],
                    hit["source_url"],
                    scraped_at,
                ),
            )
    conn.commit()


def top_candidates(listings: Iterable[dict], limit: int = 10) -> list[dict]:
    ranked = sorted(listings, key=lambda record: record.get("score", float("-inf")), reverse=True)
    return ranked[:limit]


def render_report(candidates: list[dict], path: Path) -> None:
    lines = [
        "# SUUMO used mansion shortlist",
        "",
        f"Generated at: {datetime.now().astimezone().isoformat()}",
        "",
    ]
    for idx, record in enumerate(candidates, start=1):
        exact, nearby = station_groups(record)
        station_label = ", ".join(exact) if exact else ", ".join(nearby)
        lines.extend(
            [
                f"## {idx}. {record.get('property_name') or record.get('title')}",
                "",
                f"- Score: {record.get('score')}",
                f"- URL: {record.get('url')}",
                f"- Stations: {station_label or 'n/a'}",
                f"- Price: {record.get('price_man', 0):.0f}万円",
                f"- Size: {record.get('area_sqm', 0):.2f} sqm",
                f"- Layout: {record.get('layout') or 'n/a'}",
                f"- Walk: {record.get('walk_min')} min",
                f"- Built: {record.get('built_year') or 'n/a'}",
                f"- Dishwasher: {'yes' if record.get('dishwasher_hits') else 'not confirmed'}",
                f"- Address: {record.get('address') or 'n/a'}",
                f"- Access: {record.get('access_text') or 'n/a'}",
                f"- Listing summary: {record.get('detail_summary') or record.get('list_blurb') or 'n/a'}",
                f"- Notes: {'; '.join(record.get('criteria_notes', []))}",
                "",
            ]
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def render_json(candidates: list[dict], path: Path) -> None:
    payload = []
    for record in candidates:
        exact, nearby = station_groups(record)
        payload.append(
            {
                "listing_id": record["listing_id"],
                "property_name": record.get("property_name"),
                "title": record.get("title"),
                "url": record.get("url"),
                "price_man": record.get("price_man"),
                "area_sqm": record.get("area_sqm"),
                "layout": record.get("layout"),
                "walk_min": record.get("walk_min"),
                "built_year": record.get("built_year"),
                "address": record.get("address"),
                "access_text": record.get("access_text"),
                "exact_station_hits": exact,
                "nearby_station_hits": nearby,
                "dishwasher_hits": record.get("dishwasher_hits", []),
                "brightness_hits": record.get("brightness_hits", []),
                "ceiling_hits": record.get("ceiling_hits", []),
                "feature_tags": record.get("feature_tags", []),
                "detail_summary": record.get("detail_summary"),
                "criteria_notes": record.get("criteria_notes", []),
                "score": record.get("score"),
            }
        )
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def main() -> int:
    data_dir, output_dir = ensure_dirs()
    db_path = data_dir / "suumo_listings.sqlite3"
    session = build_session()
    listings = collect_listings(session)
    enrich_details(session, listings)
    for record in listings.values():
        score_listing(record)

    conn = connect_db(db_path)
    persist(conn, listings)

    candidates = [record for record in listings.values() if record.get("score", -999) > 0]
    shortlist = top_candidates(candidates, 10)
    render_report(shortlist, output_dir / "top10.md")
    render_json(shortlist, output_dir / "top10.json")

    print(f"scraped {len(listings)} unique listings")
    print(f"ranked {len(candidates)} positive-score candidates")
    print(f"database: {db_path}")
    print(f"report: {output_dir / 'top10.md'}")
    print(f"json: {output_dir / 'top10.json'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
