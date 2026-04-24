#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup


SUUMO_BASE_URL = "https://suumo.jp"
KEN_BASE_URL = "https://www.kencorp.co.jp"
USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36"
)
SHORTLIST_LIMIT = 15
TARGET_BUDGET_MIN_MAN = 8000
TARGET_BUDGET_MAX_MAN = 15000
HARD_BUDGET_MAX_MAN = 16000
IDEAL_PRICE_MAN = 11000


@dataclass(frozen=True)
class StationSeed:
    name: str
    code: str
    priority: str
    note: str


@dataclass(frozen=True)
class PropertyConfig:
    kind: str
    label: str
    base_path: str
    db_table: str
    hits_table: str
    size_field: str
    walk_target: int
    detail_prefilter_walk: int
    output_md: str
    output_json: str


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
    StationSeed("吉祥寺", "11640", "exact", "user-added exact target"),
    StationSeed("代々木八幡", "41310", "nearby", "adjacent to 代々木公園/代々木上原"),
    StationSeed("恵比寿", "05050", "nearby", "adjacent to 代官山"),
]

MANSION = PropertyConfig(
    kind="mansion",
    label="used mansion",
    base_path="ms/chuko/tokyo",
    db_table="listings",
    hits_table="listing_station_hits",
    size_field="専有面積",
    walk_target=10,
    detail_prefilter_walk=12,
    output_md="top15_mansions.md",
    output_json="top15_mansions.json",
)
HOUSE = PropertyConfig(
    kind="house",
    label="used house",
    base_path="chukoikkodate/tokyo",
    db_table="house_listings",
    hits_table="house_listing_station_hits",
    size_field="建物面積",
    walk_target=12,
    detail_prefilter_walk=14,
    output_md="top15_houses.md",
    output_json="top15_houses.json",
)

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


def today_local() -> date:
    return datetime.now().date()


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
          source TEXT,
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
          identity_key TEXT,
          first_seen_at TEXT,
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

        CREATE TABLE IF NOT EXISTS house_listings (
          listing_id TEXT PRIMARY KEY,
          source TEXT,
          url TEXT NOT NULL,
          title TEXT,
          property_name TEXT,
          address TEXT,
          access_text TEXT,
          price_man REAL,
          area_sqm REAL,
          land_area_sqm REAL,
          layout TEXT,
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
          identity_key TEXT,
          first_seen_at TEXT,
          score REAL,
          criteria_notes_json TEXT,
          scraped_at TEXT NOT NULL,
          detail_scraped_at TEXT
        );

        CREATE TABLE IF NOT EXISTS house_listing_station_hits (
          listing_id TEXT NOT NULL,
          station_name TEXT NOT NULL,
          station_code TEXT NOT NULL,
          priority TEXT NOT NULL,
          note TEXT,
          source_url TEXT NOT NULL,
          scraped_at TEXT NOT NULL,
          PRIMARY KEY (listing_id, station_code)
        );

        CREATE TABLE IF NOT EXISTS listing_identities (
          identity_key TEXT PRIMARY KEY,
          property_type TEXT NOT NULL,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          latest_listing_id TEXT,
          latest_url TEXT,
          address TEXT,
          built_year INTEGER,
          area_sqm REAL,
          land_area_sqm REAL,
          layout TEXT
        );
        """
    )
    ensure_columns(
        conn,
        "listings",
        {"identity_key": "TEXT", "first_seen_at": "TEXT", "source": "TEXT"},
    )
    ensure_columns(
        conn,
        "house_listings",
        {"identity_key": "TEXT", "first_seen_at": "TEXT", "source": "TEXT"},
    )
    conn.execute("UPDATE listings SET source = 'suumo' WHERE source IS NULL")
    conn.execute("UPDATE house_listings SET source = 'suumo' WHERE source IS NULL")
    conn.commit()
    return conn


def ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> None:
    existing = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
    for name, column_type in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {column_type}")
    conn.commit()


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    return session


def fetch(session: requests.Session, url: str, *, sleep_s: float = 0.12) -> str:
    last_exc: Exception | None = None
    for attempt in range(5):
        try:
            response = session.get(url, timeout=30)
            response.raise_for_status()
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
        return float(match.group(1) or 0) * 10000 + float(match.group(2) or 0)
    match = re.search(r"(\d+(?:\.\d+)?)", compact)
    return float(match.group(1)) if match else None


def parse_yyyymm_year(text: str | None) -> int | None:
    if not text:
        return None
    match = re.match(r"(\d{4})(\d{2})$", text.strip())
    return int(match.group(1)) if match else None


def parse_year(text: str | None) -> int | None:
    if not text:
        return None
    match = re.search(r"(\d{4})年", text)
    return int(match.group(1)) if match else None


def parse_jp_date(text: str | None) -> date | None:
    if not text:
        return None
    match = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", text)
    if not match:
        return None
    return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))


def parse_walk_min(text: str | None) -> int | None:
    if not text:
        return None
    values = [int(n) for n in re.findall(r"(?<!停)歩(\d+)分", text)]
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


def page_urls_for_seed(session: requests.Session, seed: StationSeed, config: PropertyConfig) -> list[str]:
    base = f"{SUUMO_BASE_URL}/{config.base_path}/ek_{seed.code}/"
    first = soup_for(session, base)
    pages = {1}
    for anchor in first.select("a[href]"):
        href = anchor.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            pages.add(int(match.group(1)))
    return [base if page == 1 else f"{base}?page={page}&rn=0305" for page in range(1, max(pages) + 1)]


def collect_suumo_listings(session: requests.Session, config: PropertyConfig) -> dict[str, dict]:
    collected: dict[str, dict] = {}
    for seed in SEEDS:
        for page_url in page_urls_for_seed(session, seed, config):
            soup = soup_for(session, page_url)
            for item in soup.select("div.property_unit"):
                title_anchor = item.select_one("h2 a[href]")
                if not title_anchor:
                    continue
                detail_url = urljoin(SUUMO_BASE_URL, title_anchor["href"])
                listing_id = listing_id_from_url(detail_url)
                rows = extract_rows_from_listing(item)
                record = collected.setdefault(
                    listing_id,
                    {
                        "listing_id": listing_id,
                        "source": "suumo",
                        "property_type": config.kind,
                        "url": detail_url,
                        "title": title_anchor.get_text(" ", strip=True),
                        "property_name": rows.get("物件名", ""),
                        "address": rows.get("所在地", ""),
                        "access_text": rows.get("沿線・駅", ""),
                        "price_man": parse_price_man(rows.get("販売価格")),
                        "area_sqm": parse_float(rows.get(config.size_field)),
                        "land_area_sqm": parse_float(rows.get("土地面積")),
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


def listing_prefilter(record: dict, config: PropertyConfig) -> bool:
    price = record.get("price_man") or 0
    area = record.get("area_sqm") or 0
    walk = record.get("walk_min") or 999
    year = record.get("built_year") or 0
    rooms = layout_room_count(record.get("layout") or "") or 0
    return 8000 <= price <= HARD_BUDGET_MAX_MAN and area >= 60 and walk <= config.detail_prefilter_walk and year >= 1995 and rooms >= 2


def strict_match(record: dict, config: PropertyConfig) -> bool:
    price = record.get("price_man") or 0
    area = record.get("area_sqm") or 0
    walk = record.get("walk_min") or 999
    year = record.get("built_year") or 0
    layout = record.get("layout") or ""
    rooms = layout_room_count(layout) or 0
    return (
        TARGET_BUDGET_MIN_MAN <= price <= TARGET_BUDGET_MAX_MAN
        and area >= 65
        and walk <= config.walk_target
        and year >= 2000
        and rooms >= 2
        and ("LDK" in layout or "SLDK" in layout or "+S" in layout)
        and has_freehold_land_rights(record, config)
    )


def extract_detail_summary(soup: BeautifulSoup) -> tuple[str, list[str]]:
    feature_header = None
    for heading in soup.find_all(["h2", "h3"]):
        if heading.get_text(" ", strip=True) == "物件の特徴":
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
    return " ".join(summary_lines).strip(), sorted(dict.fromkeys(tags))


def extract_overview(soup: BeautifulSoup) -> dict[str, str]:
    overview: dict[str, str] = {}
    for table in soup.select("table"):
        for tr in table.select("tr"):
            cells = [cell.get_text(" ", strip=True) for cell in tr.select("th,td")]
            if len(cells) >= 2:
                overview[cells[0].replace(" ヒント", "")] = cells[1]
            if len(cells) >= 4:
                overview[cells[2].replace(" ヒント", "")] = cells[3]
    return overview


def extract_preview_image_url(soup: BeautifulSoup, page_url: str = "") -> str:
    for node in soup.select("img.js-scrollLazy-image[rel], input[id$='orgn'][value]"):
        value = (node.get("rel") or node.get("value") or "").strip()
        if not value:
            continue
        value = value.split(",", 1)[0].strip()
        if "img01.suumo.com" in value:
            return value
    selectors = [
        ('meta[property="og:image"]', "content"),
        ('meta[name="twitter:image"]', "content"),
        ("img[src]", "src"),
    ]
    for selector, attr in selectors:
        for node in soup.select(selector):
            value = (node.get(attr) or "").strip()
            if not value or value.startswith("data:"):
                continue
            resolved = urljoin(page_url or SUUMO_BASE_URL, value)
            if "/edit/assets/" in resolved or "/jj/jjcommon/" in resolved:
                continue
            return resolved
    return ""


def enrich_suumo_details(session: requests.Session, listings: dict[str, dict], config: PropertyConfig) -> None:
    for record in listings.values():
        if not listing_prefilter(record, config):
            continue
        soup = soup_for(session, record["url"])
        page_text = soup.get_text(" ", strip=True)
        summary, tags = extract_detail_summary(soup)
        overview = extract_overview(soup)
        record["detail_summary"] = summary
        record["feature_tags"] = tags
        record["overview"] = overview
        record["preview_image_url"] = extract_preview_image_url(soup, record["url"])
        record["dishwasher_hits"] = [kw for kw in DISHWASHER_KEYWORDS if kw in page_text]
        record["brightness_hits"] = [kw for kw in BRIGHTNESS_KEYWORDS if kw in page_text]
        record["ceiling_hits"] = [kw for kw in CEILING_WINDOW_KEYWORDS if kw in page_text]
        record["access_text"] = record.get("access_text") or overview.get("交通", "")
        record["address"] = record.get("address") or overview.get("所在地", "")
        record["walk_min"] = record.get("walk_min") or parse_walk_min(record.get("access_text"))
        record["built_year"] = record.get("built_year") or parse_year(
            overview.get("完成時期(築年月)") or overview.get("完成時期（築年月）") or overview.get("築年月")
        )
        record["layout"] = record.get("layout") or normalize_layout(overview.get("間取り"))
        record["area_sqm"] = record.get("area_sqm") or parse_float(overview.get(config.size_field))
        record["land_area_sqm"] = record.get("land_area_sqm") or parse_float(overview.get("土地面積"))


def station_groups(record: dict) -> tuple[list[str], list[str]]:
    exact = sorted({hit["station_name"] for hit in record["station_hits"] if hit["priority"] == "exact"})
    nearby = sorted({hit["station_name"] for hit in record["station_hits"] if hit["priority"] != "exact"})
    return exact, nearby


def price_score(price_man: float | None) -> float:
    if not price_man:
        return -6
    distance_kman = abs(price_man - IDEAL_PRICE_MAN) / 1000.0
    raw_score = 10.5 - 1.1 * distance_kman - 0.35 * (distance_kman**2)
    if TARGET_BUDGET_MIN_MAN <= price_man <= TARGET_BUDGET_MAX_MAN:
        return round(max(0.5, raw_score), 2)
    if 7000 <= price_man < TARGET_BUDGET_MIN_MAN or TARGET_BUDGET_MAX_MAN < price_man <= HARD_BUDGET_MAX_MAN:
        return round(max(-3.0, raw_score - 3.0), 2)
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


def walk_score(walk_min: int | None, config: PropertyConfig) -> float:
    if walk_min is None:
        return -4
    ideal_walk = 3.0
    max_walk = float(config.detail_prefilter_walk)
    if walk_min <= ideal_walk:
        return 20.0
    if walk_min <= max_walk:
        span = max_walk - ideal_walk
        progress = (walk_min - ideal_walk) / span if span else 1.0
        # Convex decay: a 1-minute increase near the station hurts more.
        return round(20.0 - 18.0 * (progress ** 0.85), 2)
    overage = walk_min - max_walk
    return round(max(-10.0, 2.0 - 2.5 * overage), 2)


def year_score(year: int | None) -> float:
    if not year:
        return -4
    age = max(0, today_local().year - year)
    if age <= 10:
        # Gentle decay for recent buildings.
        return round(13.0 - 0.2 * age, 2)
    if age <= 26:
        # Drop from ~11 at age 10 toward ~2 by age 26.
        progress = (age - 10) / 16.0
        return round(11.0 - 9.0 * (progress ** 1.15), 2)
    overage = age - 26
    return round(max(-8.0, 2.0 - 0.65 * overage), 2)


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


def house_land_rights_text(record: dict) -> str:
    overview = record.get("overview", {})
    fields = [
        overview.get("土地権利", ""),
        overview.get("土地権利・借地権", ""),
        overview.get("借地期間・地代", ""),
        record.get("detail_summary", ""),
        record.get("title", ""),
    ]
    return " ".join(field for field in fields if field)


def has_freehold_land_rights(record: dict, config: PropertyConfig) -> bool:
    if config.kind != "house":
        return True
    text = house_land_rights_text(record)
    if not text:
        return True
    if "所有権" in text:
        return True
    blocked_terms = ["借地権", "旧法借地権", "新法借地権", "定期借地権", "地上権", "賃借権"]
    return not any(term in text for term in blocked_terms)


def rounded_metric(value: float | None) -> str:
    if value is None:
        return ""
    return f"{value:.1f}"


def identity_fingerprint(record: dict, config: PropertyConfig) -> str:
    address = normalize_address(record.get("address", ""))
    name = normalize_name(record.get("property_name") or record.get("title") or "")
    year = str(record.get("built_year") or "")
    area = rounded_metric(record.get("area_sqm"))
    land = rounded_metric(record.get("land_area_sqm")) if config.kind == "house" else ""
    layout = normalize_layout(record.get("layout") or "")
    anchor = address or name or record["listing_id"]
    return "|".join(part for part in [config.kind, anchor, year, area, land, layout] if part)


def load_identity_history(conn: sqlite3.Connection, config: PropertyConfig) -> dict[str, str]:
    rows = conn.execute(
        """
        SELECT identity_key, first_seen_at
        FROM listing_identities
        WHERE property_type = ?
        """,
        (config.kind,),
    )
    return {row["identity_key"]: row["first_seen_at"] for row in rows}


def attach_identity_history(conn: sqlite3.Connection, listings: dict[str, dict], config: PropertyConfig) -> None:
    history = load_identity_history(conn, config)
    run_seen_at = now_iso()
    for record in listings.values():
        identity_key = identity_fingerprint(record, config)
        record["identity_key"] = identity_key
        record["first_seen_at"] = history.get(identity_key, run_seen_at)


def build_notes(record: dict, config: PropertyConfig) -> list[str]:
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
    if record.get("layout"):
        notes.append(f"layout: {record['layout']}")
    price_man = record.get("price_man")
    if price_man:
        if TARGET_BUDGET_MIN_MAN <= price_man <= TARGET_BUDGET_MAX_MAN:
            notes.append(f"price is within budget at {price_man:.0f}万円")
        else:
            notes.append(f"price is outside target budget at {price_man:.0f}万円")
    walk = record.get("walk_min")
    if walk is not None:
        if walk <= config.walk_target:
            notes.append(f"walk time meets target at {walk} min")
        else:
            notes.append(f"walk time misses target at {walk} min")
    year = record.get("built_year")
    if year:
        if year >= 2000:
            notes.append(f"built in {year}")
        else:
            notes.append(f"older build year: {year}")
    if config.kind == "house":
        land_rights = house_land_rights_text(record)
        if land_rights:
            if has_freehold_land_rights(record, config):
                if "所有権" in land_rights:
                    notes.append("land rights: freehold / 所有権")
            else:
                notes.append("land rights are not freehold")
    if record.get("dishwasher_hits"):
        notes.append("dishwasher mentioned in listing")
    else:
        notes.append("dishwasher not explicitly confirmed")
    if record.get("brightness_hits"):
        notes.append("brightness/window positives: " + ", ".join(record["brightness_hits"][:4]))
    if record.get("ceiling_hits"):
        notes.append("ceiling/window-height positives: " + ", ".join(record["ceiling_hits"][:3]))
    if is_basement_like(record):
        notes.append("basement / semi-basement indicators present")
    info_date = parse_jp_date(record.get("overview", {}).get("情報提供日"))
    if info_date:
        days_old = (today_local() - info_date).days
        notes.append(f"listing age: {days_old} days")
    first_seen_at = record.get("first_seen_at")
    if first_seen_at:
        first_seen_date = datetime.fromisoformat(first_seen_at).date()
        notes.append(f"first seen: {first_seen_date.isoformat()} ({(today_local() - first_seen_date).days} days ago)")
    return notes


def is_basement_like(record: dict) -> bool:
    fields = [
        record.get("title", ""),
        record.get("property_name", ""),
        record.get("detail_summary", ""),
        record.get("list_blurb", ""),
        record.get("overview", {}).get("所在階", ""),
        record.get("overview", {}).get("所在階/構造・階建", ""),
    ]
    text = " ".join(field for field in fields if field)
    basement_terms = ["地下", "半地下", "B1", "地下1階", "メゾネット"]
    return any(term in text for term in basement_terms)


def basement_score(record: dict) -> float:
    if not is_basement_like(record):
        return 0.0
    return -14.0


def house_story_score(record: dict, config: PropertyConfig) -> float:
    if config.kind != "house":
        return 0.0
    text = " ".join(
        [
            record.get("title", ""),
            record.get("property_name", ""),
            record.get("detail_summary", ""),
            record.get("list_blurb", ""),
            record.get("overview", {}).get("構造・工法", ""),
        ]
    )
    if "2階建" in text:
        return 3.0
    if "3階建" in text:
        return -3.0
    return 0.0


def freshness_score(record: dict) -> float:
    info_date = parse_jp_date(record.get("overview", {}).get("情報提供日"))
    first_seen_at = record.get("first_seen_at")
    first_seen_date = datetime.fromisoformat(first_seen_at).date() if first_seen_at else None
    ages = []
    if info_date:
        ages.append((today_local() - info_date).days)
    if first_seen_date:
        ages.append((today_local() - first_seen_date).days)
    if not ages:
        return 0.0
    effective_days_old = max(ages)
    if effective_days_old <= 3:
        return 2.0
    if effective_days_old <= 7:
        return 1.0
    if effective_days_old <= 14:
        return 0.0
    if effective_days_old <= 30:
        return -0.5
    if effective_days_old <= 60:
        return -1.0
    return -2.0


def score_listing(record: dict, config: PropertyConfig) -> float:
    price_man = record.get("price_man")
    if price_man and price_man > HARD_BUDGET_MAX_MAN:
        record["criteria_notes"] = build_notes(record, config)
        record["score"] = -999.0
        return record["score"]
    if not has_freehold_land_rights(record, config):
        record["criteria_notes"] = build_notes(record, config)
        record["score"] = -999.0
        return record["score"]
    score = 0.0
    score += station_score(record)
    score += area_score(record.get("area_sqm"))
    score += layout_score(record.get("layout", ""))
    score += price_score(record.get("price_man"))
    score += walk_score(record.get("walk_min"), config)
    score += year_score(record.get("built_year"))
    score += keyword_score(record)
    score += basement_score(record)
    score += house_story_score(record, config)
    score += freshness_score(record)
    if not record.get("detail_summary"):
        score -= 2
    if config.kind == "house" and record.get("land_area_sqm"):
        score += min(4, max(0.0, (record["land_area_sqm"] - 80) / 20))
    record["criteria_notes"] = build_notes(record, config)
    record["score"] = round(score, 2)
    return record["score"]


def persist_identity_history(conn: sqlite3.Connection, listings: dict[str, dict], config: PropertyConfig) -> None:
    seen_at = now_iso()
    for record in listings.values():
        identity_key = record.get("identity_key")
        first_seen_at = record.get("first_seen_at")
        if not identity_key or not first_seen_at:
            continue
        conn.execute(
            """
            INSERT INTO listing_identities (
              identity_key, property_type, first_seen_at, last_seen_at,
              latest_listing_id, latest_url, address, built_year, area_sqm, land_area_sqm, layout
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(identity_key) DO UPDATE SET
              last_seen_at=excluded.last_seen_at,
              latest_listing_id=excluded.latest_listing_id,
              latest_url=excluded.latest_url,
              address=excluded.address,
              built_year=excluded.built_year,
              area_sqm=excluded.area_sqm,
              land_area_sqm=excluded.land_area_sqm,
              layout=excluded.layout
            """,
            (
                identity_key,
                config.kind,
                first_seen_at,
                seen_at,
                record["listing_id"],
                record.get("url"),
                record.get("address"),
                record.get("built_year"),
                record.get("area_sqm"),
                record.get("land_area_sqm"),
                record.get("layout"),
            ),
        )
    conn.commit()


def persist_mansions(conn: sqlite3.Connection, listings: dict[str, dict]) -> None:
    scraped_at = now_iso()
    for record in listings.values():
        exact_hits, nearby_hits = station_groups(record)
        conn.execute(
            """
            INSERT INTO listings (
              listing_id, source, url, title, property_name, address, access_text,
              price_man, area_sqm, layout, balcony_sqm, walk_min, built_year,
              built_text, list_blurb, detail_summary, feature_tags_json, overview_json,
              exact_station_hits_json, nearby_station_hits_json, dishwasher,
              brightness_hits_json, ceiling_hits_json, identity_key, first_seen_at, score, criteria_notes_json,
              scraped_at, detail_scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
              source=excluded.source,
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
              identity_key=excluded.identity_key,
              first_seen_at=excluded.first_seen_at,
              score=excluded.score,
              criteria_notes_json=excluded.criteria_notes_json,
              scraped_at=excluded.scraped_at,
              detail_scraped_at=excluded.detail_scraped_at
            """,
            (
                record["listing_id"],
                record.get("source", "suumo"),
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
                record.get("identity_key"),
                record.get("first_seen_at"),
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
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


def persist_houses(conn: sqlite3.Connection, listings: dict[str, dict]) -> None:
    scraped_at = now_iso()
    for record in listings.values():
        exact_hits, nearby_hits = station_groups(record)
        conn.execute(
            """
            INSERT INTO house_listings (
              listing_id, source, url, title, property_name, address, access_text, price_man,
              area_sqm, land_area_sqm, layout, walk_min, built_year, built_text, list_blurb,
              detail_summary, feature_tags_json, overview_json, exact_station_hits_json,
              nearby_station_hits_json, dishwasher, brightness_hits_json, ceiling_hits_json,
              identity_key, first_seen_at, score, criteria_notes_json, scraped_at, detail_scraped_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(listing_id) DO UPDATE SET
              source=excluded.source,
              url=excluded.url,
              title=excluded.title,
              property_name=excluded.property_name,
              address=excluded.address,
              access_text=excluded.access_text,
              price_man=excluded.price_man,
              area_sqm=excluded.area_sqm,
              land_area_sqm=excluded.land_area_sqm,
              layout=excluded.layout,
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
              identity_key=excluded.identity_key,
              first_seen_at=excluded.first_seen_at,
              score=excluded.score,
              criteria_notes_json=excluded.criteria_notes_json,
              scraped_at=excluded.scraped_at,
              detail_scraped_at=excluded.detail_scraped_at
            """,
            (
                record["listing_id"],
                record.get("source", "suumo"),
                record["url"],
                record.get("title"),
                record.get("property_name"),
                record.get("address"),
                record.get("access_text"),
                record.get("price_man"),
                record.get("area_sqm"),
                record.get("land_area_sqm"),
                record.get("layout"),
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
                record.get("identity_key"),
                record.get("first_seen_at"),
                record.get("score"),
                json.dumps(record.get("criteria_notes", []), ensure_ascii=False),
                scraped_at,
                scraped_at if record.get("detail_summary") else None,
            ),
        )
        for hit in record["station_hits"]:
            conn.execute(
                """
                INSERT INTO house_listing_station_hits (
                  listing_id, station_name, station_code, priority, note, source_url, scraped_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
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


def parse_ken_station_options(session: requests.Session) -> dict[str, list[dict[str, str]]]:
    soup = soup_for(session, f"{KEN_BASE_URL}/housing/buy/search/line/")
    options: dict[str, list[dict[str, str]]] = {}
    current_line_name = ""
    for section in soup.select("#content__stations .bl-001_14"):
        title = section.select_one(".bl-001_14__head__title")
        if title:
            current_line_name = title.get_text(" ", strip=True)
        for item in section.select('input[name="line_stations"]'):
            if item.has_attr("disabled"):
                continue
            value = item.get("value", "").strip()
            label = item.find_next("span")
            station_name = label.get_text(" ", strip=True) if label else ""
            if not station_name or not value:
                continue
            options.setdefault(station_name, []).append(
                {"line_station": value, "line_name": current_line_name}
            )
    return options


def ken_result_url(line_station: str, config: PropertyConfig) -> str:
    build_type = "apartment" if config.kind == "mansion" else "detached"
    return (
        f"{KEN_BASE_URL}/_api/search/result/?search_type=buy&search_by=line"
        f"&line_stations={line_station}&build_type={build_type}&per_page=1000&sort_key=_created_at"
    )


def collect_ken_listings(session: requests.Session, config: PropertyConfig) -> dict[str, dict]:
    station_options = parse_ken_station_options(session)
    collected: dict[str, dict] = {}
    for seed in SEEDS:
        for option in station_options.get(seed.name, []):
            payload = fetch(session, ken_result_url(option["line_station"], config), sleep_s=0.05)
            data = json.loads(payload)
            for building in data.get("buildings", []):
                for prop in building.get("properties", []):
                    listing_id = f"ken:{prop['code']}"
                    detail_url = urljoin(KEN_BASE_URL, prop.get("url") or building.get("url") or "")
                    image_path = prop.get("image_1") or ""
                    if not image_path:
                        building_images = building.get("image_path") or []
                        if isinstance(building_images, list) and building_images:
                            image_path = building_images[0]
                        elif isinstance(building_images, str):
                            image_path = building_images
                    record = collected.setdefault(
                        listing_id,
                        {
                            "listing_id": listing_id,
                            "source": "ken",
                            "property_type": config.kind,
                            "url": detail_url,
                            "title": f"{building.get('bldg_name', '')} {building.get('bldg_ridge', '')}".strip(),
                            "property_name": building.get("bldg_name", ""),
                            "address": building.get("address", ""),
                            "access_text": building.get("route", ""),
                            "price_man": parse_price_man(prop.get("price")),
                            "area_sqm": parse_float(str(prop.get("footprint", ""))),
                            "land_area_sqm": parse_float(str(prop.get("site_area", ""))) if config.kind == "house" else None,
                            "layout": normalize_layout(prop.get("layout")),
                            "balcony_sqm": None,
                            "walk_min": parse_walk_min(building.get("route", "")),
                            "built_year": parse_yyyymm_year(building.get("complete_date")),
                            "built_text": building.get("complete_date", ""),
                            "list_blurb": "",
                            "detail_summary": "",
                            "feature_tags": [],
                            "overview": {},
                            "station_hits": [],
                            "preview_image_url": urljoin(KEN_BASE_URL, image_path) if image_path else "",
                            "dishwasher_hits": [],
                            "brightness_hits": [],
                            "ceiling_hits": [],
                        },
                    )
                    record["station_hits"].append(
                        {
                            "station_name": seed.name,
                            "station_code": option["line_station"],
                            "priority": seed.priority,
                            "note": f"KEN {option['line_name']}",
                            "source_url": ken_result_url(option["line_station"], config),
                        }
                    )
    return collected


def extract_ken_feature_tags(soup: BeautifulSoup) -> list[str]:
    tags: list[str] = []
    for row in soup.select("table tr"):
        head = row.select_one("th")
        data = row.select_one("td")
        if not head or not data:
            continue
        head_text = head.get_text(" ", strip=True)
        if head_text in {"特徴", "部屋設備", "建物設備・施設"}:
            for piece in re.split(r"[、/\n]", data.get_text(" ", strip=True)):
                piece = piece.strip()
                if piece:
                    tags.append(piece)
    return sorted(dict.fromkeys(tags))


def extract_ken_detail_summary(soup: BeautifulSoup) -> str:
    parts: list[str] = []
    for row in soup.select("table tr"):
        head = row.select_one("th")
        data = row.select_one("td")
        if not head or not data:
            continue
        head_text = head.get_text(" ", strip=True)
        if head_text in {"特徴", "部屋設備", "建物設備・施設"}:
            parts.append(data.get_text(" ", strip=True))
    return " ".join(part for part in parts if part).strip()


def normalize_name(text: str) -> str:
    if not text:
        return ""
    text = re.sub(r"[【】\[\]◇◆◎○●■□☆★…・\s]", "", text)
    text = re.sub(r"(号室|[0-9]+階|中古マンション|中古住宅|中古一戸建て)$", "", text)
    return text


def normalize_address(text: str) -> str:
    if not text:
        return ""
    text = text.split("[", 1)[0]
    return re.sub(r"\s+", "", text)


def building_key(record: dict) -> str:
    address = normalize_address(record.get("address", ""))
    year = str(record.get("built_year") or "")
    name = normalize_name(record.get("property_name") or record.get("title") or "")
    if address and year:
        return f"{address}|{year}"
    if name and year:
        return f"{name}|{year}"
    return name or record["listing_id"]


def top_candidates(listings: Iterable[dict], limit: int = SHORTLIST_LIMIT, *, dedupe_building: bool = False) -> list[dict]:
    ranked = sorted(list(listings), key=lambda record: record.get("score", float("-inf")), reverse=True)
    if not dedupe_building:
        return ranked[:limit]
    picked: list[dict] = []
    seen: set[str] = set()
    for record in ranked:
        key = building_key(record)
        if key in seen:
            continue
        seen.add(key)
        picked.append(record)
        if len(picked) >= limit:
            break
    return picked


def render_report(candidates: list[dict], path: Path, config: PropertyConfig) -> None:
    lines = [
        f"# {config.label.title()} shortlist",
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
                f"- Source: {record.get('source', 'suumo')}",
                f"- URL: {record.get('url')}",
                f"- Stations: {station_label or 'n/a'}",
                f"- Price: {record.get('price_man', 0):.0f}万円",
                f"- Size: {record.get('area_sqm', 0):.2f} sqm",
                f"- Layout: {record.get('layout') or 'n/a'}",
                f"- Walk: {record.get('walk_min')} min",
                f"- Built: {record.get('built_year') or 'n/a'}",
                f"- First Seen: {record.get('first_seen_at') or 'n/a'}",
                f"- Strict Match: {'yes' if strict_match(record, config) else 'near miss'}",
                f"- Dishwasher: {'yes' if record.get('dishwasher_hits') else 'not confirmed'}",
                f"- Address: {record.get('address') or 'n/a'}",
                f"- Access: {record.get('access_text') or 'n/a'}",
            ]
        )
        if config.kind == "house":
            lines.append(f"- Land Area: {record.get('land_area_sqm') or 0:.2f} sqm")
        lines.extend(
            [
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
                "source": record.get("source", "suumo"),
                "property_name": record.get("property_name"),
                "title": record.get("title"),
                "url": record.get("url"),
                "price_man": record.get("price_man"),
                "area_sqm": record.get("area_sqm"),
                "land_area_sqm": record.get("land_area_sqm"),
                "layout": record.get("layout"),
                "walk_min": record.get("walk_min"),
                "built_year": record.get("built_year"),
                "first_seen_at": record.get("first_seen_at"),
                "address": record.get("address"),
                "access_text": record.get("access_text"),
                "exact_station_hits": exact,
                "nearby_station_hits": nearby,
                "dishwasher_hits": record.get("dishwasher_hits", []),
                "brightness_hits": record.get("brightness_hits", []),
                "ceiling_hits": record.get("ceiling_hits", []),
                "feature_tags": record.get("feature_tags", []),
                "detail_summary": record.get("detail_summary"),
                "preview_image_url": record.get("preview_image_url"),
                "criteria_notes": record.get("criteria_notes", []),
                "score": record.get("score"),
            }
        )
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def image_extension(content_type: str, url: str) -> str:
    content_type = content_type.split(";", 1)[0].strip().lower()
    if content_type == "image/jpeg":
        return ".jpg"
    if content_type == "image/png":
        return ".png"
    if content_type == "image/webp":
        return ".webp"
    parsed = urlparse(url)
    suffix = Path(parsed.path).suffix.lower()
    if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
        return ".jpg" if suffix == ".jpeg" else suffix
    return ".jpg"


def localize_preview_images(
    session: requests.Session,
    candidates: list[dict],
    image_dir: Path,
    *,
    url_prefix: str,
) -> list[dict]:
    if image_dir.exists():
        shutil.rmtree(image_dir)
    image_dir.mkdir(parents=True, exist_ok=True)

    localized: list[dict] = []
    for record in candidates:
        payload = dict(record)
        preview_url = record.get("preview_image_url")
        if not preview_url:
            payload["preview_image_url"] = ""
            localized.append(payload)
            continue
        try:
            response = session.get(preview_url, timeout=30)
            response.raise_for_status()
            suffix = image_extension(response.headers.get("content-type", ""), preview_url)
            image_path = image_dir / f"{record['listing_id']}{suffix}"
            image_path.write_bytes(response.content)
            payload["preview_image_url"] = f"{url_prefix}/{image_path.name}"
        except Exception:  # noqa: BLE001
            payload["preview_image_url"] = ""
        localized.append(payload)
    return localized


def render_site_metadata(
    path: Path,
    *,
    generated_at: str,
    current_run_date: str,
    mansion_count: int,
    house_count: int,
    archives: list[str],
    is_latest: bool,
) -> None:
    payload = {
        "generated_at": generated_at,
        "current_run_date": current_run_date,
        "mansion_count": mansion_count,
        "house_count": house_count,
        "archives": archives,
        "is_latest": is_latest,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def list_archive_dates(docs_root: Path) -> list[str]:
    dates: list[str] = []
    if not docs_root.exists():
        return dates
    for child in docs_root.iterdir():
        if child.is_dir() and re.fullmatch(r"\d{4}-\d{2}-\d{2}", child.name):
            dates.append(child.name)
    return sorted(dates, reverse=True)


def copy_site_shell(docs_root: Path, archive_dir: Path) -> None:
    archive_dir.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(docs_root / "index.html", archive_dir / "index.html")
    shutil.copytree(docs_root / "assets", archive_dir / "assets", dirs_exist_ok=True)


def publish_docs(
    session: requests.Session,
    mansion_shortlist: list[dict],
    house_shortlist: list[dict],
    *,
    mansion_count: int,
    house_count: int,
) -> None:
    docs_root = Path("docs")
    latest_data_dir = docs_root / "data"
    latest_data_dir.mkdir(parents=True, exist_ok=True)

    run_date = today_local().isoformat()
    archive_dir = docs_root / run_date
    archive_data_dir = archive_dir / "data"
    archive_data_dir.mkdir(parents=True, exist_ok=True)
    copy_site_shell(docs_root, archive_dir)

    generated_at = datetime.now().astimezone().isoformat()

    latest_mansions = localize_preview_images(
        session,
        mansion_shortlist,
        latest_data_dir / "images" / "mansions",
        url_prefix="./data/images/mansions",
    )
    latest_houses = localize_preview_images(
        session,
        house_shortlist,
        latest_data_dir / "images" / "houses",
        url_prefix="./data/images/houses",
    )
    archive_mansions = localize_preview_images(
        session,
        mansion_shortlist,
        archive_data_dir / "images" / "mansions",
        url_prefix="./data/images/mansions",
    )
    archive_houses = localize_preview_images(
        session,
        house_shortlist,
        archive_data_dir / "images" / "houses",
        url_prefix="./data/images/houses",
    )

    render_json(latest_mansions, latest_data_dir / "mansions.json")
    render_json(latest_houses, latest_data_dir / "houses.json")
    render_json(archive_mansions, archive_data_dir / "mansions.json")
    render_json(archive_houses, archive_data_dir / "houses.json")

    archives = list_archive_dates(docs_root)
    render_site_metadata(
        latest_data_dir / "site.json",
        generated_at=generated_at,
        current_run_date=run_date,
        mansion_count=mansion_count,
        house_count=house_count,
        archives=archives,
        is_latest=True,
    )
    render_site_metadata(
        archive_data_dir / "site.json",
        generated_at=generated_at,
        current_run_date=run_date,
        mansion_count=mansion_count,
        house_count=house_count,
        archives=archives,
        is_latest=False,
    )


def enrich_ken_details(session: requests.Session, listings: dict[str, dict], config: PropertyConfig) -> None:
    for record in listings.values():
        try:
            soup = soup_for(session, record["url"])
        except Exception:  # noqa: BLE001
            continue
        page_text = soup.get_text(" ", strip=True)
        overview = extract_overview(soup)
        record["overview"] = overview
        record["detail_summary"] = extract_ken_detail_summary(soup)
        record["feature_tags"] = extract_ken_feature_tags(soup)
        record["dishwasher_hits"] = [kw for kw in DISHWASHER_KEYWORDS if kw in page_text]
        record["brightness_hits"] = [kw for kw in BRIGHTNESS_KEYWORDS if kw in page_text]
        record["ceiling_hits"] = [kw for kw in CEILING_WINDOW_KEYWORDS if kw in page_text]
        record["preview_image_url"] = record.get("preview_image_url") or extract_preview_image_url(soup, record["url"])
        title = soup.select_one("h1")
        if title:
            record["title"] = title.get_text(" ", strip=True)
        record["address"] = record.get("address") or overview.get("住所", "")
        record["access_text"] = record.get("access_text") or overview.get("交通/駅徒歩*", "") or overview.get("交通/駅徒歩", "")
        record["walk_min"] = record.get("walk_min") or parse_walk_min(record.get("access_text"))
        record["price_man"] = record.get("price_man") or parse_price_man(overview.get("価格"))
        record["layout"] = record.get("layout") or normalize_layout(overview.get("間取り / 方位", "").split("/", 1)[0].strip())
        record["area_sqm"] = record.get("area_sqm") or parse_float(overview.get("専有面積"))
        record["built_year"] = record.get("built_year") or parse_year(overview.get("完成時期(築年月)") or overview.get("築年月"))
        record["built_text"] = record.get("built_text") or overview.get("完成時期(築年月)") or overview.get("築年月", "")
        if config.kind == "house":
            record["land_area_sqm"] = record.get("land_area_sqm") or parse_float(overview.get("土地面積"))


def load_persisted_listings(conn: sqlite3.Connection, config: PropertyConfig) -> dict[str, dict]:
    table = config.db_table
    rows = conn.execute(f"SELECT * FROM {table}")
    listings: dict[str, dict] = {}
    for row in rows:
        exact_hits = json.loads(row["exact_station_hits_json"] or "[]")
        nearby_hits = json.loads(row["nearby_station_hits_json"] or "[]")
        station_hits = [
            {"station_name": name, "station_code": "", "priority": "exact", "note": "", "source_url": ""}
            for name in exact_hits
        ] + [
            {"station_name": name, "station_code": "", "priority": "nearby", "note": "", "source_url": ""}
            for name in nearby_hits
        ]
        dishwasher_hits = ["dishwasher"] if row["dishwasher"] else []
        listings[row["listing_id"]] = {
            "listing_id": row["listing_id"],
            "source": row["source"] or "suumo",
            "property_type": config.kind,
            "url": row["url"],
            "title": row["title"],
            "property_name": row["property_name"],
            "address": row["address"],
            "access_text": row["access_text"],
            "price_man": row["price_man"],
            "area_sqm": row["area_sqm"],
            "land_area_sqm": row["land_area_sqm"] if "land_area_sqm" in row.keys() else None,
            "layout": row["layout"],
            "balcony_sqm": row["balcony_sqm"] if "balcony_sqm" in row.keys() else None,
            "walk_min": row["walk_min"],
            "built_year": row["built_year"],
            "built_text": row["built_text"],
            "list_blurb": row["list_blurb"],
            "detail_summary": row["detail_summary"],
            "feature_tags": json.loads(row["feature_tags_json"] or "[]"),
            "overview": json.loads(row["overview_json"] or "{}"),
            "station_hits": station_hits,
            "dishwasher_hits": dishwasher_hits,
            "brightness_hits": json.loads(row["brightness_hits_json"] or "[]"),
            "ceiling_hits": json.loads(row["ceiling_hits_json"] or "[]"),
            "identity_key": row["identity_key"],
            "first_seen_at": row["first_seen_at"],
            "criteria_notes": json.loads(row["criteria_notes_json"] or "[]"),
            "score": row["score"],
            "preview_image_url": "",
        }
    return listings


def hydrate_preview_urls(session: requests.Session, listings: list[dict]) -> None:
    for record in listings:
        if record.get("preview_image_url"):
            continue
        try:
            soup = soup_for(session, record["url"])
        except Exception:  # noqa: BLE001
            continue
        record["preview_image_url"] = extract_preview_image_url(soup, record["url"])


def build_shortlist(listings: dict[str, dict], config: PropertyConfig) -> tuple[int, list[dict]]:
    for record in listings.values():
        score_listing(record, config)
    candidates = [record for record in listings.values() if record.get("score", -999) > 0]
    strict_candidates = [record for record in candidates if strict_match(record, config)]
    shortlist = top_candidates(strict_candidates, SHORTLIST_LIMIT, dedupe_building=True)
    if len(shortlist) < SHORTLIST_LIMIT:
        strict_ids = {record["listing_id"] for record in shortlist}
        fallback_pool = [record for record in candidates if record["listing_id"] not in strict_ids]
        fallback = top_candidates(fallback_pool, SHORTLIST_LIMIT * 2, dedupe_building=True)
        for record in fallback:
            if len(shortlist) >= SHORTLIST_LIMIT:
                break
            if building_key(record) in {building_key(item) for item in shortlist}:
                continue
            shortlist.append(record)
    return len(candidates), shortlist


def collect_listings(session: requests.Session, config: PropertyConfig) -> dict[str, dict]:
    listings = collect_suumo_listings(session, config)
    listings.update(collect_ken_listings(session, config))
    return listings


def enrich_details(session: requests.Session, listings: dict[str, dict], config: PropertyConfig) -> None:
    suumo = {k: v for k, v in listings.items() if v.get("source") == "suumo"}
    ken = {k: v for k, v in listings.items() if v.get("source") == "ken"}
    enrich_suumo_details(session, suumo, config)
    enrich_ken_details(session, ken, config)


def run_pipeline(session: requests.Session, conn: sqlite3.Connection, output_dir: Path, config: PropertyConfig) -> tuple[int, int, list[dict]]:
    listings = collect_listings(session, config)
    enrich_details(session, listings, config)
    attach_identity_history(conn, listings, config)
    candidate_count, shortlist = build_shortlist(listings, config)
    persist_identity_history(conn, listings, config)
    if config.kind == "mansion":
        persist_mansions(conn, listings)
    else:
        persist_houses(conn, listings)
    output_shortlist = localize_preview_images(
        session,
        shortlist,
        output_dir / f"{config.kind}_images",
        url_prefix=f"./{config.kind}_images",
    )
    render_report(output_shortlist, output_dir / config.output_md, config)
    render_json(output_shortlist, output_dir / config.output_json)
    return len(listings), candidate_count, shortlist


def rescore_pipeline(session: requests.Session, conn: sqlite3.Connection, output_dir: Path, config: PropertyConfig) -> tuple[int, int, list[dict]]:
    listings = load_persisted_listings(conn, config)
    candidate_count, shortlist = build_shortlist(listings, config)
    hydrate_preview_urls(session, shortlist)
    output_shortlist = localize_preview_images(
        session,
        shortlist,
        output_dir / f"{config.kind}_images",
        url_prefix=f"./{config.kind}_images",
    )
    render_report(output_shortlist, output_dir / config.output_md, config)
    render_json(output_shortlist, output_dir / config.output_json)
    return len(listings), candidate_count, shortlist


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rescore-only", action="store_true")
    args = parser.parse_args()
    data_dir, output_dir = ensure_dirs()
    db_path = data_dir / "suumo_listings.sqlite3"
    session = build_session()
    conn = connect_db(db_path)
    pipeline_fn = rescore_pipeline if args.rescore_only else run_pipeline
    mansion_count, mansion_candidates, mansion_shortlist = pipeline_fn(session, conn, output_dir, MANSION)
    house_count, house_candidates, house_shortlist = pipeline_fn(session, conn, output_dir, HOUSE)
    publish_docs(
        session,
        mansion_shortlist,
        house_shortlist,
        mansion_count=mansion_count,
        house_count=house_count,
    )
    print(f"scraped {mansion_count} unique mansion listings")
    print(f"ranked {mansion_candidates} positive-score mansion candidates")
    print(f"scraped {house_count} unique house listings")
    print(f"ranked {house_candidates} positive-score house candidates")
    print(f"database: {db_path}")
    print(f"mansion report: {output_dir / MANSION.output_md}")
    print(f"house report: {output_dir / HOUSE.output_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
