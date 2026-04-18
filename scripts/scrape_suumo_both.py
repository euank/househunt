#!/usr/bin/env python3

from __future__ import annotations

import json
import shutil
import re
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, date, timezone
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
    output_md="top10_mansions.md",
    output_json="top10_mansions.json",
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
    output_md="top10_houses.md",
    output_json="top10_houses.json",
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

        CREATE TABLE IF NOT EXISTS house_listings (
          listing_id TEXT PRIMARY KEY,
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
        """
    )
    return conn


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
    base = f"{BASE_URL}/{config.base_path}/ek_{seed.code}/"
    first = soup_for(session, base)
    pages = {1}
    for anchor in first.select("a[href]"):
        href = anchor.get("href", "")
        match = re.search(r"page=(\d+)", href)
        if match:
            pages.add(int(match.group(1)))
    return [base if page == 1 else f"{base}?page={page}&rn=0305" for page in range(1, max(pages) + 1)]


def collect_listings(session: requests.Session, config: PropertyConfig) -> dict[str, dict]:
    collected: dict[str, dict] = {}
    for seed in SEEDS:
        for page_url in page_urls_for_seed(session, seed, config):
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
    return 8000 <= price <= 18000 and area >= 60 and walk <= config.detail_prefilter_walk and year >= 1995 and rooms >= 2


def strict_match(record: dict, config: PropertyConfig) -> bool:
    price = record.get("price_man") or 0
    area = record.get("area_sqm") or 0
    walk = record.get("walk_min") or 999
    year = record.get("built_year") or 0
    layout = record.get("layout") or ""
    rooms = layout_room_count(layout) or 0
    return (
        10000 <= price <= 15000
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


def enrich_details(session: requests.Session, listings: dict[str, dict], config: PropertyConfig) -> None:
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
    if 10000 <= price_man <= 15000:
        return max(4, 12 - abs(price_man - 13000) / 350)
    if 9000 <= price_man < 10000 or 15000 < price_man <= 16000:
        return 1
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
        if 10000 <= price_man <= 15000:
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
    if not info_date:
        return 0.0
    days_old = (today_local() - info_date).days
    if days_old <= 3:
        return 2.0
    if days_old <= 7:
        return 1.0
    if days_old <= 14:
        return 0.0
    if days_old <= 30:
        return -0.5
    if days_old <= 60:
        return -1.0
    return -2.0


def score_listing(record: dict, config: PropertyConfig) -> float:
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


def persist_mansions(conn: sqlite3.Connection, listings: dict[str, dict]) -> None:
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
              listing_id, url, title, property_name, address, access_text, price_man,
              area_sqm, land_area_sqm, layout, walk_min, built_year, built_text, list_blurb,
              detail_summary, feature_tags_json, overview_json, exact_station_hits_json,
              nearby_station_hits_json, dishwasher, brightness_hits_json, ceiling_hits_json,
              score, criteria_notes_json, scraped_at, detail_scraped_at
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


def top_candidates(listings: Iterable[dict], limit: int = 10, *, dedupe_building: bool = False) -> list[dict]:
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
        f"# SUUMO {config.label} shortlist",
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
                "property_name": record.get("property_name"),
                "title": record.get("title"),
                "url": record.get("url"),
                "price_man": record.get("price_man"),
                "area_sqm": record.get("area_sqm"),
                "land_area_sqm": record.get("land_area_sqm"),
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

    render_json(mansion_shortlist, latest_data_dir / "mansions.json")
    render_json(house_shortlist, latest_data_dir / "houses.json")
    render_json(mansion_shortlist, archive_data_dir / "mansions.json")
    render_json(house_shortlist, archive_data_dir / "houses.json")

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


def run_pipeline(session: requests.Session, conn: sqlite3.Connection, output_dir: Path, config: PropertyConfig) -> tuple[int, int, list[dict]]:
    listings = collect_listings(session, config)
    enrich_details(session, listings, config)
    for record in listings.values():
        score_listing(record, config)
    if config.kind == "mansion":
        persist_mansions(conn, listings)
    else:
        persist_houses(conn, listings)
    candidates = [record for record in listings.values() if record.get("score", -999) > 0]
    strict_candidates = [record for record in candidates if strict_match(record, config)]
    shortlist = top_candidates(strict_candidates, 10, dedupe_building=True)
    if len(shortlist) < 10:
        strict_ids = {record["listing_id"] for record in shortlist}
        fallback_pool = [record for record in candidates if record["listing_id"] not in strict_ids]
        fallback = top_candidates(fallback_pool, 20, dedupe_building=True)
        for record in fallback:
            if len(shortlist) >= 10:
                break
            if building_key(record) in {building_key(item) for item in shortlist}:
                continue
            shortlist.append(record)
    render_report(shortlist, output_dir / config.output_md, config)
    render_json(shortlist, output_dir / config.output_json)
    return len(listings), len(candidates), shortlist


def main() -> int:
    data_dir, output_dir = ensure_dirs()
    db_path = data_dir / "suumo_listings.sqlite3"
    session = build_session()
    conn = connect_db(db_path)
    mansion_count, mansion_candidates, mansion_shortlist = run_pipeline(session, conn, output_dir, MANSION)
    house_count, house_candidates, house_shortlist = run_pipeline(session, conn, output_dir, HOUSE)
    publish_docs(
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
