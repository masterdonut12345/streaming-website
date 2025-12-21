#!/usr/bin/env python3
"""
scrape_games.py

FAST + SAFE scraper for:
  - sport7.pro (Sport71): today + tomorrow, ALL embeds
  - sharkstreams.net: today + tomorrow, 1 embed per game

Guarantees:
  - Manual streams preserved
  - Scraped streams replaced each run
  - No stream explosion
  - Locked CSV writes
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import pytz
import re
from urllib.parse import urljoin
from typing import List, Dict, Any
import ast
import os
import fcntl
import hashlib

# ---------------- CONFIG ----------------

BASE_URL_SPORT71 = "https://sport7.pro"
BASE_URL_SHARK   = "https://sharkstreams.net/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; StreamScraper/1.0)"}

EST = pytz.timezone("US/Eastern")
UTC = pytz.UTC

OUTPUT_FILE = "today_games_with_all_streams.csv"

MAX_STREAMS_PER_GAME = 8

CSV_COLS = [
    "source", "date_header", "sport", "time_unix", "time",
    "tournament", "tournament_url", "matchup", "watch_url",
    "is_live", "streams", "embed_url"
]

# ---------------- HELPERS ----------------

def _parse_streams_cell(val):
    if isinstance(val, list):
        return val
    if not isinstance(val, str) or not val.strip():
        return []
    try:
        parsed = ast.literal_eval(val)
        return parsed if isinstance(parsed, list) else []
    except Exception:
        return []

def _dedup_streams(streams):
    seen = set()
    out = []
    for s in streams:
        url = (s.get("embed_url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        out.append(s)
    return out[:MAX_STREAMS_PER_GAME]

def _streams_to_cell(streams):
    return repr(_dedup_streams(streams))

def _is_manual(st):
    origin = st.get("origin")
    # Treat missing/unknown origin as manual so scraper refreshes don't wipe user-added streams
    return origin != "scraped"

def _today_or_tomorrow(dt):
    now = datetime.now(EST).date()
    return dt.date() in (now, now + timedelta(days=1))

def _normalize_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "live", "t")

def _stable_game_id(row: dict) -> int:
    """
    Mirror the web app's stable ID generation so scraper merges align with cache keys.
    """
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)

def _ensure_cols(df: pd.DataFrame) -> pd.DataFrame:
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = None
    return df

def _should_keep_existing(row: dict) -> bool:
    """
    Keep manual/older rows unless they are clearly stale (older than ~1 day in UTC).
    """
    now = datetime.now(UTC)
    ts = row.get("time_unix")
    try:
        ts_val = float(ts)
        dt = datetime.fromtimestamp(ts_val / 1000, tz=UTC)
        return dt >= now - timedelta(days=1)
    except Exception:
        pass

    try:
        dt = pd.to_datetime(row.get("time"), errors="coerce")
        if not pd.isna(dt):
            if dt.tzinfo is None:
                dt = UTC.localize(dt)
            return dt >= now - timedelta(days=1)
    except Exception:
        pass

    # If we cannot parse a time, err on the side of keeping it so we don't drop valid manual entries
    return True

# ---------------- SPORT71 ----------------

_IFRAME_RE = re.compile(r'https?://[^\s"\']+')

def scrape_sport71() -> pd.DataFrame:
    r = requests.get(BASE_URL_SPORT71, headers=HEADERS, timeout=15)
    if r.status_code != 200:
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, "html.parser")
    section = soup.find("section", id="upcoming-events")
    if not section:
        return pd.DataFrame()

    rows = []

    for tr in section.select("tbody tr"):
        tds = tr.find_all("td")
        if len(tds) != 3:
            continue

        time_span = tds[0].find("span", class_="event-time")
        if not time_span:
            continue

        try:
            ts = int(time_span["data-unix-time"])
            event_dt = datetime.fromtimestamp(ts / 1000, tz=UTC).astimezone(EST)
        except Exception:
            continue

        if not _today_or_tomorrow(event_dt):
            continue

        watch_a = tds[2].find("a", class_="watch-button")
        watch_url = watch_a["href"] if watch_a else None
        if not watch_url:
            continue

        matchup = tds[1].get_text(" ", strip=True)

        rows.append({
            "source": "sport71",
            "date_header": event_dt.strftime("%A, %B %d, %Y"),
            "sport": "Unknown",
            "time_unix": ts,
            "time": event_dt,
            "tournament": None,
            "tournament_url": None,
            "matchup": matchup,
            "watch_url": watch_url,
            "is_live": False,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Fetch embeds (ONE request per game)
    streams_map = {}

    for w in df["watch_url"]:
        streams = []
        try:
            r = requests.get(w, headers=HEADERS, timeout=15)
            if r.status_code != 200:
                continue
            soup = BeautifulSoup(r.text, "html.parser")

            for iframe in soup.find_all("iframe"):
                src = iframe.get("src") or iframe.get("data-src")
                if src:
                    streams.append({
                        "label": "Stream",
                        "embed_url": urljoin(w, src),
                        "watch_url": w,
                        "origin": "scraped",
                    })

            for script in soup.find_all("script"):
                text = script.string or ""
                for m in _IFRAME_RE.findall(text):
                    if "embed" in m or "player" in m:
                        streams.append({
                            "label": "Stream",
                            "embed_url": m,
                            "watch_url": w,
                            "origin": "scraped",
                        })

        except Exception:
            pass

        streams_map[w] = _dedup_streams(streams)

    df["streams"] = df["watch_url"].map(lambda w: streams_map.get(w, []))
    df["embed_url"] = df["streams"].map(lambda s: s[0]["embed_url"] if s else None)

    return df

# ---------------- SHARKSTREAMS ----------------

_OPENEMBED_RE = re.compile(r"openEmbed\(\s*'([^']+)'\s*\)", re.I)
_WINDOWOPEN_RE = re.compile(r"window\.open\(\s*'([^']+)'\s*,", re.I)

def scrape_shark() -> pd.DataFrame:
    r = requests.get(BASE_URL_SHARK, headers=HEADERS, timeout=15)
    if r.status_code != 200:
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, "html.parser")
    rows = []

    for div in soup.find_all("div", class_="row"):
        date_span = div.find("span", class_="ch-date")
        name_span = div.find("span", class_="ch-name")
        if not (date_span and name_span):
            continue

        try:
            dt = EST.localize(datetime.strptime(date_span.text.strip(), "%Y-%m-%d %H:%M:%S"))
        except Exception:
            continue

        if not _today_or_tomorrow(dt):
            continue

        embed = None
        for a in div.find_all("a"):
            onclick = a.get("onclick", "")
            m = _OPENEMBED_RE.search(onclick) or _WINDOWOPEN_RE.search(onclick)
            if m:
                embed = urljoin(BASE_URL_SHARK, m.group(1))
                break

        if not embed:
            continue

        rows.append({
            "source": "sharkstreams",
            "date_header": dt.strftime("%A, %B %d, %Y"),
            "sport": None,
            "time_unix": int(dt.timestamp() * 1000),
            "time": dt,
            "tournament": None,
            "tournament_url": None,
            "matchup": name_span.text.strip(),
            "watch_url": embed,
            "streams": [{
                "label": "SharkStreams",
                "embed_url": embed,
                "watch_url": embed,
                "origin": "scraped",
            }],
            "embed_url": embed,
            "is_live": False,
        })

    return pd.DataFrame(rows)

# ---------------- MERGE + WRITE ----------------

def merge_streams(new, old):
    manual = [s for s in old if _is_manual(s)]
    scraped = [s for s in new if not _is_manual(s)]
    return _dedup_streams(manual + scraped)

def main():
    df_new = pd.concat([
        scrape_sport71(),
        scrape_shark()
    ], ignore_index=True)

    if df_new.empty:
        print("[scraper] No games found.")
        return

    if not os.path.exists(OUTPUT_FILE):
        pd.DataFrame(columns=CSV_COLS).to_csv(OUTPUT_FILE, index=False)

    with open(OUTPUT_FILE, "r+", encoding="utf-8") as fh:
        fcntl.flock(fh, fcntl.LOCK_EX)
        df_old = _ensure_cols(pd.read_csv(fh))
        df_new = _ensure_cols(df_new)

        # Build a lookup of existing rows by stable ID so we don't drop manual edits
        old_map = {}
        for _, row in df_old.iterrows():
            rd = row.to_dict()
            old_map[_stable_game_id(rd)] = rd

        out_rows = []

        # Merge scraped rows with existing where possible
        for _, row in df_new.iterrows():
            rd = row.to_dict()
            gid = _stable_game_id(rd)
            old = old_map.pop(gid, None)

            old_streams = _parse_streams_cell(old.get("streams")) if old else []
            merged = merge_streams(rd.get("streams") or [], old_streams)

            rd["streams"] = merged
            rd["embed_url"] = rd.get("embed_url") or (merged[0]["embed_url"] if merged else None)
            rd["is_live"] = _normalize_bool(old.get("is_live") if old else rd.get("is_live"))

            out_rows.append(rd)

        # Carry forward unmatched (typically manual) rows so they are not wiped out
        for _, rd in old_map.items():
            if not _should_keep_existing(rd):
                continue
            streams = _parse_streams_cell(rd.get("streams"))
            rd["streams"] = streams
            rd["embed_url"] = rd.get("embed_url") or (streams[0]["embed_url"] if streams else None)
            rd["is_live"] = _normalize_bool(rd.get("is_live"))
            out_rows.append(rd)

        fh.seek(0)
        fh.truncate()
        pd.DataFrame(out_rows)[CSV_COLS].to_csv(fh, index=False)
        fcntl.flock(fh, fcntl.LOCK_UN)

    print(f"[scraper] Wrote {len(out_rows)} games")

if __name__ == "__main__":
    main()
