#!/usr/bin/env python3
"""
scrape_games.py

Scrapes games from:
  - sport7.pro (sport71) upcoming events (today + tomorrow) + all stream embeds from watch pages
  - sharkstreams.net (today)

FIXES:
  1) Exclusive lock around read+merge+write
  2) Merge old streams into matching new rows with stronger fallbacks
  3) Preserve stream metadata (e.g., slug) instead of dropping fields
  4) Carry-forward old rows that disappear from scrape (so streams don’t vanish)
"""

import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import pytz
import re
from urllib.parse import urljoin
from typing import Optional, List, Dict, Any, Tuple
import ast
import os
import json
import fcntl

BASE_URL_SPORT71 = "https://sport7.pro"
BASE_URL_SHARK = "https://sharkstreams.net/"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; EventScraper/1.0)"}

EST = pytz.timezone("US/Eastern")
UTC = pytz.UTC

OUTPUT_FILE = "today_games_with_all_streams.csv"

CSV_COLS = [
    "source", "date_header", "sport", "time_unix", "time",
    "tournament", "tournament_url", "matchup", "watch_url",
    "is_live", "streams", "embed_url"
]

# ========= helpers =========
def _norm_url(u: Any) -> str:
    return u.strip() if isinstance(u, str) else ""

def _safe_to_dt_est(val: Any) -> Optional[datetime]:
    """Convert a CSV 'time' cell into timezone-aware EST datetime if possible."""
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None

    if isinstance(val, datetime):
        if val.tzinfo is None:
            return EST.localize(val)
        return val.astimezone(EST)

    if isinstance(val, pd.Timestamp):
        if pd.isna(val):
            return None
        dt = val.to_pydatetime()
        if dt.tzinfo is None:
            return EST.localize(dt)
        return dt.astimezone(EST)

    if isinstance(val, str):
        s = val.strip()
        if not s:
            return None
        ts = pd.to_datetime(s, errors="coerce", utc=False)
        if pd.isna(ts):
            return None

        if isinstance(ts, pd.Timestamp) and ts.tz is not None:
            return ts.tz_convert(EST).to_pydatetime()

        if isinstance(ts, pd.Timestamp):
            dt = ts.to_pydatetime()
            if dt.tzinfo is None:
                return EST.localize(dt)
            return dt.astimezone(EST)

    return None

def _parse_streams_cell(val: Any) -> List[Dict[str, Any]]:
    """
    Robustly parse streams cell into list[dict].
    Accepts:
      - list already
      - python-literal string (repr(list_of_dicts))
      - JSON-ish string
    """
    if isinstance(val, list):
        return [x for x in val if isinstance(x, dict)]

    if val is None or (isinstance(val, float) and pd.isna(val)):
        return []

    if not isinstance(val, str):
        return []

    s = val.strip()
    if not s:
        return []

    # Python literal
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
    except Exception:
        pass

    # JSON-ish fallback
    try:
        s2 = s.replace("None", "null").replace("True", "true").replace("False", "false")
        if "'" in s2 and '"' not in s2:
            s2 = s2.replace("'", '"')
        parsed = json.loads(s2)
        if isinstance(parsed, list):
            return [x for x in parsed if isinstance(x, dict)]
    except Exception:
        return []

    return []

def _dedup_streams_keep_order(streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Dedup by embed_url, preserve ALL metadata keys for each stream.
    """
    seen = set()
    out: List[Dict[str, Any]] = []
    for st in (streams or []):
        if not isinstance(st, dict):
            continue
        url = (st.get("embed_url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)

        # keep all keys, but normalize basics
        fixed = dict(st)
        fixed["embed_url"] = url
        fixed["label"] = fixed.get("label") or "Stream"
        if "watch_url" in fixed and isinstance(fixed["watch_url"], str):
            fixed["watch_url"] = fixed["watch_url"].strip()
        out.append(fixed)
    return out

def _streams_to_cell(streams_list: Any) -> str:
    """
    Convert list[dict] -> python-literal string suitable for ast.literal_eval in Flask.
    IMPORTANT: preserve metadata keys (slug, etc.), only enforce embed_url exists.
    """
    streams = streams_list if isinstance(streams_list, list) else _parse_streams_cell(streams_list)
    cleaned: List[Dict[str, Any]] = []
    for s in (streams or []):
        if not isinstance(s, dict):
            continue
        embed = (s.get("embed_url") or "").strip()
        if not embed:
            continue
        fixed = dict(s)
        fixed["embed_url"] = embed
        fixed["label"] = fixed.get("label") or "Stream"
        if "watch_url" in fixed and isinstance(fixed["watch_url"], str):
            fixed["watch_url"] = fixed["watch_url"].strip()
        cleaned.append(fixed)
    cleaned = _dedup_streams_keep_order(cleaned)
    return repr(cleaned)

# ========= 1) SPORT71: TODAY + TOMORROW'S GAMES =========
def scrape_today_games_sport71() -> pd.DataFrame:
    try:
        req = requests.get(BASE_URL_SPORT71, headers=HEADERS, timeout=15)
    except Exception as e:
        print("[sport71][ERROR] Requesting page failed:", e)
        return pd.DataFrame()

    html = req.text
    if "Error code 521" in html or req.status_code != 200:
        print("[sport71][ERROR] Site returned an error page or non-200 status.")
        print("[sport71][ERROR] HTTP status:", req.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", id="upcoming-events")
    if not section:
        print("[sport71][ERROR] Could not find <section id='upcoming-events'> — page may be JS-rendered.")
        return pd.DataFrame()

    today_est = datetime.now(EST)
    tmrw_est = today_est + timedelta(days=1)
    today_str = f"{today_est.strftime('%A')}, {today_est.strftime('%B')} {today_est.day}, {today_est.year}"
    tmrw_str = f"{tmrw_est.strftime('%A')}, {tmrw_est.strftime('%B')} {tmrw_est.day}, {tmrw_est.year}"

    rows = []
    for day_block in section.find_all("div", class_="mb-8", recursive=False):
        date_tag = day_block.find("h3")
        date_text = date_tag.get_text(strip=True) if date_tag else None
        if date_text not in (today_str, tmrw_str):
            continue

        for card in day_block.select("div.space-y-6 > div.bg-white"):
            header = card.find("div", class_="flex")
            sport_name_tag = header.find("span", class_="font-semibold") if header else None
            sport_name = sport_name_tag.get_text(strip=True) if sport_name_tag else "Unknown sport"

            tbody = card.find("tbody")
            if not tbody:
                continue

            for tr in tbody.find_all("tr"):
                if "seo-sport-filter" in tr.get("class", []):
                    continue
                tds = tr.find_all("td")
                if len(tds) != 3:
                    continue

                time_span = tds[0].find("span", class_="event-time")
                unix_time = time_span.get("data-unix-time") if time_span else None

                event_dt = None
                if unix_time:
                    try:
                        ts_ms = int(unix_time)
                        dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
                        event_dt = dt_utc.astimezone(EST)
                    except Exception as e:
                        print("[sport71][ERROR] failed to parse unix_time:", unix_time, e)

                tournament_link = tds[1].find("a", class_="tournament-link")
                tournament_name = tournament_link.get_text(strip=True) if tournament_link else None
                tournament_url = tournament_link["href"] if (tournament_link and tournament_link.has_attr("href")) else None

                spans = tds[1].find_all("span")
                matchup = spans[-1].get_text(strip=True) if spans else None

                watch_link = tds[2].find("a", class_="watch-button")
                watch_url = watch_link["href"] if (watch_link and watch_link.has_attr("href")) else None

                now_est = datetime.now(EST)
                is_live = bool(event_dt and event_dt <= now_est <= event_dt + timedelta(hours=5))

                rows.append({
                    "source": "sport71",
                    "date_header": date_text,
                    "sport": sport_name,
                    "time_unix": unix_time,
                    "time": event_dt,
                    "tournament": tournament_name,
                    "tournament_url": tournament_url,
                    "matchup": matchup,
                    "watch_url": watch_url,
                    "is_live": is_live,
                })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Filter out past non-live games
    try:
        now_est = datetime.now(EST)
        df = df[df["time"].isna() | (df["time"] >= now_est) | (df["is_live"] == True)]
    except Exception as e:
        print("[sport71][WARN] Failed to filter past non-live games:", e)

    return df

# ========= 2) SHARKSTREAMS: TODAY'S GAMES =========
def scrape_today_games_shark() -> pd.DataFrame:
    try:
        req = requests.get(BASE_URL_SHARK, headers=HEADERS, timeout=15)
    except Exception as e:
        print("[shark][ERROR] Requesting page failed:", e)
        return pd.DataFrame()

    if req.status_code != 200:
        print("[shark][ERROR] Non-200 HTTP status:", req.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(req.text, "html.parser")
    rows: List[Dict[str, Any]] = []

    for row_div in soup.find_all("div", class_="row"):
        date_span = row_div.find("span", class_="ch-date")
        if not date_span:
            continue

        date_str = date_span.get_text(strip=True)
        try:
            naive_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            event_dt = EST.localize(naive_dt)
        except Exception:
            print(f"[shark][ERROR] Failed to parse date: {date_str}")
            continue

        cat_span = row_div.find("span", class_="ch-category")
        sport_name = cat_span.get_text(strip=True) if cat_span else "Unknown"

        name_span = row_div.find("span", class_="ch-name")
        matchup = name_span.get_text(strip=True) if name_span else "Unknown"

        embed_link = None
        for a in row_div.find_all("a", class_="hd-link"):
            if "Embed" in a.get_text(strip=True):
                onclick = a.get("onclick", "")
                m = re.search(r"openEmbed\('([^']+)'", onclick)
                if m:
                    embed_link = m.group(1)
                break

        if not embed_link:
            for a in row_div.find_all("a", class_="hd-link"):
                if "Watch" in a.get_text(strip=True):
                    onclick = a.get("onclick", "")
                    m = re.search(r"window\.open\(\s*'([^']+)'", onclick)
                    if m:
                        rel = m.group(1)
                        embed_link = urljoin(BASE_URL_SHARK, rel)
                    break

        if not embed_link:
            print(f"[shark][ERROR] No embed link found for matchup: {matchup}")
            continue

        embed_link = urljoin(BASE_URL_SHARK, embed_link)
        time_unix = int(event_dt.timestamp() * 1000)

        streams = [{"label": "SharkStreams", "embed_url": embed_link, "watch_url": embed_link}]

        now_est = datetime.now(EST)
        is_live = bool(event_dt <= now_est <= event_dt + timedelta(hours=2.5))

        rows.append({
            "source": "sharkstreams",
            "date_header": event_dt.strftime("%A, %B %d, %Y"),
            "sport": sport_name,
            "time_unix": time_unix,
            "time": event_dt,
            "tournament": None,
            "tournament_url": None,
            "matchup": matchup,
            "watch_url": embed_link,
            "streams": streams,
            "embed_url": embed_link,
            "is_live": is_live,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    try:
        now_est = datetime.now(EST)
        df = df[df["time"].isna() | (df["time"] >= now_est) | (df["is_live"] == True)]
    except Exception as e:
        print("[shark][WARN] Failed to filter past non-live games:", e)

    return df

# ========= 3) GET STREAMS FROM SPORT71 WATCH PAGE =========
def get_all_streams_from_watch_page(watch_url: Optional[str]) -> List[Dict[str, Any]]:
    if not watch_url:
        return []

    try:
        r = requests.get(watch_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print("[sport71][WARN] Non-200 status on watch URL:", r.status_code, watch_url)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        streams: List[Dict[str, Any]] = []

        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            embed_url = iframe.get("src")
            streams.append({"label": "Main Stream", "embed_url": embed_url, "watch_url": watch_url})

        stream_links = soup.select("div.stream-picker a.stream-button")
        for link in stream_links or []:
            stream_name = link.get_text(strip=True)
            stream_page_url = link.get("href")
            full_stream_url = urljoin(watch_url, stream_page_url)

            r_stream = requests.get(full_stream_url, headers=HEADERS, timeout=15)
            if r_stream.status_code == 200:
                stream_soup = BeautifulSoup(r_stream.text, "html.parser")
                stream_iframe = stream_soup.find("iframe")
                if stream_iframe and stream_iframe.get("src"):
                    embed_url = stream_iframe.get("src")
                    streams.append({
                        "label": f"Stream: {stream_name}",
                        "embed_url": embed_url,
                        "watch_url": full_stream_url
                    })

        return _dedup_streams_keep_order(streams)
    except Exception as e:
        print("[sport71][ERROR] Error fetching streams:", e, watch_url)
        return []

# ========= 4) MATCHUP NORMALIZATION =========
TEAM_SEP_REGEX = re.compile(r"\bvs\b|\bvs.\b|\bv\b|\bv.\b| - | – | — | @ ", re.IGNORECASE)

def _normalize_team_name(team: str) -> str:
    if not isinstance(team, str):
        return ""
    return re.sub(r"[^a-z0-9]+", "", team.lower())

def make_matchup_key(name: Any) -> str:
    if not isinstance(name, str):
        return ""
    parts = TEAM_SEP_REGEX.split(name)
    teams = [_normalize_team_name(p) for p in parts]
    teams = [t for t in teams if t]
    if len(teams) >= 2:
        t1, t2 = sorted(teams[:2])
        return f"{t1}__{t2}"
    return _normalize_team_name(name)

def _row_game_key(row: Dict[str, Any]) -> Tuple[Any, str, str]:
    sport = (row.get("sport") or "").strip().lower()
    matchup_key = make_matchup_key(row.get("matchup") or "")

    time_val = row.get("time")
    if isinstance(time_val, (datetime, pd.Timestamp)):
        date_key = time_val.date()
    else:
        date_key = (row.get("date_header") or "").strip()

    return (date_key, sport, matchup_key)

def _row_game_key_loose(row: Dict[str, Any]) -> Tuple[str, str]:
    """Looser key ignores date; used as fallback."""
    sport = (row.get("sport") or "").strip().lower()
    matchup_key = make_matchup_key(row.get("matchup") or "")
    return (sport, matchup_key)

def _time_unix_int(v: Any) -> Optional[int]:
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return None
        return int(v)
    except Exception:
        return None

# ========= 5) MERGE OLD STREAMS (PRESERVE + STRONGER MATCHING) =========
def merge_existing_streams(df_new: pd.DataFrame, df_old: pd.DataFrame) -> pd.DataFrame:
    if df_new.empty or df_old.empty:
        return df_new

    # Index old rows in multiple ways
    old_streams_by_key: Dict[Tuple[Any, str, str], List[Dict[str, Any]]] = {}
    old_embed_by_key: Dict[Tuple[Any, str, str], str] = {}

    old_streams_by_watch: Dict[str, List[Dict[str, Any]]] = {}
    old_embed_by_watch: Dict[str, str] = {}

    old_streams_by_loose: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}
    old_embed_by_loose: Dict[Tuple[str, str], str] = {}

    old_by_timeunix: Dict[int, List[Dict[str, Any]]] = {}

    for _, r in df_old.iterrows():
        row = r.to_dict()
        row["time"] = _safe_to_dt_est(row.get("time"))

        streams = _dedup_streams_keep_order(_parse_streams_cell(row.get("streams")))
        key = _row_game_key(row)
        loose = _row_game_key_loose(row)

        if streams:
            old_streams_by_key.setdefault(key, []).extend(streams)
            old_streams_by_key[key] = _dedup_streams_keep_order(old_streams_by_key[key])

            old_streams_by_loose.setdefault(loose, []).extend(streams)
            old_streams_by_loose[loose] = _dedup_streams_keep_order(old_streams_by_loose[loose])

        emb = row.get("embed_url")
        if isinstance(emb, str) and emb.strip():
            old_embed_by_key.setdefault(key, emb.strip())
            old_embed_by_loose.setdefault(loose, emb.strip())

        w = _norm_url(row.get("watch_url"))
        if w:
            if streams:
                old_streams_by_watch.setdefault(w, []).extend(streams)
                old_streams_by_watch[w] = _dedup_streams_keep_order(old_streams_by_watch[w])
            if isinstance(emb, str) and emb.strip():
                old_embed_by_watch.setdefault(w, emb.strip())

        tu = _time_unix_int(row.get("time_unix"))
        if tu and streams:
            old_by_timeunix.setdefault(tu, []).extend(streams)
            old_by_timeunix[tu] = _dedup_streams_keep_order(old_by_timeunix[tu])

    merged_rows: List[Dict[str, Any]] = []

    for _, r in df_new.iterrows():
        new_row = r.to_dict()
        new_row["time"] = _safe_to_dt_est(new_row.get("time"))
        new_streams = _dedup_streams_keep_order(_parse_streams_cell(new_row.get("streams")))

        key = _row_game_key(new_row)
        loose = _row_game_key_loose(new_row)
        w = _norm_url(new_row.get("watch_url"))
        tu_new = _time_unix_int(new_row.get("time_unix"))

        old_streams = None
        old_embed = None

        # strict key
        if key in old_streams_by_key:
            old_streams = old_streams_by_key.get(key)
            old_embed = old_embed_by_key.get(key)

        # watch_url fallback
        if (not old_streams) and w and (w in old_streams_by_watch):
            old_streams = old_streams_by_watch.get(w)
            old_embed = old_embed_by_watch.get(w)

        # loose matchup fallback
        if (not old_streams) and (loose in old_streams_by_loose):
            old_streams = old_streams_by_loose.get(loose)
            old_embed = old_embed_by_loose.get(loose)

        # time_unix tolerance fallback (±15 minutes)
        if (not old_streams) and tu_new:
            window_ms = 15 * 60 * 1000
            candidates: List[Dict[str, Any]] = []
            for tu_old, ss in old_by_timeunix.items():
                if abs(tu_old - tu_new) <= window_ms:
                    candidates.extend(ss)
            if candidates:
                old_streams = _dedup_streams_keep_order(candidates)

        # Combine: old first to keep manual at top
        if old_streams:
            combined = _dedup_streams_keep_order(list(old_streams) + list(new_streams))
        else:
            combined = new_streams

        new_row["streams"] = combined

        # embed_url: use first combined, else preserve
        if combined:
            new_row["embed_url"] = (combined[0].get("embed_url") or "").strip() or new_row.get("embed_url")
        elif isinstance(old_embed, str) and old_embed.strip():
            new_row["embed_url"] = old_embed.strip()

        merged_rows.append(new_row)

    return pd.DataFrame(merged_rows)

# ========= 6) COMBINE SIMILAR GAMES =========
def combine_similar_games(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    combined: Dict[Any, Dict[str, Any]] = {}

    for _, row in df.iterrows():
        sport = (row.get("sport") or "").strip()
        matchup = row.get("matchup") or ""

        time_val = row.get("time")
        if isinstance(time_val, (datetime, pd.Timestamp)):
            date_key = time_val.date()
        else:
            date_key = (row.get("date_header") or "").strip()

        matchup_key = make_matchup_key(matchup)
        key = (date_key, sport.lower(), matchup_key)

        row_dict = row.to_dict()
        row_dict["streams"] = row_dict.get("streams") if isinstance(row_dict.get("streams"), list) else _parse_streams_cell(row_dict.get("streams"))
        row_dict["streams"] = _dedup_streams_keep_order(row_dict["streams"])

        if key not in combined:
            if row_dict["streams"] and not row_dict.get("embed_url"):
                row_dict["embed_url"] = row_dict["streams"][0].get("embed_url")
            combined[key] = row_dict
        else:
            existing = combined[key]
            all_streams: List[Dict[str, Any]] = existing.get("streams") or []
            other_streams = row_dict.get("streams") or []
            all_streams.extend(other_streams)

            existing["streams"] = _dedup_streams_keep_order(all_streams)
            if existing["streams"]:
                existing["embed_url"] = existing["streams"][0].get("embed_url")

            existing["is_live"] = bool(existing.get("is_live")) or bool(row_dict.get("is_live"))

            # Prefer earliest non-null time
            t_existing = existing.get("time")
            t_new = row_dict.get("time")
            if pd.notna(t_new):
                if pd.isna(t_existing):
                    existing["time"] = t_new
                elif isinstance(t_existing, (datetime, pd.Timestamp)) and isinstance(t_new, (datetime, pd.Timestamp)):
                    if t_new < t_existing:
                        existing["time"] = t_new

            for col in ["tournament", "tournament_url", "watch_url"]:
                if not existing.get(col) and row_dict.get(col):
                    existing[col] = row_dict.get(col)

            src_existing = existing.get("source")
            src_new = row_dict.get("source")
            if src_new:
                if src_existing and str(src_new) not in str(src_existing):
                    existing["source"] = f"{src_existing},{src_new}"
                elif not src_existing:
                    existing["source"] = src_new

    return pd.DataFrame(list(combined.values()))

# ========= 7) CARRY-FORWARD OLD ROWS THAT DISAPPEAR =========
def carry_forward_missing_old_rows(df_new: pd.DataFrame, df_old: pd.DataFrame) -> pd.DataFrame:
    """
    If a game row existed in old CSV but isn't present in new scrape,
    carry it forward if it's still relevant (today/tomorrow OR live/recent).
    This prevents streams from "vanishing" due to scrape misses.
    """
    if df_old.empty:
        return df_new

    now_est = datetime.now(EST)
    keep_recent_hours = 6  # carry forward games up to 6h in the past (covers live/recent)

    # Build sets of identifiers present in new
    new_keys = set()
    new_watch = set()
    new_loose = set()

    for _, r in df_new.iterrows():
        row = r.to_dict()
        row["time"] = _safe_to_dt_est(row.get("time"))
        new_keys.add(_row_game_key(row))
        w = _norm_url(row.get("watch_url"))
        if w:
            new_watch.add(w)
        new_loose.add(_row_game_key_loose(row))

    carry_rows: List[Dict[str, Any]] = []

    for _, r in df_old.iterrows():
        row = r.to_dict()
        row["time"] = _safe_to_dt_est(row.get("time"))

        key = _row_game_key(row)
        w = _norm_url(row.get("watch_url"))
        loose = _row_game_key_loose(row)

        # If already represented in new, skip
        if key in new_keys:
            continue
        if w and w in new_watch:
            continue
        if loose in new_loose:
            # loose match already exists; don't double-add
            continue

        # Decide if we keep it
        is_live = bool(row.get("is_live"))
        t = row.get("time")
        date_header = (row.get("date_header") or "").strip()

        keep = False
        if is_live:
            keep = True
        elif isinstance(t, datetime):
            if t >= (now_est - timedelta(hours=keep_recent_hours)) and t <= (now_est + timedelta(days=2)):
                keep = True
        else:
            # if no time, but date_header looks like today/tomorrow-ish, keep
            # (we keep it conservative by allowing non-empty date_header)
            if date_header:
                keep = True

        if keep:
            # parse & normalize streams
            streams = _dedup_streams_keep_order(_parse_streams_cell(row.get("streams")))
            row["streams"] = streams
            if not row.get("embed_url") and streams:
                row["embed_url"] = streams[0].get("embed_url")
            carry_rows.append(row)

    if not carry_rows:
        return df_new

    df_carry = pd.DataFrame(carry_rows)
    out = pd.concat([df_new, df_carry], ignore_index=True)
    # re-combine to avoid duplicates
    out = combine_similar_games(out)
    return out

# ========= 8) LOCKED READ/WRITE =========
def _ensure_csv_has_header(path: str) -> None:
    if os.path.exists(path):
        return
    df0 = pd.DataFrame(columns=CSV_COLS)
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df0.to_csv(path, index=False)

def _read_old_csv_locked(fh) -> pd.DataFrame:
    fh.seek(0)
    try:
        df_old = pd.read_csv(fh)
        if df_old is None or df_old.empty:
            return pd.DataFrame(columns=CSV_COLS)
        for c in CSV_COLS:
            if c not in df_old.columns:
                df_old[c] = ""
        return df_old
    except Exception:
        return pd.DataFrame(columns=CSV_COLS)

def _write_csv_locked(df: pd.DataFrame, fh) -> None:
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    if "streams" in df.columns:
        df["streams"] = df["streams"].apply(_streams_to_cell)

    def fix_embed(row):
        emb = row.get("embed_url")
        if isinstance(emb, str) and emb.strip():
            return emb.strip()
        streams = _parse_streams_cell(row.get("streams"))
        if streams:
            return (streams[0].get("embed_url") or "").strip() or ""
        return ""

    if "embed_url" in df.columns:
        df["embed_url"] = df.apply(fix_embed, axis=1)

    out = df[CSV_COLS].copy()

    fh.seek(0)
    fh.truncate(0)
    out.to_csv(fh, index=False)
    fh.flush()
    os.fsync(fh.fileno())

# ========= 9) MAIN =========
def main() -> None:
    df_sport71 = scrape_today_games_sport71()

    if not df_sport71.empty:
        df_sport71["streams"] = df_sport71["watch_url"].apply(get_all_streams_from_watch_page)
        df_sport71["streams"] = df_sport71["streams"].apply(lambda x: x if isinstance(x, list) else [])
        df_sport71["streams"] = df_sport71["streams"].apply(_dedup_streams_keep_order)
        df_sport71["embed_url"] = df_sport71["streams"].apply(lambda streams: streams[0]["embed_url"] if streams else None)

        # For live sport71 games require embed_url; future games can be missing streams
        try:
            df_sport71 = df_sport71[(~df_sport71["is_live"]) | df_sport71["embed_url"].notna()]
        except Exception:
            pass

    df_shark = scrape_today_games_shark()

    if not df_sport71.empty and not df_shark.empty:
        df = pd.concat([df_sport71, df_shark], ignore_index=True)
    elif not df_sport71.empty:
        df = df_sport71
    else:
        df = df_shark

    if df is None or df.empty:
        print("[main] No games found; nothing to write.")
        return

    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    # Combine similar games from sources (within this scrape run)
    df = combine_similar_games(df)

    _ensure_csv_has_header(OUTPUT_FILE)

    with open(OUTPUT_FILE, "r+", encoding="utf-8") as fh:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

        df_old = _read_old_csv_locked(fh)

        # Merge old streams into new rows
        df = merge_existing_streams(df, df_old)

        # Carry forward missing old rows so streams don't vanish when scrape misses a row
        df = carry_forward_missing_old_rows(df, df_old)

        # Write back while still locked
        _write_csv_locked(df, fh)

        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    print(f"[main] wrote {OUTPUT_FILE} ({len(df)} rows)")

if __name__ == "__main__":
    main()
