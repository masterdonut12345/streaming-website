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

BASE_URL_SPORT71 = "https://sport7.pro"
BASE_URL_SHARK = "https://sharkstreams.net/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EventScraper/1.0)"
}

# Define time zones
EST = pytz.timezone("US/Eastern")
UTC = pytz.UTC

OUTPUT_FILE = "today_games_with_all_streams.csv"


# ========= helpers =========
def _norm_url(u: Any) -> str:
    if not isinstance(u, str):
        return ""
    return u.strip()

def _safe_to_dt_est(val: Any) -> Optional[datetime]:
    """
    Convert a CSV 'time' cell into a python datetime in EST if possible.
    Handles:
      - tz-aware strings like '2025-12-19 19:00:00-05:00'
      - naive strings
      - pandas Timestamps
      - already-datetime
    """
    if val is None:
        return None
    if isinstance(val, float) and pd.isna(val):
        return None

    if isinstance(val, datetime):
        dt = val
        if dt.tzinfo is None:
            return EST.localize(dt)
        return dt.astimezone(EST)

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

        # If timestamp is tz-aware, normalize to EST
        if isinstance(ts, pd.Timestamp) and ts.tz is not None:
            return ts.tz_convert(EST).to_pydatetime()

        # tz-naive -> assume EST
        if isinstance(ts, pd.Timestamp):
            dt = ts.to_pydatetime()
            if dt.tzinfo is None:
                return EST.localize(dt)
            return dt.astimezone(EST)

    return None


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

                # time (sport71 gives a unix ms timestamp — assume UTC)
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
                        event_dt = None

                # tournament
                tournament_link = tds[1].find("a", class_="tournament-link")
                tournament_name = tournament_link.get_text(strip=True) if tournament_link else None
                tournament_url = (
                    tournament_link["href"]
                    if tournament_link and tournament_link.has_attr("href")
                    else None
                )

                # matchup
                spans = tds[1].find_all("span")
                matchup = spans[-1].get_text(strip=True) if spans else None

                # watch page url
                watch_link = tds[2].find("a", class_="watch-button")
                watch_url = (
                    watch_link["href"]
                    if watch_link and watch_link.has_attr("href")
                    else None
                )

                # Determine if the game is live (comparing in EST)
                is_live = False
                now_est = datetime.now(EST)
                if event_dt and event_dt <= now_est <= event_dt + timedelta(hours=2.5):
                    is_live = True

                rows.append(
                    {
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
                    }
                )

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    # Filter out past non-live games
    try:
        now_est = datetime.now(EST)
        if "time" in df.columns and "is_live" in df.columns:
            df = df[
                df["time"].isna()
                | (df["time"] >= now_est)
                | (df["is_live"] == True)
            ]
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
        raw_matchup = name_span.get_text(strip=True) if name_span else "Unknown"
        matchup = raw_matchup

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

        streams = [{"label": "SharkStreams", "embed_url": embed_link}]

        now_est = datetime.now(EST)
        is_live = event_dt <= now_est <= event_dt + timedelta(hours=2.5)

        rows.append(
            {
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
            }
        )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    try:
        now_est = datetime.now(EST)
        df = df[
            df["time"].isna()
            | (df["time"] >= now_est)
            | (df["is_live"] == True)
        ]
    except Exception as e:
        print("[shark][WARN] Failed to filter past non-live games:", e)

    return df


# ========= 3) FUNCTION TO GET STREAMS FROM WATCH PAGE =========
def get_all_streams_from_watch_page(watch_url: Optional[str]) -> List[Dict[str, Any]]:
    if not watch_url:
        return []

    try:
        r = requests.get(watch_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print("[ERROR] Non-200 status on watch URL:", r.status_code)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        streams: List[Dict[str, Any]] = []

        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            embed_url = iframe.get("src")
            streams.append({
                "label": "Main Stream",
                "embed_url": embed_url,
                "watch_url": watch_url,
            })

        stream_links = soup.select("div.stream-picker a.stream-button")
        if stream_links:
            for link in stream_links:
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
                            "watch_url": full_stream_url,
                        })

        return streams
    except Exception as e:
        print("[ERROR] Error fetching streams:", e)
        return []


# ========= 4) MATCHUP NORMALIZATION =========
TEAM_SEP_REGEX = re.compile(
    r'\bvs\b|\bvs.\b|\bv\b|\bv.\b| - | – | — | @ ',
    re.IGNORECASE,
)

def _normalize_team_name(team: str) -> str:
    if not isinstance(team, str):
        return ""
    s = team.lower()
    s = re.sub(r'[^a-z0-9]+', '', s)
    return s

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
    matchup = row.get("matchup") or ""
    matchup_key = make_matchup_key(matchup)

    time_val = row.get("time")
    if isinstance(time_val, (datetime, pd.Timestamp)):
        date_key = time_val.date()
    else:
        date_key = (row.get("date_header") or "").strip()

    return (date_key, sport, matchup_key)


# ========= 5) STREAM PARSING / MERGING (PRESERVE MANUAL STREAMS) =========
def _parse_streams_cell(val: Any) -> List[Dict[str, Any]]:
    if isinstance(val, list):
        return [x for x in val if isinstance(x, dict)]
    if val is None:
        return []
    if isinstance(val, float) and pd.isna(val):
        return []
    if isinstance(val, str):
        s = val.strip()
        if not s:
            return []
        try:
            parsed = ast.literal_eval(s)
            if isinstance(parsed, list):
                return [x for x in parsed if isinstance(x, dict)]
        except Exception:
            return []
    return []

def _dedup_streams_keep_order(streams: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for st in streams:
        if not isinstance(st, dict):
            continue
        url = (st.get("embed_url") or "").strip()
        if not url:
            continue
        if url in seen:
            continue
        seen.add(url)
        out.append(st)
    return out

def merge_existing_csv_streams(df_new: pd.DataFrame, existing_csv_path: str) -> pd.DataFrame:
    """
    Merge any streams already present in an existing CSV into df_new.
    Existing streams come FIRST (so manually-added ones are preserved and remain prominent).

    Also tries a fallback match on watch_url if the main key doesn't match
    (helpful when matchup wording changes).
    """
    if df_new.empty:
        return df_new

    if not os.path.exists(existing_csv_path):
        return df_new

    try:
        df_old = pd.read_csv(existing_csv_path)
    except Exception as e:
        print(f"[merge][WARN] Could not read existing CSV {existing_csv_path}: {e}")
        return df_new

    if df_old.empty:
        return df_new

    old_by_key: Dict[Tuple[Any, str, str], Dict[str, Any]] = {}
    old_by_watch: Dict[str, Dict[str, Any]] = {}

    for _, r in df_old.iterrows():
        row_dict = r.to_dict()

        # FIX: ALWAYS normalize time (tz-aware strings included)
        row_dict["time"] = _safe_to_dt_est(row_dict.get("time"))

        # parse streams
        row_dict["streams"] = _parse_streams_cell(row_dict.get("streams"))

        key = _row_game_key(row_dict)
        old_by_key[key] = row_dict

        w = _norm_url(row_dict.get("watch_url"))
        if w:
            old_by_watch[w] = row_dict

    merged_rows: List[Dict[str, Any]] = []
    for _, r in df_new.iterrows():
        new_row = r.to_dict()

        # normalize time so key matches old
        new_row["time"] = _safe_to_dt_est(new_row.get("time"))

        new_streams = _parse_streams_cell(new_row.get("streams"))
        new_row["streams"] = new_streams

        key = _row_game_key(new_row)
        old_row = old_by_key.get(key)

        # fallback by watch_url (covers matchup text changes)
        if old_row is None:
            w = _norm_url(new_row.get("watch_url"))
            if w:
                old_row = old_by_watch.get(w)

        if old_row:
            old_streams = _parse_streams_cell(old_row.get("streams"))

            # existing first (preserve manual)
            combined = old_streams + new_streams
            combined = _dedup_streams_keep_order(combined)
            new_row["streams"] = combined

            # embed_url priority: first combined stream
            if combined:
                new_row["embed_url"] = combined[0].get("embed_url")

            # preserve old embed_url if still missing
            if not new_row.get("embed_url") and old_row.get("embed_url"):
                new_row["embed_url"] = old_row.get("embed_url")

        new_row["streams"] = _dedup_streams_keep_order(new_row.get("streams") or [])
        if (not new_row.get("embed_url")) and new_row["streams"]:
            new_row["embed_url"] = new_row["streams"][0].get("embed_url")

        merged_rows.append(new_row)

    return pd.DataFrame(merged_rows)


# ========= 6) COMBINE SIMILAR GAMES (team-order–independent) =========
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

        if key not in combined:
            new_row = row.to_dict()
            streams_val = new_row.get("streams")
            new_row["streams"] = streams_val if isinstance(streams_val, list) else []
            combined[key] = new_row
        else:
            existing = combined[key]

            all_streams: List[Dict[str, Any]] = existing.get("streams") or []
            other_streams = row.get("streams")
            if isinstance(other_streams, list):
                all_streams.extend(other_streams)

            existing["streams"] = _dedup_streams_keep_order(all_streams)

            if existing["streams"]:
                existing["embed_url"] = existing["streams"][0].get("embed_url")
            else:
                if not existing.get("embed_url") and row.get("embed_url"):
                    existing["embed_url"] = row.get("embed_url")

            try:
                existing["is_live"] = bool(existing.get("is_live")) or bool(row.get("is_live"))
            except Exception:
                pass

            t_existing = existing.get("time")
            t_new = row.get("time")
            if pd.notna(t_new):
                if pd.isna(t_existing):
                    existing["time"] = t_new
                elif isinstance(t_existing, (datetime, pd.Timestamp)) and isinstance(t_new, (datetime, pd.Timestamp)):
                    if t_new < t_existing:
                        existing["time"] = t_new

            for col in ["tournament", "tournament_url", "watch_url"]:
                if not existing.get(col) and row.get(col):
                    existing[col] = row.get(col)

            src_existing = existing.get("source")
            src_new = row.get("source")
            if src_new:
                if src_existing:
                    if str(src_new) not in str(src_existing):
                        existing["source"] = f"{src_existing},{src_new}"
                else:
                    existing["source"] = src_new

    return pd.DataFrame(list(combined.values()))


# ========= 7) MAIN FUNCTION =========
def main() -> None:
    df_sport71 = scrape_today_games_sport71()

    if not df_sport71.empty:
        df_sport71["streams"] = df_sport71["watch_url"].apply(get_all_streams_from_watch_page)
        df_sport71["streams"] = df_sport71["streams"].apply(lambda x: x if isinstance(x, list) else [])
        df_sport71["embed_url"] = df_sport71["streams"].apply(
            lambda streams: streams[0]["embed_url"] if streams else None
        )

        # For *live* sport71 games, require an embed_url. Future games can be missing streams.
        try:
            df_sport71 = df_sport71[
                (~df_sport71["is_live"]) | df_sport71["embed_url"].notna()
            ]
        except Exception as e:
            print("[main][WARN] Could not filter sport71 rows by embed_url:", e)

    df_shark = scrape_today_games_shark()

    if not df_sport71.empty and not df_shark.empty:
        df = pd.concat([df_sport71, df_shark], ignore_index=True)
    elif not df_sport71.empty:
        df = df_sport71
    else:
        df = df_shark

    if df.empty:
        print("[main] No games found; nothing to write.")
        return

    # Combine similar games from sources
    df = combine_similar_games(df)

    # ✅ Merge existing CSV streams so manual additions survive
    df = merge_existing_csv_streams(df, OUTPUT_FILE)

    try:
        df.to_csv(OUTPUT_FILE, index=False)
        print(f"[main] wrote {OUTPUT_FILE} ({len(df)} rows)")
    except Exception as e:
        print(f"[main][ERROR] Failed to write CSV to {OUTPUT_FILE}:", e)


if __name__ == "__main__":
    main()
