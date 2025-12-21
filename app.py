# app.py
from flask import (
    Flask, render_template, abort, request, session, jsonify, redirect, url_for, make_response
)
import pandas as pd
import ast
import os
import random
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta, timezone
import uuid
import hashlib
import re
from urllib.parse import urljoin
import time
import threading

# For safe-ish CSV updates on Linux
import fcntl

# Optional: your scraper module (DO NOT run it in the web process by default)
import scrape_games

DATA_PATH = "today_games_with_all_streams.csv"

app = Flask(__name__)
app.secret_key = os.environ.get(
    "FLASK_SECRET_KEY",
    # fallback (you should set FLASK_SECRET_KEY in Render)
    "heg9q3248hg90a8dhg98q3h23948ghasdpghiuhweioruhgq8934ghiksadhg2398t394t9y898ydjdf8*898394ghhhgh%%%wghjgh23hgh"
)

# ====================== PERFORMANCE CONTROLS ======================
# Cache games in memory to avoid pd.read_csv + ast parsing on every request
GAMES_CACHE = {
    "games": [],
    "ts": 0.0,
    "mtime": 0.0,
}
GAMES_CACHE_LOCK = threading.Lock()

# Refresh at most every N seconds OR when file mtime changes
GAMES_CACHE_TTL_SECONDS = int(os.environ.get("GAMES_CACHE_TTL_SECONDS", "10"))

# Cloudflare / browser caching for HTML (keep short to avoid stale)
HTML_CACHE_SECONDS = int(os.environ.get("HTML_CACHE_SECONDS", "30"))

# Viewer tracking: keep it, but reduce work
ENABLE_VIEWER_TRACKING = os.environ.get("ENABLE_VIEWER_TRACKING", "1") == "1"

# IMPORTANT: do not run scraper in the web process unless explicitly enabled
ENABLE_SCRAPER_IN_WEB = os.environ.get("ENABLE_SCRAPER_IN_WEB", "0") == "1"
SCRAPE_INTERVAL_MINUTES = int(os.environ.get("SCRAPE_INTERVAL_MINUTES", "10"))

# ====================== ACTIVE VIEWER TRACKER ======================
ACTIVE_VIEWERS = {}        # session_id → last_seen timestamp
ACTIVE_PAGE_VIEWS = {}     # (session_id, path) → last_seen timestamp
LAST_VIEWER_PRINT = None   # throttle printing


def get_session_id():
    if "sid" not in session:
        session["sid"] = str(uuid.UUID(bytes=os.urandom(16)))
    return session["sid"]


def mark_active():
    """Fast path: only do work if enabled."""
    if not ENABLE_VIEWER_TRACKING:
        return
    sid = get_session_id()
    now = datetime.now(timezone.utc)
    ACTIVE_VIEWERS[sid] = now

    cutoff = now - timedelta(seconds=45)
    # keep cleanup cheap
    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    global LAST_VIEWER_PRINT

    if not ENABLE_VIEWER_TRACKING:
        return jsonify({"ok": True, "disabled": True})

    sid = get_session_id()
    now = datetime.now(timezone.utc)

    data = request.get_json(silent=True) or {}
    path = data.get("path") or request.path

    ACTIVE_VIEWERS[sid] = now
    ACTIVE_PAGE_VIEWS[(sid, path)] = now

    cutoff = now - timedelta(seconds=45)

    for key, ts in list(ACTIVE_PAGE_VIEWS.items()):
        if ts < cutoff:
            del ACTIVE_PAGE_VIEWS[key]

    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]

    # print at most once per minute
    if LAST_VIEWER_PRINT is None or (now - LAST_VIEWER_PRINT) > timedelta(seconds=60):
        total_active = len(ACTIVE_VIEWERS)

        home_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p == "/"}
        home_count = len(home_sids)

        game_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p.startswith("/game/") or p.startswith("/g/")}
        game_count = len(game_sids)

        print(f"[VIEWERS] Total active sessions (≈people): {total_active}")
        print(f"[VIEWERS] Active on '/': {home_count}")
        print(f"[VIEWERS] Active on game pages: {game_count}")

        LAST_VIEWER_PRINT = now

    return jsonify({"ok": True})


# ====================== UTILITIES ======================
TEAM_SEP_REGEX = re.compile(r"\bvs\b|\bvs.\b|\bv\b|\bv.\b| - | – | — | @ ", re.IGNORECASE)
SLUG_CLEAN_QUOTES = re.compile(r"['\"`]")
SLUG_NON_ALNUM = re.compile(r"[^a-z0-9]+")
SLUG_MULTI_DASH = re.compile(r"-{2,}")


def safe_lower(value):
    return value.lower() if isinstance(value, str) else ""


def normalize_sport_name(value):
    """
    Normalize sport values so grouping/sorting never mixes types.
    """
    if isinstance(value, str):
        value = value.strip()
        return value or "Other"
    try:
        text = str(value).strip()
        return text or "Other"
    except Exception:
        return "Other"


def coerce_start_datetime(rowd):
    """
    Try to produce a timezone-aware UTC datetime from available fields.
    Returns None if no reliable timestamp is present.
    """
    # Prefer unix timestamp if present
    ts = rowd.get("time_unix")
    if ts not in (None, ""):
        try:
            ts_float = float(ts)
            if not pd.isna(ts_float):
                # detect ms vs seconds
                if ts_float > 1e11:  # likely ms
                    ts_float = ts_float / 1000.0
                return datetime.fromtimestamp(ts_float, tz=timezone.utc)
        except Exception:
            pass

    # fallback: parse "time" column
    raw_time = rowd.get("time")
    if isinstance(raw_time, str) and raw_time.strip():
        try:
            dt = pd.to_datetime(raw_time, utc=True, errors="coerce")
            if isinstance(dt, pd.Timestamp) and not pd.isna(dt):
                return dt.to_pydatetime()
        except Exception:
            return None

    # final fallback: date_header only (assume start at midnight UTC)
    date_header = rowd.get("date_header")
    if isinstance(date_header, str) and date_header.strip():
        try:
            dt = pd.to_datetime(date_header, utc=True, errors="coerce")
            if isinstance(dt, pd.Timestamp) and not pd.isna(dt):
                return dt.to_pydatetime()
        except Exception:
            return None

    return None


def make_stable_id(row):
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def slugify(text: str) -> str:
    if not isinstance(text, str):
        return ""
    s = text.strip().lower()
    s = SLUG_CLEAN_QUOTES.sub("", s)
    s = SLUG_NON_ALNUM.sub("-", s)
    s = SLUG_MULTI_DASH.sub("-", s).strip("-")
    return s


def game_slug(game: dict) -> str:
    date_part = slugify(str(game.get("date_header") or "today"))
    matchup_part = slugify(str(game.get("matchup") or "game"))
    sport_part = slugify(str(game.get("sport") or "sport"))
    base = f"{date_part}-{matchup_part}-{sport_part}"
    base = SLUG_MULTI_DASH.sub("-", base).strip("-")

    gid = str(game.get("id") or "")
    suffix = gid[-4:] if gid else "0000"
    return f"{base}-{suffix}"


def normalize_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "live", "t")


def parse_streams_cell(cell_value):
    """CSV stores streams as python-literal list of dicts."""
    if cell_value is None or (isinstance(cell_value, float) and pd.isna(cell_value)):
        return []
    if isinstance(cell_value, list):
        return cell_value

    s = str(cell_value).strip()
    if not s:
        return []
    try:
        parsed = ast.literal_eval(s)
        if isinstance(parsed, list):
            out = []
            for item in parsed:
                if isinstance(item, dict) and item.get("embed_url"):
                    # keep extra keys if present, but normalize basics
                    fixed = dict(item)
                    fixed["label"] = fixed.get("label") or "Stream"
                    fixed["embed_url"] = fixed.get("embed_url")
                    fixed["watch_url"] = fixed.get("watch_url")
                    out.append(fixed)
            return out
    except Exception:
        return []
    return []


def streams_to_cell(streams_list):
    cleaned = []
    for s in (streams_list or []):
        if not isinstance(s, dict):
            continue
        embed = s.get("embed_url")
        if not embed:
            continue
        fixed = dict(s)
        fixed["label"] = fixed.get("label") or "Stream"
        fixed["embed_url"] = embed
        cleaned.append(fixed)
    return repr(cleaned)


def ensure_csv_exists_with_header():
    if os.path.exists(DATA_PATH):
        return
    cols = [
        "source", "date_header", "sport", "time_unix", "time",
        "tournament", "tournament_url", "matchup", "watch_url",
        "is_live", "streams", "embed_url"
    ]
    df = pd.DataFrame(columns=cols)
    df.to_csv(DATA_PATH, index=False)
    print(f"[csv] Created empty CSV at {DATA_PATH}")


def _read_csv_shared_locked(path: str) -> pd.DataFrame:
    """
    FAST READ PATH:
    - shared lock (LOCK_SH) so we don't block readers on writers more than necessary
    - used only when refreshing the in-memory cache
    """
    ensure_csv_exists_with_header()
    with open(path, "r", encoding="utf-8") as fh:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_SH)
        except Exception:
            pass
        try:
            df = pd.read_csv(fh)
        except Exception:
            df = pd.DataFrame(columns=[
                "source", "date_header", "sport", "time_unix", "time",
                "tournament", "tournament_url", "matchup", "watch_url",
                "is_live", "streams", "embed_url"
            ])
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
    return df


def read_csv_locked_for_write():
    """
    WRITE PATH (admin APIs):
    exclusive lock to safely modify file
    """
    ensure_csv_exists_with_header()
    fh = open(DATA_PATH, "r+", encoding="utf-8")
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX)

    fh.seek(0)
    try:
        df = pd.read_csv(fh)
    except Exception:
        df = pd.DataFrame(columns=[
            "source", "date_header", "sport", "time_unix", "time",
            "tournament", "tournament_url", "matchup", "watch_url",
            "is_live", "streams", "embed_url"
        ])
    return df, fh


def write_csv_locked(df, fh):
    fh.seek(0)
    fh.truncate(0)
    df.to_csv(fh, index=False)
    fh.flush()
    os.fsync(fh.fileno())
    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    fh.close()


def require_admin():
    required = os.environ.get("ADMIN_API_KEY", "").strip()
    if not required:
        return True
    got = request.headers.get("X-API-Key", "").strip()
    return got == required


def _dedup_stream_slug(slug: str, seen: set) -> str:
    if not slug:
        slug = "stream"
    base = slug
    i = 2
    while slug in seen:
        slug = f"{base}-{i}"
        i += 1
    seen.add(slug)
    return slug


# ====================== FAST GAME LOADER (CACHED) ======================
SPORT_MAP = {
    "Football": "Soccer",
    "American Football": "NFL",
    "NBA": "Basketball",
}

SPORT_KEYWORD_MAP = [
    ("nba", "Basketball"),
    ("basketball", "Basketball"),
    ("wnba", "Basketball"),
    ("ncaa basketball", "Basketball"),
    ("college basketball", "Basketball"),
    ("nfl", "NFL"),
    ("american football", "NFL"),
    ("ncaa football", "College Football"),
    ("college football", "College Football"),
    ("mlb", "MLB"),
    ("baseball", "MLB"),
    ("nhl", "Ice Hockey"),
    ("hockey", "Ice Hockey"),
    ("soccer", "Soccer"),
    ("mls", "Soccer"),
    ("premier league", "Soccer"),
    ("la liga", "Soccer"),
    ("bundesliga", "Soccer"),
    ("serie a", "Soccer"),
    ("ligue 1", "Soccer"),
    ("champions league", "Soccer"),
    ("uefa", "Soccer"),
    ("ucl", "Soccer"),
    ("ufc", "MMA"),
    ("mma", "MMA"),
    ("bellator", "MMA"),
    ("boxing", "Boxing"),
    ("formula 1", "Motorsport"),
    ("formula1", "Motorsport"),
    ("f1", "Motorsport"),
    ("f2", "Motorsport"),
    ("nascar", "Motorsport"),
    ("motogp", "Motorsport"),
    ("tennis", "Tennis"),
    ("atp", "Tennis"),
    ("wta", "Tennis"),
    ("golf", "Golf"),
    ("pga", "Golf"),
    ("lpga", "Golf"),
    ("cricket", "Cricket"),
    ("rugby", "Rugby"),
]


def _build_games_from_df(df: pd.DataFrame):
    if df is None or df.empty:
        return []

    # Ensure columns exist
    for col in ["streams", "is_live", "sport", "time", "date_header", "tournament", "tournament_url", "matchup", "watch_url", "time_unix"]:
        if col not in df.columns:
            df[col] = ""

    games = []
    now_utc = datetime.now(timezone.utc)
    stale_cutoff = now_utc - timedelta(hours=6)
    live_window_after_start = timedelta(hours=5)  # typical game length coverage

    # Iterate rows once, parse streams once
    for _, row in df.iterrows():
        rowd = row.to_dict()

        # Parse streams (fast)
        streams = parse_streams_cell(rowd.get("streams"))

        # stable id (fast)
        game_id = make_stable_id(rowd)

        raw_sport = rowd.get("sport")
        raw_sport = raw_sport.strip() if isinstance(raw_sport, str) else raw_sport
        sport = SPORT_MAP.get(raw_sport, raw_sport)
        if not sport:
            # infer from other fields to avoid "unknown" buckets
            haystack_parts = [
                rowd.get("tournament", ""),
                rowd.get("matchup", ""),
                rowd.get("watch_url", ""),
                rowd.get("source", ""),
            ]
            haystack = " ".join([str(p or "") for p in haystack_parts]).lower()
            for keyword, mapped in SPORT_KEYWORD_MAP:
                if keyword in haystack:
                    sport = mapped
                    break
        sport = sport or "Other"

        normalized_sport = sport.lower() if isinstance(sport, str) else ""
        needs_infer = (
            not normalized_sport
            or normalized_sport in ("other", "unknown", "nan", "n/a", "none")
        )

        if needs_infer:
            # infer from other fields to avoid "unknown" buckets
            haystack_parts = [
                rowd.get("sport", ""),
                rowd.get("tournament", ""),
                rowd.get("matchup", ""),
                rowd.get("watch_url", ""),
                rowd.get("source", ""),
            ]
            haystack = " ".join([str(p or "") for p in haystack_parts]).lower()
            for keyword, mapped in SPORT_KEYWORD_MAP:
                if keyword in haystack:
                    sport = mapped
                    break

        sport = sport or "Sports"

        normalized_sport = sport.lower() if isinstance(sport, str) else ""
        needs_infer = (
            not normalized_sport
            or normalized_sport in ("other", "unknown", "nan", "n/a", "none")
        )

        if needs_infer:
            # infer from other fields to avoid "unknown" buckets
            haystack_parts = [
                rowd.get("sport", ""),
                rowd.get("tournament", ""),
                rowd.get("matchup", ""),
                rowd.get("watch_url", ""),
                rowd.get("source", ""),
            ]
            haystack = " ".join([str(p or "") for p in haystack_parts]).lower()
            for keyword, mapped in SPORT_KEYWORD_MAP:
                if keyword in haystack:
                    sport = mapped
                    break

        sport = sport or "Sports"

        normalized_sport = sport.lower() if isinstance(sport, str) else ""
        needs_infer = (
            not normalized_sport
            or normalized_sport in ("other", "unknown", "nan", "n/a", "none")
        )

        if needs_infer:
            # infer from other fields to avoid "unknown" buckets
            haystack_parts = [
                rowd.get("sport", ""),
                rowd.get("tournament", ""),
                rowd.get("matchup", ""),
                rowd.get("watch_url", ""),
                rowd.get("source", ""),
            ]
            haystack = " ".join([str(p or "") for p in haystack_parts]).lower()
            for keyword, mapped in SPORT_KEYWORD_MAP:
                if keyword in haystack:
                    sport = mapped
                    break

        sport = sport or "Sports"

        # Determine start time once for both filtering and live inference
        start_dt = coerce_start_datetime(rowd)

        # Skip streams that started >6 hours ago (based on best-effort timestamp)
        if start_dt and start_dt < stale_cutoff:
            continue

        # Base live flag from data
        is_live = normalize_bool(rowd.get("is_live"))
        if not is_live and start_dt:
            # Auto-mark live if we're within a reasonable window of the start
            if (start_dt - timedelta(minutes=15)) <= now_utc <= (start_dt + live_window_after_start):
                is_live = True

        # Skip streams that started >6 hours ago
        start_dt = coerce_start_datetime(rowd)
        if start_dt and start_dt < stale_cutoff:
            continue

        # format time once
        time_display = None
        raw_time = rowd.get("time")
        if isinstance(raw_time, str) and raw_time.strip():
            try:
                dt = pd.to_datetime(raw_time, errors="coerce")
                if not pd.isna(dt):
                    time_display = dt.strftime("%I:%M %p ET").lstrip("0")
            except Exception:
                time_display = None

        game_obj = {
            "id": game_id,
            "date_header": rowd.get("date_header"),
            "sport": sport,
            "time_unix": rowd.get("time_unix"),
            "time": time_display,
            "tournament": rowd.get("tournament"),
            "tournament_url": rowd.get("tournament_url"),
            "matchup": rowd.get("matchup"),
            "watch_url": rowd.get("watch_url"),
            "streams": streams,
            "is_live": is_live,
        }

        game_obj["slug"] = game_slug(game_obj)

        # attach stream slugs
        seen = set()
        for s in game_obj["streams"]:
            label = (s.get("label") or "Stream")
            s_slug = slugify(label)
            s["slug"] = _dedup_stream_slug(s_slug, seen)

        games.append(game_obj)

    return games


def load_games_cached():
    """
    This is the #1 speed win:
      - no pd.read_csv per request
      - no ast.literal_eval per request
    """
    now = time.time()
    try:
        mtime = os.path.getmtime(DATA_PATH) if os.path.exists(DATA_PATH) else 0.0
    except Exception:
        mtime = 0.0

    with GAMES_CACHE_LOCK:
        cache_ok = (
            GAMES_CACHE["games"]
            and (now - GAMES_CACHE["ts"] < GAMES_CACHE_TTL_SECONDS)
            and (mtime == GAMES_CACHE["mtime"])
        )
        if cache_ok:
            return GAMES_CACHE["games"]

    # Refresh outside lock (avoid blocking concurrent requests)
    df = _read_csv_shared_locked(DATA_PATH)
    games = _build_games_from_df(df)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["games"] = games
        GAMES_CACHE["ts"] = now
        GAMES_CACHE["mtime"] = mtime

    return games


# ====================== VIEW COUNTS ======================
def get_game_view_counts(cutoff_seconds=45):
    if not ENABLE_VIEWER_TRACKING:
        return {}

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(seconds=cutoff_seconds)
    counts = {}

    for (sid, path), ts in list(ACTIVE_PAGE_VIEWS.items()):
        if ts < cutoff:
            continue
        if not path.startswith("/game/"):
            continue
        try:
            game_id_str = path.rstrip("/").split("/")[-1]
            game_id = int(game_id_str)
        except ValueError:
            continue
        counts[game_id] = counts.get(game_id, 0) + 1

    return counts


def get_most_viewed_games(all_games, limit=5):
    counts = get_game_view_counts()
    if not counts:
        return []

    games_by_id = {g["id"]: g for g in all_games}
    sorted_ids = sorted(counts.keys(), key=lambda gid: counts[gid], reverse=True)

    result = []
    for gid in sorted_ids:
        game = games_by_id.get(gid)
        if not game:
            continue
        g_copy = dict(game)
        g_copy["active_viewers"] = counts[gid]
        result.append(g_copy)
        if len(result) >= limit:
            break

    return result


# ====================== WRITE API ======================
CSV_COLS = [
    "source", "date_header", "sport", "time_unix", "time",
    "tournament", "tournament_url", "matchup", "watch_url",
    "is_live", "streams", "embed_url"
]


def find_row_index_by_game_id(df, game_id: int):
    # NOTE: still O(n), but admin-only; fine.
    for i, row in df.iterrows():
        try:
            rid = make_stable_id(row)
            if rid == game_id:
                return int(i)
        except Exception:
            continue
    return None


def merge_streams(existing, incoming):
    def norm(s):
        return (
            (s.get("embed_url") or "").strip(),
            (s.get("watch_url") or "").strip(),
            (s.get("label") or "").strip().lower()
        )

    seen = set()
    out = []

    for s in (existing or []):
        if not isinstance(s, dict) or not s.get("embed_url"):
            continue
        k = norm(s)
        if k in seen:
            continue
        seen.add(k)
        out.append(dict(s))

    for s in (incoming or []):
        if not isinstance(s, dict) or not s.get("embed_url"):
            continue
        k = norm(s)
        if k in seen:
            continue
        seen.add(k)
        out.append(dict(s))

    # normalize labels
    for s in out:
        s["label"] = s.get("label") or "Stream"
    return out


@app.route("/api/streams/add", methods=["POST"])
def api_add_streams():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    if "game_id" not in payload:
        return jsonify({"ok": False, "error": "missing game_id"}), 400

    try:
        game_id = int(payload["game_id"])
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be an int"}), 400

    incoming_streams = payload.get("streams")
    if incoming_streams is None:
        single = payload.get("stream")
        incoming_streams = [single] if single else []

    if not isinstance(incoming_streams, list):
        return jsonify({"ok": False, "error": "streams must be a list"}), 400

    df, fh = read_csv_locked_for_write()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    idx = find_row_index_by_game_id(df, game_id)
    if idx is None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()
        return jsonify({"ok": False, "error": "game not found", "game_id": game_id}), 404

    existing_streams = parse_streams_cell(df.at[idx, "streams"])
    merged = merge_streams(existing_streams, incoming_streams)

    df.at[idx, "streams"] = streams_to_cell(merged)

    set_embed_url = payload.get("set_embed_url")
    if isinstance(set_embed_url, str) and set_embed_url.strip():
        df.at[idx, "embed_url"] = set_embed_url.strip()
    else:
        cur_embed = df.at[idx, "embed_url"] if "embed_url" in df.columns else ""
        if (not isinstance(cur_embed, str) or not cur_embed.strip()) and merged:
            df.at[idx, "embed_url"] = merged[0].get("embed_url")

    if "set_is_live" in payload:
        df.at[idx, "is_live"] = bool(payload.get("set_is_live"))

    write_csv_locked(df[CSV_COLS], fh)

    # IMPORTANT: invalidate in-memory cache so next request sees changes immediately
    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "game_id": game_id, "streams_count": len(merged)})


@app.route("/api/games/remove", methods=["POST"])
def api_games_remove():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}

    if "game_id" not in payload:
        return jsonify({"ok": False, "error": "missing game_id"}), 400

    try:
        game_id = int(payload["game_id"])
    except Exception:
        return jsonify({"ok": False, "error": "game_id must be an int"}), 400

    df, fh = read_csv_locked_for_write()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    idx = find_row_index_by_game_id(df, game_id)
    if idx is None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()
        return jsonify({"ok": False, "error": "game not found", "game_id": game_id}), 404

    df = df.drop(index=idx).reset_index(drop=True)
    write_csv_locked(df[CSV_COLS], fh)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "removed": True, "game_id": game_id, "rows_now": int(len(df))})


@app.route("/api/games/upsert", methods=["POST"])
def api_games_upsert():
    if not require_admin():
        return jsonify({"ok": False, "error": "unauthorized"}), 401

    payload = request.get_json(silent=True) or {}

    games = []
    if isinstance(payload.get("game"), dict):
        games = [payload["game"]]
    elif isinstance(payload.get("games"), list):
        games = [g for g in payload["games"] if isinstance(g, dict)]
    else:
        return jsonify({"ok": False, "error": "expected 'game' object or 'games' list"}), 400

    df, fh = read_csv_locked_for_write()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    upserted = []
    for g in games:
        row_like = {
            "date_header": g.get("date_header", ""),
            "sport": g.get("sport", ""),
            "tournament": g.get("tournament", ""),
            "matchup": g.get("matchup", ""),
        }
        game_id = make_stable_id(row_like)

        idx = find_row_index_by_game_id(df, game_id)

        streams_list = g.get("streams")
        if isinstance(streams_list, str):
            streams_list = parse_streams_cell(streams_list)
        elif not isinstance(streams_list, list):
            streams_list = []

        embed_url = g.get("embed_url")
        if not (isinstance(embed_url, str) and embed_url.strip()):
            embed_url = streams_list[0].get("embed_url") if streams_list else ""

        new_row = {
            "source": g.get("source", ""),
            "date_header": g.get("date_header", ""),
            "sport": g.get("sport", ""),
            "time_unix": g.get("time_unix", ""),
            "time": g.get("time", ""),
            "tournament": g.get("tournament", ""),
            "tournament_url": g.get("tournament_url", ""),
            "matchup": g.get("matchup", ""),
            "watch_url": g.get("watch_url", ""),
            "is_live": normalize_bool(g.get("is_live")),
            "streams": streams_to_cell(streams_list),
            "embed_url": embed_url or "",
        }

        if idx is None:
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            action = "inserted"
        else:
            existing_streams = parse_streams_cell(df.at[idx, "streams"])
            merged = merge_streams(existing_streams, streams_list)
            new_row["streams"] = streams_to_cell(merged)
            if not new_row["embed_url"] and merged:
                new_row["embed_url"] = merged[0].get("embed_url") or ""

            for k, v in new_row.items():
                df.at[idx, k] = v
            action = "updated"

        upserted.append({"game_id": game_id, "action": action})

    write_csv_locked(df[CSV_COLS], fh)

    with GAMES_CACHE_LOCK:
        GAMES_CACHE["ts"] = 0
        GAMES_CACHE["mtime"] = 0

    return jsonify({"ok": True, "results": upserted})


# ====================== ROUTES ======================
def _absolute_url(path: str) -> str:
    return urljoin(request.url_root, path.lstrip("/"))


@app.after_request
def add_cache_headers(resp):
    """
    Helps Cloudflare + browser caching. Keep short so you can update quickly.
    """
    try:
        # only cache GET HTML responses
        if request.method == "GET" and resp.mimetype in ("text/html", "text/plain"):
            resp.headers["Cache-Control"] = f"public, max-age={HTML_CACHE_SECONDS}"
    except Exception:
        pass
    return resp


@app.route("/")
def index():
    mark_active()

    all_games = load_games_cached()
    games = list(all_games)

    q = request.args.get("q", "").strip().lower()
    if q:
        games = [
            g for g in games
            if q in safe_lower(g.get("matchup"))
            or q in safe_lower(g.get("sport"))
            or q in safe_lower(g.get("tournament"))
        ]

    live_only = request.args.get("live_only", "").lower() in ("1", "true", "yes", "on")
    if live_only:
        games = [g for g in games if g.get("is_live")]

    sections_by_sport = {}
    for g in games:
        sport = normalize_sport_name(g.get("sport"))
        sections_by_sport.setdefault(sport, []).append(g)

    sections = [{"sport": s, "games": lst} for s, lst in sections_by_sport.items()]
    sections.sort(key=lambda s: normalize_sport_name(s["sport"]).lower())

    most_viewed_games = get_most_viewed_games(all_games, limit=5)

    return render_template(
        "index.html",
        sections=sections,
        search_query=q,
        live_only=live_only,
        most_viewed_games=most_viewed_games,
    )


@app.route("/game/<int:game_id>")
def game_detail(game_id: int):
    mark_active()

    requested_stream_slug = request.args.get("stream", "").strip()

    games = load_games_cached()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    other_games = [
        g for g in games
        if g["id"] != game_id and g.get("streams") and len(g["streams"]) > 0
    ]

    open_ad = random.random() < 0.25

    slug = game.get("slug") or game_slug(game)
    share_id_url = _absolute_url(url_for("game_detail", game_id=game_id))
    share_slug_url = _absolute_url(url_for("game_by_slug", slug=slug))
    og_image_url = _absolute_url(url_for("static", filename="preview.png"))

    return render_template(
        "game.html",
        game=game,
        other_games=other_games,
        open_ad=open_ad,
        share_id_url=share_id_url,
        share_slug_url=share_slug_url,
        og_image_url=og_image_url,
        requested_stream_slug=requested_stream_slug,
    )


@app.route("/g/<slug>")
def game_by_slug(slug: str):
    mark_active()

    slug = (slug or "").strip().lower()
    if not slug:
        abort(404)

    games = load_games_cached()
    game = next((g for g in games if (g.get("slug") or "").lower() == slug), None)
    if not game:
        game = next((g for g in games if (g.get("slug") or "").lower().startswith(slug)), None)
    if not game:
        abort(404)

    qs = request.query_string.decode("utf-8", errors="ignore").strip()
    target = url_for("game_detail", game_id=game["id"])
    if qs:
        target = f"{target}?{qs}"

    return redirect(target, code=302)


# ====================== SCHEDULER (OFF BY DEFAULT) ======================
def run_scraper_job():
    try:
        scrape_games.main()
        # invalidate cache after scrape
        with GAMES_CACHE_LOCK:
            GAMES_CACHE["ts"] = 0
            GAMES_CACHE["mtime"] = 0
    except Exception as e:
        print(f"[scheduler][ERROR] Scraper error: {e}")


def start_scheduler():
    # DO NOT do an immediate blocking run (this causes slow boot & can stall the web service)
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scraper_job,
        "interval",
        minutes=SCRAPE_INTERVAL_MINUTES,
        id="scrape_job",
        replace_existing=True,
    )
    scheduler.start()
    print("[scheduler] Background scheduler started.")
    atexit.register(lambda: scheduler.shutdown(wait=False))


def trigger_startup_scrape():
    """Kick off a one-time scrape at startup without blocking boot."""

    def _run():
        print("[scheduler] Running initial scrape on startup...")
        run_scraper_job()

    t = threading.Thread(target=_run, daemon=True)
    t.start()


if ENABLE_SCRAPER_IN_WEB:
    # Only start in web if explicitly enabled via env var
    if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
        trigger_startup_scrape()
        start_scheduler()


if __name__ == "__main__":
    # Local dev
    app.run(host="127.0.0.1", port=5000, debug=False)
