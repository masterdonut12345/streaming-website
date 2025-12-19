from flask import Flask, render_template, abort, request, session, jsonify
import pandas as pd
import ast
import os
import random
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
import uuid
import hashlib  # for stable IDs

# For safe-ish CSV updates on Linux
import fcntl
import json

import scrape_games  # our scraper module

DATA_PATH = "today_games_with_all_streams.csv"

app = Flask(__name__)
app.secret_key = "heg9q3248hg90a8dhg98q3h23948ghasdpghiuhweioruhgq8934ghiksadhg2398t394t9y898ydjdf8*898394ghhhgh%%%wghjgh23hgh"

# ---------------------- ACTIVE VIEWER TRACKER ----------------------

ACTIVE_VIEWERS = {}        # session_id → last_seen timestamp
ACTIVE_PAGE_VIEWS = {}     # (session_id, path) → last_seen timestamp
LAST_VIEWER_PRINT = None   # throttle printing


def get_session_id():
    """Assign each visitor a unique ID if they don't already have one."""
    if "sid" not in session:
        session["sid"] = str(uuid.UUID(bytes=os.urandom(16)))
    return session["sid"]


def mark_active():
    """Mark this session as active (site-wide) and clean out inactive ones."""
    sid = get_session_id()
    now = datetime.utcnow()
    ACTIVE_VIEWERS[sid] = now

    cutoff = now - timedelta(minutes=2)
    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    global LAST_VIEWER_PRINT

    sid = get_session_id()
    now = datetime.utcnow()

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

    if LAST_VIEWER_PRINT is None or (now - LAST_VIEWER_PRINT) > timedelta(seconds=60):
        total_active = len(ACTIVE_VIEWERS)

        home_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p == "/"}
        home_count = len(home_sids)

        game_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p.startswith("/game/")}
        game_count = len(game_sids)

        print(f"[VIEWERS] Total active sessions (≈people): {total_active}")
        print(f"[VIEWERS] Active on '/': {home_count}")
        print(f"[VIEWERS] Active on game pages: {game_count}")

        LAST_VIEWER_PRINT = now

    return jsonify({"ok": True})


# ---------------------- UTILITIES ----------------------

def safe_lower(value):
    return value.lower() if isinstance(value, str) else ""


def make_stable_id(row):
    """
    Build a stable integer ID from key fields so that the same game
    always gets the same ID even if CSV row order or row index changes.
    """
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def normalize_bool(v):
    if isinstance(v, bool):
        return v
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "yes", "y", "live", "t")


def parse_streams_cell(cell_value):
    """
    CSV stores streams as a python-literal list of dicts, e.g.:
      "[{'label': 'Main Stream', 'embed_url': '...', 'watch_url': '...'}]"
    We parse safely with ast.literal_eval.
    """
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
                    out.append({
                        "label": item.get("label") or "Stream",
                        "embed_url": item.get("embed_url"),
                        "watch_url": item.get("watch_url"),
                    })
            return out
    except Exception:
        return []
    return []


def streams_to_cell(streams_list):
    """
    Convert list[dict] -> a python-literal string that your current load_games()
    can ast.literal_eval().
    """
    cleaned = []
    for s in (streams_list or []):
        if not isinstance(s, dict):
            continue
        embed = s.get("embed_url")
        if not embed:
            continue
        cleaned.append({
            "label": s.get("label") or "Stream",
            "embed_url": embed,
            "watch_url": s.get("watch_url"),
        })
    return repr(cleaned)


def ensure_csv_exists_with_header():
    """
    If the CSV doesn't exist yet, create it with your expected columns.
    """
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


def read_csv_locked():
    """
    Read CSV under an exclusive lock so concurrent requests don't corrupt the file.
    Returns (df, file_handle). Caller MUST close fh.
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
    """
    Write df back into the same file handle fh (which is LOCK_EX).
    """
    fh.seek(0)
    fh.truncate(0)
    df.to_csv(fh, index=False)
    fh.flush()
    os.fsync(fh.fileno())
    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    fh.close()


def require_admin():
    """
    Very simple shared-secret auth for write endpoints.
    Set ADMIN_API_KEY in env. If not set, endpoint is open (not recommended).
    """
    required = os.environ.get("ADMIN_API_KEY", "").strip()
    if not required:
        return True  # open if unset
    got = request.headers.get("X-API-Key", "").strip()
    return got == required


def load_games():
    if not os.path.exists(DATA_PATH):
        print(f"[load_games][ERROR] CSV not found at {DATA_PATH}")
        return []

    try:
        df = pd.read_csv(DATA_PATH)
    except Exception as e:
        print(f"[load_games][ERROR] Error reading CSV: {e}")
        return []

    if df.empty:
        return []

    games = []

    sport_map = {
        "Football": "Soccer",
        "American Football": "NFL",
        "NBA": "Basketball",
    }

    for idx, row in df.iterrows():
        streams = []
        if "streams" in df.columns and pd.notna(row.get("streams")):
            try:
                parsed = ast.literal_eval(str(row["streams"]))
                if isinstance(parsed, list):
                    for s in parsed:
                        if isinstance(s, dict) and s.get("embed_url"):
                            streams.append({
                                "label": s.get("label"),
                                "embed_url": s.get("embed_url"),
                            })
            except Exception as e:
                print(f"[load_games][ERROR] Error parsing streams for row {idx}: {e}")
                streams = []

        game_id = make_stable_id(row)

        raw_sport = row.get("sport")
        if isinstance(raw_sport, str):
            raw_sport = raw_sport.strip()
        sport = sport_map.get(raw_sport, raw_sport)

        is_live = False
        if "is_live" in df.columns:
            raw = str(row.get("is_live")).lower().strip()
            is_live = raw in ("1", "true", "yes", "y", "live")

        raw_time = row.get("time")
        time_display = None

        if isinstance(raw_time, str) and raw_time.strip():
            try:
                dt = pd.to_datetime(raw_time)
                time_display = dt.strftime("%I:%M %p ET").lstrip("0")
            except Exception as e:
                print(f"[load_games][WARN] Could not parse time for row {idx}: {raw_time} ({e})")
                time_display = None

        games.append({
            "id": game_id,
            "date_header": row.get("date_header"),
            "sport": sport,
            "time_unix": row.get("time_unix"),
            "time": time_display,
            "tournament": row.get("tournament"),
            "tournament_url": row.get("tournament_url"),
            "matchup": row.get("matchup"),
            "watch_url": row.get("watch_url"),
            "streams": streams,
            "is_live": is_live,
        })
    return games


def get_game_view_counts(cutoff_seconds=45):
    now = datetime.utcnow()
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


# ---------------------- WRITE API: ADD/UPSERT STREAMS ----------------------

CSV_COLS = [
    "source", "date_header", "sport", "time_unix", "time",
    "tournament", "tournament_url", "matchup", "watch_url",
    "is_live", "streams", "embed_url"
]


def find_row_index_by_game_id(df, game_id: int):
    """
    Your CSV doesn't store ID, so we compute stable IDs for each row and match.
    """
    for i, row in df.iterrows():
        try:
            rid = make_stable_id(row)
            if rid == game_id:
                return int(i)
        except Exception:
            continue
    return None


def merge_streams(existing, incoming):
    """
    Deduplicate by embed_url + watch_url + label (normalized).
    """
    def norm(s):
        return (s.get("embed_url") or "").strip(), (s.get("watch_url") or "").strip(), (s.get("label") or "").strip().lower()

    seen = set()
    out = []
    for s in (existing or []):
        if not isinstance(s, dict) or not s.get("embed_url"):
            continue
        k = norm(s)
        if k in seen:
            continue
        seen.add(k)
        out.append({
            "label": s.get("label") or "Stream",
            "embed_url": s.get("embed_url"),
            "watch_url": s.get("watch_url"),
        })

    for s in (incoming or []):
        if not isinstance(s, dict) or not s.get("embed_url"):
            continue
        k = norm(s)
        if k in seen:
            continue
        seen.add(k)
        out.append({
            "label": s.get("label") or "Stream",
            "embed_url": s.get("embed_url"),
            "watch_url": s.get("watch_url"),
        })

    return out


@app.route("/api/streams/add", methods=["POST"])
def api_add_streams():
    """
    Add one or more streams to an existing game.
    Request JSON:
      {
        "game_id": 123456789,
        "streams": [
          {"label": "Stream: X", "embed_url": "...", "watch_url": "..."},
          ...
        ],
        "set_is_live": true/false (optional),
        "set_embed_url": "..." (optional)  # sets the row-level embed_url column
      }

    Auth:
      X-API-Key: <ADMIN_API_KEY>
    """
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
        # Allow single stream object as "stream"
        single = payload.get("stream")
        if single:
            incoming_streams = [single]
        else:
            incoming_streams = []

    if not isinstance(incoming_streams, list):
        return jsonify({"ok": False, "error": "streams must be a list"}), 400

    df, fh = read_csv_locked()
    # Ensure columns exist (older files / weird writes)
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

    # Optionally keep embed_url column in sync with "main" stream
    set_embed_url = payload.get("set_embed_url")
    if isinstance(set_embed_url, str) and set_embed_url.strip():
        df.at[idx, "embed_url"] = set_embed_url.strip()
    else:
        # If embed_url empty, set it to first stream's embed_url
        cur_embed = df.at[idx, "embed_url"] if "embed_url" in df.columns else ""
        if (not isinstance(cur_embed, str) or not cur_embed.strip()) and merged:
            df.at[idx, "embed_url"] = merged[0].get("embed_url")

    if "set_is_live" in payload:
        df.at[idx, "is_live"] = bool(payload.get("set_is_live"))

    write_csv_locked(df[CSV_COLS], fh)

    return jsonify({
        "ok": True,
        "game_id": game_id,
        "streams_count": len(merged)
    })


@app.route("/api/games/upsert", methods=["POST"])
def api_games_upsert():
    """
    Upsert a full game row into the CSV (update if stable-id matches, else append).

    Request JSON:
      {
        "game": {
          "source": "sport71",
          "date_header": "Friday, December 19, 2025",
          "sport": "Basketball",
          "time_unix": 1766113200000,
          "time": "2025-12-18 22:00:00-05:00",
          "tournament": "NBA",
          "tournament_url": "https://...",
          "matchup": "Portland Trail Blazers - Sacramento Kings",
          "watch_url": "https://...",
          "is_live": true,
          "streams": [
            {"label":"Main Stream","embed_url":"https://...","watch_url":"https://..."}
          ],
          "embed_url": "https://..."   # optional; if omitted we set to first stream
        }
      }

    You can also send:
      { "games": [ {game1}, {game2}, ... ] }

    Auth:
      X-API-Key: <ADMIN_API_KEY>
    """
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

    df, fh = read_csv_locked()
    for c in CSV_COLS:
        if c not in df.columns:
            df[c] = ""

    upserted = []
    for g in games:
        # Build a "row-like" dict for stable id computation
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
            # If caller mistakenly sends CSV-literal string, parse it
            streams_list = parse_streams_cell(streams_list)
        elif not isinstance(streams_list, list):
            streams_list = []

        embed_url = g.get("embed_url")
        if not (isinstance(embed_url, str) and embed_url.strip()):
            if streams_list:
                embed_url = streams_list[0].get("embed_url")
            else:
                embed_url = ""

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
            # Update fields, but if streams provided, MERGE (don’t blow away existing)
            existing_streams = parse_streams_cell(df.at[idx, "streams"])
            merged = merge_streams(existing_streams, streams_list)
            new_row["streams"] = streams_to_cell(merged)

            # Keep embed_url sane
            if not new_row["embed_url"] and merged:
                new_row["embed_url"] = merged[0].get("embed_url") or ""

            for k, v in new_row.items():
                df.at[idx, k] = v
            action = "updated"

        upserted.append({"game_id": game_id, "action": action})

    write_csv_locked(df[CSV_COLS], fh)

    return jsonify({"ok": True, "results": upserted})


# ---------------------- ROUTES ----------------------

@app.route("/")
def index():
    mark_active()

    all_games = load_games()
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
        sport = g.get("sport") or "Other"
        sections_by_sport.setdefault(sport, []).append(g)

    sections = [{"sport": s, "games": lst} for s, lst in sections_by_sport.items()]
    sections.sort(key=lambda s: s["sport"])

    most_viewed_games = get_most_viewed_games(all_games, limit=5)

    return render_template(
        "index.html",
        sections=sections,
        search_query=q,
        live_only=live_only,
        most_viewed_games=most_viewed_games,
    )


@app.route("/game/<int:game_id>")
def game_detail(game_id):
    mark_active()

    games = load_games()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    other_games = [
        g for g in games
        if g["id"] != game_id and g.get("streams") and len(g["streams"]) > 0
    ]

    open_ad = random.random() < 0.25

    return render_template(
        "game.html",
        game=game,
        other_games=other_games,
        open_ad=open_ad,
    )


# ---------------------- SCHEDULER ----------------------

def run_scraper_job():
    try:
        scrape_games.main()
    except Exception as e:
        print(f"[scheduler][ERROR] Scraper error: {e}")


def start_scheduler():
    run_scraper_job()

    scheduler = BackgroundScheduler()
    scheduler.add_job(
        run_scraper_job,
        "interval",
        minutes=5,
        id="scrape_job",
        replace_existing=True,
    )

    scheduler.start()
    print("[scheduler] Background scheduler started.")
    atexit.register(lambda: scheduler.shutdown(wait=False))


if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    start_scheduler()


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=False)
