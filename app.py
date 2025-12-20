from flask import Flask, render_template, abort, request, session, jsonify, redirect, url_for
import pandas as pd
import ast
import os
import random
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
import uuid
import hashlib  # for stable IDs
import re
from urllib.parse import urljoin

# For safe-ish CSV updates on Linux
import fcntl
import json

import scrape_games  # your scraper module

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

        game_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p.startswith("/game/") or p.startswith("/g/")}
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
    Stable ID based on fields so CSV reorder doesn't change it.
    """
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = hashlib.md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def slugify(text: str) -> str:
    """
    URL-safe slug: lowercase, remove punctuation, collapse dashes.
    """
    if not isinstance(text, str):
        return ""
    s = text.strip().lower()
    s = re.sub(r"['\"`]", "", s)
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s


def game_slug(game: dict) -> str:
    """
    Deterministic name-based slug for /g/<slug>.
    Includes date + matchup + sport for uniqueness.
    """
    # date_header might be like "Friday, December 19, 2025"
    date_part = slugify(str(game.get("date_header") or "today"))
    matchup_part = slugify(str(game.get("matchup") or "game"))
    sport_part = slugify(str(game.get("sport") or "sport"))
    base = f"{date_part}-{matchup_part}-{sport_part}"
    base = re.sub(r"-{2,}", "-", base).strip("-")

    # short stable suffix from game_id to prevent collisions
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
    """
    CSV stores streams as python-literal list of dicts.
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
    """
    Ensure unique slugs per game stream list.
    """
    if not slug:
        slug = "stream"
    base = slug
    i = 2
    while slug in seen:
        slug = f"{base}-{i}"
        i += 1
    seen.add(slug)
    return slug


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
                                "label": s.get("label") or "Stream",
                                "embed_url": s.get("embed_url"),
                                "watch_url": s.get("watch_url"),
                            })
            except Exception as e:
                print(f"[load_games][ERROR] Error parsing streams for row {idx}: {e}")
                streams = []

        # stable game id
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
            except Exception:
                time_display = None

        game_obj = {
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
        }

        # attach game slug
        game_obj["slug"] = game_slug(game_obj)

        # attach stream slugs (based on label)
        seen = set()
        for s in game_obj["streams"]:
            label = (s.get("label") or "Stream")
            s_slug = slugify(label)
            s["slug"] = _dedup_stream_slug(s_slug, seen)

        games.append(game_obj)

    return games


def get_game_view_counts(cutoff_seconds=45):
    now = datetime.utcnow()
    cutoff = now - timedelta(seconds=cutoff_seconds)
    counts = {}

    for (sid, path), ts in list(ACTIVE_PAGE_VIEWS.items()):
        if ts < cutoff:
            continue
        if not (path.startswith("/game/") or path.startswith("/g/")):
            continue

        # only count numeric /game/<id>
        if path.startswith("/game/"):
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


# ---------------------- WRITE API ----------------------

CSV_COLS = [
    "source", "date_header", "sport", "time_unix", "time",
    "tournament", "tournament_url", "matchup", "watch_url",
    "is_live", "streams", "embed_url"
]


def find_row_index_by_game_id(df, game_id: int):
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

    df, fh = read_csv_locked()
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

    df, fh = read_csv_locked()
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

    df, fh = read_csv_locked()
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


def _absolute_url(path: str) -> str:
    """
    Convert a relative path (/static/...) to absolute for OG tags.
    """
    return urljoin(request.url_root, path.lstrip("/"))


@app.route("/game/<int:game_id>")
def game_detail(game_id: int):
    mark_active()

    requested_stream_slug = request.args.get("stream", "").strip()

    games = load_games()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    # Other games for multiview
    other_games = [
        g for g in games
        if g["id"] != game_id and g.get("streams") and len(g["streams"]) > 0
    ]

    open_ad = random.random() < 0.25

    # share urls
    slug = game.get("slug") or game_slug(game)
    share_id_url = _absolute_url(url_for("game_detail", game_id=game_id))
    share_slug_url = _absolute_url(url_for("game_by_slug", slug=slug))

    # OG image absolute
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
    """
    Name-based permanent-ish link:
      /g/<slug>
    We resolve to the correct game_id then render the same page.
    """
    mark_active()

    slug = (slug or "").strip().lower()
    if not slug:
        abort(404)

    games = load_games()

    # exact match
    game = next((g for g in games if (g.get("slug") or "").lower() == slug), None)

    # fallback: try "starts with" (in case you changed suffix behavior)
    if not game:
        game = next((g for g in games if (g.get("slug") or "").lower().startswith(slug)), None)

    if not game:
        abort(404)

    # keep stream selection param if present
    qs = request.query_string.decode("utf-8", errors="ignore").strip()
    target = url_for("game_detail", game_id=game["id"])
    if qs:
        target = f"{target}?{qs}"

    # Redirect to canonical numeric route (good for caching + consistent view counting)
    return redirect(target, code=302)


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
        minutes=10,
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
