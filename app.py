from flask import Flask, render_template, abort, request, session, jsonify
import pandas as pd
import ast
import os
import random
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
import uuid

import scrape_games  # our scraper module

DATA_PATH = "today_games_with_all_streams.csv"

app = Flask(__name__)
app.secret_key = "replace_this_with_random"

# ---------------------- ACTIVE VIEWER TRACKER ----------------------

# Tracks "people" (sessions) active anywhere on the site
ACTIVE_VIEWERS = {}        # session_id → last_seen timestamp
# Tracks activity per-page (path)
ACTIVE_PAGE_VIEWS = {}     # (session_id, path) → last_seen timestamp
LAST_VIEWER_PRINT = None   # throttle printing


def get_session_id():
    """Assign each visitor a unique ID if they don't already have one."""
    if "sid" not in session:
        # stable random ID per browser session
        session["sid"] = str(uuid.UUID(bytes=os.urandom(16)))
    return session["sid"]


def mark_active():
    """
    Mark this session as active (site-wide) and clean out inactive ones.
    This is called on page load (index/game). Heartbeat does more precise
    tracking and printing.
    """
    sid = get_session_id()
    now = datetime.utcnow()
    ACTIVE_VIEWERS[sid] = now

    cutoff = now - timedelta(minutes=2)
    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]


@app.route("/heartbeat", methods=["POST"])
def heartbeat():
    """
    Lightweight endpoint the browser calls periodically while a page is open.
    Used to track active sessions and active viewers per page path.
    """
    global LAST_VIEWER_PRINT

    sid = get_session_id()
    now = datetime.utcnow()

    data = request.get_json(silent=True) or {}
    path = data.get("path") or request.path  # usually window.location.pathname

    # Update site-wide activity
    ACTIVE_VIEWERS[sid] = now
    # Update per-page activity
    ACTIVE_PAGE_VIEWS[(sid, path)] = now

    # Expire entries older than ~45 seconds
    cutoff = now - timedelta(seconds=45)

    for key, ts in list(ACTIVE_PAGE_VIEWS.items()):
        if ts < cutoff:
            del ACTIVE_PAGE_VIEWS[key]

    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]

    # Log counts at most once per 60 seconds
    if LAST_VIEWER_PRINT is None or (now - LAST_VIEWER_PRINT) > timedelta(seconds=60):
        total_active = len(ACTIVE_VIEWERS)

        # Example: how many are on home page "/"
        home_sids = {sid for (sid, p) in ACTIVE_PAGE_VIEWS.keys() if p == "/"}
        home_count = len(home_sids)

        # Example: how many are on any game page (URL starting with "/game/")
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
        # Not strictly an error, just nothing to show
        return []

    games = []

    # mapping: normalize sport names
    sport_map = {
        "Football" : "Soccer",
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

        game_id = int(row["id"]) if "id" in df.columns and not pd.isna(row.get("id")) else int(idx)

        # normalize sport
        raw_sport = row.get("sport")
        if isinstance(raw_sport, str):
            raw_sport = raw_sport.strip()
        sport = sport_map.get(raw_sport, raw_sport)

        is_live = False
        if "is_live" in df.columns:
            raw = str(row.get("is_live")).lower().strip()
            is_live = raw in ("1", "true", "yes", "y", "live")

        
        # --- Format time as "h:mm AM/PM ET" ---
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
        "time": time_display,   # <<< use the formatted time
        "tournament": row.get("tournament"),
        "tournament_url": row.get("tournament_url"),
        "matchup": row.get("matchup"),
        "watch_url": row.get("watch_url"),
        "streams": streams,
        "is_live": is_live,
        })
    return games


# ---------------------- ROUTES ----------------------

@app.route("/")
def index():
    mark_active()
    games = load_games()

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

    return render_template("index.html", sections=sections, search_query=q, live_only=live_only)


@app.route("/game/<int:game_id>")
def game_detail(game_id):
    mark_active()

    games = load_games()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    # Other games with streams (for multi-view)
    other_games = [
        g for g in games
        if g["id"] != game_id and g.get("streams") and len(g["streams"]) > 0
    ]
    return render_template("game.html", game=game, other_games=other_games)


# ---------------------- SCHEDULER ----------------------

def run_scraper_job():
    try:
        scrape_games.main()
    except Exception as e:
        print(f"[scheduler][ERROR] Scraper error: {e}")


def start_scheduler():
    # run scraper immediately on boot
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
