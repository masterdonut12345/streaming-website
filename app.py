from flask import Flask, render_template, abort, request, session
import pandas as pd
import ast
import os
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
from datetime import datetime, timedelta
import uuid

import scrape_games  # our scraper module

DATA_PATH = "today_games_with_all_streams.csv"

app = Flask(__name__)
app.secret_key = "replace_this_with_random"

# ---------------------- ACTIVE VIEWER TRACKER ----------------------

ACTIVE_VIEWERS = {}  # session_id → last_seen timestamp


def get_session_id():
    """Assign each visitor a unique ID if they don't already have one."""
    if "sid" not in session:
        session["sid"] = str(uuid.uuid4())
    return session["sid"]


def mark_active():
    """Mark this session as active and clean out inactive ones."""
    sid = get_session_id()
    now = datetime.utcnow()
    ACTIVE_VIEWERS[sid] = now

    # remove sessions inactive for 2 minutes
    cutoff = now - timedelta(minutes=2)
    for s, ts in list(ACTIVE_VIEWERS.items()):
        if ts < cutoff:
            del ACTIVE_VIEWERS[s]


def print_active_viewers():
    """Print active viewer count every minute."""
    count = len(ACTIVE_VIEWERS)
    print(f"[ACTIVE VIEWERS] Currently active users: {count}")


@app.before_request
def track_active_viewer():
    """Run before every request so we always update activity."""
    mark_active()


# ---------------------- UTILITIES ----------------------

def safe_lower(value):
    return value.lower() if isinstance(value, str) else ""


def parse_is_live(row, idx):
    """
    Robustly parse live flag from typical CSV forms:
    - 1, 1.0, True, 'true', 'True', 'live', etc.
    """
    if "is_live" not in row.index:
        return False

    raw = row.get("is_live")
    if pd.isna(raw):
        return False

    is_live = False

    # numeric forms like 1, 1.0
    if isinstance(raw, (int, float)):
        if raw == 1 or raw == 1.0:
            is_live = True

    # string forms
    raw_str = str(raw).strip().lower()
    if raw_str in ("1", "1.0", "true", "yes", "y", "live", "inplay", "in-play"):
        is_live = True

    # small debug for first few rows
    if idx < 5:
        print(f"[load_games] row {idx} is_live raw={raw!r} parsed={is_live}")

    return is_live


def load_games():
    if not os.path.exists(DATA_PATH):
        print(f"[load_games] CSV not found at {DATA_PATH}")
        return []

    try:
        df = pd.read_csv(DATA_PATH)
    except Exception as e:
        print(f"[load_games] Error reading CSV: {e}")
        return []

    if df.empty:
        print("[load_games] CSV loaded but has 0 rows.")
        return []

    games = []

    for idx, row in df.iterrows():
        # --- streams ---
        streams = []
        if "streams" in df.columns and pd.notna(row.get("streams")):
            try:
                parsed = ast.literal_eval(str(row["streams"]))
                if isinstance(parsed, list):
                    for s in parsed:
                        if isinstance(s, dict) and s.get("embed_url"):
                            streams.append(
                                {
                                    "label": s.get("label"),
                                    "embed_url": s.get("embed_url"),
                                }
                            )
            except Exception:
                streams = []

        # id
        if "id" in df.columns and not pd.isna(row.get("id")):
            game_id = int(row["id"])
        else:
            game_id = int(idx)

        # live flag
        is_live = parse_is_live(row, idx)

        game = {
            "id": game_id,
            "date_header": row.get("date_header"),
            "sport": row.get("sport"),
            "time_unix": row.get("time_unix"),
            "time": row.get("time"),
            "tournament": row.get("tournament"),
            "tournament_url": row.get("tournament_url"),
            "matchup": row.get("matchup"),
            "watch_url": row.get("watch_url"),
            "streams": streams,
            "is_live": is_live,
        }

        games.append(game)

    print(f"[load_games] Loaded {len(games)} games.")
    return games


# ---------------------- ROUTES ----------------------

@app.route("/")
def index():
    games = load_games()

    # search
    q = request.args.get("q", "").strip().lower()
    if q:
        games = [
            g
            for g in games
            if q in safe_lower(g.get("matchup"))
            or q in safe_lower(g.get("sport"))
            or q in safe_lower(g.get("tournament"))
        ]

    # live-only filter
    live_only = request.args.get("live_only", "").lower() in ("1", "true", "yes", "on")
    if live_only:
        games = [g for g in games if g.get("is_live")]

    # group by sport
    sections_by_sport = {}
    for g in games:
        sport = g.get("sport") or "Other"
        sections_by_sport.setdefault(sport, []).append(g)

    sections = [{"sport": s, "games": lst} for s, lst in sections_by_sport.items()]
    sections.sort(key=lambda s: s["sport"])

    return render_template(
        "index.html",
        sections=sections,
        search_query=q,
        live_only=live_only,
    )


@app.route("/game/<int:game_id>")
def game_detail(game_id):
    games = load_games()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    # Other games with streams (for multi-view)
    other_games = [
        g
        for g in games
        if g["id"] != game_id and g.get("streams") and len(g["streams"]) > 0
    ]

    return render_template("game.html", game=game, other_games=other_games)


# ---------------------- SCHEDULER ----------------------

def run_scraper_job():
    print("[scheduler] Running scraper...")
    try:
        scrape_games.main()
        print("[scheduler] Scraper finished.")
    except Exception as e:
        print(f"[scheduler] Scraper error: {e}")


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

    # print active viewer count every 60 seconds
    scheduler.add_job(
        print_active_viewers,
        "interval",
        seconds=60,
        id="active_viewer_job",
        replace_existing=True,
    )

    scheduler.start()
    print("[scheduler] Background scheduler started.")
    atexit.register(lambda: scheduler.shutdown())


if __name__ == "__main__":
    start_scheduler()
    app.run(host="127.0.0.1", port=5000, debug=True)
