from flask import Flask, render_template, abort, redirect
import pandas as pd
import ast
import os
import random

from apscheduler.schedulers.background import BackgroundScheduler
import atexit

import scrape_games  # our scraper module

# Path to your CSV with games + streams
DATA_PATH = "today_games_with_all_streams.csv"

app = Flask(__name__)


def load_games():
    """
    Load games from the CSV and return a list of dicts.
    Each game dict will have:
      - id
      - date_header
      - sport
      - time_unix
      - time
      - tournament
      - tournament_url
      - matchup
      - watch_url
      - streams: list of {"label": ..., "embed_url": ...}
    """
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
        # --- parse streams column safely ---
        streams = []
        if "streams" in df.columns and pd.notna(row.get("streams")):
            try:
                parsed = ast.literal_eval(str(row["streams"]))
                if isinstance(parsed, list):
                    for s in parsed:
                        if not isinstance(s, dict):
                            continue
                        label = s.get("label")
                        embed_url = s.get("embed_url")
                        if not embed_url:
                            continue
                        streams.append(
                            {
                                "label": label,
                                "embed_url": embed_url,
                            }
                        )
                else:
                    print(f"[load_games] streams is not a list for row {idx}: {parsed}")
            except Exception as e:
                print(f"[load_games] Error parsing streams for row {idx}: {e}")
                streams = []

        # pick an id: use 'id' column if it exists, otherwise use the row index
        if "id" in df.columns and not pd.isna(row.get("id")):
            game_id = int(row["id"])
        else:
            game_id = int(idx)

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
        }

        games.append(game)

    print(f"[load_games] Loaded {len(games)} games from CSV.")
    return games


@app.route("/")
def index():
    games = load_games()

    sections_by_sport = {}
    for game in games:
        sport = game.get("sport") or "Other"
        sections_by_sport.setdefault(sport, []).append(game)

    sections = [
        {"sport": sport, "games": game_list}
        for sport, game_list in sections_by_sport.items()
    ]

    sections.sort(key=lambda s: s["sport"])

    return render_template("index.html", sections=sections)


@app.route("/game/<int:game_id>")
def game_detail(game_id):
    games = load_games()
    game = next((g for g in games if g["id"] == game_id), None)
    if not game:
        abort(404)

    print(f"[game_detail] Game id={game_id}")
    print(f"[game_detail]  matchup={game.get('matchup')}")
    print(f"[game_detail]  streams={game.get('streams')}")

    if random.random() < 0.1:
        return redirect("https://www.effectivegatecpm.com/d01t94kua?key=491dbdc350af1bf2b2f5c05ef1a574df")
    return render_template("game.html", game=game)


# -------------- SCHEDULER SETUP --------------

def run_scraper_job():
    """Background job that runs the scraper and updates the CSV."""
    print("[scheduler] Starting scraper job...")
    try:
        scrape_games.main()
        print("[scheduler] Scraper job completed.")
    except Exception as e:
        print(f"[scheduler] Scraper job failed: {e}")


def start_scheduler():
    # run once immediately at startup
    run_scraper_job()

    scheduler = BackgroundScheduler()
    # run every 5 minutes (adjust if you want)
    scheduler.add_job(
        run_scraper_job,
        "interval",
        minutes=5,
        id="scrape_job",
        replace_existing=True,
    )
    scheduler.start()
    print("[scheduler] Started background scheduler.")

    atexit.register(lambda: scheduler.shutdown())


# Start scheduler on import (avoid double-start in debug)
if not app.debug or os.environ.get("WERKZEUG_RUN_MAIN") == "true":
    start_scheduler()


if __name__ == "__main__":
    # Local dev
    app.run(host="127.0.0.1", port=5000, debug=True)
