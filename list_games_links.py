#!/usr/bin/env python3
"""
Utility script (local use only):
  - Runs the existing scrapers
  - Groups games by sport
  - Prints TheStreamDen links for each game

This does NOT modify the CSV or run the webserver. It’s just a helper to grab
current games + links for sharing (e.g., into Discord).
"""

from hashlib import md5
from typing import Dict, List, Any
import os

import pandas as pd  # type: ignore

import scrape_games


BASE_SITE_URL = os.environ.get("STREAMDEN_BASE_URL", "https://thestreamden.onrender.com")


INVALID_SPORT_MARKERS = {"other", "unknown", "nan", "n/a", "none", "null", ""}


def normalize_sport(value) -> str:
    if isinstance(value, str):
        v = value.strip()
        return v or "Other"
    try:
        text = str(value).strip()
        return text or "Other"
    except Exception:
        return "Other"


def sport_is_invalid(value) -> bool:
    return normalize_sport(value).lower() in INVALID_SPORT_MARKERS


def make_stable_id(row: Dict[str, Any]) -> int:
    """
    Matches the app's stable ID logic so links align with /game/<id>.
    """
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def collect_games() -> List[Dict[str, Any]]:
    dfs = [
        scrape_games.scrape_sport71(),
        scrape_games.scrape_shark(),
    ]

    games: Dict[int, Dict[str, Any]] = {}

    for df in dfs:
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            data = row.to_dict()
            if sport_is_invalid(data.get("sport")):
                continue
            gid = make_stable_id(data)
            games[gid] = {
                "id": gid,
                "sport": normalize_sport(data.get("sport")),
                "matchup": data.get("matchup"),
                "tournament": data.get("tournament"),
                "time": data.get("time"),
            }
    return list(games.values())


def format_game_line(base_url: str, game: Dict[str, Any]) -> str:
    link = f"{base_url}/game/{game['id']}"
    matchup = game.get("matchup") or "Game"
    tournament = game.get("tournament")
    time_display = game.get("time")

    extra = []
    if tournament:
        extra.append(str(tournament))
    if isinstance(time_display, pd.Timestamp):
        extra.append(time_display.strftime("%I:%M %p ET").lstrip("0"))
    elif time_display:
        extra.append(str(time_display))

    suffix = f" — {' • '.join(extra)}" if extra else ""
    return f"- {matchup}{suffix}\n  {link}"


def main():
    games = collect_games()
    if not games:
        print("No games found.")
        return

    by_sport: Dict[str, List[Dict[str, Any]]] = {}
    for g in games:
        by_sport.setdefault(g["sport"], []).append(g)

    for sport in sorted(by_sport.keys()):
        print(f"=== {sport} ===")
        for game in sorted(by_sport[sport], key=lambda g: (g.get("time") or "", g.get("matchup") or "")):
            print(format_game_line(BASE_SITE_URL, game))
        print()


if __name__ == "__main__":
    main()
