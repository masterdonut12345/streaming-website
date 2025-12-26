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
import sys

import pandas as pd  # type: ignore

import scrape_games


BASE_SITE_URL = os.environ.get("STREAMDEN_BASE_URL", "https://thestreamden.com")
CSV_FALLBACK_PATH = os.environ.get("STREAMDEN_CSV_PATH", "today_games_with_all_streams.csv")

INVALID_SPORT_MARKERS = {"other", "unknown", "nan", "n/a", "none", "null", ""}
SPORT_KEYWORD_MAP = [
    ("nba", "Basketball"),
    ("basketball", "Basketball"),
    ("wnba", "Basketball"),
    ("ncaa basketball", "Basketball"),
    ("college basketball", "Basketball"),
    ("nba g-league", "Basketball"),
    ("nfl", "American Football"),
    ("american football", "American Football"),
    ("ncaa football", "College Football"),
    ("college football", "College Football"),
    ("mlb", "MLB"),
    ("baseball", "MLB"),
    ("nhl", "Ice Hockey"),
    ("hockey", "Ice Hockey"),
    ("ice hockey", "Ice Hockey"),
    ("pwhl", "Ice Hockey"),
    ("soccer", "Soccer"),
    ("football", "Soccer"),
    ("mls", "Soccer"),
    ("premier league", "Soccer"),
    ("la liga", "Soccer"),
    ("bundesliga", "Soccer"),
    ("serie a", "Soccer"),
    ("ligue 1", "Soccer"),
    ("champions league", "Soccer"),
    ("uefa", "Soccer"),
    ("ucl", "Soccer"),
    ("africa cup of nations", "Soccer"),
    ("copa", "Soccer"),
    ("eredivisie", "Soccer"),
    ("laliga", "Soccer"),
    ("ligue 2", "Soccer"),
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
    ("ashes", "Cricket"),
    ("t20", "Cricket"),
    ("bbl", "Cricket"),
    ("big bash", "Cricket"),
    ("international league t20", "Cricket"),
    ("ilt20", "Cricket"),
    ("test series", "Cricket"),
    ("one day", "Cricket"),
    ("odi", "Cricket"),
    ("rugby", "Rugby"),
    ("rugby union", "Rugby"),
    ("top 14", "Rugby"),
    ("premiership", "Rugby"),
    ("handball", "Handball"),
    ("volleyball", "Volleyball"),
    ("darts", "Darts"),
    ("equestrian", "Equestrian"),
    ("curling", "Curling"),
    ("horse racing", "Horse Racing"),
]


def normalize_sport(value) -> str:
    if isinstance(value, str):
        v = value.strip()
        return v or "Other"
    try:
        text = str(value).strip()
        return text or "Other"
    except Exception:
        return "Other"


def infer_sport(row: Dict[str, Any]) -> str:
    """Best-effort inference similar to app.py."""
    sport = normalize_sport(row.get("sport"))
    normalized = sport.lower()
    needs_infer = normalized in INVALID_SPORT_MARKERS
    if not needs_infer:
        return sport

    parts = [
        row.get("sport", ""),
        row.get("tournament", ""),
        row.get("matchup", ""),
        row.get("watch_url", ""),
        row.get("source", ""),
    ]
    haystack = " ".join([str(p or "") for p in parts]).lower()
    for keyword, mapped in SPORT_KEYWORD_MAP:
        if keyword in haystack:
            return mapped

    # Try URL path fragments
    if "/" in haystack:
        segments = [p for p in haystack.replace("-", " ").split("/") if p]
        for keyword, mapped in SPORT_KEYWORD_MAP:
            if any(keyword in seg for seg in segments):
                return mapped

    return "Unclassified"


def sport_is_invalid(value) -> bool:
    return normalize_sport(value).lower() in INVALID_SPORT_MARKERS


def make_stable_id(row: Dict[str, Any]) -> int:
    """
    Matches the app's stable ID logic so links align with /game/<id>.
    """
    key = f"{row.get('date_header', '')}|{row.get('sport', '')}|{row.get('tournament', '')}|{row.get('matchup', '')}"
    digest = md5(key.encode("utf-8")).hexdigest()
    return int(digest[:8], 16)


def collect_games_from_scrapers() -> List[Dict[str, Any]]:
    dfs = [
        scrape_games.scrape_streamed_api(),
    ]

    games: Dict[int, Dict[str, Any]] = {}

    for df in dfs:
        if df is None or df.empty:
            continue
        for _, row in df.iterrows():
            data = row.to_dict()
            sport = infer_sport(data)
            gid = make_stable_id(data)
            games[gid] = {
                "id": gid,
                "sport": sport,
                "matchup": data.get("matchup"),
                "tournament": data.get("tournament"),
                "time": data.get("time"),
                "time_unix": data.get("time_unix"),
            }
    return list(games.values())


def collect_games_from_csv() -> List[Dict[str, Any]]:
    if not os.path.exists(CSV_FALLBACK_PATH):
        return []
    try:
        df = pd.read_csv(CSV_FALLBACK_PATH)
    except Exception:
        return []

    games: Dict[int, Dict[str, Any]] = {}
    for _, row in df.iterrows():
        data = row.to_dict()
        sport = infer_sport(data)
        gid = make_stable_id(data)
        games[gid] = {
            "id": gid,
            "sport": sport,
            "matchup": data.get("matchup"),
            "tournament": data.get("tournament"),
            "time": data.get("time"),
            "time_unix": data.get("time_unix"),
        }
    return list(games.values())


def collect_games() -> List[Dict[str, Any]]:
    games = collect_games_from_scrapers()
    if games:
        return games
    # Fallback: use local CSV if scraping finds nothing (offline or sites down)
    fallback_games = collect_games_from_csv()
    if fallback_games:
        print("[info] Scrapers returned no games; using CSV fallback.", file=sys.stderr)
    return fallback_games


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

    def sort_key(g: Dict[str, Any]):
        # Prefer numeric time_unix, then string time, then matchup
        tu = g.get("time_unix")
        try:
            tu_val = float(tu) if tu is not None and tu != "" else None
        except Exception:
            tu_val = None
        return (
            tu_val if tu_val is not None else float("inf"),
            str(g.get("time") or ""),
            str(g.get("matchup") or ""),
        )

    # Sort sports alphabetically, and games inside each sport by date/time
    for sport in sorted(by_sport.keys()):
        print(f"=== {sport} ===")
        for game in sorted(by_sport[sport], key=sort_key):
            print(format_game_line(BASE_SITE_URL, game))
        print()


if __name__ == "__main__":
    main()
