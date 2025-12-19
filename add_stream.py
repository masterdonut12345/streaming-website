#!/usr/bin/env python3
"""
upsert_game.py

Usage:
  export ADMIN_API_KEY="your-key"     # optional but recommended
  python3 upsert_game.py

Only edit the GAME dict below.
"""

import json
import os
import sys
import urllib.request
import urllib.error


API_URL = "https://thestreamden.onrender.com/api/games/upsert"

# ✅ Edit ONLY this dictionary
GAME = {
    "source": "manual",
    "date_header": "Friday, December 19, 2025",
    "sport": "Boxing",
    "time_unix": 1766113200000,
    "time": "2025-12-18 22:00:00-05:00",
    "tournament": "PPV",
    "tournament_url": "https://thestreamden.com",
    "matchup": "Jake Paul vs Anthony Joshua",
    "watch_url": "https://app.buffstream.io/boxing-streams/jake-paul-live-stream",
    "is_live": False,
    "streams": [
        {
            "label": "Main Stream",
            "embed_url": "https://embedsports.me/boxing/jake-paul-vs-anthony-joshua-stream-1",
            "watch_url": "https://app.buffstream.io/boxing-streams/jake-paul-live-stream",
        }
    ],
    # "embed_url": "https://example.com/embed/main",  # optional (server will fill from streams[0])
}


def post_json(url: str, payload: dict, api_key: str | None):
    data = json.dumps(payload).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if api_key:
        headers["X-API-Key"] = api_key

    req = urllib.request.Request(url, data=data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8", errors="replace")
        return e.code, body
    except Exception as e:
        return None, str(e)


def main():
    api_key = "023489h0weihg092ghsdojklgh2pq904eighwaoedjghvbnwoEPUG"

    payload = {"game": GAME}

    status, body = post_json(API_URL, payload, api_key)

    if status is None:
        print(f"[ERROR] Request failed: {body}", file=sys.stderr)
        sys.exit(2)

    print(f"[HTTP {status}]")
    try:
        print(json.dumps(json.loads(body), indent=2))
    except Exception:
        print(body)

    if status >= 400:
        sys.exit(1)


if __name__ == "__main__":
    main()
