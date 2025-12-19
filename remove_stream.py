#!/usr/bin/env python3
"""
remove_game.py

Usage:
  export ADMIN_API_KEY="your-key"   # optional; you can also hardcode below (not recommended)
  python3 remove_game.py

Only edit the REQUEST dict below.
"""

import json
import os
import sys
import urllib.request
import urllib.error

API_URL = "https://thestreamden.onrender.com/api/games/remove"

# ✅ Edit ONLY this dictionary
REQUEST = {
    # One of the following:
    "game_id": 123456789,

    # Optional: if your server also supports it (it doesn't in your current code):
    # "watch_url": "https://example.com/watch/page",

    # Optional: dry run flag (server must support; safe to leave False)
    # "dry_run": False,
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
    # Prefer env var, but allow hardcode if needed.
    api_key = os.environ.get("ADMIN_API_KEY", "").strip() or None

    status, body = post_json(API_URL, REQUEST, api_key)

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
