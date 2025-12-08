import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re
from urllib.parse import urljoin
from typing import Optional, List, Dict, Any


BASE_URL_SPORT71 = "https://sport71.pro"
BASE_URL_SHARK = "https://sharkstreams.net/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EventScraper/1.0)"
}


# ========= 1) SPORT71: TODAY'S GAMES =========

def scrape_today_games_sport71() -> pd.DataFrame:
    """
    Scrape today's games from sport71.pro and return a DataFrame
    with basic info, including watch_url.
    """
    print("[sport71] Fetching:", BASE_URL_SPORT71)
    try:
        req = requests.get(BASE_URL_SPORT71, headers=HEADERS, timeout=15)
    except Exception as e:
        print("[sport71] Error requesting page:", e)
        return pd.DataFrame()

    html = req.text

    if "Error code 521" in html or req.status_code != 200:
        print("[sport71] Site returned an error page — cannot scrape upcoming events.")
        print("[sport71] HTTP status:", req.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", id="upcoming-events")
    if not section:
        print("[sport71] Could not find <section id='upcoming-events'> — page may be JS-rendered.")
        return pd.DataFrame()

    today = datetime.now()
    today_str = f"{today.strftime('%A')}, {today.strftime('%B')} {today.day}, {today.year}"
    print("[sport71] Looking for games on:", today_str)

    rows = []

    # day_block = container for a day's events
    for day_block in section.find_all("div", class_="mb-8", recursive=False):
        date_tag = day_block.find("h3")
        date_text = date_tag.get_text(strip=True) if date_tag else None

        # Only today's block
        if date_text != today_str:
            continue

        # Each card is a sport group
        for card in day_block.select("div.space-y-6 > div.bg-white"):
            header = card.find("div", class_="flex")
            sport_name_tag = header.find("span", class_="font-semibold") if header else None
            sport_name = sport_name_tag.get_text(strip=True) if sport_name_tag else "Unknown sport"

            tbody = card.find("tbody")
            if not tbody:
                continue

            for tr in tbody.find_all("tr"):
                # Skip SEO filter row
                if "seo-sport-filter" in tr.get("class", []):
                    continue

                tds = tr.find_all("td")
                if len(tds) != 3:
                    continue

                # time
                time_span = tds[0].find("span", class_="event-time")
                unix_time = time_span.get("data-unix-time") if time_span else None
                event_dt = None
                if unix_time:
                    try:
                        event_dt = datetime.fromtimestamp(int(unix_time) / 1000)
                    except Exception:
                        event_dt = None

                # tournament
                tournament_link = tds[1].find("a", class_="tournament-link")
                tournament_name = tournament_link.get_text(strip=True) if tournament_link else None
                tournament_url = (
                    tournament_link["href"]
                    if tournament_link and tournament_link.has_attr("href")
                    else None
                )

                # matchup
                spans = tds[1].find_all("span")
                matchup = spans[-1].get_text(strip=True) if spans else None

                # watch page url
                watch_link = tds[2].find("a", class_="watch-button")
                watch_url = (
                    watch_link["href"]
                    if watch_link and watch_link.has_attr("href")
                    else None
                )

                rows.append(
                    {
                        "source": "sport71",
                        "date_header": date_text,
                        "sport": sport_name,
                        "time_unix": unix_time,
                        "time": event_dt,
                        "tournament": tournament_name,
                        "tournament_url": tournament_url,
                        "matchup": matchup,
                        "watch_url": watch_url,
                    }
                )

    df = pd.DataFrame(rows)
    print(f"[sport71] Found {len(df)} games for today.")
    return df


# ========= 2) SHARKSTREAMS: TODAY'S GAMES =========

def scrape_today_games_shark() -> pd.DataFrame:
    """
    Scrape games from sharkstreams.net homepage and return
    a DataFrame with the same columns as sport71, including a
    'streams' list and 'embed_url' prefilled.
    """
    print("[shark] Fetching:", BASE_URL_SHARK)
    try:
        req = requests.get(BASE_URL_SHARK, headers=HEADERS, timeout=15)
    except Exception as e:
        print("[shark] Error requesting page:", e)
        return pd.DataFrame()

    if req.status_code != 200:
        print("[shark] Non-200 HTTP status:", req.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(req.text, "html.parser")

    today = datetime.now().date()
    rows: List[Dict[str, Any]] = []

    # Each channel row looks like <div class="row"> ... with spans ch-date, ch-category, ch-name
    for row_div in soup.find_all("div", class_="row"):
        # date/time
        date_span = row_div.find("span", class_="ch-date")
        if not date_span:
            continue
        date_str = date_span.get_text(strip=True)
        try:
            # Example: "2025-12-07 13:00:00"
            event_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            print(f"[shark] Failed to parse date: {date_str}")
            continue

        # keep today and future games, skip only past dates
        if event_dt.date() < today:
            continue

        # category (sport)
        cat_span = row_div.find("span", class_="ch-category")
        sport_name = cat_span.get_text(strip=True) if cat_span else "Unknown"

        # matchup / channel name

        # matchup / channel name
        name_span = row_div.find("span", class_="ch-name")
        raw_matchup = name_span.get_text(strip=True) if name_span else "Unknown"

        # mark SharkStreams entries as alternative links
        matchup = f"{raw_matchup} (alternative)"
        # "Embed" button – URL is inside onclick="openEmbed('https://sharkstreams.net/player.php?channel=8')"
        embed_link = None
        for a in row_div.find_all("a", class_="hd-link"):
            if "Embed" in a.get_text(strip=True):
                onclick = a.get("onclick", "")
                m = re.search(r"openEmbed\('([^']+)'", onclick)
                if m:
                    embed_link = m.group(1)
                break

        if not embed_link:
            # fallback: try to derive from watch button (player.php?channel=...)
            for a in row_div.find_all("a", class_="hd-link"):
                if "Watch" in a.get_text(strip=True):
                    onclick = a.get("onclick", "")
                    m = re.search(r"window\.open\(\s*'([^']+)'", onclick)
                    if m:
                        rel = m.group(1)
                        embed_link = urljoin(BASE_URL_SHARK, rel)
                    break

        if not embed_link:
            print(f"[shark] No embed link found for {matchup}")
            continue

        time_unix = int(event_dt.timestamp() * 1000)

        streams = [
            {
                "label": "SharkStreams",
                "embed_url": embed_link,
            }
        ]

        rows.append(
            {
                "source": "sharkstreams",
                "date_header": event_dt.strftime("%A, %B %d, %Y"),
                "sport": sport_name,
                "time_unix": time_unix,
                "time": event_dt,
                "tournament": None,
                "tournament_url": None,
                "matchup": matchup,
                "watch_url": embed_link,
                "streams": streams,
                "embed_url": embed_link,
            }
        )

    df = pd.DataFrame(rows)
    print(f"[shark] Found {len(df)} games (today and future).")
    return df
# ========= 3) OLD SPORT71 STREAM HELPERS =========

def extract_embed_url_from_soup(soup: BeautifulSoup) -> Optional[str]:
    """
    Given a BeautifulSoup object of a watch page,
    extract the iframe src from <pre id="embed-code">.
    (Used only for sport71-style watch pages.)
    """
    pre = soup.find("pre", id="embed-code")
    if not pre:
        return None

    embed_code_text = pre.get_text(" ", strip=True)
    m = re.search(r'src="([^"]+)"', embed_code_text)
    if not m:
        return None

    embed_url = m.group(1)
    return embed_url


def get_all_streams_from_watch_page(watch_url: Optional[str]) -> List[Dict[str, Any]]:
    """
    For a sport71-style base watch URL, load it, find stream buttons,
    and extract the embed iframe src for each.
    Returns a list of dicts: [{label, embed_url, watch_url}, ...]
    """
    if not watch_url:
        return []

    # If this is a sharkstreams URL, we *already* handled streams
    if "sharkstreams.net" in watch_url:
        return []

    print(f"[sport71] [get_all_streams] Fetching watch page: {watch_url}")
    try:
        r = requests.get(watch_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print("[sport71] [get_all_streams] Non-200 status on base watch URL:", r.status_code)
            return []

        soup = BeautifulSoup(r.text, "html.parser")

        stream_links = soup.select("div.stream-picker a.stream-button")
        streams: List[Dict[str, Any]] = []

        # If there are no explicit stream links, just try this single page
        if not stream_links:
            embed_url = extract_embed_url_from_soup(soup)
            if embed_url:
                streams.append(
                    {
                        "label": "Stream 1",
                        "embed_url": embed_url,
                        "watch_url": watch_url,
                    }
                )
            return streams

        for i, a in enumerate(stream_links, start=1):
            href = a.get("href")
            if not href:
                continue

            stream_watch_url = urljoin(watch_url, href)
            label = " ".join(a.stripped_strings) or f"Stream {i}"

            try:
                print(f"[sport71] [get_all_streams]   Fetching stream option: {stream_watch_url}")
                r2 = requests.get(stream_watch_url, headers=HEADERS, timeout=15)
                if r2.status_code != 200:
                    print(
                        "[sport71] [get_all_streams]   Non-200 status on stream URL:",
                        stream_watch_url,
                        r2.status_code,
                    )
                    continue

                soup2 = BeautifulSoup(r2.text, "html.parser")
                embed_url = extract_embed_url_from_soup(soup2)
                if not embed_url:
                    print("[sport71] [get_all_streams]   No embed URL found on stream page.")
                    continue

                streams.append(
                    {
                        "label": label,
                        "embed_url": embed_url,
                        "watch_url": stream_watch_url,
                    }
                )
            except Exception as e:
                print("[sport71] [get_all_streams]   Error fetching stream option:", e)
                continue

        return streams

    except Exception as e:
        print("[sport71] [get_all_streams] Error fetching base watch URL:", e)
        return []


def first_embed_or_none(stream_list: List[Dict[str, Any]]) -> Optional[str]:
    if not stream_list:
        return None
    return stream_list[0].get("embed_url")


# ========= 4) MAIN: COMBINE BOTH SOURCES & WRITE CSV =========

def main() -> None:
    print("[main] Starting scrape...")

    # 1) sport71 games
    df_sport71 = scrape_today_games_sport71()

    # if we got any sport71 games, fetch their streams
    if not df_sport71.empty:
        print("[main] Fetching streams for sport71 games...")
        df_sport71["streams"] = df_sport71["watch_url"].apply(get_all_streams_from_watch_page)
        df_sport71["embed_url"] = df_sport71["streams"].apply(first_embed_or_none)
    else:
        df_sport71["streams"] = []
        df_sport71["embed_url"] = None

    # 2) sharkstreams games (already come with streams + embed_url)
    df_shark = scrape_today_games_shark()

    # 3) combine
    if not df_sport71.empty and not df_shark.empty:
        df = pd.concat([df_sport71, df_shark], ignore_index=True)
    elif not df_sport71.empty:
        df = df_sport71
    else:
        df = df_shark

    if df.empty:
        print("[main] No rows found for today from any source; nothing to do.")
        return

    print("[main] Combined rows:", len(df))
    print("[main] Sample rows:")
    print(df.head())

    output_file = "today_games_with_all_streams.csv"
    df.to_csv(output_file, index=False)
    print(f"[main] Wrote {len(df)} rows to {output_file}")


if __name__ == "__main__":
    main()
