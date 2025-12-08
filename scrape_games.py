import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import re
from urllib.parse import urljoin
from typing import Optional, List, Dict, Any


BASE_URL = "https://sport71.pro"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EventScraper/1.0)"
}


def scrape_today_games() -> pd.DataFrame:
    """
    Scrape today's games from sport71.pro and return a DataFrame
    with basic info, including watch_url.
    """
    print("[scrape_today_games] Fetching:", BASE_URL)
    try:
        req = requests.get(BASE_URL, headers=HEADERS, timeout=15)
    except Exception as e:
        print("[scrape_today_games] Error requesting page:", e)
        return pd.DataFrame()

    html = req.text

    if "Error code 521" in html or req.status_code != 200:
        print("[scrape_today_games] Site returned an error page — cannot scrape upcoming events.")
        print("[scrape_today_games] HTTP status:", req.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", id="upcoming-events")
    if not section:
        print("[scrape_today_games] Could not find <section id='upcoming-events'> — page may be JS-rendered.")
        return pd.DataFrame()

    today = datetime.now()
    today_str = f"{today.strftime('%A')}, {today.strftime('%B')} {today.day}, {today.year}"
    print("[scrape_today_games] Looking for games on:", today_str)

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
    print(f"[scrape_today_games] Found {len(df)} games for today.")
    return df


# -------- 2) HELPERS TO EXTRACT ALL STREAM EMBEDS --------

def extract_embed_url_from_soup(soup: BeautifulSoup) -> Optional[str]:
    """
    Given a BeautifulSoup object of a watch page,
    extract the iframe src from <pre id="embed-code">.
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
    For a base watch URL like:
      https://vipsrst.shop/sport7/watch/atlanta-falcons-seattle-seahawks
    1) Load it
    2) Find all <a class="stream-button"> links
    3) For each stream link, load it and extract its embed iframe src
    Returns a list of dicts: [{label, embed_url, watch_url}, ...]
    """
    if not watch_url:
        return []

    print(f"[get_all_streams] Fetching watch page: {watch_url}")
    try:
        r = requests.get(watch_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print("[get_all_streams] Non-200 status on base watch URL:", r.status_code)
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

            # stream-specific watch URL (may be relative)
            stream_watch_url = urljoin(watch_url, href)

            # nice label, e.g. "Sport7 (25)"
            label = " ".join(a.stripped_strings) or f"Stream {i}"

            try:
                print(f"[get_all_streams]   Fetching stream option: {stream_watch_url}")
                r2 = requests.get(stream_watch_url, headers=HEADERS, timeout=15)
                if r2.status_code != 200:
                    print(
                        "[get_all_streams]   Non-200 status on stream URL:",
                        stream_watch_url,
                        r2.status_code,
                    )
                    continue

                soup2 = BeautifulSoup(r2.text, "html.parser")
                embed_url = extract_embed_url_from_soup(soup2)
                if not embed_url:
                    print("[get_all_streams]   No embed URL found on stream page.")
                    continue

                streams.append(
                    {
                        "label": label,
                        "embed_url": embed_url,
                        "watch_url": stream_watch_url,
                    }
                )
            except Exception as e:
                print("[get_all_streams]   Error fetching stream option:", e)
                continue

        return streams

    except Exception as e:
        print("[get_all_streams] Error fetching base watch URL:", e)
        return []


def first_embed_or_none(stream_list: List[Dict[str, Any]]) -> Optional[str]:
    if not stream_list:
        return None
    return stream_list[0].get("embed_url")


# -------- 3) MAIN ENTRYPOINT --------

def main() -> None:
    print("[main] Starting scrape...")
    df = scrape_today_games()

    if df.empty:
        print("[main] No rows found for today; nothing to do.")
        return

    print("[main] Fetching streams for each game...")
    df["streams"] = df["watch_url"].apply(get_all_streams_from_watch_page)
    df["embed_url"] = df["streams"].apply(first_embed_or_none)

    print("[main] Sample rows:")
    print(df.head())

    output_file = "today_games_with_all_streams.csv"
    df.to_csv(output_file, index=False)
    print(f"[main] Wrote {len(df)} rows to {output_file}")


if __name__ == "__main__":
    main()
