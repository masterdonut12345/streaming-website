import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import pandas as pd
import pytz
import re
from urllib.parse import urljoin
from typing import Optional, List, Dict, Any

BASE_URL_SPORT71 = "https://sport71.pro"
BASE_URL_SHARK = "https://sharkstreams.net/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EventScraper/1.0)"
}

# Define time zones
EST = pytz.timezone("US/Eastern")
UTC = pytz.UTC


# ========= 1) SPORT71: TODAY'S GAMES =========
def scrape_today_games_sport71() -> pd.DataFrame:
    """
    Scrape today's games from sport71.pro and return a DataFrame
    with basic info, including watch_url.
    All date logic is done relative to US/Eastern.
    """
    try:
        req = requests.get(BASE_URL_SPORT71, headers=HEADERS, timeout=15)
    except Exception as e:
        print("[sport71][ERROR] Requesting page failed:", e)
        return pd.DataFrame()

    html = req.text

    if "Error code 521" in html or req.status_code != 200:
        print("[sport71][ERROR] Site returned an error page or non-200 status.")
        print("[sport71][ERROR] HTTP status:", req.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(html, "html.parser")
    section = soup.find("section", id="upcoming-events")
    if not section:
        print("[sport71][ERROR] Could not find <section id='upcoming-events'> — page may be JS-rendered.")
        return pd.DataFrame()

    # Use "today" in EST so server timezone (UTC) doesn't mess us up
    today_est = datetime.now(EST)
    today_str = f"{today_est.strftime('%A')}, {today_est.strftime('%B')} {today_est.day}, {today_est.year}"

    rows = []

    for day_block in section.find_all("div", class_="mb-8", recursive=False):
        date_tag = day_block.find("h3")
        date_text = date_tag.get_text(strip=True) if date_tag else None

        # Only today's block
        if date_text != today_str:
            continue

        for card in day_block.select("div.space-y-6 > div.bg-white"):
            header = card.find("div", class_="flex")
            sport_name_tag = header.find("span", class_="font-semibold") if header else None
            sport_name = sport_name_tag.get_text(strip=True) if sport_name_tag else "Unknown sport"

            tbody = card.find("tbody")
            if not tbody:
                continue

            for tr in tbody.find_all("tr"):
                if "seo-sport-filter" in tr.get("class", []):
                    continue

                tds = tr.find_all("td")
                if len(tds) != 3:
                    continue

                # time (sport71 gives a unix ms timestamp — assume UTC)
                time_span = tds[0].find("span", class_="event-time")
                unix_time = time_span.get("data-unix-time") if time_span else None
                event_dt = None
                if unix_time:
                    try:
                        ts_ms = int(unix_time)
                        # Make this explicitly UTC, then convert to EST
                        dt_utc = datetime.fromtimestamp(ts_ms / 1000.0, tz=UTC)
                        event_dt = dt_utc.astimezone(EST)
                    except Exception as e:
                        print("[sport71][ERROR] failed to parse unix_time:", unix_time, e)
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

                # Determine if the game is live (comparing in EST)
                is_live = False
                now_est = datetime.now(EST)
                if event_dt and event_dt <= now_est <= event_dt + timedelta(hours=2.5):
                    is_live = True

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
                        "is_live": is_live,
                    }
                )

    df = pd.DataFrame(rows)
    return df


# ========= 2) SHARKSTREAMS: TODAY'S GAMES =========
def scrape_today_games_shark() -> pd.DataFrame:
    """
    Scrape games from sharkstreams.net homepage and return
    a DataFrame with the same columns as sport71, including a
    'streams' list and 'embed_url' prefilled.

    IMPORTANT: sharkstreams times are assumed to be listed in US/Eastern.
    We localize them as EST (no conversion from server local time).
    """
    try:
        req = requests.get(BASE_URL_SHARK, headers=HEADERS, timeout=15)
    except Exception as e:
        print("[shark][ERROR] Requesting page failed:", e)
        return pd.DataFrame()

    if req.status_code != 200:
        print("[shark][ERROR] Non-200 HTTP status:", req.status_code)
        return pd.DataFrame()

    soup = BeautifulSoup(req.text, "html.parser")

    rows: List[Dict[str, Any]] = []

    for row_div in soup.find_all("div", class_="row"):
        # date/time
        date_span = row_div.find("span", class_="ch-date")
        if not date_span:
            continue

        date_str = date_span.get_text(strip=True)
        try:
            naive_dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
            # Treat this as a *local EST time*, not server local or UTC
            event_dt = EST.localize(naive_dt)
        except Exception:
            print(f"[shark][ERROR] Failed to parse date: {date_str}")
            continue

        # category (sport)
        cat_span = row_div.find("span", class_="ch-category")
        sport_name = cat_span.get_text(strip=True) if cat_span else "Unknown"

        # matchup / channel name
        name_span = row_div.find("span", class_="ch-name")
        raw_matchup = name_span.get_text(strip=True) if name_span else "Unknown"

        # mark SharkStreams entries as alternative links
        matchup = raw_matchup

        embed_link = None
        for a in row_div.find_all("a", class_="hd-link"):
            if "Embed" in a.get_text(strip=True):
                onclick = a.get("onclick", "")
                m = re.search(r"openEmbed\('([^']+)'", onclick)
                if m:
                    embed_link = m.group(1)
                break

        if not embed_link:
            for a in row_div.find_all("a", class_="hd-link"):
                if "Watch" in a.get_text(strip=True):
                    onclick = a.get("onclick", "")
                    m = re.search(r"window\.open\(\s*'([^']+)'", onclick)
                    if m:
                        rel = m.group(1)
                        embed_link = urljoin(BASE_URL_SHARK, rel)
                    break

        if not embed_link:
            print(f"[shark][ERROR] No embed link found for matchup: {matchup}")
            continue

        embed_link = urljoin(BASE_URL_SHARK, embed_link)

        # Use the EST-aware datetime to compute unix time
        time_unix = int(event_dt.timestamp() * 1000)

        streams = [
            {
                "label": "SharkStreams",
                "embed_url": embed_link,
            }
        ]

        # Determine if the game is live (based on EST)
        now_est = datetime.now(EST)
        is_live = event_dt <= now_est <= event_dt + timedelta(hours=2.5)

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
                "is_live": is_live,
            }
        )

    df = pd.DataFrame(rows)
    return df


# ========= 3) FUNCTION TO GET STREAMS FROM WATCH PAGE =========
def get_all_streams_from_watch_page(watch_url: Optional[str]) -> List[Dict[str, Any]]:
    """
    Given a sport71-style watch URL, extract the stream URLs from it.
    Returns a list of streams: each one is a dictionary with label and embed_url.
    """
    if not watch_url:
        return []

    try:
        r = requests.get(watch_url, headers=HEADERS, timeout=15)
        if r.status_code != 200:
            print("[ERROR] Non-200 status on watch URL:", r.status_code)
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        streams: List[Dict[str, Any]] = []

        # Try to extract the main embed link (the iframe)
        iframe = soup.find("iframe")
        if iframe and iframe.get("src"):
            embed_url = iframe.get("src")
            streams.append({
                "label": "Main Stream",  # Default label for the main stream
                "embed_url": embed_url,
                "watch_url": watch_url,
            })
        
        # If no iframe is found, extract the stream URLs from the page
        stream_links = soup.select("div.stream-picker a.stream-button")
        if stream_links:
            for link in stream_links:
                stream_name = link.get_text(strip=True)
                stream_page_url = link.get("href")
                full_stream_url = urljoin(watch_url, stream_page_url)
                
                # Now, fetch the embed URL from the individual stream page
                r_stream = requests.get(full_stream_url, headers=HEADERS, timeout=15)
                if r_stream.status_code == 200:
                    stream_soup = BeautifulSoup(r_stream.text, "html.parser")
                    stream_iframe = stream_soup.find("iframe")
                    if stream_iframe and stream_iframe.get("src"):
                        embed_url = stream_iframe.get("src")
                        streams.append({
                            "label": f"Stream: {stream_name}",
                            "embed_url": embed_url,
                            "watch_url": full_stream_url,
                        })

        return streams
    except Exception as e:
        print("[ERROR] Error fetching streams:", e)
        return []


# ========= 4) EXTRACT EMBED URL =========
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


# ========= 5) MAIN FUNCTION TO COMBINE DATA AND WRITE CSV =========
def main() -> None:
    # 1) sport71 games
    df_sport71 = scrape_today_games_sport71()

    # if we got any sport71 games, fetch their streams
    if not df_sport71.empty:
        df_sport71["streams"] = df_sport71["watch_url"].apply(get_all_streams_from_watch_page)
        df_sport71["embed_url"] = df_sport71["streams"].apply(
            lambda streams: streams[0]["embed_url"] if streams else None
        )

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
        return

    output_file = "today_games_with_all_streams.csv"
    try:
        df.to_csv(output_file, index=False)
    except Exception as e:
        print(f"[main][ERROR] Failed to write CSV to {output_file}:", e)


if __name__ == "__main__":
    main()
