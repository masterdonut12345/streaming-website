import requests
from datetime import datetime

API_KEY = 'd9896a5835b877da485ec5a0812b9328'  # Replace with your actual API key
BASE_URL = 'https://api.the-odds-api.com/v4/sports/basketball_nba/odds'

def fetch_nba_odds():
    params = {
        'regions': 'us',
        'markets': 'h2h',
        'oddsFormat': 'american',
        'apiKey': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    
    if response.status_code != 200:
        print(f"Error fetching odds: {response.status_code}")
        return []
    
    return response.json()

def format_tweet(game):
    home = game['home_team']
    away = game['away_team']
    commence_time = game['commence_time']
    # Get odds from bookmakers
    try:
        book = game['bookmakers'][0]
        outcomes = book['markets'][0]['outcomes']
        home_odds = next(o['price'] for o in outcomes if o['name'] == home)
        away_odds = next(o['price'] for o in outcomes if o['name'] == away)
    except (IndexError, KeyError):
        home_odds = away_odds = "N/A"
    
    tweet = f"🏀 {home} vs {away}\n"
    tweet += f"Start: {commence_time}\n"
    tweet += f"Odds: {home} {home_odds}, {away} {away_odds}\n"
    return tweet

def main():
    games = fetch_nba_odds()
    
    if not games:
        print("No NBA games/odds found.")
        return
    
    # Filter out only the NBA games
    nba_games = [game for game in games if game['sport_key'] == 'basketball_nba']
    
    if not nba_games:
        print("No NBA games found.")
        return
    
    # Format and print each game as a tweet
    for game in nba_games:
        print(format_tweet(game))
        print("-" * 40)

if __name__ == "__main__":
    main()
