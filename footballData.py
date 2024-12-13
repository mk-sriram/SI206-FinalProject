import requests
import sqlite3
from dotenv import load_dotenv
import os

load_dotenv()
apiKey = os.getenv("API_KEY")
HEADERS = {"Authorization": f"Bearer {apiKey}", "accept": "application/json"}


def fetch_games(year, season_type="regular"):
    try:
        games_url = "https://api.collegefootballdata.com/games"
        params = {"year": year, "seasonType": season_type, "limit": 25,}
        response = requests.get(games_url, headers=HEADERS, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching games data: {e}")
        return []

def fetch_venues():
    try:
        venues_url = "https://api.collegefootballdata.com/venues"
        response = requests.get(venues_url, headers=HEADERS)
        response.raise_for_status()
        return response.json()
    except:
        print(f"Error fetching venue data")
        return []

def combine_data(games, venues):
    try:
        combined_data = []
        # Create a dictionary for quick venue lookup by venue_id
        venue_dict = {venue["id"]: venue for venue in venues}

        for game in games:
            game_id = game["id"]
            venue_id = game.get("venue_id")
            venue_data = venue_dict.get(venue_id, {})

            combined_entry = {
                "game_id": game_id,
                "game_date": game["start_date"].split("T")[0] if "start_date" in game else None,
                "game_time": game["start_date"].split("T")[1] if "start_date" in game and "T" in game["start_date"] else None,
                "city": venue_data.get("city", "Unknown"),
                "latitude": venue_data.get("location", {}).get("x"),
                "longitude": venue_data.get("location", {}).get("y"),
                "home_team": game["home_team"],
                "away_team": game["away_team"],
                "home_team_score": game["home_points"],
                "away_team_score": game["away_points"],
                "winning_team": game["home_team"] if game["home_points"] > game["away_points"] else game["away_team"],
                "total_points": game["home_points"] + game["away_points"] if game["home_points"] and game["away_points"] else None,
                "point_difference": abs(game["home_points"] - game["away_points"]) if game["home_points"] and game["away_points"] else None,
                "game_result": "home_team" if game["home_points"] > game["away_points"] else "away_team",
            }

            combined_data.append(combined_entry)
        return combined_data
    except Exception as e:
        print(f"Error combining data: {e}")
        return []

def create_table():
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS football_games (
        game_id INTEGER PRIMARY KEY,
        game_date TEXT NOT NULL,
        game_time TEXT,
        city TEXT NOT NULL,
        latitude REAL,
        longitude REAL,
        home_team TEXT NOT NULL,
        away_team TEXT NOT NULL,
        home_team_score INTEGER NOT NULL,
        away_team_score INTEGER NOT NULL,
        winning_team TEXT,
        total_points INTEGER,
        point_difference INTEGER,
        game_result TEXT,
        weather_id INTEGER DEFAULT NULL,  -- Foreign key to a weather table (can be NULL initially)
        FOREIGN KEY (weather_id) REFERENCES weather_table(weather_id)
    );
    ''')
    conn.commit()
    conn.close()
    print("Table 'football_games' created successfully.")

def addFootBallDataToTable(combined_data):
    # Connect to SQLite database
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()

    # Insert combined data into the table
    count = 0
    for game in combined_data:
        if count < 100: 
            cursor.execute('''
            INSERT OR REPLACE INTO football_games (
                game_id, game_date, game_time, city, latitude, longitude, 
                home_team, away_team, home_team_score, away_team_score, 
                winning_team, total_points, point_difference, game_result, weather_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                game["game_id"],
                game["game_date"],
                game["game_time"],
                game["city"],
                game["latitude"],
                game["longitude"],
                game["home_team"],
                game["away_team"],
                game["home_team_score"],
                game["away_team_score"],
                game["winning_team"],
                game["total_points"],
                game["point_difference"],
                game["game_result"],
                None
            
            ))
            count += 1
        else:
            break 
        
    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("Data successfully added to the 'football_games' table.")
