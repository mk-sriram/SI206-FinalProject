import requests
import json
import sqlite3
import time 

# Install the SDK first: pip install openmeteo-requests
from openmeteo_requests import Client
from openmeteo_sdk.Variable import Variable

API_KEY = "YCk0uppo2tGeUarljXWxdKx61/+KnkWISco6EwKZnycklitueS8CAOFAQGHia1Sq"
HEADERS = {"Authorization": f"Bearer {API_KEY}", "accept": "application/json"}



def fetch_games(year, season_type="regular"):
    try:
        games_url = "https://api.collegefootballdata.com/games"
        params = {"year": year, "seasonType": season_type}
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
                "latitude": venue_data.get("location", {}).get("y"),
                "longitude": venue_data.get("location", {}).get("x"),
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


## WEATHER STUFF

def fetch_weather(lat, lon, start_date, end_date):
    """
    Fetch hourly weather data for a location using the Open-Meteo SDK.
    """
    try:
        #print(lat, lon, start_date, end_date)
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,relative_humidity_2m,precipitation,cloud_cover,wind_speed_10m,wind_direction_10m"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching games data: {response}")
        return []

def create_weather_table():
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS weather_data (
        weather_id INTEGER PRIMARY KEY,
        city TEXT NOT NULL,
        latitude REAL,
        longitude REAL,
        weather_date DATE NOT NULL,
        temperature REAL,
        precipitation REAL,
        wind_speed REAL,
        wind_direction REAL,
        humidity REAL,
        cloud_cover REAL
    );
    ''')
    conn.commit()
    conn.close()
    print("Table 'weather_data' created successfully with additional fields.")

from datetime import datetime

from datetime import datetime, timedelta

def find_closest_time_index(time_list, target_time):
    """
    Find the index of the closest time in the time_list to the given target_time in Zulu time.

    Args:
        time_list (list): List of time strings in ISO 8601 format without timezone information (local time).
        target_time (str): Target time string in ISO 8601 format with 'Z' (UTC).

    Returns:
        int: Index of the closest time in the list.
    """
    target_dt_utc = datetime.strptime(target_time, "%H:%M:%S.%fZ")

    # Parse the time list into naive datetime objects
    parsed_times = [datetime.strptime(t, "%Y-%m-%dT%H:%M") for t in time_list]

    # Find the index of the closest time
    closest_index = min(range(len(parsed_times)), key=lambda i: abs(parsed_times[i] - target_dt_utc)) 
    return closest_index # Return the index and the closest time


#     """
#     Fetch weather data for games and add it to the weather_data table, linking it with football_games.
#     """
#     conn = sqlite3.connect("football_data.db")
#     cursor = conn.cursor()

#     for game in combined_data:
#         lat = game["latitude"]
#         lon = game["longitude"]
#         date = game["game_date"]

#         if lat and lon and date:
#             # Fetch weather data for the game's location and date
#             weather_data = fetch_weather(lat, lon, date, date)

#             if weather_data:
#                 # Extract relevant weather data using the Open-Meteo SDK
#                 hourly = weather_data.Hourly()
#                 temperature = hourly.Variables(Variable.temperature, altitude=2).ValueArray() if hourly else None
#                 precipitation = hourly.Variables(Variable.precipitation).ValueArray() if hourly else None
#                 snowfall = hourly.Variables(Variable.snowfall).ValueArray() if hourly else None
#                 wind_speed = hourly.Variables(Variable.wind_speed, altitude=10).ValueArray() if hourly else None
#                 wind_direction = hourly.Variables(Variable.wind_direction, altitude=10).ValueArray() if hourly else None
#                 humidity = hourly.Variables(Variable.relative_humidity, altitude=2).ValueArray() if hourly else None
#                 cloud_cover = hourly.Variables(Variable.cloud_cover).ValueArray() if hourly else None

#                 # Insert weather data into the weather_data table
#                 cursor.execute('''
#                 INSERT INTO weather_data (
#                     city, latitude, longitude, weather_date, temperature, 
#                     precipitation, snowfall, wind_speed, wind_direction, humidity, 
#                     cloud_cover
#                 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                 ''', (
#                     game["city"],
#                     lat,
#                     lon,
#                     date,
#                     temperature[0] if temperature else None,
#                     precipitation[0] if precipitation else None,
#                     snowfall[0] if snowfall else None,
#                     wind_speed[0] if wind_speed else None,
#                     wind_direction[0] if wind_direction else None,
#                     humidity[0] if humidity else None,
#                     cloud_cover[0] if cloud_cover else None
#                 ))

#                 # Get the last inserted weather_id
#                 weather_id = cursor.lastrowid

#                 # Update football_games table with the weather_id
#                 cursor.execute('''
#                 UPDATE football_games SET weather_id = ? WHERE game_id = ?
#                 ''', (weather_id, game["game_id"]))

#     conn.commit()
#     conn.close()
#     print("Weather data successfully added to the 'weather_data' table and linked to 'football_games'.")
# def addWeatherDataToTable(combined_data):
#     """
#     Fetch weather data for games and add it to the weather_data table, linking it with football_games.
#     """
#     conn = sqlite3.connect("football_data.db")
#     cursor = conn.cursor()

#     for game in combined_data:
#         lat = game["latitude"]
#         lon = game["longitude"]
#         date = game["game_date"]
#         id = game["game_id"]
#         gameTime = game["game_time"]
#         if lat and lon and date:
#             # Fetch weather data for the game's location and date
#             weather_data = fetch_weather(lat, lon, date, date)
#             if weather_data:
#                 print(weather_data["hourly"]["time"]) 
#                 print(gameTime)
#                 print(id) 
                
#                 correct_index = find_closest_time_index(weather_data["hourly"]["time"],gameTime)
#                 print("index :",correct_index)
#                 # temperature
#                 # precipitation
#                 # snowfall
#                 # wind_speed
#                 # wind_direction
#                 # humidity
#                 # cloud_cover
#                 # cursor.execute('''
#                 #         INSERT INTO weather_data (``
#                 #             city, latitude, longitude, weather_date, temperature, 
#                 #             precipitation, snowfall, wind_speed, wind_direction, humidity, 
#                 #             cloud_cover
#                 #         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#                 #         ''', (
#                 #             game["city"],
#                 #             lat,
#                 #             lon,
#                 #             date,
#                 #             temperature,
#                 #             precipitation,
#                 #             snowfall,
#                 #             wind_speed,
#                 #             wind_direction,
#                 #             humidity,
#                 #             cloud_cover
#                 #         ))

#                 #         # Get the last inserted weather_id
#                 #         weather_id = cursor.lastrowid

#                 #         # Update football_games table with the weather_id
#                 #         cursor.execute('''
#                 #         UPDATE football_games SET weather_id = ? WHERE game_id = ?
#                 #         ''', (weather_id, game["game_id"]))
#     conn.commit()
#     conn.close()
#     print("Weather data successfully added to the 'weather_data' table and linked to 'football_games'.")

def fetch_football_data_from_db():
    """
    Fetch all rows from the football_games table.
    """
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()
    cursor.execute("SELECT game_id, game_date, game_time, city, latitude, longitude FROM football_games WHERE weather_id IS NULL")
    rows = cursor.fetchall()
    conn.close()
    return rows

def addWeatherDataFromDb():
    """
    Fetch weather data for football games and update the database.
    """
    # Fetch data from football_games table
    football_data = fetch_football_data_from_db()
    
    if not football_data:
        print("No football games to process for weather data.")
        return

    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()

    for game in football_data:
        game_id, game_date, game_time, city, lat, lon = game

        # Skip rows with missing location data
        if not lat or not lon or not game_date:
            print(f"Skipping game {game_id} due to missing location or date.")
            continue

        # Fetch weather data for the game's location and date
        weather_data = fetch_weather(lat, lon, game_date, game_date)
        if not weather_data or "hourly" not in weather_data:
            print(f"Failed to fetch weather data for game {game_id}.")
            continue

        # Find the closest time index
        hourly_times = weather_data["hourly"]["time"]
        closest_index = find_closest_time_index(hourly_times, game_time)

        # Extract relevant weather data
        hourly = weather_data["hourly"]
        temperature = hourly["temperature_2m"][closest_index]
        precipitation = hourly["precipitation"][closest_index]
        wind_speed = hourly["wind_speed_10m"][closest_index]
        wind_direction = hourly["wind_direction_10m"][closest_index]
        humidity = hourly["relative_humidity_2m"][closest_index]
        cloud_cover = hourly["cloud_cover"][closest_index]

        # Insert weather data into weather_data table
        cursor.execute('''
            INSERT INTO weather_data (
                city, latitude, longitude, weather_date, temperature, 
                precipitation, wind_speed, wind_direction, humidity, 
                cloud_cover
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            city, lat, lon, game_date, temperature, 
            precipitation, wind_speed, wind_direction, 
            humidity, cloud_cover
        ))

        # Get the last inserted weather_id
        weather_id = cursor.lastrowid

        # Update football_games table with the weather_id
        cursor.execute('''
            UPDATE football_games SET weather_id = ? WHERE game_id = ?
        ''', (weather_id, game_id))

    # Commit changes and close connection
    conn.commit()
    conn.close()
    print("Weather data successfully added to the 'weather_data' table and linked to 'football_games'.")
    

#Main stuff
year = 2020
season_type = "regular"
games = fetch_games(year, season_type)
print(f"Fetched {len(games)} games.")
venues = fetch_venues()
print(f"Fetched {len(venues)} venues.")

combined_data = combine_data(games, venues)

print("Creating table...")
create_table()

# Add data to table
print("Adding data...")
addFootBallDataToTable(combined_data)

print(f"Combined data saved to table.")


#Weather
create_weather_table()

# Add weather data and link to football_games table
print("Fetching weather data and updating tables...")
addWeatherDataFromDb()