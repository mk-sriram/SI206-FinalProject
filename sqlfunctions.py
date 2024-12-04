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
        print(lat, lon, start_date, end_date)
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
def addWeatherDataToTable(combined_data):
    """
    Fetch weather data for games and add it to the weather_data table, linking it with football_games.
    """
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()

    for game in combined_data:
        lat = game["latitude"]
        lon = game["longitude"]
        date = game["game_date"]

        if lat and lon and date:
            # Fetch weather data for the game's location and date
            weather_data = fetch_weather(lat, lon, date, date)
            if weather_data:
                # Process the hourly data
                print(game["game_id"], " ", weather_data)
                # hourly = weather_data.Hourly()
                # if hourly:
                    #print()
                    # Extract time and variables
                    # times = [hourly.Time(i) for i in range(hourly.TimeLength())]
                    # temperature_values = [hourly.Variables(Variable.temperature_2m).Values(i) for i in range(hourly.VariablesLength())]
                    # precipitation_values = [hourly.Variables(Variable.precipitation).Values(i) for i in range(hourly.VariablesLength())]
                    # snowfall_values = [hourly.Variables(Variable.snowfall).Values(i) for i in range(hourly.VariablesLength())]
                    # wind_speed_values = [hourly.Variables(Variable.wind_speed_10m).Values(i) for i in range(hourly.VariablesLength())]
                    # wind_direction_values = [hourly.Variables(Variable.wind_direction_10m).Values(i) for i in range(hourly.VariablesLength())]
                    # humidity_values = [hourly.Variables(Variable.relative_humidity_2m).Values(i) for i in range(hourly.VariablesLength())]
                    # cloud_cover_values = [hourly.Variables(Variable.cloud_cover).Values(i) for i in range(hourly.VariablesLength())]

                    # # Match the closest time to the game's start time (assumes "game_time" is HH:MM)
                    # game_time = f"{date}T{game['game_time']}" if game["game_time"] else None
                    # if game_time in times:
                    #     index = times.index(game_time)

                    #     # Retrieve weather details for the matching time
                    #     temperature = temperature_values[index]
                    #     precipitation = precipitation_values[index]
                    #     snowfall = snowfall_values[index]
                    #     wind_speed = wind_speed_values[index]
                    #     wind_direction = wind_direction_values[index]
                    #     humidity = humidity_values[index]
                    #     cloud_cover = cloud_cover_values[index]

                    #     # Insert weather data into the weather_data table
                    #     cursor.execute('''
                    #     INSERT INTO weather_data (
                    #         city, latitude, longitude, weather_date, temperature, 
                    #         precipitation, snowfall, wind_speed, wind_direction, humidity, 
                    #         cloud_cover
                    #     ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    #     ''', (
                    #         game["city"],
                    #         lat,
                    #         lon,
                    #         date,
                    #         temperature,
                    #         precipitation,
                    #         snowfall,
                    #         wind_speed,
                    #         wind_direction,
                    #         humidity,
                    #         cloud_cover
                    #     ))

                    #     # Get the last inserted weather_id
                    #     weather_id = cursor.lastrowid

                    #     # Update football_games table with the weather_id
                    #     cursor.execute('''
                    #     UPDATE football_games SET weather_id = ? WHERE game_id = ?
                    #     ''', (weather_id, game["game_id"]))

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
addWeatherDataToTable(combined_data)