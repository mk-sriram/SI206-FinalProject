## WEATHER STUFF
import requests
import sqlite3
from datetime import datetime

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

def find_closest_time_index(time_list, target_time):
    
    """""
    DON"T do all that the 23rd index will be the 23rd index in that array, just extract the hour value and index 
    
    
    """
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
        print(game)
        game_id, game_date, game_time, city, lat, lon = game

        # Skip rows with missing location data
        if not lat or not lon or not game_date:
            print(f"Skipping game {game_id} due to missing location or date.")
            continue

        # Fetch weather data for the game's location and date
        weather_data = fetch_weather(lat, lon, game_date, game_date)
        if not weather_data or "hourly" not in weather_data:
            print(f"Failed to fetch weather data for game {game_id} {lat} {lon}.")
            continue
        #print(weather_data)

        # Find the closest time index
        # DON"T do all that the 23rd index will be the 23rd index in that array, just extract the hour value and index 
    
        hourly_times = weather_data["hourly"]["time"]
        closest_index = int(game_time[:2])
        
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
    