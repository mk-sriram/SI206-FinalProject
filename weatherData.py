## WEATHER STUFF
import requests
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
import os

load_dotenv()
apiKey = os.getenv("VISUAL")




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

def fetch_visibility(lat, lon, date, apiKey):
    """
    Fetch hourly visibility data for a location and date using the Visual Crossing Weather API.
    We assume we only need the data for one date (same start and end date).
    """
    try:
        url = (f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/"
               f"timeline/{lat},{lon}/{date}/{date}?unitGroup=us&include=hours&key={apiKey}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error fetching visibility data: {e}")
        return {}

def create_weather_table():
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()
    
#     cursor.execute('''
#     DROP TABLE IF EXISTS weather_data;
# ''')
    cursor.execute('''
    DROP TABLE IF EXISTS weather_data;
    ''')

    cursor.execute('''
    CREATE TABLE  weather_data (
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
        cloud_cover REAL,
        visibility REAL
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

def addWeatherDataFromDb(batch_size=25):
    football_data = fetch_football_data_from_db()
    
    if not football_data:
        print("No football games to process for weather data.")
        return

    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()

    try:
        total_games = len(football_data)
        print(f"Processing {total_games} games in batches of {batch_size}.")

        for batch_start in range(0, total_games, batch_size):
            batch_games = football_data[batch_start: batch_start + batch_size]
            batch_inserts = []
            updates = []

            for game in batch_games:
                game_id, game_date, game_time, city, lat, lon = game

                if not lat or not lon or not game_date:
                    print(f"Skipping game {game_id} due to missing location or date.")
                    continue

                weather_data = fetch_weather(lat, lon, game_date, game_date)
                if not weather_data or "hourly" not in weather_data:
                    print(f"Failed to fetch weather data for game {game_id}.")
                    continue

                hourly_times = weather_data["hourly"]["time"]
                closest_index = int(game_time[:2])

                hourly = weather_data["hourly"]
                temperature = hourly["temperature_2m"][closest_index]
                precipitation = hourly["precipitation"][closest_index]
                wind_speed = hourly["wind_speed_10m"][closest_index]
                wind_direction = hourly["wind_direction_10m"][closest_index]
                humidity = hourly["relative_humidity_2m"][closest_index]
                cloud_cover = hourly["cloud_cover"][closest_index]

                visibility_data = fetch_visibility(lat, lon, game_date, apiKey)
                visibility = None
                if "days" in visibility_data and visibility_data["days"]:
                    hours_data = visibility_data["days"][0].get("hours", [])
                    if len(hours_data) > closest_index:
                        visibility = hours_data[closest_index].get("visibility", None)

                batch_inserts.append((
                    city, lat, lon, game_date, temperature, precipitation,
                    wind_speed, wind_direction, humidity, cloud_cover, visibility
                ))

                updates.append((game_id,))

            if batch_inserts:
                cursor.executemany('''
                    INSERT INTO weather_data (
                        city, latitude, longitude, weather_date, temperature, 
                        precipitation, wind_speed, wind_direction, humidity, 
                        cloud_cover, visibility
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', batch_inserts)
                conn.commit()

                weather_ids = cursor.execute('SELECT last_insert_rowid()').fetchall()
                for idx, (weather_id,) in enumerate(weather_ids):
                    cursor.execute('''
                        UPDATE football_games SET weather_id = ? WHERE game_id = ?
                    ''', (weather_id, updates[idx][0]))
                conn.commit()

            print(f"Batch {batch_start // batch_size + 1} processed successfully.")

    except Exception as e:
        print(f"Error during batch processing: {e}")
    finally:
        conn.close()
        print("Weather data processed and linked successfully.")
