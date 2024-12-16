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

def fetch_football_data_from_db(conn):
    cursor = conn.cursor()
    cursor.execute("SELECT game_id, game_date, game_time, city, latitude, longitude FROM football_games WHERE weather_id IS NULL")
    rows = cursor.fetchall()
    return rows
def create_weather_table():
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()
    
    cursor.execute('DROP TABLE IF EXISTS weather_data;')

    cursor.execute('''
    CREATE TABLE weather_data (
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
    print("Table 'weather_data' created successfully.")

def create_visibility_table():
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()
    
    cursor.execute('DROP TABLE IF EXISTS visibility_data;')

    cursor.execute('''
    CREATE TABLE visibility_data (
        visibility_id INTEGER PRIMARY KEY,
        latitude REAL,
        longitude REAL,
        weather_date DATE NOT NULL,
        visibility REAL
    );
    ''')
    conn.commit()
    conn.close()
    print("Table 'visibility_data' created successfully.")

def fetch_weather_batch(football_data):
    
    weather_batch = []
    for game in football_data:
        game_id, game_date, _, city, lat, lon = game
        if not lat or not lon or not game_date:
            print(f"Skipping game {game_id} due to missing location or date.")
            continue

        weather_data = fetch_weather(lat, lon, game_date, game_date)
        if not weather_data or "hourly" not in weather_data:
            print(f"Failed to fetch weather data for game {game_id}.")
            continue

        hourly = weather_data["hourly"]
        hourly_times = hourly["time"]
        closest_index = int(game[2][:2])  # Extract hour from game_time

        weather_batch.append((
            city, lat, lon, game_date, 
            hourly["temperature_2m"][closest_index],
            hourly["precipitation"][closest_index],
            hourly["wind_speed_10m"][closest_index],
            hourly["wind_direction_10m"][closest_index],
            hourly["relative_humidity_2m"][closest_index],
            hourly["cloud_cover"][closest_index]
        ))
  
    return weather_batch

def fetch_visibility_batch(football_data):
    visibility_batch = []
    for game in football_data:
        game_id, game_date, _, _, lat, lon = game
        if not lat or not lon or not game_date:
            print(f"Skipping game {game_id} due to missing location or date.")
            continue

        visibility_data = fetch_visibility(lat, lon, game_date, apiKey)
        if not visibility_data or "days" not in visibility_data or not visibility_data["days"]:
            print(f"Failed to fetch visibility data for game {game_id}.")
            continue

        hours_data = visibility_data["days"][0].get("hours", [])
        closest_index = int(game[2][:2])  # Extract hour from game_time
        visibility = None
        if len(hours_data) > closest_index:
            visibility = hours_data[closest_index].get("visibility", None)

        visibility_batch.append((lat, lon, game_date, visibility))

    return visibility_batch

def store_weather_data(conn, weather_batch, batch_number):
    cursor = conn.cursor()
    try:
        cursor.executemany('''
            INSERT INTO weather_data (
                city, latitude, longitude, weather_date, temperature, 
                precipitation, wind_speed, wind_direction, humidity, 
                cloud_cover
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', weather_batch)

        cursor.execute('SELECT weather_id FROM weather_data ORDER BY weather_id DESC LIMIT ?', (len(weather_batch),))
        weather_ids = [row[0] for row in cursor.fetchall()]
        print(f"Weather data batch {batch_number} inserted successfully.")
        return list(reversed(weather_ids))
    except sqlite3.OperationalError as e:
        print(f"Error storing weather data: {e}")
        return []
    finally:
        conn.commit()

def store_visibility_data(conn, visibility_batch, batch_number):
    cursor = conn.cursor()
    try:
        cursor.executemany('''
            INSERT INTO visibility_data (
                latitude, longitude, weather_date, visibility
            ) VALUES (?, ?, ?, ?)
        ''', visibility_batch)

        cursor.execute('SELECT visibility_id FROM visibility_data ORDER BY visibility_id DESC LIMIT ?', (len(visibility_batch),))
        visibility_ids = [row[0] for row in cursor.fetchall()]
        print(f"Visibility data batch {batch_number} inserted successfully.")
        return list(reversed(visibility_ids))
    except sqlite3.OperationalError as e:
        print(f"Error storing visibility data: {e}")
        return []
    finally:
        conn.commit()

def link_weather_to_football(conn, football_data, weather_ids):
    cursor = conn.cursor()
    try:
        for idx, game in enumerate(football_data):
            game_id = game[0]
            weather_id = weather_ids[idx]
            cursor.execute("UPDATE football_games SET weather_id = ? WHERE game_id = ?", (weather_id, game_id))
    except sqlite3.OperationalError as e:
        print(f"Error linking weather data: {e}")
    finally:
        conn.commit()

def link_visibility_to_football(conn, football_data, visibility_ids):
    cursor = conn.cursor()
    try:
        for idx, game in enumerate(football_data):
            game_id = game[0]
            visibility_id = visibility_ids[idx]
            cursor.execute("UPDATE football_games SET visibility_id = ? WHERE game_id = ?", (visibility_id, game_id))
    except sqlite3.OperationalError as e:
        print(f"Error linking visibility data: {e}")
    finally:
        conn.commit()

def addWeatherAndVisibilityDataFromDb(batch_size=25):
    conn = sqlite3.connect("football_data.db", check_same_thread=False)
    try:
        football_data = fetch_football_data_from_db(conn)
        if not football_data:
            print("No football games to process for weather or visibility data.")
            return

        total_games = len(football_data)
        print(f"Processing {total_games} games in batches of {batch_size}.")

        weather_batch_no = 0
        visibility_batch_no = 0 

        for batch_start in range(0, total_games, batch_size):
            batch_games = football_data[batch_start: batch_start + batch_size]

            weather_batch = fetch_weather_batch(batch_games)
            if weather_batch:
                weather_ids = store_weather_data(conn, weather_batch, weather_batch_no)
                link_weather_to_football(conn, batch_games, weather_ids)
                weather_batch_no += 1

            visibility_batch = fetch_visibility_batch(batch_games)
            if visibility_batch:
                visibility_ids = store_visibility_data(conn, visibility_batch, visibility_batch_no)
                link_visibility_to_football(conn, batch_games, visibility_ids)
                visibility_batch_no += 1

            print(f"Batch {batch_start // batch_size + 1} processed successfully.")

    except sqlite3.OperationalError as e:
        print(f"Database error: {e}")
    finally:
        conn.close()