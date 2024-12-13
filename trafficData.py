import requests
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Traffic API Setup
API_KEY = os.getenv("TRAFFIC_API_KEY")  # Add your traffic API key to .env
TRAFFIC_API_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

def create_daily_traffic_table():
    """
    Create a table for storing hourly traffic data for a specific day.
    """
    conn = sqlite3.connect("football_data.db")
    cursor = conn.cursor()
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS daily_traffic_data (
        traffic_id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        congestion_level TEXT,
        avg_speed REAL,
        free_flow_speed REAL,
        current_travel_time INTEGER,
        free_flow_travel_time INTEGER,
        confidence REAL,
        road_closure BOOLEAN
    );
    ''')
    conn.commit()
    conn.close()
    print("Table 'daily_traffic_data' created successfully.")

def fetch_traffic_data(lat, lon):
    """
    Fetch traffic data for a specific location using the TomTom Traffic API.
    """
    try:
        params = {
            "point": f"{lat},{lon}",  # Latitude and Longitude
            "unit": "KMPH",
            "key": API_KEY,
        }
        response = requests.get(TRAFFIC_API_URL, params=params)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching traffic data: {e}")
        return None

def add_hourly_traffic_data_to_db(timestamp, traffic_data):
    """
    Add hourly traffic data to the daily_traffic_data table.
    """
    try:
        conn = sqlite3.connect("football_data.db")
        cursor = conn.cursor()

        # Extract relevant data from traffic API response
        flow_segment_data = traffic_data.get("flowSegmentData", {})
        congestion_level = flow_segment_data.get("frc", "Unknown")
        avg_speed = flow_segment_data.get("currentSpeed", None)
        free_flow_speed = flow_segment_data.get("freeFlowSpeed", None)
        current_travel_time = flow_segment_data.get("currentTravelTime", None)
        free_flow_travel_time = flow_segment_data.get("freeFlowTravelTime", None)
        confidence = flow_segment_data.get("confidence", None)
        road_closure = flow_segment_data.get("roadClosure", False)

        # Insert traffic data into the table
        cursor.execute('''
            INSERT INTO daily_traffic_data (
                timestamp, congestion_level, avg_speed, 
                free_flow_speed, current_travel_time, 
                free_flow_travel_time, confidence, road_closure
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', (timestamp, congestion_level, avg_speed, free_flow_speed, 
              current_travel_time, free_flow_travel_time, confidence, road_closure))

        conn.commit()
        conn.close()
        print(f"Traffic data added for {timestamp}.")
    except Exception as e:
        print(f"Error adding traffic data to DB: {e}")

def fetch_daily_traffic_data(lat, lon, date):
    """
    Fetch traffic data for every hour of a specific day.
    """
    # Start at midnight
    start_time = datetime.strptime(f"{date} 00:00:00", "%Y-%m-%d %H:%M:%S")
    end_time = start_time + timedelta(days=1)  # End of the day (24 hours)

    current_time = start_time
    while current_time < end_time:
        print(f"Fetching traffic data for {current_time}")
        traffic_data = fetch_traffic_data(lat, lon)
        if traffic_data:
            add_hourly_traffic_data_to_db(current_time.isoformat(), traffic_data)
        else:
            print(f"Traffic data not available for {current_time}.")
        
        # Increment time by 1 hour
        current_time += timedelta(hours=1)

if __name__ == "__main__":
    # Ensure the table exists
    create_daily_traffic_table()

    # Example location: Ann Arbor
    latitude = 33.7724449
    longitude = -84.3928054
    date = "2023-12-13"  # Replace with the date you want to fetch data for

    # Fetch traffic data for the day
    fetch_daily_traffic_data(latitude, longitude, date)
