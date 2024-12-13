import requests

API_KEY = "iZcaJH99TAjGvykTMeGT2xjAqaLUkAPx"
TRAFFIC_API_URL = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

def fetch_traffic(lat, lon):
    params = {
        "point": f"{lat},{lon}",  # Latitude and Longitude
        "key": API_KEY,
        "unit": "KMPH",
    }
    response = requests.get(TRAFFIC_API_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None

# Example usage
lat, lon = 33.7724449, -84.3928054  # Coordinates for Denton, Texas
traffic_data = fetch_traffic(lat, lon)
if traffic_data:
    print(traffic_data)
