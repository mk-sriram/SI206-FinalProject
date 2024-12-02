import sqlite3
import cfbd

# Step 1: Configure API Client
configuration = cfbd.Configuration()
configuration.api_key['Authorization'] = 'your_api_key'  # Replace with your actual API key
configuration.api_key_prefix['Authorization'] = 'Bearer'
api_client = cfbd.ApiClient(configuration)
games_api = cfbd.GamesApi(api_client)

# Step 2: Fetch Game Data
year = 2023  # Replace with your desired year
games = games_api.get_games(year=year)

# Step 3: Connect to SQLite Database
conn = sqlite3.connect('football_data.db')
cursor = conn.cursor()

