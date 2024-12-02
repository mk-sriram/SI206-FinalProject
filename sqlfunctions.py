import sqlite3
import cfbd

# Step 1: Configure API Client
configuration = cfbd.Configuration()
configuration.api_key['Authorization'] = 'YCk0uppo2tGeUarljXWxdKx61/+KnkWISco6EwKZnycklitueS8CAOFAQGHia1Sq'  # Replace with your actual API key
configuration.api_key_prefix['Authorization'] = 'Bearer'
api_client = cfbd.ApiClient(configuration)
games_api = cfbd.GamesApi(api_client)

# Step 2: Fetch Game Data
year = 2020  # Replace with your desired year
games = games_api.get_games(year=year)

print(games)