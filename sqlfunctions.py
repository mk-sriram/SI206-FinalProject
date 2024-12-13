from footballData import fetch_games, fetch_venues, combine_data, create_table, addFootBallDataToTable
from weatherData import create_weather_table, addWeatherDataFromDb
#Main stuff
year = 2020
season_type = "regular"
games = fetch_games(year, season_type)
print(f"Fetched {len(games)} games.")
venues = fetch_venues()
print(f"Fetched {len(venues)} venues.")
# with open("venues.json", "w") as json_file:
#     json.dump(venues, json_file, indent=4)
# quit()
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