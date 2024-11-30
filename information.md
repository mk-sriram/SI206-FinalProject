# To-Do List

## Read and Setup APIs
- Read the documentation for all APIs (Football API, Weather API 1, Weather API 2).
- Obtain API keys for all required APIs.
- Test the API endpoints to verify they work as expected.
- Write authentication functions if the API requires authentication.

## Plan Data Collection
- Identify and plan the data to collect from each API:
  - **Football API**:
    - Game date
    - Teams
    - Scores
    - Game location
    - Outcome
    - Yards (to measure distance run)
    - Turnovers (potentially linked to slippery weather)
  - **Weather APIs**:
    - Temperature
    - Wind speed
    - Humidity
    - Precipitation (snow, rain)
    - Air quality (from secondary Weather API)

## Data Matching
- Plan ways to match data from one API to another:
  - Check how the Football API tracks locations (e.g., coordinates, city names, or addresses).
  - Check how the Weather API provides location data (e.g., coordinates, city names).
  - If both APIs use city names or addresses, map locations directly to cities and create a mapping table for consistency.
  - If the APIs use coordinates, use latitude and longitude to match locations.

## Database Design
- Design a SQLite database schema:
  - Create a `football_games` table to store game data.
  - Create a `weather_data` table to store weather data.
  - Link the two tables with a foreign key (e.g., `weather_id`).

## Data Integration
- Write scripts to:
  - Fetch data from the Football API and store it in the `football_games` table.
  - Fetch data from the Weather APIs and store it in the `weather_data` table.
  - Map and link the weather data to the game data using the foreign key.

## Incremental Data Fetching
- Ensure each script stores only 25 or fewer items per execution.
- Run the scripts multiple times to gather at least 100 rows of data for each API.
- Avoid duplicating data in the database during incremental fetching.

----------------------------------------------------------
### Thoughts
1. goal where would be to see the yards increase and turnovers decrease when the weather coditions are better ( less rain, snow, wind etc )

2. once you have the game data, you can get the weather data for that specific game using the API and then map that to the game ID in the weather table 
    
    http://api.weatherstack.com/historical
    ? access_key = 01ad861dc5a07d983151c86fada007fb
    & query = New York ( city )
    & historical_date = 2015-01-21 ( date of the game )
    & hourly = 1 ( 24 -> day average , or 1,3,6 -> and then map it to when the game occurs ? (data only matters when the game occurs))



# API keys 
working on this rn

### database schemas 

CREATE TABLE football_games (
    game_id INTEGER PRIMARY KEY,
    game_date DATE NOT NULL,
    game_time TIME,
    city TEXT NOT NULL,   -- city where the game occured
    latitude REAL, 
    longitude REAL,
    home_team TEXT NOT NULL, 
    away_team TEXT NOT NULL,
    winning_team TEXT,
    home_team_score INTEGER NOT NULL,
    away_team_score INTEGER NOT NULL,
    game_result TEXT,
    total_points INTEGER,
    point_difference INTEGER,
    yards_gained INTEGER,
    turnovers INTEGER,
    penalties INTEGER,
);
 

CREATE TABLE weather_data (
    
    weather_id INTEGER PRIMARY KEY,           
    city TEXT NOT NULL,                       -- Name of the city where weather data is recorded
    latitude REAL,                            
    longitude REAL,                           
    weather_date DATE NOT NULL,               -- Date for the weather data
    temperature REAL,                         -- Average temperature on the given date
    precipitation REAL,                       --total precipitation (rain/snow) on the given date
    wind_speed REAL,                          --
    humidity REAL,                            --humidity level on the given date (percentage)
    weather_conditions TEXT                   
);



