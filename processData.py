
import sqlite3
import json

#defining constants
FOOTBALL_DB_FILENAME = "football_data.db"
POINTS_INDEX = 0
TEMPERATURE_INDEX = 1
PRECIPITATION_INDEX = 2
WIND_INDEX = 3
JSON_OUTPUT_FILENAME = "process_data.json"


#sample join of table
# SELECT football_games.total_points, weather_data.temperature, weather_data.precipitation, weather_data.wind_speed
# FROM football_games
# INNER JOIN weather_data ON football_games.weather_id=weather_data.weather_id

def is_valid_row(list):
    return not any(value is None for value in list)

#JOIN databases
def fetch_football_weather_data():
    """
    Preform an INNER JOIN to retreive each football game's total points, temp, precipitation, and wind speed.
    """
    join_command = f"""SELECT football_games.total_points, 
                          weather_data.temperature, 
                          weather_data.precipitation, 
                          weather_data.wind_speed
                   FROM football_games
                   INNER JOIN weather_data 
                   ON football_games.weather_id = weather_data.weather_id"""
    conn = sqlite3.connect(FOOTBALL_DB_FILENAME)
    cursor = conn.cursor()
    cursor.execute(join_command)
    rows = cursor.fetchall()
    conn.close()
    return list(filter(lambda row: is_valid_row(row), rows))


#calculate linear regression
def calc_linear_regression(rows, x_index, y_index):
    """
    Calculate the linear regression (slope and intercept) between two variables.
    
    Args:
        rows (list of tuples): the data retrieved from the database.
        x_index (int): the index of the independent variable (temp, precipitation, or wind_speed).
        y_index (int): the index of the dependent variable (total_points).

    Returns:
        dict: a dictionary with the slope and intercept.
    """
    # extracting x and y values
    x_values = [row[x_index] for row in rows]
    y_values = [row[y_index] for row in rows]
    # print(x_values)
    # print(y_values)
    
    # number of data points
    n = len(x_values)
    
    # calculate sum
    x_sum = sum(x_values)
    # print(x_sum)
    y_sum = sum(y_values)
    # print(y_sum)
    x_sum_squared = sum(x ** 2 for x in x_values)
    # print(x_sum_squared)
    xy_sum = sum(x * y for x, y in zip(x_values, y_values))
    print(xy_sum)

    # calculate slope (m) and intercept (b):
    denominator = (n * x_sum_squared - x_sum ** 2)
    if not denominator == 0:
        m = (n * xy_sum - x_sum * y_sum) / denominator
    else:
        m = 99999999
    # print(m)
    b = (y_sum - m * x_sum) / n
    # print(b)
    
    return {"slope": m, "intercept": b}   


# # saving to json
def save_to_json_file(rows, regressions, filename=JSON_OUTPUT_FILENAME): 
    """
    Save the joined data and regression results to the JSON file.

    Args:
        rows (list of tuples): joined data.
        regressions (dict): regression results.
        filename (str): file name to save JSON data.
    """
    data = [
        {
            "total_points": row[POINTS_INDEX],
            "temperature": row[TEMPERATURE_INDEX],
            "precipitation": row[PRECIPITATION_INDEX],
            "wind_speed": row[WIND_INDEX],
        }
        for row in rows
    ]

    output = {
        "linear_regressions": regressions,
        "data": data,
    }


    with open(filename, "w") as json_file:
        json.dump(output, json_file, indent=4)
    print(f"Data saved to {filename}")


#Main 
if __name__ == "__main__":
    # fetch data
    output = fetch_football_weather_data()
    #print(output)
    # quit()

    # test_data = [(10, 60.0, 0.5, 5.0),
    #              (11, 64.0, 0.6, 6.0),
    #              (12, 62.0, 0.7, 7.0)]
    # test_regression = calc_linear_regression(test_data, 1, 0)
    # print(test_regression)
    # quit()

    # calculate regressions
    temp_regression = calc_linear_regression(output, TEMPERATURE_INDEX, POINTS_INDEX)
    precip_regression = calc_linear_regression(output, PRECIPITATION_INDEX, POINTS_INDEX)
    wind_regression = calc_linear_regression(output, WIND_INDEX, POINTS_INDEX)

    regressions = {
        "temperature": temp_regression,
        "precipitation": precip_regression,
        "wind_speed": wind_regression,
    }

    # print(regressions)
    # quit()

    #save to JSON
    save_to_json_file(output, regressions, JSON_OUTPUT_FILENAME)

