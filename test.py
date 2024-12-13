import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect("football_data.db")
cursor = conn.cursor()

# Alter the 'football_games' table to add a new column
cursor.execute('''
    DROP TABLE weather_data;
''')

# Commit the changes and close the connection
conn.commit()
conn.close()

