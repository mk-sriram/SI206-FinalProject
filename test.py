import sqlite3

# Connect to the SQLite database
conn = sqlite3.connect("football_data.db")
cursor = conn.cursor()

# Alter the 'football_games' table to add a new column
cursor.execute('''
    ALTER TABLE football_games ADD COLUMN traffic_id INTEGER DEFAULT NULL;
''')

# Commit the changes and close the connection
conn.commit()
conn.close()

print("Column 'traffic_id' added successfully to the 'football_games' table.")
