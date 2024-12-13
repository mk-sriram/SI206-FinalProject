import sqlite3

def create_football_games_table(db_name):
    try:
        conn = sqlite3.connect(db_name)
        cursor = conn.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS football_games (
            game_id INTEGER PRIMARY KEY,
            game_date DATE NOT NULL,
            game_time TIME,
            city TEXT NOT NULL,
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
            penalties INTEGER
        );
        """

        cursor.execute(create_table_query)
        conn.commit()
        print("Tablecreated.")

    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()

create_football_games_table("final_project.db")
