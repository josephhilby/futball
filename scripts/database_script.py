# Populate database with CSV

import pandas as pd
import sqlite3

conn = sqlite3.connect("Sqlite3.db")
c = conn.cursor()

game_table = {
  "name": "games",
  "columns": "(game_id int, season int, type text, date_time text, away_team_id int, home_team_id int, away_goals int, home_goals int, venue text, venue_link text)"
}

teams_table = {
  "name": "teams",
  "columns": "(team_id int, franchiseId int, teamName text, abbreviation text, Stadium text, link text)"
}

game_teams_table = {
  "name": "game_teams",
  "columns": "(game_id int, team_id int, HoA text, result text, settled_in text, head_coach text, goals int, shots int, tackles int, pim int, powerPlayOpportunities int, powerPlayGoals int, faceOffWinPercentage float, giveaways int, takeaways int)"
}

def create_table(name, columns):
  if not (table_exists(name)):
    c.execute(f'''CREATE TABLE {name} {columns}''')
    table = pd.read_csv(f"./data/raw/{name}.csv")
    table.to_sql(f"{name}", conn, if_exists="replace", index=False)
    print(f"{name} table created")
  else:
    print(f"{name} table already exists")

def table_exists(name):
  r = c.execute('''SELECT name FROM sqlite_schema WHERE type='table' AND name=?;''', [name]).fetchall()
  if r == []:
    return False
  else:
    return True

create_table(game_table["name"], game_table["columns"])
create_table(teams_table["name"], teams_table["columns"])
create_table(game_teams_table["name"], game_teams_table["columns"])