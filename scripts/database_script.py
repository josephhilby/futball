# Populate database with CSV files in ../data/raw

import pandas as pd
import sqlite3
import csv
import os


con = sqlite3.connect("Sqlite3.db")
cur = con.cursor()

cur.execute(
    "CREATE TABLE games(game_id INTEGER PRIMARY KEY, season TEXT, type TEXT, away_team_id INTEGER, home_team_id INTEGER, away_goals INTEGER,home_goals INTEGER,venue TEXT, venue_link TEXT)"
    )
games = pd.read_csv("./data/raw/games.csv")
games.to_sql("games", con, if_exists="replace", index=True)

cur.execute(
    "CREATE TABLE teams(team_id INTEGER PRIMARY KEY,franchise_id INTEGER,team_name TEXT, abbreviation TEXT, stadium TEXT, link TEXT)"
    )
teams = pd.read_csv("./data/raw/teams.csv")
teams.to_sql("teams", con, if_exists="replace", index=True)

cur.execute(
    "CREATE TABLE game_teams(game_id INTEGER, team_id INTEGER, home_or_away TEXT, result TEXT, settled_in TEXT, head_coach TEXT, goals INTEGER, shots INTEGER, tackles INTEGER, penalty_minutes INTEGER, power_play_opportunities INTEGER, power_play_goals INTEGER,face_off_win_percentage REAL, giveaways INTEGER, takeaways INTEGER, FOREIGN KEY (game_id) REFERENCES games (game_id), FOREIGN KEY (team_id) REFERENCES teams (team_id))"
)
game_teams = pd.read_csv("./data/raw/game_teams.csv")
game_teams.to_sql("game_teams", con, if_exists="replace", index=True)
con.close()


# Having issues with below code not correctly creating fk and assoications
# tables = []

# def read_csv_headers():
#   path = "./data/raw"
#   files = os.listdir(path)

#   for file in files:
#     database_table = {}

#     name = file.replace(".csv", "")
#     database_table["name"] = name

#     file_path = open(f"{path}/{file}")
#     csv_reader = csv.reader(file_path)

#     columns_array = next(csv_reader)
#     database_table["columns"] = columns_array

#     tables.append(database_table)

# def determine_relationships():
#   for table in tables:
#     if "_" in table["name"]:
#       joins_table_pk = []
#       name_array = table["name"].split("_")
#       for name in name_array:
#         name_no_s = name.removesuffix('s')
#         joins_table_pk.append(f"{name_no_s}_id")
#         table["columns"].append(
#             f"CONSTRAINT fk_{name_no_s}s FOREIGN KEY ({name_no_s}_id) REFERENCES {name_no_s}s ({name_no_s}_id)")
#     else:
#       name_no_s = table["name"].removesuffix('s')
#       add_pk = table["columns"][0] = f"{name_no_s}_id PRIMARY KEY"

# def create_sql_call():
#   for table in tables:
#     columns_string = ", ".join(table["columns"])
#     table["columns"] = f"({columns_string})"

# def connect_database():
#   return sqlite3.connect("Sqlite3.db")

# def table_exists(name, cur):
#   first_row = cur.execute(
#       '''SELECT name FROM sqlite_schema WHERE type='table' AND name=?;''', [name]).fetchone()
#   if first_row == None:
#     return False
#   else:
#     return True

# def create_table(name, columns):
#   conn = connect_database()
#   cur = conn.cursor()

#   if not (table_exists(name, cur)):
#     cur.execute(f"CREATE TABLE {name} {columns}")
#     table = pd.read_csv(f"./data/raw/{name}.csv")
#     if "_" in name:
#       table.to_sql(f"{name}", conn, if_exists="replace", index=True)
#     else:
#       table.to_sql(f"{name}", conn, if_exists="replace", index=False)
#     print(f"CREATE TABLE {name} {columns}")
#     print(f"{name} table created")
#   else:
#     print(f"{name} table already exists")

# def upload_to_database():
#   read_csv_headers()
#   determine_relationships()
#   create_sql_call()
#   for table in tables:
#     create_table(table["name"], table["columns"])

# upload_to_database()