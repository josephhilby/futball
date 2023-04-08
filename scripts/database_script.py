# Populate database with CSV

import pandas as pd
import sqlite3

conn = sqlite3.connect("Sqlite3.db")
c = conn.cursor()
c.execute('''CREATE TABLE games (game_id int, season int, type text, date_time text, away_team_id int, home_team_id int, away_goals int, home_goals int, venue text, venue_link text)''')
c.execute('''CREATE TABLE teams (team_id int, franchiseId int, teamName text, abbreviation text, Stadium text, link text)''')
# c.execute('''CREATE TABLE game_teams (game_id int, team_id int, HoA text, result text, settled_in text, head_coach text, goals int, shots int, tackles int, pim int, powerPlayOpportunities int, powerPlayGoals int, faceOffWinPercentage float, giveaways int, takeaways int)''')

games = pd.read_csv(".data/raw/games.csv")
games.to_sql("games", conn, if_exists="append", index = False)