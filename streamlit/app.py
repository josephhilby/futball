import pandas as pd
import streamlit as st
import plotly.express as px
import sqlite3

con = sqlite3.connect("../Sqlite3.db")


df = pd.read_sql_query("SELECT season, COUNT(*) AS game_count FROM games GROUP BY season", con)

print(df)