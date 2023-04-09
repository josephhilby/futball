# Populate database with CSV

import pandas as pd
import sqlite3
import csv
import os

def populate_database():
  path = "./data/raw"
  files = os.listdir(path)

  for file in files:
    database_table = {}

    name = file.replace(".csv", "")
    database_table["name"] = name

    file_path = open(f"{path}/{file}")
    csv_reader = csv.reader(file_path)

    columns_array = next(csv_reader)
    columns_string = ", ".join(columns_array)
    database_table["columns"] = f"({columns_string})"

    connect_database
    create_table(database_table["name"], database_table["columns"])

def connect_database():
  conn = sqlite3.connect("Sqlite3.db")
  cur = conn.cursor()

def create_table(name, columns):
  if not (table_exists(name)):
    cur.execute(f'''CREATE TABLE {name}{columns}''')
    table = pd.read_csv(f"./data/raw/{name}.csv")
    table.to_sql(f"{name}", conn, if_exists="replace", index=False)
    print(f"{name} table created")
  else:
    print(f"{name} table already exists")

def table_exists(name):
  first_row = cur.execute('''SELECT name FROM sqlite_schema WHERE type='table' AND name=?;''', [name]).fetchone()
  if first_row == None:
    return False
  else:
    return True

populate_database