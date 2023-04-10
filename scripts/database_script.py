# Populate database with CSV files in ../data/raw

import pandas as pd
import sqlite3
import csv
import os


tables = []

def read_csv_headers():
  path = "./data/raw"
  files = os.listdir(path)

  for file in files:
    database_table = {}

    name = file.replace(".csv", "")
    database_table["name"] = name

    file_path = open(f"{path}/{file}")
    csv_reader = csv.reader(file_path)

    columns_array = next(csv_reader)
    database_table["columns"] = columns_array

    tables.append(database_table)

def determine_relationships():
  for table in tables:
    if "_" in table["name"]:
      name_array = table["name"].split("_")
      for name in name_array:
        name_no_s = name.removesuffix('s')
        table["columns"].append(f"FOREIGN KEY({name_no_s}_id) REFERENCES {name_no_s}s({name_no_s}_id)")

def create_sql_call():
  for table in tables:
    columns_string = ", ".join(table["columns"])
    table["columns"] = f"({columns_string})"

def connect_database():
  return sqlite3.connect("Sqlite3.db")

def table_exists(name, cur):
  first_row = cur.execute(
      '''SELECT name FROM sqlite_schema WHERE type='table' AND name=?;''', [name]).fetchone()
  if first_row == None:
    return False
  else:
    return True

def create_table(name, columns):
  conn = connect_database()
  cur = conn.cursor()

  if not (table_exists(name, cur)):
    cur.execute(f'''CREATE TABLE {name}{columns}''')
    table = pd.read_csv(f"./data/raw/{name}.csv")
    table.to_sql(f"{name}", conn, if_exists="replace", index=True)
    print(f"{name} table created")
  else:
    print(f"{name} table already exists")

def upload_to_database():
  read_csv_headers()
  determine_relationships()
  create_sql_call()
  for table in tables:
    create_table(table["name"], table["columns"])

upload_to_database()