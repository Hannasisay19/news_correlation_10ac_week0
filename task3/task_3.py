from sqlalchemy import create_engine, MetaData, Table
import csv
from sqlalchemy import create_engine, MetaData, Table, Column, String
import sys

csv.field_size_limit(2147483647)

# Replace 'username', 'password', 'hostname', 'port', and 'database_name' with your actual credentials
DB_URL = 'postgresql://postgres:12345@localhost:5432/postgres'

engine = create_engine(DB_URL)

connection = engine.connect()
metadata = MetaData()
metadata.reflect(bind=engine)

# Path to your CSV file
csv_file_path = 'task3/data.csv'
# Open the CSV file with explicit encoding specified
with open(csv_file_path, 'r', encoding='utf-8') as file:
    # Read the first row to get column names
    reader = csv.reader(file)
    columns = next(reader)

    # Define the structure of the table based on column names
    metadata = MetaData()
    table = Table('dynamic_table', metadata, *[Column(column_name, String) for column_name in columns])

    # Reflect the table structure to ensure it exists in the database
    metadata.create_all(engine)

    # Insert data into the dynamically created table
    for row in reader:
        connection.execute(table.insert().values(**{columns[i]: value for i, value in enumerate(row)}))

print("Data imported successfully from CSV to dynamically created PostgreSQL table.")

