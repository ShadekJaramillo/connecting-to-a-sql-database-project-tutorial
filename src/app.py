import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

def connect():
    try:
        connection_string = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
        print("Starting the connection...")
        engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
        engine.connect()
        print("Connected successfully!")
        return engine
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        exit()

with open(r'src/sql/create.sql') as create_tables_script:
    create_tables_text = text(create_tables_script.read())

with open(r'src/sql/insert.sql') as insert_tables_script:
    insert_tables_text = text(insert_tables_script.read())

with open(r'src/sql/drop.sql') as drop_tables_script:
    drop_tables_text = text(drop_tables_script.read())

# Load environment variables to the python app
load_dotenv()

# 1) Connect to the database with SQLAlchemy

engine = connect()

with engine.connect() as con:
    con.execute(drop_tables_text)

# 2) Create the tables
with engine.connect() as con:
    con.execute(create_tables_text)
# 3) Insert data

with engine.connect() as con:
    con.execute(insert_tables_text)

# 4) Use Pandas to read and display a table

df = pd.read_sql("SELECT * FROM books", engine.connect())
print(df)
