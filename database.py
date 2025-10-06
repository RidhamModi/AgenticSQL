import os
import logging
from sqlalchemy import create_engine, text 
from dotenv import load_dotenv 

#load environment variables
load_dotenv()

#congiure logging
logging.basicConfig(level=logging.DEBUG)

#database credentials
MYSQL_HOST=os.getenv("MYSQL_HOST")
MYSQL_USER=os.getenv("MYSQL_USER")
MYSQL_PASSWORD=os.getenv("MYSQL_PASSWORD")
MYSQL_DATABASE=os.getenv("MYSQL_DATABASE")
MYSQL_PORT=os.getenv("MYSQL_PORT","3306")

#Create MYSQL connection URL
DATABASE_URL = f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}"

#Create SQLAlchemy engine
try:
    logging.debug(f"Connecting to MySQL at {MYSQL_HOST}:{MYSQL_PORT}")
    engine= create_engine(DATABASE_URL, echo=True)
    logging.debug("Database Connection Successful!")
except Exception as e:
    logging.error(f"Database connection Failed: {str(e)}")
    exit()

#Function to list Databases
def list_databases():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SHOW DATABASES;")).fetchall()
            return {"databases": [row[0] for row in result]}
    except Exception as e:
        return{"error" : str(e)}
    
#Function to list Tables
def list_tables(database_name):
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SHOW TABLES from {database_name};")).fetchall()
            return {"tables": [row[0] for row in result]}
    except Exception as e:
        return{"error" : str(e)}

#Function to list Columns
def list_columns(database_name, table_name):
    try:
        with engine.connect() as connection:
            result = connection.execute(text(f"SHOW COLUMNS from {database_name}.{table_name};")).fetchall()
            return {"columns": [row[0] for row in result]}
    except Exception as e:
        return{"error" : str(e)}
