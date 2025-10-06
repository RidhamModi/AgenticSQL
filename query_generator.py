import os 
from openai import OpenAI
import re
from dotenv import load_dotenv 
from sqlalchemy import text 
from sqlalchemy.exc import SQLAlchemyError 
from database import engine, list_databases, list_tables, list_columns 

#load env variables
load_dotenv()

#OpenAI API key
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

#Limits to avoid token limit issues
MAX_TABLES=5
MAX_COLUMNS_PER_TABLE=5

def clean_sql_output(response_text):
    """Removes markdown formatting and extracts the raw SQL query."""
    #Remove Markdown code block formatting ('''sql ...''')
    clean_query = re.sub(r"'''sql\n(.*?)\n'''", r"\1", response_text, flags=re.DOTALL)

    #Extract only valid SQL (handles AI exceptions)
    sql_match = re.search(r"SELECT .*?;", clean_query, re.DOTALL | re.IGNORECASE)

    return sql_match.group(0) if sql_match else clean_query.strip()

def get_limited_schema():
    """Fetches a reduced Database to fir token limits"""
    schema={}
    databases = list_databases().get("databases", [])
    for db in databases:
        schema[db]= {}
        tables = list_tables(db).get("tables", [])[:MAX_TABLES]
        for table in tables:
            schema[db][table]= list_columns(db,table).get("columns", [])[:MAX_COLUMNS_PER_TABLE]
    
    return schema
    
def generate_sql_query(nl_query):
    """Converts natural language query to an optimized SQL query."""
    schema = get_limited_schema()

    schema_text = "\n".join([
        f"{db}.{table}: {', '.join(columns)}" for db, tables in schema.items() for table, columns in tables.items()
        ])

    prompt= f""" 
    You are an SQL expert. Convert the following natural language query into an optimized MySQL query.
    Ensure:
    - Proper use of INDEXING where applicable.
    - Use of efficient JOINS instead of nested queries.
    - Use GROUP BY when aggregations are needed.
    - Ensure SQL is valid and optimized for execution.

    Database Schema:
    {schema_text}

    User Request: {nl_query}
    
    SQL Query:
"""
    try:
        response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a SQL optimization expert."},
            {"role": "user", "content": prompt}
        ]
    )
        raw_sql_query = response.choices[0].message.content.strip()
        # Clean the response to extract only the SQL query
        clean_query = clean_sql_output(raw_sql_query)
        return clean_query
    
    except Exception as e:
        return f"Error generating SQL query: {e}"

def execute_query(clean_query):
    """Executes a validated and optimized SQL query."""

    try:
        # Open a separate connection for query execution
        with engine.connect() as connection:
            result = connection.execute(text(clean_query))
            rows = result.fetchall()

        # Get Columns names
        column_names= result.keys()

        #Converst results into a list of dictionaries
        formatted_results = [dict(zip(column_names, row)) for row in rows]

        return {"results": formatted_results}

    except SQLAlchemyError as e:
        return {"results": str(e)}
