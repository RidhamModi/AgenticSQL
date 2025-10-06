from fastapi import FastAPI
import logging
from database import list_databases, list_tables, list_columns
from fastapi import FastAPI, Query, HTTPException
from query_generator import generate_sql_query, execute_query

# Initialize FastAPI app
app = FastAPI()

#congiure logging
logging.basicConfig(level=logging.DEBUG)

#API: List all Databases
@app.get("/list_databases/")
def get_databases():
    return list_databases()

#API: List all tables in a database
@app.get("/list_tables/{database_name}")
def get_tables(database_name: str): 
    return list_tables(database_name)

#API: List all columns in a Table
@app.get("/list_columns/{database_name}/{table_name}")
def get_columnss(database_name: str, table_name: str): 
    return list_columns(database_name, table_name)

#API: Generate SQL Query from Natural Language 
@app.post("/generate_sql/")
def generate_sql(natural_language_query: str):
    logging.debug(f"Generating SQL Query for: {natural_language_query}")
    sql_query = generate_sql_query(natural_language_query)
    
    if sql_query:
        return {"sql_query": sql_query}
    return {"error": "Failed to generate SQL"}

# @app.post("/generate_sql/")
# async def generate_sql(natural_language_query: str = Query(..., description="User's natural language query")):
#     logging.debug(f"Generating SQL Query for: {natural_language_query}")
#     try:
#         sql_query = generate_sql_query(natural_language_query)
#         if "Error" in sql_query:
#             raise HTTPException(status_code=500, detail=sql_query)
#         return {"sql_query": sql_query}
#     except Exception as e:
#         logging.exception("Error in /generate_sql endpoint")
#         raise HTTPException(status_code=500, detail=str(e))
    
#API: To Execute SQL Query 
@app.post("/execute_sql/")
def execute_sql(sql_query: str = Query(..., description="SQL query to execute")):
    """Execute a given SQL query and return results."""
    results = execute_query(sql_query)
    return results #now properly formatted for JSON response
