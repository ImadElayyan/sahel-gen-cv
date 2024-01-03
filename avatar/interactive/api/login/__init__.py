import logging
import os
import json
import requests
from datetime import datetime, timedelta
import pyodbc

import azure.functions as func

sql_db_server = os.getenv("SQL_DB_SERVER")
sql_db_user = os.getenv("SQL_DB_USER")
sql_db_password = os.getenv("SQL_DB_PASSWORD")
sql_db_name = os.getenv("SQL_DB_NAME")

server_connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{sql_db_server},1433;Uid={sql_db_user};Pwd={sql_db_password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
database_connection_string = server_connection_string + f"Database={sql_db_name};"


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    
    # Parse the request body into a Python dictionary
    # request_body = json.loads(req.get_body())

    civilId = req.params.get('username')
    password = "P@ssw0rd"

    # Get the username and password from the request body
    # username = request_body.get('username')
    # password = request_body.get('password')
    # messages = request_body.get('messages')

    response = get_account_id(civilId, password)
    # messages.append({ "account_id": response['account_id'] })

    
    # logging.info(json.dumps(messages))

    response_object = {
        "messages": response
    }

    return func.HttpResponse(
        json.dumps(response_object),
        status_code=200
    )

def execute_sql_query(query, connection_string=database_connection_string, params=None):
    """Execute a SQL query and return the results."""
    results = []
    print('database_connection_string', database_connection_string)
    
    # Establish the connection
    with pyodbc.connect(connection_string) as conn:
        cursor = conn.cursor()
        
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        
        # If the query is a SELECT statement, fetch results
        if query.strip().upper().startswith('SELECT'):
            results = cursor.fetchall()
        
        conn.commit()

    return results

def get_account_id(civilId, password):
    """Retrieve Account ID from username and password."""
     
    # Define the SQL query to retrieve loyalty_points for the given account_id
    #query = "select account_id from customers where user_name = ? and password = ?"
    query = "select CID, Name from citizens where CID = ?"

    print("civilId: " + civilId)
    # Execute the query with account_id as a parameter
    results = execute_sql_query(query, params=(civilId,))

    # print("results: " + results)
    # If results are empty, return an error message in JSON format
    if not results:
        return json.dumps({"error": "Account not found"})

    # Get the loyalty_points value
    cid = results[0][0]
    name = results[0][1]

    # Create a JSON object with the required keys and values
    response_json = json.dumps({
        "civil_id": cid,
        "name": name
    })
    
    # print("Login Response: " + response_json)

    return response_json

