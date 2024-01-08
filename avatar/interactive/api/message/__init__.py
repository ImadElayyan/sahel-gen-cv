import logging
import os
import json
import requests
from datetime import datetime, timedelta
import pyodbc

import azure.functions as func

search_endpoint = os.getenv("AZURE_SEARCH_ENDPOINT")
search_key = os.getenv("AZURE_SEARCH_API_KEY") 
search_api_version = '2023-07-01-Preview'
search_index_name = os.getenv("AZURE_SEARCH_INDEX")

AOAI_endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
AOAI_key = os.getenv("AZURE_OPENAI_API_KEY")
AOAI_api_version = os.getenv("AZURE_OPENAI_API_VERSION")
embeddings_deployment = os.getenv("AZURE_OPENAI_EMBEDDINGS_DEPLOYMENT")
chat_deployment = os.getenv("AZURE_OPENAI_CHAT_DEPLOYMENT")

sql_db_server = os.getenv("SQL_DB_SERVER")
sql_db_user = os.getenv("SQL_DB_USER")
sql_db_password = os.getenv("SQL_DB_PASSWORD")
sql_db_name = os.getenv("SQL_DB_NAME")

blob_sas_url = os.getenv("BLOB_SAS_URL")

server_connection_string = f"Driver={{ODBC Driver 17 for SQL Server}};Server=tcp:{sql_db_server},1433;Uid={sql_db_user};Pwd={sql_db_password};Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;"
database_connection_string = server_connection_string + f"Database={sql_db_name};"

# font color adjustments
blue, end_blue = '\033[36m', '\033[0m'

place_orders = False

functions = [
    {
        "name": "get_work_place",
        "description": "Check the citizen work place based on the provided parameters and fetch the salary and the hire date",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "number",
                    "description": "9 digits Civil ID (i.e., 123456789012, 123456789013, etc.)"
                },
            },
            "required": ["account_id"],
        }
    },
    {
        "name": "get_citizen_documents",
        "description": "Check citizen account and fetch the required document details",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "number",
                    "description": "9 Civil ID (i.e., 123456789012, 123456789013, etc.)"
                },
            },
            "required": ["account_id"],
        }
    },
    {
        "name": "get_citizen_bills",
        "description": "Check if there are any bills for the citizen account and fetch the required details",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "number",
                    "description": "9 Civil ID (i.e., 123456789012, 123456789013, etc.)"
                },
            },
            "required": ["account_id"],
        }
    },
    {
        "name": "pay_citizen_bills",
        "description": "pay the required bill for the citizen account and fetch the required details",
        "parameters": {
            "type": "object",
            "properties": {
                "bill_number": {
                    "type": "number",
                    "description": "Bill number (i.e., 123456789012, 123456789013, etc.)"
                },
                "amount": {
                    "type": "number",
                    "description": "decimal amount (i.e., 10.000, 20.250, etc.)"
                },
            },
            "required": ["bill_number", "amount"],
        },
    },
    {
        "name": "renew_citizen_documents",
        "description": "renew the required document for the citizen account and fetch the required details",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "number",
                    "description": "9 Civil ID (i.e., 123456789012, 123456789013, etc.)"
                },
                "documentNumber": {
                    "type": "string",
                    "description": "document number (i.e., 123456789012, 123456789013, etc.)"
                },
            },
            "required": ["account_id", "documentNumber"],
        },
    },
    {
        "name": "get_citizen_details",
        "description": "get the citizen details based on a user question. Use only if the requested information is not available in the conversation context.",
        "parameters": {
            "type": "object",
            "properties": {
                "account_id": {
                    "type": "number",
                    "description": "9 Civil ID (i.e., 123456789012, 123456789013, etc.)"
                },
                "user_question": {
                    "type": "string",
                    "description": "User question (i.e., What is my DOB?, What is my address?, what is my nationality?, what is the full name, etc.)"
                }
            },
            "required": ["account_id", "user_question"],
        },
    }

]

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    messages = json.loads(req.get_body())
    logging.info("Messages:" +  json.dumps(messages))
    response = chat_complete(messages, functions= functions, function_call= "auto")
    
    logging.info("response:" + json.dumps(response))

    products = []
    
    try:
        response_message = response["choices"][0]["message"]
    except:
        logging.info(response)

    # if the model wants to call a function
    if response_message.get("function_call"):
        # Call the function. The JSON response may not always be valid so make sure to handle errors
        function_name = response_message["function_call"]["name"]

        available_functions = {
                "get_work_place": get_work_place,
                "get_citizen_documents": get_citizen_documents,
                "get_citizen_information": get_citizen_information,
                "get_citizen_bills": get_citizen_bills,
                "pay_citizen_bills": pay_citizen_bills,
                "renew_citizen_documents": renew_citizen_documents,
                "get_citizen_details": get_citizen_details,
        }
        function_to_call = available_functions[function_name] 

        function_args = json.loads(response_message["function_call"]["arguments"])
        function_response = function_to_call(**function_args)
        # print(function_name, function_args)

        # Add the assistant response and function response to the messages
        messages.append({
            "role": response_message["role"],
            "function_call": {
                "name": function_name,
                "arguments": response_message["function_call"]["arguments"],
            },
            "content": None
        })

        # if function_to_call == get_product_information:
        #     product_info = json.loads(function_response)
        #     # show product information after search for a different product that the current one
        #     # if product_info['product_image_file'] != current_product_image:
                
        #     products = [display_product_info(product_info)]
        #     current_product_image = product_info['product_image_file']
            
        #     # return only product description to LLM to avoid chatting about prices and image files 
        #     function_response = product_info['description']

        messages.append({
            "role": "function",
            "name": function_name,
            "content": function_response,
        })
     
        response = chat_complete(messages, functions= functions, function_call= "none")
        
        response_message = response["choices"][0]["message"]

    messages.append({'role' : response_message['role'], 'content' : response_message['content']})

    logging.info(json.dumps(response_message))

    response_object = {
        "messages": messages,
        "products": products
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

def get_work_place(account_id):
    """Retrieve Citizen work place from account_id, with the salary and the number of days off."""
     
    # Define the SQL query to retrieve loyalty_points for the given account_id
    query = "SELECT Company, HireDate, Salary FROM WorkPlaces WHERE CID = ?"

    # Execute the query with account_id as a parameter
    results = execute_sql_query(query, params=(account_id,))

    # If results are empty, return an error message in JSON format
    if not results:
        return json.dumps({"error": "Account not found"})

    # Get the loyalty_points value
    company = results[0][0]
    hire_date = results[0][1]
    salary = results[0][2]


    # Create a JSON object with the required keys and values
    response_json = json.dumps({
        "work_place": company,
        "hire_date": str(hire_date),
        "salary": str(salary)
    })

    print("Response :" + response_json)

    return response_json


def get_citizen_documents(account_id):
     
    # Get orders and corresponding product names for the account_id
    query = '''
        SELECT CID, Category, IssueDate, ExpiryDate, DocumentNumber
        FROM Documents
        WHERE CID = ?
    '''
    documents = execute_sql_query(query, params=(account_id,))
    
    # Get today's date and calculate the expected delivery date for each order
    today = datetime.today()
    
    # Create a JSON object with the required details
    document_details = [
        {
            "cid": document.CID,
            "Category": document.Category,
            "IssueDate": str(document.IssueDate),
            "ExpiryDate": str(document.ExpiryDate),
            "Expired":  document.ExpiryDate < today.date(), 
            "DocumentNumber": document.DocumentNumber
        }
        for document in documents
    ]
    
    # Return the JSON object
    return json.dumps(document_details)

def get_citizen_information(account_id):
     
    # Get orders and corresponding product names for the account_id
    query = '''
        SELECT CID, Name
        FROM Citizens
        WHERE CID = ?
    '''
    citizen = execute_sql_query(query, params=(account_id,))
    
    # Get today's date and calculate the expected delivery date for each order
    today = datetime.today()

    # Create a JSON object with the required keys and values
    citizen_information = json.dumps({
        "CID": citizen[0][0],
        "Name": citizen[0][1]
    })
    
    
    # Return the JSON object
    return json.dumps(citizen_information)

def get_citizen_bills(account_id):
    """Retrieve Citizen bills from account_id, with the bill number, due date and amount."""
     
    # Define the SQL query to retrieve loyalty_points for the given account_id
    query = "SELECT BillNumber, DueDate, Amount, BillType FROM Bills WHERE CID = ?"

    # Execute the query with account_id as a parameter
    bills = execute_sql_query(query, params=(account_id,))

    # If results are empty, return an error message in JSON format
    if not bills:
        return json.dumps({"error": "Account not found"})

    bill_type_details = [
        {
            "cid": account_id,
            "bill_number": bill.BillNumber,
            "due_date": str(bill.DueDate),
            "amount": str(bill.Amount),
            "bill_type": bill.BillType
        }
        for bill in bills
    ]

    # Create a JSON object with the required keys and values
    response_json = json.dumps(bill_type_details)

    print("Response :" + response_json)

    return response_json

def pay_citizen_bills(bill_number, amount):
    """Retrieve Citizen bills from account_id, with the bill number, due date and amount."""
     
    # Define the SQL query to retrieve loyalty_points for the given account_id
    query = "INSERT INTO BillPayment (BillNumber, amount, TrxDate) VALUES (?, ?, GETDATE())"

    # Execute the query with account_id as a parameter
    results = execute_sql_query(query, params=(bill_number, amount))

    query = "SELECT amount, DueDate, BillType FROM Bills WHERE BillNumber = ?"

    results = execute_sql_query(query, params=(bill_number))

    original_amount = results[0][0]
    due_date = results[0][1]
    bill_type = results[0][2]

    remaining_amount = original_amount - amount

    query = "Update Bills SET amount = ? WHERE BillNumber = ?"

    results = execute_sql_query(query, params=(remaining_amount, bill_number))

   



    # Create a JSON object with the required keys and values
    response_json = json.dumps({
        "info": "Bill paid successfully",
        "bill_number": str(bill_number),
        "due_date": str(due_date),
        "bill_type": bill_type,
        "remaining_amount": str(remaining_amount)
    })

    print("Response :" + response_json)

    return response_json
    
def renew_citizen_documents(account_id, documentNumber):
    """Renew the expired document."""
     
    # Define the SQL query to retrieve loyalty_points for the given account_id
    query = "UPDATE Documents SET ExpiryDate = DATEADD(YEAR, 3, ExpiryDate) WHERE CID = ? AND DocumentNumber = ?"

    # Execute the query with account_id as a parameter
    results = execute_sql_query(query, params=(account_id, documentNumber))

   
    query = '''
        SELECT CID, Category, IssueDate, ExpiryDate, DocumentNumber
        FROM Documents
        WHERE CID = ? AND DocumentNumber = ?
    '''
    documents = execute_sql_query(query, params=(account_id, documentNumber))

    # Create a JSON object with the required keys and values
    response_json = json.dumps({
        "info": "Document renewed successfully",
        "document_number": documentNumber,
        "expiry_date": str(documents[0][3])
    })

    print("Response :" + response_json)

    return response_json
def generate_embeddings(text):
    """ Generate embeddings for an input string using embeddings API """

    url = f"{AOAI_endpoint}/openai/deployments/{embeddings_deployment}/embeddings?api-version={AOAI_api_version}"

    headers = {
        "Content-Type": "application/json",
        "api-key": AOAI_key,
    }

    data = {"input": text}

    response = requests.post(url, headers=headers, data=json.dumps(data)).json()
    return response['data'][0]['embedding']

def get_citizen_details(account_id, user_question, top_k=1):
    """ Vectorize user query to search Cognitive Search vector search on index_name. Optional filter on categories field. """
     
    url = f"{search_endpoint}/indexes/{search_index_name}/docs/search?api-version={search_api_version}"

    headers = {
        "Content-Type": "application/json",
        "api-key": f"{search_key}",
    }
    
    vector = generate_embeddings(user_question)

    data = {
        "vectors": [
            {
                "value": vector,
                "fields": "fullName_vector",
                "k": top_k
            },
            {
                "value": vector,
                "fields": "address_vector",
                "k": top_k
            },
        ],
        "select": "Name, Nationality, Address, DOB, Gender, CID",
    }

    data["filter"] = f"CID eq '{account_id}'"

    print (data)
    results = requests.post(url, headers=headers, data=json.dumps(data))  

    print(results)
    results_json = results.json()
    print(results_json)
    
    # Extracting the required fields from the results JSON
    citiezn_data = results_json['value'][0] # hard limit to top result for now

    response_data = {
        "name": citiezn_data.get('Name'),
        "nationality": citiezn_data.get('Nationality'),
        "address": citiezn_data.get('Address'),
        "date_of_birth": citiezn_data.get('DOB'),
        "account_id": citiezn_data.get('CID'),
    }
    return json.dumps(response_data)

def chat_complete(messages, functions, function_call='auto'):
    """  Return assistant chat response based on user query. Assumes existing list of messages """
    
    url = f"{AOAI_endpoint}/openai/deployments/{chat_deployment}/chat/completions?api-version={AOAI_api_version}"

    headers = {
        "Content-Type": "application/json",
        "api-key": AOAI_key
    }

    data = {
        "messages": messages,
        "functions": functions,
        "function_call": function_call,
        "temperature" : 0,
    }

    response = requests.post(url, headers=headers, data=json.dumps(data)).json()

    return response
