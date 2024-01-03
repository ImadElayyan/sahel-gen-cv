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
    }
    # },
    # {
    #     "name": "order_product",
    #     "description": "Order a product based on the provided parameters",
    #     "parameters": {
    #         "type": "object",
    #         "properties": {
    #             "account_id": {
    #                 "type": "number",
    #                 "description": "Four digit account number (i.e., 1005, 2345, etc.)"
    #             },
    #             "product_name": {
    #                 "type": "string",
    #                 "description": "Name of the product to order (i.e., Elysian Voyager, Terra Roamer, AceMaster 3000, Server & Style)"
    #             },
    #             "quantity": {
    #                 "type": "number",
    #                 "description": "Quantity of the product to order (i.e., 1, 2, etc.)"
    #             }
    #         },
    #         "required": ["account_id", "product_name", "quantity"],
    #     }
    # },
    #     {
    #     "name": "get_product_information",
    #     "description": "Find information about a product based on a user question. Use only if the requested information if not already available in the conversation context.",
    #     "parameters": {
    #         "type": "object",
    #         "properties": {
    #             "user_question": {
    #                 "type": "string",
    #                 "description": "User question (i.e., do you have tennis shoes for men?, etc.)"
    #             },
    #         },
    #         "required": ["user_question"],
    #     }
    # }
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
                # "get_product_information": get_product_information,
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

        if function_to_call == get_product_information:
            product_info = json.loads(function_response)
            # show product information after search for a different product that the current one
            # if product_info['product_image_file'] != current_product_image:
                
            products = [display_product_info(product_info)]
            current_product_image = product_info['product_image_file']
            
            # return only product description to LLM to avoid chatting about prices and image files 
            function_response = product_info['description']

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

def order_product(account_id, product_name, quantity=1):
     
    # Step 1: Find the maximum existing order_id
    query = "SELECT MAX(order_id) FROM Orders"
    results = execute_sql_query(query)
    max_order_id = results[0][0] if results[0][0] is not None else 0

    # Step 2 & 3: Find product ID and check stock
    query = "SELECT id, name, stock FROM Products WHERE LOWER(name) LIKE LOWER(?)"
    params = (f'%{product_name}%',)
    results = execute_sql_query(query, params=params)
    
    # Handling no match found
    if not results:
        return json.dumps({"info": "No matching product found"})
    
    product_id, product_name_corrected, stock = results[0]
    
    # Check if the stock is sufficient
    if stock < quantity:
        return json.dumps({"info": "Insufficient stock"})
    
    # Step 4: Place the order
    # Deducting the ordered quantity from the stock
    query = "UPDATE Products SET stock = stock - ? WHERE id = ?"
    params = (quantity, product_id)
    if place_orders: execute_sql_query(query, params=params)

    # Adding the order details to the Orders table
    days_to_delivery = 5
    for i in range(quantity):
        max_order_id += 1
        query = "INSERT INTO Orders (order_id, product_id, days_to_delivery, account_id) VALUES (?, ?, ?, ?)"
        params = (max_order_id, product_id, days_to_delivery, account_id)
        if place_orders: execute_sql_query(query, params=params)
    
    # Step 5: Calculate the expected delivery date and return the JSON object
    today = datetime.now()
    expected_delivery_date = today + timedelta(days=days_to_delivery)
    
    return json.dumps({
        "info": "Order placed",
        "product_name": product_name_corrected,
        "expected_delivery_date": expected_delivery_date.strftime('%Y-%m-%d')
    })

def display_product_info(product_info, display_size=40):
    """ Display product information """

    # Show image
    image_file = product_info['product_image_file']

    image_url = blob_sas_url.split("?")[0] + f"{image_file}" #+ blob_sas_url.split("?")[1]

    response = requests.get(image_url)
    print(image_url)

    # Check if the request was successful
    if response.status_code == 200:
        return {
            "tagline": product_info['tagline'],
            "original_price": product_info['original_price'],
            "special_offer": product_info['special_offer'],
            "image_url": image_url 
            }
    else:
        print(f"Failed to retrieve image. HTTP Status code: {response.status_code}")

    print(f"""
    {product_info['tagline']}
    Original price: ${product_info['original_price']} Special offer: ${product_info['special_offer']} 
    """)
    
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

def get_product_information(user_question, categories='*', top_k=1):
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
                "fields": "description_vector",
                "k": top_k
            },
        ],
        "select": "tagline, description, original_price, special_offer, product_image_file",
    }

    # optional filtered search
    if categories != '*':
        data["filter"] = f"category eq '{categories}'"

    results = requests.post(url, headers=headers, data=json.dumps(data))    
    results_json = results.json()
    
    # Extracting the required fields from the results JSON
    product_data = results_json['value'][0] # hard limit to top result for now

    response_data = {
        "tagline": product_data.get('tagline'),
        "description": product_data.get('description'),
        "original_price": product_data.get('original_price'),
        "special_offer": product_data.get('special_offer'),
        "product_image_file": product_data.get('product_image_file'),
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
