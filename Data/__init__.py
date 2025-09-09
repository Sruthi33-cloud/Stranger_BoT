import logging
import os
import json
import azure.functions as func
import sys
import asyncio

# Bot Framework SDK Libraries
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity, ActivityTypes
from botframework.connector.auth import MicrosoftAppCredentials

import pandas as pd
import requests
import snowflake.connector
import random
from openai import AzureOpenAI
from time import sleep

# Setup logging
logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)

# --- NEW: Custom Credentials Class for Single-Tenant ---
class SingleTenantAppCredentials(MicrosoftAppCredentials):
    """Custom credentials class that forces single-tenant OAuth endpoint"""
    def __init__(self, app_id: str, password: str, tenant_id: str):
        super().__init__(app_id, password)
        self.tenant_id = tenant_id
        self.oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}"
        print(f"SingleTenantAppCredentials initialized with authority: {self.oauth_endpoint}")

# --- Configuration: Get environment variables from Azure Function App settings ---
print("Loading environment variables...") # DEBUG
APP_ID = os.environ.get("MicrosoftAppId", "")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
APP_TYPE = os.environ.get("MicrosoftAppType", "")
APP_TENANT_ID = os.environ.get("MicrosoftAppTenantId", "")

AZURE_OPENAI_KEY = os.environ.get("AZURE_OPENAI_KEY")
AZURE_OPENAI_ENDPOINT = os.environ.get("AZURE_OPENAI_ENDPOINT")
AZURE_OPENAI_DEPLOYMENT_NAME = os.environ.get("AZURE_OPENAI_DEPLOYMENT_NAME")

SNOWFLAKE_USER = os.environ.get("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.environ.get("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.environ.get("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.environ.get("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.environ.get("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.environ.get("SNOWFLAKE_SCHEMA")

print(f"APP_ID: {APP_ID}, APP_PASSWORD: {(APP_PASSWORD)}") # DEBUG
print(f"AZURE_OPENAI_KEY: {bool(AZURE_OPENAI_KEY)}, AZURE_OPENAI_ENDPOINT: {AZURE_OPENAI_ENDPOINT}, AZURE_OPENAI_DEPLOYMENT_NAME: {AZURE_OPENAI_DEPLOYMENT_NAME}") # DEBUG
print(f"SNOWFLAKE_USER: {SNOWFLAKE_USER}, SNOWFLAKE_ACCOUNT: {SNOWFLAKE_ACCOUNT}") # DEBUG

# --- Initialize the Azure OpenAI Client ---
# NOTE: Removed the global ADAPTER variable as it's now created inside the main function for each request
try:
    AZURE_OPENAI_CLIENT = AzureOpenAI(
        api_key=AZURE_OPENAI_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version="2024-02-15-preview"
    )
    print("AzureOpenAI client initialized.") # DEBUG
except Exception as e:
    print(f"Error initializing AzureOpenAI client: {e}") # DEBUG
    AZURE_OPENAI_CLIENT = None

# --- Dynamic knowledge base loading from a file ---
def load_knowledge_base():
    print("Loading knowledge_base.json...") # DEBUG
    try:
        path = os.path.join(os.path.dirname(__file__), "knowledge_base.json")
        print(f"Knowledge base path: {path}") # DEBUG
        with open(path, "r") as f:
            kb = json.load(f)
            print(f"Successfully loaded knowledge_base.json, length: {len(kb)}") # DEBUG
            return kb
    except FileNotFoundError:
        print("knowledge_base.json not found. Please ensure it is in the same folder.") # DEBUG
        return []
    except Exception as e:
        print(f"Error loading knowledge_base.json: {e}") # DEBUG
        return []

KNOWLEDGE_BASE_DATA = load_knowledge_base()

# --- Snowflake Connection & Data Utilities ---
def get_rbac_table(conn):
    print("Fetching RBAC table from Snowflake...") # DEBUG
    query = "SELECT USER_ID, ROLE, STORE_ID FROM ENTERPRISE.RETAIL_DATA.RBAC_WORK_TABLE"
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    df.columns = df.columns.str.lower()
    df = df.rename(columns={'username': 'user_id', 'role': 'role', 'location_id': 'store_id'})
    print(f"RBAC table loaded, shape: {df.shape}") # DEBUG
    return df

def get_sales_territory_keys(conn):
    print("Fetching SalesTerritoryKeys from Snowflake...") # DEBUG
    query = "SELECT DISTINCT SalesTerritoryKey FROM ENTERPRISE.RETAIL_DATA.SALES_FACT ORDER BY SalesTerritoryKey"
    cur = conn.cursor()
    cur.execute(query)
    df = cur.fetch_pandas_all()
    cur.close()
    print(f"SalesTerritoryKeys loaded, count: {len(df)}") # DEBUG
    return df['SALESTERRITORYKEY'].tolist()

def create_location_to_territory_mapping(rbac_df, territory_keys):
    print("Creating location-to-territory mapping...") # DEBUG
    all_locations = sorted(rbac_df['store_id'].unique().tolist())
    location_to_territory_map = {}
    shuffled_territory_keys = territory_keys[:]
    random.shuffle(shuffled_territory_keys)
    territory_count = len(shuffled_territory_keys)
    for i, location_id in enumerate(all_locations):
        assigned_territory_key = shuffled_territory_keys[i % territory_count]
        location_to_territory_map[location_id] = assigned_territory_key
    print(f"Location-to-territory mapping created, length: {len(location_to_territory_map)}") # DEBUG
    return location_to_territory_map

def get_user_access(user_id, rbac_df):
    print(f"Getting user access for user_id: {user_id}") # DEBUG
    user_row = rbac_df[rbac_df['user_id'] == user_id]
    if user_row.empty:
        print("User access not found in RBAC table.") # DEBUG
        return None
    print("User access found.") # DEBUG
    return {
        "role": user_row.iloc[0]['role'],
        "store_id": user_row.iloc[0]['store_id']
    }

def get_metric_value(conn, measure, store_id, location_to_territory_map):
    print(f"Getting metric value for measure: {measure}, store_id: {store_id}") # DEBUG
    sales_territory_key = location_to_territory_map.get(store_id)
    if sales_territory_key is None:
        print("Sales territory key not found for store_id.") # DEBUG
        return {}
    if measure['measure_name'].lower() == "sales amount":
        query = f"""
            SELECT SUM(SalesAmount) AS total_sales_amount
            FROM ENTERPRISE.RETAIL_DATA.SALES_FACT
            WHERE SalesTerritoryKey = {sales_territory_key}
        """
        try:
            cur = conn.cursor()
            cur.execute(query)
            df_sales = cur.fetch_pandas_all()
            cur.close()
            df_sales.columns = df_sales.columns.str.lower()
            total_sales = df_sales['total_sales_amount'][0] if not df_sales.empty and not df_sales['total_sales_amount'].isnull().iloc[0] else 0.0
            print(f"Fetched total sales amount: {total_sales}") # DEBUG
            return {"Sales Amount": total_sales}
        except Exception as e:
            print(f"Error querying Snowflake for Sales Amount: {e}") # DEBUG
            return {"Sales Amount": 0.0}
    if measure['measure_name'].lower() == "traffic conversion":
        print("Returning default Traffic Conversion value.") # DEBUG
        return {"Traffic Conversion": 0.0}
    return {}

def find_measure_with_llm(user_query, knowledge_base):
    print(f"Finding measure with LLM for query: {user_query}") # DEBUG
    measure_names = [item['measure_name'] for item in knowledge_base]
    prompt = f"""
    The user's query is: "{user_query}"
    Based on the query, identify the most relevant measure from the following list.
    If a measure is clearly mentioned or implied, respond ONLY with the measure name, nothing else.
    If no measure is found, respond ONLY with the text "NO_MEASURE_FOUND".
    Available measures: {', '.join(measure_names)}
    Example 1:
    Query: "What is my total sales amount for the month?"
    Response: Sales Amount
    Your response for the user's query:
    """
    try:
        identified_measure = call_azure_openai(prompt, temperature=0.0, max_tokens=100)
        print(f"Identified measure from LLM: {identified_measure}") # DEBUG
        if identified_measure and identified_measure.strip().replace('.', '').lower() in [m.lower() for m in measure_names]:
            for measure in knowledge_base:
                if measure['measure_name'].lower() == identified_measure.strip().replace('.', '').lower():
                    print(f"Returning found measure: {measure}") # DEBUG
                    return measure
    except Exception as e:
        print(f"Error during LLM intent recognition: {e}") # DEBUG
    print("No measure found.") # DEBUG
    return None

def build_llm_prompt(user, access, user_query, measure, sf_data=None, location_to_territory_map=None):
    print("Building LLM prompt...") # DEBUG
    if not measure or not access or location_to_territory_map is None:
        print("Missing measure/access/data map for prompt.") # DEBUG
        return "Sorry, I couldn't find the measure, your access info, or a valid data map."
    sales_territory_key = location_to_territory_map.get(access['store_id'])
    prompt_template = f"""
    You are an AI assistant for a retail data team. Your user is a {access['role']} for store {access['store_id']} (which maps to sales territory {sales_territory_key}).
    The user's query is: '{user_query}'
    Here is the official definition of the requested measure and its current value for the user's store:
    Description: {measure['description']}
    DAX Formula: {measure['dax_formula']}
    """
    if sf_data and measure['measure_name'] in sf_data:
        data_value = sf_data[measure['measure_name']]
        if measure['measure_name'] == "Sales Amount":
            data_string = f"${data_value:,.2f}"
        else:
            data_string = f"{data_value:.3f}"
        prompt_template += f"\nFor your store ({access['store_id']}), the current {measure['measure_name']} is {data_string}."

    prompt_template += f"""
    Based ONLY on this information, provide a clear and concise answer to the user's question. After your answer, suggest 2-3 similar questions with insights and relevant measures that the user might want to ask next. Do not use any bolding, formatting, or headers like 'Answer:' or 'Suggestions:'. Just provide the response as plain text.
    Your response:
    """
    print("LLM prompt built.") # DEBUG
    return prompt_template.strip()

def call_azure_openai(prompt, temperature=0.7, max_tokens=500):
    print("Calling Azure OpenAI...") # DEBUG
    if not AZURE_OPENAI_KEY or not AZURE_OPENAI_ENDPOINT or not AZURE_OPENAI_DEPLOYMENT_NAME:
        print("Azure OpenAI configuration missing.") # DEBUG
        return None
    try:
        response = AZURE_OPENAI_CLIENT.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[
                {"role": "user", "content": prompt}
            ],
            temperature=temperature,
            max_tokens=max_tokens
        )
        print("Azure OpenAI response received.") # DEBUG
        return response.choices[0].message.content
    except Exception as e:
        print(f"Azure OpenAI API Error: {e}") # DEBUG
        return None

# --- The Core Bot Logic Handler ---
async def message_handler(turn_context: TurnContext):
    print("Entered message_handler...")
    user_query = turn_context.activity.text
    print(f"user_query: {user_query}")
    teams_user_id = turn_context.activity.from_property.id
    print(f"teams_user_id: {teams_user_id}")
    final_answer = ""
    conn = None

    try:
        try:
            print("Connecting to Snowflake...")
            conn = snowflake.connector.connect(
                user=SNOWFLAKE_USER,
                password=SNOWFLAKE_PASSWORD,
                account=SNOWFLAKE_ACCOUNT,
                warehouse=SNOWFLAKE_WAREHOUSE,
                database=SNOWFLAKE_DATABASE,
                schema=SNOWFLAKE_SCHEMA
            )
            print("Connected to Snowflake.")
        except Exception as e:
            print(f"Snowflake Connection Error: {e}")
            final_answer = "Sorry, I'm unable to connect to the data source right now. Please try again later."
            await turn_context.send_activity(Activity(text=final_answer, type=ActivityTypes.message))
            return # Exit if connection fails

        try:
            print("Fetching RBAC and Territory Data from Snowflake...")
            rbac_df = get_rbac_table(conn)
            territory_keys = get_sales_territory_keys(conn)
            location_to_territory_map = create_location_to_territory_mapping(rbac_df, territory_keys)
            print("Successfully fetched RBAC and Territory data.")
        except Exception as e:
            print(f"Error fetching Snowflake data: {e}")
            final_answer = "Sorry, there was an issue retrieving user access data. Please contact support."
            await turn_context.send_activity(Activity(text=final_answer, type=ActivityTypes.message))
            return # Exit if data fetching fails

        # --- HIGHLIGHT: Replaced the user_id_mapping with a static user for testing ---
        rbac_user_id = "victor"
        print("Using static RBAC user_id for testing:", rbac_user_id)

        access = get_user_access(rbac_user_id, rbac_df)
        print(f"access: {access}")

        if not access:
            print("Access information not found in RBAC table.")
            final_answer = "Sorry, your access information could not be found in the RBAC table."
        else:
            try:
                print("Calling find_measure_with_llm...")
                measure = find_measure_with_llm(user_query, KNOWLEDGE_BASE_DATA)
                print(f"measure: {measure}")
            except Exception as e:
                print(f"Error in find_measure_with_llm: {e}")
                final_answer = "Sorry, I'm having trouble understanding your request right now."
                await turn_context.send_activity(Activity(text=final_answer, type=ActivityTypes.message))
                return # Exit if LLM call fails

            if measure:
                try:
                    print("Calling get_metric_value...")
                    sf_data = get_metric_value(conn, measure, access['store_id'], location_to_territory_map)
                    print(f"sf_data: {sf_data}")
                    llm_prompt = build_llm_prompt(
                        {"user_id": rbac_user_id},
                        access,
                        user_query,
                        measure,
                        sf_data=sf_data,
                        location_to_territory_map=location_to_territory_map
                    )
                    print(f"llm_prompt: {llm_prompt[:200]}...")
                    
                    print("Calling call_azure_openai...")
                    final_answer = call_azure_openai(llm_prompt)
                    print(f"final_answer: {final_answer}")
                except Exception as e:
                    print(f"Error getting metric value or calling Azure OpenAI: {e}")
                    final_answer = "Sorry, an internal error occurred while retrieving and processing your data."
            else:
                print("Measure not found.")
                final_answer = "Sorry, I can't find that measure. Please ask about sales, traffic, or basket size."
            
    except Exception as e:
        # This is the outer catch-all. If this is hit, something unexpected happened.
        print(f"Unhandled error in message_handler outer: {e}")
        final_answer = "Sorry, an internal error occurred while processing your request. Please try again later."
    finally:
        if conn:
            conn.close()
            print("Snowflake connection closed.")

    await turn_context.send_activity(Activity(text=final_answer, type=ActivityTypes.message))
    print("Activity sent to bot.")


# --- The Main Azure Functions Entry Point ---
def main(req: func.HttpRequest) -> func.HttpResponse:
    print('Python HTTP trigger function received a request.') # DEBUG

    if req.method == 'GET':
        return func.HttpResponse("Bot endpoint is healthy", status_code=200)

    if req.method != 'POST':
        return func.HttpResponse("Only POST requests are supported for bot messages.", status_code=405)

    try:
        req_json = req.get_json()
        print(f"Request JSON: {req_json}") # DEBUG

    except ValueError:
        print("HTTP request does not contain valid JSON data. This may be a non-bot request.")
        return func.HttpResponse(
            "Please pass a valid JSON activity in the request body.",
            status_code=400
        )
    except Exception as error:
        print(f"Failed to get JSON from request: {error}")
        return func.HttpResponse(
            f"An error occurred while parsing the request body: {error}",
            status_code=500
        )

    activity = Activity.deserialize(req_json)
    auth_header = req.headers.get('Authorization') or req.headers.get('authorization') or ''

    APP_ID = os.environ.get("MicrosoftAppId", "")
    APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
    APP_TYPE = os.environ.get("MicrosoftAppType", "")
    APP_TENANT_ID = os.environ.get("MicrosoftAppTenantId", "")

    try:
        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            credentials = SingleTenantAppCredentials(APP_ID, APP_PASSWORD, APP_TENANT_ID)
            print("Using SingleTenantAppCredentials")
        else:
            credentials = MicrosoftAppCredentials(APP_ID, APP_PASSWORD)
            print("Using standard MicrosoftAppCredentials")

        settings = BotFrameworkAdapterSettings(
            app_id=APP_ID,
            app_password=APP_PASSWORD
        )

        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            settings.oauth_endpoint = f"https://login.microsoftonline.com/{APP_TENANT_ID}"
            print(f"Set OAuth authority to: {settings.oauth_endpoint}")

        adapter = BotFrameworkAdapter(settings)
        adapter._credentials = credentials
        
        print("Adapter created.")

    except Exception as adapter_error:
        print(f"Failed to create adapter: {adapter_error}")
        return func.HttpResponse("Adapter creation failed.", status_code=500)

    try:
        asyncio_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(asyncio_loop)

        response = asyncio_loop.run_until_complete(
            adapter.process_activity(
                activity,
                auth_header,
                message_handler
            )
        )

        if response:
            return func.HttpResponse(
                body=response.body,
                mimetype=response.content_type,
                status_code=response.status
            )
        else:
            return func.HttpResponse(status_code=202)

    except Exception as process_error:
        print(f"Error processing bot request: {process_error}")
        return func.HttpResponse(
            "An error occurred while processing the request.",
            status_code=500
        )

