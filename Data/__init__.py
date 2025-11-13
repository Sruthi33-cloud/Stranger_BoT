import logging
import os
import json
import azure.functions as func
import sys
import asyncio
from typing import Dict, List, Optional, Any
import re
import traceback

from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity, ActivityTypes
from botframework.connector.auth import MicrosoftAppCredentials

import pandas as pd
import snowflake.connector
from openai import AzureOpenAI

# Robust logging configuration for Azure Functions
LOG_FORMAT = "[%(levelname)s] %(asctime)s - %(name)s - %(message)s"
# Ensure root logger writes to stdout (Functions captures stdout)
root = logging.getLogger()
if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    root.addHandler(stream_handler)
root.setLevel(logging.INFO)

logger = logging.getLogger("azure_bot_message_handler")
logger.setLevel(logging.INFO)

# Single-tenant auth
class SingleTenantAppCredentials(MicrosoftAppCredentials):
    def __init__(self, app_id: str, password: str, tenant_id: str):
        super().__init__(app_id, password)
        self.tenant_id = tenant_id
        self.oauth_endpoint = f"https://login.microsoftonline.com/{tenant_id}"

# Env vars (Ensure all these are set in your Azure Function App Settings!)
APP_ID = os.environ.get("MicrosoftAppId", "")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
APP_TYPE = os.environ.get("MicrosoftAppType", "SingleTenant")
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

# Log env var presence (do NOT log secrets)
logger.info(f"Env check - MicrosoftAppId set: {bool(APP_ID)}; App Type: {APP_TYPE}; Tenant set: {bool(APP_TENANT_ID)}")
logger.info(f"Env check - Snowflake set: {all([SNOWFLAKE_USER, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA])}")
logger.info(f"Env check - AppInsights present: {bool(os.environ.get('APPLICATIONINSIGHTS_CONNECTION_STRING') or os.environ.get('APPINSIGHTS_INSTRUMENTATIONKEY'))}")

# OpenAI client
AZURE_OPENAI_CLIENT = None
try:
    if AZURE_OPENAI_KEY and AZURE_OPENAI_ENDPOINT and AZURE_OPENAI_DEPLOYMENT_NAME:
        AZURE_OPENAI_CLIENT = AzureOpenAI(
            api_key=AZURE_OPENAI_KEY,
            azure_endpoint=AZURE_OPENAI_ENDPOINT,
            api_version="2025-01-01-preview"
        )
        logger.info("AzureOpenAI client initialized.")
    else:
        logger.warning("Azure OpenAI environment variables are missing. LLM fallback disabled.")
except Exception:
    logger.exception("Error initializing AzureOpenAI client")
    AZURE_OPENAI_CLIENT = None

# SOLUTION: PURELY ROBUST DATA-TYPE-BASED COLUMN DETECTION
_column_cache = {}

def get_table_columns(conn, table_name: str) -> Dict[str, str]:
    """Get column names and data types dynamically - cached for performance"""
    upper_table_name = table_name.upper()
    if upper_table_name in _column_cache:
        return _column_cache[upper_table_name]

    try:
        cur = conn.cursor()
        cur.execute(f"""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{SNOWFLAKE_SCHEMA}'
        AND TABLE_NAME = '{upper_table_name}'
        ORDER BY ORDINAL_POSITION
        """)

        columns = {}
        for row in cur.fetchall():
            col_name = row[0]
            data_type = row[1].upper()
            columns[col_name] = data_type

        cur.close()
        _column_cache[upper_table_name] = columns
        logger.info(f"Cached columns for {upper_table_name}: {[(name, dtype) for name, dtype in columns.items()]}")
        return columns

    except Exception:
        logger.exception(f"Error getting columns for {upper_table_name}")
        return {}

def find_column_by_data_type(columns: Dict[str, str], data_type_patterns: List[str]) -> Optional[str]:
    for col_name, data_type in columns.items():
        for pattern in data_type_patterns:
            if pattern.upper() in data_type.upper():
                logger.info(f"Found column '{col_name}' with data type '{data_type}' matching pattern '{pattern}'")
                return col_name
    return None

class RobustQueryBuilder:
    def __init__(self, conn):
        self.conn = conn
        self.sales_fact_cols = get_table_columns(conn, 'SALES_FACT')
        self.rbac_cols = get_table_columns(conn, 'RBAC_WORK_TABLE')

    def build_sales_query(self, user_id: str) -> Optional[str]:
        sales_amount_col = find_column_by_data_type(self.sales_fact_cols,
            ['NUMBER', 'DECIMAL', 'FLOAT', 'NUMERIC', 'REAL', 'DOUBLE']
        )
        sales_date_col = find_column_by_data_type(self.sales_fact_cols,
            ['DATE', 'TIMESTAMP', 'TIME', 'DATETIME']
        )
        rbac_date_col = find_column_by_data_type(self.rbac_cols,
            ['DATE', 'TIMESTAMP', 'TIME', 'DATETIME']
        )
        rbac_user_col = find_column_by_data_type(self.rbac_cols, 
            ['VARCHAR', 'TEXT', 'STRING', 'CHAR']
        )

        if not sales_amount_col:
            logger.error("No numeric (sales) column found in SALES_FACT table")
            return None
        if not sales_date_col:
            logger.error("No date column found in SALES_FACT table for join")
            return None
        if not rbac_date_col:
            logger.error("No date column found in RBAC_WORK_TABLE for join")
            return None
        if not rbac_user_col:
            logger.error("No user ID column found in RBAC_WORK_TABLE for filtering")
            return None

        DB_SCHEMA_SALES = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.SALES_FACT"
        DB_SCHEMA_RBAC = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RBAC_WORK_TABLE"

        query = f"""
        SELECT COALESCE(SUM(t1.{sales_amount_col}), 0) 
        FROM {DB_SCHEMA_SALES} t1
        JOIN {DB_SCHEMA_RBAC} t2
        ON t1.{sales_date_col} = t2.{rbac_date_col}
        WHERE t2.{rbac_user_col} = '{user_id}';
        """

        logger.info(f"Built adaptive query: {query}")
        logger.info(f" Amount column: {sales_amount_col}")
        logger.info(f" Sales date column: {sales_date_col}")
        logger.info(f" RBAC date column: {rbac_date_col}")
        logger.info(f" RBAC User column: {rbac_user_col}")

        return query

# Knowledge base loading unchanged except improved exception logging
def load_knowledge_base():
    try:
        path = os.path.join(os.path.dirname(__file__), "knowledge_base.json")
        if not os.path.exists(path):
            raise FileNotFoundError(f"knowledge_base.json not found at {path}")

        with open(path, "r") as f:
            return json.load(f)
    except Exception:
        logger.exception("Error loading knowledge_base.json, using default KB")
        return [
            {
                "measure_name": "Sales Amount",
                "tool_name": "sales_amount",
                "description": "The total monetary value of all sales transactions. This is a key measure for a store's financial performance.",
                "dax_formula": "SUM('SalesFact'[SalesAmount])",
                "aliases": ["sales", "sales revenue", "total sales", "store sales"]
            },
            {
                "measure_name": "Traffic Conversion",
                "tool_name": "traffic_conversion", 
                "description": "Calculated as Total Sales Transactions divided by Total Store Visits.",
                "dax_formula": "[Total Transactions] / [Store Visits]",
                "aliases": ["conversion rate", "traffic to sales", "customer conversion"]
            }
        ]

KNOWLEDGE_BASE_DATA = load_knowledge_base()
TOOL_NAME_TO_MEASURE = {item['tool_name']: item for item in KNOWLEDGE_BASE_DATA}
ALIAS_TO_TOOL_NAME = {}
for item in KNOWLEDGE_BASE_DATA:
    for alias in item['aliases']:
        ALIAS_TO_TOOL_NAME[alias.lower()] = item['tool_name']

_intent_cache = {}

def get_user_data(user_id: str, conn) -> Optional[Dict[str, Any]]:
    query = f"SELECT USER_ID, ROLE, STORE_ID FROM {SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.RBAC_WORK_TABLE WHERE USER_ID = '{user_id.upper()}'"
    try:
        cur = conn.cursor()
        cur.execute(query)
        df = cur.fetch_pandas_all()
        cur.close()
        df.columns = df.columns.str.lower()
        user_row = df[df['user_id'].str.lower() == user_id.lower()]
        if user_row.empty:
            logger.error(f"User ID {user_id} not found in RBAC table.")
            return None
        return {"role": user_row.iloc[0]['role'],
                "store_id": user_row.iloc[0]['store_id']
                }
    except Exception:
        logger.exception("Error getting user data")
        return None

def get_available_metrics_list() -> str:
    metrics = [item['measure_name'] for item in KNOWLEDGE_BASE_DATA]
    if len(metrics) <= 2:
        return " and ".join(metrics)
    else:
        return ", ".join(metrics[:-1]) + f", and {metrics[-1]}"

def identify_metric_intent(user_query: str) -> Optional[str]:
    query_key = user_query.lower().strip()
    if query_key in _intent_cache:
        logger.info(f"Cache hit for query: '{user_query}' -> {_intent_cache[query_key]}")
        return _intent_cache[query_key]

    query_lower = user_query.lower()
    logger.info(f"Intent recognition for query: '{user_query}'")

    unrelated_patterns = ["how are you", "what's your name", "weather", "time",
                          "joke", "story", "recipe", "news", "sports", "music"]

    for pattern in unrelated_patterns:
        if pattern in query_lower and len(user_query.strip()) < 50:
            logger.info(f"Detected unrelated pattern: {pattern}")
            _intent_cache[query_key] = "UNRELATED"
            return "UNRELATED"

    traffic_keywords = ["traffic", "conversion", "convert", "visits"]
    for keyword in traffic_keywords:
        if keyword in query_lower:
            logger.info(f"Traffic keyword match found: {keyword}")
            _intent_cache[query_key] = "traffic_conversion"
            return "traffic_conversion"

    sales_keywords = ["sales", "revenue", "amount", "money", "dollar"]
    for keyword in sales_keywords:
        if keyword in query_lower:
            logger.info(f"Sales keyword match found: {keyword}")
            _intent_cache[query_key] = "sales_amount"
            return "sales_amount"

    for alias, tool_name in ALIAS_TO_TOOL_NAME.items():
        if alias in query_lower:
            logger.info(f"Alias match found: {alias} -> {tool_name}")
            _intent_cache[query_key] = tool_name
            return tool_name

    if not AZURE_OPENAI_CLIENT:
        _intent_cache[query_key] = None
        return None

    prompt = f"'{user_query}' sales or traffic?"
    try:
        response = AZURE_OPENAI_CLIENT.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_completion_tokens=8
        )
        result = response.choices[0].message.content.strip().lower()
        logger.info(f"LLM result: {result}")
        if "traffic" in result:
            _intent_cache[query_key] = "traffic_conversion"
            return "traffic_conversion"
        elif "sales" in result:
            _intent_cache[query_key] = "sales_amount"
            return "sales_amount"
        else:
            _intent_cache[query_key] = "UNRELATED"
            return "UNRELATED"
    except Exception:
        logger.exception("LLM intent error")
        _intent_cache[query_key] = None
        return None

def get_metric_value_fast(conn, tool_name: str, store_id: int, user_id: str, query: str) -> Optional[float | str]:
    try:
        user_session = get_user_session(user_id, conn)
        if not user_session:
            return "Access denied"

        if store_id != user_session.store_id:
            access_denied_response = """Sorry I couldn't provide that information. I can only provide data for your assigned store (Store ID: {}).
            
            Examples of questions I can answer:
            • "What are my sales for this month?"
            • "How's the traffic conversion rate?"
            • "Show me the current sales amount"
            
            What would you like to know about your store performance?""".format(user_session.store_id)
            return access_denied_response

        if tool_name == "sales_amount":
            cur = conn.cursor()
            cur.execute(query)
            result = cur.fetchone()
            cur.close()
            return float(result[0]) if result and result[0] is not None else 0.0
        elif tool_name == "traffic_conversion":
            return 0.000
        return None
    except Exception:
        logger.exception("Metric query error")
        return "data_retrieval_failed"

def generate_rich_response(user_query: str, tool_name: str, metric_value: float, store_id: int) -> str:
    logger.info(f"Generating response for tool_name: {tool_name}, value: {metric_value}")
    measure_info = TOOL_NAME_TO_MEASURE.get(tool_name)
    if not measure_info:
        logger.error(f"No measure info found for tool_name: {tool_name}")
        return f"Metric not found for store {store_id}."

    if tool_name == "sales_amount":
        formatted_value = f"${metric_value:,.2f}"
    elif tool_name == "traffic_conversion":
        formatted_value = f"{metric_value:.3f}"
    else:
        formatted_value = f"{metric_value:.2f}"

    logger.info(f"Formatted value: {formatted_value}")
    measure_name = measure_info['measure_name']
    description = measure_info['description']

    base_response = f"For store {store_id}, **{measure_name}** is defined as {description.lower()}, and the current {measure_name} value is **{formatted_value}**."

    if tool_name == "sales_amount":
        suggestions = ' You might also ask: "How does this compare to last month?" or "What are my top performing products?"'
    elif tool_name == "traffic_conversion":
        suggestions = ' You might also ask: "What are the total store visits for my store?" or "How many sales transactions did we record to calculate this measure?"'
    else:
        suggestions = ' You might also ask: "How can I improve this metric?" or "What factors influence this measure?"'

    final_response = base_response + suggestions
    logger.info(f"Final response generated: {final_response[:100]}...")
    return final_response

class UserSession:
    def __init__(self, user_id: str, role: str, store_id: int):
        self.user_id = user_id
        self.role = role
        self.store_id = store_id
        self.last_queries = []

_user_sessions = {}

def get_user_session(user_id: str, conn) -> Optional[UserSession]:
    if user_id not in _user_sessions:
        user_data = get_user_data(user_id, conn)
        if user_data:
            _user_sessions[user_id] = UserSession(
                user_id, user_data["role"], user_data["store_id"]
            )
    return _user_sessions.get(user_id)

# Main message handler
async def message_handler(turn_context: TurnContext):
    if turn_context.activity.type != ActivityTypes.message:
        return

    user_query = (turn_context.activity.text or "").strip()
    # NOTE: Hardcoded user ID - replace this with dynamic extraction in production!
    user_id = "victor"

    if not user_query or len(user_query) > 300:
        await turn_context.send_activity("Please ask a specific question about store metrics.")
        return

    conn = None
    try:
        if not all([SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA]):
             logger.error("One or more Snowflake environment variables are missing.")
             await turn_context.send_activity("Database configuration is incomplete. Please contact support.")
             return

        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )

        session = get_user_session(user_id, conn)
        if not session:
            await turn_context.send_activity("Access denied. Could not find user data. Contact support.")
            return

        logger.info(f"Processing user query: '{user_query}'")
        tool_name = identify_metric_intent(user_query)
        logger.info(f"Identified tool_name: {tool_name}")

        if tool_name == "UNRELATED":
            available_metrics = get_available_metrics_list()
            unrelated_response = f"""Sorry, I couldn't provide you that information. Perhaps I can help you with these details:
            Available metrics for your store:
            • {available_metrics}
            Examples of questions I can answer:
            • "What are my sales for this month?"
            • "How's the traffic conversion rate?"
            • "Show me the current sales amount"
            What would you like to know about your store performance?"""
            await turn_context.send_activity(unrelated_response)
            return

        if not tool_name:
            available_metrics = get_available_metrics_list()
            no_match_response = f"""I'm not sure what metric you're asking about. I can help you with: {available_metrics}.
            Try asking:
            • "What are my current sales?"
            • "How's my traffic conversion?"
            • "Show me store performance data"
            What specific metric would you like to see?"""
            await turn_context.send_activity(no_match_response)
            return

        requested_store_id = session.store_id
        user_query_lower = user_query.lower()
        store_match = re.search(r'store\s*#?(\d+)', user_query_lower)
        if store_match:
            try:
                requested_store_id = int(store_match.group(1))
            except (ValueError, IndexError):
                pass

        query_builder = RobustQueryBuilder(conn)
        query = query_builder.build_sales_query(user_id)
        if query is None:
            await turn_context.send_activity("The requested data is not available because of missing schema metadata. Please check the database connection and table structures.")
            return

        metric_value = get_metric_value_fast(conn, tool_name, requested_store_id, user_id, query)
        logger.info(f"Retrieved metric_value: {metric_value} for tool_name: {tool_name}")

        if isinstance(metric_value, str):
            if metric_value == "data_retrieval_failed":
                await turn_context.send_activity("There was a problem retrieving data for that metric. Please check the Function logs for a detailed SQL error.")
            else:
                await turn_context.send_activity(metric_value)
            return

        if metric_value is None:
            await turn_context.send_activity(f"Cannot retrieve data for store {requested_store_id}.")
            return

        response = generate_rich_response(user_query, tool_name, metric_value, requested_store_id)
        await turn_context.send_activity(response)

        session.last_queries.append({"query": user_query, "metric": tool_name})
        if len(session.last_queries) > 3:
            session.last_queries.pop(0)

    except Exception:
        # Log full traceback to Application Insights / stdout
        logger.exception("FATAL ERROR in message_handler")
        await turn_context.send_activity("Service temporarily unavailable. A critical error occurred. Please try again.")
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                logger.exception("Error closing DB connection")

# Main function
def main(req: func.HttpRequest) -> func.HttpResponse:
    logger.info(f"Main entry - method={req.method}")
    if req.method == 'GET':
        return func.HttpResponse("Bot endpoint is healthy", status_code=200)

    if req.method != 'POST':
        return func.HttpResponse("Only POST requests supported.", status_code=405)

    try:
        try:
            req_json = req.get_json()
        except Exception:
            req_body = req.get_body().decode('utf-8', errors='ignore')
            logger.info("Could not json-decode body; logging raw body for debugging")
            logger.info(req_body)
            req_json = json.loads(req_body) if req_body else {}

        logger.info(f"Incoming activity payload keys: {list(req_json.keys())}")
        activity = Activity.deserialize(req_json)
        auth_header = req.headers.get('Authorization') or req.headers.get('authorization') or ''

        if APP_TYPE == "SingleTenant" and APP_TENANT_ID:
            credentials = SingleTenantAppCredentials(APP_ID, APP_PASSWORD, APP_TENANT_ID)
            settings = BotFrameworkAdapterSettings(app_id=APP_ID, app_password=APP_PASSWORD)
            settings.oauth_endpoint = f"https://login.microsoftonline.com/{APP_TENANT_ID}"
        else:
            credentials = MicrosoftAppCredentials(APP_ID, APP_PASSWORD)
            settings = BotFrameworkAdapterSettings(app_id=APP_ID, app_password=APP_PASSWORD)

        adapter = BotFrameworkAdapter(settings)
        adapter._credentials = credentials

        asyncio_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(asyncio_loop)

        # Wrap the adapter processing to capture exceptions to logs
        try:
            response = asyncio_loop.run_until_complete(
                adapter.process_activity(activity, auth_header, message_handler)
            )
            logger.info(f"Adapter processed activity. Response: {getattr(response, 'status', None)}")
        except Exception:
            logger.exception("Error during adapter.process_activity")
            raise

        return func.HttpResponse(
            body=response.body if response else "",
            status_code=response.status if response else 202
        )

    except Exception:
        logger.exception("Error processing request in main")
        return func.HttpResponse("Internal server error during request processing.", status_code=500)
