import azure.functions as func
import os
import asyncio
import base64
import json
import jwt
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext
from botbuilder.schema import Activity, ActivityTypes
from botframework.connector.auth import JwtTokenValidation, SkillValidation
import logging
import sys
import aiohttp

# Setup logging
logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("[%(levelname)s] %(asctime)s - %(message)s")
handler.setFormatter(formatter)
if not logger.hasHandlers():
    logger.addHandler(handler)

# Get environment variables
APP_ID = os.environ.get("MicrosoftAppId", "")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")
APP_TYPE = os.environ.get("MicrosoftAppType", "SingleTenant")
APP_TENANT_ID = os.environ.get("MicrosoftAppTenantId", "")

logger.info(f"Bot configured: AppId={APP_ID[:8]}..., Type={APP_TYPE}")

def decode_jwt_payload(token):
    """Decode JWT payload without verification for debugging"""
    try:
        if not token or not token.startswith('Bearer '):
            return None
            
        jwt_token = token.replace('Bearer ', '')
        
        # Decode header and payload (without verification)
        header = jwt.get_unverified_header(jwt_token)
        payload = jwt.decode(jwt_token, options={"verify_signature": False})
        
        return {
            "header": header,
            "payload": payload
        }
    except Exception as e:
        logger.error(f"Error decoding JWT: {e}")
        return None

async def diagnose_token_issue(activity, auth_header):
    """Diagnose token validation issues"""
    logger.info("=== DIAGNOSTIC ANALYSIS ===\n")
    
    # 1. Check incoming auth header
    if auth_header:
        logger.info(f"Auth header present: True")
        decoded = decode_jwt_payload(auth_header)
        if decoded:
            logger.info(f"Incoming token audience: {decoded['payload'].get('aud')}")
            logger.info(f"Incoming token issuer: {decoded['payload'].get('iss')}")
            logger.info(f"Incoming token app id: {decoded['payload'].get('appid')}")
            logger.info(f"Incoming token service url: {decoded['payload'].get('serviceurl')}")
    else:
        logger.info("Auth header present: False")
    
    # 2. Check service URL
    logger.info(f"Activity service URL: {activity.service_url}")
    
    # 3. Check our app credentials
    logger.info(f"Our App ID: {APP_ID}")
    logger.info(f"Our App Type: {APP_TYPE}")
    
    # 4. Try to get our own token for comparison
    try:
        token_url = f"https://login.microsoftonline.com/{APP_TENANT_ID}/oauth2/v2.0/token" if APP_TYPE == "SingleTenant" else "https://login.microsoftonline.com/botframework.com/oauth2/v2.0/token"
        data = {
            'grant_type': 'client_credentials',
            'client_id': APP_ID,
            'client_secret': APP_PASSWORD,
            'scope': 'https://api.botframework.com/.default'
        }
        
        async with aiohttp.ClientSession() as session:
            async with session.post(token_url, data=data) as response:
                if response.status == 200:
                    token_response = await response.json()
                    decoded_our = decode_jwt_payload(f"Bearer {token_response.get('access_token')}")
                    if decoded_our:
                        logger.info("Successfully got our access token")
                        logger.info(f"Our token audience: {decoded_our['payload'].get('aud')}")
                        logger.info(f"Our token issuer: {decoded_our['payload'].get('iss')}")
                        logger.info(f"Our token app id: {decoded_our['payload'].get('appid')}")
                else:
                    error_text = await response.text()
                    logger.error(f"Failed to get our token: {response.status} - {error_text}")
                    
    except Exception as e:
        logger.error(f"Exception getting our token: {e}")
    
    logger.info("=== END DIAGNOSTIC ===")

async def bot_logic(turn_context: TurnContext):
    try:
        if turn_context.activity.type == ActivityTypes.message:
            user_message = turn_context.activity.text or ""
            logger.info(f"Received message: {user_message}")
            
            response_text = f"Echo: {user_message}"
            
            await turn_context.send_activity(response_text)
            logger.info(f"Sent response: {response_text}")
        elif turn_context.activity.type == "membersAdded":
            # Handle new members
            if hasattr(turn_context.activity, 'members_added') and turn_context.activity.members_added:
                for member in turn_context.activity.members_added:
                    if member.id != turn_context.activity.recipient.id:
                        await turn_context.send_activity("Hello! I'm an echo bot. Send me a message!")
                        logger.info("Sent welcome message to new member")
    except Exception as e:
        logger.error(f"Error in bot_logic: {str(e)}", exc_info=True)
        raise

async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logger.info(f"Processing bot request... Method: {req.method}")
        
        if req.method == 'GET':
            return func.HttpResponse("Bot endpoint is healthy", status_code=200)
        
        if req.method != 'POST':
            return func.HttpResponse("Only GET and POST requests are supported", status_code=405)
        
        body = req.get_json()
        if not body:
            return func.HttpResponse("Request body is required", status_code=400)
        
        activity = Activity.deserialize(body)
        auth_header = req.headers.get('Authorization') or req.headers.get('authorization') or ''

        # Create settings and adapter per request
        settings = BotFrameworkAdapterSettings(
            app_id=APP_ID,
            app_password=APP_PASSWORD,
            tenant_id=APP_TENANT_ID if APP_TYPE == "SingleTenant" else None
        )
        
        adapter = BotFrameworkAdapter(settings)
        
        logger.info(f"Activity type: {activity.type}, Channel: {activity.channel_id}")
        logger.info(f"Service URL: {activity.service_url}")
        
        # Diagnostic
        await diagnose_token_issue(activity, auth_header)
        
        # Trust the service URL
        await adapter.trust_service_url(activity.service_url)
        
        # Process the activity
        invoke_response = await adapter.process_activity(activity, auth_header, bot_logic)
        
        if invoke_response:
            return func.HttpResponse(
                body=invoke_response.body,
                status_code=invoke_response.status,
                headers={"Content-Type": "application/json"}
            )
        else:
            return func.HttpResponse(status_code=202)
            
    except Exception as error:
        logger.error(f"Unhandled error in main: {str(error)}", exc_info=True)
        return func.HttpResponse(f"Internal server error: {str(error)}", status_code=500)