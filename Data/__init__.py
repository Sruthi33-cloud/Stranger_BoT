import azure.functions as func
import os
import logging
import sys
from botbuilder.core import BotFrameworkAdapter, BotFrameworkAdapterSettings, TurnContext, MessageFactory
from botbuilder.schema import ActivityTypes

# Setup logging
logger = logging.getLogger("azure")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
if not logger.hasHandlers():
    logger.addHandler(handler)

# Get bot credentials from environment variables
# These must be set in your Azure Function App's "Configuration" tab
APP_ID = os.environ.get("MicrosoftAppId", "")
APP_PASSWORD = os.environ.get("MicrosoftAppPassword", "")

# The bot's logic
async def bot_logic(turn_context: TurnContext):
    if turn_context.activity.type == ActivityTypes.message:
        user_message = turn_context.activity.text or ""
        logger.info(f"Received message: {user_message}")
        
        # The line that sends the echo reply
        response_text = f"Echo: {user_message}"
        await turn_context.send_activity(MessageFactory.text(response_text))
        logger.info(f"Sent response: {response_text}")

    elif turn_context.activity.type == ActivityTypes.members_added:
        for member in turn_context.activity.members_added:
            if member.id != turn_context.activity.recipient.id:
                await turn_context.send_activity("Hello! I'm an echo bot.")
                logger.info("Sent welcome message to new member")

# Create the adapter with the correct settings
SETTINGS = BotFrameworkAdapterSettings(APP_ID, APP_PASSWORD)
ADAPTER = BotFrameworkAdapter(SETTINGS)

async def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logger.info(f"Processing bot request... Method: {req.method}")

        if req.method == 'GET':
            return func.HttpResponse("Bot endpoint is healthy", status_code=200)

        if req.method != 'POST':
            return func.HttpResponse("Only POST requests are supported", status_code=405)

        # Process the incoming request
        body = req.get_json()
        auth_header = req.headers.get('Authorization') or req.headers.get('authorization')
        
        # This is the key line that processes the activity and runs your bot logic
        invoke_response = await ADAPTER.process_activity(
            body,
            auth_header,
            bot_logic
        )
        
        if invoke_response:
            return func.HttpResponse(
                body=invoke_response.body,
                status_code=invoke_response.status,
                headers={"Content-Type": "application/json"}
            )
        else:
            return func.HttpResponse(status_code=202)

    except Exception as error:
        logger.error(f"Unhandled error: {str(error)}", exc_info=True)
        return func.HttpResponse(f"Internal server error: {str(error)}", status_code=500)
