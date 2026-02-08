# -*- coding: utf-8 -*-
"""
Main bot entry point.
"""
import os
import sys
import logging
import asyncio

from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler, CallbackQueryHandler

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config import config
from bot.handlers import (
    start_command, text_handler, callback_handler,
    show_main_menu
)


# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Check bot token
if not config.BOT_TOKEN:
    logger.error("BOT_TOKEN not set! Please set the BOT_TOKEN environment variable.")
    sys.exit(1)

# Create bot client
bot = Client(
    name="inviter_bot",
    bot_token=config.BOT_TOKEN,
    api_id=config.API_ID or 1,  # Dummy for bot
    api_hash=config.API_HASH or "dummy"
)



# Define admin filter
admin_filter = filters.user(config.ADMIN_IDS)

# ============== Handlers ==============

@bot.on_message(filters.command("start") & filters.private & admin_filter)
async def start_handler(client, message):
    """Handle /start command."""
    await start_command(client, message)


@bot.on_message(filters.command("sessions") & filters.private & admin_filter)
async def sessions_handler(client, message):
    """Handle /sessions command."""
    from bot.session_handlers import sessions_command
    await sessions_command(client, message)


@bot.on_message(filters.command("status") & filters.private & admin_filter)
async def status_handler(client, message):
    """Handle /status command."""
    from bot.handlers import show_tasks_status
    await show_tasks_status(client, message)


@bot.on_message(filters.text & filters.private & admin_filter)
async def text_message_handler(client, message):
    """Handle all text messages."""
    # Skip commands
    if message.text.startswith('/'):
        return
    
    await text_handler(client, message)


@bot.on_callback_query(admin_filter)
async def callback_query_handler(client, callback_query):
    """Handle all callback queries."""
    try:
        await callback_handler(client, callback_query)
    except Exception as e:
        logger.error(f"Error handling callback query: {e}", exc_info=True)
        # Try to answer the callback query if it's still valid
        try:
            await callback_query.answer("❌ Произошла ошибка при обработке запроса", show_alert=True)
        except Exception:
            pass  # Query expired or already answered, ignore


# ============== Main ==============

def main():
    """Run the bot."""
    logger.info("Starting Inviter Bot...")
    logger.info(f"Parser service URL: {config.PARSER_SERVICE_URL}")
    
    try:
        bot.run()
    except KeyboardInterrupt:
        logger.info("Bot stopped by user")
    except Exception as e:
        logger.error(f"Bot error: {e}", exc_info=True)


if __name__ == "__main__":
    main()
