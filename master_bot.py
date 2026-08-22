#!/usr/bin/env python3
"""
Master Arbitrage Bot — Tether + Gold
Telegram: @chiszegbot
Deploy: Render.com Web Service
"""

import asyncio
import logging
import os
import sys

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes

# Import gold module
import gold_bot

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("master_bot")

# ---------------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------------

TELEGRAM_TOKEN = "8268413497:AAGp50wjtVTMlpM5nGpB80NOB53WWgR7AGE"
CHAT_ID = "7130660440"
PORT = int(os.environ.get("PORT", 10000))

# ---------------------------------------------------------------------------
# TELEGRAM HANDLERS
# ---------------------------------------------------------------------------

async def cmd_gold(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /gold command."""
    await update.message.reply_text("🟡 در حال دریافت قیمت طلا...")
    await gold_bot.handle_gold_command(context.application, update.message)

async def cmd_gold_alert(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /gold_alert command."""
    await gold_bot.handle_gold_alert_command(context.application, update.message)

async def cmd_gold_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle /gold_help command."""
    await gold_bot.handle_gold_help_command(context.application, update.message)

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🟡 خوش آمدید!\n\n"
        "دستورات:\n"
        "/gold — وضعیت بازار طلا\n"
        "/gold_help — راهنما\n\n"
        "⚠️ ربات تتر به زودی اضافه میشود."
    )

# ---------------------------------------------------------------------------
# HTTP SERVER (Render health check)
# ---------------------------------------------------------------------------

from aiohttp import web

async def health_handler(request):
    return web.json_response({"status": "ok", "service": "master-arbitrage-bot"})

def create_web_app():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    return app

# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------

async def main():
    logger.info("Starting master arbitrage bot...")

    # Build Telegram app
    app = Application.builder().token(TELEGRAM_TOKEN).build()

    # Register gold command handlers
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("gold", cmd_gold))
    app.add_handler(CommandHandler("gold_alert", cmd_gold_alert))
    app.add_handler(CommandHandler("gold_help", cmd_gold_help))

    # Start gold monitoring in background
    await gold_bot.start_gold_service(app, CHAT_ID)

    # Start Telegram polling (non-blocking)
    await app.initialize()
    await app.start()
    logger.info("Telegram polling started")

    # Start HTTP server for Render health checks
    web_app = create_web_app()
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health server on port {PORT}")

    # Keep alive
    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("Shutting down...")
        gold_bot.state.running = False
        await app.stop()
        await app.shutdown()
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())