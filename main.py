import os
import asyncio
from flask import Flask, request
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ConversationHandler, filters
)
from handlers.start import start
from handlers.search_tp import (
    search_tp_start, search_tp_query, search_tp_choose,
    SEARCH_TP, CHOOSE_TP
)
from config import TOKEN, PORT, SELF_URL

app = Flask(__name__)
WEBHOOK_PATH = "/webhook"
application = None

def setup_app():
    global application
    application = ApplicationBuilder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^Поиск по ТП$"), search_tp_start)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp_query)],
            CHOOSE_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp_choose)],
        },
        fallbacks=[]
    )
    application.add_handler(conv_handler)

setup_app()

async def on_startup():
    await application.initialize()
    await application.bot.delete_webhook()
    await application.bot.set_webhook(url=SELF_URL + WEBHOOK_PATH)
    print(f"Webhook set: {SELF_URL + WEBHOOK_PATH}")

@app.route(WEBHOOK_PATH, methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    # Костыль: отдельный event loop для каждого потока Flask
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(application.process_update(update))
    loop.close()
    return "OK"

@app.route("/")
def index():
    return "VOLS Assistant TG Bot is running!"

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(on_startup())
    app.run(host="0.0.0.0", port=PORT)
