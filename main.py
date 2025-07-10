import os
import asyncio
from flask import Flask, request
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    ConversationHandler, MessageHandler, ContextTypes, filters
)
from handlers.start import start
from handlers.search_tp import search_tp_start, search_tp_query, search_tp_choose, SEARCH_TP, CHOOSE_TP
from config import TOKEN, PORT, SELF_URL

app = Flask(__name__)
application = None

@app.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        update = Update.de_json(request.get_json(force=True), application.bot)
        asyncio.run(application.process_update(update))
        return "ok"
    return "method not allowed", 405

@app.route("/ping")
def ping():
    return "pong"

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

async def set_webhook():
    bot = Bot(TOKEN)
    webhook_url = f"{SELF_URL}/webhook"
    await bot.delete_webhook()
    await bot.set_webhook(url=webhook_url)
    print(f"Webhook set: {webhook_url}")

if __name__ == "__main__":
    setup_app()
    # Устанавливаем webhook (асинхронно)
    asyncio.run(set_webhook())
    # Запускаем Flask
    app.run(host="0.0.0.0", port=PORT)
