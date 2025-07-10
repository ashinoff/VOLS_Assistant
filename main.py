import os
import threading
from flask import Flask, request

from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    ConversationHandler, MessageHandler, filters
)
from handlers.start import start
from handlers.search_tp import search_tp_start, search_tp_query, search_tp_choose, SEARCH_TP, CHOOSE_TP
from utils.autoping import start_ping
from config import TOKEN, PORT

# --- Flask сервер ---
app = Flask(__name__)

@app.route("/")
def root():
    return "VOLS Assistant: online!"

@app.route("/ping")
def ping():
    return "pong"

def run_flask():
    # Запуск Flask на порту из ENV
    app.run(host="0.0.0.0", port=PORT)

# --- Telegram polling-бот ---
def run_bot():
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

    # Запускаем автопинг
    start_ping()

    application.run_polling()

if __name__ == "__main__":
    # Flask сервер в отдельном потоке (чтобы Render видел порт)
    threading.Thread(target=run_flask, daemon=True).start()
    # Бот (polling) — в основном потоке
    run_bot()
