import os
import threading
from flask import Flask
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    ConversationHandler, MessageHandler, filters
)
from handlers.start import start
from handlers.search_tp import search_tp_start, search_tp_query, search_tp_choose, SEARCH_TP, CHOOSE_TP
from utils.autoping import start_ping
from config import TOKEN, PORT

# Flask для самопинга
flask_app = Flask(__name__)

@flask_app.route("/ping")
def ping():
    return "pong"

# Основная функция для Telegram polling
def run_polling():
    application = ApplicationBuilder().token(TOKEN).build()

    # Start command
    application.add_handler(CommandHandler("start", start))

    # Conversation handler
    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.Regex("^Поиск по ТП$"), search_tp_start)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp_query)],
            CHOOSE_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp_choose)],
        },
        fallbacks=[]
    )
    application.add_handler(conv_handler)

    # Запускаем polling
    application.run_polling()

if __name__ == "__main__":
    # Запускаем автопинг (если используешь для Render)
    start_ping()

    # Polling запускаем в отдельном потоке
    threading.Thread(target=run_polling, daemon=True).start()
    # Flask — в главном потоке
    flask_app.run(host="0.0.0.0", port=PORT)
