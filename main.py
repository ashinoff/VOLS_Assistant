import os
from flask import Flask, request
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    ConversationHandler, MessageHandler, filters
)
from handlers.start import start
from handlers.search_tp import search_tp_start, search_tp_query, search_tp_choose, SEARCH_TP, CHOOSE_TP
from utils.autoping import start_ping
from config import TOKEN, PORT

app = Flask(__name__)

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

@app.route("/ping")
def ping():
    return "pong"

# Запускаем автопинг
start_ping()

if __name__ == "__main__":
    application.run_polling()
    app.run(host="0.0.0.0", port=PORT)
