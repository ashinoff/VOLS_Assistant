import os
from flask import Flask, request
from telegram import Update, Bot
from telegram.ext import (
    ApplicationBuilder, CommandHandler,
    ConversationHandler, MessageHandler, filters
)
from handlers.start import start
from handlers.search_tp import search_tp_start, search_tp_query, search_tp_choose, SEARCH_TP, CHOOSE_TP
from config import TOKEN, PORT, SELF_URL

app = Flask(__name__)

application = ApplicationBuilder().token(TOKEN).build()

# Хэндлеры
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

# --- WEBHOOK ROUTE ---
@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put(update)
    return "ok"

@app.route("/ping")
def ping():
    return "pong"

if __name__ == "__main__":
    # Устанавливаем webhook для Telegram (один раз или на каждый запуск)
    bot = Bot(TOKEN)
    webhook_url = f"{SELF_URL}/webhook"
    bot.delete_webhook()  # на всякий случай удаляем старый
    bot.set_webhook(url=webhook_url)

    print(f"Webhook set: {webhook_url}")

    # Запускаем Flask (webhook будет слушать POST-запросы)
    app.run(host="0.0.0.0", port=PORT)
