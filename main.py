import os
import logging
import requests
import pandas as pd
import io
from flask import Flask, request
from telegram import Update
from telegram.ext import (
    ApplicationBuilder, CommandHandler, ContextTypes
)

TOKEN = os.getenv("TOKEN", "YOUR_TOKEN")
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "https://docs.google.com/....")

# ---- ПОЛУЧЕНИЕ ТАБЛИЦЫ, ПРОВЕРКА СТОЛБЦОВ ----

def get_zones_df():
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    print("СТОЛБЦЫ CSV:", df.columns.tolist())
    return df

def get_user_rights(telegram_id: int):
    df = get_zones_df()
    print("ПРОВЕРКА ПЕРВЫХ СТРОК:", df.head(1).to_dict())
    try:
        user_row = df[df['Telegram ID'] == telegram_id]
    except KeyError as e:
        print("ОШИБКА: Нет колонки Telegram ID")
        return None
    if user_row.empty:
        return None
    row = user_row.iloc[0]
    # Безопасно получаем, даже если столбец пустой
    return {
        "zone": row.get('Видимость', ''),
        "filial": row.get('Филиал', ''),
        "res": row.get('РЭС', ''),
        "fio": row.get('ФИО', ''),
        "responsible": row.get('Ответственный', ''),
    }

# ---- TELEGRAM ----

application = ApplicationBuilder().token(TOKEN).build()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rights = get_user_rights(user_id)
    if not rights:
        await update.message.reply_text("Нет доступа.")
        return
    msg = f"Добро пожаловать, {rights['fio']}!\n" \
          f"Видимость: {rights['zone']}\n" \
          f"Филиал: {rights['filial']}\n" \
          f"РЭС: {rights['res']}"
    await update.message.reply_text(msg)

application.add_handler(CommandHandler("start", start))

# ---- FLASK ----

app = Flask(__name__)

@app.route("/")
def index():
    return "OK"

@app.route("/webhook", methods=["POST"])
def webhook():
    if request.method == "POST":
        update = Update.de_json(request.get_json(force=True), application.bot)
        application.create_task(application.process_update(update))
        return "ok"
    return "Only POST"

# ---- УСТАНОВКА ВЕБХУКА ----

def set_webhook():
    url = os.getenv("SELF_URL", "https://vols-assistant.onrender.com") + "/webhook"
    application.bot.set_webhook(url)
    print("Webhook set:", url)

if __name__ == "__main__":
    set_webhook()
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
