import os
import requests
import pandas as pd
import io
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TOKEN = os.getenv("TOKEN", "YOUR_TOKEN")
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "https://docs.google.com/....")
PORT = int(os.getenv("PORT", 8000))
SELF_URL = os.getenv("SELF_URL", "https://vols-assistant.onrender.com")

def get_zones_df():
    r = requests.get(ZONES_CSV_URL)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text))
    return df

def get_user_rights(telegram_id: int):
    df = get_zones_df()
    try:
        user_row = df[df['Telegram ID'] == telegram_id]
    except KeyError:
        return None
    if user_row.empty:
        return None
    row = user_row.iloc[0]
    return {
        "zone": row.get('Видимость', ''),
        "filial": row.get('Филиал', ''),
        "res": row.get('РЭС', ''),
        "fio": row.get('ФИО', ''),
        "responsible": row.get('Ответственный', ''),
    }

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

if __name__ == "__main__":
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    print(f"СТАРТ telegram-бота на порту {PORT}")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=f"{SELF_URL}/webhook"
    )
