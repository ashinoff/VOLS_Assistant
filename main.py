import logging
import pandas as pd
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)
from config import TOKEN, ZONES_CSV_URL, SELF_URL, PORT

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Initialize Telegram application
application = None

# Load user data from CSV on Google Drive
def load_user_data():
    users = {}
    try:
        df = pd.read_csv(ZONES_CSV_URL, encoding="utf-8")
        for _, row in df.iterrows():
            users[str(row["Telegram ID"])] = {
                "Visibility": row["Видимость"],
                "Branch": row["Филиал"],
                "RES": row["РЭС"],
                "FIO": row["ФИО"],
                "Responsible": row["Ответственный"],
            }
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных пользователей: {e}")
    return users

# Check user visibility for a specific menu item
def has_access(user_data, required_visibility):
    if not user_data:
        return False
    user_visibility = user_data.get("Visibility", "").lower()
    return (
        user_visibility == "all"
        or required_visibility.lower() == "all"
        or user_visibility == required_visibility.lower()
    )

# Define menu buttons with visibility requirements
MENU_BUTTONS = [
    {
        "text": "Россети Кубань ⚡️",
        "callback_data": "rosseti_kuban",
        "visibility": "all",
    },
    {
        "text": "Россети ЮГ 🔌",
        "callback_data": "rosseti_yug",
        "visibility": "all",
    },
    {
        "text": "Выгрузить отчеты 📊",
        "callback_data": "download_reports",
        "visibility": "all",
    },
    {
        "text": "Телефонный справочник 📞",
        "callback_data": "phone_directory",
        "visibility": "all",
    },
    {
        "text": "Справка ❓",
        "callback_data": "help",
        "visibility": "all",
    },
    {
        "text": "Руководство пользователя 📖",
        "callback_data": "user_guide",
        "visibility": "all",
    },
]

# Build keyboard based on user visibility
def build_menu(user_data):
    keyboard = []
    for button in MENU_BUTTONS:
        if has_access(user_data, button["visibility"]):
            keyboard.append(
                [InlineKeyboardButton(button["text"], callback_data=button["callback_data"])]
            )
    return InlineKeyboardMarkup(keyboard)

# Start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text("Извините, вы не зарегистрированы в системе.")
        return

    fio = user_data["FIO"]
    keyboard = build_menu(user_data)
    await update.message.reply_text(
        f"Здравствуйте, {fio}! Выберите действие:", reply_markup=keyboard
    )

# Button callback handler
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # Placeholder responses
    callback_data = query.data
    responses = {
        "rosseti_kuban": "Добро пожаловать в раздел Россети Кубань ⚡️. Функциональность в разработке.",
        "rosseti_yug": "Добро пожаловать в раздел Россети ЮГ 🔌. Функциональность в разработке.",
        "download_reports": "Выгрузка отчетов 📊. Функционал в разработке.",
        "phone_directory": "Телефонный справочник 📞. Функционал в разработке.",
        "help": "Справка ❓. Функционал в разработке.",
        "user_guide": "Руководство пользователя 📖. Функционал в разработке.",
    }

    await query.message.reply_text(responses.get(callback_data, "Неизвестная команда."))

# Error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")

# Webhook endpoint
@app.post("/webhook")
async def webhook(request: Request):
    if application is None:
        raise HTTPException(status_code=503, detail="Bot not initialized")
    update = Update.de_json(await request.json(), application.bot)
    await application.process_update(update)
    return {"status": "ok"}

# Root endpoint for health check
@app.get("/")
async def root():
    return {"message": "Bot is running"}

async def setup_webhook():
    webhook_url = f"{SELF_URL}/webhook"
    await application.bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook set to {webhook_url}")

def main():
    global application
    # Initialize bot
    application = Application.builder().token(TOKEN).build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_error_handler(error_handler)

    # Set webhook
    application.run_coroutine(setup_webhook())

    # Start FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
