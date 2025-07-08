import csv
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Path to zones file
ZONES_FILE = "zones_rk_ug.csv"

# Load user data from CSV
def load_user_data():
    users = {}
    try:
        with open(ZONES_FILE, newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                users[row["Telegram ID"]] = {
                    "Visibility": row["Visibility"],
                    "Branch": row["Филиал"],
                    "RES": row["РЭС"],
                    "FIO": row["ФИО"],
                    "Responsible": row["Ответственный"],
                }
    except FileNotFoundError:
        logger.error(f"Zones file {ZONES_FILE} not found.")
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

# Button callback handler (placeholder for future functionality)
async def button_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    # Placeholder responses for each button
    callback_data = query.data
    responses = {
        "rosseti_kuban": "Добро пожаловать в раздел Россети Кубань ⚡️. Функциональность в разработке.",
        "rosseti_yug": "Добро пожаловать в раздел Россети ЮГ ⚡️. Функциональность в разработке.",
        "download_reports": "Выгрузка отчетов 📊. Функционал в разработке.",
        "phone_directory": "Телефонный справочник 📞. Функционал в разработке.",
        "help": "Справка ❓. Функционал в разработке.",
        "user_guide": "Руководство пользователя 📖. Функционал в разработке.",
    }

    await query.message.reply_text(responses.get(callback_data, "Неизвестная команда."))

# Error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")

def main():
    # Replace 'YOUR_TOKEN' with your bot token
    application = Application.builder().token("YOUR_TOKEN").build()

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(button_callback))
    application.add_error_handler(error_handler)

    # Start the bot
    application.run_polling()

if __name__ == "__main__":
    main()
