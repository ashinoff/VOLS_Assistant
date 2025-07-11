import os
import logging
from dotenv import load_dotenv

from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, ContextTypes, filters
)

# Подгружаем переменные окружения
load_dotenv()

TOKEN = os.getenv("TOKEN")
SELF_URL = os.getenv("SELF_URL", "")
PORT = int(os.getenv("PORT", 8000))

# Импорт хендлеров
from handlers.help import help_command
from services.user_access import get_user_rights

# ============ НАСТРОЙКА ЛОГГЕРА ============
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO
)

# ============ ОБРАБОТЧИК /start ============
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    # пример вызова проверки прав доступа
    try:
        rights = get_user_rights(user_id)
        await update.message.reply_text(
            f"Добро пожаловать, {update.effective_user.first_name}! Ваши права: {rights}"
        )
    except Exception as e:
        await update.message.reply_text("Ошибка доступа. Обратитесь к администратору.")
        logging.exception(e)

# ============ ОБРАБОТЧИК ЭХО/ТЕКСТА ============
async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Пока только echo. Используйте /help.")

# ============ MAIN ============

def main():
    application = ApplicationBuilder().token(TOKEN).build()

    # Команды
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))

    # Пример на любую другую команду/кнопку
    # application.add_handler(CommandHandler("some", some_command))

    # Обработчик всех текстовых сообщений (эко)
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), echo))

    # WEBHOOK (если нужен для Render)
    if SELF_URL:
        print(f"СТАРТ telegram-бота на порту {PORT}")
        application.run_webhook(
            listen="0.0.0.0",
            port=PORT,
            webhook_url=f"{SELF_URL}/webhook"
        )
    else:
        print("СТАРТ telegram-бота в режиме polling")
        application.run_polling()

if __name__ == "__main__":
    main()
