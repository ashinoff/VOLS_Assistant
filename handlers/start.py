from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes
from services.user_access import get_user_rights

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rights = get_user_rights(user_id)
    if rights is None:
        await update.message.reply_text("Нет доступа. Обратитесь к администратору.")
        return
    # Выдаем меню согласно зоне видимости (здесь пример)
    buttons = [["РОССЕТИ КУБАНЬ", "РОССЕТИ ЮГ"], ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"], ["СПРАВКА"]]
    await update.message.reply_text("Выберите действие:", reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True))
