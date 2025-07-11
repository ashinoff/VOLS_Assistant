import os
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes
)
from config import TOKEN, SELF_URL, PORT
from services.user_access import get_user_rights

import logging
logging.basicConfig(level=logging.INFO)

# ============================
# СТАРТОВОЕ МЕНЮ ПО ТЗ
# ============================

MAIN_MENUS = {
    "All": [
        ["РОССЕТИ ЮГ", "РОССЕТИ КУБАНЬ"],
        ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"],
        ["СПРАВКА"]
    ],
    "RK": [
        ["РОССЕТИ КУБАНЬ"],
        ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"],
        ["СПРАВКА"]
    ],
    "UG": [
        ["РОССЕТИ ЮГ"],
        ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"],
        ["СПРАВКА"]
    ],
}

def get_main_menu(rights):
    """Определяет стартовое меню по правам пользователя."""
    zone = rights["zone"]
    filial = rights["filial"]
    res = rights["res"]
    # Видимость All — все меню
    if zone == "All" and filial == "All" and res == "All":
        return MAIN_MENUS["All"]
    # Видимость RK только по Кубани
    if zone == "RK":
        if filial == "All" and res == "All":
            return MAIN_MENUS["RK"]
        if filial != "All" and res == "All":
            return [[f"{filial} ЭС"], ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"], ["СПРАВКА"]]
        if filial != "All" and res != "All":
            return [[f"{filial} ЭС"], ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ"], ["СПРАВКА"]]
    # Видимость UG только по Югу
    if zone == "UG":
        if filial == "All" and res == "All":
            return MAIN_MENUS["UG"]
        if filial != "All" and res == "All":
            return [[f"{filial} ЭС"], ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"], ["СПРАВКА"]]
        if filial != "All" and res != "All":
            return [[f"{filial} ЭС"], ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ"], ["СПРАВКА"]]
    # Если что-то необычное — минимальное меню
    return [["СПРАВКА"]]

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    telegram_id = update.effective_user.id
    rights = get_user_rights(telegram_id)
    if rights is None:
        await update.message.reply_text("У вас нет доступа.")
        return
    menu = get_main_menu(rights)
    reply_markup = ReplyKeyboardMarkup(menu, resize_keyboard=True)
    await update.message.reply_text(
        f"Привет, {rights['fio']}!\nВыберите раздел:", reply_markup=reply_markup
    )

async def handle_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    # Для теста просто выводим название нажатой кнопки
    await update.message.reply_text(f"Вы выбрали: {text}")

if __name__ == "__main__":
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_menu))
    print(f"СТАРТ telegram-бота на порту {PORT}")
    app.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        webhook_url=f"{SELF_URL}/webhook",
    )
