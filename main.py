import os
import logging
from flask import Flask, request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ContextTypes, filters
)
from config import TOKEN, PORT, SELF_URL
from services.user_access import get_user_rights

# Flask app для webhook
app = Flask(__name__)

application = Application.builder().token(TOKEN).build()

# ==========================
# МЕНЮ ПО ПРАВАМ ДОСТУПА
# ==========================
def build_main_menu(user_rights):
    kb = []
    # 1. Полная видимость (ALL)
    if user_rights['zone'] == "All":
        kb = [
            ["РОССЕТИ КУБАНЬ", "РОССЕТИ ЮГ"],
            ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"],
            ["СПРАВКА"]
        ]
    # 2. Только КУБАНЬ (RK)
    elif user_rights['zone'] == "RK":
        kb = [
            ["РОССЕТИ КУБАНЬ"],
            ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"],
            ["СПРАВКА"]
        ]
    # 3. Только ЮГ (UG)
    elif user_rights['zone'] == "UG":
        kb = [
            ["РОССЕТИ ЮГ"],
            ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"],
            ["СПРАВКА"]
        ]
    # 4. Только ФИЛИАЛ
    elif user_rights['filial'] != "All":
        kb = [
            [user_rights['filial']],
            ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ", "ОТЧЕТЫ"] if user_rights['res'] == "All" else ["ТЕЛЕФОНЫ КОНТРАГЕНТОВ"],
            ["СПРАВКА"]
        ]
    else:
        kb = [["СПРАВКА"]]
    return ReplyKeyboardMarkup(kb, resize_keyboard=True)

# ==========================
# /start Хендлер
# ==========================
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user_rights = get_user_rights(tg_id)
    if not user_rights:
        await update.message.reply_text("⛔️ Нет доступа. Обратитесь к администратору.")
        return
    menu = build_main_menu(user_rights)
    await update.message.reply_text(
        "Выберите действие:",
        reply_markup=menu
    )

application.add_handler(CommandHandler("start", start))

# ==========================
# МЕНЮ: обработчик главных кнопок (пример, дорабатываем дальше)
# ==========================
async def main_menu_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tg_id = update.effective_user.id
    user_rights = get_user_rights(tg_id)
    text = update.message.text

    # ===== КУБАНЬ =====
    if text == "РОССЕТИ КУБАНЬ" and (user_rights['zone'] in ("All", "RK")):
        filial_buttons = [
            ["Юго-Западные ЭС", "Усть-Лабинские ЭС", "Тимашевские ЭС"],
            ["Тихорецкие ЭС", "Сочинские ЭС", "Славянские ЭС"],
            ["Ленинградские ЭС", "Лабинские ЭС", "Краснодарские ЭС"],
            ["Армавирские ЭС", "Адыгейские ЭС"],
            ["Назад"]
        ]
        await update.message.reply_text("Выберите филиал:", reply_markup=ReplyKeyboardMarkup(filial_buttons, resize_keyboard=True))
        return

    # ===== ЮГ =====
    if text == "РОССЕТИ ЮГ" and (user_rights['zone'] in ("All", "UG")):
        filial_buttons = [
            ["Юго-Западные ЭС", "Центральные ЭС", "Западные ЭС"],
            ["Восточные ЭС", "Южные ЭС", "Северо-Восточные ЭС"],
            ["Юго-Восточные ЭС", "Северные ЭС"],
            ["Назад"]
        ]
        await update.message.reply_text("Выберите филиал:", reply_markup=ReplyKeyboardMarkup(filial_buttons, resize_keyboard=True))
        return

    # ===== ОТЧЕТЫ =====
    if text == "ОТЧЕТЫ":
        buttons = []
        if user_rights['zone'] in ("All", "RK"):
            buttons.append("Уведомления РОССЕТИ КУБАНЬ")
        if user_rights['zone'] in ("All", "UG"):
            buttons.append("Уведомления РОССЕТИ ЮГ")
        buttons.append("Назад")
        await update.message.reply_text("Выберите отчет:", reply_markup=ReplyKeyboardMarkup([[b] for b in buttons], resize_keyboard=True))
        return

    # ===== СПРАВКА =====
    if text == "СПРАВКА":
        await update.message.reply_text("Здесь будет список справок (позже).", reply_markup=ReplyKeyboardMarkup([["Назад"]], resize_keyboard=True))
        return

    # ===== НАЗАД =====
    if text == "Назад":
        await start(update, context)
        return

    # ===== Остальное =====
    await update.message.reply_text("Пункт в разработке.")

application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, main_menu_handler))

# ==========================
# WEBHOOK PART
# ==========================
@app.route("/webhook", methods=["POST"])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put(update)
    return "ok"

# ==========================
# MAIN
# ==========================
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(f"СТАРТ telegram-бота на порту {PORT}")

    application.bot.set_webhook(url=f"{SELF_URL}/webhook")
    app.run(host="0.0.0.0", port=PORT)
