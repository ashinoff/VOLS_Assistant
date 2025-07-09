import logging
import pandas as pd
import uvicorn
import asyncio
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ContextTypes,
    filters,
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
application = Application.builder().token(TOKEN).build()

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

# Define main menu buttons with visibility
MAIN_MENU = [
    {"text": "Россети Кубань ⚡️", "visibility": "all"},
    {"text": "Россети ЮГ 🔌", "visibility": "all"},
    {"text": "Выгрузить отчеты 📊", "visibility": "all"},
    {"text": "Телефонный справочник 📞", "visibility": "all"},
    {"text": "Справка ❓", "visibility": "all"},
    {"text": "Руководство пользователя 📖", "visibility": "all"},
]

# Define Rosseti Yug submenu with visibility
ROSSETI_YUG_MENU = [
    {"text": "Юго-Западные ЭС", "visibility": "yugo_zapad_yug"},
    {"text": "Центральные ЭС", "visibility": "central_yug"},
    {"text": "Западные ЭС", "visibility": "zapad_yug"},
    {"text": "Восточные ЭС", "visibility": "vostoch_yug"},
    {"text": "Южные ЭС", "visibility": "yuzh_yug"},
    {"text": "Северо-Восточные ЭС", "visibility": "severo_vostoch_yug"},
    {"text": "Юго-Восточные ЭС", "visibility": "yugo_vostoch_yug"},
    {"text": "Северные ЭС", "visibility": "sever_yug"},
]

# Define Rosseti Kuban submenu with visibility
ROSSETI_KUBAN_MENU = [
    {"text": "Юго-Западные ЭС", "visibility": "yugo_zapad_kuban"},
    {"text": "Усть-Лабинские ЭС", "visibility": "ust_labinsk_kuban"},
    {"text": "Тимашевские ЭС", "visibility": "timashevsk_kuban"},
    {"text": "Тихорецкие ЭС", "visibility": "tikhoretsk_kuban"},
    {"text": "Сочинские ЭС", "visibility": "sochi_kuban"},
    {"text": "Славянские ЭС", "visibility": "slavyansk_kuban"},
    {"text": "Ленинградские ЭС", "visibility": "leningradsk_kuban"},
    {"text": "Лабинские ЭС", "visibility": "labinsk_kuban"},
    {"text": "Краснодарские ЭС", "visibility": "krasnodar_kuban"},
    {"text": "Армавирские ЭС", "visibility": "armavir_kuban"},
    {"text": "Адыгейские ЭС", "visibility": "adygeysk_kuban"},
]

# Define ES submenu with visibility
ES_SUBMENU = [
    {"text": "Поиск по ТП 🔍", "visibility": "all"},
    {"text": "Отправить уведомление о БД ВОЛС 📬", "visibility": "all"},
    {"text": "Справка ❓", "visibility": "all"},
    {"text": "Назад ⬅️", "visibility": "all"},
]

# Build main menu based on user visibility
def build_main_menu(user_data):
    keyboard = []
    row = []
    for button in MAIN_MENU:
        if has_access(user_data, button["visibility"]):
            row.append(button["text"])
            if len(row) == 2:
                keyboard.append(row)
                row = []
    if row:
        keyboard.append(row)
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build Rosseti Yug submenu based on user visibility
def build_rosseti_yug_menu(user_data):
    keyboard = []
    row = []
    for button in ROSSETI_YUG_MENU:
        if has_access(user_data, button["visibility"]):
            row.append(button["text"])
            if len(row) == 2:
                keyboard.append(row)
                row = []
    if row:
        keyboard.append(row)
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build Rosseti Kuban submenu based on user visibility
def build_rosseti_kuban_menu(user_data):
    keyboard = []
    row = []
    for button in ROSSETI_KUBAN_MENU:
        if has_access(user_data, button["visibility"]):
            row.append(button["text"])
            if len(row) == 2:
                keyboard.append(row)
                row = []
    if row:
        keyboard.append(row)
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build ES submenu based on user visibility
def build_es_submenu(user_data):
    keyboard = []
    row = []
    for button in ES_SUBMENU:
        if has_access(user_data, button["visibility"]):
            row.append(button["text"])
            if len(row) == 2:
                keyboard.append(row)
                row = []
    if row:
        keyboard.append(row)
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return

    fio = user_data["FIO"]
    context.user_data["state"] = "MAIN_MENU"
    await update.message.reply_text(
        f"Здравствуйте, {fio}! Выберите действие:", reply_markup=build_main_menu(user_data)
    )

# Message handler for button presses
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return

    text = update.message.text
    state = context.user_data.get("state", "MAIN_MENU")

    # Main menu actions
    if state == "MAIN_MENU":
        if text == "Россети Кубань ⚡️" and has_access(user_data, "all"):
            context.user_data["state"] = "ROSSETI_KUBAN"
            context.user_data["previous_state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите ЭС:", reply_markup=build_rosseti_kuban_menu(user_data)
            )
        elif text == "Россети ЮГ 🔌" and has_access(user_data, "all"):
            context.user_data["state"] = "ROSSETI_YUG"
            context.user_data["previous_state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите ЭС:", reply_markup=build_rosseti_yug_menu(user_data)
            )
        elif text == "Выгрузить отчеты 📊" and has_access(user_data, "all"):
            await update.message.reply_text("Выгрузка отчетов 📊. Функционал в разработке.")
        elif text == "Телефонный справочник 📞" and has_access(user_data, "all"):
            await update.message.reply_text("Телефонный справочник 📞. Функционал в разработке.")
        elif text == "Справка ❓" and has_access(user_data, "all"):
            await update.message.reply_text("Справка ❓. Функционал в разработке.")
        elif text == "Руководство пользователя 📖" and has_access(user_data, "all"):
            await update.message.reply_text("Руководство пользователя 📖. Функционал в разработке.")
        else:
            await update.message.reply_text("Пожалуйста, выберите действие из меню.")

    # Rosseti Yug submenu actions
    elif state == "ROSSETI_YUG":
        for button in ROSSETI_YUG_MENU:
            if text == button["text"] and has_access(user_data, button["visibility"]):
                context.user_data["state"] = "ES_SUBMENU"
                context.user_data["selected_es"] = text
                context.user_data["previous_state"] = "ROSSETI_YUG"
                await update.message.reply_text(
                    f"Вы выбрали {text}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                )
                return
        await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")

    # Rosseti Kuban submenu actions
    elif state == "ROSSETI_KUBAN":
        for button in ROSSETI_KUBAN_MENU:
            if text == button["text"] and has_access(user_data, button["visibility"]):
                context.user_data["state"] = "ES_SUBMENU"
                context.user_data["selected_es"] = text
                context.user_data["previous_state"] = "ROSSETI_KUBAN"
                await update.message.reply_text(
                    f"Вы выбрали {text}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                )
                return
        await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")

    # ES submenu actions
    elif state == "ES_SUBMENU":
        selected_es = context.user_data.get("selected_es", "")
        if text == "Поиск по ТП 🔍" and has_access(user_data, "all"):
            await update.message.reply_text(f"Поиск по ТП для {selected_es} 🔍. Функционал в разработке.")
        elif text == "Отправить уведомление о БД ВОЛС 📬" and has_access(user_data, "all"):
            await update.message.reply_text(f"Отправка уведомления для {selected_es} 📬. Функционал в разработке.")
        elif text == "Справка ❓" and has_access(user_data, "all"):
            await update.message.reply_text(f"Справка для {selected_es} ❓. Функционал в разработке.")
        elif text == "Назад ⬅️" and has_access(user_data, "all"):
            previous_state = context.user_data.get("previous_state", "MAIN_MENU")
            context.user_data["state"] = previous_state
            if previous_state == "ROSSETI_YUG":
                await update.message.reply_text("Выберите ЭС:", reply_markup=build_rosseti_yug_menu(user_data))
            elif previous_state == "ROSSETI_KUBAN":
                await update.message.reply_text("Выберите ЭС:", reply_markup=build_rosseti_kuban_menu(user_data))
            else:
                context.user_data["state"] = "MAIN_MENU"
                await update.message.reply_text("Выберите действие:", reply_markup=build_main_menu(user_data))
        else:
            await update.message.reply_text("Пожалуйста, выберите действие из меню.")

# Error handler
async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    logger.error(f"Update {update} caused error {context.error}")

# Webhook endpoint
@app.post("/webhook")
async def webhook(request: Request):
    update = Update.de_json(await request.json(), application.bot)
    if update:
        await application.process_update(update)
    return {"status": "ok"}

# Root endpoint for health check
@app.get("/")
async def root():
    return {"message": "Bot is running"}

# FastAPI startup event to set webhook
@app.on_event("startup")
async def on_startup():
    webhook_url = f"{SELF_URL}/webhook"
    await application.bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook set to {webhook_url}")
    await application.initialize()

# FastAPI shutdown event
@app.on_event("shutdown")
async def on_shutdown():
    await application.stop()

def main():
    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_error_handler(error_handler)

    # Start FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
