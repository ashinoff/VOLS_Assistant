import logging
import pandas as pd
import uvicorn
import asyncio
import re
import requests
from fastapi import FastAPI, Request, HTTPException
from telegram import Update, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)
from config import (
    TOKEN, ZONES_CSV_URL, SELF_URL, PORT,
    YUGO_ZAPAD_URL_UG, CENTRAL_URL_UG, ZAPAD_URL_UG, VOSTOCH_URL_UG,
    YUZH_URL_UG, SEVERO_VOSTOCH_URL_UG, YUGO_VOSTOCH_URL_UG, SEVER_URL_UG,
    YUGO_ZAPAD_URL_RK, UST_LABINSK_URL_RK, TIMASHEVSK_URL_RK, TIKHORETSK_URL_RK,
    SOCHI_URL_RK, SLAVYANSK_URL_RK, LENINGRADSK_URL_RK, LABINSK_URL_RK,
    KRASNODAR_URL_RK, ARMAVIR_URL_RK, ADYGEYSK_URL_RK
)

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI()

# Initialize Telegram application
application = Application.builder().token(TOKEN).build()

# States for ConversationHandler
SEARCH_TP, SELECT_TP = range(2)

# Mapping of ES names to their URLs
ES_URL_MAPPING = {
    "Юго-Западные ЭС_UG": YUGO_ZAPAD_URL_UG,
    "Центральные ЭС": CENTRAL_URL_UG,
    "Западные ЭС": ZAPAD_URL_UG,
    "Восточные ЭС": VOSTOCH_URL_UG,
    "Южные ЭС": YUZH_URL_UG,
    "Северо-Восточные ЭС": SEVERO_VOSTOCH_URL_UG,
    "Юго-Восточные ЭС": YUGO_VOSTOCH_URL_UG,
    "Северные ЭС": SEVER_URL_UG,
    "Юго-Западные ЭС_RK": YUGO_ZAPAD_URL_RK,
    "Усть-Лабинские ЭС": UST_LABINSK_URL_RK,
    "Тимашевские ЭС": TIMASHEVSK_URL_RK,
    "Тихорецкие ЭС": TIKHORETSK_URL_RK,
    "Сочинские ЭС": SOCHI_URL_RK,
    "Славянские ЭС": SLAVYANSK_URL_RK,
    "Ленинградские ЭС": LENINGRADSK_URL_RK,
    "Лабинские ЭС": LABINSK_URL_RK,
    "Краснодарские ЭС": KRASNODAR_URL_RK,
    "Армавирские ЭС": ARMAVIR_URL_RK,
    "Адыгейские ЭС": ADYGEYSK_URL_RK,
}

# Load user data from CSV for access control
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

# Load TP data from ES-specific URL
def load_tp_data(es_name, is_rosseti_yug):
    suffix = "_UG" if is_rosseti_yug else "_RK"
    es_key = es_name if not es_name.startswith("Юго-Западные ЭС") else f"Юго-Западные ЭС{suffix}"
    url = ES_URL_MAPPING.get(es_key)
    if not url:
        logger.error(f"URL для {es_name} не найден")
        return pd.DataFrame()
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(url, encoding="utf-8")
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных ТП для {es_name}: {e}")
        return pd.DataFrame()

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
    {"text": "Назад ⬅️", "visibility": "all"},
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
    {"text": "Назад ⬅️", "visibility": "all"},
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
    keyboard = [[button["text"]] for button in MAIN_MENU if has_access(user_data, button["visibility"])]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build Rosseti Yug submenu based on user visibility
def build_rosseti_yug_menu(user_data):
    keyboard = [[button["text"]] for button in ROSSETI_YUG_MENU if has_access(user_data, button["visibility"])]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build Rosseti Kuban submenu based on user visibility
def build_rosseti_kuban_menu(user_data):
    keyboard = [[button["text"]] for button in ROSSETI_KUBAN_MENU if has_access(user_data, button["visibility"])]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build ES submenu based on user visibility
def build_es_submenu(user_data):
    keyboard = [[button["text"]] for button in ES_SUBMENU if has_access(user_data, button["visibility"])]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build TP selection keyboard
def build_tp_selection_menu(tp_options):
    keyboard = [[tp] for tp in tp_options]
    keyboard.append(["Отмена 🚫"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# Fuzzy search for TP
def fuzzy_search_tp(search_term, df):
    if not isinstance(search_term, str):
        return []
    # Normalize search term: remove hyphens, spaces, convert to lowercase
    search_term = re.sub(r'[- ]', '', search_term.lower())
    matches = []
    for tp in df["Наименование ТП"].dropna().unique():
        if not isinstance(tp, str):
            continue
        # Normalize TP name
        normalized_tp = re.sub(r'[- ]', '', tp.lower())
        if search_term in normalized_tp:
            matches.append(tp)
    return matches

# Start command handler
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    fio = user_data["FIO"]
    context.user_data["state"] = "MAIN_MENU"
    await update.message.reply_text(
        f"Здравствуйте, {fio}! Выберите действие:", reply_markup=build_main_menu(user_data)
    )
    return ConversationHandler.END

# Message handler for button presses
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    text = update.message.text
    state = context.user_data.get("state", "MAIN_MENU")

    # Main menu actions
    if state == "MAIN_MENU":
        if text == "Россети Кубань ⚡️" and has_access(user_data, "all"):
            context.user_data["state"] = "ROSSETI_KUBAN"
            context.user_data["previous_state"] = "MAIN_MENU"
            context.user_data["is_rosseti_yug"] = False
            await update.message.reply_text(
                "Выберите ЭС:", reply_markup=build_rosseti_kuban_menu(user_data)
            )
        elif text == "Россети ЮГ 🔌" and has_access(user_data, "all"):
            context.user_data["state"] = "ROSSETI_YUG"
            context.user_data["previous_state"] = "MAIN_MENU"
            context.user_data["is_rosseti_yug"] = True
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
        return ConversationHandler.END

    # Rosseti Yug submenu actions
    elif state == "ROSSETI_YUG":
        if text == "Назад ⬅️" and has_access(user_data, "all"):
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_main_menu(user_data)
            )
        else:
            for button in ROSSETI_YUG_MENU:
                if text == button["text"] and has_access(user_data, button["visibility"]):
                    context.user_data["state"] = "ES_SUBMENU"
                    context.user_data["selected_es"] = text
                    context.user_data["previous_state"] = "ROSSETI_YUG"
                    await update.message.reply_text(
                        f"Вы выбрали {text}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                    )
                    return ConversationHandler.END
            await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")
        return ConversationHandler.END

    # Rosseti Kuban submenu actions
    elif state == "ROSSETI_KUBAN":
        if text == "Назад ⬅️" and has_access(user_data, "all"):
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_main_menu(user_data)
            )
        else:
            for button in ROSSETI_KUBAN_MENU:
                if text == button["text"] and has_access(user_data, button["visibility"]):
                    context.user_data["state"] = "ES_SUBMENU"
                    context.user_data["selected_es"] = text
                    context.user_data["previous_state"] = "ROSSETI_KUBAN"
                    await update.message.reply_text(
                        f"Вы выбрали {text}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                    )
                    return ConversationHandler.END
            await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")
        return ConversationHandler.END

    # ES submenu actions
    elif state == "ES_SUBMENU":
        selected_es = context.user_data.get("selected_es", "")
        if text == "Поиск по ТП 🔍" and has_access(user_data, "all"):
            await update.message.reply_text(
                f"Введите наименование ТП для поиска в {selected_es}:", reply_markup=ReplyKeyboardRemove()
            )
            return SEARCH_TP
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
        return ConversationHandler.END

# Search TP handler
async def search_tp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    search_term = update.message.text
    selected_es = context.user_data.get("selected_es", "")
    is_rosseti_yug = context.user_data.get("is_rosseti_yug", False)
    df = load_tp_data(selected_es, is_rosseti_yug)

    if df.empty:
        await update.message.reply_text(
            f"Ошибка загрузки данных для {selected_es}. Попробуйте позже.", 
            reply_markup=build_es_submenu(user_data)
        )
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END

    # Exact match
    exact_match = df[df["Наименование ТП"] == search_term]
    if not exact_match.empty:
        await send_tp_results(update, context, exact_match, selected_es)
        context.user_data["state"] = "ES_SUBMENU"
        await update.message.reply_text(
            f"Выберите действие для {selected_es}:", reply_markup=build_es_submenu(user_data)
        )
        return ConversationHandler.END

    # Fuzzy search
    tp_options = fuzzy_search_tp(search_term, df)
    if not tp_options:
        await update.message.reply_text(
            f"ТП с названием '{search_term}' не найдено в {selected_es}. Попробуйте еще раз:",
            reply_markup=ReplyKeyboardRemove()
        )
        return SEARCH_TP

    context.user_data["tp_options"] = tp_options
    await update.message.reply_text(
        f"ТП с названием '{search_term}' не найдено. Похожие варианты:", 
        reply_markup=build_tp_selection_menu(tp_options)
    )
    return SELECT_TP

# Select TP handler
async def select_tp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    text = update.message.text
    selected_es = context.user_data.get("selected_es", "")
    is_rosseti_yug = context.user_data.get("is_rosseti_yug", False)
    df = load_tp_data(selected_es, is_rosseti_yug)

    if text == "Отмена 🚫":
        context.user_data["state"] = "ES_SUBMENU"
        await update.message.reply_text(
            f"Выберите действие для {selected_es}:", reply_markup=build_es_submenu(user_data)
        )
        return ConversationHandler.END

    if text in context.user_data.get("tp_options", []):
        df_filtered = df[df["Наименование ТП"] == text]
        await send_tp_results(update, context, df_filtered, selected_es)
        context.user_data["state"] = "ES_SUBMENU"
        await update.message.reply_text(
            f"Выберите действие для {selected_es}:", reply_markup=build_es_submenu(user_data)
        )
        return ConversationHandler.END

    await update.message.reply_text(
        "Пожалуйста, выберите ТП из предложенных вариантов:", 
        reply_markup=build_tp_selection_menu(context.user_data.get("tp_options", []))
    )
    return SELECT_TP

# Send TP results
async def send_tp_results(update: Update, context: ContextTypes.DEFAULT_TYPE, df, selected_es):
    count = len(df)
    await update.message.reply_text(f"В {selected_es} найдено {count} ВОЛС с договором аренды.")
    
    for _, row in df.iterrows():
        message = (
            f"🔌 ВЛ: {row['Наименование ВЛ']}\n"
            f"Опоры: {row['Опоры']}\n"
            f"Количество: {row['Количество опор']}\n"
            f"Наименование Провайдера: {row['Наименование Провайдера']}"
        )
        await update.message.reply_text(message)

# Cancel search
async def cancel_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)
    selected_es = context.user_data.get("selected_es", "")
    context.user_data["state"] = "ES_SUBMENU"
    await update.message.reply_text(
        f"Поиск отменен. Выберите действие для {selected_es}:", 
        reply_markup=build_es_submenu(user_data)
    )
    return ConversationHandler.END

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
    # Conversation handler for TP search
    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp)],
            SELECT_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_tp)],
        },
        fallbacks=[CommandHandler("cancel", cancel_search)],
    )

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    # Start FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
