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
    KRASNODAR_URL_RK, ARMAVIR_URL_RK, ADYGEYSK_URL_RK,
    YUGO_ZAPAD_URL_UG_SP, CENTRAL_URL_UG_SP, ZAPAD_URL_UG_SP, VOSTOCH_URL_UG_SP,
    YUZH_URL_UG_SP, SEVERO_VOSTOCH_URL_UG_SP, YUGO_VOSTOCH_URL_UG_SP, SEVER_URL_UG_SP,
    YUGO_ZAPAD_URL_RK_SP, UST_LABINSK_URL_RK_SP, TIMASHEVSK_URL_RK_SP, TIKHORETSK_URL_RK_SP,
    SOCHI_URL_RK_SP, SLAVYANSK_URL_RK_SP, LENINGRADSK_URL_RK_SP, LABINSK_URL_RK_SP,
    KRASNODAR_URL_RK_SP, ARMAVIR_URL_RK_SP, ADYGEYSK_URL_RK_SP
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
SEARCH_TP, SELECT_TP, NOTIFY_TP, NOTIFY_VL, NOTIFY_GEO = range(5)

# Mapping of ES names to their URLs for TP search
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

# Mapping of ES names to their URLs for notification directory
ES_SP_URL_MAPPING = {
    "Юго-Западные ЭС_UG": YUGO_ZAPAD_URL_UG_SP,
    "Центральные ЭС": CENTRAL_URL_UG_SP,
    "Западные ЭС": ZAPAD_URL_UG_SP,
    "Восточные ЭС": VOSTOCH_URL_UG_SP,
    "Южные ЭС": YUZH_URL_UG_SP,
    "Северо-Восточные ЭС": SEVERO_VOSTOCH_URL_UG_SP,
    "Юго-Восточные ЭС": YUGO_VOSTOCH_URL_UG_SP,
    "Северные ЭС": SEVER_URL_UG_SP,
    "Юго-Западные ЭС_RK": YUGO_ZAPAD_URL_RK_SP,
    "Усть-Лабинские ЭС": UST_LABINSK_URL_RK_SP,
    "Тимашевские ЭС": TIMASHEVSK_URL_RK_SP,
    "Тихорецкие ЭС": TIKHORETSK_URL_RK_SP,
    "Сочинские ЭС": SOCHI_URL_RK_SP,
    "Славянские ЭС": SLAVYANSK_URL_RK_SP,
    "Ленинградские ЭС": LENINGRADSK_URL_RK_SP,
    "Лабинские ЭС": LABINSK_URL_RK_SP,
    "Краснодарские ЭС": KRASNODAR_URL_RK_SP,
    "Армавирские ЭС": ARMAVIR_URL_RK_SP,
    "Адыгейские ЭС": ADYGEYSK_URL_RK_SP,
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

# Load TP directory data for notifications
def load_tp_directory_data(es_name, is_rosseti_yug):
    suffix = "_UG" if is_rosseti_yug else "_RK"
    es_key = es_name if not es_name.startswith("Юго-Западные ЭС") else f"Юго-Западные ЭС{suffix}"
    url = ES_SP_URL_MAPPING.get(es_key)
    if not url:
        logger.error(f"URL справочника для {es_name} не найден")
        return pd.DataFrame()
    try:
        response = requests.get(url)
        response.raise_for_status()
        df = pd.read_csv(url, encoding="utf-8")
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке справочника для {es_name}: {e}")
        return pd.DataFrame()

# Find responsible user for RES
def find_responsible(res, users):
    for user_id, user_data in users.items():
        if user_data["Responsible"] == res:
            return user_id, user_data["FIO"]
    return None, None

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
    {"text": "⚡️ Россети ЮГ", "visibility": "all"},
    {"text": "⚡️ Россети Кубань", "visibility": "all"},
    {"text": "📊 Выгрузить отчеты", "visibility": "all"},
    {"text": "📞 Телефонный справочник", "visibility": "all"},
    {"text": "📖 Руководство пользователя", "visibility": "all"},
    {"text": "📚 Справка", "visibility": "all"},
    {"text": "⬅️ Назад", "visibility": "all"},
]

# Define Rosseti Yug submenu with visibility
ROSSETI_YUG_MENU = [
    {"text": "⚡️ Юго-Западные ЭС", "visibility": "yugo_zapad_yug"},
    {"text": "⚡️ Центральные ЭС", "visibility": "central_yug"},
    {"text": "⚡️ Западные ЭС", "visibility": "zapad_yug"},
    {"text": "⚡️ Восточные ЭС", "visibility": "vostoch_yug"},
    {"text": "⚡️ Южные ЭС", "visibility": "yuzh_yug"},
    {"text": "⚡️ Северо-Восточные ЭС", "visibility": "severo_vostoch_yug"},
    {"text": "⚡️ Юго-Восточные ЭС", "visibility": "yugo_vostoch_yug"},
    {"text": "⚡️ Северные ЭС", "visibility": "sever_yug"},
    {"text": "⬅️ Назад", "visibility": "all"},
]

# Define Rosseti Kuban submenu with visibility
ROSSETI_KUBAN_MENU = [
    {"text": "⚡️ Юго-Западные ЭС", "visibility": "yugo_zapad_kuban"},
    {"text": "⚡️ Усть-Лабинские ЭС", "visibility": "ust_labinsk_kuban"},
    {"text": "⚡️ Тимашевские ЭС", "visibility": "timashevsk_kuban"},
    {"text": "⚡️ Тихорецкие ЭС", "visibility": "tikhoretsk_kuban"},
    {"text": "⚡️ Сочинские ЭС", "visibility": "sochi_kuban"},
    {"text": "⚡️ Славянские ЭС", "visibility": "slavyansk_kuban"},
    {"text": "⚡️ Ленинградские ЭС", "visibility": "leningradsk_kuban"},
    {"text": "⚡️ Лабинские ЭС", "visibility": "labinsk_kuban"},
    {"text": "⚡️ Краснодарские ЭС", "visibility": "krasnodar_kuban"},
    {"text": "⚡️ Армавирские ЭС", "visibility": "armavir_kuban"},
    {"text": "⚡️ Адыгейские ЭС", "visibility": "adygeysk_kuban"},
    {"text": "⬅️ Назад", "visibility": "all"},
]

# Define ES submenu with visibility
ES_SUBMENU = [
    {"text": "🔍 Поиск по ТП", "visibility": "all"},
    {"text": "🔔 Отправить уведомление", "visibility": "all"},
    {"text": "📚 Справка", "visibility": "all"},
    {"text": "⬅️ Назад", "visibility": "all"},
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
    keyboard.append(["⬅️ Назад"])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

# Build VL selection keyboard
def build_vl_selection_menu(vl_options):
    keyboard = [[vl] for vl in vl_options]
    keyboard.append(["⬅️ Назад"])
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
        if text == "⚡️ Россети ЮГ" and has_access(user_data, "all"):
            context.user_data["state"] = "ROSSETI_YUG"
            context.user_data["previous_state"] = "MAIN_MENU"
            context.user_data["is_rosseti_yug"] = True
            await update.message.reply_text(
                "Выберите ЭС:", reply_markup=build_rosseti_yug_menu(user_data)
            )
        elif text == "⚡️ Россети Кубань" and has_access(user_data, "all"):
            context.user_data["state"] = "ROSSETI_KUBAN"
            context.user_data["previous_state"] = "MAIN_MENU"
            context.user_data["is_rosseti_yug"] = False
            await update.message.reply_text(
                "Выберите ЭС:", reply_markup=build_rosseti_kuban_menu(user_data)
            )
        elif text == "📊 Выгрузить отчеты" and has_access(user_data, "all"):
            await update.message.reply_text("Выгрузка отчетов 📊. Функционал в разработке.")
        elif text == "📞 Телефонный справочник" and has_access(user_data, "all"):
            await update.message.reply_text("Телефонный справочник 📞. Функционал в разработке.")
        elif text == "📖 Руководство пользователя" and has_access(user_data, "all"):
            await update.message.reply_text("Руководство пользователя 📖. Функционал в разработке.")
        elif text == "📚 Справка" and has_access(user_data, "all"):
            await update.message.reply_text("Справка 📚. Функционал в разработке.")
        elif text == "⬅️ Назад" and has_access(user_data, "all"):
            await start(update, context)
        else:
            await update.message.reply_text("Пожалуйста, выберите действие из меню.")
        return ConversationHandler.END

    # Rosseti Yug submenu actions
    elif state == "ROSSETI_YUG":
        if text == "⬅️ Назад" and has_access(user_data, "all"):
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_main_menu(user_data)
            )
        else:
            for button in ROSSETI_YUG_MENU:
                if text == button["text"] and has_access(user_data, button["visibility"]):
                    context.user_data["state"] = "ES_SUBMENU"
                    context.user_data["selected_es"] = text.replace("⚡️ ", "")
                    context.user_data["previous_state"] = "ROSSETI_YUG"
                    await update.message.reply_text(
                        f"Вы выбрали {text.replace('⚡️ ', '')}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                    )
                    return ConversationHandler.END
            await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")
        return ConversationHandler.END

    # Rosseti Kuban submenu actions
    elif state == "ROSSETI_KUBAN":
        if text == "⬅️ Назад" and has_access(user_data, "all"):
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_main_menu(user_data)
            )
        else:
            for button in ROSSETI_KUBAN_MENU:
                if text == button["text"] and has_access(user_data, button["visibility"]):
                    context.user_data["state"] = "ES_SUBMENU"
                    context.user_data["selected_es"] = text.replace("⚡️ ", "")
                    context.user_data["previous_state"] = "ROSSETI_KUBAN"
                    await update.message.reply_text(
                        f"Вы выбрали {text.replace('⚡️ ', '')}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                    )
                    return ConversationHandler.END
            await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")
        return ConversationHandler.END

    # ES submenu actions
    elif state == "ES_SUBMENU":
        selected_es = context.user_data.get("selected_es", "")
        if text == "🔍 Поиск по ТП" and has_access(user_data, "all"):
            await update.message.reply_text(
                f"Введите наименование ТП для поиска в {selected_es}:", reply_markup=ReplyKeyboardRemove()
            )
            return SEARCH_TP
        elif text == "🔔 Отправить уведомление" and has_access(user_data, "all"):
            back_button = [["⬅️ Назад"]]
            await update.message.reply_text(
                "Введите наименование ТП где обнаружен бездоговорной ВОЛС:", reply_markup=ReplyKeyboardMarkup(back_button, resize_keyboard=True)
            )
            return NOTIFY_TP
        elif text == "📚 Справка" and has_access(user_data, "all"):
            await update.message.reply_text(f"Справка 📚. Функционал в разработке.")
        elif text == "⬅️ Назад" and has_access(user_data, "all"):
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
        await send_tp_results(update, context, exact_match, selected_es, search_term)
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

# Select TP handler (for search)
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

    if text == "⬅️ Назад":
        context.user_data["state"] = "ES_SUBMENU"
        await update.message.reply_text(
            f"Выберите действие для {selected_es}:", reply_markup=build_es_submenu(user_data)
        )
        return ConversationHandler.END

    if text in context.user_data.get("tp_options", []):
        df_filtered = df[df["Наименование ТП"] == text]
        await send_tp_results(update, context, df_filtered, selected_es, text)
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

# Notify TP handler
async def notify_tp(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    text = update.message.text
    if text == "⬅️ Назад":
        selected_es = context.user_data.get("selected_es", "")
        context.user_data["state"] = "ES_SUBMENU"
        await update.message.reply_text(
            f"Выберите действие для {selected_es}:", reply_markup=build_es_submenu(user_data)
        )
        return ConversationHandler.END

    search_term = text
    selected_es = context.user_data.get("selected_es", "")
    is_rosseti_yug = context.user_data.get("is_rosseti_yug", False)
    df = load_tp_directory_data(selected_es, is_rosseti_yug)

    if df.empty:
        await update.message.reply_text(
            f"Ошибка загрузки справочника для {selected_es}. Попробуйте позже.", 
            reply_markup=build_es_submenu(user_data)
        )
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END

    # Exact match
    exact_match = df[df["Наименование ТП"] == search_term]
    if not exact_match.empty:
        vl_options = exact_match["Наименование ВЛ"].dropna().unique().tolist()
        context.user_data["selected_tp"] = search_term
        context.user_data["vl_options"] = vl_options
        await update.message.reply_text(
            f"Выберите ВЛ для {search_term}:", 
            reply_markup=build_vl_selection_menu(vl_options)
        )
        return NOTIFY_VL

    # Fuzzy search
    tp_options = fuzzy_search_tp(search_term, df)
    if not tp_options:
        back_button = [["⬅️ Назад"]]
        await update.message.reply_text(
            f"ТП с названием '{search_term}' не найдено в справочнике {selected_es}. Попробуйте еще раз:",
            reply_markup=ReplyKeyboardMarkup(back_button, resize_keyboard=True)
        )
        return NOTIFY_TP

    context.user_data["tp_options"] = tp_options
    await update.message.reply_text(
        f"ТП с названием '{search_term}' не найдено. Похожие варианты:", 
        reply_markup=build_tp_selection_menu(tp_options)
    )
    return NOTIFY_TP

# Notify VL handler
async def notify_vl(update: Update, context: ContextTypes.DEFAULT_TYPE):
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
    vl_options = context.user_data.get("vl_options", [])

    if text == "⬅️ Назад":
        back_button = [["⬅️ Назад"]]
        await update.message.reply_text(
            "Введите наименование ТП где обнаружен бездоговорной ВОЛС:", 
            reply_markup=ReplyKeyboardMarkup(back_button, resize_keyboard=True)
        )
        return NOTIFY_TP

    if text in vl_options:
        context.user_data["selected_vl"] = text
        location_button = [[{"text": "📍 Отправить местоположение", "request_location": True}]]
        await update.message.reply_text(
            "Отправьте ваше местоположение:", reply_markup=ReplyKeyboardMarkup(location_button, resize_keyboard=True)
        )
        return NOTIFY_GEO

    # Check if it's a TP selection from fuzzy search
    is_rosseti_yug = context.user_data.get("is_rosseti_yug", False)
    df = load_tp_directory_data(selected_es, is_rosseti_yug)
    if text in context.user_data.get("tp_options", []):
        context.user_data["selected_tp"] = text
        vl_options = df[df["Наименование ТП"] == text]["Наименование ВЛ"].dropna().unique().tolist()
        context.user_data["vl_options"] = vl_options
        await update.message.reply_text(
            f"Выберите ВЛ для {text}:", 
            reply_markup=build_vl_selection_menu(vl_options)
        )
        return NOTIFY_VL

    await update.message.reply_text(
        "Пожалуйста, выберите ВЛ из предложенных вариантов:", 
        reply_markup=build_vl_selection_menu(vl_options)
    )
    return NOTIFY_VL

# Notify Geo handler
async def notify_geo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)

    if not user_data:
        await update.message.reply_text(
            "Извините, вы не зарегистрированы в системе.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END

    if not update.message.location:
        await update.message.reply_text(
            "Пожалуйста, отправьте местоположение.", reply_markup=ReplyKeyboardRemove()
        )
        return NOTIFY_GEO

    latitude = update.message.location.latitude
    longitude = update.message.location.longitude
    geo_data = f"{latitude}, {longitude}"
    selected_es = context.user_data.get("selected_es", "")
    selected_tp = context.user_data.get("selected_tp", "")
    selected_vl = context.user_data.get("selected_vl", "")
    is_rosseti_yug = context.user_data.get("is_rosseti_yug", False)
    df = load_tp_directory_data(selected_es, is_rosseti_yug)

    # Find RES for the selected TP and VL
    res = df[(df["Наименование ТП"] == selected_tp) & (df["Наименование ВЛ"] == selected_vl)]["РЭС"].iloc[0] if not df.empty else None
    if not res:
        await update.message.reply_text(
            f"Ошибка: не найден РЭС для ТП {selected_tp} и ВЛ {selected_vl}.",
            reply_markup=build_es_submenu(user_data)
        )
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END

    # Find responsible user
    responsible_id, responsible_fio = find_responsible(res, users)
    if not responsible_id:
        await update.message.reply_text(
            f"🚫 Ответственный по {res} не назначен!",
            reply_markup=build_es_submenu(user_data)
        )
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END

    # Send notification to responsible
    sender_fio = user_data["FIO"]
    notification = f"⚠️ Уведомление! Найден бездоговорной ВОЛС! {sender_fio}, {selected_tp}, {selected_vl}. Геоданные."
    await context.bot.send_message(chat_id=responsible_id, text=notification)
    await context.bot.send_location(chat_id=responsible_id, latitude=latitude, longitude=longitude)
    await context.bot.send_message(chat_id=responsible_id, text=geo_data)
    await update.message.reply_text(
        f"✅ Уведомление отправлено! {res} РЭС, {responsible_fio}.",
        reply_markup=build_es_submenu(user_data)
    )
    context.user_data["state"] = "ES_SUBMENU"
    return ConversationHandler.END

# Send TP results
async def send_tp_results(update: Update, context: ContextTypes.DEFAULT_TYPE, df, selected_es, tp_name):
    count = len(df)
    res = df["РЭС"].iloc[0] if not df.empty else selected_es
    await update.message.reply_text(f"В {res} на ТП {tp_name} найдено {count} ВОЛС с договором аренды.")
    
    for _, row in df.iterrows():
        message = (
            f"📍 ВЛ: {row['Наименование ВЛ']}\n"
            f"Опоры: {row['Опоры']}\n"
            f"Количество: {row['Количество опор']}\n"
            f"Наименование Провайдера: {row['Наименование Провайдера']}"
        )
        await update.message.reply_text(message)

# Cancel search or notification
async def cancel_action(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    users = load_user_data()
    user_data = users.get(user_id)
    selected_es = context.user_data.get("selected_es", "")
    context.user_data["state"] = "ES_SUBMENU"
    await update.message.reply_text(
        f"Действие отменено. Выберите действие для {selected_es}:", 
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
    # Conversation handler for TP search and notifications
    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp)],
            SELECT_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_tp)],
            NOTIFY_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_tp)],
            NOTIFY_VL: [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_vl)],
            NOTIFY_GEO: [MessageHandler(filters.LOCATION, notify_geo)],
        },
        fallbacks=[CommandHandler("cancel", cancel_action)],
    )

    # Add handlers
    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)
    application.add_error_handler(error_handler)

    # Start FastAPI server
    uvicorn.run(app, host="0.0.0.0", port=PORT)

if __name__ == "__main__":
    main()
