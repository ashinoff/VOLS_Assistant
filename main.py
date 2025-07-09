import logging
import pandas as pd
import uvicorn
import asyncio
import re
import requests
import sqlite3
import datetime
import os
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

# States for ConversationHandler (changed to strings)
SEARCH_TP = "search_tp"
SELECT_TP = "select_tp"
NOTIFY_TP = "notify_tp"
NOTIFY_VL = "notify_vl"
NOTIFY_GEO = "notify_geo"
REPORTS_MENU = "reports_menu"

# SQLite database setup
def init_db():
    conn = sqlite3.connect("notifications.db")
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS notifications_yug (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch TEXT,
            res TEXT,
            sender_fio TEXT,
            sender_id TEXT,
            receiver_fio TEXT,
            receiver_id TEXT,
            timestamp TEXT,
            coordinates TEXT
        )
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS notifications_kuban (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            branch TEXT,
            res TEXT,
            sender_fio TEXT,
            sender_id TEXT,
            receiver_fio TEXT,
            receiver_id TEXT,
            timestamp TEXT,
            coordinates TEXT
        )
    """)
    conn.commit()
    conn.close()

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
        return dfAbstract
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
def has_access(user_data, required_visibility, required_branch=None):
    if not user_data:
        return False
    user_visibility = user_data.get("Visibility", "").lower()
    user_branch = user_data.get("Branch", "").lower()

    if user_visibility not in ["all", "rk", "ug"]:
        return False
    if required_visibility.lower() == "all":
        visibility_match = True
    elif required_visibility.lower() in ["rk", "ug"]:
        visibility_match = user_visibility in ["all", required_visibility.lower()]
    else:
        visibility_match = False

    if required_branch:
        required_branch = required_branch.lower()
        branch_match = user_branch in ["all", required_branch]
    else:
        branch_match = True

    return visibility_match and branch_match

# Define main menu buttons with visibility
MAIN_MENU = [
    {"text": "⚡️ Россети ЮГ", "visibility": "UG"},
    {"text": "⚡️ Россети Кубань", "visibility": "RK"},
    {"text": "📊 Выгрузить отчеты", "visibility": "All"},
    {"text": "📞 Телефонный справочник", "visibility": "All"},
    {"text": "📖 Руководство пользователя", "visibility": "All"},
    {"text": "📚 Справка", "visibility": "All"},
    {"text": "⬅️ Назад", "visibility": "All"},
]

# Define Rosseti Yug submenu with visibility and branch
ROSSETI_YUG_MENU = [
    {"text": "⚡️ Юго-Западные ЭС", "visibility": "UG", "branch": "Юго-Западные ЭС"},
    {"text": "⚡️ Центральные ЭС", "visibility": "UG", "branch": "Центральные ЭС"},
    {"text": "⚡️ Западные ЭС", "visibility": "UG", "branch": "Западные ЭС"},
    {"text": "⚡️ Восточные ЭС", "visibility": "UG", "branch": "Восточные ЭС"},
    {"text": "⚡️ Южные ЭС", "visibility": "UG", "branch": "Южные ЭС"},
    {"text": "⚡️ Северо-Восточные ЭС", "visibility": "UG", "branch": "Северо-Восточные ЭС"},
    {"text": "⚡️ Юго-Восточные ЭС", "visibility": "UG", "branch": "Юго-Восточные ЭС"},
    {"text": "⚡️ Северные ЭС", "visibility": "UG", "branch": "Северные ЭС"},
    {"text": "⬅️ Назад", "visibility": "All"},
]

# Define Rosseti Kuban submenu with visibility and branch
ROSSETI_KUBAN_MENU = [
    {"text": "⚡️ Юго-Западные ЭС", "visibility": "RK", "branch": "Юго-Западные ЭС"},
    {"text": "⚡️ Усть-Лабинские ЭС", "visibility": "RK", "branch": "Усть-Лабинские ЭС"},
    {"text": "⚡️ Тимашевские ЭС", "visibility": "RK", "branch": "Тимашевские ЭС"},
    {"text": "⚡️ Тихорецкие ЭС", "visibility": "RK", "branch": "Тихорецкие ЭС"},
    {"text": "⚡️ Сочинские ЭС", "visibility": "RK", "branch": "Сочинские ЭС"},
    {"text": "⚡️ Славянские ЭС", "visibility": "RK", "branch": "Славянские ЭС"},
    {"text": "⚡️ Ленинградские ЭС", "visibility": "RK", "branch": "Ленинградские ЭС"},
    {"text": "⚡️ Лабинские ЭС", "visibility": "RK", "branch": "Лабинские ЭС"},
    {"text": "⚡️ Краснодарские ЭС", "visibility": "RK", "branch": "Краснодарские ЭС"},
    {"text": "⚡️ Армавирские ЭС", "visibility": "RK", "branch": "Армавирские ЭС"},
    {"text": "⚡️ Адыгейские ЭС", "visibility": "RK", "branch": "Адыгейские ЭС"},
    {"text": "⬅️ Назад", "visibility": "All"},
]

# Define ES submenu with visibility
ES_SUBMENU = [
    {"text": "🔍 Поиск по ТП", "visibility": "All"},
    {"text": "🔔 Отправить уведомление", "visibility": "All"},
    {"text": "📚 Справка", "visibility": "All"},
    {"text": "⬅️ Назад", "visibility": "All"},
]

# Define Reports submenu
REPORTS_MENU = [
    {"text": "📤 Выгрузка уведомлений Россети ЮГ", "visibility": "UG"},
    {"text": "📤 Выгрузка уведомлений Россети Кубань", "visibility": "RK"},
    {"text": "⬅️ Назад", "visibility": "All"},
]

# Build main menu based on user visibility
def build_main_menu(user_data):
    keyboard = [[button["text"]] for button in MAIN_MENU if has_access(user_data, button["visibility"])]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build Rosseti Yug submenu based on user visibility and branch
def build_rosseti_yug_menu(user_data):
    keyboard = [[button["text"]] for button in ROSSETI_YUG_MENU if has_access(user_data, button["visibility"], button.get("branch"))]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build Rosseti Kuban submenu based on user visibility and branch
def build_rosseti_kuban_menu(user_data):
    keyboard = [[button["text"]] for button in ROSSETI_KUBAN_MENU if has_access(user_data, button["visibility"], button.get("branch"))]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build ES submenu based on user visibility
def build_es_submenu(user_data):
    keyboard = [[button["text"]] for button in ES_SUBMENU if has_access(user_data, button["visibility"])]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

# Build Reports submenu
def build_reports_menu(user_data):
    keyboard = [[button["text"]] for button in REPORTS_MENU if has_access(user_data, button["visibility"])]
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
    search_term = re.sub(r'[- ]', '', search_term.lower())
    matches = []
    for tp in df["Наименование ТП"].dropna().unique():
        if not isinstance(tp, str):
            continue
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

    if state == "MAIN_MENU":
        if text == "⚡️ Россети ЮГ" and has_access(user_data, "UG"):
            context.user_data["state"] = "ROSSETI_YUG"
            context.user_data["previous_state"] = "MAIN_MENU"
            context.user_data["is_rosseti_yug"] = True
            await update.message.reply_text(
                "Выберите ЭС:", reply_markup=build_rosseti_yug_menu(user_data)
            )
        elif text == "⚡️ Россети Кубань" and has_access(user_data, "RK"):
            context.user_data["state"] = "ROSSETI_KUBAN"
            context.user_data["previous_state"] = "MAIN_MENU"
            context.user_data["is_rosseti_yug"] = False
            await update.message.reply_text(
                "Выберите ЭС:", reply_markup=build_rosseti_kuban_menu(user_data)
            )
        elif text == "📊 Выгрузить отчеты" and has_access(user_data, "All"):
            context.user_data["state"] = "REPORTS_MENU"
            context.user_data["previous_state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите отчет:", reply_markup=build_reports_menu(user_data)
            )
        elif text == "📞 Телефонный справочник" and has_access(user_data, "All"):
            await update.message.reply_text("Телефонный справочник 📞. Функционал в разработке.")
        elif text == "📖 Руководство пользователя" and has_access(user_data, "All"):
            await update.message.reply_text("Руководство пользователя 📖. Функционал в разработке.")
        elif text == "📚 Справка" and has_access(user_data, "All"):
            await update.message.reply_text("Справка 📚. Функционал в разработке.")
        elif text == "⬅️ Назад" and has_access(user_data, "All"):
            await start(update, context)
        else:
            await update.message.reply_text("Пожалуйста, выберите действие из меню.")
        return ConversationHandler.END

    elif state == "ROSSETI_YUG":
        if text == "⬅️ Назад" and has_access(user_data, "All"):
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_main_menu(user_data)
            )
        else:
            for button in ROSSETI_YUG_MENU:
                if text == button["text"] and has_access(user_data, button["visibility"], button.get("branch")):
                    context.user_data["state"] = "ES_SUBMENU"
                    context.user_data["selected_es"] = text.replace("⚡️ ", "")
                    context.user_data["previous_state"] = "ROSSETI_YUG"
                    await update.message.reply_text(
                        f"Вы выбрали {text.replace('⚡️ ', '')}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                    )
                    return ConversationHandler.END
            await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")
        return ConversationHandler.END

    elif state == "ROSSETI_KUBAN":
        if text == "⬅️ Назад" and has_access(user_data, "All"):
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_main_menu(user_data)
            )
        else:
            for button in ROSSETI_KUBAN_MENU:
                if text == button["text"] and has_access(user_data, button["visibility"], button.get("branch")):
                    context.user_data["state"] = "ES_SUBMENU"
                    context.user_data["selected_es"] = text.replace("⚡️ ", "")
                    context.user_data["previous_state"] = "ROSSETI_KUBAN"
                    await update.message.reply_text(
                        f"Вы выбрали {text.replace('⚡️ ', '')}. Выберите действие:", reply_markup=build_es_submenu(user_data)
                    )
                    return ConversationHandler.END
            await update.message.reply_text("Пожалуйста, выберите ЭС из меню.")
        return ConversationHandler.END

    elif state == "ES_SUBMENU":
        selected_es = context.user_data.get("selected_es", "")
        if text == "🔍 Поиск по ТП" and has_access(user_data, "All"):
            await update.message.reply_text(
                f"Введите наименование ТП для поиска в {selected_es}:", reply_markup=ReplyKeyboardRemove()
            )
            return SEARCH_TP
        elif text == "🔔 Отправить уведомление" and has_access(user_data, "All"):
            back_button = [["⬅️ Назад"]]
            await update.message.reply_text(
                "Введите наименование ТП где обнаружен бездоговорной ВОЛС:", reply_markup=ReplyKeyboardMarkup(back_button, resize_keyboard=True)
            )
            return NOTIFY_TP
        elif text == "📚 Справка" and has_access(user_data, "All"):
            await update.message.reply_text(f"Справка 📚. Функционал в разработке.")
        elif text == "⬅️ Назад" and has_access(user_data, "All"):
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

    elif state == "REPORTS_MENU":
        if text == "📤 Выгрузка уведомлений Россети ЮГ" and has_access(user_data, "UG"):
            await export_to_bot(update, context, "yug")
            context.user_data["state"] = "REPORTS_MENU"
            await update.message.reply_text(
                "Выберите отчет:", reply_markup=build_reports_menu(user_data)
            )
        elif text == "📤 Выгрузка уведомлений Россети Кубань" and has_access(user_data, "RK"):
            await export_to_bot(update, context, "kuban")
            context.user_data["state"] = "REPORTS_MENU"
            await update.message.reply_text(
                "Выберите отчет:", reply_markup=build_reports_menu(user_data)
            )
        elif text == "⬅️ Назад" and has_access(user_data, "All"):
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_main_menu(user_data)
            )
        else:
            await update.message.reply_text("Пожалуйста, выберите действие из меню.")
        return ConversationHandler.END

# Export to bot
async def export_to_bot(update: Update, context: ContextTypes.DEFAULT_TYPE, export_type: str):
    table = "notifications_yug" if export_type == "yug" else "notifications_kuban"
    filename = f"report_{export_type}.xlsx"
    
    try:
        conn = sqlite3.connect("notifications.db")
        df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
        conn.close()

        if df.empty:
            await update.message.reply_text("Нет данных для выгрузки.")
            return

        df.to_excel(filename, index=False)
        with open(filename, "rb") as f:
            await update.message.reply_document(document=f, filename=filename)
        os.remove(filename)
        await update.message.reply_text("Отчет успешно отправлен в бот!")
    except Exception as e:
        logger.error(f"Ошибка при выгрузке отчета: {e}")
        await update.message.reply_text("Ошибка при выгрузке отчета. Попробуйте позже.")

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

    exact_match = df[df["Наименование ТП"] == search_term]
    if not exact_match.empty:
        await send_tp_results(update, context, exact_match, selected_es, search_term)
        context.user_data["state"] = "ES_SUBMENU"
        await update.message.reply_text(
            f"Выберите действие для {selected_es}:", reply_markup=build_es_submenu(user_data)
        )
        return ConversationHandler.END

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

    res = df[(df["Наименование ТП"] == selected_tp) & (df["Наименование ВЛ"] == selected_vl)]["РЭС"].iloc[0] if not df.empty else None
    if not res:
        await update.message.reply_text(
            f"Ошибка: не найден РЭС для ТП {selected_tp} и ВЛ {selected_vl}.",
            reply_markup=build_es_submenu(user_data)
        )
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END

    responsible_id, responsible_fio = find_responsible(res, users)
    if not responsible_id:
        await update.message.reply_text(
            f"🚫 Ответственный по {res} не назначен!",
            reply_markup=build_es_submenu(user_data)
        )
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END

    # Log notification to SQLite
    conn = sqlite3.connect("notifications.db")
    cursor = conn.cursor()
    table = "notifications_yug" if is_rosseti_yug else "notifications_kuban"
    branch = "Россети ЮГ" if is_rosseti_yug else "Россети Кубань"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        f"""
        INSERT INTO {table} (branch, res, sender_fio, sender_id, receiver_fio, receiver_id, timestamp, coordinates)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (branch, res, user_data["FIO"], user_id, responsible_fio, responsible_id, timestamp, geo_data)
    )
    conn.commit()
    conn.close()

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
    res = df.get("РЭС", pd.Series([selected_es])).iloc[0] if not df.empty else selected_es
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
        f"Действие отменено. Пожалуйста, выберите действие для {selected_es}:", 
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

# Lifespan event handler for startup and shutdown
from contextlib import asynccontextmanager
@asynccontextmanager
async def lifespan(app):
    init_db()
    webhook_url = f"{SELF_URL}/webhook"
    await application.bot.set_webhook(url=webhook_url)
    logger.info(f"Webhook set to {webhook_url}")
    await application.initialize()
    try:
        yield
    finally:
        await application.stop()

app.lifespan = lifespan

def main():
    # Conversation handler for TP search, notifications, and reports
    conv_handler = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT, handle_message)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT, search_tp)],
            SELECT_TP: [MessageHandler(filters.TEXT, select_tp)],
            NOTIFY_TP: [MessageHandler(filters.TEXT, notify_tp)],
            NOTIFY_VL: [MessageHandler(filters.TEXT, notify_vl)],
            NOTIFY_GEO: [MessageHandler(filters.LOCATION, notify_geo)],
            REPORTS_MENU: [MessageHandler(filters.TEXT, handle_message)],
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
