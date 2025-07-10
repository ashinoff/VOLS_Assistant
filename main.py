# config.py
import os
from dotenv import load_dotenv

load_dotenv()

# Telegram Bot
TOKEN = os.getenv("TOKEN")
SELF_URL = os.getenv("SELF_URL")  # e.g., https://your-service.onrender.com
PORT = int(os.getenv("PORT", 8000))

# Общая таблица зон
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL")

# Rosseti ЮГ (UG) — полные и упрощённые базы ТП
YUGO_ZAPAD_URL_UG = os.getenv("YUGO_ZAPAD_URL_UG")
YUGO_ZAPAD_URL_UG_SP = os.getenv("YUGO_ZAPAD_URL_UG_SP")
CENTRAL_URL_UG = os.getenv("CENTRAL_URL_UG")
CENTRAL_URL_UG_SP = os.getenv("CENTRAL_URL_UG_SP")
ZAPAD_URL_UG = os.getenv("ZAPAD_URL_UG")
ZAPAD_URL_UG_SP = os.getenv("ZAPAD_URL_UG_SP")
VOSTOCH_URL_UG = os.getenv("VOSTOCH_URL_UG")
VOSTOCH_URL_UG_SP = os.getenv("VOSTOCH_URL_UG_SP")
YUZH_URL_UG = os.getenv("YUZH_URL_UG")
YUZH_URL_UG_SP = os.getenv("YUZH_URL_UG_SP")
SEVERO_VOSTOCH_URL_UG = os.getenv("SEVERO_VOSTOCH_URL_UG")
SEVERO_VOSTOCH_URL_UG_SP = os.getenv("SEVERO_VOSTOCH_URL_UG_SP")
YUGO_VOSTOCH_URL_UG = os.getenv("YUGO_VOSTOCH_URL_UG")
YUGO_VOSTOCH_URL_UG_SP = os.getenv("YUGO_VOSTOCH_URL_UG_SP")
SEVER_URL_UG = os.getenv("SEVER_URL_UG")
SEVER_URL_UG_SP = os.getenv("SEVER_URL_UG_SP")

# Rosseti Кубань (RK)
YUGO_ZAPAD_URL_RK = os.getenv("YUGO_ZAPAD_URL_RK")
YUGO_ZAPAD_URL_RK_SP = os.getenv("YUGO_ZAPAD_URL_RK_SP")
UST_LABINSK_URL_RK = os.getenv("UST_LABINSK_URL_RK")
UST_LABINSK_URL_RK_SP = os.getenv("UST_LABINSK_URL_RK_SP")
TIMASHEVSK_URL_RK = os.getenv("TIMASHEVSK_URL_RK")
TIMASHEVSK_URL_RK_SP = os.getenv("TIMASHEVSK_URL_RK_SP")
TIKHORETSK_URL_RK = os.getenv("TIKHORETSK_URL_RK")
TIKHORETSK_URL_RK_SP = os.getenv("TIKHORETSK_URL_RK_SP")
SOCHI_URL_RK = os.getenv("SOCHI_URL_RK")
SOCHI_URL_RK_SP = os.getenv("SOCHI_URL_RK_SP")
SLAVYANSK_URL_RK = os.getenv("SLAVYANSK_URL_RK")
SLAVYANSK_URL_RK_SP = os.getenv("SLAVYANSK_URL_RK_SP")
LENINGRADSK_URL_RK = os.getenv("LENINGRADSK_URL_RK")
LENINGRADSK_URL_RK_SP = os.getenv("LENINGRADSK_URL_RK_SP")
LABINSK_URL_RK = os.getenv("LABINSK_URL_RK")
LABINSK_URL_RK_SP = os.getenv("LABINSK_URL_RK_SP")
KRASNODAR_URL_RK = os.getenv("KRASNODAR_URL_RK")
KRASNODAR_URL_RK_SP = os.getenv("KRASNODAR_URL_RK_SP")
ARMAVIR_URL_RK = os.getenv("ARMAVIR_URL_RK")
ARMAVIR_URL_RK_SP = os.getenv("ARMAVIR_URL_RK_SP")
ADYGEYSK_URL_RK = os.getenv("ADYGEYSK_URL_RK")
ADYGEYSK_URL_RK_SP = os.getenv("ADYGEYSK_URL_RK_SP")

# Логи уведомлений
NOTIFY_LOG_FILE_UG = os.getenv("NOTIFY_LOG_FILE_UG", "notify_log_ug.csv")
NOTIFY_LOG_FILE_RK = os.getenv("NOTIFY_LOG_FILE_RK", "notify_log_rk.csv")


# main.py
import logging
import io
from functools import lru_cache
from typing import Any, Dict

import pandas as pd
import httpx
import uvicorn
from fastapi import FastAPI, Request
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ConversationHandler, ContextTypes, filters
)

from config import *
from zones import load_zones_cached

# Logging
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
application: Application

# States
(
    STATE_MAIN,
    STATE_BRANCH_MENU,
    STATE_ACTION_MENU,
    STATE_SEARCH_TP,
    STATE_NOTIFY,
) = range(5)

# Menus
MAIN_MENU = [[KeyboardButton("⚡️ Россети ЮГ")], [KeyboardButton("⚡️ Россети Кубань")]]
UG_MENU = [[KeyboardButton(b)] for b in [
    "Юго-Западные ЭС", "Центральные ЭС", "Западные ЭС", "Восточные ЭС",
    "Южные ЭС", "Северо-Восточные ЭС", "Юго-Восточные ЭС", "Северные ЭС"
]] + [[KeyboardButton("⬅️ Назад")]]
RK_MENU = [[KeyboardButton(b)] for b in [
    "Юго-Западные ЭС", "Усть-Лабинские ЭС", "Тимашевские ЭС", "Тихорецкие ЭС",
    "Сочинские ЭС", "Славянские ЭС", "Ленинградские ЭС", "Лабинские ЭС",
    "Краснодарские ЭС", "Армавирские ЭС", "Адыгейские ЭС"
]] + [[KeyboardButton("⬅️ Назад")]]
ACTION_MENU = [[KeyboardButton("🔍 Поиск ТП")], [KeyboardButton("📨 Отправить Уведомление")],
               [KeyboardButton("⬅️ Назад")], [KeyboardButton("ℹ️ Справка")]]

# URL map
URL_MAP: Dict[str, Dict[str, tuple]] = {
    "ЮГ": {
        "Юго-Западные ЭС": (YUGO_ZAPAD_URL_UG, YUGO_ZAPAD_URL_UG_SP),
        "Центральные ЭС": (CENTRAL_URL_UG, CENTRAL_URL_UG_SP),
        "Западные ЭС": (ZAPAD_URL_UG, ZAPAD_URL_UG_SP),
        "Восточные ЭС": (VOSTOCH_URL_UG, VOSTOCH_URL_UG_SP),
        "Южные ЭС": (YUZH_URL_UG, YUZH_URL_UG_SP),
        "Северо-Восточные ЭС": (SEVERO_VOSTOCH_URL_UG, SEVERO_VOSTOCH_URL_UG_SP),
        "Юго-Восточные ЭС": (YUGO_VOSTOCH_URL_UG, YUGO_VOSTOCH_URL_UG_SP),
        "Северные ЭС": (SEVER_URL_UG, SEVER_URL_UG_SP),
    },
    "Кубань": {
        "Юго-Западные ЭС": (YUGO_ZAPAD_URL_RK, YUGO_ZAPAD_URL_RK_SP),
        "Усть-Лабинские ЭС": (UST_LABINSK_URL_RK, UST_LABINSK_URL_RK_SP),
        "Тимашевские ЭС": (TIMASHEVSK_URL_RK, TIMASHEVSK_URL_RK_SP),
        "Тихорецкие ЭС": (TIKHORETSK_URL_RK, TIKHORETSK_URL_RK_SP),
        "Сочинские ЭС": (SOCHI_URL_RK, SOCHI_URL_RK_SP),
        "Славянские ЭС": (SLAVYANSK_URL_RK, SLAVYANSK_URL_RK_SP),
        "Ленинградские ЭС": (LENINGRADSK_URL_RK, LENINGRADSK_URL_RK_SP),
        "Лабинские ЭС": (LABINSK_URL_RK, LABINSK_URL_RK_SP),
        "Краснодарские ЭС": (KRASNODAR_URL_RK, KRASNODAR_URL_RK_SP),
        "Армавирские ЭС": (ARMAVIR_URL_RK, ARMAVIR_URL_RK_SP),
        "Адыгейские ЭС": (ADYGEYSK_URL_RK, ADYGEYSK_URL_RK_SP),
    }
}

# Cache CSV
_csv_cache: Dict[str, pd.DataFrame] = {}
async def fetch_csv(url: str) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
    if url in _csv_cache:
        return _csv_cache[url]
    resp = await app.state.http.get(url)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    _csv_cache[url] = df
    return df

@lru_cache(maxsize=1)
def load_users() -> Dict[str, Any]:
    df = pd.read_csv(ZONES_CSV_URL)
    return {str(r['Telegram ID']): r for _, r in df.iterrows()}

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = str(update.effective_user.id)
    users = load_users()
    if user_id not in users:
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END
    await update.message.reply_text("Выберите сеть:", reply_markup=ReplyKeyboardMarkup(MAIN_MENU, resize_keyboard=True))
    return STATE_BRANCH_MENU

async def select_network(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    net = update.message.text.split()[-1]
    if net not in URL_MAP:
        return await start(update, ctx)
    ctx.user_data['network'] = net
    menu = UG_MENU if net == 'ЮГ' else RK_MENU
    await update.message.reply_text(f"{net}: выберите филиал:", reply_markup=ReplyKeyboardMarkup(menu, resize_keyboard=True))
    return STATE_ACTION_MENU

async def select_branch(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    if text == '⬅️ Назад':
        return await start(update, ctx)
    ctx.user_data['branch'] = text
    await update.message.reply_text(f"Филиал {text}: выберите действие:", reply_markup=ReplyKeyboardMarkup(ACTION_MENU, resize_keyboard=True))
    return STATE_SEARCH_TP

async def branch_action(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    action = update.message.text
    net = ctx.user_data['network']
    branch = ctx.user_data['branch']
    full_url, sp_url = URL_MAP[net][branch]
    if action == '⬅️ Назад':
        return await select_network(update, ctx)
    if action == 'ℹ️ Справка':
        await update.message.reply_text("Здесь будет справка по работе бота.")
        return STATE_SEARCH_TP
    if action == '🔍 Поиск ТП':
        ctx.user_data['urls'] = (full_url, sp_url)
        await update.message.reply_text("Введите название ТП:")
        return STATE_NOTIFY
    if action == '📨 Отправить Уведомление':
        # TODO: implement notification send
        await update.message.reply_text("Функция отправки уведомления пока не реализована.")
        return STATE_SEARCH_TP
    await update.message.reply_text("Не понял действие.")
    return STATE_SEARCH_TP

async def search_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    term = update.message.text
    full_url, sp_url = ctx.user_data.get('urls', (None, None))
    df = await fetch_csv(full_url)
    results = df[df['Наименование ТП'].str.contains(term, case=False, na=False)]
    if results.empty:
        await update.message.reply_text(f"ТП '{term}' не найдено.")
    else:
        for _, row in results.iterrows():
            await update.message.reply_text(
                f"ТП: {row['Наименование ТП']}\nВЛ: {row['Наименование ВЛ']}\nКоличество опор: {row.get('Количество опор', '')}"            )
    return STATE_SEARCH_TP

async def error_handler(update: Any, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Handler error: %s", ctx.error)

@app.on_event("startup")
async def on_startup() -> None:
    app.state.http = httpx.AsyncClient()
    global application
    application = Application.builder().token(TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            STATE_BRANCH_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_network)],
            STATE_ACTION_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_branch)],
            STATE_SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, branch_action)],
            STATE_NOTIFY: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp)],
        },
        fallbacks=[CommandHandler("cancel", start)],
    )
    application.add_handler(conv)
    application.add_error_handler(error_handler)
    await application.initialize()
    await application.bot.set_webhook(f"{SELF_URL}/webhook")

@app.post("/webhook")
async def webhook(request: Request) -> Dict[str, str]:
    update = Update.de_json(await request.json(), application.bot)
    await application.process_update(update)
    return {"status": "ok"}

@app.on_event("shutdown")
async def on_shutdown() -> None:
    await application.stop()
    await app.state.http.aclose()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
