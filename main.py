import logging
import io
from functools import lru_cache
from typing import Any, Dict, List

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

# Logging setup
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
application: Application

# Conversation states
(
    STATE_MAIN,
    STATE_BRANCH_MENU,
    STATE_ACTION_MENU,
    STATE_SEARCH_TP,
    STATE_NOTIFY,
    STATE_VARIANT,
) = range(6)

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

# URL map for regions
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

# CSV cache
_csv_cache: Dict[str, pd.DataFrame] = {}
async def fetch_csv(url: str) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
    if url in _csv_cache:
        return _csv_cache[url]
    resp = await app.state.http.get(url, follow_redirects=True)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    _csv_cache[url] = df
    return df

@lru_cache(maxsize=1)
def load_users() -> Dict[str, Any]:
    df = pd.read_csv(ZONES_CSV_URL)
    return {str(r['Telegram ID']): r for _, r in df.iterrows()}

# Show search results for exact match
async def show_results(update: Update, ctx: ContextTypes.DEFAULT_TYPE, tp_name: str, df: pd.DataFrame) -> int:
    filtered = df[df['Наименование ТП'] == tp_name]
    count = len(filtered)
    await update.message.reply_text(f"На {tp_name} найдено {count} договоров ВОЛС.")
    for _, row in filtered.iterrows():
        await update.message.reply_text(
            f"📡 {row.get('Наименование ВЛ','')}\n"
            f"Опоры: {row.get('Опоры','')} ({row.get('Количество опор','')})\n"
            f"Провайдер: {row.get('Наименование Провайдера','')}"
        )
    # Return to action menu
    await update.message.reply_text("Действия:", reply_markup=ReplyKeyboardMarkup(ACTION_MENU, resize_keyboard=True))
    return STATE_ACTION_MENU

# Handle variant selection
async def variant_selection(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text
    if choice == '⬅️ Назад':
        return await select_branch(update, ctx)
    variants = ctx.user_data.get('variants', [])
    if choice not in variants:
        await update.message.reply_text("Неизвестный вариант. Попробуйте ещё раз или нажмите Назад.")
        return STATE_VARIANT
    full_url, _ = ctx.user_data['urls']
    df = await fetch_csv(full_url)
    return await show_results(update, ctx, choice, df)

# Search TP with normalization and variants
async def search_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    term = update.message.text or ''
    norm = term.lower().replace(' ', '').replace('-', '')
    full_url, _ = ctx.user_data.get('urls', (None, None))
    df = await fetch_csv(full_url)
    if df.empty or 'Наименование ТП' not in df.columns:
        await update.message.reply_text('Ошибка загрузки данных ТП.')
        return STATE_ACTION_MENU
    df['__norm'] = df['Наименование ТП'].str.lower().str.replace(' ', '').str.replace('-', '')
    matches = df[df['__norm'].str.contains(norm, na=False)]
    if matches.empty:
        await update.message.reply_text(f"ТП '{term}' не найдено.")
        return STATE_ACTION_MENU
    tps = matches['Наименование ТП'].drop_duplicates().tolist()
    if len(tps) == 1:
        return await show_results(update, ctx, tps[0], matches)
    opts = tps[:10]
    ctx.user_data['variants'] = opts
    kb = ReplyKeyboardMarkup([[KeyboardButton(o)] for o in opts] + [[KeyboardButton('⬅️ Назад')]], resize_keyboard=True)
    await update.message.reply_text('Найдены варианты ТП, выберите один:', reply_markup=kb)
    return STATE_VARIANT

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    user_id = str(update.effective_user.id)
    users = load_users()
    if user_id not in users:
        await update.message.reply_text('Нет доступа.')
        return ConversationHandler.END
    await update.message.reply_text('Выберите сеть:', reply_markup=ReplyKeyboardMarkup(MAIN_MENU, resize_keyboard=True))
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
    net = ctx.user_data['network']
    full_url, sp_url = URL_MAP[net][text]
    ctx.user_data['urls'] = (full_url, sp_url)
    await update.message.reply_text(f"Филиал {text}: выберите действие:", reply_markup=ReplyKeyboardMarkup(ACTION_MENU, resize_keyboard=True))
    return STATE_SEARCH_TP

async def branch_action(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    action = update.message.text
    if action == '⬅️ Назад':
        return await select_network(update, ctx)
    if action == 'ℹ️ Справка':
        await update.message.reply_text('Здесь будет справка по работе бота.')
        return STATE_SEARCH_TP
    if action == '🔍 Поиск ТП':
        await update.message.reply_text('Введите название ТП:')
        return STATE_NOTIFY
    if action == '📨 Отправить Уведомление':
        await update.message.reply_text('Функция отправки уведомления пока не реализована.')
        return STATE_SEARCH_TP
    await update.message.reply_text('Не понял действие.')
    return STATE_SEARCH_TP

async def error_handler(update: Any, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error('Handler error: %s', ctx.error)

@app.on_event('startup')
async def on_startup() -> None:
    app.state.http = httpx.AsyncClient()
    global application
    application = Application.builder().token(TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler('start', start)],
        states={
            STATE_BRANCH_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_network)],
            STATE_ACTION_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_branch)],
            STATE_SEARCH_TP:   [MessageHandler(filters.TEXT & ~filters.COMMAND, branch_action)],
            STATE_NOTIFY:      [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp)],
            STATE_VARIANT:     [MessageHandler(filters.TEXT & ~filters.COMMAND, variant_selection)],
        },
        fallbacks=[CommandHandler('cancel', start)],
    )
    application.add_handler(conv)
    application.add_error_handler(error_handler)
    await application.initialize()
    await application.bot.set_webhook(f"{SELF_URL}/webhook")

@app.post('/webhook')
async def webhook(request: Request) -> Dict[str, str]:
    update = Update.de_json(await request.json(), application.bot)
    await application.process_update(update)
    return {'status': 'ok'}

@app.on_event('shutdown')
async def on_shutdown() -> None:
    await application.stop()
    await app.state.http.aclose()

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=PORT)
