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

# Rosseti ЮГ (UG) — полные базы ТП
YUGO_ZAPAD_URL_UG = os.getenv("YUGO_ZAPAD_URL_UG")
CENTRAL_URL_UG = os.getenv("CENTRAL_URL_UG")
ZAPAD_URL_UG = os.getenv("ZAPAD_URL_UG")
VOSTOCH_URL_UG = os.getenv("VOSTOCH_URL_UG")
YUZH_URL_UG = os.getenv("YUZH_URL_UG")
SEVERO_VOSTOCH_URL_UG = os.getenv("SEVERO_VOSTOCH_URL_UG")
YUGO_VOSTOCH_URL_UG = os.getenv("YUGO_VOSTOCH_URL_UG")
SEVER_URL_UG = os.getenv("SEVER_URL_UG")

# Rosseti Кубань (RK) — полные базы ТП
YUGO_ZAPAD_URL_RK = os.getenv("YUGO_ZAPAD_URL_RK")
UST_LABINSK_URL_RK = os.getenv("UST_LABINSK_URL_RK")
TIMASHEVSK_URL_RK = os.getenv("TIMASHEVSK_URL_RK")
TIKHORETSK_URL_RK = os.getenv("TIKHORETSK_URL_RK")
SOCHI_URL_RK = os.getenv("SOCHI_URL_RK")
SLAVYANSK_URL_RK = os.getenv("SLAVYANSK_URL_RK")
LENINGRADSK_URL_RK = os.getenv("LENINGRADSK_URL_RK")
LABINSK_URL_RK = os.getenv("LABINSK_URL_RK")
KRASNODAR_URL_RK = os.getenv("KRASNODAR_URL_RK")
ARMAVIR_URL_RK = os.getenv("ARMAVIR_URL_RK")
ADYGEYSK_URL_RK = os.getenv("ADYGEYSK_URL_RK")

# Упрощённые базы ТП (для геолокации в уведомлении)
YUGO_ZAPAD_URL_UG_SP = os.getenv("YUGO_ZAPAD_URL_UG_SP")
CENTRAL_URL_UG_SP = os.getenv("CENTRAL_URL_UG_SP")
ZAPAD_URL_UG_SP = os.getenv("ZAPAD_URL_UG_SP")
VOSTOCH_URL_UG_SP = os.getenv("VOSTOCH_URL_UG_SP")
YUZH_URL_UG_SP = os.getenv("YUZH_URL_UG_SP")
SEVERO_VOSTOCH_URL_UG_SP = os.getenv("SEVERO_VOSTOCH_URL_UG_SP")
YUGO_VOSTOCH_URL_UG_SP = os.getenv("YUGO_VOSTOCH_URL_UG_SP")
SEVER_URL_UG_SP = os.getenv("SEVER_URL_UG_SP")

YUGO_ZAPAD_URL_RK_SP = os.getenv("YUGO_ZAPAD_URL_RK_SP")
UST_LABINSK_URL_RK_SP = os.getenv("UST_LABINSK_URL_RK_SP")
TIMASHEVSK_URL_RK_SP = os.getenv("TIMASHEVSK_URL_RK_SP")
TIKHORETSK_URL_RK_SP = os.getenv("TIKHORETSK_URL_RK_SP")
SOCHI_URL_RK_SP = os.getenv("SOCHI_URL_RK_SP")
SLAVYANSK_URL_RK_SP = os.getenv("SLAVYANSK_URL_RK_SP")
LENINGRADSK_URL_RK_SP = os.getenv("LENINGRADSK_URL_RK_SP")
LABINSK_URL_RK_SP = os.getenv("LABINSK_URL_RK_SP")
KRASNODAR_URL_RK_SP = os.getenv("KRASNODAR_URL_RK_SP")
ARMAVIR_URL_RK_SP = os.getenv("ARMAVIR_URL_RK_SP")
ADYGEYSK_URL_RK_SP = os.getenv("ADYGEYSK_URL_RK_SP")

# Логи уведомлений
NOTIFY_LOG_FILE_UG = os.getenv("NOTIFY_LOG_FILE_UG", "notify_log_ug.csv")
NOTIFY_LOG_FILE_RK = os.getenv("NOTIFY_LOG_FILE_RK", "notify_log_rk.csv")


# main.py
import logging
import io
from functools import lru_cache
from typing import Any, Dict, List, Optional

import pandas as pd
import httpx
import uvicorn
from fastapi import FastAPI, Request
from telegram import (
    Update,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ConversationHandler,
    ContextTypes,
    filters,
)

from config import (
    TOKEN,
    ZONES_CSV_URL,
    SELF_URL,
    PORT,
    # URLs для TP данных
    YUGO_ZAPAD_URL_UG,
    CENTRAL_URL_UG,
    ZAPAD_URL_UG,
    VOSTOCH_URL_UG,
    YUZH_URL_UG,
    SEVERO_VOSTOCH_URL_UG,
    YUGO_VOSTOCH_URL_UG,
    SEVER_URL_UG,
    YUGO_ZAPAD_URL_RK,
    UST_LABINSK_URL_RK,
    TIMASHEVSK_URL_RK,
    TIKHORETSK_URL_RK,
    SOCHI_URL_RK,
    SLAVYANSK_URL_RK,
    LENINGRADSK_URL_RK,
    LABINSK_URL_RK,
    KRASNODAR_URL_RK,
    ARMAVIR_URL_RK,
    ADYGEYSK_URL_RK,
    # SP URLs
    YUGO_ZAPAD_URL_UG_SP,
    CENTRAL_URL_UG_SP,
    ZAPAD_URL_UG_SP,
    VOSTOCH_URL_UG_SP,
    YUZH_URL_UG_SP,
    SEVERO_VOSTOCH_URL_UG_SP,
    YUGO_VOSTOCH_URL_UG_SP,
    SEVER_URL_UG_SP,
    YUGO_ZAPAD_URL_RK_SP,
    UST_LABINSK_URL_RK_SP,
    TIMASHEVSK_URL_RK_SP,
    TIKHORETSK_URL_RK_SP,
    SOCHI_URL_RK_SP,
    SLAVYANSK_URL_RK_SP,
    LENINGRADSK_URL_RK_SP,
    LABINSK_URL_RK_SP,
    KRASNODAR_URL_RK_SP,
    ARMAVIR_URL_RK_SP,
    ADYGEYSK_URL_RK_SP,
    # Логи
    NOTIFY_LOG_FILE_UG,
    NOTIFY_LOG_FILE_RK,
)
from zones import load_zones_cached

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# FastAPI и Telegram App
app = FastAPI()
application: Application

# Состояния для ConversationHandler
SEARCH_TP, SELECT_TP, REPORT_MENU, NOTIFY_TP, NOTIFY_VL, NOTIFY_GEO = range(6)

# Меню и опции
MAIN_MENU = [
    "⚡️ Россети ЮГ",
    "⚡️ Россети Кубань",
    "📊 Выгрузить отчеты",
    "📡 Показать зоны",
    "🔍 Поиск ТП",
    "⬅️ Назад",
]
REPORT_MENU_OPTS = [
    "Уведомления о бездоговорных ВОЛС ЮГ",
    "Уведомления о бездоговорных ВОЛС Кубань",
    "Справочник контрагентов",
    "⬅️ Назад",
]

# Mapping URLs
ES_URL = {
    **{name + suffix: url
       for name, (url, _sp) in {
           "Юго-Западные ЭС": (YUGO_ZAPAD_URL_UG, YUGO_ZAPAD_URL_UG_SP),
           "Центральные ЭС": (CENTRAL_URL_UG, CENTRAL_URL_UG_SP),
           "Западные ЭС": (ZAPAD_URL_UG, ZAPAD_URL_UG_SP),
           "Восточные ЭС": (VOSTOCH_URL_UG, VOSTOCH_URL_UG_SP),
           "Южные ЭС": (YUZH_URL_UG, YUZH_URL_UG_SP),
           "Северо-Восточные ЭС": (SEVERO_VOSTOCH_URL_UG, SEVERO_VOSTOCH_URL_UG_SP),
           "Юго-Восточные ЭС": (YUGO_VOSTOCH_URL_UG, YUGO_VOSTOCH_URL_UG_SP),
           "Северные ЭС": (SEVER_URL_UG, SEVER_URL_UG_SP),
       }.items()
       for suffix in ("_UG", "_UG_SP")
       for url in [_[0] if suffix == "_UG" else _[1]]
       if url},
    **{name + suffix: url
       for name, (rk, rk_sp) in {
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
       }.items()
       for suffix in ("_RK", "_RK_SP")
       for url in [rk if suffix == "_RK" else rk_sp]
       if url},
}

@lru_cache(maxsize=1)
def load_users() -> Dict[str, Dict[str, Any]]:
    try:
        df = pd.read_csv(ZONES_CSV_URL, encoding="utf-8")
        return {
            str(r["Telegram ID"]): {
                "vis": r["Видимость"].lower(),
                "FIO": r["ФИО"],
                "res": r["РЭС"],
                "resp": r["Ответственный"],
            }
            for _, r in df.iterrows()
        }
    except Exception as e:
        logger.error("User load fail: %s", e)
        return {}

def can(user, req) -> bool:
    return user.get("vis") in ("all", req.lower())

_csv_cache: Dict[str, pd.DataFrame] = {}

async def fetch_csv(url: str) -> pd.DataFrame:
    if not url:
        return pd.DataFrame()
    if url in _csv_cache:
        return _csv_cache[url]
    resp = await app.state.http.get(url)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text), encoding="utf-8")
    _csv_cache[url] = df
    return df

# Обработчики
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    u = load_users().get(uid)
    if not u:
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END
    ctx.user_data.clear()
    ctx.user_data.update(state="MAIN")
    kb = ReplyKeyboardMarkup([[m] for m in MAIN_MENU], resize_keyboard=True)
    await update.message.reply_text(f"Здравствуйте, {u['FIO']}!", reply_markup=kb)
    return SEARCH_TP

async def handle_main(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    uid = str(update.effective_user.id)
    u = load_users()[uid]
    if text == "⬅️ Назад":
        return await start(update, ctx)
    if text.startswith("⚡️ Россети"):
        es = text.replace("⚡️ ", "")
        is_ug = es.endswith("ЮГ")
        ctx.user_data.update(state="ES", es=es, ug=is_ug)
        kb = ReplyKeyboardMarkup([[m] for m in ES_MENU], resize_keyboard=True)
        await update.message.reply_text(es, reply_markup=kb)
        return SELECT_TP
    if text == "📊 Выгрузить отчеты":
        ctx.user_data['state'] = 'REPORT'
        kb = ReplyKeyboardMarkup([[m] for m in REPORT_MENU_OPTS], resize_keyboard=True)
        await update.message.reply_text("Выберите отчёт:", reply_markup=kb)
        return REPORT_MENU
    if text == "📡 Показать зоны":
        try:
            vis_map, raw_branch_map, res_map, names, resp_map = await load_zones_cached(ctx, ZONES_CSV_URL)
            msg = [f"ID: {tid}, ФИО: {names[tid]}, Филиал: {raw_branch_map[tid]}" for tid in names]
            await update.message.reply_text("\n".join(msg) or "Нет данных по зонам.")
        except Exception:
            await update.message.reply_text("Ошибка при загрузке зон.")
        return SEARCH_TP
    if text == "🔍 Поиск ТП":
        return await search_tp(update, ctx)
    await update.message.reply_text("Не понял команду 🤔")
    return SEARCH_TP

async def report_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    opt = update.message.text
    if opt == "⬅️ Назад":
        await start(update, ctx)
        return SEARCH_TP
    if "ЮГ" in opt or "Кубань" in opt:
        fn = NOTIFY_LOG_FILE_UG if "ЮГ" in opt else NOTIFY_LOG_FILE_RK
        df = pd.read_csv(fn)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            sheet = "UG" if "ЮГ" in opt else "RK"
            df.to_excel(writer, index=False, sheet_name=sheet)
        bio.seek(0)
        caption = f"Отчёт по уведомленияем бездоговорных ВОЛС {'ЮГ' if 'ЮГ' in opt else 'Кубань'}"
        await update.message.reply_document(
            document=bio,
            filename=f"log_{sheet.lower()}.xlsx",
            caption=caption
        )
        return SEARCH_TP
    if "Справочник" in opt:
        await update.message.reply_text("Скоро!", reply_markup=ReplyKeyboardMarkup([[m] for m in REPORT_MENU_OPTS], resize_keyboard=True))
        return REPORT_MENU
    return SEARCH_TP

async def search_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    term = update.message.text
    es = ctx.user_data['es']
    is_ug = ctx.user_data['ug']
    url_key = es + ("_UG" if is_ug else "_RK")
    df = await fetch_csv(ES_URL.get(url_key, ""))
    if df.empty:
        await update.message.reply_text("Ошибка загрузки.")
        return ConversationHandler.END
    exact = df[df["Наименование ТП"] == term]
    if not exact.empty:
        return await send_results(update, ctx, exact)
    opts = [o for o in df["Наименование ТП"].drop_duplicates() if term.lower() in o.lower()][:10]
    if not opts:
        await update.message.reply_text(f"Не найдено {term}")
        return SEARCH_TP
    ctx.user_data['opts'] = opts
    kb = ReplyKeyboardMarkup([[o] for o in opts] + [["⬅️ Назад"]], resize_keyboard=True)
    await update.message.reply_text("Варианты:", reply_markup=kb)
    return SELECT_TP

async def select_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sel = update.message.text
    if sel == "⬅️ Назад":
        return await handle_main(update, ctx)
    es = ctx.user_data['es']
    is_ug = ctx.user_data['ug']
    url_key = es + ("_UG" if is_ug else "_RK")
    df = await fetch_csv(ES_URL.get(url_key, ""))
    res = df[df["Наименование ТП"] == sel]
    return await send_results(update, ctx, res)

async def send_results(update: Update, ctx: ContextTypes.DEFAULT_TYPE, df: pd.DataFrame) -> int:
    if df.empty:
        await update.message.reply_text("Пусто.")
        return ConversationHandler.END
    for _, r in df.iterrows():
        await update.message.reply_text(
            f"ВЛ: {r['Наименование ВЛ']}\nОпоры: {r['Опоры']}\nКол-во: {r['Количество опор']}"
        )
    return ConversationHandler.END

async def error_handler(update: Any, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Error %s", ctx.error)

@app.on_event("startup")
async def on_startup() -> None:
    app.state.http = httpx.AsyncClient()
    global application
    application = Application.builder().token(TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp)],
            SELECT_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_tp)],
            REPORT_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, report_menu)],
        },
        fallbacks=[CommandHandler("cancel", start)],
    )
    application.add_handler(conv)
    application.add_error_handler(error_handler)
    await application.initialize()
    await application.bot.set_webhook(f"{SELF_URL}/webhook")

@app.post("/webhook")
async def webhook(request: Request) -> Dict[str, str]:
    upd = Update.de_json(await request.json(), application.bot)
    await application.process_update(upd)
    return {"status": "ok"}

@app.on_event("shutdown")
async def on_shutdown() -> None:
    await application.stop()
    await app.state.http.aclose()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
