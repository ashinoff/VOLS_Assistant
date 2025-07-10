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

# Файлы логов уведомлений
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
from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler,
    ConversationHandler, ContextTypes, filters
)

from config import (
    TOKEN, SELF_URL, PORT, ZONES_CSV_URL,
    YUGO_ZAPAD_URL_UG, CENTRAL_URL_UG, ZAPAD_URL_UG, VOSTOCH_URL_UG,
    YUZH_URL_UG, SEVERO_VOSTOCH_URL_UG, YUGO_VOSTOCH_URL_UG, SEVER_URL_UG,
    YUGO_ZAPAD_URL_RK, UST_LABINSK_URL_RK, TIMASHEVSK_URL_RK,
    TIKHORETSK_URL_RK, SOCHI_URL_RK, SLAVYANSK_URL_RK,
    LENINGRADSK_URL_RK, LABINSK_URL_RK, KRASNODAR_URL_RK,
    ARMAVIR_URL_RK, ADYGEYSK_URL_RK,
    YUGO_ZAPAD_URL_UG_SP, CENTRAL_URL_UG_SP, ZAPAD_URL_UG_SP,
    VOSTOCH_URL_UG_SP, YUZH_URL_UG_SP, SEVERO_VOSTOCH_URL_UG_SP,
    YUGO_VOSTOCH_URL_UG_SP, SEVER_URL_UG_SP,
    YUGO_ZAPAD_URL_RK_SP, UST_LABINSK_URL_RK_SP, TIMASHEVSK_URL_RK_SP,
    TIKHORETSK_URL_RK_SP, SOCHI_URL_RK_SP, SLAVYANSK_URL_RK_SP,
    LENINGRADSK_URL_RK_SP, LABINSK_URL_RK_SP, KRASNODAR_URL_RK_SP,
    ARMAVIR_URL_RK_SP, ADYGEYSK_URL_RK_SP,
    NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK
)
from zones import load_zones_cached

# Logging setup
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
application: Application

# Conversation states
SEARCH_TP, SELECT_TP, REPORT_MENU = range(3)

# Menus
MAIN_MENU = [
    ["⚡️ Россети ЮГ", "⚡️ Россети Кубань"],
    ["📊 Выгрузить отчеты", "📡 Показать зоны", "🔍 Поиск ТП"]
]
REPORT_MENU_OPTS = [
    ["Уведомления о бездоговорных ВОЛС ЮГ"],
    ["Уведомления о бездоговорных ВОЛС Кубань"],
    ["⬅️ Назад"]
]

# Build ES_URL mapping
ES_URL: Dict[str, str] = {}
# UG regions
ug_regions = {
    "Юго-Западные ЭС":     (YUGO_ZAPAD_URL_UG, YUGO_ZAPAD_URL_UG_SP),
    "Центральные ЭС":      (CENTRAL_URL_UG, CENTRAL_URL_UG_SP),
    "Западные ЭС":         (ZAPAD_URL_UG, ZAPAD_URL_UG_SP),
    "Восточные ЭС":        (VOSTOCH_URL_UG, VOSTOCH_URL_UG_SP),
    "Южные ЭС":           (YUZH_URL_UG, YUZH_URL_UG_SP),
    "Северо-Восточные ЭС": (SEVERO_VOSTOCH_URL_UG, SEVERO_VOSTOCH_URL_UG_SP),
    "Юго-Восточные ЭС":    (YUGO_VOSTOCH_URL_UG, YUGO_VOSTOCH_URL_UG_SP),
    "Северные ЭС":         (SEVER_URL_UG, SEVER_URL_UG_SP),
}
for name, (full, sp) in ug_regions.items():
    if full:
        ES_URL[f"{name}_UG"] = full
    if sp:
        ES_URL[f"{name}_UG_SP"] = sp
# RK regions
rk_regions = {
    "Юго-Западные ЭС":     (YUGO_ZAPAD_URL_RK, YUGO_ZAPAD_URL_RK_SP),
    "Усть-Лабинские ЭС":   (UST_LABINSK_URL_RK, UST_LABINSK_URL_RK_SP),
    "Тимашевские ЭС":      (TIMASHEVSK_URL_RK, TIMASHEVSK_URL_RK_SP),
    "Тихорецкие ЭС":       (TIKHORETSK_URL_RK, TIKHORETSK_URL_RK_SP),
    "Сочинские ЭС":        (SOCHI_URL_RK, SOCHI_URL_RK_SP),
    "Славянские ЭС":       (SLAVYANSK_URL_RK, SLAVYANSK_URL_RK_SP),
    "Ленинградские ЭС":    (LENINGRADSK_URL_RK, LENINGRADSK_URL_RK_SP),
    "Лабинские ЭС":        (LABINSK_URL_RK, LABINSK_URL_RK_SP),
    "Краснодарские ЭС":    (KRASNODAR_URL_RK, KRASNODAR_URL_RK_SP),
    "Армавирские ЭС":      (ARMAVIR_URL_RK, ARMAVIR_URL_RK_SP),
    "Адыгейские ЭС":       (ADYGEYSK_URL_RK, ADYGEYSK_URL_RK_SP),
}
for name, (full, sp) in rk_regions.items():
    if full:
        ES_URL[f"{name}_RK"] = full
    if sp:
        ES_URL[f"{name}_RK_SP"] = sp

# Cache for CSVs
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

@lru_cache(maxsize=1)
def load_users() -> Dict[str, Dict[str, Any]]:
    try:
        df = pd.read_csv(ZONES_CSV_URL, encoding="utf-8")
        return {
            str(r["Telegram ID"]): {
                "vis": r.get("Видимость", "all").lower(),
                "FIO": r.get("ФИО", ""),
                "res": r.get("РЭС", ""),
            }
            for _, r in df.iterrows()
        }
    except Exception as e:
        logger.error("User load failed: %s", e)
        return {}

async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    users = load_users()
    if uid not in users:
        await update.message.reply_text("Нет доступа.")
        return ConversationHandler.END
    kb = ReplyKeyboardMarkup(MAIN_MENU, resize_keyboard=True)
    await update.message.reply_text(
        f"Здравствуйте, {users[uid]['FIO']}!", reply_markup=kb
    )
    return SEARCH_TP

async def handle_main(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    if text == "⬅️ Назад":
        return await start(update, ctx)
    if text == "📡 Показать зоны":
        try:
            vis_map, raw_branch_map, res_map, names, resp_map = await load_zones_cached(ctx, ZONES_CSV_URL)
            msg = [f"ID: {tid}, ФИО: {names[tid]}, Филиал: {raw_branch_map[tid]}" for tid in names]
            await update.message.reply_text("\n".join(msg) or "Нет данных по зонам.")
        except Exception:
            await update.message.reply_text("Ошибка при загрузке зон.")
        return SEARCH_TP
    if text == "📊 Выгрузить отчеты":
        kb = ReplyKeyboardMarkup(REPORT_MENU_OPTS, resize_keyboard=True)
        await update.message.reply_text("Выберите отчёт:", reply_markup=kb)
        return REPORT_MENU
    if text == "🔍 Поиск ТП":
        await update.message.reply_text("Введите название ТП:")
        return SEARCH_TP
    # Берём регион из текста
    es_key = None
    if text.startswith("⚡️ "):
        region = text.replace("⚡️ ", "")
        es_key = region + ("_UG" if "ЮГ" in region else "_RK")
        ctx.user_data['es_key'] = es_key
        await update.message.reply_text(f"Выбран регион: {region}\nВведите название ТП:")
        return SEARCH_TP
    await update.message.reply_text("Не понял команду 🤔")
    return SEARCH_TP

async def report_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    opt = update.message.text
    if opt == "⬅️ Назад":
        return await start(update, ctx)
    file_map = {
        "Уведомления о бездоговорных ВОЛС ЮГ": (NOTIFY_LOG_FILE_UG, "UG"),
        "Уведомления о бездоговорных ВОЛС Кубань": (NOTIFY_LOG_FILE_RK, "RK"),
    }
    if opt in file_map:
        fn, sheet = file_map[opt]
        df = pd.read_csv(fn)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name=sheet)
        bio.seek(0)
        await update.message.reply_document(
            document=bio,
            filename=f"log_{sheet.lower()}.xlsx",
            caption=f"Отчёт по уведомлениям бездоговорных ВОЛС {sheet}"
        )
    return SEARCH_TP

async def search_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    term = update.message.text
    es_key = ctx.user_data.get('es_key', '')
    df = await fetch_csv(ES_URL.get(es_key, ""))
    if df.empty:
        await update.message.reply_text("Ошибка загрузки данных ТП.")
        return ConversationHandler.END
    matches = df[df["Наименование ТП"].str.contains(term, case=False, na=False)]
    if matches.empty:
        await update.message.reply_text(f"ТП '{term}' не найдено.")
    else:
        for _, row in matches.iterrows():
            await update.message.reply_text(
                f"ТП: {row['Наименование ТП']}\nВЛ: {row['Наименование ВЛ']}\nОпор: {row.get('Количество опор', '')}"
            )
    return ConversationHandler.END

async def error_handler(update: Any, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error("Ошибка обработчика: %s", ctx.error)

@app.on_event("startup")
async def on_startup() -> None:
    app.state.http = httpx.AsyncClient()
    global application
    application = Application.builder().token(TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", start), MessageHandler(filters.TEXT & ~filters.COMMAND, handle_main)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp)],
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
