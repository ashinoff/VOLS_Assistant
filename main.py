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
    # URLs for TP data
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
    # Logs
    NOTIFY_LOG_FILE_UG,
    NOTIFY_LOG_FILE_RK,
)

# Logging setup
logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# App init
app = FastAPI()
application: Application

# States
SEARCH_TP, SELECT_TP, NOTIFY_TP, NOTIFY_VL, NOTIFY_GEO, REPORT_MENU = range(6)

MAIN_MENU = ["⚡️ Россети ЮГ", "⚡️ Россети Кубань", "📊 Выгрузить отчеты", "📞 Телефонный справочник", "📖 Руководство пользователя", "📚 Справка", "⬅️ Назад"]
ES_MENU = ["🔍 Поиск по ТП", "🔔 Отправить уведомление", "📚 Справка", "⬅️ Назад"]
REPORT_MENU_OPTS = ["📊 Уведомления о бездоговорных ВОЛС ЮГ", "📊 Уведомления о бездоговорных ВОЛС Кубань", "📋 Справочник контрагентов", "⬅️ Назад"]

# URL mappings
ES_URL = {
    **{name + suffix: url for name, (url, _) in {
        "Юго-Западные ЭС": (YUGO_ZAPAD_URL_UG, YUGO_ZAPAD_URL_RK),
        "Центральные ЭС": (CENTRAL_URL_UG, CENTRAL_URL_UG_SP),
        "Западные ЭС": (ZAPAD_URL_UG, ZAPAD_URL_UG_SP),
        "Восточные ЭС": (VOSTOCH_URL_UG, VOSTOCH_URL_UG_SP),
        "Южные ЭС": (YUZH_URL_UG, YUZH_URL_UG_SP),
        "Северо-Восточные ЭС": (SEVERO_VOSTOCH_URL_UG, SEVERO_VOSTOCH_URL_UG_SP),
        "Юго-Восточные ЭС": (YUGO_VOSTOCH_URL_UG, YUGO_VOSTOCH_URL_UG_SP),
        "Северные ЭС": (SEVER_URL_UG, SEVER_URL_UG_SP),
    }.items() for suffix in ("_UG", "_UG_SP") for url in [_[0] if suffix=="_UG" else _[1]] if url},
    **{name + suffix: url for name, (_, url) in {
        "Юго-Западные ЭС": (YUGO_ZAPAD_URL_UG, YUGO_ZAPAD_URL_RK),
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
    }.items() for suffix in ("_RK", "_RK_SP") }
}

# Load user config\@lru_cache(maxsize=1)
def load_users() -> Dict[str, Dict[str, Any]]:
    try:
        df = pd.read_csv(ZONES_CSV_URL, encoding="utf-8")
        return {str(r["Telegram ID"]): {"vis": r["Видимость"].lower(), "FIO": r["ФИО"], "res": r["РЭС"], "resp": r["Ответственный"]} for _, r in df.iterrows()}
    except Exception as e:
        logger.error("User load fail: %s", e)
        return {}

def can(user, req) -> bool:
    return user.get("vis") in ("all", req.lower())

# CSV fetch cache
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

# Handlers                  
async def start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    u = load_users().get(uid)
    if not u:
        return await update.message.reply_text("🚫 Нет доступа.") or ConversationHandler.END
    ctx.user_data.clear()
    ctx.user_data.update(state="MAIN")
    kb = ReplyKeyboardMarkup([[m] for m in MAIN_MENU], resize_keyboard=True)
    await update.message.reply_text(f"Здравствуйте, {u['FIO']}!", reply_markup=kb)
    return SEARCH_TP

async def handle_main(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    text = update.message.text
    uid = str(update.effective_user.id)
    u = load_users()[uid]
    if text == "⬅️ Назад": return await start(update, ctx)
    if text.startswith("⚡️ Россети"):
        es = text.replace("⚡️ ", "")
        is_ug = es.endswith("ЮГ")
        ctx.user_data.update(state="ES", es=es, ug=is_ug)
        kb = ReplyKeyboardMarkup([[m] for m in ES_MENU], resize_keyboard=True)
        return await update.message.reply_text(f"{es}", reply_markup=kb) or SELECT_TP
    if text == "📊 Выгрузить отчеты":
        ctx.user_data['state'] = 'REPORT'
        kb = ReplyKeyboardMarkup([[m] for m in REPORT_MENU_OPTS], resize_keyboard=True)
        await update.message.reply_text("Выберите отчёт:", reply_markup=kb)
        return REPORT_MENU
    return ConversationHandler.END

async def report_menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    opt = update.message.text
    if opt == "⬅️ Назад": return await start(update, ctx)
    if "ЮГ" in opt or "Кубань" in opt:
        fn = NOTIFY_LOG_FILE_UG if "ЮГ" in opt else NOTIFY_LOG_FILE_RK
        df = pd.read_csv(fn)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as w:
            df.to_excel(w, index=False)
        bio.seek(0)
        return await update.message.reply_document(bio, filename=f"{opt}.xlsx") or REPORT_MENU
    if "Справочник" in opt:
        return await update.message.reply_text("Скоро!", reply_markup=ReplyKeyboardMarkup([[m] for m in REPORT_MENU_OPTS], resize_keyboard=True)) or REPORT_MENU
    return REPORT_MENU

async def search_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    term = update.message.text
    es, is_ug = ctx.user_data['es'], ctx.user_data['ug']
    df = await fetch_csv(ES_URL.get(es + ("_UG" if is_ug else "_RK"), ""))
    if df.empty:
        return await update.message.reply_text("Ошибка загрузки.") or ConversationHandler.END
    exact = df[df["Наименование ТП"] == term]
    if not exact.empty:
        return await send_results(update, ctx, exact)
    opts = df["Наименование ТП"].drop_duplicates().tolist()
    # simple contains filter
    opts = [o for o in opts if term.lower() in o.lower()][:10]
    if not opts:
        return await update.message.reply_text(f"Не найдено {term}") or SEARCH_TP
    ctx.user_data['opts'] = opts
    kb = ReplyKeyboardMarkup([[o] for o in opts] + [["⬅️ Назад"]], resize_keyboard=True)
    await update.message.reply_text("Варианты:", reply_markup=kb)
    return SELECT_TP

async def select_tp(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    sel = update.message.text
    if sel == "⬅️ Назад": return await handle_main(update, ctx)
    es, is_ug = ctx.user_data['es'], ctx.user_data['ug']
    df = await fetch_csv(ES_URL.get(es + ("_UG" if is_ug else "_RK"), ""))
    res = df[df["Наименование ТП"] == sel]
    return await send_results(update, ctx, res)

async def send_results(update: Update, ctx: ContextTypes.DEFAULT_TYPE, df: pd.DataFrame) -> int:
    if df.empty:
        return await update.message.reply_text("Пусто.") or ConversationHandler.END
    for _, r in df.iterrows():
        await update.message.reply_text(f"ВЛ: {r['Наименование ВЛ']}\nОпоры: {r['Опоры']}\nКол-во: {r['Количество опор']}")
    return ConversationHandler.END

# Similar notify handlers...

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
            # Add notify states
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
