import logging
import io
import re
from functools import lru_cache
from typing import Any, Dict, List, Optional, Tuple

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
    # URLs for notification directory
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
    # Report files
    NOTIFY_LOG_FILE_UG,
    NOTIFY_LOG_FILE_RK,
)

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# FastAPI app placeholder; Telegram application initialized on startup
app = FastAPI()
application: Application

# Conversation states
(
    SEARCH_TP,
    SELECT_TP,
    NOTIFY_TP,
    NOTIFY_VL,
    NOTIFY_GEO,
    REPORT_MENU,
) = range(6)

# ES URL mappings
ES_URL_MAPPING: Dict[str, str] = {
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
ES_SP_URL_MAPPING: Dict[str, str] = {**ES_URL_MAPPING}
ES_SP_URL_MAPPING.update({
    k.replace("_UG", "_UG_SP").replace("_RK", "_RK_SP"): v for k, v in ES_URL_MAPPING.items()
})

# Main menu
MAIN_MENU = [
    {"text": "⚡️ Россети ЮГ", "vis": "all"},
    {"text": "⚡️ Россети Кубань", "vis": "all"},
    {"text": "📊 Выгрузить отчеты", "vis": "all"},
    {"text": "📞 Телефонный справочник", "vis": "all"},
    {"text": "📖 Руководство пользователя", "vis": "all"},
    {"text": "📚 Справка", "vis": "all"},
    {"text": "⬅️ Назад", "vis": "all"},
]
# ES submenu
ES_SUBMENU = [
    {"text": "🔍 Поиск по ТП", "vis": "all"},
    {"text": "🔔 Отправить уведомление", "vis": "all"},
    {"text": "📚 Справка", "vis": "all"},
    {"text": "⬅️ Назад", "vis": "all"},
]
# Report submenu
REPORT_SUBMENU = [
    {"text": "📊 Уведомления о бездоговорных ВОЛС ЮГ", "vis": "all"},
    {"text": "📊 Уведомления о бездоговорных ВОЛС Кубань", "vis": "all"},
    {"text": "📋 Справочник контрагентов", "vis": "all"},
    {"text": "⬅️ Назад", "vis": "all"},
]

# Utility: build keyboard based on visibility
def build_menu(
    buttons: List[Dict[str, str]], user_data: Dict[str, Any]
) -> ReplyKeyboardMarkup:
    keyboard = [[btn["text"]] for btn in buttons if has_access(user_data, btn["vis"])]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True) if keyboard else ReplyKeyboardRemove()

@lru_cache(maxsize=1)
def load_user_data() -> Dict[str, Dict[str, Any]]:
    users: Dict[str, Dict[str, Any]] = {}
    try:
        df = pd.read_csv(ZONES_CSV_URL, encoding="utf-8")
        for _, row in df.iterrows():
            uid = str(row["Telegram ID"])
            users[uid] = {
                "Visibility": row["Видимость"],
                "Branch": row["Филиал"],
                "RES": row["РЭС"],
                "FIO": row["ФИО"],
                "Responsible": row["Ответственный"],
            }
    except Exception as e:
        logger.error(f"Error loading user data: {e}")
    return users

def has_access(user_data: Dict[str, Any], required_vis: str) -> bool:
    uv = user_data.get("Visibility", "").lower()
    return uv == "all" or required_vis == "all" or uv == required_vis.lower()

# Async data loaders
def _get_url_key(es_name: str, is_ug: bool) -> str:
    suffix = "_UG" if is_ug else "_RK"
    return es_name if not es_name.startswith("Юго-Западные ЭС") else f"Юго-Западные ЭС{suffix}"

async def load_tp_data(es_name: str, is_ug: bool) -> pd.DataFrame:
    url = ES_URL_MAPPING.get(_get_url_key(es_name, is_ug), "")
    if not url:
        return pd.DataFrame()
    resp = await app.state.http.get(url)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text), encoding="utf-8")

async def load_tp_directory_data(es_name: str, is_ug: bool) -> pd.DataFrame:
    url = ES_SP_URL_MAPPING.get(_get_url_key(es_name, is_ug), "")
    if not url:
        return pd.DataFrame()
    resp = await app.state.http.get(url)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text), encoding="utf-8")

# Search and notify logic
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    ud = load_user_data().get(uid)
    if not ud:
        await update.message.reply_text("🚫 У вас нет доступа.", reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data.update(state="MAIN_MENU")
    await update.message.reply_text(
        f"Здравствуйте, {ud['FIO']}! Выберите действие:",
        reply_markup=build_menu(MAIN_MENU, ud),
    )
    return ConversationHandler.END

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    ud = load_user_data().get(uid)
    text = update.message.text
    state = context.user_data.get("state", "MAIN_MENU")

    if state == "MAIN_MENU":
        if text == "⬅️ Назад":
            return await start(update, context)
        if text in [b["text"] for b in MAIN_MENU]:
            if text.startswith("⚡️ Россети"):
                context.user_data.update(
                    state="ES_SUBMENU",
                    selected_es=text.replace("⚡️ ", ""),
                    is_ug=text.endswith("ЮГ"),
                )
                await update.message.reply_text(
                    f"Вы выбрали {context.user_data['selected_es']}.",
                    reply_markup=build_menu(ES_SUBMENU, ud),
                )
                return ConversationHandler.END
            if text == "📊 Выгрузить отчеты":
                context.user_data["state"] = "REPORT_MENU"
                await update.message.reply_text(
                    "📝 Выберите тип отчёта:",
                    reply_markup=build_menu(REPORT_SUBMENU, ud),
                )
                return REPORT_MENU
    elif state == "REPORT_MENU":
        if text == "⬅️ Назад":
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_menu(MAIN_MENU, ud)
            )
            return ConversationHandler.END
        if text == "📊 Уведомления о бездоговорных ВОЛС ЮГ":
            df = pd.read_csv(NOTIFY_LOG_FILE_UG)
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="UG")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_ug.xlsx")
        elif text == "📊 Уведомления о бездоговорных ВОЛС Кубань":
            df = pd.read_csv(NOTIFY_LOG_FILE_RK)
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as w:
                df.to_excel(w, index=False, sheet_name="RK")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_rk.xlsx")
        elif text == "📋 Справочник контрагентов":
            await update.message.reply_text(
                "📋 Справочник контрагентов — скоро будет!",
                reply_markup=build_menu(REPORT_SUBMENU, ud),
            )
        return REPORT_MENU

    # ES_SUBMENU
    if context.user_data.get("state") == "ES_SUBMENU":
        es = context.user_data["selected_es"]
        if text == "🔍 Поиск по ТП":
            await update.message.reply_text(
                f"Введите наименование ТП для поиска в {es}:",
                reply_markup=ReplyKeyboardRemove(),
            )
            return SEARCH_TP
        if text == "🔔 Отправить уведомление":
            await update.message.reply_text(
                f"Введите наименование ТП для уведомления в {es}:",
                reply_markup=ReplyKeyboardRemove(),
            )
            return NOTIFY_TP
        if text == "⬅️ Назад":
            return await start(update, context)

    return ConversationHandler.END

async def search_tp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    ud = load_user_data().get(uid)
    term = update.message.text
    es = context.user_data.get("selected_es")
    df = await load_tp_data(es, context.user_data.get("is_ug", False))
    if df.empty:
        await update.message.reply_text("Ошибка загрузки данных.", reply_markup=build_menu(ES_SUBMENU, ud))
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END
    exact = df[df["Наименование ТП"] == term]
    if not exact.empty:
        await send_tp_results(update, context, exact)
        context.user_data["state"] = "ES_SUBMENU"
        await update.message.reply_text("Готово.", reply_markup=build_menu(ES_SUBMENU, ud))
        return ConversationHandler.END
    options = fuzzy_search_tp(term, df)
    if not options:
        await update.message.reply_text(
            f"ТП '{term}' не найдено.", reply_markup=ReplyKeyboardRemove()
        )
        return SEARCH_TP
    context.user_data["tp_options"] = options
    kb = ReplyKeyboardMarkup([[o] for o in options] + [["⬅️ Назад"]], resize_keyboard=True)
    await update.message.reply_text("Похожие варианты:", reply_markup=kb)
    return SELECT_TP

async def select_tp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    ud = load_user_data().get(uid)
    choice = update.message.text
    if choice == "⬅️ Назад":
        context.user_data["state"] = "ES_SUBMENU"
        return ConversationHandler.END
    es = context.user_data.get("selected_es")
    df = await load_tp_data(es, context.user_data.get("is_ug", False))
    sel = df[df["Наименование ТП"] == choice]
    await send_tp_results(update, context, sel)
    context.user_data["state"] = "ES_SUBMENU"
    await update.message.reply_text("Готово.", reply_markup=build_menu(ES_SUBMENU, ud))
    return ConversationHandler.END

async def notify_tp(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    uid = str(update.effective_user.id)
    ud = load_user_data().get(uid)
    term = update.message.text
    es = context.user_data.get("selected_es")
    df = await load_tp_directory_data(es, context.user_data.get("is_ug", False))
    if df.empty:
        await update.message.reply_text("Ошибка справочника.", reply_markup=build_menu(ES_SUBMENU, ud))
        return ConversationHandler.END
    exact = df[df["Наименование ТП"] == term]
    if not exact.empty:
        context.user_data["tp_options"] = [term]
        context.user_data["vl_options"] = exact["Наименование ВЛ"].dropna().unique().tolist()
        kb = ReplyKeyboardMarkup([[v] for v in context.user_data["vl_options"]] + [["⬅️ Назад"]], resize_keyboard=True)
        await update.message.reply_text(f"Выберите ВЛ для {term}:", reply_markup=kb)
        return NOTIFY_VL
    opts = fuzzy_search_tp(term, df)
    if not opts:
        await update.message.reply_text(f"ТП '{term}' не найдено.", reply_markup=ReplyKeyboardRemove())
        return NOTIFY_TP
    context.user_data["tp_options"] = opts
    kb = ReplyKeyboardMarkup([[o] for o in opts] + [["⬅️ Назад"]], resize_keyboard=True)
    await update.message.reply_text("Похожие варианты:", reply_markup=kb)
    return NOTIFY_TP

async def notify_vl(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    choice = update.message.text
    if choice == "⬅️ Назад":
        return await handle_message(update, context)
    context.user_data["selected_vl"] = choice
    kb = ReplyKeyboardMarkup([[KeyboardButton("📍 Отправить местоположение", request_location=True)]], resize_keyboard=True)
    await update.message.reply_text("Отправьте местоположение:", reply_markup=kb)
    return NOTIFY_GEO

async def notify_geo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    loc = update.message.location
    es = context.user_data.get("selected_es")
    tp = context.user_data.get("tp_options", [None])[0]
    vl = context.user_data.get("selected_vl")
    df = await load_tp_directory_data(es, context.user_data.get("is_ug", False))
    row = df[(df["Наименование ТП"] == tp) & (df["Наименование ВЛ"] == vl)]
    res = row["РЭС"].iloc[0] if not row.empty else None
    users = load_user_data()
    uid_resp, fio_resp = find_responsible(res, users)
    if not uid_resp:
        await update.message.reply_text("Ответственный не найден.", reply_markup=build_menu(ES_SUBMENU, users[str(update.effective_user.id)]))
        return ConversationHandler.END
    msg = f"⚠️ Уведомление: бездоговорная ВОЛС {tp}, {vl}, РЭС {res}"
    await application.bot.send_message(uid_resp, msg)
    await application.bot.send_location(uid_resp, loc.latitude, loc.longitude)
    # Log
    with open(NOTIFY_LOG_FILE_UG if context.user_data.get("is_ug") else NOTIFY_LOG_FILE_RK, "a", encoding="utf-8") as f:
        f.write(f"{es},{tp},{vl},{loc.latitude},{loc.longitude}\n")
    await update.message.reply_text("Уведомление отправлено.", reply_markup=build_menu(ES_SUBMENU, users[str(update.effective_user.id)]))
    return ConversationHandler.END

async def send_tp_results(update: Update, context: ContextTypes.DEFAULT_TYPE, df: pd.DataFrame) -> None:
    res = df["РЭС"].iloc[0] if not df.empty else ""
    tp = context.user_data.get("tp_options", [None])[0]
    await update.message.reply_text(f"В {res} на ТП {tp} найдено {len(df)} ВОЛС:")
    for _, r in df.iterrows():
        await update.message.reply_text(
            f"📍 ВЛ: {r['Наименование ВЛ']}\n"
            f"Опоры: {r['Опоры']}\n"
            f"Кол-во: {r['Количество опор']}"
        )

async def error_handler(update: Any, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.error(f"Update {update} caused error {context.error}")

@app.post("/webhook")
async def webhook(request: Request) -> Dict[str, str]:
    upd = Update.de_json(await request.json(), application.bot)
    if upd:
        await application.process_update(upd)
    return {"status": "ok"}

@app.on_event("startup")
async def on_startup() -> None:
    app.state.http = httpx.AsyncClient()
    global application
    application = Application.builder().token(TOKEN).build()
    application.add_handler(CommandHandler("start", start))
    conv = ConversationHandler(
        entry_points=[MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)],
        states={
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_tp)],
            SELECT_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, select_tp)],
            NOTIFY_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_tp)],
            NOTIFY_VL: [MessageHandler(filters.TEXT & ~filters.COMMAND, notify_vl)],
            NOTIFY_GEO: [MessageHandler(filters.LOCATION, notify_geo)],
            REPORT_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)],
        },
        fallbacks=[CommandHandler("cancel", start)],
    )
    application.add_handler(conv)
    application.add_error_handler(error_handler)
    await application.initialize()
    await application.bot.set_webhook(f"{SELF_URL}/webhook")

@app.on_event("shutdown")
async def on_shutdown() -> None:
    await application.stop()
    await app.state.http.aclose()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
