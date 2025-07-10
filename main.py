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
    keyboard = []
    for btn in buttons:
        if has_access(user_data, btn["vis"]):
            keyboard.append([btn["text"]])
    return (
        ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        if keyboard
        else ReplyKeyboardRemove()
    )

# Load and cache user data
@lru_cache(maxsize=1)
def load_user_data() -> Dict[str, Dict[str, Any]]:
    users = {}
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

# Access check

def has_access(user_data: Dict[str, Any], required_vis: str) -> bool:
    if not user_data:
        return False
    uv = user_data.get("Visibility", "").lower()
    return uv == "all" or required_vis == "all" or uv == required_vis.lower()

# ... (other utility functions: load_tp_data, load_tp_directory_data, fuzzy_search_tp, find_responsible) ...

# Handlers
async def start(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    ud = load_user_data().get(uid)
    if not ud:
        await update.message.reply_text(
            "🚫 У вас нет доступа.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    context.user_data.clear()
    context.user_data.update({"state": "MAIN_MENU"})
    await update.message.reply_text(
        f"Здравствуйте, {ud['FIO']}! Выберите действие:",
        reply_markup=build_menu(MAIN_MENU, ud),
    )
    return ConversationHandler.END

async def handle_message(
    update: Update, context: ContextTypes.DEFAULT_TYPE
) -> int:
    uid = str(update.effective_user.id)
    ud = load_user_data().get(uid)
    if not ud:
        await update.message.reply_text(
            "🚫 У вас нет доступа.", reply_markup=ReplyKeyboardRemove()
        )
        return ConversationHandler.END
    text = update.message.text
    state = context.user_data.get("state", "MAIN_MENU")

    # MAIN_MENU
    if state == "MAIN_MENU":
        if text == "⚡️ Россети ЮГ":
            context.user_data.update(state="ROSSETI_YUG", is_rosseti_yug=True)
            # show submenu...
        elif text == "⚡️ Россети Кубань":
            context.user_data.update(state="ROSSETI_KUBAN", is_rosseti_yug=False)
        elif text == "📊 Выгрузить отчеты":
            context.user_data["state"] = "REPORT_MENU"
            await update.message.reply_text(
                "📝 Выберите тип отчёта:",
                reply_markup=build_menu(REPORT_SUBMENU, ud),
            )
            return REPORT_MENU
        elif text == "⬅️ Назад":
            return await start(update, context)
        # ... other MAIN_MENU items
        return ConversationHandler.END

    # REPORT_MENU
    if state == "REPORT_MENU":
        if text == "📊 Уведомления о бездоговорных ВОЛС ЮГ":
            df = pd.read_csv(NOTIFY_LOG_FILE_UG)
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="UG")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_ug.xlsx")
        elif text == "📊 Уведомления о бездоговорных ВОЛС Кубань":
            df = pd.read_csv(NOTIFY_LOG_FILE_RK)
            bio = io.BytesIO()
            with pd.ExcelWriter(bio, engine="openpyxl") as writer:
                df.to_excel(writer, index=False, sheet_name="RK")
            bio.seek(0)
            await update.message.reply_document(bio, filename="log_rk.xlsx")
        elif text == "📋 Справочник контрагентов":
            await update.message.reply_text(
                "📋 Справочник контрагентов — скоро будет!",
                reply_markup=build_menu(REPORT_SUBMENU, ud),
            )
        elif text == "⬅️ Назад":
            context.user_data["state"] = "MAIN_MENU"
            await update.message.reply_text(
                "Выберите действие:", reply_markup=build_menu(MAIN_MENU, ud)
            )
            return ConversationHandler.END
        # stay in REPORT_MENU for further selections
        return REPORT_MENU

    # ... other states handlers (TP search, notify, etc.)
    return ConversationHandler.END

async def error_handler(
    update: Any, context: ContextTypes.DEFAULT_TYPE
) -> None:
    logger.error(f"Update {update} caused error {context.error}")

# FastAPI endpoints and startup/shutdown
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
            SEARCH_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, /*...*/)],
            SELECT_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, /*...*/)],
            NOTIFY_TP: [MessageHandler(filters.TEXT & ~filters.COMMAND, /*...*/)],
            NOTIFY_VL: [MessageHandler(filters.TEXT & ~filters.COMMAND, /*...*/)],
            NOTIFY_GEO: [MessageHandler(filters.LOCATION, /*...*/)],
            REPORT_MENU: [MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)],
        },
        fallbacks=[CommandHandler("cancel", start)],
    )
    application.add_handler(conv)
    application.add_error_handler(error_handler)
    await application.initialize()
    webhook_url = f"{SELF_URL}/webhook"
    await application.bot.set_webhook(webhook_url)

@app.on_event("shutdown")
async def on_shutdown() -> None:
    await application.stop()
    await app.state.http.aclose()

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=PORT)
