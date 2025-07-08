import os
import asyncio
import logging
from enum import Enum
from telegram import Update, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    filters,
    ContextTypes,
)
import aiohttp
import aiofiles
import pandas as pd
import csv
import re
from datetime import datetime, timezone
from io import BytesIO

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from config import TOKEN, SELF_URL, PORT, BRANCH_URLS, NOTIFY_URLS, ZONES_CSV_URL, NOTIFY_LOG_FILE_UG, NOTIFY_LOG_FILE_RK
from zones import normalize_sheet_url, load_zones_cached

# Перечисление для шагов
class BotStep(Enum):
    INIT = "INIT"
    NET = "NET"
    BRANCH = "BRANCH"
    AWAIT_TP_INPUT = "AWAIT_TP_INPUT"
    NOTIFY_AWAIT_TP = "NOTIFY_AWAIT_TP"
    NOTIFY_VL = "NOTIFY_VL"
    NOTIFY_GEO = "NOTIFY_GEO"

# Клавиатуры
kb_back = ReplyKeyboardMarkup([["🔙 Назад"]], resize_keyboard=True)
kb_actions = ReplyKeyboardMarkup([["🔍 Поиск по ТП"], ["🔔 Отправить уведомление"], ["🔙 Назад"]], resize_keyboard=True)
kb_request_location = ReplyKeyboardMarkup([[KeyboardButton("📍 Отправить геолокацию", request_location=True)], ["🔙 Назад"]], resize_keyboard=True)

def build_initial_kb(vis_flag: str, res_flag: str) -> ReplyKeyboardMarkup:
    f = vis_flag.strip().upper()
    nets = ["⚡ Россети ЮГ", "⚡ Россети Кубань"] if f == "ALL" else ["⚡ Россети ЮГ"] if f == "UG" else ["⚡ Россети Кубань"]
    buttons = [[n] for n in nets]
    buttons.append(["📖 Помощь"])
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

async def get_cached_csv(context, url, cache_key, ttl=3600):
    if cache_key not in context.bot_data or context.bot_data[cache_key]["expires"] < time.time():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(await normalize_sheet_url(url), timeout=10) as response:
                    response.raise_for_status()
                    df = pd.read_csv(BytesIO(await response.read()))
            context.bot_data[cache_key] = {"data": df, "expires": time.time() + ttl}
        except aiohttp.ClientError as e:
            logger.error(f"Ошибка загрузки CSV: {e}")
            raise
        except pd.errors.EmptyDataError:
            logger.error(f"CSV-файл пуст: {url}")
            raise
    return context.bot_data[cache_key]["data"]

async def log_notification(log_file, data):
    try:
        async with aiofiles.open(log_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
            await f.write(writer.writerow(data))
    except Exception as e:
        logger.error(f"Ошибка записи в лог {log_file}: {e}")

# === /start ===
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    try:
        vis_map, raw_branch_map, res_map, names, resp_map = await load_zones_cached(context)
    except Exception as e:
        logger.error(f"Ошибка загрузки зон для {uid}: {e}")
        await update.message.reply_text(f"⚠️ Ошибка: {e}", reply_markup=kb_back)
        return
    if uid not in raw_branch_map:
        await update.message.reply_text("🚫 Нет доступа.", reply_markup=kb_back)
        return

    raw = raw_branch_map[uid]
    branch_key = "All" if raw == "All" else raw
    context.user_data.clear()
    context.user_data.update({
        "step": BotStep.BRANCH.value if branch_key != "All" else BotStep.INIT.value,
        "vis_flag": vis_map[uid],
        "branch_user": branch_key,
        "res_user": res_map[uid],
        "name": names[uid],
        "resp_map": resp_map
    })

    if branch_key != "All":
        await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)
    else:
        await update.message.reply_text(f"👋 {names[uid]}, выберите сеть:", reply_markup=build_initial_kb(vis_map[uid], res_map[uid]))

# === /help ===
async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📖 Инструкции:\n1. /start — начать.\n2. Выберите сеть и действие.\n3. Поиск: введите ТП (ТП-123).\n4. Уведомление: выберите ТП, ВЛ, отправьте геолокацию.",
        reply_markup=kb_back
    )

# === Обработчики шагов ===
async def handle_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text.strip()
    if "step" not in context.user_data:
        await start_cmd(update, context)
        return
    try:
        vis_map, raw_branch_map, res_map, names, resp_map = await load_zones_cached(context)
    except Exception as e:
        logger.error(f"Ошибка загрузки зон: {e}")
        await update.message.reply_text(f"⚠️ Ошибка: {e}", reply_markup=kb_back)
        return
    step = context.user_data["step"]
    vis_flag = context.user_data["vis_flag"]
    name = context.user_data["name"]

    if step == BotStep.INIT.value:
        if text == "🔙 Назад":
            await start_cmd(update, context)
            return
        if text in ["⚡ Россети ЮГ", "⚡ Россети Кубань"]:
            context.user_data["step"] = BotStep.NET.value
            context.user_data["net"] = text.replace("⚡ ", "")
            branches = list(BRANCH_URLS[context.user_data["net"]].keys())
            kb = ReplyKeyboardMarkup([[b] for b in branches] + [["🔙 Назад"]], resize_keyboard=True)
            await update.message.reply_text("Выберите филиал:", reply_markup=kb)
            return
        await update.message.reply_text(f"{name}, доступны: {vis_flag}", reply_markup=build_initial_kb(vis_flag, ""))

    elif step == BotStep.NET.value:
        if text == "🔙 Назад":
            await start_cmd(update, context)
            return
        if text in BRANCH_URLS[context.user_data["net"]]:
            context.user_data["step"] = BotStep.BRANCH.value
            context.user_data["branch"] = text
            await update.message.reply_text("Действие:", reply_markup=kb_actions)
            return
        await update.message.reply_text("⚠ Филиал не найден.", reply_markup=kb_back)

    elif step == BotStep.BRANCH.value:
        if text == "🔍 Поиск по ТП":
            context.user_data["step"] = BotStep.AWAIT_TP_INPUT.value
            await update.message.reply_text("Введите ТП (например, ТП-123):", reply_markup=kb_back)
        elif text == "🔔 Отправить уведомление":
            context.user_data["step"] = BotStep.NOTIFY_AWAIT_TP.value
            await update.message.reply_text("Введите ТП для уведомления:", reply_markup=kb_back)
        else:
            await update.message.reply_text("Выберите действие:", reply_markup=kb_actions)

    elif step == BotStep.AWAIT_TP_INPUT.value:
        if text == "🔙 Назад":
            context.user_data["step"] = BotStep.BRANCH.value
            await update.message.reply_text("Действие:", reply_markup=kb_actions)
            return
        net = context.user_data["net"]
        branch = context.user_data["branch"]
        url = BRANCH_URLS[net][branch]
        if not url:
            await update.message.reply_text(f"⚠ URL для {branch} не настроен.", reply_markup=kb_back)
            return
        try:
            df = await get_cached_csv(context, url, f"{net}_{branch}")
            if not all(col in df.columns for col in ["Наименование ТП", "РЭС", "Наименование ВЛ"]):
                await update.message.reply_text("⚠ CSV без нужных столбцов.", reply_markup=kb_back)
                return
        except Exception as e:
            logger.error(f"Ошибка данных для {net}/{branch}: {e}")
            await update.message.reply_text(f"⚠ Ошибка: {e}", reply_markup=kb_back)
            return
        q = re.sub(r"\W", "", text.upper())
        found = df[df["Наименование ТП"].str.upper().str.replace(r"\W", "", regex=True).str.contains(q)]
        if found.empty:
            await update.message.reply_text("ТП не найдено.", reply_markup=kb_back)
            return
        await update.message.reply_text(f"Найдено: {found['Наименование ТП'].iloc[0]}", reply_markup=kb_actions)
        context.user_data["step"] = BotStep.BRANCH.value

    elif step == BotStep.NOTIFY_AWAIT_TP.value:
        if text == "🔙 Назад":
            context.user_data["step"] = BotStep.BRANCH.value
            await update.message.reply_text("Действие:", reply_markup=kb_actions)
            return
        net = context.user_data["net"]
        branch = context.user_data["branch"]
        url = NOTIFY_URLS[net][branch]
        if not url:
            await update.message.reply_text(f"⚠ URL уведомлений для {branch} не настроен.", reply_markup=kb_back)
            return
        try:
            df = await get_cached_csv(context, url, f"notify_{net}_{branch}")
            if not all(col in df.columns for col in ["Наименование ТП", "РЭС", "Наименование ВЛ"]):
                await update.message.reply_text("⚠ CSV уведомлений без столбцов.", reply_markup=kb_back)
                return
        except Exception as e:
            logger.error(f"Ошибка уведомлений для {net}/{branch}: {e}")
            await update.message.reply_text(f"⚠ Ошибка: {e}", reply_markup=kb_back)
            return
        q = re.sub(r"\W", "", text.upper())
        found = df[df["Наименование ТП"].str.upper().str.replace(r"\W", "", regex=True).str.contains(q)]
        if found.empty:
            await update.message.reply_text("ТП не найдено.", reply_markup=kb_back)
            return
        context.user_data["tp"] = found["Наименование ТП"].iloc[0]
        context.user_data["step"] = BotStep.NOTIFY_VL.value
        await update.message.reply_text("Выберите ВЛ:", reply_markup=ReplyKeyboardMarkup([[vl] for vl in found["Наименование ВЛ"].unique()] + [["🔙 Назад"]], resize_keyboard=True))

    elif step == BotStep.NOTIFY_VL.value:
        if text == "🔙 Назад":
            context.user_data["step"] = BotStep.NOTIFY_AWAIT_TP.value
            await update.message.reply_text("Введите ТП для уведомления:", reply_markup=kb_back)
            return
        context.user_data["vl"] = text
        context.user_data["step"] = BotStep.NOTIFY_GEO.value
        await update.message.reply_text("Отправьте геолокацию:", reply_markup=kb_request_location)

# === Обработчик геолокации ===
async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if context.user_data.get("step") != BotStep.NOTIFY_GEO.value:
        return
    loc = update.message.location
    tp = context.user_data["tp"]
    vl = context.user_data["vl"]
    net = context.user_data["net"]
    branch = context.user_data["branch"]
    try:
        vis_map, raw_branch_map, res_map, names, resp_map = await load_zones_cached(context)
        res_tp = next((r for r in res_map.values() if r), "")
        sender = context.user_data["name"]
        recipients = [uid for uid, r in resp_map.items() if r and r.strip().lower() == res_tp.strip().lower()]
        msg = f"🔔 Уведомление от {sender}, {res_tp} РЭС, {tp}, {vl} – Бездоговорной ВОЛС"
        log_f = NOTIFY_LOG_FILE_UG if net == "Россети ЮГ" else NOTIFY_LOG_FILE_RK
        for cid in recipients:
            await context.bot.send_message(cid, msg)
            await context.bot.send_location(cid, loc.latitude, loc.longitude)
            await context.bot.send_message(cid, f"📍 {loc.latitude:.6f}, {loc.longitude:.6f}")
            await log_notification(log_f, [branch, res_tp, update.effective_user.id, sender, cid, resp_map.get(cid, ""), datetime.now(timezone.utc).isoformat(), f"{loc.latitude:.6f},{loc.longitude:.6f}"])
        await update.message.reply_text(f"✅ Уведомление отправлено: {', '.join(recipients)}", reply_markup=kb_actions)
    except Exception as e:
        logger.error(f"Ошибка геолокации: {e}")
        await update.message.reply_text(f"⚠️ Ошибка: {e}", reply_markup=kb_actions)
    context.user_data["step"] = BotStep.BRANCH.value

# Инициализация приложения
application = ApplicationBuilder().token(TOKEN).build()
application.add_handler(CommandHandler("start", start_cmd))
application.add_handler(CommandHandler("help", help_cmd))
application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text))
application.add_handler(MessageHandler(filters.LOCATION, location_handler))

if __name__ == "__main__":
    try:
        if SELF_URL:
            application.run_webhook(listen="0.0.0.0", port=PORT, url_path="webhook", webhook_url=f"{SELF_URL}/webhook")
        else:
            logger.warning("SELF_URL пуст, использую polling")
            application.run_polling()
    except Exception as e:
        logger.error(f"Ошибка запуска: {e}")
