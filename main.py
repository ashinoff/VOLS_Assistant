import logging
import io
from functools import lru_cache
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

logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
application: Application

(CH_NET, CH_BR, CH_ACT, IN_TP, VAR_SEL) = range(5)

MAIN_MENU = [[KeyboardButton("⚡️ Россети ЮГ")], [KeyboardButton("⚡️ Россети Кубань")]]
UG_MENU = [[KeyboardButton(v)] for v in [
    "Юго-Западные ЭС","Центральные ЭС","Западные ЭС","Восточные ЭС",
    "Южные ЭС","Северо-Восточные ЭС","Юго-Восточные ЭС","Северные ЭС"]] + [[KeyboardButton("⬅️ Назад")]]
RK_MENU = [[KeyboardButton(v)] for v in [
    "Юго-Западные ЭС","Усть-Лабинские ЭС","Тимашевские ЭС","Тихорецкие ЭС",
    "Сочинские ЭС","Славянские ЭС","Ленинградские ЭС","Лабинские ЭС",
    "Краснодарские ЭС","Армавирские ЭС","Адыгейские ЭС"]] + [[KeyboardButton("⬅️ Назад")]]
ACTION_MENU = [[KeyboardButton("🔍 Поиск ТП")], [KeyboardButton("📨 Отправить Уведомление")],
               [KeyboardButton("⬅️ Назад")], [KeyboardButton("ℹ️ Справка")]]
SPARK_MENU = [
    [KeyboardButton("📊 Уведомления о бездоговорных ВОЛС ЮГ")],
    [KeyboardButton("📊 Уведомления о бездоговорных ВОЛС Кубань")],
    [KeyboardButton("⬅️ Назад")]
]

URL_MAP = {
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

_csv_cache = {}
async def fetch_csv(url):
    if not url: return pd.DataFrame()
    if url in _csv_cache: return _csv_cache[url]
    resp = await app.state.http.get(url, follow_redirects=True)
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.text))
    _csv_cache[url] = df
    return df

@lru_cache(maxsize=1)
def load_users():
    return {str(r['Telegram ID']): r for _,r in pd.read_csv(ZONES_CSV_URL).iterrows()}

async def show_results(update, ctx, tp, df):
    filt = df[df['Наименование ТП']==tp]
    await update.message.reply_text(f"На {tp} найдено {len(filt)} договоров ВОЛС.")
    for _,r in filt.iterrows():
        await update.message.reply_text(
            f"📡 {r['Наименование ВЛ']}\nОпоры: {r['Опоры']} ({r['Количество опор']})\nПровайдер: {r['Наименование Провайдера']}"
        )
    await update.message.reply_text('Действия:', reply_markup=ReplyKeyboardMarkup(ACTION_MENU, resize_keyboard=True))
    return CH_ACT

async def variant_selection(update, ctx):
    txt = update.message.text
    if txt=='⬅️ Назад': return await select_branch(update, ctx)
    opts = ctx.user_data.get('variants', [])
    if txt not in opts:
        await update.message.reply_text('Неизвестный вариант.')
        return VAR_SEL
    df = await fetch_csv(ctx.user_data['urls'][0])
    return await show_results(update, ctx, txt, df)

async def search_tp(update, ctx):
    term = update.message.text or ''
    norm = term.lower().replace(' ','').replace('-','')
    full,sp = ctx.user_data['urls']
    df = await fetch_csv(full)
    df['__n']=df['Наименование ТП'].str.lower().str.replace(' ','').str.replace('-','')
    m=df[df['__n'].str.contains(norm,na=False)]
    if m.empty:
        await update.message.reply_text(f"ТП '{term}' не найдено.")
        return CH_ACT
    tps = m['Наименование ТП'].drop_duplicates().tolist()
    if len(tps)==1:
        return await show_results(update, ctx, tps[0], m)
    opts=tps[:10]
    ctx.user_data['variants']=opts
    kb=[[KeyboardButton(o)] for o in opts]+[[KeyboardButton('⬅️ Назад')]]
    await update.message.reply_text('Варианты:',reply_markup=ReplyKeyboardMarkup(kb,resize_keyboard=True))
    return VAR_SEL

async def start(update, ctx):
    uid=str(update.effective_user.id)
    if uid not in load_users():
        await update.message.reply_text('Нет доступа.')
        return ConversationHandler.END
    await update.message.reply_text('Выберите сеть:',reply_markup=ReplyKeyboardMarkup(MAIN_MENU,resize_keyboard=True))
    return CH_NET

async def select_network(update, ctx):
    net=update.message.text.replace('⚡️ ','')
    if net not in URL_MAP: return await start(update,ctx)
    ctx.user_data['network']=net
    menu=UG_MENU if net=='ЮГ' else RK_MENU
    await update.message.reply_text(f'{net}: выберите филиал:',reply_markup=ReplyKeyboardMarkup(menu,resize_keyboard=True))
    return CH_BR

async def select_branch(update, ctx):
    b=update.message.text
    if b=='⬅️ Назад': return await start(update,ctx)
    net=ctx.user_data['network']
    if b not in URL_MAP[net]: return await select_network(update,ctx)
    ctx.user_data['urls']=URL_MAP[net][b]
    await update.message.reply_text(f'Филиал {b}:', reply_markup=ReplyKeyboardMarkup(ACTION_MENU,resize_keyboard=True))
    return CH_ACT

async def branch_action(update, ctx):
    act=update.message.text
    # Справка
    if act=='ℹ️ Справка':
        await update.message.reply_text('Справка и выгрузки:', reply_markup=ReplyKeyboardMarkup(SPARK_MENU, resize_keyboard=True))
        return CH_ACT
    # Кнопки выгрузки логов
    if act == "📊 Уведомления о бездоговорных ВОЛС ЮГ":
        df = pd.read_csv(NOTIFY_LOG_FILE_UG)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="UG")
        bio.seek(0)
        await update.message.reply_document(bio, filename="log_ug.xlsx")
        return CH_ACT
    if act == "📊 Уведомления о бездоговорных ВОЛС Кубань":
        df = pd.read_csv(NOTIFY_LOG_FILE_RK)
        bio = io.BytesIO()
        with pd.ExcelWriter(bio, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="RK")
        bio.seek(0)
        await update.message.reply_document(bio, filename="log_rk.xlsx")
        return CH_ACT
    if act=='⬅️ Назад':
        return await select_network(update,ctx)
    if act=='🔍 Поиск ТП':
        await update.message.reply_text('Введите ТП:')
        return IN_TP
    if act=='📨 Отправить Уведомление':
        await update.message.reply_text('Отправка...')
        return CH_ACT
    await update.message.reply_text('Не понял')
    return CH_ACT

async def error_handler(update,ctx):
    logger.error(ctx.error)

@app.on_event('startup')
async def on_startup():
    app.state.http=httpx.AsyncClient()
    global application
    application=Application.builder().token(TOKEN).build()
    conv=ConversationHandler(
        entry_points=[CommandHandler('start',start)],
        states={
            CH_NET:[MessageHandler(filters.TEXT&~filters.COMMAND,select_network)],
            CH_BR:[MessageHandler(filters.TEXT&~filters.COMMAND,select_branch)],
            CH_ACT:[MessageHandler(filters.TEXT&~filters.COMMAND,branch_action)],
            IN_TP:[MessageHandler(filters.TEXT&~filters.COMMAND,search_tp)],
            VAR_SEL:[MessageHandler(filters.TEXT&~filters.COMMAND,variant_selection)],
        },
        fallbacks=[CommandHandler('cancel',start)],
    )
    application.add_handler(conv)
    application.add_error_handler(error_handler)
    await application.initialize()
    await application.bot.set_webhook(f"{SELF_URL}/webhook")

@app.post('/webhook')
async def webhook(request:Request):
    upd=Update.de_json(await request.json(),application.bot)
    await application.process_update(upd)
    return {'status':'ok'}

@app.on_event('shutdown')
async def on_shutdown():
    await application.stop()
    await app.state.http.aclose()

if __name__=='__main__':
    uvicorn.run(app,host='0.0.0.0',port=PORT)
