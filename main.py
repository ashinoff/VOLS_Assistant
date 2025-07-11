import os
import re
import logging
import sys
import pandas as pd
from io import BytesIO
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters
)

# Configure logging
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
WEBHOOK_URL = os.getenv('WEBHOOK_URL')  # e.g. https://your-app.onrender.com
ZONES_CSV_URL = os.getenv('ZONES_CSV_URL')  # zones and rights CSV URL

# Validate critical ENV vars
if not BOT_TOKEN:
    logging.error('Environment variable BOT_TOKEN is not set. Exiting.')
    sys.exit(1)
if not WEBHOOK_URL:
    logging.error('Environment variable WEBHOOK_URL is not set. Exiting.')
    sys.exit(1)
if not ZONES_CSV_URL:
    logging.error('Environment variable ZONES_CSV_URL is not set. Exiting.')
    sys.exit(1)

# Load zones/rights
try:
    zones_df = pd.read_csv(ZONES_CSV_URL)
except Exception as e:
    logging.error(f"Failed to load zones CSV from {ZONES_CSV_URL}: {e}")
    sys.exit(1)

# Load contract and reference CSVs dynamically
contracts = {}
references = {}
for key, val in os.environ.items():
    if key.endswith('_URL_RK') or key.endswith('_URL_UG'):
        try:
            contracts[key] = pd.read_csv(val)
        except Exception as e:
            logging.error(f"Failed to load contract CSV {key}: {e}")
    if key.endswith('_URL_RK_SP') or key.endswith('_URL_UG_SP'):
        try:
            references[key] = pd.read_csv(val)
        except Exception as e:
            logging.error(f"Failed to load reference CSV {key}: {e}")

# In-memory storage for notifications
notifications = {'RK': [], 'UG': []}

# Normalize TP strings
def normalize_tp(s: str) -> str:
    return re.sub(r'[^0-9A-Za-z]', '', s).upper()

# Access rights lookup
def get_user_rights(user_id: int) -> dict:
    row = zones_df[zones_df['Telegram ID'] == user_id]
    if row.empty:
        return {'zone': None, 'filial': None, 'res': None}
    r = row.iloc[0]
    return {'zone': r['Видимость'], 'filial': r['Филиал'], 'res': r['РЭС']}

# Build main menu based on rights
def build_main_menu(rights: dict) -> InlineKeyboardMarkup:
    buttons = []
    zone = rights['zone']
    if zone in ['All', 'UG']:
        buttons.append([InlineKeyboardButton('РОССЕТИ ЮГ', callback_data='net_UG')])
    if zone in ['All', 'RK']:
        buttons.append([InlineKeyboardButton('РОССЕТИ КУБАНЬ', callback_data='net_RK')])
    if zone == 'All':
        buttons.append([InlineKeyboardButton('ТЕЛЕФОНЫ КОНТРАГЕНТОВ', callback_data='phones')])
        buttons.append([InlineKeyboardButton('ОТЧЕТЫ', callback_data='reports')])
    buttons.append([InlineKeyboardButton('СПРАВКА', callback_data='help')])
    return InlineKeyboardMarkup(buttons)

# Handlers
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.effective_user.id
    rights = get_user_rights(user_id)
    context.user_data['rights'] = rights
    await update.message.reply_text(
        'Выберите раздел:', reply_markup=build_main_menu(rights)
    )

async def go_back(update: Update, context: ContextTypes.DEFAULT_TYPE):
    callback = update.callback_query
    await callback.answer()
    return await start(update, context)

async def networks_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    callback = update.callback_query
    await callback.answer()
    _, net = callback.data.split('_')
    rights = context.user_data['rights']
    branches = {
        'RK': ['Юго-Западные ЭС','Усть-Лабинские ЭС','Тимашевские ЭС','Тихорецкие ЭС','Сочинские ЭС','Славянские ЭС','Ленинградские ЭС','Лабинские ЭС','Краснодарские ЭС','Армавирские ЭС','Адыгейские ЭС'],
        'UG': ['Юго-Западные ЭС','Центральные ЭС','Западные ЭС','Восточные ЭС','Южные ЭС','Северо-Восточные ЭС','Юго-Восточные ЭС','Северные ЭС']
    }
    if rights['filial'] and rights['filial'] != 'All':
        branches[net] = [rights['filial']]
    buttons = [[InlineKeyboardButton(b, callback_data=f'branch_{net}|{b}')] for b in branches[net]]
    buttons.append([InlineKeyboardButton('Назад', callback_data='back')])
    await callback.edit_message_text('Выберите филиал:', reply_markup=InlineKeyboardMarkup(buttons))

async def branch_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    callback = update.callback_query
    await callback.answer()
    net, branch = callback.data.split('_',1)[1].split('|',1)
    context.user_data.update({'net': net, 'branch': branch})
    buttons = [
        [InlineKeyboardButton('Поиск по ТП', callback_data='tp_search')],
        [InlineKeyboardButton('Отправить уведомление', callback_data='tp_notify')],
        [InlineKeyboardButton('Справка', callback_data='help')],
        [InlineKeyboardButton('Назад', callback_data=f'net_{net}')]
    ]
    await callback.edit_message_text(f'{branch}: выберите действие', reply_markup=InlineKeyboardMarkup(buttons))

async def tp_search_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cb = update.callback_query
    await cb.answer()
    context.user_data['action'] = 'search'
    await cb.edit_message_text('Введите наименование ТП:')

async def tp_notify_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cb = update.callback_query
    await cb.answer()
    context.user_data['action'] = 'notify'
    await cb.edit_message_text('Введите наименование ТП:')

async def tp_input_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = update.message.text
    action = context.user_data.get('action')
    net = context.user_data['net']
    branch = context.user_data['branch']
    key_base = branch.upper().replace(' ','').replace('-','')
    df = contracts.get(f"{key_base}_ES_URL_{net}") if action=='search' else references.get(f"{key_base}_ES_URL_{net}_SP")
    if df is None:
        return await update.message.reply_text('База не найдена.')
    df['norm_tp'] = df['Наименование ТП'].astype(str).apply(normalize_tp)
    matches = df[df['norm_tp'].str.contains(normalize_tp(text))]
    if matches.empty:
        return await update.message.reply_text('Совпадений не найдено.')
    context.user_data['tp_df'] = matches
    tps = matches['Наименование ТП'].unique()
    buttons = [[InlineKeyboardButton(tp, callback_data=f"tp_sel|{tp}")] for tp in tps]
    buttons.append([InlineKeyboardButton('Назад', callback_data=f'branch_{net}|{branch}')])
    await update.message.reply_text('Выберите ТП:', reply_markup=InlineKeyboardMarkup(buttons))

async def tp_select_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cb = update.callback_query
    await cb.answer()
    _, tp = cb.data.split('|',1)
    action = context.user_data['action']
    df = context.user_data['tp_df']
    sel = df[df['norm_tp']==normalize_tp(tp)]
    if action=='search':
        for _, r in sel.iterrows():
            await cb.message.reply_text(
                f"{r['РЭС']} - найдено {len(sel)} ВОЛС... 🔹 ВЛ: {r['Наименование ВЛ']}"
            )
    else:
        vl_list = sel['Наименование ВЛ'].unique()
        buttons = [[InlineKeyboardButton(vl, callback_data=f"vl_sel|{tp}|{vl}")] for vl in vl_list]
        buttons.append([InlineKeyboardButton('Назад', callback_data=f"branch_{context.user_data['net']}|{context.user_data['branch']}")])
        await cb.edit_message_text('Выберите ВЛ:', reply_markup=InlineKeyboardMarkup(buttons))
        context.user_data['sel_row'] = sel.iloc[0]

async def vl_select_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cb = update.callback_query
    await cb.answer()
    _, tp, vl = cb.data.split('|',2)
    context.user_data['selected_vl'] = vl
    await cb.edit_message_text(
        'Отправьте геолокацию:',
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton('📍 Геолокация', request_location=True)]])
    )

async def location_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    loc = update.message.location
    user = update.effective_user
    rights = context.user_data['rights']
    sel = context.user_data['sel_row']
    branch = context.user_data['branch']
    res = sel['РЭС']
    recips = zones_df[zones_df['Ответственный'].isin([branch, res])]
    if recips.empty:
        return await update.message.reply_text(f"🔔 Нет ответственных для {res}.")
    for _, r in recips.iterrows():
        await context.bot.send_message(
            chat_id=r['Telegram ID'],
            text=f"{user.full_name} нашел ВОЛС на {sel['Наименование ТП']}, {context.user_data['selected_vl']}\nCoord: {loc.longitude},{loc.latitude}"
        )
    await update.message.reply_text(f"🔔 Уведомление отправлено ответственным.")
    notifications[rights['zone']].append({...})

async def reports_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cb = update.callback_query
    await cb.answer()
    df = pd.DataFrame(notifications.get(context.user_data['rights']['zone'], []))
    if df.empty:
        return await cb.edit_message_text('Нет уведомлений.')
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)
    output.seek(0)
    await cb.message.reply_document(document=output, filename='report.xlsx')

async def help_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cb = update.callback_query
    await cb.answer()
    await cb.edit_message_text('Справка: [ссылки]')

async def phones_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    cb = update.callback_query
    await cb.answer()
    await cb.edit_message_text('В разработке.')

# Entry point
if __name__ == '__main__':
    app = Application.builder().token(BOT_TOKEN).build()
    app.add_handler(CommandHandler('start', start))
    app.add_handler(CallbackQueryHandler(go_back, pattern='^back$'))
    app.add_handler(CallbackQueryHandler(networks_handler, pattern='^net_'))
    app.add_handler(CallbackQueryHandler(branch_handler, pattern='^branch_'))
    app.add_handler(CallbackQueryHandler(tp_search_start, pattern='^tp_search$'))
    app.add_handler(CallbackQueryHandler(tp_notify_start, pattern='^tp_notify$'))
    app.add_handler(CallbackQueryHandler(tp_select_handler, pattern='^tp_sel\|'))
    app.add_handler(CallbackQueryHandler(vl_select_handler, pattern='^vl_sel\|'))
    app.add_handler(CallbackQueryHandler(reports_handler, pattern='^reports$'))
    app.add_handler(CallbackQueryHandler(help_handler, pattern='^help$'))
    app.add_handler(CallbackQueryHandler(phones_handler, pattern='^phones$'))
    app.add_handler(MessageHandler(filters.LOCATION, location_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, tp_input_handler))
    app.run_webhook(
        listen='0.0.0.0',
        port=int(os.getenv('PORT', 8443)),
        url_path='webhook',
        webhook_url=f"{WEBHOOK_URL}/webhook"
    )
