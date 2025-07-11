import os
import re
import logging
import pandas as pd
from io import BytesIO
from flask import Flask, request
from telegram import Bot, Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaDocument
from telegram.ext import Dispatcher, CommandHandler, CallbackQueryHandler, MessageHandler, Filters, CallbackContext

# Configure logging
tlogging = logging.getLogger()
tlogging.setLevel(logging.INFO)

# Environment variables
BOT_TOKEN = os.getenv('BOT_TOKEN')
ZONES_CSV_URL = os.getenv('ZONES_CSV_URL')  # zones and rights
# Load zones/rights
zones_df = pd.read_csv(ZONES_CSV_URL)

# Helper to load all contract and reference CSVs from env
contracts = {}  # key: env var, value: DataFrame
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

# Normalize function for TP search

def normalize_tp(s: str) -> str:
    return re.sub(r'[^0-9A-Za-z]', '', s).upper()

# Create Flask app and Telegram bot\ napp = Flask(__name__)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot, None, use_context=True)

# User state tracking
USER_STATE = {}  # chat_id -> state dict

# Access rights lookup
def get_user_rights(user_id):
    row = zones_df[zones_df['Telegram ID'] == user_id]
    if row.empty:
        return {'zone': None, 'filial': None, 'res': None}
    r = row.iloc[0]
    return {'zone': r['Видимость'], 'filial': r['Филиал'], 'res': r['РЭС']}

# Build main menu based on rights

def build_main_menu(rights):
    buttons = []
    zone = rights['zone']
    # Networks menus
    if zone in ['All', 'RK', 'UG']:
        buttons.append([InlineKeyboardButton('РОССЕТИ ЮГ', callback_data='net_UG')])
    if zone in ['All', 'RK']:
        buttons.append([InlineKeyboardButton('РОССЕТИ КУБАНЬ', callback_data='net_RK')])
    # Other always if All
    if zone == 'All':
        buttons.append([InlineKeyboardButton('ТЕЛЕФОНЫ КОНТРАГЕНТОВ', callback_data='phones')])
        buttons.append([InlineKeyboardButton('ОТЧЕТЫ', callback_data='reports')])
    buttons.append([InlineKeyboardButton('СПРАВКА', callback_data='help')])
    return InlineKeyboardMarkup(buttons)

# /start handler

def start(update: Update, context: CallbackContext):
    user_id = update.effective_user.id
    rights = get_user_rights(user_id)
    context.user_data['rights'] = rights
    update.message.reply_text(
        'Выберите раздел:',
        reply_markup=build_main_menu(rights)
    )

# Generic back

def go_back(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    return start(update, context)

# Handlers for networks
def networks_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    _, net = query.data.split('_')  # UG or RK
    rights = context.user_data['rights']
    # list of branches per network
    branches = {
        'RK': ['Юго-Западные ЭС','Усть-Лабинские ЭС','Тимашевские ЭС','Тихорецкие ЭС','Сочинские ЭС','Славянские ЭС','Ленинградские ЭС','Лабинские ЭС','Краснодарские ЭС','Армавирские ЭС','Адыгейские ЭС'],
        'UG': ['Юго-Западные ЭС','Центральные ЭС','Западные ЭС','Восточные ЭС','Южные ЭС','Северо-Восточные ЭС','Юго-Восточные ЭС','Северные ЭС']
    }
    # filter by filial rights
    if rights['filial'] and rights['filial'] != 'All':
        branches[net] = [rights['filial']]
    # build buttons
    buttons = [[InlineKeyboardButton(b, callback_data=f'branch_{net}|{b}')] for b in branches[net]]
    buttons.append([InlineKeyboardButton('Назад', callback_data='back')])
    query.edit_message_text('Выберите филиал:', reply_markup=InlineKeyboardMarkup(buttons))

# Branch menu

def branch_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    data = query.data.split('_',1)[1]  # RK|branch_name
    net, branch = data.split('|',1)
    context.user_data['net'] = net
    context.user_data['branch'] = branch
    # submenu: Поиск по ТП, Отправить уведомление, Справка, Назад
    buttons = [
        [InlineKeyboardButton('Поиск по ТП', callback_data='tp_search')],
        [InlineKeyboardButton('Отправить уведомление', callback_data='tp_notify')],
        [InlineKeyboardButton('Справка', callback_data='help')],
        [InlineKeyboardButton('Назад', callback_data=f'net_{net}')]
    ]
    query.edit_message_text(f'{branch}: выберите действие', reply_markup=InlineKeyboardMarkup(buttons))

# TP search

def tp_search_start(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    update = query
    context.user_data['action'] = 'search'
    update.edit_message_text('Введите наименование ТП:')

# TP notify

def tp_notify_start(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    context.user_data['action'] = 'notify'
    query.edit_message_text('Введите наименование ТП:')

# Handle TP input

def tp_input_handler(update: Update, context: CallbackContext):
    text = update.message.text
    action = context.user_data.get('action')
    net = context.user_data['net']
    branch = context.user_data['branch']
    # select contract or reference df
    if action == 'search':
        env_key = f"{branch.upper().replace(' ','').replace('-','')}_ES_URL_{net}"
        df = contracts.get(env_key)
        col = 'Наименование ТП'
    else:
        env_key = f"{branch.upper().replace(' ','').replace('-','')}_ES_URL_{net}_SP"
        df = references.get(env_key)
        col = 'Наименование ТП'
    if df is None:
        update.message.reply_text('База не найдена.')
        return
    norm = normalize_tp(text)
    df['norm_tp'] = df[col].astype(str).apply(normalize_tp)
    matches = df[df['norm_tp'].str.contains(norm)]
    if matches.empty:
        update.message.reply_text('Совпадений не найдено.')
        return
    # buttons for TPs
    tps = matches[col].unique()
    buttons = [[InlineKeyboardButton(tp, callback_data=f"tp_sel|{tp}")] for tp in tps]
    buttons.append([InlineKeyboardButton('Назад', callback_data=f'branch_{net}|{branch}')])
    update.message.reply_text('Выберите ТП:', reply_markup=InlineKeyboardMarkup(buttons))
    # store df for later
    context.user_data['tp_df'] = df

# TP selection

def tp_select_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    _, tp = query.data.split('|',1)
    action = context.user_data.get('action')
    df = context.user_data.get('tp_df')
    sel = df[df['norm_tp'] == normalize_tp(tp)]
    net = context.user_data['net']
    branch = context.user_data['branch']
    if action == 'search':
        # output each match
        messages = []
        for _, row in sel.iterrows():
            msg = (f"{row['РЭС']} - найдено {len(sel)} ВОЛС с договором аренды.\n"
                   f"🔹 ВЛ: {row['Наименование ВЛ']}\n"
                   f"Опоры: {row['Опоры']}, Кол-во опор: {row['Количество опор']}\n"
                   f"Контрагент: {row['Наименование Провайдера']}")
            messages.append(msg)
        for m in messages:
            query.message.reply_text(m)
    else:
        # notification: select VL
        vl_list = sel['Наименование ВЛ'].unique()
        buttons = [[InlineKeyboardButton(vl, callback_data=f"vl_sel|{tp}|{vl}")] for vl in vl_list]
        buttons.append([InlineKeyboardButton('Назад', callback_data=f'branch_{net}|{branch}')])
        query.edit_message_text('Выберите ВЛ:', reply_markup=InlineKeyboardMarkup(buttons))
        context.user_data['sel_row'] = sel.iloc[0]

# VL selection for notify

def vl_select_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    _, tp, vl = query.data.split('|',2)
    context.user_data['selected_vl'] = vl
    # ask for location
    query.edit_message_text('Пожалуйста, отправьте геолокацию:', reply_markup=InlineKeyboardMarkup(
        [[InlineKeyboardButton('Отправить геолокацию', request_location=True)]])
    )

# Location handler

def location_handler(update: Update, context: CallbackContext):
    loc = update.message.location
    user = update.effective_user
    rights = context.user_data['rights']
    sel = context.user_data['sel_row']
    branch = context.user_data['branch']
    res = sel['РЭС']
    # determine recipients
    cond = zones_df['Ответственный'].isin([branch, res])
    recips = zones_df[cond]
    if recips.empty:
        update.message.reply_text(f"🔔 Ответственный не назначен на {res} РЭС.")
        return
    for _, r in recips.iterrows():
        bot.send_message(r['Telegram ID'],
                         f"{user.full_name} нашел бездоговорной ВОЛС на {sel['Наименование ТП']}, {context.user_data['selected_vl']}.\n"
                         f"Coord: {loc.longitude}, {loc.latitude}")
    update.message.reply_text(f"🔔 Уведомление отправлено ответственному за {res} РЭС.")
    # save notification
    notifications[rights['zone']].append({
        'filial': branch,
        'res': res,
        'from_name': user.full_name,
        'from_id': user.id,
        'to': recips[['ФИО','Telegram ID']].to_dict('records'),
        'datetime': pd.Timestamp.now(),
        'coords': (loc.longitude, loc.latitude)
    })

# Reports handler

def reports_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    rights = context.user_data['rights']
    zone = rights['zone']
    df = pd.DataFrame(notifications.get(zone, []))
    if df.empty:
        query.edit_message_text('Нет уведомлений для отчета.')
        return
    # filter by filial if needed
    if rights['filial'] not in ['All', None]:
        df = df[df['filial'] == rights['filial']]
    # build excel
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Report')
        workbook = writer.book
        worksheet = writer.sheets['Report']
        # set column widths & header format
        for i, col in enumerate(df.columns):
            width = max(df[col].astype(str).map(len).max(), len(col)) + 2
            worksheet.set_column(i, i, width)
        header_fmt = workbook.add_format({'bg_color': '#FFC0CB'})
        for col_num, value in enumerate(df.columns):
            worksheet.write(0, col_num, value, header_fmt)
    output.seek(0)
    query.message.reply_document(document=output, filename=f"report_{zone}.xlsx")

# Help handler

def help_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    query.answer()
    query.edit_message_text('Справка доступна по ссылкам в документации.')

# Register handlers
dp.add_handler(CommandHandler('start', start))
dp.add_handler(CallbackQueryHandler(go_back, pattern='^back$'))
dp.add_handler(CallbackQueryHandler(networks_handler, pattern='^net_'))
dp.add_handler(CallbackQueryHandler(branch_handler, pattern='^branch_'))
dp.add_handler(CallbackQueryHandler(tp_search_start, pattern='^tp_search$'))
dp.add_handler(CallbackQueryHandler(tp_notify_start, pattern='^tp_notify$'))
dp.add_handler(CallbackQueryHandler(tp_select_handler, pattern='^tp_sel\|'))

dp.add_handler(CallbackQueryHandler(vl_select_handler, pattern='^vl_sel\|'))
dp.add_handler(CallbackQueryHandler(reports_handler, pattern='^reports$'))
dp.add_handler(CallbackQueryHandler(help_handler, pattern='^help$'))
dp.add_handler(MessageHandler(Filters.location, location_handler))
dp.add_handler(MessageHandler(Filters.text & ~Filters.command, tp_input_handler))

# Webhook route
@app.route('/webhook', methods=['POST'])
def webhook():
    update = Update.de_json(request.get_json(force=True), bot)
    dp.process_update(update)
    return 'ok'

if __name__ == '__main__':
    # Set webhook on start (modify URL to your domain)
    WEBHOOK_URL = os.getenv('WEBHOOK_URL')
    bot.set_webhook(WEBHOOK_URL + '/webhook')
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 8443)))
