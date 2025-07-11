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
    # Networks
    if zone in ['All', 'UG']:
        buttons.append([InlineKeyboardButton('РОССЕТИ ЮГ', callback_data='net_UG')])
    if zone in ['All', 'RK']:
        buttons.append([InlineKeyboardButton('РОССЕТИ КУБАНЬ', callback_data='net_RK')])
    # Other for All
    if zone == 'All':
        buttons.append([InlineKeyboardButton('ТЕЛЕФОНЫ КОНТРАГЕНТОВ', callback_data='phones')])
        buttons.append([InlineKeyboardButton('ОТЧЕТЫ', callback_data='reports')])
    buttons.append([InlineKeyboardButton('СПРАВКА', callback_data='help')])
    return InlineKeyboardMarkup(buttons)

# Handler definitions omitted for brevity...
# (Keep remaining handlers as in previous version)

# Build and run the app
if __name__ == '__main__':
    app = Application.builder().token(BOT_TOKEN).build()
    # Register handlers (as before)
    app.add_handler(CommandHandler('start', start))
    # ... other handlers ...

    # Set webhook and start
    app.run_webhook(
        listen='0.0.0.0',
        port=int(os.environ.get('PORT', 8443)),
        url_path='webhook',
        webhook_url=f"{WEBHOOK_URL}/webhook"
    )
