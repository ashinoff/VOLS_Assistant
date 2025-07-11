import os
from flask import Flask, request
from telegram import Update
from telegram.ext import Application, CommandHandler, MessageHandler, filters, ContextTypes

from handlers.start import start  # обязательно должен быть этот файл и функция
from handlers.help import help_command  # по желанию, если есть

TOKEN = os.getenv('TOKEN')
SELF_URL = os.getenv('SELF_URL')
PORT = int(os.getenv('PORT', 8000))

app = Flask(__name__)
application = Application.builder().token(TOKEN).build()

# --- ОБЯЗАТЕЛЬНО РЕГИСТРИРУЕМ ХЭНДЛЕРЫ ---
application.add_handler(CommandHandler('start', start))
# application.add_handler(CommandHandler('help', help_command))
# application.add_handler(MessageHandler(filters.TEXT, handle_message))
# добавляй свои остальные хэндлеры тут

@app.route('/webhook', methods=['POST'])
def webhook():
    update = Update.de_json(request.get_json(force=True), application.bot)
    application.update_queue.put_nowait(update)
    return 'ok'

if __name__ == '__main__':
    application.bot.set_webhook(url=f'{SELF_URL}/webhook')
    app.run(host='0.0.0.0', port=PORT)
