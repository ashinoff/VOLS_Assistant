import os
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ContextTypes
from utils.folder_reader import get_public_folder_files

HELP_FOLDER_URL = os.getenv("HELP_FOLDER_URL")

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    files = get_public_folder_files(HELP_FOLDER_URL)
    if not files:
        await update.message.reply_text("В папке справки пока нет файлов.")
        return

    keyboard = [
        [InlineKeyboardButton(name, url=url)]
        for name, url in files
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("Справочные файлы:", reply_markup=reply_markup)
