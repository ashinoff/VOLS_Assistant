from telegram import Update, ReplyKeyboardMarkup
from telegram.ext import ContextTypes, ConversationHandler
from utils.normalize import normalize_tp_name
from services.file_loader import load_csv
from config import TIMASHEVSKIE_ES_URL_RK  # Пример, подставлять динамически по филиалу

SEARCH_TP, CHOOSE_TP = range(2)

async def search_tp_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Введите наименование ТП:")
    return SEARCH_TP

async def search_tp_query(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = normalize_tp_name(update.message.text)
    df = load_csv(TIMASHEVSKIE_ES_URL_RK)  # подставлять по выбранному филиалу!
    df['normalized'] = df['Наименование ТП'].apply(normalize_tp_name)
    found = df[df['normalized'].str.contains(query)]
    if found.empty:
        await update.message.reply_text("ТП не найдено. Попробуйте еще раз или 'Назад'.")
        return SEARCH_TP
    buttons = [[row['Наименование ТП']] for _, row in found.iterrows()]
    buttons.append(["Назад"])
    await update.message.reply_text("Выберите ТП:", reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True))
    context.user_data['tp_found'] = found
    return CHOOSE_TP

async def search_tp_choose(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tp = update.message.text
    found = context.user_data.get('tp_found')
    if found is not None:
        row = found[found['Наименование ТП'] == tp]
        if row.empty:
            await update.message.reply_text("ТП не найдено. Попробуйте снова.")
            return CHOOSE_TP
        res = row.iloc[0]['РЭС']
        # Форматируем ответ по ТЗ
        await update.message.reply_text(f"({res}) РЭС, на ({tp}) найдено ...")
        # Здесь добавить подробную информацию из строк found, как в ТЗ
        return ConversationHandler.END
    await update.message.reply_text("Ошибка поиска.")
    return ConversationHandler.END
