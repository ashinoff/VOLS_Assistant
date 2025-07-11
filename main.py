import os
import logging
import csv
import io
import re
from datetime import datetime
from typing import Dict, List, Tuple, Optional
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import pandas as pd
from io import BytesIO

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы
BOT_TOKEN = os.environ.get('BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
PORT = int(os.environ.get('PORT', 5000))
ZONES_CSV_URL = os.environ.get('ZONES_CSV_URL')

# Списки филиалов
ROSSETI_KUBAN_BRANCHES = [
    "Юго-Западные ЭС", "Усть-Лабинские ЭС", "Тимашевские ЭС", "Тихорецкие ЭС",
    "Славянские ЭС", "Ленинградские ЭС", "Лабинские ЭС",
    "Краснодарские ЭС", "Армавирские ЭС", "Адыгейские ЭС", "Сочинские ЭС"
]

ROSSETI_YUG_BRANCHES = [
    "Юго-Западные ЭС", "Центральные ЭС", "Западные ЭС", "Восточные ЭС",
    "Южные ЭС", "Северо-Восточные ЭС", "Юго-Восточные ЭС", "Северные ЭС"
]

# Хранилище уведомлений
notifications_storage = {
    'RK': [],
    'UG': []
}

# Состояния пользователей
user_states = {}

# Кеш данных пользователей
users_cache = {}

def get_env_key_for_branch(branch: str, network: str, is_reference: bool = False) -> str:
    """Получить ключ переменной окружения для филиала"""
    # Транслитерация русских названий в латиницу
    translit_map = {
        'Юго-Западные': 'YUGO_ZAPADNYE',
        'Усть-Лабинские': 'UST_LABINSKIE', 
        'Тимашевские': 'TIMASHEVSKIE',
        'Тихорецкие': 'TIKHORETSKIE',
        'Сочинские': 'SOCHINSKIE',
        'Славянские': 'SLAVYANSKIE',
        'Ленинградские': 'LENINGRADSKIE',
        'Лабинские': 'LABINSKIE',
        'Краснодарские': 'KRASNODARSKIE',
        'Армавирские': 'ARMAVIRSKIE',
        'Адыгейские': 'ADYGEYSKIE',
        'Центральные': 'TSENTRALNYE',
        'Западные': 'ZAPADNYE',
        'Восточные': 'VOSTOCHNYE',
        'Южные': 'YUZHNYE',
        'Северо-Восточные': 'SEVERO_VOSTOCHNYE',
        'Юго-Восточные': 'YUGO_VOSTOCHNYE',
        'Северные': 'SEVERNYE'
    }
    
    # Убираем "ЭС" и ищем в словаре транслитерации
    branch_clean = branch.replace(' ЭС', '').strip()
    branch_key = translit_map.get(branch_clean, branch_clean.upper().replace(' ', '_').replace('-', '_'))
    
    suffix = f"_{network}_SP" if is_reference else f"_{network}"
    env_key = f"{branch_key}_URL{suffix}"
    logger.info(f"Ищем переменную окружения: {env_key} для филиала: {branch}")
    return env_key

def load_csv_from_url(url: str) -> List[Dict]:
    """Загрузить CSV файл по URL"""
    try:
        response = requests.get(url)
        response.raise_for_status()
        response.encoding = 'utf-8-sig'
        
        csv_file = io.StringIO(response.text)
        reader = csv.DictReader(csv_file)
        
        # Нормализуем заголовки - убираем лишние пробелы
        data = []
        for row in reader:
            normalized_row = {key.strip(): value.strip() if value else '' for key, value in row.items()}
            data.append(normalized_row)
        
        return data
    except Exception as e:
        logger.error(f"Ошибка загрузки CSV: {e}")
        return []

def load_users_data():
    """Загрузить данные пользователей из CSV"""
    global users_cache
    try:
        data = load_csv_from_url(ZONES_CSV_URL)
        users_cache = {}
        for row in data:
            telegram_id = row.get('Telegram ID', '').strip()
            if telegram_id:
                users_cache[telegram_id] = {
                    'visibility': row.get('Видимость', '').strip(),
                    'branch': row.get('Филиал', '').strip(),
                    'res': row.get('РЭС', '').strip(),
                    'name': row.get('ФИО', '').strip(),
                    'responsible': row.get('Ответственный', '').strip()
                }
        logger.info(f"Загружено {len(users_cache)} пользователей")
    except Exception as e:
        logger.error(f"Ошибка загрузки данных пользователей: {e}")

def get_user_permissions(user_id: str) -> Dict:
    """Получить права пользователя"""
    if not users_cache:
        load_users_data()
    
    return users_cache.get(str(user_id), {
        'visibility': None,
        'branch': None,
        'res': None,
        'name': 'Неизвестный',
        'responsible': None
    })

def normalize_tp_name(name: str) -> str:
    """Нормализовать название ТП для поиска"""
    # Убираем все символы кроме цифр
    return ''.join(filter(str.isdigit, name))

def search_tp_in_data(tp_query: str, data: List[Dict], column: str) -> List[Dict]:
    """Поиск ТП в данных"""
    normalized_query = normalize_tp_name(tp_query)
    results = []
    
    for row in data:
        tp_name = row.get(column, '')
        normalized_tp = normalize_tp_name(tp_name)
        
        if normalized_query in normalized_tp:
            results.append(row)
    
    return results

def get_main_keyboard(permissions: Dict) -> ReplyKeyboardMarkup:
    """Получить главную клавиатуру в зависимости от прав"""
    keyboard = []
    
    visibility = permissions.get('visibility')
    branch = permissions.get('branch')
    res = permissions.get('res')
    
    # РОССЕТИ кнопки
    if visibility == 'All':
        keyboard.append(['🏢 РОССЕТИ КУБАНЬ'])
        keyboard.append(['🏢 РОССЕТИ ЮГ'])
    elif visibility == 'RK':
        if branch == 'All':
            keyboard.append(['🏢 РОССЕТИ КУБАНЬ'])
        else:
            keyboard.append([f'⚡ {branch}'])
    elif visibility == 'UG':
        if branch == 'All':
            keyboard.append(['🏢 РОССЕТИ ЮГ'])
        else:
            keyboard.append([f'⚡ {branch}'])
    
    # Телефоны контрагентов
    keyboard.append(['📞 ТЕЛЕФОНЫ КОНТРАГЕНТОВ'])
    
    # Отчеты
    if res == 'All':
        if visibility == 'All':
            keyboard.append(['📊 ОТЧЕТЫ'])
        elif visibility == 'RK':
            keyboard.append(['📊 ОТЧЕТЫ'])
        elif visibility == 'UG':
            keyboard.append(['📊 ОТЧЕТЫ'])
    
    # Справка
    keyboard.append(['ℹ️ СПРАВКА'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_branch_keyboard(branches: List[str]) -> ReplyKeyboardMarkup:
    """Получить клавиатуру с филиалами"""
    keyboard = []
    
    if len(branches) == 11:  # РОССЕТИ КУБАНЬ
        # 5 слева, 5 справа, 1 внизу (Сочинские)
        for i in range(0, 10, 2):
            keyboard.append([f'⚡ {branches[i]}', f'⚡ {branches[i+1]}'])
        keyboard.append([f'⚡ {branches[10]}'])  # Сочинские ЭС
    elif len(branches) == 8:  # РОССЕТИ ЮГ  
        # 4 слева, 4 справа
        for i in range(0, 8, 2):
            keyboard.append([f'⚡ {branches[i]}', f'⚡ {branches[i+1]}'])
    else:
        # Для других случаев - по одному в строку
        for branch in branches:
            keyboard.append([f'⚡ {branch}'])
    
    keyboard.append(['⬅️ Назад'])
    
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_branch_menu_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура меню филиала"""
    keyboard = [
        ['🔍 Поиск по ТП'],
        ['📨 Отправить уведомление'],
        ['ℹ️ Справка'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_reports_keyboard(permissions: Dict) -> ReplyKeyboardMarkup:
    """Клавиатура отчетов"""
    keyboard = []
    visibility = permissions.get('visibility')
    
    if visibility == 'All':
        keyboard.append(['📊 Уведомления РОССЕТИ КУБАНЬ'])
        keyboard.append(['📊 Уведомления РОССЕТИ ЮГ'])
    elif visibility == 'RK':
        keyboard.append(['📊 Уведомления РОССЕТИ КУБАНЬ'])
    elif visibility == 'UG':
        keyboard.append(['📊 Уведомления РОССЕТИ ЮГ'])
    
    keyboard.append(['⬅️ Назад'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_reference_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура справки с динамическими кнопками из Google Drive"""
    keyboard = []
    
    # Получаем список файлов из переменной окружения
    folder_id = os.environ.get('REFERENCE_FOLDER_ID')
    if folder_id:
        try:
            # Здесь должен быть код для получения списка файлов из Google Drive
            # Пока используем статические кнопки
            keyboard.append(['📄 Форма доп соглашения'])
            keyboard.append(['📄 Форма претензии'])
        except Exception as e:
            logger.error(f"Ошибка получения файлов справки: {e}")
            keyboard.append(['📄 Форма доп соглашения'])
            keyboard.append(['📄 Форма претензии'])
    else:
        # Статические кнопки по умолчанию
        keyboard.append(['📄 Форма доп соглашения'])
        keyboard.append(['📄 Форма претензии'])
    
    keyboard.append(['⬅️ Назад'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = str(update.effective_user.id)
    permissions = get_user_permissions(user_id)
    
    # Логируем для отладки
    logger.info(f"Пользователь {user_id} ({update.effective_user.first_name}) запустил бота")
    
    if not permissions['visibility']:
        await update.message.reply_text(
            f"❌ У вас нет доступа к боту.\n"
            f"Ваш ID: {user_id}\n"
            f"Обратитесь к администратору для получения прав."
        )
        return
    
    user_states[user_id] = {'state': 'main'}
    
    await update.message.reply_text(
        f"👋 Добро пожаловать, {permissions['name']}!\n"
        f"Ваш ID: {user_id}\n"
        f"Ваши права: {permissions['visibility']} | {permissions['branch']} | {permissions['res']}",
        reply_markup=get_main_keyboard(permissions)
    )

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = str(update.effective_user.id)
    text = update.message.text
    permissions = get_user_permissions(user_id)
    
    if not permissions['visibility']:
        await update.message.reply_text("❌ У вас нет доступа к боту.")
        return
    
    state = user_states.get(user_id, {}).get('state', 'main')
    
    # Обработка кнопки Назад
    if text == '⬅️ Назад':
        if state in ['rosseti_kuban', 'rosseti_yug', 'reports', 'reference', 'phones']:
            user_states[user_id] = {'state': 'main'}
            await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(permissions))
        elif state.startswith('branch_'):
            network = user_states[user_id].get('network')
            if network == 'RK':
                user_states[user_id] = {'state': 'rosseti_kuban', 'network': 'RK'}
                branches = ROSSETI_KUBAN_BRANCHES
            else:
                user_states[user_id] = {'state': 'rosseti_yug', 'network': 'UG'}
                branches = ROSSETI_YUG_BRANCHES
            await update.message.reply_text("Выберите филиал", reply_markup=get_branch_keyboard(branches))
        elif state in ['search_tp', 'send_notification']:
            branch = user_states[user_id].get('branch')
            user_states[user_id]['state'] = f'branch_{branch}'
            await update.message.reply_text("Меню филиала", reply_markup=get_branch_menu_keyboard())
        return
    
    # Главное меню
    if state == 'main':
        if text == '🏢 РОССЕТИ КУБАНЬ':
            if permissions['visibility'] in ['All', 'RK']:
                if permissions['branch'] == 'All':
                    user_states[user_id] = {'state': 'rosseti_kuban', 'network': 'RK'}
                    await update.message.reply_text(
                        "Выберите филиал РОССЕТИ КУБАНЬ",
                        reply_markup=get_branch_keyboard(ROSSETI_KUBAN_BRANCHES)
                    )
                else:
                    # Если доступен только один филиал
                    user_states[user_id] = {'state': f'branch_{permissions["branch"]}', 'branch': permissions['branch'], 'network': 'RK'}
                    await update.message.reply_text(
                        f"Меню филиала {permissions['branch']}",
                        reply_markup=get_branch_menu_keyboard()
                    )
        
        elif text == '🏢 РОССЕТИ ЮГ':
            if permissions['visibility'] in ['All', 'UG']:
                if permissions['branch'] == 'All':
                    user_states[user_id] = {'state': 'rosseti_yug', 'network': 'UG'}
                    await update.message.reply_text(
                        "Выберите филиал РОССЕТИ ЮГ",
                        reply_markup=get_branch_keyboard(ROSSETI_YUG_BRANCHES)
                    )
                else:
                    user_states[user_id] = {'state': f'branch_{permissions["branch"]}', 'branch': permissions['branch'], 'network': 'UG'}
                    await update.message.reply_text(
                        f"Меню филиала {permissions['branch']}",
                        reply_markup=get_branch_menu_keyboard()
                    )
        
        elif text.startswith('⚡ '):
            # Обработка прямого перехода к филиалу для пользователей с ограниченными правами
            branch = text[2:]
            network = 'RK' if permissions['visibility'] == 'RK' else 'UG'
            user_states[user_id] = {'state': f'branch_{branch}', 'branch': branch, 'network': network}
            await update.message.reply_text(
                f"Меню филиала {branch}",
                reply_markup=get_branch_menu_keyboard()
            )
        
        elif text == '📊 ОТЧЕТЫ':
            user_states[user_id] = {'state': 'reports'}
            await update.message.reply_text(
                "Выберите тип отчета",
                reply_markup=get_reports_keyboard(permissions)
            )
        
        elif text == 'ℹ️ СПРАВКА':
            user_states[user_id] = {'state': 'reference'}
            await update.message.reply_text(
                "Выберите документ",
                reply_markup=get_reference_keyboard()
            )
        
        elif text == '📞 ТЕЛЕФОНЫ КОНТРАГЕНТОВ':
            await update.message.reply_text("🚧 Раздел в разработке")
    
    # Выбор филиала
    elif state in ['rosseti_kuban', 'rosseti_yug']:
        if text.startswith('⚡ '):
            branch = text[2:]  # Убираем символ молнии
            user_states[user_id]['state'] = f'branch_{branch}'
            user_states[user_id]['branch'] = branch
            await update.message.reply_text(
                f"Меню филиала {branch}",
                reply_markup=get_branch_menu_keyboard()
            )
    
    # Меню филиала
    elif state.startswith('branch_'):
        if text == '🔍 Поиск по ТП':
            user_states[user_id]['state'] = 'search_tp'
            user_states[user_id]['action'] = 'search'
            keyboard = [['⬅️ Назад']]
            await update.message.reply_text(
                "🔍 Введите наименование ТП для поиска\n"
                "Примеры: Н-6477, 6-47, 6477, 477",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        elif text == '📨 Отправить уведомление':
            user_states[user_id]['state'] = 'send_notification'
            user_states[user_id]['action'] = 'notification_tp'
            keyboard = [['⬅️ Назад']]
            await update.message.reply_text(
                "📨 Введите наименование ТП для уведомления\n"
                "Примеры: Н-6477, 6-47, 6477, 477",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        elif text == 'ℹ️ Справка':
            user_states[user_id]['state'] = 'reference'
            await update.message.reply_text(
                "Выберите документ",
                reply_markup=get_reference_keyboard()
            )
    
    # Поиск ТП
    elif state == 'search_tp' and user_states[user_id].get('action') == 'search':
        branch = user_states[user_id].get('branch')
        network = user_states[user_id].get('network')
        
        logger.info(f"Поиск ТП для филиала: {branch}, сеть: {network}")
        
        # Загружаем данные филиала
        env_key = get_env_key_for_branch(branch, network)
        csv_url = os.environ.get(env_key)
        
        logger.info(f"URL из переменной {env_key}: {csv_url}")
        
        if not csv_url:
            # Показываем все доступные переменные окружения для отладки
            available_vars = [key for key in os.environ.keys() if 'URL' in key and network in key]
            logger.error(f"Доступные переменные для {network}: {available_vars}")
            await update.message.reply_text(
                f"❌ Данные для филиала {branch} не найдены\n"
                f"Искали переменную: {env_key}\n"
                f"Доступные: {', '.join(available_vars[:5])}"
            )
            return
        
        data = load_csv_from_url(csv_url)
        results = search_tp_in_data(text, data, 'Наименование ТП')
        
        if not results:
            await update.message.reply_text("❌ ТП не найдено. Попробуйте другой запрос.")
            return
        
        # Группируем результаты по ТП
        tp_list = list(set([r['Наименование ТП'] for r in results]))
        
        if len(tp_list) == 1:
            # Если найдена только одна ТП, сразу показываем результаты
            await show_tp_results(update, results, tp_list[0])
        else:
            # Показываем список найденных ТП
            keyboard = []
            for tp in tp_list[:10]:  # Ограничиваем 10 результатами
                keyboard.append([tp])
            keyboard.append(['⬅️ Назад'])
            
            user_states[user_id]['search_results'] = results
            user_states[user_id]['action'] = 'select_tp'
            
            await update.message.reply_text(
                f"Найдено {len(tp_list)} ТП. Выберите нужную:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
    
    # Выбор ТП из результатов поиска
    elif state == 'search_tp' and user_states[user_id].get('action') == 'select_tp':
        results = user_states[user_id].get('search_results', [])
        filtered_results = [r for r in results if r['Наименование ТП'] == text]
        
        if filtered_results:
            await show_tp_results(update, filtered_results, text)
            user_states[user_id]['action'] = 'search'
    
    # Уведомление - поиск ТП
    elif state == 'send_notification' and user_states[user_id].get('action') == 'notification_tp':
        branch = user_states[user_id].get('branch')
        network = user_states[user_id].get('network')
        
        # Загружаем справочник
        env_key = get_env_key_for_branch(branch, network, is_reference=True)
        csv_url = os.environ.get(env_key)
        
        if not csv_url:
            await update.message.reply_text(f"❌ Справочник для филиала {branch} не найден")
            return
        
        data = load_csv_from_url(csv_url)
        results = search_tp_in_data(text, data, 'Наименование ТП')
        
        if not results:
            await update.message.reply_text("❌ ТП не найдено. Попробуйте другой запрос.")
            return
        
        # Группируем результаты по ТП
        tp_list = list(set([r['Наименование ТП'] for r in results]))
        
        keyboard = []
        for tp in tp_list[:10]:
            keyboard.append([tp])
        keyboard.append(['⬅️ Назад'])
        
        user_states[user_id]['notification_results'] = results
        user_states[user_id]['action'] = 'select_notification_tp'
        
        await update.message.reply_text(
            f"Найдено {len(tp_list)} ТП. Выберите нужную:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
    
    # Выбор ТП для уведомления
    elif state == 'send_notification' and user_states[user_id].get('action') == 'select_notification_tp':
        results = user_states[user_id].get('notification_results', [])
        filtered_results = [r for r in results if r['Наименование ТП'] == text]
        
        if filtered_results:
            # Сохраняем выбранную ТП
            user_states[user_id]['selected_tp'] = text
            user_states[user_id]['tp_data'] = filtered_results[0]
            
            # Получаем список ВЛ для выбранной ТП
            vl_list = list(set([r['Наименование ВЛ'] for r in filtered_results]))
            
            keyboard = []
            for vl in vl_list:
                keyboard.append([vl])
            keyboard.append(['⬅️ Назад'])
            
            user_states[user_id]['action'] = 'select_vl'
            
            await update.message.reply_text(
                f"Выберите ВЛ для ТП {text}:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
    
    # Выбор ВЛ
    elif state == 'send_notification' and user_states[user_id].get('action') == 'select_vl':
        user_states[user_id]['selected_vl'] = text
        user_states[user_id]['action'] = 'send_location'
        
        keyboard = [[KeyboardButton("📍 Отправить местоположение", request_location=True)]]
        keyboard.append(['⬅️ Назад'])
        
        await update.message.reply_text(
            "📍 Отправьте ваше местоположение",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )
    
    # Отчеты
    elif state == 'reports':
        if text == '📊 Уведомления РОССЕТИ КУБАНЬ':
            await generate_report(update, context, 'RK', permissions)
        elif text == '📊 Уведомления РОССЕТИ ЮГ':
            await generate_report(update, context, 'UG', permissions)
    
    # Справка
    elif state == 'reference':
        if text == '📄 Форма доп соглашения':
            # Здесь должна быть загрузка документа с Google Drive
            await update.message.reply_text("📄 Документ будет отправлен...")
        elif text == '📄 Форма претензии':
            # Здесь должна быть загрузка документа с Google Drive
            await update.message.reply_text("📄 Документ будет отправлен...")

async def show_tp_results(update: Update, results: List[Dict], tp_name: str):
    """Показать результаты поиска по ТП"""
    if not results:
        await update.message.reply_text("❌ Результаты не найдены")
        return
        
    # Получаем РЭС из первого результата
    res_name = results[0].get('РЭС', 'Неизвестный')
    
    message = f"📍 {res_name} РЭС, на {tp_name} найдено {len(results)} ВОЛС с договором аренды.\n\n"
    
    for result in results:
        # Обрабатываем каждый результат
        vl = result.get('Наименование ВЛ', '-')
        supports = result.get('Опоры', '-')
        supports_count = result.get('Количество опор', '-')
        provider = result.get('Наименование Провайдера', '-')
        
        message += f"⚡ ВЛ: {vl}\n"
        message += f"Опоры: {supports}, Количество опор: {supports_count}\n"
        message += f"Контрагент: {provider}\n\n"
    
    # Отправляем сообщение по частям, если оно слишком длинное
    if len(message) > 4000:
        parts = []
        current_part = f"📍 {res_name} РЭС, на {tp_name} найдено {len(results)} ВОЛС с договором аренды.\n\n"
        
        for result in results:
            result_text = f"⚡ ВЛ: {result.get('Наименование ВЛ', '-')}\n"
            result_text += f"Опоры: {result.get('Опоры', '-')}, Количество опор: {result.get('Количество опор', '-')}\n"
            result_text += f"Контрагент: {result.get('Наименование Провайдера', '-')}\n\n"
            
            if len(current_part + result_text) > 4000:
                parts.append(current_part)
                current_part = result_text
            else:
                current_part += result_text
        
        if current_part:
            parts.append(current_part)
        
        for part in parts:
            await update.message.reply_text(part)
    else:
        await update.message.reply_text(message)

async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка геолокации"""
    user_id = str(update.effective_user.id)
    state = user_states.get(user_id, {}).get('state')
    
    if state == 'send_notification' and user_states[user_id].get('action') == 'send_location':
        location = update.message.location
        tp_data = user_states[user_id].get('tp_data', {})
        selected_tp = user_states[user_id].get('selected_tp')
        selected_vl = user_states[user_id].get('selected_vl')
        
        # Получаем данные об ответственных
        branch = tp_data.get('Филиал', '')
        res = tp_data.get('РЭС', '')
        
        logger.info(f"Ищем ответственных для Филиал: {branch}, РЭС: {res}")
        
        # Загружаем свежие данные пользователей
        load_users_data()
        
        # Ищем ответственных
        responsible_users = []
        for tid, user_data in users_cache.items():
            responsible = user_data.get('responsible', '')
            # Проверяем совпадение с филиалом или РЭС
            if responsible and (responsible.strip() == branch.strip() or responsible.strip() == res.strip()):
                responsible_users.append((tid, user_data))
                logger.info(f"Найден ответственный: {tid} - {user_data['name']} (ответственный за: {responsible})")
        
        if not responsible_users:
            await update.message.reply_text(f"❌ Ответственный не назначен на {res} РЭС")
            # Возвращаемся в меню филиала
            branch_name = user_states[user_id].get('branch')
            user_states[user_id] = {'state': f'branch_{branch_name}', 'branch': branch_name, 'network': user_states[user_id].get('network')}
            await update.message.reply_text("Меню филиала", reply_markup=get_branch_menu_keyboard())
            return
        
        # Отправляем уведомления
        sender_permissions = get_user_permissions(user_id)
        sender_name = sender_permissions['name']
        
        success_count = 0
        failed_users = []
        
        for recipient_id, recipient_data in responsible_users:
            try:
                # Отправляем сообщение
                await context.bot.send_message(
                    chat_id=recipient_id,
                    text=f"🔔 {sender_name} нашел бездоговорной ВОЛС на {selected_tp}, {selected_vl}"
                )
                
                # Отправляем локацию
                await context.bot.send_location(
                    chat_id=recipient_id,
                    latitude=location.latitude,
                    longitude=location.longitude
                )
                
                # Отправляем координаты текстом
                await context.bot.send_message(
                    chat_id=recipient_id,
                    text=f"{location.latitude}, {location.longitude}"
                )
                
                success_count += 1
                
                # Сохраняем уведомление
                notification = {
                    'branch': branch,
                    'res': res,
                    'sender_name': sender_name,
                    'sender_id': user_id,
                    'recipient_name': recipient_data['name'],
                    'recipient_id': recipient_id,
                    'datetime': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'coordinates': f"{location.latitude}, {location.longitude}",
                    'tp': selected_tp,
                    'vl': selected_vl
                }
                
                network = user_states[user_id].get('network')
                notifications_storage[network].append(notification)
                
            except Exception as e:
                logger.error(f"Ошибка отправки уведомления пользователю {recipient_id}: {e}")
                if "Chat not found" in str(e):
                    failed_users.append(f"{recipient_data['name']} (не начал диалог с ботом)")
                else:
                    failed_users.append(f"{recipient_data['name']} ({str(e)})")
        
        # Формируем ответ
        if success_count > 0 and not failed_users:
            await update.message.reply_text(f"✅ Уведомление отправлено ответственному за {res} РЭС")
        elif success_count > 0 and failed_users:
            await update.message.reply_text(
                f"⚠️ Уведомление отправлено {success_count} из {len(responsible_users)} ответственных за {res} РЭС\n\n"
                f"Не удалось отправить:\n" + "\n".join(f"• {user}" for user in failed_users)
            )
        else:
            await update.message.reply_text(
                f"❌ Не удалось отправить уведомления\n\n"
                f"Проблемы:\n" + "\n".join(f"• {user}" for user in failed_users)
            )
        
        # Возвращаемся в меню филиала
        branch_name = user_states[user_id].get('branch')
        user_states[user_id] = {'state': f'branch_{branch_name}', 'branch': branch_name, 'network': user_states[user_id].get('network')}
        await update.message.reply_text("Меню филиала", reply_markup=get_branch_menu_keyboard())

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета"""
    notifications = notifications_storage[network]
    
    if not notifications:
        await update.message.reply_text("📊 Нет данных для отчета")
        return
    
    # Фильтруем уведомления в зависимости от прав
    if permissions['branch'] != 'All':
        notifications = [n for n in notifications if n['branch'] == permissions['branch']]
    
    if not notifications:
        await update.message.reply_text("📊 Нет данных для отчета по вашему филиалу")
        return
    
    # Создаем DataFrame
    df = pd.DataFrame(notifications)
    df = df[['branch', 'res', 'sender_name', 'sender_id', 'recipient_name', 'recipient_id', 'datetime', 'coordinates']]
    df.columns = ['ФИЛИАЛ', 'РЭС', 'ФИО ОТПРАВИТЕЛЯ', 'ID ОТПРАВИТЕЛЯ', 'ФИО ПОЛУЧАТЕЛЯ', 'ID ПОЛУЧАТЕЛЯ', 'ВРЕМЯ ДАТА', 'КООРДИНАТЫ']
    
    # Создаем Excel файл
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, sheet_name='Уведомления', index=False)
        
        # Форматирование
        workbook = writer.book
        worksheet = writer.sheets['Уведомления']
        
        # Формат заголовков
        header_format = workbook.add_format({
            'bg_color': '#FFE6E6',
            'bold': True,
            'text_wrap': True,
            'valign': 'vcenter',
            'align': 'center',
            'border': 1
        })
        
        # Применяем формат к заголовкам
        for col_num, value in enumerate(df.columns.values):
            worksheet.write(0, col_num, value, header_format)
        
        # Автоподбор ширины колонок
        for i, col in enumerate(df.columns):
            column_len = df[col].astype(str).map(len).max()
            column_len = max(column_len, len(col)) + 2
            worksheet.set_column(i, i, column_len)
    
    output.seek(0)
    
    # Отправляем файл
    network_name = "РОССЕТИ КУБАНЬ" if network == 'RK' else "РОССЕТИ ЮГ"
    filename = f"Уведомления_{network_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    
    await update.message.reply_document(
        document=output,
        filename=filename,
        caption=f"📊 Отчет по уведомлениям {network_name}"
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Exception while handling an update: {context.error}")
    """Проверка доступности пользователя для отправки сообщений"""
    if len(context.args) == 0:
        await update.message.reply_text("Использование: /checkuser <telegram_id>")
        return
    
    target_id = context.args[0]
    
    try:
        # Пытаемся получить информацию о чате
        chat = await context.bot.get_chat(chat_id=target_id)
        await update.message.reply_text(
            f"✅ Пользователь доступен\n"
            f"ID: {target_id}\n"
            f"Имя: {chat.first_name} {chat.last_name or ''}\n"
            f"Username: @{chat.username or 'нет'}"
        )
    except Exception as e:
        await update.message.reply_text(
            f"❌ Не могу отправить сообщения пользователю {target_id}\n"
            f"Ошибка: {str(e)}\n\n"
            f"Возможные причины:\n"
            f"• Пользователь не начал диалог с ботом\n"
            f"• Пользователь заблокировал бота\n"
            f"• Неверный ID"
        )

if __name__ == '__main__':
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("checkuser", check_user))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.LOCATION, handle_location))
    application.add_error_handler(error_handler)
    
    # Загружаем данные пользователей
    load_users_data()
    
    # Запускаем webhook
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=BOT_TOKEN,
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        drop_pending_updates=True
    )
