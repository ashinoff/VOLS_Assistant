import os
import logging
import csv
import io
import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import requests
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, KeyboardButton, InputFile
from telegram.ext import Application, CommandHandler, MessageHandler, CallbackQueryHandler, filters, ContextTypes
import pandas as pd
from io import BytesIO
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import asyncio
import aiohttp
import pytz

# Настройка логирования
logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

# Константы
BOT_TOKEN = os.environ.get('BOT_TOKEN')
WEBHOOK_URL = os.environ.get('WEBHOOK_URL')
PORT = int(os.environ.get('PORT', 5000))
ZONES_CSV_URL = os.environ.get('ZONES_CSV_URL')

# Email настройки
SMTP_SERVER = os.environ.get('SMTP_SERVER', 'smtp.yandex.ru')
SMTP_PORT = int(os.environ.get('SMTP_PORT', '587'))
SMTP_EMAIL = os.environ.get('SMTP_EMAIL')
SMTP_PASSWORD = os.environ.get('SMTP_PASSWORD')

# Московский часовой пояс
MOSCOW_TZ = pytz.timezone('Europe/Moscow')

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

# Последние сгенерированные отчеты
last_reports = {}

# Кэш документов
documents_cache = {}
documents_cache_time = {}

# Хранилище активности пользователей
user_activity = {}  # {user_id: {'last_activity': datetime, 'count': int}}

# Справочные документы - настройте в переменных окружения
REFERENCE_DOCS = {
    'План по выручке ВОЛС на ВЛ 24-26 годы': os.environ.get('DOC_PLAN_VYRUCHKA_URL'),
    'Регламент ВОЛС': os.environ.get('DOC_REGLAMENT_VOLS_URL'),
    'Форма акта инвентаризации': os.environ.get('DOC_AKT_INVENTARIZACII_URL'),
    'Форма гарантийного письма': os.environ.get('DOC_GARANTIJNOE_PISMO_URL'),
    'Форма претензионного письма': os.environ.get('DOC_PRETENZIONNOE_PISMO_URL'),
    'Отчет по контрагентам': os.environ.get('DOC_OTCHET_KONTRAGENTY_URL'),
}

# URL руководства пользователя
USER_GUIDE_URL = os.environ.get('USER_GUIDE_URL', 'https://docs.google.com/document/d/YOUR_GUIDE_ID')

def get_moscow_time():
    """Получить текущее время в Москве"""
    return datetime.now(MOSCOW_TZ)

async def download_document(url: str) -> Optional[BytesIO]:
    """Скачать документ по URL"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    content = await response.read()
                    return BytesIO(content)
    except Exception as e:
        logger.error(f"Ошибка загрузки документа: {e}")
    return None

async def get_cached_document(doc_name: str, doc_url: str) -> Optional[BytesIO]:
    """Получить документ из кэша или загрузить"""
    now = datetime.now()
    
    # Проверяем кэш
    if doc_name in documents_cache:
        cache_time = documents_cache_time.get(doc_name)
        if cache_time and (now - cache_time) < timedelta(hours=1):
            # Возвращаем копию из кэша
            cached_doc = documents_cache[doc_name]
            cached_doc.seek(0)
            return BytesIO(cached_doc.read())
    
    # Загружаем документ
    logger.info(f"Загружаем документ {doc_name} из {doc_url}")
    
    # Определяем тип документа по URL
    if 'docs.google.com/document' in doc_url and '/d/' in doc_url:
        doc_id = doc_url.split('/d/')[1].split('/')[0]
        download_url = f"https://docs.google.com/document/d/{doc_id}/export?format=pdf"
    elif 'docs.google.com/spreadsheets' in doc_url and '/d/' in doc_url:
        doc_id = doc_url.split('/d/')[1].split('/')[0]
        download_url = f"https://docs.google.com/spreadsheets/d/{doc_id}/export?format=xlsx"
    elif 'drive.google.com' in doc_url and '/file/d/' in doc_url:
        file_id = doc_url.split('/file/d/')[1].split('/')[0]
        download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    else:
        download_url = doc_url
    
    document = await download_document(download_url)
    
    if document:
        # Сохраняем в кэш
        document.seek(0)
        documents_cache[doc_name] = BytesIO(document.read())
        documents_cache_time[doc_name] = now
        document.seek(0)
        
    return document

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
                    'responsible': row.get('Ответственный', '').strip(),
                    'email': row.get('Email', '').strip()  # Добавляем email
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

def update_user_activity(user_id: str):
    """Обновить активность пользователя"""
    if user_id not in user_activity:
        user_activity[user_id] = {'last_activity': get_moscow_time(), 'count': 0}
    user_activity[user_id]['last_activity'] = get_moscow_time()

def get_main_keyboard(permissions: Dict) -> ReplyKeyboardMarkup:
    """Получить главную клавиатуру в зависимости от прав"""
    keyboard = []
    
    visibility = permissions.get('visibility')
    branch = permissions.get('branch')
    res = permissions.get('res')
    
    # РОССЕТИ кнопки - исправленная логика видимости
    if visibility == 'All':
        keyboard.append(['🏢 РОССЕТИ КУБАНЬ'])
        keyboard.append(['🏢 РОССЕТИ ЮГ'])
    elif visibility == 'RK':
        keyboard.append(['🏢 РОССЕТИ КУБАНЬ'])
    elif visibility == 'UG':
        keyboard.append(['🏢 РОССЕТИ ЮГ'])
    
    # Телефоны контрагентов
    keyboard.append(['📞 ТЕЛЕФОНЫ КОНТРАГЕНТОВ'])
    
    # Отчеты - показываем только если есть права
    if res == 'All' and visibility in ['All', 'RK', 'UG']:
        keyboard.append(['📊 ОТЧЕТЫ'])
    
    # Справка
    keyboard.append(['ℹ️ СПРАВКА'])
    
    # Персональные настройки
    keyboard.append(['⚙️ МОИ НАСТРОЙКИ'])
    
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
        keyboard.append(['📈 Активность РОССЕТИ КУБАНЬ'])
        keyboard.append(['📈 Активность РОССЕТИ ЮГ'])
    elif visibility == 'RK':
        keyboard.append(['📊 Уведомления РОССЕТИ КУБАНЬ'])
        keyboard.append(['📈 Активность РОССЕТИ КУБАНЬ'])
    elif visibility == 'UG':
        keyboard.append(['📊 Уведомления РОССЕТИ ЮГ'])
        keyboard.append(['📈 Активность РОССЕТИ ЮГ'])
    
    keyboard.append(['⬅️ Назад'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_settings_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура персональных настроек"""
    keyboard = [
        ['📖 Руководство пользователя'],
        ['ℹ️ Моя информация'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_reference_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура справки с документами"""
    keyboard = []
    
    # Добавляем только те документы, для которых есть ссылки
    # Размещаем по одному в строке из-за длинных названий
    for doc_name, doc_url in REFERENCE_DOCS.items():
        if doc_url:
            # Сокращаем название для кнопки если оно слишком длинное
            button_text = doc_name
            if len(doc_name) > 30:
                # Сокращенные версии для длинных названий
                if 'План по выручке' in doc_name:
                    button_text = '📊 План выручки ВОЛС 24-26'
                elif 'Форма акта инвентаризации' in doc_name:
                    button_text = '📄 Акт инвентаризации'
                elif 'Форма гарантийного письма' in doc_name:
                    button_text = '📄 Гарантийное письмо'
                elif 'Форма претензионного письма' in doc_name:
                    button_text = '📄 Претензионное письмо'
                else:
                    button_text = f'📄 {doc_name[:27]}...'
            else:
                button_text = f'📄 {doc_name}'
            
            keyboard.append([button_text])
    
    keyboard.append(['⬅️ Назад'])
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_document_action_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с документом"""
    keyboard = [
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_after_search_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура после результатов поиска"""
    keyboard = [
        ['🔍 Новый поиск'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

def get_report_action_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с отчетом"""
    keyboard = [
        ['📧 Отправить отчет на почту'],
        ['⬅️ Назад']
    ]
    return ReplyKeyboardMarkup(keyboard, resize_keyboard=True)

async def send_email(to_email: str, subject: str, body: str, attachment_data: BytesIO = None, attachment_name: str = None):
    """Отправка email через SMTP с защитой от спам-фильтров"""
    if not SMTP_EMAIL or not SMTP_PASSWORD:
        logger.error("Email настройки не заданы")
        return False
    
    try:
        # Создаем сообщение
        msg = MIMEMultipart('alternative')
        msg['From'] = f"ВОЛС Ассистент <{SMTP_EMAIL}>"
        msg['To'] = to_email
        msg['Subject'] = subject
        msg['Reply-To'] = SMTP_EMAIL
        
        # Добавляем заголовки для предотвращения блокировки
        import uuid
        from email.utils import formatdate
        msg['Message-ID'] = f"<{uuid.uuid4()}@{SMTP_EMAIL.split('@')[1]}>"
        msg['Date'] = formatdate(localtime=True)
        msg['X-Mailer'] = 'VOLS Assistant Bot v1.0'
        msg['X-Priority'] = '3'  # Нормальный приоритет
        msg['Importance'] = 'Normal'
        
        # Создаем HTML версию письма
        html_body = f"""
        <html>
        <head>
            <meta charset="utf-8">
        </head>
        <body style="font-family: Arial, sans-serif; line-height: 1.6; color: #333;">
            <div style="max-width: 600px; margin: 0 auto; padding: 20px;">
                <div style="background-color: #f8f9fa; padding: 20px; border-radius: 10px;">
                    <h2 style="color: #2c3e50; margin-bottom: 20px;">ВОЛС Ассистент</h2>
                    <div style="white-space: pre-wrap;">{body.replace('\n', '<br>')}</div>
                </div>
                <div style="margin-top: 20px; padding-top: 20px; border-top: 1px solid #ddd; font-size: 12px; color: #666;">
                    <p>Это автоматическое сообщение от системы ВОЛС Ассистент.</p>
                    <p>Пожалуйста, не отвечайте на это письмо.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        # Добавляем текстовую и HTML версии
        msg.attach(MIMEText(body, 'plain', 'utf-8'))
        msg.attach(MIMEText(html_body, 'html', 'utf-8'))
        
        # Добавляем вложение если есть
        if attachment_data and attachment_name:
            attachment_data.seek(0)
            
            # Определяем MIME тип по расширению файла
            if attachment_name.endswith('.xlsx'):
                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                mime_subtype = 'xlsx'
            elif attachment_name.endswith('.xls'):
                mime_type = 'application/vnd.ms-excel'
                mime_subtype = 'xls'
            elif attachment_name.endswith('.pdf'):
                mime_type = 'application/pdf'
                mime_subtype = 'pdf'
            elif attachment_name.endswith('.doc'):
                mime_type = 'application/msword'
                mime_subtype = 'doc'
            elif attachment_name.endswith('.docx'):
                mime_type = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'
                mime_subtype = 'docx'
            else:
                mime_type = 'application/octet-stream'
                mime_subtype = 'octet-stream'
            
            part = MIMEBase('application', mime_subtype)
            part.set_payload(attachment_data.read())
            encoders.encode_base64(part)
            part.add_header('Content-Type', mime_type)
            part.add_header('Content-Disposition', f'attachment; filename="{attachment_name}"')
            msg.attach(part)
        
        # Добавляем небольшую задержку для предотвращения блокировки
        await asyncio.sleep(0.5)
        
        # Отправляем (разная логика для разных портов)
        if SMTP_PORT == 465:
            # SSL соединение (Mail.ru)
            import ssl
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(SMTP_SERVER, SMTP_PORT, context=context) as server:
                server.login(SMTP_EMAIL, SMTP_PASSWORD)
                server.send_message(msg)
        else:
            # TLS соединение (Яндекс, Gmail)
            with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_EMAIL, SMTP_PASSWORD)
                server.send_message(msg)
        
        logger.info(f"Email успешно отправлен на {to_email}")
        return True
        
    except Exception as e:
        logger.error(f"Ошибка отправки email на {to_email}: {e}")
        return False

# ========== ОБРАБОТЧИКИ ==========

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик команды /start"""
    user_id = str(update.effective_user.id)
    permissions = get_user_permissions(user_id)
    
    # Логируем для отладки
    logger.info(f"Пользователь {user_id} ({update.effective_user.first_name}) запустил бота")
    
    if not permissions['visibility']:
        await update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=caption
        )
        
        # Сохраняем последний отчет
        output.seek(0)
        last_reports[user_id] = {
            'data': BytesIO(output.read()),
            'filename': filename,
            'type': f"Полный реестр активности {network_name}",
            'datetime': moscow_time.strftime('%d.%m.%Y %H:%M')
        }
        
        # Устанавливаем состояние для действий с отчетом
        user_states[user_id]['state'] = 'report_actions'
        
        # Показываем кнопки действий
        await update.message.reply_text(
            "Выберите действие:",
            reply_markup=get_report_action_keyboard()
        )eply_text(
            f"❌ У вас нет доступа к боту.\n"
            f"Ваш ID: {user_id}\n"
            f"Обратитесь к администратору для получения прав."
        )
        return
    
    user_states[user_id] = {'state': 'main'}
    
    await update.message.reply_text(
        f"👋 Добро пожаловать, {permissions['name']}!",
        reply_markup=get_main_keyboard(permissions)
    )

async def send_notification(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Отправить уведомление ответственным лицам"""
    user_id = str(update.effective_user.id)
    user_data = user_states.get(user_id, {})
    
    # Получаем данные отправителя
    sender_info = get_user_permissions(user_id)
    
    # Получаем данные уведомления
    tp_data = user_data.get('tp_data', {})
    selected_tp = user_data.get('selected_tp')
    selected_vl = user_data.get('selected_vl')
    location = user_data.get('location', {})
    photo_id = user_data.get('photo_id')
    comment = user_data.get('comment', '')
    
    # Получаем данные из справочника (колонки A и B)
    branch_from_reference = tp_data.get('Филиал', '').strip()  # Колонка A
    res_from_reference = tp_data.get('РЭС', '').strip()  # Колонка B
    
    branch = user_data.get('branch')
    network = user_data.get('network')
    
    # Показываем анимированное сообщение отправки
    sending_messages = [
        "📨 Подготовка уведомления...",
        "🔍 Поиск ответственных лиц...",
        "📤 Отправка уведомлений...",
        "📧 Отправка email...",
        "✅ Почти готово..."
    ]
    
    loading_msg = await update.message.reply_text(sending_messages[0])
    
    for msg_text in sending_messages[1:]:
        await asyncio.sleep(0.5)
        try:
            await loading_msg.edit_text(msg_text)
        except Exception:
            pass
    
    # Ищем всех ответственных в базе
    responsible_users = []
    
    logger.info(f"Ищем ответственных для:")
    logger.info(f"  Филиал из справочника: '{branch_from_reference}'")
    logger.info(f"  РЭС из справочника: '{res_from_reference}'")
    
    # Проходим по всем пользователям и проверяем колонку "Ответственный"
    for uid, udata in users_cache.items():
        responsible_for = udata.get('responsible', '').strip()
        
        if not responsible_for:
            continue
            
        # Проверяем совпадение с филиалом или РЭС из справочника
        if responsible_for == branch_from_reference or responsible_for == res_from_reference:
            responsible_users.append({
                'id': uid,
                'name': udata.get('name', 'Неизвестный'),
                'email': udata.get('email', ''),
                'responsible_for': responsible_for
            })
            logger.info(f"Найден ответственный: {udata.get('name')} (ID: {uid}) - отвечает за '{responsible_for}'")
    
    # Формируем текст уведомления с московским временем
    moscow_time = get_moscow_time()
    notification_text = f"""🚨 НОВОЕ УВЕДОМЛЕНИЕ О БЕЗДОГОВОРНОМ ВОЛС

📍 Филиал: {branch}
📍 РЭС: {res_from_reference}
📍 ТП: {selected_tp}
⚡ ВЛ: {selected_vl}

👤 Отправитель: {sender_info['name']}
🕐 Время: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"""

    if location:
        lat = location.get('latitude')
        lon = location.get('longitude')
        notification_text += f"\n📍 Координаты: {lat:.6f}, {lon:.6f}"
        notification_text += f"\n🗺 [Открыть на карте](https://maps.google.com/?q={lat},{lon})"
    
    if comment:
        notification_text += f"\n\n💬 Комментарий: {comment}"
    
    # Формируем список получателей для записи в хранилище
    recipients_info = ", ".join([f"{u['name']} ({u['id']})" for u in responsible_users]) if responsible_users else "Не найдены"
    
    # Сохраняем уведомление в хранилище
    notification_data = {
        'branch': branch,
        'res': res_from_reference,
        'tp': selected_tp,
        'vl': selected_vl,
        'sender_name': sender_info['name'],
        'sender_id': user_id,
        'recipient_name': recipients_info,
        'recipient_id': ", ".join([u['id'] for u in responsible_users]) if responsible_users else 'Не найдены',
        'datetime': moscow_time.strftime('%d.%m.%Y %H:%M'),
        'coordinates': f"{location.get('latitude', 0):.6f}, {location.get('longitude', 0):.6f}" if location else 'Не указаны',
        'comment': comment,
        'has_photo': bool(photo_id)
    }
    
    notifications_storage[network].append(notification_data)
    
    # Обновляем активность пользователя
    if user_id not in user_activity:
        user_activity[user_id] = {'last_activity': get_moscow_time(), 'count': 0}
    user_activity[user_id]['count'] += 1
    user_activity[user_id]['last_activity'] = get_moscow_time()
    
    # Отправляем уведомления всем найденным ответственным
    success_count = 0
    email_success_count = 0
    failed_users = []
    
    for responsible in responsible_users:
        try:
            # Отправляем текст
            await context.bot.send_message(
                chat_id=responsible['id'],
                text=notification_text,
                parse_mode='Markdown'
            )
            
            # Отправляем локацию
            if location:
                await context.bot.send_location(
                    chat_id=responsible['id'],
                    latitude=location.get('latitude'),
                    longitude=location.get('longitude')
                )
            
            # Отправляем фото
            if photo_id:
                await context.bot.send_photo(
                    chat_id=responsible['id'],
                    photo=photo_id,
                    caption=f"Фото с {selected_tp}"
                )
            
            success_count += 1
            
            # Отправляем email если есть адрес
            if responsible['email']:
                email_subject = f"ВОЛС: Уведомление от {sender_info['name']}"
                email_body = f"""Добрый день, {responsible['name']}!

Получено новое уведомление о бездоговорном ВОЛС.

Филиал: {branch}
РЭС: {res_from_reference}
ТП: {selected_tp}
ВЛ: {selected_vl}

Отправитель: {sender_info['name']}
Время: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"""

                if location:
                    lat = location.get('latitude')
                    lon = location.get('longitude')
                    email_body += f"\n\nКоординаты: {lat:.6f}, {lon:.6f}"
                    email_body += f"\nСсылка на карту: https://maps.google.com/?q={lat},{lon}"
                
                if comment:
                    email_body += f"\n\nКомментарий: {comment}"
                    
                if photo_id:
                    email_body += f"\n\nК уведомлению приложено фото (доступно в Telegram)"
                
                email_body += f"""

Для просмотра деталей и фотографий откройте Telegram.

С уважением,
Бот ВОЛС Ассистент"""
                
                # Исправлено: отправляем email асинхронно
                email_sent = await send_email(responsible['email'], email_subject, email_body)
                if email_sent:
                    email_success_count += 1
                    logger.info(f"Email успешно отправлен для {responsible['name']} на {responsible['email']}")
                else:
                    logger.error(f"Не удалось отправить email для {responsible['name']} на {responsible['email']}")
                
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления пользователю {responsible['name']} ({responsible['id']}): {e}")
            failed_users.append(f"{responsible['name']} ({responsible['id']}): {str(e)}")
    
    # Удаляем анимированное сообщение
    await loading_msg.delete()
    
    # Формируем результат
    if responsible_users:
        if success_count == len(responsible_users):
            result_text = f"""✅ Уведомления успешно отправлены!

📨 Получатели ({success_count}):"""
            for responsible in responsible_users:
                result_text += f"\n• {responsible['name']} (отвечает за {responsible['responsible_for']})"
            
            if email_success_count > 0:
                result_text += f"\n\n📧 Email отправлено: {email_success_count} из {len([r for r in responsible_users if r['email']])}"
        else:
            result_text = f"""⚠️ Уведомления отправлены частично

✅ Успешно: {success_count} из {len(responsible_users)}
📧 Email отправлено: {email_success_count}

❌ Ошибки:"""
            for failed in failed_users:
                result_text += f"\n• {failed}"
    else:
        result_text = f"""❌ Ответственные не найдены

Для данной ТП не назначены ответственные лица.
Уведомление сохранено в системе и будет доступно в отчетах.

Отладочная информация:
- Филиал из справочника: "{branch_from_reference}"
- РЭС из справочника: "{res_from_reference}"
- Всего пользователей в базе: {len(users_cache)}"""
    
    # Очищаем состояние
    user_states[user_id] = {'state': f'branch_{branch}', 'branch': branch, 'network': network}
    
    await update.message.reply_text(
        result_text,
        reply_markup=get_branch_menu_keyboard()
    )

async def generate_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета"""
    try:
        user_id = str(update.effective_user.id)
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
        
        # Показываем анимированное сообщение
        report_messages = [
            "📊 Собираю данные...",
            "📈 Формирую статистику...",
            "📝 Создаю таблицы...",
            "🎨 Оформляю отчет...",
            "💾 Сохраняю файл..."
        ]
        
        loading_msg = await update.message.reply_text(report_messages[0])
        
        for msg_text in report_messages[1:]:
            await asyncio.sleep(0.5)
            try:
                await loading_msg.edit_text(msg_text)
            except Exception:
                pass
        
        # Создаем DataFrame
        df = pd.DataFrame(notifications)
        
        # Проверяем наличие необходимых колонок
        required_columns = ['branch', 'res', 'sender_name', 'sender_id', 'recipient_name', 'recipient_id', 'datetime', 'coordinates']
        existing_columns = [col for col in required_columns if col in df.columns]
        
        if not existing_columns:
            await loading_msg.delete()
            await update.message.reply_text("📊 Недостаточно данных для формирования отчета")
            return
            
        df = df[existing_columns]
        
        # Переименовываем колонки
        column_mapping = {
            'branch': 'ФИЛИАЛ',
            'res': 'РЭС', 
            'sender_name': 'ФИО ОТПРАВИТЕЛЯ',
            'sender_id': 'ID ОТПРАВИТЕЛЯ',
            'recipient_name': 'ФИО ПОЛУЧАТЕЛЯ',
            'recipient_id': 'ID ПОЛУЧАТЕЛЯ',
            'datetime': 'ВРЕМЯ ДАТА',
            'coordinates': 'КООРДИНАТЫ'
        }
        df.rename(columns=column_mapping, inplace=True)
        
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
        
        # ВАЖНО: Перемещаем указатель в начало после записи
        output.seek(0)
        
        # Удаляем анимированное сообщение
        await loading_msg.delete()
        
        # Отправляем файл в чат
        network_name = "РОССЕТИ КУБАНЬ" if network == 'RK' else "РОССЕТИ ЮГ"
        moscow_time = get_moscow_time()
        filename = f"Уведомления_{network_name}_{moscow_time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        # Создаем InputFile для правильной отправки
        await update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=f"📊 Отчет по уведомлениям {network_name}\n🕐 Сформировано: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"
        )
        
        # Сохраняем последний отчет пользователя
        output.seek(0)
        last_reports[user_id] = {
            'data': BytesIO(output.read()),
            'filename': filename,
            'type': f"Уведомления {network_name}",
            'datetime': moscow_time.strftime('%d.%m.%Y %H:%M')
        }
        
        # Устанавливаем состояние для действий с отчетом
        user_states[user_id]['state'] = 'report_actions'
        
        # Показываем кнопки действий с отчетом
        await update.message.reply_text(
            "Выберите действие:",
            reply_markup=get_report_action_keyboard()
        )
                
    except Exception as e:
        logger.error(f"Ошибка генерации отчета: {e}")
        if 'loading_msg' in locals():
            await loading_msg.delete()
        await update.message.reply_text(f"❌ Ошибка генерации отчета: {str(e)}")

async def generate_activity_report(update: Update, context: ContextTypes.DEFAULT_TYPE, network: str, permissions: Dict):
    """Генерация отчета по активности пользователей с полным реестром"""
    try:
        user_id = str(update.effective_user.id)
        
        # Показываем анимированное сообщение
        loading_msg = await update.message.reply_text("📈 Формирую полный отчет активности...")
        
        # Собираем данные всех пользователей из CSV
        all_users_data = []
        
        for uid, user_info in users_cache.items():
            # Фильтруем по сети
            if user_info.get('visibility') not in ['All', network]:
                continue
            
            # Фильтруем по филиалу если нужно
            if permissions['branch'] != 'All' and user_info.get('branch') != 'All' and user_info.get('branch') != permissions['branch']:
                continue
            
            # Получаем данные активности
            activity = user_activity.get(uid, None)
            
            # Определяем статус активности
            if activity:
                is_active = True
                notification_count = activity['count']
                last_activity = activity['last_activity'].strftime('%d.%m.%Y %H:%M')
            else:
                is_active = False
                notification_count = 0
                last_activity = 'Нет активности'
            
            all_users_data.append({
                'ФИО': user_info.get('name', 'Не указано'),
                'Telegram ID': uid,
                'Филиал': user_info.get('branch', '-'),
                'РЭС': user_info.get('res', '-'),
                'Ответственный': user_info.get('responsible', '-'),
                'Email': user_info.get('email', '-'),
                'Статус': 'Активный' if is_active else 'Неактивный',
                'Количество уведомлений': notification_count,
                'Последняя активность': last_activity
            })
        
        if not all_users_data:
            await loading_msg.delete()
            await update.message.reply_text("📈 Нет данных для отчета")
            return
        
        # Создаем DataFrame и сортируем
        df = pd.DataFrame(all_users_data)
        df = df.sort_values(['Статус', 'Количество уведомлений'], ascending=[True, False])
        
        # Создаем Excel файл с форматированием
        output = BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, sheet_name='Активность пользователей', index=False)
            
            # Получаем объекты workbook и worksheet
            workbook = writer.book
            worksheet = writer.sheets['Активность пользователей']
            
            # Формат заголовков
            header_format = workbook.add_format({
                'bg_color': '#4B4B4B',
                'font_color': 'white',
                'bold': True,
                'text_wrap': True,
                'valign': 'vcenter',
                'align': 'center',
                'border': 1
            })
            
            # Форматы для активных и неактивных пользователей
            active_format = workbook.add_format({
                'bg_color': '#E8F5E9',  # Нежно зеленый
                'border': 1
            })
            
            inactive_format = workbook.add_format({
                'bg_color': '#FFEBEE',  # Нежно красный
                'border': 1
            })
            
            # Применяем формат к заголовкам
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
            
            # Применяем цветовую индикацию к строкам
            for row_num, (index, row) in enumerate(df.iterrows(), start=1):
                cell_format = active_format if row['Статус'] == 'Активный' else inactive_format
                for col_num, value in enumerate(row):
                    worksheet.write(row_num, col_num, value, cell_format)
            
            # Автоподбор ширины колонок
            for i, col in enumerate(df.columns):
                column_len = df[col].astype(str).map(len).max()
                column_len = max(column_len, len(col)) + 2
                worksheet.set_column(i, i, min(column_len, 40))
            
            # Добавляем автофильтр
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)
        
        output.seek(0)
        
        # Удаляем анимированное сообщение
        await loading_msg.delete()
        
        # Подсчитываем статистику
        active_count = len(df[df['Статус'] == 'Активный'])
        inactive_count = len(df[df['Статус'] == 'Неактивный'])
        
        # Отправляем файл
        network_name = "РОССЕТИ КУБАНЬ" if network == 'RK' else "РОССЕТИ ЮГ"
        moscow_time = get_moscow_time()
        filename = f"Полный_реестр_активности_{network_name}_{moscow_time.strftime('%Y%m%d_%H%M%S')}.xlsx"
        
        caption = f"""📈 Полный отчет активности {network_name}

👥 Всего пользователей: {len(df)}
✅ Активных: {active_count} (нежно-зеленый)
❌ Неактивных: {inactive_count} (нежно-красный)

📊 Отчет содержит полный реестр пользователей с цветовой индикацией активности
🕐 Сформировано: {moscow_time.strftime('%d.%m.%Y %H:%M')} МСК"""
        
        await update.message.reply_document(
            document=InputFile(output, filename=filename),
            caption=caption,
            reply_markup=get_reports_keyboard(permissions)
        )
        
    except Exception as e:
        logger.error(f"Ошибка генерации отчета активности: {e}")
        if 'loading_msg' in locals():
            await loading_msg.delete()
        await update.message.reply_text(f"❌ Ошибка генерации отчета: {str(e)}")

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик текстовых сообщений"""
    user_id = str(update.effective_user.id)
    text = update.message.text
    permissions = get_user_permissions(user_id)
    
    if not permissions['visibility']:
        await update.message.reply_text("❌ У вас нет доступа к боту.")
        return
    
    # Обновляем активность пользователя
    update_user_activity(user_id)
    
    state = user_states.get(user_id, {}).get('state', 'main')
    
    # Обработка кнопки Назад
    if text == '⬅️ Назад':
        if state in ['rosseti_kuban', 'rosseti_yug', 'reports', 'reference', 'phones', 'settings']:
            user_states[user_id] = {'state': 'main'}
            await update.message.reply_text("Главное меню", reply_markup=get_main_keyboard(permissions))
        elif state == 'document_actions':
            user_states[user_id]['state'] = 'reference'
            await update.message.reply_text("Выберите документ", reply_markup=get_reference_keyboard())
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
        
        elif text == '⚙️ МОИ НАСТРОЙКИ':
            user_states[user_id] = {'state': 'settings'}
            await update.message.reply_text(
                "⚙️ Персональные настройки",
                reply_markup=get_settings_keyboard()
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
                "🔍 Введите наименование ТП для поиска:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        elif text == '📨 Отправить уведомление':
            user_states[user_id]['state'] = 'send_notification'
            user_states[user_id]['action'] = 'notification_tp'
            keyboard = [['⬅️ Назад']]
            await update.message.reply_text(
                "📨 Введите наименование ТП для уведомления:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        elif text == 'ℹ️ Справка':
            user_states[user_id]['state'] = 'reference'
            await update.message.reply_text(
                "Выберите документ",
                reply_markup=get_reference_keyboard()
            )
    
    # Поиск ТП
    elif state == 'search_tp':
        if text == '🔍 Новый поиск':
            # Остаемся в том же состоянии
            keyboard = [['⬅️ Назад']]
            await update.message.reply_text(
                "🔍 Введите наименование ТП для поиска:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        elif user_states[user_id].get('action') == 'search':
            branch = user_states[user_id].get('branch')
            network = user_states[user_id].get('network')
            
            logger.info(f"Поиск ТП для филиала: {branch}, сеть: {network}")
            
            # Показываем анимированное сообщение
            search_messages = [
                "🔍 Ищу информацию...",
                "📡 Подключаюсь к базе данных...",
                "⚡ Сканирую электросети...",
                "📊 Анализирую данные...",
                "🔄 Обрабатываю результаты..."
            ]
            
            # Отправляем первое сообщение
            loading_msg = await update.message.reply_text(search_messages[0])
            
            # Анимация поиска
            for i, msg_text in enumerate(search_messages[1:], 1):
                await asyncio.sleep(0.5)  # Задержка между сообщениями
                try:
                    await loading_msg.edit_text(msg_text)
                except Exception:
                    pass  # Игнорируем ошибки редактирования
            
            # Загружаем данные филиала
            env_key = get_env_key_for_branch(branch, network)
            csv_url = os.environ.get(env_key)
            
            logger.info(f"URL из переменной {env_key}: {csv_url}")
            
            if not csv_url:
                # Показываем все доступные переменные окружения для отладки
                available_vars = [key for key in os.environ.keys() if 'URL' in key and network in key]
                logger.error(f"Доступные переменные для {network}: {available_vars}")
                await loading_msg.delete()
                await update.message.reply_text(
                    f"❌ Данные для филиала {branch} не найдены\n"
                    f"Искали переменную: {env_key}\n"
                    f"Доступные: {', '.join(available_vars[:5])}"
                )
                return
            
            data = load_csv_from_url(csv_url)
            results = search_tp_in_data(text, data, 'Наименование ТП')
            
            # Удаляем анимированное сообщение
            await loading_msg.delete()
            
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
                    f"✅ Найдено {len(tp_list)} ТП. Выберите нужную:",
                    reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
                )
        
        # Выбор ТП из списка найденных
        elif user_states[user_id].get('action') == 'select_tp':
            results = user_states[user_id].get('search_results', [])
            filtered_results = [r for r in results if r['Наименование ТП'] == text]
            
            if filtered_results:
                await show_tp_results(update, filtered_results, text)
                # Возвращаем в состояние поиска
                user_states[user_id]['action'] = 'search'
        
    # Уведомление - поиск ТП
    elif state == 'send_notification' and user_states[user_id].get('action') == 'notification_tp':
        branch = user_states[user_id].get('branch')
        network = user_states[user_id].get('network')
        
        # Показываем анимированное сообщение
        notification_messages = [
            "🔍 Поиск в справочнике...",
            "📋 Проверяю базу данных...",
            "🌐 Загружаю информацию...",
            "✨ Почти готово..."
        ]
        
        loading_msg = await update.message.reply_text(notification_messages[0])
        
        for msg_text in notification_messages[1:]:
            await asyncio.sleep(0.4)
            try:
                await loading_msg.edit_text(msg_text)
            except Exception:
                pass
        
        # Загружаем справочник
        env_key = get_env_key_for_branch(branch, network, is_reference=True)
        csv_url = os.environ.get(env_key)
        
        if not csv_url:
            await loading_msg.delete()
            await update.message.reply_text(f"❌ Справочник для филиала {branch} не найден")
            return
        
        data = load_csv_from_url(csv_url)
        results = search_tp_in_data(text, data, 'Наименование ТП')
        
        await loading_msg.delete()
        
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
            f"✅ Найдено {len(tp_list)} ТП. Выберите нужную:",
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
    
    # Обработка действий при отправке уведомления с фото
    elif state == 'send_notification':
        action = user_states[user_id].get('action')
        
        # Пропуск фото и переход к комментарию
        if action == 'request_photo' and text == '⏭ Пропустить и добавить комментарий':
            user_states[user_id]['action'] = 'add_comment'
            keyboard = [
                ['📤 Отправить без комментария'],
                ['⬅️ Назад']
            ]
            await update.message.reply_text(
                "💬 Введите комментарий к уведомлению:",
                reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
            )
        
        # Отправка без фото и комментария
        elif action == 'request_photo' and text == '📤 Отправить без фото и комментария':
            await send_notification(update, context)
        
        # Отправка без комментария (с фото или без)
        elif action == 'add_comment' and text == '📤 Отправить без комментария':
            await send_notification(update, context)
        
        # Добавление комментария
        elif action == 'add_comment' and text not in ['⬅️ Назад', '📤 Отправить без комментария']:
            user_states[user_id]['comment'] = text
            await send_notification(update, context)
    
    # Персональные настройки
    elif state == 'settings':
        if text == '📖 Руководство пользователя':
            # Показываем сообщение о загрузке
            loading_msg = await update.message.reply_text("⏳ Загружаю руководство пользователя...")
            
            try:
                if USER_GUIDE_URL:
                    # Получаем документ
                    document = await get_cached_document('Руководство пользователя', USER_GUIDE_URL)
                    
                    if document:
                        # Отправляем документ
                        await update.message.reply_document(
                            document=InputFile(document, filename="Руководство_пользователя.pdf"),
                            caption="📖 Руководство пользователя"
                        )
                        await loading_msg.delete()
                    else:
                        await loading_msg.delete()
                        await update.message.reply_text(
                            f"❌ Не удалось загрузить руководство.\n\n"
                            f"Вы можете открыть его по ссылке:\n{USER_GUIDE_URL}"
                        )
                else:
                    await loading_msg.delete()
                    await update.message.reply_text("❌ Ссылка на руководство не настроена в системе")
            except Exception as e:
                logger.error(f"Ошибка загрузки руководства: {e}")
                await loading_msg.delete()
                await update.message.reply_text("❌ Ошибка загрузки руководства")
        
        elif text == 'ℹ️ Моя информация':
            user_data = users_cache.get(user_id, {})
            
            info_text = f"""ℹ️ Ваша информация:

👤 ФИО: {user_data.get('name', 'Не указано')}
🆔 Telegram ID: {user_id}
📧 Email: {user_data.get('email', 'Не указан')}

🔐 Права доступа:
• Видимость: {user_data.get('visibility', '-')}
• Филиал: {user_data.get('branch', '-')}
• РЭС: {user_data.get('res', '-')}
• Ответственность: {user_data.get('responsible', 'Не назначена')}"""
            
            await update.message.reply_text(info_text)
    
    # Отчеты
    elif state == 'reports':
        if text == '📊 Уведомления РОССЕТИ КУБАНЬ':
            await generate_report(update, context, 'RK', permissions)
        elif text == '📊 Уведомления РОССЕТИ ЮГ':
            await generate_report(update, context, 'UG', permissions)
        elif text == '📈 Активность РОССЕТИ КУБАНЬ':
            await generate_activity_report(update, context, 'RK', permissions)
        elif text == '📈 Активность РОССЕТИ ЮГ':
            await generate_activity_report(update, context, 'UG', permissions)
    
    # Справка
    elif state == 'reference':
        if text.startswith('📄 ') or text.startswith('📊 '):
            # Убираем эмодзи и ищем соответствующий документ
            button_text = text[2:].strip()
            
            # Маппинг сокращенных названий к полным
            doc_mapping = {
                'План выручки ВОЛС 24-26': 'План по выручке ВОЛС на ВЛ 24-26 годы',
                'Акт инвентаризации': 'Форма акта инвентаризации',
                'Гарантийное письмо': 'Форма гарантийного письма',
                'Претензионное письмо': 'Форма претензионного письма',
                'Регламент ВОЛС': 'Регламент ВОЛС',
                'Отчет по контрагентам': 'Отчет по контрагентам'
            }
            
            # Ищем полное название
            doc_name = doc_mapping.get(button_text, button_text)
            
            # Если не нашли в маппинге, ищем прямое совпадение
            if doc_name not in REFERENCE_DOCS:
                # Ищем частичное совпадение
                for full_name in REFERENCE_DOCS.keys():
                    if button_text in full_name or full_name in button_text:
                        doc_name = full_name
                        break
            
            doc_url = REFERENCE_DOCS.get(doc_name)
            
            if doc_url:
                # Показываем сообщение о загрузке
                loading_msg = await update.message.reply_text("⏳ Загружаю документ...")
                
                try:
                    # Получаем документ из кэша или загружаем
                    document = await get_cached_document(doc_name, doc_url)
                    
                    if document:
                        # Определяем расширение файла
                        if 'spreadsheet' in doc_url or 'xlsx' in doc_url:
                            extension = 'xlsx'
                        elif 'document' in doc_url or 'pdf' in doc_url:
                            extension = 'pdf'
                        else:
                            extension = 'pdf'  # по умолчанию
                        
                        filename = f"{doc_name}.{extension}"
                        
                        # Отправляем документ
                        await update.message.reply_document(
                            document=InputFile(document, filename=filename),
                            caption=f"📄 {doc_name}"
                        )
                        
                        # Удаляем сообщение о загрузке
                        await loading_msg.delete()
                        
                        # Сохраняем информацию о документе в состоянии
                        user_states[user_id]['state'] = 'document_actions'
                        user_states[user_id]['current_document'] = {
                            'name': doc_name,
                            'url': doc_url,
                            'filename': filename
                        }
                        
                        # Показываем кнопки действий
                        await update.message.reply_text(
                            "Документ загружен",
                            reply_markup=get_document_action_keyboard()
                        )
                    else:
                        await loading_msg.delete()
                        await update.message.reply_text(
                            f"❌ Не удалось загрузить документ.\n\n"
                            f"Вы можете открыть его по ссылке:\n{doc_url}"
                        )
                        
                except Exception as e:
                    logger.error(f"Ошибка обработки документа {doc_name}: {e}")
                    await loading_msg.delete()
                    await update.message.reply_text(
                        f"❌ Ошибка загрузки документа.\n\n"
                        f"Вы можете открыть его по ссылке:\n{doc_url}"
                    )
            else:
                await update.message.reply_text(f"❌ Документ не найден")

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
    
    # Показываем клавиатуру с кнопками после поиска
    await update.message.reply_text(
        "Выберите действие:",
        reply_markup=get_after_search_keyboard()
    )

async def handle_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка геолокации"""
    user_id = str(update.effective_user.id)
    state = user_states.get(user_id, {}).get('state')
    
    if state == 'send_notification' and user_states[user_id].get('action') == 'send_location':
        location = update.message.location
        tp_data = user_states[user_id].get('tp_data', {})
        selected_tp = user_states[user_id].get('selected_tp')
        selected_vl = user_states[user_id].get('selected_vl')
        
        # Сохраняем локацию
        user_states[user_id]['location'] = {
            'latitude': location.latitude,
            'longitude': location.longitude
        }
        
        # Переходим к запросу фото
        user_states[user_id]['action'] = 'request_photo'
        
        keyboard = [
            ['⏭ Пропустить и добавить комментарий'],
            ['📤 Отправить без фото и комментария'],
            ['⬅️ Назад']
        ]
        
        # Отправляем анимированную подсказку
        photo_tips = [
            "📸 Подготовьте камеру...",
            "📷 Сфотографируйте бездоговорной ВОЛС...",
            "💡 Совет: Снимите общий вид и детали"
        ]
        
        tip_msg = await update.message.reply_text(photo_tips[0])
        
        for tip in photo_tips[1:]:
            await asyncio.sleep(1.5)
            try:
                await tip_msg.edit_text(tip)
            except Exception:
                pass
        
        await asyncio.sleep(1.5)
        await tip_msg.delete()
        
        # Отправляем основное сообщение
        await update.message.reply_text(
            "📸 Сделайте фото бездоговорного ВОЛС\n\n"
            "Как отправить фото:\n"
            "📱 **Мобильный**: нажмите 📎 → Камера\n"
            "💻 **Компьютер**: нажмите 📎 → Фото\n\n"
            "Или выберите действие ниже:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True),
            parse_mode='Markdown'
        )

async def handle_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка фотографий"""
    user_id = str(update.effective_user.id)
    state = user_states.get(user_id, {}).get('state')
    
    if state == 'send_notification' and user_states[user_id].get('action') == 'request_photo':
        # Сохраняем фото
        photo = update.message.photo[-1]  # Берем фото в максимальном качестве
        file_id = photo.file_id
        
        user_states[user_id]['photo_id'] = file_id
        user_states[user_id]['action'] = 'add_comment'
        
        keyboard = [
            ['📤 Отправить без комментария'],
            ['⬅️ Назад']
        ]
        
        await update.message.reply_text(
            "✅ Фото получено!\n\n"
            "Теперь добавьте комментарий к уведомлению или отправьте без комментария:",
            reply_markup=ReplyKeyboardMarkup(keyboard, resize_keyboard=True)
        )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    logger.error(f"Exception while handling an update: {context.error}")
    
    # Попытаемся уведомить пользователя об ошибке
    try:
        if update and update.effective_message:
            await update.effective_message.reply_text(
                "⚠️ Произошла ошибка при обработке вашего запроса. Попробуйте еще раз."
            )
    except Exception:
        pass

async def check_user(update: Update, context: ContextTypes.DEFAULT_TYPE):
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

async def preload_documents():
    """Предзагрузка документов в кэш при старте"""
    logger.info("Начинаем предзагрузку документов...")
    
    for doc_name, doc_url in REFERENCE_DOCS.items():
        if doc_url:
            try:
                logger.info(f"Загружаем {doc_name}...")
                await get_cached_document(doc_name, doc_url)
                logger.info(f"✅ {doc_name} загружен в кэш")
            except Exception as e:
                logger.error(f"❌ Ошибка загрузки {doc_name}: {e}")
    
    logger.info("Предзагрузка документов завершена")

async def refresh_users_data():
    """Периодическое обновление данных пользователей"""
    while True:
        await asyncio.sleep(300)  # Обновляем каждые 5 минут
        logger.info("Обновляем данные пользователей...")
        try:
            load_users_data()
            logger.info("✅ Данные пользователей обновлены")
        except Exception as e:
            logger.error(f"❌ Ошибка обновления данных пользователей: {e}")

async def refresh_documents_cache():
    """Периодическое обновление кэша документов"""
    while True:
        await asyncio.sleep(3600)  # Ждем час
        logger.info("Обновляем кэш документов...")
        
        for doc_name in list(documents_cache.keys()):
            doc_url = REFERENCE_DOCS.get(doc_name)
            if doc_url:
                try:
                    # Очищаем старый кэш
                    del documents_cache[doc_name]
                    del documents_cache_time[doc_name]
                    
                    # Загружаем заново
                    await get_cached_document(doc_name, doc_url)
                    logger.info(f"✅ Обновлен кэш для {doc_name}")
                except Exception as e:
                    logger.error(f"❌ Ошибка обновления кэша {doc_name}: {e}")

if __name__ == '__main__':
    # Создаем приложение
    application = Application.builder().token(BOT_TOKEN).build()
    
    # Добавляем обработчики
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("checkuser", check_user))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))
    application.add_handler(MessageHandler(filters.LOCATION, handle_location))
    application.add_handler(MessageHandler(filters.PHOTO, handle_photo))
    application.add_error_handler(error_handler)
    
    # Загружаем данные пользователей
    load_users_data()
    
    # Создаем корутину для инициализации
    async def init_and_start():
        """Инициализация и запуск"""
        # Предзагружаем документы
        await preload_documents()
        
        # Запускаем фоновые задачи
        asyncio.create_task(refresh_documents_cache())
        asyncio.create_task(refresh_users_data())
    
    # Добавляем обработчик для инициализации при старте
    async def post_init(application: Application) -> None:
        """Вызывается после инициализации приложения"""
        await init_and_start()
    
    # Устанавливаем post_init callback
    application.post_init = post_init
    
    # Запускаем webhook
    application.run_webhook(
        listen="0.0.0.0",
        port=PORT,
        url_path=BOT_TOKEN,
        webhook_url=f"{WEBHOOK_URL}/{BOT_TOKEN}",
        drop_pending_updates=True
    )
