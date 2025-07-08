TOKEN = "8066747417:AAG3nzy1wuK4uogGYwUrWZtZyUxKdx-7ufQ"  # Замените на токен из @BotFather
SELF_URL = "https://your-new-app.onrender.com"  # Замените на URL после деплоя
PORT = 5000

BRANCH_URLS = {
    "Россети ЮГ": {
        "Тимашевские ЭС": "YOUR_GOOGLE_DRIVE_URL_UG",  # Замените на URL CSV
    },
    "Россети Кубань": {
        "Тимашевские ЭС": "YOUR_GOOGLE_DRIVE_URL_RK",  # Замените на URL CSV
    },
}

NOTIFY_URLS = {
    "Россети ЮГ": {
        "Тимашевские ЭС": "YOUR_NOTIFY_URL_UG",  # Замените на URL справочника
    },
    "Россети Кубань": {
        "Тимашевские ЭС": "YOUR_NOTIFY_URL_RK",  # Замените на URL справочника
    },
}

NOTIFY_LOG_FILE_UG = "/app/notify_log_ug.csv"
NOTIFY_LOG_FILE_RK = "/app/notify_log_rk.csv"
