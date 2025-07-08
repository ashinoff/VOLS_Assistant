import os

TOKEN = os.getenv("TOKEN", "8066747417:AAG3nzy1wuK4uogGYwUrWZtZyUxKdx-7ufQ")
SELF_URL = os.getenv("SELF_URL", "https://vols-assistant.onrender.com")
PORT = int(os.getenv("PORT", 5000))

# Динамическая сборка словарей из переменных окружения
BRANCH_URLS = {
    "Россети ЮГ": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_UG", ""),
    },
    "Россети Кубань": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_RK", ""),
    },
}

NOTIFY_URLS = {
    "Россети ЮГ": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_UG_SP", ""),
    },
    "Россети Кубань": {
        "Тимашевские ЭС": os.getenv("TIMASHEV_ES_URL_RK_SP", ""),
    },
}

NOTIFY_LOG_FILE_UG = "/app/notify_log_ug.csv"
NOTIFY_LOG_FILE_RK = "/app/notify_log_rk.csv"
