import os

TOKEN = os.getenv("TOKEN", "8066747417:AAG3nzy1wuK4uogGYwUrWZtZyUxKdx-7ufQ")
SELF_URL = os.getenv("SELF_URL", "https://vols-assistant.onrender.com")
PORT = int(os.getenv("PORT", 5000))

BRANCH_URLS = {
    "Россети ЮГ": {
        "Тимашевские ЭС": os.getenv("BRANCH_URLS_UG", ""),
    },
    "Россети Кубань": {
        "Тимашевские ЭС": os.getenv("BRANCH_URLS_RK", ""),
    },
}

NOTIFY_URLS = {
    "Россети ЮГ": {
        "Тимашевские ЭС": os.getenv("NOTIFY_URLS_UG", ""),
    },
    "Россети Кубань": {
        "Тимашевские ЭС": os.getenv("NOTIFY_URLS_RK", ""),
    },
}

NOTIFY_LOG_FILE_UG = "/app/notify_log_ug.csv"
NOTIFY_LOG_FILE_RK = "/app/notify_log_rk.csv"
