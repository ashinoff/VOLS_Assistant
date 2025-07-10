import os

TOKEN = os.environ.get("TOKEN")
SELF_URL = os.environ.get("SELF_URL", "")
PORT = int(os.environ.get("PORT", 10000))

ZONES_CSV_URL = os.environ.get("ZONES_CSV_URL", "")

# Пример для одного филиала. Остальные добавь по образцу.
TIMASHEVSKIE_ES_URL_RK = os.environ.get("TIMASHEVSKIE_ES_URL_RK", "")
YUGO_ZAPAD_URL_UG = os.environ.get("YUGO_ZAPAD_URL_UG", "")

TIMASHEVSKIE_ES_URL_RK_SP = os.environ.get("TIMASHEVSKIE_ES_URL_RK_SP", "")
YUGO_ZAPAD_URL_UG_SP = os.environ.get("YUGO_ZAPAD_URL_UG_SP", "")

HELP_FOLDER_URL = os.environ.get("HELP_FOLDER_URL", "")

NOTIFY_LOG_FILE_RK = os.environ.get("NOTIFY_LOG_FILE_RK", "")
NOTIFY_LOG_FILE_UG = os.environ.get("NOTIFY_LOG_FILE_UG", "")

# Для автопинга
PING_URL = os.environ.get("PING_URL", SELF_URL)
