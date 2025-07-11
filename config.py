import os
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("TOKEN")
SELF_URL = os.getenv("SELF_URL")         # твой render-сайт (https://...)
PORT = int(os.getenv("PORT", 8000))

ZONES_CSV_URL = os.getenv("ZONES_CSV_URL")  # ссылка на таблицу прав

# Для будущих доработок
HELP_FOLDER_URL = os.getenv("HELP_FOLDER_URL", "")
# ... и так далее, если надо
