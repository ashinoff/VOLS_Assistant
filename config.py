import os
from dotenv import load_dotenv

load_dotenv()

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN")
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "YOUR_GOOGLE_DRIVE_CSV_URL")
PING_URL = os.getenv("PING_URL", "")
