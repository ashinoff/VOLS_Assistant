import os
from dotenv import load_dotenv

load_dotenv()

# Bot configuration
TOKEN = os.getenv("TOKEN", "YOUR_TOKEN")
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", "YOUR_GOOGLE_DRIVE_CSV_URL")
SELF_URL = os.getenv("SELF_URL", "")
PORT = int(os.getenv("PORT", 8000))

# Reserved for future search logic
# TIMASHEV_ES_URL_RK = os.getenv("TIMASHEV_ES_URL_RK", "")
# TIMASHEV_ES_URL_RK_SP = os.getenv("TIMASHEV_ES_URL_RK_SP", "")
# TIMASHEV_ES_URL_UG = os.getenv("TIMASHEV_ES_URL_UG", "")
# TIMASHEV_ES_URL_UG_SP = os.getenv("TIMASHEV_ES_URL_UG_SP", "")
