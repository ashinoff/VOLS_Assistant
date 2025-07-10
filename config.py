import os
from dotenv import load_dotenv

load_dotenv()

# Bot configuration
TOKEN = os.getenv("TOKEN", "YOUR_TOKEN")
ZONES_CSV_URL = os.getenv("ZONES_CSV_URL", zones.py"YOUR_GOOGLE_DRIVE_CSV_URL")
SELF_URL = os.getenv("SELF_URL", "")
PORT = int(os.getenv("PORT", 8000))

# Reserved for TP search (Rosseti Yug)
YUGO_ZAPAD_URL_UG = os.getenv("YUGO_ZAPAD_URL_UG", "")
CENTRAL_URL_UG = os.getenv("CENTRAL_URL_UG", "")
ZAPAD_URL_UG = os.getenv("ZAPAD_URL_UG", "")
VOSTOCH_URL_UG = os.getenv("VOSTOCH_URL_UG", "")
YUZH_URL_UG = os.getenv("YUZH_URL_UG", "")
SEVERO_VOSTOCH_URL_UG = os.getenv("SEVERO_VOSTOCH_URL_UG", "")
YUGO_VOSTOCH_URL_UG = os.getenv("YUGO_VOSTOCH_URL_UG", "")
SEVER_URL_UG = os.getenv("SEVER_URL_UG", "")

# Reserved for TP search (Rosseti Kuban)
YUGO_ZAPAD_URL_RK = os.getenv("YUGO_ZAPAD_URL_RK", "")
UST_LABINSK_URL_RK = os.getenv("UST_LABINSK_URL_RK", "")
TIMASHEVSK_URL_RK = os.getenv("TIMASHEVSK_URL_RK", "")
TIKHORETSK_URL_RK = os.getenv("TIKHORETSK_URL_RK", "")
SOCHI_URL_RK = os.getenv("SOCHI_URL_RK", "")
SLAVYANSK_URL_RK = os.getenv("SLAVYANSK_URL_RK", "")
LENINGRADSK_URL_RK = os.getenv("LENINGRADSK_URL_RK", "")
LABINSK_URL_RK = os.getenv("LABINSK_URL_RK", "")
KRASNODAR_URL_RK = os.getenv("KRASNODAR_URL_RK", "")
ARMAVIR_URL_RK = os.getenv("ARMAVIR_URL_RK", "")
ADYGEYSK_URL_RK = os.getenv("ADYGEYSK_URL_RK", "")

# Reserved for notification directory (Rosseti Yug)
YUGO_ZAPAD_URL_UG_SP = os.getenv("YUGO_ZAPAD_URL_UG_SP", "")
CENTRAL_URL_UG_SP = os.getenv("CENTRAL_URL_UG_SP", "")
ZAPAD_URL_UG_SP = os.getenv("ZAPAD_URL_UG_SP", "")
VOSTOCH_URL_UG_SP = os.getenv("VOSTOCH_URL_UG_SP", "")
YUZH_URL_UG_SP = os.getenv("YUZH_URL_UG_SP", "")
SEVERO_VOSTOCH_URL_UG_SP = os.getenv("SEVERO_VOSTOCH_URL_UG_SP", "")
YUGO_VOSTOCH_URL_UG_SP = os.getenv("YUGO_VOSTOCH_URL_UG_SP", "")
SEVER_URL_UG_SP = os.getenv("SEVER_URL_UG_SP", "")

# Reserved for notification directory (Rosseti Kuban)
YUGO_ZAPAD_URL_RK_SP = os.getenv("YUGO_ZAPAD_URL_RK_SP", "")
UST_LABINSK_URL_RK_SP = os.getenv("UST_LABINSK_URL_RK_SP", "")
TIMASHEVSK_URL_RK_SP = os.getenv("TIMASHEVSK_URL_RK_SP", "")
TIKHORETSK_URL_RK_SP = os.getenv("TIKHORETSK_URL_RK_SP", "")
SOCHI_URL_RK_SP = os.getenv("SOCHI_URL_RK_SP", "")
SLAVYANSK_URL_RK_SP = os.getenv("SLAVYANSK_URL_RK_SP", "")
LENINGRADSK_URL_RK_SP = os.getenv("LENINGRADSK_URL_RK_SP", "")
LABINSK_URL_RK_SP = os.getenv("LABINSK_URL_RK_SP", "")
KRASNODAR_URL_RK_SP = os.getenv("KRASNODAR_URL_RK_SP", "")
ARMAVIR_URL_RK_SP = os.getenv("ARMAVIR_URL_RK_SP", "")
ADYGEYSK_URL_RK_SP = os.getenv("ADYGEYSK_URL_RK_SP", "")

NOTIFY_LOG_FILE_UG = "notify_log_ug.csv"
NOTIFY_LOG_FILE_RK = "notify_log_rk.csv"
