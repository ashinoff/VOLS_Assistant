import os
from dotenv import load_dotenv

load_dotenv()

# Bot configuration
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_TOKEN")

# Google Drive configuration
GOOGLE_DRIVE_FILE_ID = os.getenv("GOOGLE_DRIVE_FILE_ID", "YOUR_GOOGLE_DRIVE_FILE_ID")
SCOPES = ["https://www.googleapis.com/auth/drive.file"]
