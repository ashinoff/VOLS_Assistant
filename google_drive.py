import csv
import io
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from config import GOOGLE_DRIVE_FILE_ID, SCOPES

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

def get_drive_service():
    creds = None
    # Load credentials from token.json if it exists
    try:
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    except FileNotFoundError:
        pass

    # If no valid credentials, run the OAuth flow
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save credentials for next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    return build("drive", "v3", credentials=creds)

def load_user_data():
    users = {}
    try:
        service = get_drive_service()
        # Download zones_rk_ug.csv
        request = service.files().get_media(fileId=GOOGLE_DRIVE_FILE_ID)
        file_stream = io.BytesIO()
        downloader = MediaIoBaseDownload(file_stream, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()

        # Read CSV from stream
        file_stream.seek(0)
        csv_content = file_stream.getvalue().decode("utf-8")
        csv_reader = csv.DictReader(io.StringIO(csv_content))
        for row in csv_reader:
            users[row["Telegram ID"]] = {
                "Visibility": row["Visibility"],
                "Branch": row["Филиал"],
                "RES": row["РЭС"],
                "FIO": row["ФИО"],
                "Responsible": row["Ответственный"],
            }
    except Exception as e:
        logger.error(f"Error loading user data from Google Drive: {e}")
    return users
