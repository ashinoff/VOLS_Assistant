import threading
import time
import requests
import logging
from config import SELF_URL

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)

def keep_alive():
    def ping():
        while True:
            try:
                response = requests.get(SELF_URL)
                logger.info(f"Pinged {SELF_URL}: {response.status_code}")
            except Exception as e:
                logger.error(f"Ping failed: {e}")
            time.sleep(600)  # Ping every 10 minutes

    if SELF_URL:
        thread = threading.Thread(target=ping, daemon=True)
        thread.start()
        logger.info("Keep-alive thread started")
