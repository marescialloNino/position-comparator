import logging
import requests
import os
from dotenv import load_dotenv
from pathlib import Path

from common.paths import LOG_DIR

LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'telegram_messenger.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()

class TGMessenger:
    api = 'https://api.telegram.org/bot'
    tg_bot_name = 'leperespreader'
    tg_username = 'alphabet_pairspreader_bot'
    TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
    chat_name = {'bitget_monitor': '-920364090'}

    @staticmethod
    def send(message, chat_channel):
        """
        Send a message to the specified Telegram chat channel.

        :param message: The message to send.
        :param chat_channel: The name of the chat channel (e.g., 'bitget_monitor').
        :return: The Telegram API response or error message.
        """
        if not TGMessenger.TOKEN:
            logger.error("TELEGRAM_BOT_TOKEN not found in environment variables")
            return {"ok": False, "error": "Missing TELEGRAM_BOT_TOKEN"}

        if chat_channel not in TGMessenger.chat_name:
            logger.error(f"Invalid chat channel: {chat_channel}")
            return {"ok": False, "error": f"Invalid chat channel: {chat_channel}"}

        chat_id = TGMessenger.chat_name[chat_channel]
        url_tg = f'{TGMessenger.api}{TGMessenger.TOKEN}/sendMessage?chat_id={chat_id}&text={message}'
        
        try:
            response = requests.get(url_tg, timeout=10)
            result_tg = response.json()
            if response.status_code != 200 or not result_tg.get("ok", False):
                logger.error(f"Telegram API error: {result_tg}")
            else:
                logger.info(f"Sent Telegram message to {chat_channel}: {message}")
            return result_tg
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {str(e)}")
            return {"ok": False, "error": str(e)}