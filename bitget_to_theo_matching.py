import json
import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
from typing import Dict, List
import os
from dotenv import load_dotenv
import telegram
import asyncio

# Import paths
from paths import AGGREGATED_POSITIONS_FILE, BITGET_POSITIONS_FILE, ENV_FILE, LOG_DIR


LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'compare_positions.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_json_file(file_path: Path) -> Dict:
    """Load JSON file and return its contents."""
    if not file_path.exists():
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found: {file_path}")
    try:
        with file_path.open('r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {str(e)}")
        raise

async def send_telegram_alert(message: str, bot_token: str, chat_id: str):
    """Send a Telegram alert."""
    try:
        bot = telegram.Bot(token=bot_token)
        await bot.send_message(chat_id=chat_id, text=message)
        logger.info(f"Sent Telegram alert: {message}")
    except Exception as e:
        logger.error(f"Failed to send Telegram alert: {str(e)}")

def within_tolerance(value1: float, value2: float, tolerance_percent: float = 1.0) -> bool:
    """Check if two values are within a given percentage tolerance."""
    if value1 == 0 and value2 == 0:
        return True
    if value1 == 0 or value2 == 0:
        return abs(value1 - value2) <= abs(0.01 * tolerance_percent * max(abs(value1), abs(value2)))
    return abs(value1 - value2) / abs(value1) * 100 <= tolerance_percent

def compare_positions(theo_data: Dict, bitget_data: Dict, account: str, bot_token: str, chat_id: str) -> None:
    """Compare theoretical and Bitget positions, sending Telegram alerts for mismatches."""
    theo_positions = {pos['asset']: pos for pos in theo_data.get('positions', [])}
    bitget_positions = {pos['asset']: pos for pos in bitget_data.get('positions', [])}

    # Initialize asyncio event loop for Telegram alerts
    loop = asyncio.get_event_loop()

    # Compare tokens in theoretical positions
    for asset, theo_pos in theo_positions.items():
        theo_qty = theo_pos['qty']
        executing = theo_pos.get('executing', False)
        target_execution_qty = theo_pos.get('target_execution_qty', 0)
        usd_value = theo_pos['usd_value']

        if asset not in bitget_positions:
            message = (
                f"Alert: Asset {asset} in theoretical positions for account {account} "
                f"(qty: {theo_qty}, executing: {executing}) not found in Bitget positions."
            )
            loop.run_until_complete(send_telegram_alert(message, bot_token, chat_id))
            continue

        bitget_qty = bitget_positions[asset]['qty']
        bitget_usd_value = bitget_positions[asset]['usd_value']

        if executing:
            # For executing positions, check if Bitget qty is within [qty, qty + target_execution_qty]
            min_qty = min(theo_qty, theo_qty + target_execution_qty)
            max_qty = max(theo_qty, theo_qty + target_execution_qty)
            # Apply 1% tolerance to the range boundaries
            tolerance = 0.01 * max(abs(min_qty), abs(max_qty))
            if not (min_qty - tolerance <= bitget_qty <= max_qty + tolerance):
                message = (
                    f"Alert: Asset {asset} in account {account} has executing position. "
                    f"Theoretical qty: {theo_qty}, target_execution_qty: {target_execution_qty}, "
                    f"Bitget qty: {bitget_qty}. Bitget qty not in range [{min_qty}, {max_qty}] ± 1%."
                )
                loop.run_until_complete(send_telegram_alert(message, bot_token, chat_id))
        else:
            # For non-executing positions, check if quantities are within 1% tolerance
            if not within_tolerance(theo_qty, bitget_qty, 1.0):
                message = (
                    f"Alert: Asset {asset} in account {account} has quantity mismatch. "
                    f"Theoretical qty: {theo_qty}, Bitget qty: {bitget_qty} (diff > 1%)."
                )
                loop.run_until_complete(send_telegram_alert(message, bot_token, chat_id))

    # Check for tokens in Bitget positions but not in theoretical positions
    for asset in bitget_positions:
        if asset not in theo_positions:
            bitget_qty = bitget_positions[asset]['qty']
            message = (
                f"Alert: Asset {asset} in Bitget positions for account {account} "
                f"(qty: {bitget_qty}) not found in theoretical positions."
            )
            loop.run_until_complete(send_telegram_alert(message, bot_token, chat_id))

def main():
    """Main function to compare theoretical and Bitget positions."""
    parser = argparse.ArgumentParser(description="Compare theoretical and Bitget positions for a Bitget account.")
    parser.add_argument("--account", default="2", help="Bitget account identifier (e.g., '2', 'H1')")
    args = parser.parse_args()

    try:
        # Load .env file
        if not ENV_FILE.exists():
            logger.error(f".env file not found at {ENV_FILE}")
            raise FileNotFoundError(f".env file not found at {ENV_FILE}")
        load_dotenv(dotenv_path=ENV_FILE)
        bot_token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        if not all([bot_token, chat_id]):
            logger.error("Missing Telegram environment variables: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID")
            raise ValueError("Missing Telegram environment variables")

        # Load JSON files
        theo_file = AGGREGATED_POSITIONS_FILE(args.account)
        bitget_file = BITGET_POSITIONS_FILE(args.account)
        logger.info(f"Loading theoretical positions from {theo_file}")
        logger.info(f"Loading Bitget positions from {bitget_file}")
        theo_data = load_json_file(theo_file)
        bitget_data = load_json_file(bitget_file)

        # Compare positions
        logger.info(f"Comparing positions for account {args.account}")
        compare_positions(theo_data, bitget_data, args.account, bot_token, chat_id)
        logger.info("Position comparison completed.")

    except Exception as e:
        logger.error(f"Failed to compare positions: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()