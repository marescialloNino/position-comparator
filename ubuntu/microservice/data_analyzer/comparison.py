import json
import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
from typing import Dict, List
import os
from dotenv import load_dotenv

# Import TGMessenger
from common.bot_reporting import TGMessenger

# Import paths
from common.paths import AGGREGATED_POSITIONS_FILE, BITGET_POSITIONS_FILE, ENV_FILE

# Configure logging
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
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

def send_telegram_alert(message: str):
    """Send alert message to configured Telegram channel."""
    logger.info(f"Sending alert: {message}")
    try:
        response = TGMessenger.send(message, 'bitget_monitor')
        if isinstance(response, dict) and not response.get("ok", False):
            logger.error(f"Telegram response error: {response}")
    except Exception as e:
        logger.error(f"Telegram alert failed: {e}")

def within_tolerance(value1: float, value2: float, tolerance_percent: float = 1.0) -> bool:
    """Check if two values are within a given percentage tolerance."""
    if value1 == 0 and value2 == 0:
        return True
    if value1 == 0 or value2 == 0:
        return abs(value1 - value2) <= abs(0.01 * tolerance_percent * max(abs(value1), abs(value2)))
    return abs(value1 - value2) / abs(value1) * 100 <= tolerance_percent

def compare_positions(theo_data: Dict, bitget_data: Dict, account: str) -> None:
    """Compare theoretical and Bitget positions, sending Telegram alerts for mismatches."""
    theo_positions = {pos['asset']: pos for pos in theo_data.get('positions', [])}
    bitget_positions = {pos['asset']: pos for pos in bitget_data.get('positions', [])}

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
            send_telegram_alert(message)
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
                send_telegram_alert(message)
        else:
            # For non-executing positions, check if quantities are within 1% tolerance
            if not within_tolerance(theo_qty, bitget_qty, 1.0):
                message = (
                    f"Alert: Asset {asset} in account {account} has quantity mismatch. "
                    f"Theoretical qty: {theo_qty}, Bitget qty: {bitget_qty} (diff > 1%)."
                )
                send_telegram_alert(message)

    # Check for tokens in Bitget positions but not in theoretical positions
    for asset in bitget_positions:
        if asset not in theo_positions:
            bitget_qty = bitget_positions[asset]['qty']
            message = (
                f"Alert: Asset {asset} in Bitget positions for account {account} "
                f"(qty: {bitget_qty}) not found in theoretical positions."
            )
            send_telegram_alert(message)

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
        if not bot_token:
            logger.error("Missing TELEGRAM_BOT_TOKEN environment variable")
            raise ValueError("Missing TELEGRAM_BOT_TOKEN")

        # Load JSON files
        theo_file = AGGREGATED_POSITIONS_FILE(args.account)
        bitget_file = BITGET_POSITIONS_FILE(args.account)
        logger.info(f"Loading theoretical positions from {theo_file}")
        logger.info(f"Loading Bitget positions from {bitget_file}")
        theo_data = load_json_file(theo_file)
        bitget_data = load_json_file(bitget_file)

        # Compare positions
        logger.info(f"Comparing positions for account {args.account}")
        compare_positions(theo_data, bitget_data, args.account)
        logger.info("Position comparison completed.")

    except Exception as e:
        logger.error(f"Failed to compare positions: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()