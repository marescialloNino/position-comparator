
import asyncio
import sys
import os
from dotenv import load_dotenv
import json
from pathlib import Path
from datetime import datetime
import logging
import argparse

# Add project root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from hedge_monitoring.datafeed import bitgetfeed as bg

# --- CONFIG ---
MODULE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = MODULE_DIR / "output_bitget"
LOG_DIR = MODULE_DIR / "logs"
ERROR_FLAGS_PATH = OUTPUT_DIR / "hedge_error_flags.json"
ENV_FILE = MODULE_DIR / ".env"

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'bitget_position_fetcher.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Ensure output directory exists
def ensure_output_directory():
    """Ensure output and log directories exist."""
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    except Exception as e:
        logger.error(f"Error creating output directory {OUTPUT_DIR}: {str(e)}")

# Load error flags
def load_error_flags():
    """Load existing error flags or return defaults."""
    try:
        if ERROR_FLAGS_PATH.exists():
            with ERROR_FLAGS_PATH.open('r') as f:
                return json.load(f)
    except Exception as e:
        logger.error(f"Error reading error flags from {ERROR_FLAGS_PATH}: {str(e)}")
    return {
        "HEDGING_FETCHING_BITGET_ERROR": False,
        "last_updated_hedge": "",
        "bitget_error_message": ""
    }

# Update error flags
def update_error_flags(flags: dict):
    """Update error flags JSON file."""
    try:
        with ERROR_FLAGS_PATH.open('w') as f:
            json.dump(flags, f, indent=4)
        logger.info(f"Updated error flags: {json.dumps(flags)}")
    except Exception as e:
        logger.error(f"Error writing error flags to {ERROR_FLAGS_PATH}: {str(e)}")

# Fix for Windows event loop
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

async def fetch_bitget_positions(account: str):
    """Fetch current positions from Bitget for the specified account."""
    logger.info(f"Starting Bitget position fetcher for account {account}...")
    
    # Ensure output directory
    ensure_output_directory()
    
    # Initialize error flags
    error_flags = load_error_flags()
    error_flags.update({
        "HEDGING_FETCHING_BITGET_ERROR": False,
        "bitget_error_message": ""
    })
    update_error_flags(error_flags)
    
    # Check .env file
    if not ENV_FILE.exists():
        error_msg = f".env file not found at {ENV_FILE}"
        logger.error(error_msg)
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": True,
            "bitget_error_message": error_msg
        })
        update_error_flags(error_flags)
        raise FileNotFoundError(error_msg)
    
    # Load API credentials
    load_dotenv(dotenv_path=ENV_FILE)
    api_key = os.getenv(f"BITGET_{account}_API_KEY") or os.getenv(f"BITGET_API_KEY_{account}")
    api_secret = os.getenv(f"BITGET_{account}_API_SECRET") or os.getenv(f"BITGET_API_SECRET_{account}")
    api_password = os.getenv("BITGET_API_PASSWORD")
    
    if not all([api_key, api_secret, api_password]):
        missing_vars = []
        if not api_key:
            missing_vars.append(f"BITGET_{account}_API_KEY or BITGET_API_KEY_{account}")
        if not api_secret:
            missing_vars.append(f"BITGET_{account}_API_SECRET or BITGET_API_SECRET_{account}")
        if not api_password:
            missing_vars.append("BITGET_API_PASSWORD")
        error_msg = f"Missing environment variables: {', '.join(missing_vars)}"
        logger.error(error_msg)
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": True,
            "bitget_error_message": error_msg
        })
        update_error_flags(error_flags)
        raise ValueError(error_msg)
    
    logger.info(f"Loaded API credentials for account {account}")
    
    # Initialize Bitget market with credentials
    market = bg.BitgetMarket(account=account, api_key=api_key, api_secret=api_secret, api_password=api_password)

    try:
        logger.info(f"Fetching positions from Bitget for account {account}...")
        positions = await market.get_positions_async()
        
        current_time = datetime.utcnow().isoformat()
        position_data = []
        for symbol, (qty, amount, entry_price, entry_ts) in positions.items():
            position_data.append({
                "asset": symbol,
                "qty": qty,
                "usd_value": amount,
                "entry_price": entry_price,
                "entry_time": entry_ts.strftime('%Y/%m/%d %H:%M:%S.%f') if entry_ts else current_time
            })

        # Save positions to JSON
        output_file = OUTPUT_DIR / f"bitget_positions_{account}.json"
        with output_file.open('w') as f:
            json.dump({
                "timestamp": current_time,
                "account": account,
                "positions": position_data
            }, f, indent=2)
        logger.info(f"Saved {len(position_data)} positions to {output_file}")

        # Update error flags on success
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": False,
            "last_updated_hedge": current_time,
            "bitget_error_message": ""
        })
        update_error_flags(error_flags)

        return position_data

    except Exception as e:
        error_msg = f"Error fetching positions for account {account}: {str(e)}"
        logger.error(error_msg)
        error_flags.update({
            "HEDGING_FETCHING_BITGET_ERROR": True,
            "bitget_error_message": error_msg
        })
        update_error_flags(error_flags)
        raise
    finally:
        await market._exchange_async.close()
        logger.info("Exchange connection closed.")

def main():
    """Main function to run the position fetcher."""
    parser = argparse.ArgumentParser(description="Fetch Bitget positions for a specified account.")
    parser.add_argument("--account", default="2", help="Bitget account identifier (e.g., '2', 'H1')")
    args = parser.parse_args()

    try:
        asyncio.run(fetch_bitget_positions(args.account))
    except Exception as e:
        logger.error(f"Failed to fetch positions: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()