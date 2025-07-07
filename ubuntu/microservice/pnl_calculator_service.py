#!/usr/bin/env python3
"""
pnl_calculator_service.py
~~~~~~~~~~~~~~~~~~~~~~~~
External service to compute and save PnL every 12 hours for active strategies and accounts.
Saves results to output_bitget/pnl_snapshots/ with timestamps.
"""

import json
import sys
from pathlib import Path
from datetime import datetime
import logging
import schedule
import time
from typing import Dict, Any
import argparse

from data_analyzer.calculate_theoretical_pnl import compute_pnl_timeline, load_portfolios
from data_analyzer.calculate_real_pnl import compute_real_pnl_timeline
from paths import DATA_DIR, LOG_DIR

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'pnl_calculator_service.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_config() -> Dict[str, Any]:
    """Load configuration file."""
    if not CONFIG_FILE.exists():
        logger.error(f"Config file not found at {CONFIG_FILE}")
        raise FileNotFoundError(f"Config file not found at {CONFIG_FILE}")
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def compute_and_save_pnl(account: str, exchange: str = "bitget"):
    """Compute theoretical and real PnL for an account and save to timestamped files."""
    try:
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        output_dir = OUTPUT_DIR / "pnl_snapshots"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Compute theoretical PnL
        logger.info(f"Computing theoretical PnL for account {account} on {exchange}")
        portfolios = load_portfolios()
        theo_timeline = compute_pnl_timeline(portfolios, account, exchange, fee_rate=-0.0003)
        theo_output_file = output_dir / f"pnl_theo_snapshot_{account}_{timestamp}.json"
        with open(theo_output_file, "w") as f:
            json.dump(theo_timeline, f, indent=2)
        logger.info(f"Saved theoretical PnL snapshot to {theo_output_file}")

        # Compute real PnL
        logger.info(f"Computing real PnL for account {account} on {exchange}")
        real_timeline = compute_real_pnl_timeline(account, exchange)
        real_output_file = output_dir / f"pnl_real_snapshot_{account}_{timestamp}.json"
        with open(real_output_file, "w") as f:
            json.dump(real_timeline, f, indent=2)
        logger.info(f"Saved real PnL snapshot to {real_output_file}")

    except Exception as e:
        logger.error(f"Failed to compute PnL for account {account}: {str(e)}")

def main():
    """Main function to run the PnL calculator service."""
    parser = argparse.ArgumentParser(description="PnL Calculator Service for Bitget accounts")
    parser.add_argument("--account", default="2", help="Bitget account identifier (e.g., '2', 'H1')")
    args = parser.parse_args()

    config = load_config()
    exchange = "bitget"
    account = args.account

    # Run immediately on start
    compute_and_save_pnl(account, exchange)

    # Schedule to run every 12 hours
    schedule.every(12).hours.do(compute_and_save_pnl, account=account, exchange=exchange)
    logger.info(f"Scheduled PnL calculation for account {account} every 12 hours")

    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()