import json
import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
from typing import Dict, List

# Import paths
from common.paths import CONFIG_FILE, OUTPUT_DIR, LOG_DIR, CURRENT_STATE_FILE, AGGREGATED_POSITIONS_FILE

# Configure logging
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'aggregate_positions.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def load_config() -> Dict:
    """Load configuration file."""
    if not CONFIG_FILE.exists():
        logger.error(f"Config file not found at {CONFIG_FILE}")
        raise FileNotFoundError(f"Config file not found at {CONFIG_FILE}")
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def get_active_strategies(account: str, config: Dict) -> List[str]:
    """Return active strategies for the specified account."""
    active_strategies = [
        strat_name for strat_name, strat_config in config.get('strategy', {}).items()
        if strat_config.get('active', False)
    ]
    account_strategies = config.get('allocations', {}).get('bitget', {}).get(account, {}).get('strategies', active_strategies)
    active_account_strategies = [s for s in account_strategies if s in active_strategies]
    logger.info(f"Active strategies for account {account}: {active_account_strategies}")
    return active_account_strategies

def load_strategy_positions(strategy: str) -> Dict:
    """Load positions from bot_data/{strategy}/current_state.json."""
    path = CURRENT_STATE_FILE(strategy)
    if not path.exists():
        logger.warning(f"Strategy file not found: {path}")
        return {}
    try:
        with open(path, 'r') as f:
            data = json.load(f)
        return data.get('current_coin_info', {})
    except Exception as e:
        logger.error(f"Failed to load {path}: {str(e)}")
        return {}

def aggregate_positions(strategies: List[str]) -> List[Dict]:
    """Aggregate positions across strategies."""
    asset_positions = {}  # {asset: {'qty': float, 'total_value': float, 'total_qty': float, 'executing': bool, 'target_execution_qty': float}}

    for strategy in strategies:
        positions = load_strategy_positions(strategy)
        for asset, pos_data in positions.items():
            qty = pos_data.get('quantity', 0) * pos_data.get('position', 1)
            entry_price = pos_data.get('entry_exec', 0)
            in_execution = pos_data.get('in_execution', False)
            target_qty = pos_data.get('target_qty', 0) if in_execution else 0

            if qty == 0 or entry_price == 0:
                continue

            if asset not in asset_positions:
                asset_positions[asset] = {
                    'qty': 0,
                    'total_value': 0,
                    'total_qty': 0,
                    'executing': False,
                    'target_execution_qty': 0
                }

            asset_positions[asset]['qty'] += qty
            asset_positions[asset]['total_value'] += qty * entry_price
            asset_positions[asset]['total_qty'] += abs(qty)
            asset_positions[asset]['executing'] |= in_execution  # Set to True if any position is executing
            asset_positions[asset]['target_execution_qty'] += target_qty if in_execution else 0

    # Convert to output format
    position_data = []
    for asset, data in asset_positions.items():
        qty = data['qty']
        if qty == 0:
            continue  # Skip zero-quantity positions
        avg_entry_price = data['total_value'] / data['total_qty'] if data['total_qty'] != 0 else 0
        usd_value = qty * avg_entry_price
        position_data.append({
            "asset": asset,
            "qty": qty,
            "usd_value": usd_value,
            "executing": data['executing'],
            "target_execution_qty": data['target_execution_qty']
        })
    
    return position_data

def main():
    """Main function to aggregate theoretical positions."""
    parser = argparse.ArgumentParser(description="Aggregate theoretical positions for a Bitget account.")
    parser.add_argument("--account", default="2", help="Bitget account identifier (e.g., '2', 'H1')")
    args = parser.parse_args()

    try:
        # Load config
        config = load_config()
        
        # Get active strategies
        strategies = get_active_strategies(args.account, config)
        if not strategies:
            logger.error(f"No active strategies found for account {args.account}")
            sys.exit(1)
        
        # Aggregate positions
        logger.info(f"Aggregating positions for account {args.account} from strategies: {strategies}")
        position_data = aggregate_positions(strategies)
        
        # Save to JSON
        current_time = datetime.utcnow().isoformat()
        output_file = AGGREGATED_POSITIONS_FILE(args.account)
        with output_file.open('w') as f:
            json.dump({
                "timestamp": current_time,
                "account": args.account,
                "positions": position_data
            }, f, indent=2)
        logger.info(f"Saved {len(position_data)} aggregated positions to {output_file}")
        
    except Exception as e:
        logger.error(f"Failed to aggregate positions: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()