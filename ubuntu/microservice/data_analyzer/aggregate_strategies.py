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
                    'executing': False,
                    'target_execution_qty': 0
                }

            asset_positions[asset]['qty'] += qty
            asset_positions[asset]['executing'] |= in_execution  # Set to True if any position is executing
            asset_positions[asset]['target_execution_qty'] += target_qty if in_execution else 0

    # Convert to output format
    position_data = []
    for asset, data in asset_positions.items():
        qty = data['qty']
        if qty == 0:
            continue  # Skip zero-quantity positions
        position_data.append({
            "asset": asset,
            "qty": qty,
            "executing": data['executing'],
            "target_execution_qty": data['target_execution_qty']
        })
    
    return position_data

from typing import List, Dict, Union
async def aggregate_theo_positions(strategy_positions: Union[List[Dict], Dict]) -> Dict:
    """Aggregate theoretical positions from a list of strategy state dictionaries or a single current_coin_info dictionary.
    
    Args:
        strategy_positions: Either a list of dictionaries containing strategy state data (with 'current_coin_info' key)
                          or a single dictionary containing current_coin_info data.
        
    Returns:
        Dict with structure {'pose': {asset: {'quantity': float, 'ref_price': float, 
                            'amount': float, 'entry_ts': str, 'executing': bool, 
                            'target_execution_qty': float}}}.
    """
    logger = logging.getLogger(__name__)
    logger.debug(f"Input strategy_positions type: {type(strategy_positions)}")
    
    asset_positions = {}
    if isinstance(strategy_positions, dict):
        logger.debug("Processing single current_coin_info dictionary")
        coin_info_list = [strategy_positions] if strategy_positions else []
    elif isinstance(strategy_positions, list):
        logger.debug("Processing list of strategy states")
        coin_info_list = []
        for state in strategy_positions:
            if not isinstance(state, dict):
                logger.warning(f"Skipping invalid strategy state: expected dict, got {type(state)}")
                continue
            if not state:
                logger.debug("Empty strategy state received")
                continue
            coin_info = state.get('current_coin_info', {})
            if not isinstance(coin_info, dict):
                logger.warning(f"Invalid current_coin_info: expected dict, got {type(coin_info)}")
                continue
            if coin_info:
                coin_info_list.append(coin_info)
            else:
                logger.debug("Empty current_coin_info for strategy state")
    else:
        logger.error(f"Invalid input type for strategy_positions: {type(strategy_positions)}")
        return {'pose': {}}

    logger.debug(f"Processing {len(coin_info_list)} coin_info dictionaries")
    
    for coin_info in coin_info_list:
        if not coin_info:
            logger.debug("Empty coin_info received")
            continue
        for asset, pos_data in coin_info.items():
            if not isinstance(pos_data, dict):
                logger.warning(f"Skipping invalid pos_data for {asset}: expected dict, got {type(pos_data)}")
                continue
            try:
                qty = pos_data.get('quantity', 0) * pos_data.get('position', 1)
                
                in_execution = pos_data.get('in_execution', False)
                target_qty = pos_data.get('target_qty', 0) if in_execution else 0
                
            except Exception as e:
                logger.error(f"Error processing pos_data for {asset}: {e}, data: {pos_data}")
                continue
            if qty == 0 :
                logger.debug(f"Skipping zero quantity for {asset}")
                continue
            
            if asset not in asset_positions:
                asset_positions[asset] = {
                    'qty': 0.0,
                    'executing': False,
                    'target_execution_qty': 0.0,
                }
            asset_positions[asset]['qty'] += qty
            asset_positions[asset]['executing'] |= in_execution
            asset_positions[asset]['target_execution_qty'] += target_qty

    
    position_data = {}
    for asset, data in asset_positions.items():
        qty = data['qty']
        if qty == 0:
            logger.debug(f"Skipping zero-quantity position for {asset}")
            continue
        position_data[asset] = {
            'quantity': qty,
            'executing': data['executing'],
            'target_execution_qty': data['target_execution_qty']
        }
    
    logger.info(f"Aggregated {len(position_data)} positions")
    return {'pose': position_data}



def main():
    import asyncio
    strategies = ['basket1']
    input_data = []
    base_path = Path('C:/Users/Z640/dev/position-comparator/data/bot_data')
    for strategy in strategies:
        state_file = base_path / strategy / 'current_state.json'
        if state_file.exists():
            try:
                with open(state_file, 'r') as f:
                    data = json.load(f)
                    if isinstance(data, dict):
                        input_data.append(data)
                        logger.info(f"Loaded {state_file}")
                    else:
                        logger.warning(f"Invalid JSON in {state_file}: expected dict, got {type(data)}")
            except Exception as e:
                logger.error(f"Failed to load {state_file}: {e}")
        else:
            logger.warning(f"File not found: {state_file}")
    logger.info(f"Testing aggregate_theo_positions with {len(input_data)} strategy states")
    result = asyncio.run(aggregate_theo_positions(input_data))
    current_time = datetime.utcnow().isoformat()
    output = {
        "timestamp": current_time,
        "account": "2",
        "positions": result['pose']
    }
    print(json.dumps(output, indent=2))
    with open('aggregate_theo_positions_output.json', 'w') as f:
        json.dump(output, f, indent=2)
    logger.info("Saved output to aggregate_theo_positions_output.json")


if __name__ == "__main__":
    main()