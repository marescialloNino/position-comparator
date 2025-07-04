import json
import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
from typing import List, Dict, Union

logger = logging.getLogger(__name__)


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
