import logging
from typing import Dict, List, Tuple
from datetime import datetime, timezone as UTC
import traceback
import numpy as np
import json
import os
from datafeed.utils_online import parse_pair

logger = logging.getLogger(__name__)

def within_tolerance(value1: float, value2: float, tolerance: float = 0.1) -> bool:
    """Check if two values are within a given percentage tolerance."""
    if value1 == 0 and value2 == 0:
        return True
    if abs(value1 + value2) < 1e-8:  # Avoid division by zero
        return abs(value1 - value2) < 1e-8
    return abs(value1 - value2) / abs(value1 + value2) <= tolerance

def compare_positions(theo_positions: Dict, real_positions: Dict, session_key: str, tolerance: float = 0.1, processor=None) -> Tuple[Dict, List[str]]:
    """
    Compare theoretical and real positions, prioritizing USD amounts when prices are available,
    falling back to quantities when prices are not available. Returns a dictionary with matching
    results, dust/mismatch status, mismatch counts, and a list of mismatch messages.
    Dust is set to True if strategy_count == 0 and USD amount < 20% of median position size (when prices available),
    or if strategy_count == 0 and theo_qty == 0 and real_qty != 0 (when prices unavailable).
    Requires processor instance for price cache, median position size, and mismatch counts.
    """
    result = {}
    messages = []
    exchange = session_key.split('_')[0]
    current_time = datetime.utcnow()
    
    # Get median position size from processor
    median_size = processor.median_position_sizes.get(session_key, 0) if processor else 0
    if median_size == 0:
        logger.warning(f"Median position size is zero for {session_key}, dust checks may be inaccurate")

    # Initialize mismatch_counts for session_key if not exists
    if processor and not hasattr(processor, 'mismatch_counts'):
        processor.mismatch_counts = {}
    if processor and session_key not in processor.mismatch_counts:
        processor.mismatch_counts[session_key] = {}

    # Count strategies for each token
    strategy_counts = {}
    if processor:
        working_directory = processor.session_configs.get(exchange, {}).get('working_directory', '')
        for strategy_name, (strat_exchange, strat_account) in processor.used_accounts.get(exchange, {}).items():
            if f"{strat_exchange}_{strat_account}" == session_key:
                strategy_dir = os.path.join(working_directory, strategy_name)
                state_file = os.path.join(strategy_dir, 'current_state.json')
                if os.path.exists(state_file):
                    try:
                        with open(state_file, 'r') as f:
                            state = json.loads(f.read())
                        if 'current_coin_info' in state:
                            for coin, coin_info in state['current_coin_info'].items():
                                if coin_info.get('quantity', 0) != 0:
                                    strategy_counts[coin] = strategy_counts.get(coin, 0) + 1
                        elif 'current_pair_info' in state:
                            for pair_name, pair_info in state['current_pair_info'].items():
                                s1, s2 = parse_pair(pair_name)
                                if pair_info.get('quantity', [0, 0])[0] != 0:
                                    strategy_counts[s1] = strategy_counts.get(s1, 0) + 1
                                if pair_info.get('quantity', [0, 0])[1] != 0:
                                    strategy_counts[s2] = strategy_counts.get(s2, 0) + 1
                    except Exception as e:
                        logger.error(f"Error reading/parsing {state_file}: {str(e)}")
                        logger.error(traceback.format_exc())

    # Compare tokens in theoretical positions
    for asset, theo_pos in theo_positions.items():
        theo_qty = theo_pos['quantity']
        executing = theo_pos.get('executing', False)
        target_execution_qty = theo_pos.get('target_execution_qty', 0)
        strategy_count = strategy_counts.get(asset, 0)
        
        # Get price from cache
        price = None
        if processor and exchange in processor.price_cache and asset in processor.price_cache[exchange]:
            price_info = processor.price_cache[exchange][asset]
            price = price_info['price']
        else:
            logger.warning(f"No price available for {asset} on {exchange}, falling back to quantity comparison")
        
        real_qty = real_positions.get(asset, {}).get('quantity', 0.0)
        theo_amount = abs(theo_qty * price) if price is not None else None
        real_amount = abs(real_qty * price) if price is not None else None
        is_dust = False
        is_mismatch = False

        # Perform comparison: prefer amount-based, fallback to quantity-based
        if price is not None and theo_amount is not None and real_amount is not None:
            # Amount-based comparison
            if executing:
                # For executing positions, check if real amount is within range
                min_amount = min(theo_qty, theo_qty + target_execution_qty) * price
                max_amount = max(theo_qty, theo_qty + target_execution_qty) * price
                tolerance_amount = 0.01 * max(abs(min_amount), abs(max_amount))
                if not (min_amount - tolerance_amount <= real_amount <= max_amount + tolerance_amount):
                    is_mismatch = True
            else:
                # Non-executing positions: check if amounts are within tolerance
                if not within_tolerance(theo_amount, real_amount, tolerance):
                    is_mismatch = True
        else:
            # Fallback to quantity-based comparison
            if executing:
                min_qty = min(theo_qty, theo_qty + target_execution_qty)
                max_qty = max(theo_qty, theo_qty + target_execution_qty)
                tolerance_qty = 0.01 * max(abs(min_qty), abs(max_qty))
                if not (min_qty - tolerance_qty <= real_qty <= max_qty + tolerance_qty):
                    is_mismatch = True
            else:
                if not within_tolerance(theo_qty, real_qty, tolerance):
                    is_mismatch = True

        # Dust check: set is_dust to True if strategy_count == 0 and amount < 0.2 * median_size
        if theo_qty == 0 and real_qty != 0 and strategy_count == 0:
            if real_amount is not None and median_size > 0:
                if real_amount < 0.2 * median_size:
                    is_dust = True
                    logger.info(f"Flagged {asset} as dust: real_amount={real_amount:.2f}, median_size={median_size:.2f}")
                else:
                    is_mismatch = True
            else:
                # No price available, flag as dust if strategy_count == 0 and theo_qty == 0
                is_dust = True
                logger.info(f"Flagged {asset} as dust (quantity-based): real_qty={real_qty}, strategy_count=0")

        matching = not is_mismatch

        # Update mismatch count
        mismatch_count = processor.mismatch_counts[session_key].get(asset, 0) if processor else 0
        if is_mismatch:
            mismatch_count += 1
        else:
            mismatch_count = 0  # Reset on match
        if processor:
            processor.mismatch_counts[session_key][asset] = mismatch_count

        if is_mismatch:
            message = (
                f"** POSITION MISMATCH **\n"
                f"Asset {asset} in {session_key} has {'amount' if price is not None else 'quantity'} mismatch.\n"
                f"Theoretical qty: {theo_qty}, Real qty: {real_qty}, Strategy count: {strategy_count}\n"
            )
            if price is not None:
                message += f"Theoretical amount: {theo_amount:.2f} USD, Real amount: {real_amount:.2f} USD\n"
            message += f"Median position size: {median_size:.2f} USD\n"
            message += f"Consecutive mismatches: {mismatch_count}\n"
            message += f"{', executing: True' if executing else ''}."
            messages.append(message)

        result[asset] = {
            'theo_qty': theo_qty,
            'real_qty': real_qty,
            'theo_amount': theo_amount,
            'real_amount': real_amount,
            'executing': executing,
            'matching': matching,
            'strategy_count': strategy_count,
            'is_dust': is_dust,
            'is_mismatch': is_mismatch,
            'mismatch_count': mismatch_count
        }

    # Check for tokens in real positions but not in theoretical positions
    for asset in real_positions:
        if asset not in theo_positions:
            real_qty = real_positions[asset]['quantity']
            strategy_count = strategy_counts.get(asset, 0)
            price = None
            if processor and exchange in processor.price_cache and asset in processor.price_cache[exchange]:
                price_info = processor.price_cache[exchange][asset]
                price = price_info['price']
            else:
                logger.warning(f"No price available for {asset} on {exchange}, falling back to quantity comparison")
            real_amount = abs(real_qty * price) if price is not None else None
            is_dust = False
            is_mismatch = False

            # Dust check: set is_dust to True if strategy_count == 0 and amount < 0.2 * median_size
            if strategy_count == 0:
                if real_amount is not None and median_size > 0:
                    if real_amount < 0.2 * median_size:
                        is_dust = True
                        logger.info(f"Flagged {asset} as dust: real_amount={real_amount:.2f}, median_size={median_size:.2f}")
                    else:
                        is_mismatch = True
                else:
                    # No price available, flag as dust if strategy_count == 0
                    is_dust = True
                    logger.info(f"Flagged {asset} as dust (quantity-based): real_qty={real_qty}, strategy_count=0")

            # Update mismatch count
            mismatch_count = processor.mismatch_counts[session_key].get(asset, 0) if processor else 0
            if is_mismatch:
                mismatch_count += 1
            else:
                mismatch_count = 0  # Reset on match
            if processor:
                processor.mismatch_counts[session_key][asset] = mismatch_count

            result[asset] = {
                'theo_qty': 0.0,
                'real_qty': real_qty,
                'theo_amount': 0.0,
                'real_amount': real_amount,
                'executing': False,
                'matching': False,
                'strategy_count': strategy_count,
                'is_dust': is_dust,
                'is_mismatch': is_mismatch,
                'mismatch_count': mismatch_count
            }

            if is_mismatch:
                message = (
                    f"** POSITION MISMATCH **\n"
                    f"Asset {asset} in {session_key} found in real positions but not in theoretical.\n"
                    f"Real qty: {real_qty}, Strategy count: {strategy_count}\n"
                )
                if real_amount is not None:
                    message += f"Real amount: {real_amount:.2f} USD\n"
                message += f"Median position size: {median_size:.2f} USD\n"
                message += f"Consecutive mismatches: {mismatch_count}."
                messages.append(message)

    return result, messages