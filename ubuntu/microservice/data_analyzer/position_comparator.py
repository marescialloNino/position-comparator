import logging
from typing import Dict
import aiohttp
from reporting.bot_reporting import TGMessenger

logger = logging.getLogger(__name__)

def within_tolerance(value1: float, value2: float, tolerance_percent: float = 1.0) -> bool:
    """Check if two values are within a given percentage tolerance."""
    if value1 == 0 and value2 == 0:
        return True
    if value1 == 0 or value2 == 0:
        return abs(value1 - value2) <= abs(0.01 * tolerance_percent * max(abs(value1), abs(value2)))
    return abs(value1 - value2) / abs(value1) * 100 <= tolerance_percent

def compare_positions(theo_positions: Dict, real_positions: Dict, session_key: str) -> Dict:
    """Compare theoretical and real positions, returning a dictionary with matching results and sending alerts for mismatches."""
    result = {}
    
    # Compare tokens in theoretical positions
    for asset, theo_pos in theo_positions.items():
        theo_qty = theo_pos['quantity']
        executing = theo_pos.get('executing', False)
        target_execution_qty = theo_pos.get('target_execution_qty', 0)

        if asset not in real_positions:
            result[asset] = {
                'theo_qty': theo_qty,
                'real_qty': 0.0,
                'executing': executing,
                'matching': False
            }
            message = (
                f"Alert: Asset {asset} in theoretical positions for {session_key} "
                f"(qty: {theo_qty}, executing: {executing}) not found in real positions."
            )
            try:
                response = TGMessenger.send_message(message, 'CM', use_telegram=False)
                if response.get('ok'):
                    logger.info(f"Sent message to CM: {message}")
                else:
                    logger.error(f"Failed to send message to CM: {response}")
            except Exception as e:
                logger.error(f"Error sending message to CM: {e}")
            continue

        real_qty = real_positions[asset]['quantity']
        matching = True

        if executing:
            # For executing positions, check if real qty is within [qty, qty + target_execution_qty]
            min_qty = min(theo_qty, theo_qty + target_execution_qty)
            max_qty = max(theo_qty, theo_qty + target_execution_qty)
            tolerance = 0.01 * max(abs(min_qty), abs(max_qty))
            if not (min_qty - tolerance <= real_qty <= max_qty + tolerance):
                matching = False
                message = (
                    f"Alert: Asset {asset} in {session_key} has executing position. "
                    f"Theoretical qty: {theo_qty}, target_execution_qty: {target_execution_qty}, "
                    f"Real qty: {real_qty}. Real qty not in range [{min_qty}, {max_qty}] ± 1%."
                )
                try:
                    response = TGMessenger.send_message(message, 'CM', use_telegram=False)
                    if response.get('ok'):
                        logger.info(f"Sent message to CM: {message}")
                    else:
                        logger.error(f"Failed to send message to CM: {response}")
                except Exception as e:
                    logger.error(f"Error sending message to CM: {e}")
        else:
            # For non-executing positions, check if quantities are within 1% tolerance
            if not within_tolerance(theo_qty, real_qty, 1.0):
                matching = False
                message = (
                    f"Alert: Asset {asset} in {session_key} has quantity mismatch. "
                    f"Theoretical qty: {theo_qty}, Real qty: {real_qty} (diff > 1%)."
                )
                try:
                    response =  TGMessenger.send_message(message, 'CM', use_telegram=False)
                    if response.get('ok'):
                        logger.info(f"Sent message to CM: {message}")
                    else:
                        logger.error(f"Failed to send message to CM: {response}")
                except Exception as e:
                    logger.error(f"Error sending message to CM: {e}")

        result[asset] = {
            'theo_qty': theo_qty,
            'real_qty': real_qty,
            'executing': executing,
            'matching': matching
        }

    # Check for tokens in real positions but not in theoretical positions
    for asset in real_positions:
        if asset not in theo_positions:
            real_qty = real_positions[asset]['quantity']
            result[asset] = {
                'theo_qty': 0.0,
                'real_qty': real_qty,
                'executing': False,
                'matching': False
            }
            message = (
                f"Alert: Asset {asset} in real positions for {session_key} "
                f"(qty: {real_qty}) not found in theoretical positions."
            )
            try:
                response = TGMessenger.send_message(message, 'CM', use_telegram=False)
                if response.get('ok'):
                    logger.info(f"Sent message to CM: {message}")
                else:
                    logger.error(f"Failed to send message to CM: {response}")
            except Exception as e:
                logger.error(f"Error sending message to CM: {e}")

    return result