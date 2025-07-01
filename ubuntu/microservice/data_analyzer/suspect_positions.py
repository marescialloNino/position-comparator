import json
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
from typing import Dict, List, Any

from common.paths import BOT_DATA_DIR, OUTPUT_DIR, CONFIG_FILE

# Configure logging
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'find_suspect_positions.log'),
        logging.StreamHandler()
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

def get_active_strategies() -> List[str]:
    """Return list of active strategy names."""
    config = load_config()
    active_strategies = [
        strat_name for strat_name, strat_config in config.get('strategy', {}).items()
        if strat_config.get('active', False)
    ]
    logger.info(f"Active strategies: {active_strategies}")
    return active_strategies

def load_closed_positions(strategy: str) -> Dict[str, List[Dict]]:
    """Load closed positions from closed_positions_{strategy}.json."""
    file_path = OUTPUT_DIR / f"closed_positions_{strategy}.json"
    if not file_path.exists():
        logger.warning(f"Closed positions file not found for strategy {strategy} at {file_path}")
        return {}
    try:
        with open(file_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {str(e)}")
        return {}

def load_real_pnl(strategy: str) -> pd.DataFrame:
    """Load pnl_real.csv for a strategy."""
    file_path = BOT_DATA_DIR / strategy / "pnl_real.csv"
    if not file_path.exists():
        logger.warning(f"pnl_real.csv not found for strategy {strategy} at {file_path}")
        return pd.DataFrame()
    try:
        df = pd.read_csv(file_path)
        # Parse timestamp with mixed format
        df['ts'] = pd.to_datetime(df['ts'], format='mixed', utc=True).dt.tz_localize(None)
        logger.info(f"Loaded {len(df)} real PnL entries for strategy {strategy}")
        return df
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {str(e)}")
        return pd.DataFrame()

def calculate_position_return(position: Dict) -> float:
    """Calculate percentage return for a closed position.
    For long positions (entry_value > 0): 1 - (entry_value / exit_value)
    For short positions (entry_value < 0): -(1 - entry_value / exit_value)
    """
    entry_value = position['entry_value']
    exit_value = position['exit_value']
    if exit_value == 0:
        logger.warning(f"Zero exit value for position {position['asset']} at {position['exit_time']}")
        return float('inf')
    base_return = 1 - (entry_value / exit_value)
    # If entry_value is negative, this is a short position
    return -base_return if entry_value < 0 else base_return

def find_suspect_positions():
    """Find positions where calculated return differs from pnl_real by more than 0.01."""
    strategies = get_active_strategies()
    suspect_positions: Dict[str, List[Dict]] = {}
    summary = []
    total_usd_difference = 0.0

    for strategy in strategies:
        # Load data
        closed_positions = load_closed_positions(strategy)
        real_pnl_df = load_real_pnl(strategy)
        if not closed_positions or real_pnl_df.empty:
            logger.warning(f"Skipping strategy {strategy} due to missing data")
            continue

        strategy_suspects = []
        adjusted_pnl_df = real_pnl_df.copy()  # Copy for adjusted PnL
        # Process each closed position
        for exit_time_str, positions in closed_positions.items():
            try:
                exit_time = datetime.strptime(exit_time_str, '%Y/%m/%d %H:%M:%S.%f')
            except ValueError:
                logger.warning(f"Invalid timestamp format: {exit_time_str} in strategy {strategy}")
                continue

            for position in positions:
                if position['status'] != 'CLOSED':
                    continue
                asset = position['asset']
                calculated_return = calculate_position_return(position)

                # Find matching real_pnl.csv entry
                real_pnl_match = real_pnl_df[
                    (real_pnl_df['ts'].dt.floor('us') == exit_time) &
                    (real_pnl_df['coin'] == asset)
                ]
                if real_pnl_match.empty:
                    logger.warning(f"No matching real PnL entry for {asset} at {exit_time} in strategy {strategy}")
                    continue

                pnl_real = real_pnl_match.iloc[0]['pnl_real']
                difference = abs(calculated_return - pnl_real)
                if difference > 0.003:
                    # Calculate USD difference
                    entry_value = position['entry_value']
                    usd_difference = entry_value * difference
                    total_usd_difference += usd_difference
                    # Convert Timestamp to string in real_pnl_entry
                    real_pnl_entry = real_pnl_match.iloc[0].to_dict()
                    real_pnl_entry['ts'] = real_pnl_entry['ts'].strftime('%Y-%m-%d %H:%M:%S.%f')
                    suspect_entry = {
                        'strategy': strategy,
                        'asset': asset,
                        'exit_time': exit_time_str,
                        'calculated_return': calculated_return,
                        'pnl_real': pnl_real,
                        'difference': difference,
                        'usd_difference': usd_difference,
                        'position_details': position,
                        'real_pnl_entry': real_pnl_entry
                    }
                    strategy_suspects.append(suspect_entry)
                    summary.append(suspect_entry)
                    logger.info(f"Flagged suspect position in {strategy}: {asset} at {exit_time}, "
                                f"calculated_return={calculated_return:.6f}, pnl_real={pnl_real:.6f}, "
                                f"difference={difference:.6f}, usd_difference={usd_difference:.2f}")

                    # Adjust pnl_real in adjusted_pnl_df
                    match_index = real_pnl_match.index[0]
                    amount_in = real_pnl_match.iloc[0]['amount_in']
                    if abs(amount_in) > 0:
                        pnl_adjustment = usd_difference / abs(amount_in)
                        adjusted_pnl_df.loc[match_index, 'pnl_real'] = (
                            adjusted_pnl_df.loc[match_index, 'pnl_real'] + pnl_adjustment
                        )
                    else:
                        logger.warning(f"Zero amount_in for {asset} at {exit_time} in strategy {strategy}")

        if strategy_suspects:
            suspect_positions[strategy] = strategy_suspects
            # Write strategy-specific suspect positions
            output_file = OUTPUT_DIR / f"suspect_positions_{strategy}.json"
            with open(output_file, 'w') as f:
                json.dump(strategy_suspects, f, indent=4)
            logger.info(f"Wrote suspect positions for {strategy} to {output_file}")

            # Write adjusted pnl_real.csv
            adjusted_output_file = OUTPUT_DIR / f"adjusted_pnl_real_{strategy}.csv"
            adjusted_pnl_df.to_csv(adjusted_output_file, index=False)
            logger.info(f"Wrote adjusted real PnL for {strategy} to {adjusted_output_file}")

    # Write summary file with total USD difference
    if summary:
        summary_file = OUTPUT_DIR / "suspect_positions_summary.json"
        summary_output = {
            'total_usd_difference': total_usd_difference,
            'suspect_positions': summary
        }
        with open(summary_file, 'w') as f:
            json.dump(summary_output, f, indent=4)
        logger.info(f"Wrote summary of suspect positions with total USD difference {total_usd_difference:.2f} to {summary_file}")
    else:
        logger.info("No suspect positions found across all strategies")

if __name__ == "__main__":
    find_suspect_positions()