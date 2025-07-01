import json
import sys
from pathlib import Path
from datetime import datetime
import logging
import argparse
from typing import Dict, List, Any
import pandas as pd

# Import paths
from common.paths import CONFIG_FILE, OUTPUT_DIR, BOT_DATA_DIR

# Configure logging
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / 'calculate_real_pnl.log'),
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

def get_active_strategies(account: str, config: Dict) -> List[str]:
    """Return active strategies for the specified account."""
    active_strategies = [
        strat_name for strat_name, strat_config in config.get('strategy', {}).items()
        if strat_config.get('active', False) and
        strat_config.get('account_trade', '') == account and
        strat_config.get('exchange_trade', 'bitget') == 'bitget'
    ]
    logger.info(f"Active strategies for account {account}: {active_strategies}")
    return active_strategies

def load_real_pnl(strategy: str) -> pd.DataFrame:
    """Load pnl_real.csv for a strategy and compute USD PnL."""
    file_path = BOT_DATA_DIR / strategy / "pnl_real.csv"
    if not file_path.exists():
        logger.warning(f"pnl_real.csv not found for strategy {strategy} at {file_path}")
        return pd.DataFrame()
    
    try:
        # Read the first few lines for debugging
        with open(file_path, 'r') as f:
            preview = [next(f).strip() for _ in range(min(3, sum(1 for _ in open(file_path))))]
        logger.info(f"Preview of {file_path}:\n" + "\n".join(preview))

        # Try loading with flexible timestamp parsing
        df = pd.read_csv(file_path)
        
        # Check for possible timestamp column names
        possible_ts_columns = ['ts', 'timestamp', 'time']
        ts_column = next((col for col in possible_ts_columns if col in df.columns), None)
        if ts_column is None:
            logger.error(f"No timestamp column found in {file_path}. Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        # Parse timestamps with mixed format
        df[ts_column] = pd.to_datetime(df[ts_column], format='mixed', utc=True).dt.tz_localize(None)
        df = df.rename(columns={ts_column: 'ts'})
        
        # Verify required columns
        required_columns = ['pnl_real', 'amount_in']
        if not all(col in df.columns for col in required_columns):
            logger.error(f"Missing required columns in {file_path}. Available columns: {list(df.columns)}")
            return pd.DataFrame()
        
        # Calculate USD PnL
        df['usd_pnl'] = df['pnl_real'] * abs(df['amount_in'])
        df = df.sort_values('ts')
        df['cumulative_usd_pnl'] = df['usd_pnl'].cumsum()
        
        logger.info(f"Loaded {len(df)} real PnL entries for strategy {strategy}")
        return df
    except Exception as e:
        logger.error(f"Failed to load {file_path}: {str(e)}")
        return pd.DataFrame()

def compute_real_pnl_timeline(account: str, exchange: str = "bitget") -> Dict[str, Any]:
    """Compute real USD PnL timeline for each strategy and account."""
    config = load_config()
    strategies = get_active_strategies(account, config)
    timeline: Dict[str, Any] = {}

    # Collect all timestamps across strategies
    all_timestamps = set()
    strategy_dfs = {}
    for strategy in strategies:
        df = load_real_pnl(strategy)
        if not df.empty:
            strategy_dfs[strategy] = df
            all_timestamps.update(df['ts'].dt.strftime("%Y/%m/%d %H:%M:%S.%f"))
    logger.info(f"Found {len(all_timestamps)} unique timestamps for account {account}")

    if not all_timestamps:
        logger.error(f"No real USD PnL data found for account {account}")
        return timeline

    # Strategy-level timelines
    for strategy in strategies:
        if strategy not in strategy_dfs:
            logger.warning(f"No real USD PnL data for strategy {strategy}, skipping")
            continue
        df = strategy_dfs[strategy]
        strategy_timeline = {}
        
        for ts_str in sorted(all_timestamps):
            try:
                ts = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
            except ValueError:
                logger.warning(f"Invalid timestamp format: {ts_str}")
                continue
            
            # Find the most recent PnL entry at or before ts
            prior_entries = df[df['ts'] <= ts]
            if prior_entries.empty:
                cumulative_usd_pnl = 0.0
            else:
                cumulative_usd_pnl = prior_entries['cumulative_usd_pnl'].iloc[-1]
            
            strategy_timeline[ts_str] = {
                "cumulative_usd_pnl": cumulative_usd_pnl
            }
        
        timeline[strategy] = strategy_timeline
        logger.info(f"Calculated USD PnL timeline for strategy {strategy} with {len(strategy_timeline)} entries")

    # Account-level timeline
    account_timeline = {}
    for ts_str in sorted(all_timestamps):
        try:
            ts = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
        except ValueError:
            continue
        
        total_cumulative_usd_pnl = 0.0
        for strategy in strategies:
            if strategy in strategy_dfs:
                df = strategy_dfs[strategy]
                prior_entries = df[df['ts'] <= ts]
                if not prior_entries.empty:
                    total_cumulative_usd_pnl += prior_entries['cumulative_usd_pnl'].iloc[-1]
        
        account_timeline[ts_str] = {
            "cumulative_usd_pnl": total_cumulative_usd_pnl
        }
    
    timeline[f"account_{account}"] = account_timeline
    logger.info(f"Calculated USD PnL timeline for account {account} with {len(account_timeline)} entries")
    
    return timeline

def main():
    """Main function to compute real USD PnL timelines."""
    parser = argparse.ArgumentParser(description="Compute real USD PnL timelines for Bitget account.")
    parser.add_argument("--account", default="2", help="Bitget account identifier (e.g., '2', 'H1')")
    args = parser.parse_args()

    try:
        # Compute real USD PnL timelines
        logger.info(f"Calculating real USD PnL for account {args.account} on bitget...")
        real_timeline = compute_real_pnl_timeline(args.account, exchange="bitget")
        if not real_timeline:
            logger.error(f"No real USD PnL data processed for account {args.account}")
            sys.exit(1)

        # Save real USD PnL timelines
        for key, data in real_timeline.items():
            out_file = OUTPUT_DIR / f"bot_real_pnl_{key}.json"
            with open(out_file, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Wrote real USD PnL timeline for {key} to {out_file}")

    except Exception as e:
        logger.error(f"Failed to process real USD PnL: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()