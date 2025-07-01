import json
import os
from pathlib import Path
from datetime import datetime
import pandas as pd
from common.paths import CONFIG_FILE, BOT_DATA_DIR

def load_config() -> dict:
    """Load configuration file."""
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_FILE}")
    with open(CONFIG_FILE, 'r') as f:
        return json.load(f)

def aggregate_avg_pnl_by_type(strategy_type: str, start_time: datetime, end_time: datetime) -> dict:
    """
    Aggregate average PnL by trade for a given strategy type within a time window.
    
    Args:
        strategy_type (str): The type of strategy (e.g., 'basketspread').
        start_time (datetime): Start of the time window.
        end_time (datetime): End of the time window.
    
    Returns:
        dict: Contains 'avg_pnl_normal' and 'avg_pnl_wrt_account' for the strategy type.
    """
    cfg = load_config()
    strategies = [
        strat for strat, config in cfg.get('strategy', {}).items()
        if config.get('active', False) and config.get('type') == strategy_type
    ]
    if not strategies:
        print(f"No active strategies found for type {strategy_type}")
        return {"avg_pnl_normal": None, "avg_pnl_wrt_account": None}

    all_pnl_data = []
    for strategy in strategies:
        pnl_file = BOT_DATA_DIR / strategy / "pnl.csv"
        if not pnl_file.exists():
            print(f"Warning: PNL file not found for strategy {strategy} at {pnl_file}")
            continue
        
        try:
            # Load CSV and attempt to parse timestamps
            df = pd.read_csv(pnl_file)
            print(f"Raw ts sample for {strategy}: {df['ts'].head(1).iloc[0]}")  # Debug raw data
            df["ts"] = pd.to_datetime(df["ts"], format="%Y-%m-%d %H:%M:%S%z", errors='coerce')
            if df["ts"].isnull().all():
                print(f"Warning: All timestamps failed to parse for {strategy}. Trying ISO8601...")
                df["ts"] = pd.to_datetime(df["ts"], format="ISO8601", errors='coerce')
            # Convert to tz-naive for comparison
            df["ts"] = df["ts"].dt.tz_localize(None) if df["ts"].dt.tz else df["ts"]
            df = df[(df["ts"] >= start_time) & (df["ts"] <= end_time)]
            if not df.empty:
                all_pnl_data.append(df)
        except Exception as e:
            print(f"Error processing PNL file for {strategy}: {e}")
            continue

    if not all_pnl_data:
        print(f"No PNL data found for {strategy_type} in the time window")
        return {"avg_pnl_normal": None, "avg_pnl_wrt_account": None}

    combined_df = pd.concat(all_pnl_data, ignore_index=True)
    avg_pnl_normal = combined_df["pnl_theo"].mean() if not combined_df.empty else None
    avg_pnl_wrt_account = (combined_df["pnl_theo"] / combined_df["allocation"]).mean() if not combined_df.empty else None

    print(f"Aggregated for {strategy_type}: Normal avg PnL = {avg_pnl_normal}, Account-weighted avg PnL = {avg_pnl_wrt_account}")
    return {
        "avg_pnl_normal": avg_pnl_normal,
        "avg_pnl_wrt_account": avg_pnl_wrt_account
    }

if __name__ == "__main__":
    # Example usage with current date and time (June 24, 2025, 05:08 PM CEST)
    start = datetime(2025, 6, 1, 0, 0, 0)  
    end = datetime(2025, 6, 15, 17, 8, 0)  
    result = aggregate_avg_pnl_by_type("basketspread", start, end)
    print(result)