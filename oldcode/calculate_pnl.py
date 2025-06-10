#!/usr/bin/env python3
"""
calculate_pnl.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For each portfolio snapshot, compute per-position current price,
current value, unrealized PnL, position value ratio to AUM and AUM imbalance ratio,
plus portfolio realized PnL, unrealized PnL timeline,
and portfolio return percentage including realized gains.
Writes separate PnL timelines per strategy (pnl_timeline_<strategy>.json)
and account portfolio (pnl_timeline_account_<account>.json).
Missing prices are null in JSON. Tiny tokens scaled per-unit.
"""

import json
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
import math
import time

import pandas as pd
import numpy as np

# ensure project root on path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from datafeed.utils_online import extract_coin

# --- CONFIG ---
MODULE_DIR      = Path(__file__).resolve().parent
DATA_DIR        = MODULE_DIR / "binance_data"
PRICE_MATRIX    = DATA_DIR / "binance_open_15m.csv"
PORTFOLIO_DIR   = MODULE_DIR / "output_bin"
CONFIG_FILE     = MODULE_DIR / "config_test.json"
SIGNAL_FILE     = MODULE_DIR / "signals.csv"

# closed positions directory and filename pattern
CLOSED_PATTERN = "closed_positions_{strategy}.json"

# tokens with price columns representing 1000 units
TINY = {"SHIB", "PEPE", "SATS", "LUNC", "XEC", "BONK", "FLOKI", "CAT", "RATS", "WHY", "X"}


def map_to_display_ticker(ticker: str) -> str:
    coin = extract_coin(ticker)
    return f"1000{ticker}" if coin in TINY else ticker


def load_price_matrix() -> pd.DataFrame:
    if not PRICE_MATRIX.exists():
        raise FileNotFoundError(f"No price matrix found at {PRICE_MATRIX}")
    df = pd.read_csv(PRICE_MATRIX, parse_dates=["timestamp"]).set_index("timestamp")
    df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
    return df


def load_config() -> Dict[str, Any]:
    if not CONFIG_FILE.exists():
        raise FileNotFoundError(f"Config file not found at {CONFIG_FILE}")
    with open(CONFIG_FILE) as f:
        return json.load(f)


def load_portfolios() -> Dict[str, Dict[str, Any]]:
    cfg = load_config()
    portfolios: Dict[str, Dict[str, Any]] = {}
    for strat in cfg.get("strategy", {}):
        path = PORTFOLIO_DIR / f"portfolio_{strat}.json"
        if path.exists():
            portfolios[strat] = json.loads(path.read_text())
        else:
            print(f"Warning: portfolio file for strategy '{strat}' not found at {path}, skipping.")
    return portfolios


def load_account_portfolios() -> Dict[str, Dict[str, Any]]:
    cfg = load_config()
    accounts = set()
    for strat, strat_cfg in cfg.get("strategy", {}).items():
        if strat_cfg.get("active", False):
            accounts.add(strat_cfg.get("account_trade", "default"))
    account_portfolios: Dict[str, Dict[str, Any]] = {}
    for account in accounts:
        path = PORTFOLIO_DIR / f"portfolio_{account}.json"
        if path.exists():
            account_portfolios[account] = json.loads(path.read_text())
        else:
            print(f"Warning: account portfolio file for '{account}' not found at {path}, skipping.")
    return account_portfolios


def load_closed_positions() -> Dict[str, List[Tuple[datetime, float]]]:
    cfg = load_config()
    closed_map: Dict[str, List[Tuple[datetime, float]]] = {}
    for strat in cfg.get("strategy", {}):
        fname = CLOSED_PATTERN.format(strategy=strat)
        path = PORTFOLIO_DIR / fname
        entries: List[Tuple[datetime, float]] = []
        if path.exists():
            data = json.loads(path.read_text())
            for ts_str, positions in data.items():
                for pos in positions:
                    pnl = pos.get("realized_pnl")
                    exit_ts = pos.get("exit_time") or ts_str
                    try:
                        dt = datetime.strptime(exit_ts, "%Y/%m/%d %H:%M:%S.%f")
                        entries.append((dt, pnl or 0.0))
                    except Exception:
                        continue
        closed_map[strat] = sorted(entries, key=lambda x: x[0])
    return closed_map


def load_strategy_pnl_timelines() -> Dict[str, Dict[str, Any]]:
    pnl_timelines: Dict[str, Dict[str, Any]] = {}
    for file in PORTFOLIO_DIR.glob("pnl_timeline_*.json"):
        strat = file.name.replace("pnl_timeline_", "").replace(".json", "")
        if not strat.startswith("account_"):
            pnl_timelines[strat] = json.loads(file.read_text())
    return pnl_timelines


def compute_unrealized_timeline(
    price_matrix: pd.DataFrame,
    portfolios: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    cfg = load_config()
    closed_map = load_closed_positions()
    timeline: Dict[str, Any] = {}

    for strategy, snaps in portfolios.items():
        seen_assets = set()
        strat_cfg = cfg["strategy"].get(strategy, {})
        alloc = strat_cfg.get("allocation", 1.0)
        lev   = strat_cfg.get("leverage", 1.0)
        denom = alloc * lev

        realized_list = closed_map.get(strategy, [])
        run_realized = 0.0
        idx_real = 0
        n_real = len(realized_list)

        for ts_str, snap in snaps.items():
            entry_time = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
            while idx_real < n_real and realized_list[idx_real][0] <= entry_time:
                run_realized += realized_list[idx_real][1]
                idx_real += 1

            key = entry_time.isoformat(timespec='microseconds')
            positions = snap.get("portfolio", [])
            enriched = []
            total_unreal = 0.0
            has_missing_price = False

            for pos in positions:
                asset = pos.get("asset")
                qty   = pos.get("qty", 0.0)
                entry_price = pos.get("entry_price", 0.0)
                display = map_to_display_ticker(asset)

                if asset not in seen_assets:
                    current_price: Optional[float] = entry_price
                else:
                    current_price = None
                    if display in price_matrix.columns:
                        deltas = abs(price_matrix.index - entry_time)
                        idx = deltas.argmin()
                        if deltas[idx] <= timedelta(minutes=30):
                            current_price = float(price_matrix.iloc[idx][display])
                            if pd.isna(current_price):
                                current_price = None
                seen_assets.add(asset)

                if current_price is None:
                    has_missing_price = True

                coin = extract_coin(asset)
                if current_price is not None and coin in TINY:
                    current_price /= 1000.0

                current_value = None
                unreal_pnl = None
                if current_price is not None:
                    current_value = current_price * qty
                    unreal_pnl = current_value - (qty * entry_price)
                    total_unreal += unreal_pnl

                value_ratio = None
                if current_value is not None:
                    value_ratio = current_value / denom

                enriched.append({
                    "asset": asset,
                    "qty": qty,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "current_value": current_value,
                    "unrealized_pnl": unreal_pnl,
                    "value_ratio_to_aum": value_ratio
                })

            portfolio_unbalance = None
            valid_ratios = [p["value_ratio_to_aum"] for p in enriched if p.get("value_ratio_to_aum") is not None]
            if valid_ratios:
                portfolio_unbalance = sum(valid_ratios)

            portfolio_realized = run_realized
            portfolio_unrealized_pnl = total_unreal if not has_missing_price else None
            portfolio_total_pnl = (portfolio_realized + total_unreal) if not has_missing_price else None
            portfolio_return_pct = None if denom == 0 or has_missing_price else portfolio_total_pnl / denom

            if has_missing_price:
                portfolio_unrealized_pnl = None
                portfolio_total_pnl = None
                portfolio_return_pct = None
                portfolio_unbalance = None

            timeline.setdefault(strategy, {})[key] = {
                "positions": enriched,
                "portfolio_realized_pnl": portfolio_realized,
                "portfolio_unrealized_pnl": portfolio_unrealized_pnl,
                "portfolio_total_pnl": portfolio_total_pnl,
                "portfolio_return_pct": portfolio_return_pct,
                "portfolio_unbalance": portfolio_unbalance
            }

    return timeline


def parse_timestamp(ts_str: str) -> datetime:
    """
    Parse a timestamp string, handling both with and without microseconds.
    """
    try:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S.%f")
    except ValueError:
        return datetime.strptime(ts_str, "%Y-%m-%dT%H:%M:%S")


def compute_account_pnl_timeline(
    price_matrix: pd.DataFrame,
    account_portfolio: Dict[str, Any],
    strategy_pnl_timelines: Dict[str, Dict[str, Any]],
    cfg: Dict[str, Any]
) -> Dict[str, Any]:
    timeline: Dict[str, Any] = {}
    account = next(iter(account_portfolio.values())).get("account") if account_portfolio else "unknown"

    # Calculate total AUM as sum of allocation * leverage for all active strategies under this account
    total_aum = 0.0
    for strat, strat_cfg in cfg.get("strategy", {}).items():
        if strat_cfg.get("account_trade") == account and strat_cfg.get("active", False):
            alloc = float(strat_cfg.get("allocation", 1.0))
            lev = float(strat_cfg.get("leverage", 1.0))
            total_aum += alloc * lev

    # Prepare cumulative realized PnL per strategy up to each timestamp
    cum_realized_per_strat: Dict[str, List[Tuple[datetime, float]]] = {}
    # Prepare latest unrealized PnL per strategy up to each timestamp
    cum_unrealized_per_strat: Dict[str, List[Tuple[datetime, float]]] = {}
    for strat, pnl_data in strategy_pnl_timelines.items():
        if cfg["strategy"].get(strat, {}).get("account_trade") != account or not cfg["strategy"].get(strat, {}).get("active", False):
            continue
        # Realized PnL
        cum_real = 0.0
        real_entries = []
        # Unrealized PnL
        cum_unreal = 0.0
        unreal_entries = []
        for ts_str, snap in sorted(pnl_data.items(), key=lambda x: parse_timestamp(x[0])):
            entry_time = parse_timestamp(ts_str)
            real_pnl = snap.get("portfolio_realized_pnl")
            unreal_pnl = snap.get("portfolio_unrealized_pnl")
            if real_pnl is not None:
                cum_real = real_pnl  # Already cumulative
            real_entries.append((entry_time, cum_real))
            if unreal_pnl is not None:
                cum_unreal = unreal_pnl  # Use the latest unrealized PnL
            unreal_entries.append((entry_time, cum_unreal))
        cum_realized_per_strat[strat] = real_entries
        cum_unrealized_per_strat[strat] = unreal_entries

    # Process each timestamp in the account portfolio
    for ts_str, snap in account_portfolio.items():
        entry_time = datetime.strptime(ts_str, "%Y/%m/%d %H:%M:%S.%f")
        key = entry_time.isoformat(timespec='microseconds')
        positions = snap.get("portfolio", [])
        enriched = []
        total_position_value = 0.0
        has_missing_price = False

        # Fetch prices and calculate position values
        for pos in positions:
            asset = pos.get("asset")
            qty = pos.get("qty", 0.0)
            display = map_to_display_ticker(asset)

            current_price = None
            if display in price_matrix.columns:
                deltas = abs(price_matrix.index - entry_time)
                idx = deltas.argmin()
                if deltas[idx] <= timedelta(minutes=30):
                    current_price = float(price_matrix.iloc[idx][display])
                    if pd.isna(current_price):
                        current_price = None
            if current_price is None:
                has_missing_price = True

            coin = extract_coin(asset)
            if current_price is not None and coin in TINY:
                current_price /= 1000.0

            current_value = current_price * qty if current_price is not None else None
            value_ratio = current_value / total_aum if current_value is not None and total_aum > 0 else None

            if current_value is not None:
                total_position_value += current_value

            enriched.append({
                "asset": asset,
                "qty": qty,
                "current_price": current_price,
                "current_value": current_value,
                "value_ratio_to_aum": value_ratio
            })

        # Portfolio unbalance (sum of value ratios)
        portfolio_unbalance = None
        valid_ratios = [p["value_ratio_to_aum"] for p in enriched if p.get("value_ratio_to_aum") is not None]
        if valid_ratios:
            portfolio_unbalance = sum(valid_ratios)

        # Calculate realized PnL as sum of latest realized PnLs from strategies
        portfolio_realized = 0.0
        for strat, entries in cum_realized_per_strat.items():
            cum_real = 0.0
            for ts, val in entries:
                if ts <= entry_time:
                    cum_real = val
                else:
                    break
            portfolio_realized += cum_real

        # Calculate unrealized PnL as sum of latest unrealized PnLs from strategies
        portfolio_unrealized = 0.0
        for strat, entries in cum_unrealized_per_strat.items():
            cum_unreal = 0.0
            for ts, val in entries:
                if ts <= entry_time:
                    cum_unreal = val
                else:
                    break
            portfolio_unrealized += cum_unreal

        # Calculate total PnL as sum of realized and unrealized
        portfolio_usd_pnl = portfolio_realized + portfolio_unrealized

        # Calculate return percentage
        portfolio_return_pct = None if total_aum == 0 else portfolio_usd_pnl / total_aum

        # Set to None if missing price data
        if has_missing_price:
            portfolio_unrealized = None
            portfolio_usd_pnl = None
            portfolio_return_pct = None
            portfolio_unbalance = None

        timeline[key] = {
            "positions": enriched,
            "portfolio_realized_pnl": portfolio_realized,
            "portfolio_unrealized_pnl": portfolio_unrealized,
            "portfolio_usd_pnl": portfolio_usd_pnl,
            "portfolio_return_pct": portfolio_return_pct,
            "portfolio_unbalance": portfolio_unbalance
        }

    return timeline


def sanitize(obj: Any) -> Any:
    """
    Convert NaN values to None for JSON serialization as null.
    Recursively processes dictionaries, lists, and floats.
    """
    if isinstance(obj, dict):
        return {k: sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [sanitize(v) for v in obj]
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    return obj


def wait_for_portfolios(portfolio_dir: Path, strategies: List[str], accounts: List[str], timeout: int = 60):
    start_time = time.time()
    while time.time() - start_time < timeout:
        all_exist = all((portfolio_dir / f"portfolio_{strat}.json").exists() for strat in strategies) and \
                    all((portfolio_dir / f"portfolio_{account}.json").exists() for account in accounts)
        if all_exist:
            return True
        time.sleep(1)
    print(f"Warning: Timed out waiting for portfolio files after {timeout} seconds.")
    return False


if __name__ == "__main__":
    cfg = load_config()
    strategies = [strat for strat, conf in cfg.get("strategy", {}).items() if conf.get("active", False)]
    accounts = list(set(conf.get("account_trade", "default") for conf in cfg.get("strategy", {}).values() if conf.get("active", False)))

    print("Adding active strategies:")
    for strat in strategies:
        print(f"Adding strategy: {strat}")

    print("Processing signals for all strategies:")
    if not wait_for_portfolios(PORTFOLIO_DIR, strategies, accounts):
        sys.exit(1)

    print("Building portfolios for all accounts:")
    for account in accounts:
        print(f"Building portfolio for account: {account}")

    print("Calculating unrealized PnL for all strategies...")
    price_matrix = load_price_matrix()
    portfolios = load_portfolios()
    result = compute_unrealized_timeline(price_matrix, portfolios)
    safe = sanitize(result)
    os.makedirs(PORTFOLIO_DIR, exist_ok=True)

    for strat, data in safe.items():
        out_file = PORTFOLIO_DIR / f"pnl_timeline_{strat}.json"
        with open(out_file, "w") as f:
            json.dump(data, f, indent=2)
        print(f"Wrote PnL timeline for {strat} to {out_file}")

    account_portfolios = load_account_portfolios()
    strategy_pnl_timelines = load_strategy_pnl_timelines()

    for account, account_portfolio in account_portfolios.items():
        if account_portfolio:
            account_result = compute_account_pnl_timeline(price_matrix, account_portfolio, strategy_pnl_timelines, cfg)
            safe_account = sanitize(account_result)
            out_file = PORTFOLIO_DIR / f"pnl_timeline_account_{account}.json"
            with open(out_file, "w") as f:
                json.dump(safe_account, f, indent=2)
            print(f"Wrote PnL timeline for account '{account}' to {out_file}")