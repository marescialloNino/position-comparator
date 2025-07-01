#!/usr/bin/env python3
"""
strategies.py
~~~~~~~~~~~~~
Manages trading strategies and aggregates account-level portfolios.
Handles current_state.log parsing and portfolio snapshot generation.
"""

from typing import Dict, List, Optional
from dataclasses import dataclass
import json
import os
import re
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path

from common.config_manager import ConfigManager

from common.paths import BOT_DATA_DIR, CONFIG_FILE, PRICES_DIR, OUTPUT_DIR

@dataclass
class PortfolioEntry:
    asset: str
    qty: float  # Number of tokens
    entry_price: float
    entry_time: str  # Entry timestamp
    usd_value_at_entry: float  # qty * entry_price
    current_price: float  # Price at current timestamp
    current_value: float  # qty * current_price
    unrealized_pnl: float  # current_value - usd_value_at_entry

class Strategy:
    def __init__(self, name: str, family: str, config_manager: ConfigManager):
        self.name = name
        self.family = family
        self.config_manager = config_manager
        self.config = config_manager.get_strategy_config(name)
        if not self.config:
            raise ValueError(f"No config found for strategy {name}")
        print(f"Initializing strategy {name} with config: {self.config}")
        self.portfolio: Dict[str, Dict] = {}  # asset -> {qty, entry_price, entry_time, usd_value_at_entry}
        self.first_timestamp: str = None
        self.closed_positions: Dict[str, List[Dict]] = {}  # exit_time -> list of closed positions
        self.portfolio_snapshots: Dict[str, Dict] = {}  # Store snapshots for aggregation

    def get_aum_and_max_positions(self) -> tuple[float, int]:
        """Calculate strategy AUM and maximum number of positions."""
        account = self.config.get('account_trade')
        exchange = self.config.get('exchange_trade', 'bitget')
        aum, max_positions = self.config_manager.get_aum_and_max_positions(self.name, account, exchange)
        print(f"Strategy {self.name}: AUM={aum}, Max Positions={max_positions}")
        return aum, max_positions

    def get_price_at_time(self, price_matrix: pd.DataFrame, asset: str, time: datetime) -> Optional[float]:
        """Fetch the price of an asset at a given time from the price matrix."""
        if asset not in price_matrix.columns:
            print(f"Warning: Asset {asset} not found in price matrix for strategy {self.name}")
            return None
        deltas = abs(price_matrix.index - time)
        idx = deltas.argmin()
        if deltas[idx] <= timedelta(minutes=120):  # Increased tolerance to 120 minutes
            price = float(price_matrix.iloc[idx][asset])
            if pd.isna(price):
                print(f"Warning: NaN price for {asset} at {time} in strategy {self.name}")
                return None
            print(f"Price for {asset} at {time}: {price}")
            return price
        print(f"Warning: No price within 120 minutes of {time} for {asset} in strategy {self.name}")
        return None

    def process_current_state_log(self, price_matrix: pd.DataFrame, output_file: str, closed_file: str):
        """Process current_state.log to generate portfolio and closed positions JSON files."""
        file_path = BOT_DATA_DIR / self.name / "current_state.log"
        if not file_path.exists():
            print(f"Warning: current_state.log not found for strategy '{self.name}' at {file_path}")
            return

        # Calculate AUM and max positions
        aum, max_positions = self.get_aum_and_max_positions()
        if aum <= 0 or max_positions <= 0:
            print(f"Warning: Invalid AUM ({aum}) or max positions ({max_positions}) for strategy {self.name}")
            return
        notional_per_position = aum / max_positions  # Absolute value per position
        print(f"Strategy {self.name}: Notional per position={notional_per_position}")

        # Parse the log file
        timestamps = []
        portfolio_states = {}
        timestamp_pattern = re.compile(r'^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)')
        timestamp_formats = [
            "%Y/%m/%d %H:%M:%S.%f",
            "%Y/%m/%d %H:%M:%S"  # Fallback without microseconds
        ]

        with open(file_path, 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line or line == "ts;positions":
                    continue  # Skip empty lines or header
                
                try:
                    # Split on semicolon to separate timestamp and positions
                    parts = line.split(';', 1)
                    if len(parts) != 2:
                        raise ValueError(f"Invalid line format, expected 'ts;positions' in line {i+1}")
                    timestamp_str, pos_str = parts
                    
                    # Validate timestamp using regex
                    timestamp_match = timestamp_pattern.match(timestamp_str)
                    if not timestamp_match:
                        raise ValueError(f"Invalid timestamp format in line {i+1}: {timestamp_str}")
                    timestamp_str = timestamp_match.group(1)
                    
                    # Parse timestamp
                    timestamp = None
                    for fmt in timestamp_formats:
                        try:
                            timestamp = datetime.strptime(timestamp_str, fmt)
                            break
                        except ValueError:
                            continue
                    if timestamp is None:
                        raise ValueError(f"Cannot parse timestamp: {timestamp_str}")
                    
                    # Parse position string
                    positions = []
                    if not pos_str:
                        print(f"Warning: Empty position string at line {i+1} in {file_path}")
                        continue
                    items = pos_str.split(',')
                    for item in items:
                        item = item.strip()
                        if not item:
                            continue
                        asset_qty = item.rsplit(':', 1)
                        if len(asset_qty) != 2:
                            print(f"Warning: Invalid asset-quantity pair in line {i+1}: {item}")
                            continue
                        asset, direction = asset_qty
                        asset = asset.strip("'").strip('"')
                        try:
                            direction = int(float(direction.strip()))  # Convert to int, expecting 1 or -1
                            if direction not in [1, -1]:
                                print(f"Warning: Invalid direction {direction} for {asset} in {file_path}")
                            positions.append((asset, direction))
                        except ValueError:
                            print(f"Warning: Invalid quantity in line {i+1}: {direction}")
                            continue
                    if positions:
                        timestamps.append(timestamp)
                        portfolio_states[timestamp] = positions
                        print(f"Strategy {self.name}: Parsed {len(positions)} positions at {timestamp}")
                    else:
                        print(f"Warning: No valid positions parsed at line {i+1} in {file_path}")
                except Exception as e:
                    print(f"Warning: Failed to parse line in {file_path}: {line}. Error: {e}")
                    continue

        if not timestamps:
            print(f"Warning: No valid entries found in {file_path} for strategy {self.name}")
            return

        print(f"Strategy {self.name}: Parsed {len(timestamps)} timestamps")
        timestamps.sort()
        current_positions = {}  # {asset: {'qty': float, 'entry_time': datetime, 'entry_price': float, 'usd_value_at_entry'}}
        self.portfolio_snapshots = {}

        for i, ts in enumerate(timestamps):
            prev_state = portfolio_states[timestamps[i-1]] if i > 0 else []
            current_state = portfolio_states[ts]

            # Convert prev_state and current_state to dict for comparison
            prev_positions = {asset: direction for asset, direction in prev_state}
            current_positions_dict = {asset: direction for asset, direction in current_state}

            # Detect closed positions
            for asset in list(current_positions.keys()):
                if asset not in current_positions_dict:
                    pos = current_positions[asset]
                    exit_time = ts
                    exit_price = self.get_price_at_time(price_matrix, asset, exit_time)
                    if exit_price is not None:
                        entry_price = pos['entry_price']
                        qty = pos['qty']
                        realized_pnl = qty * (exit_price - entry_price)
                        closed_entry = {
                            'asset': asset,
                            'entry_time': pos['entry_time'].strftime('%Y/%m/%d %H:%M:%S.%f'),
                            'entry_price': entry_price,
                            'entry_value': qty * entry_price,
                            'exit_time': exit_time.strftime('%Y/%m/%d %H:%M:%S.%f'),
                            'exit_value': qty * exit_price,
                            'realized_pnl': realized_pnl,
                            'lifetime': (exit_time - pos['entry_time']).total_seconds(),
                            'status': 'CLOSED'
                        }
                        self.closed_positions.setdefault(exit_time.strftime('%Y/%m/%d %H:%M:%S.%f'), []).append(closed_entry)
                    del current_positions[asset]

            # Update current positions with new positions
            for asset, direction in current_state:
                if asset not in current_positions:
                    entry_time = ts
                    entry_price = self.get_price_at_time(price_matrix, asset, entry_time)
                    if entry_price is not None and entry_price > 0:
                        qty = (notional_per_position / entry_price) * direction  # direction is 1 or -1
                        usd_value_at_entry = qty * entry_price
                        current_positions[asset] = {
                            'qty': qty,
                            'entry_time': entry_time,
                            'entry_price': entry_price,
                            'usd_value_at_entry': usd_value_at_entry
                        }
                    else:
                        print(f"Warning: Skipping position for {asset} at {ts} due to invalid entry price")

            # Record portfolio snapshot
            snapshot = []
            total_unrealized_pnl = 0.0
            for asset, pos in current_positions.items():
                current_price = self.get_price_at_time(price_matrix, asset, ts)
                if current_price is None:
                    print(f"Warning: Skipping snapshot for {asset} at {ts} due to missing current price")
                    continue
                current_value = pos['qty'] * current_price
                unrealized_pnl = current_value - pos['usd_value_at_entry']
                total_unrealized_pnl += unrealized_pnl
                snapshot.append({
                    'asset': asset,
                    'qty': pos['qty'],
                    'entry_price': pos['entry_price'],
                    'entry_time': pos['entry_time'].strftime('%Y/%m/%d %H:%M:%S.%f'),
                    'usd_value_at_entry': pos['usd_value_at_entry'],
                    'current_price': current_price,
                    'current_value': current_value,
                    'unrealized_pnl': unrealized_pnl
                })
            
            if snapshot:
                self.portfolio_snapshots[ts.strftime('%Y/%m/%d %H:%M:%S.%f')] = {
                    'strategy': self.name,
                    'portfolio': snapshot,
                    'portfolio_unbalance_usd': total_unrealized_pnl
                }
                print(f"Strategy {self.name}: Added snapshot at {ts} with {len(snapshot)} positions")

        # Record remaining open positions at the last timestamp
        if current_positions and timestamps:
            last_ts = timestamps[-1].strftime('%Y/%m/%d %H:%M:%S.%f')
            if last_ts not in self.closed_positions:
                self.closed_positions[last_ts] = []
            for asset, pos in current_positions.items():
                entry = {
                    'asset': asset,
                    'entry_time': pos['entry_time'].strftime('%Y/%m/%d %H:%M:%S.%f'),
                    'entry_price': pos['entry_price'],
                    'entry_value': pos['usd_value_at_entry'],
                    'status': 'OPEN'
                }
                self.closed_positions[last_ts].append(entry)

        print(f"Strategy {self.name}: Generated {len(self.portfolio_snapshots)} snapshots")
        if not self.portfolio_snapshots:
            print(f"Warning: No snapshots generated for strategy {self.name}")

        # Write output files
        output_path = OUTPUT_DIR / Path(output_file).name
        closed_path = OUTPUT_DIR / Path(closed_file).name
        with open(output_path, 'w') as f:
            json.dump(self.portfolio_snapshots, f, indent=4)
        with open(closed_path, 'w') as f:
            json.dump(self.closed_positions, f, indent=4)
        print(f"Generated {output_path} and {closed_path} for strategy {self.name}")

class StrategyManager:
    def __init__(self, config_file: str = CONFIG_FILE):
        self.config_manager = ConfigManager(config_file)
        self.strategies: Dict[str, Strategy] = {}

    def add_strategy(self, name: str, family: str):
        self.strategies[name] = Strategy(name, family, self.config_manager)

    def load_price_matrix(self, exchange: str) -> pd.DataFrame:
        """Load the price matrix for a given exchange."""
        price_matrix_path = PRICES_DIR / exchange.lower() / "theoretical_open_15m.csv"
        if not price_matrix_path.exists():
            raise FileNotFoundError(f"No price matrix found at {price_matrix_path}")
        df = pd.read_csv(price_matrix_path, parse_dates=["timestamp"]).set_index("timestamp")
        df.index = pd.to_datetime(df.index, utc=True).tz_localize(None)
        return df

    def aggregate_account_portfolio(self, account: str, exchange: str, price_matrix: pd.DataFrame):
        """Aggregate positions across all strategies in the same account."""
        # Collect all timestamps and relevant strategies
        all_timestamps = set()
        relevant_strategies = []
        for strat in self.strategies.values():
            print(f"Checking strategy {strat.name}: active={strat.config.get('active', True)}, "
                  f"account={strat.config.get('account_trade')}, exchange={strat.config.get('exchange_trade')}, "
                  f"snapshots={len(strat.portfolio_snapshots)}")
            if (strat.config.get('active', True) and
                str(strat.config.get('account_trade')) == str(account) and
                strat.config.get('exchange_trade', 'bitget') == exchange):
                if not strat.portfolio_snapshots:
                    print(f"Warning: No portfolio snapshots for strategy {strat.name}")
                    continue
                relevant_strategies.append(strat)
                try:
                    timestamps = {
                        datetime.strptime(ts, '%Y/%m/%d %H:%M:%S.%f')
                        for ts in strat.portfolio_snapshots.keys()
                    }
                    all_timestamps.update(timestamps)
                    print(f"Found {len(timestamps)} snapshots for strategy {strat.name}")
                except ValueError as e:
                    print(f"Error parsing timestamps for strategy {strat.name}: {e}")
                    continue
            else:
                print(f"Strategy {strat.name} excluded: "
                      f"active={strat.config.get('active', True)}, "
                      f"account={strat.config.get('account_trade')} vs {account}, "
                      f"exchange={strat.config.get('exchange_trade')} vs {exchange}")
        
        if not all_timestamps or not relevant_strategies:
            print(f"No portfolio snapshots found for account {account} on exchange {exchange}. "
                  f"Relevant strategies: {len(relevant_strategies)}, Timestamps: {len(all_timestamps)}")
            return

        # Aggregate positions at each timestamp
        account_portfolio = {}
        for ts in sorted(all_timestamps):
            positions = {}  # {asset: quantity}
            # Track the most recent snapshot for each strategy
            for strat in relevant_strategies:
                # Find the most recent snapshot at or before ts
                strat_timestamps = sorted(
                    datetime.strptime(ts_str, '%Y/%m/%d %H:%M:%S.%f')
                    for ts_str in strat.portfolio_snapshots.keys()
                    if datetime.strptime(ts_str , '%Y/%m/%d %H:%M:%S.%f') <= ts
                )
                if not strat_timestamps:
                    continue
                latest_ts = strat_timestamps[-1].strftime('%Y/%m/%d %H:%M:%S.%f')
                snapshot = strat.portfolio_snapshots.get(latest_ts, {}).get('portfolio', [])
                for pos in snapshot:
                    asset = pos['asset']
                    qty = pos['qty']
                    positions[asset] = positions.get(asset, 0.0) + qty
            
            # Build portfolio entry
            portfolio_entry = []
            for asset, quantity in positions.items():
                if abs(quantity) < 1e-8:  # Skip near-zero quantities
                    continue
                # Use Strategy.get_price_at_time for consistent price lookup
                current_price = relevant_strategies[0].get_price_at_time(price_matrix, asset, ts)
                if current_price is None:
                    print(f"Warning: No price for {asset} at {ts} for account {account}")
                    continue
                current_value = quantity * current_price
                portfolio_entry.append({
                    'assetname': asset,
                    'quantity': quantity,
                    'currentprice': current_price,
                    'currentvalue': current_value
                })
            
            if portfolio_entry:
                account_portfolio[ts.strftime('%Y/%m/%d %H:%M:%S.%f')] = portfolio_entry

        if not account_portfolio:
            print(f"No aggregated portfolio entries generated for account {account} on exchange {exchange}")
            return

        # Write output file
        output_path = OUTPUT_DIR / f"account_{account}_portfolio.json"
        with open(output_path, 'w') as f:
            json.dump(account_portfolio, f, indent=4)
        print(f"Generated {output_path} for account {account} on exchange {exchange}")

    def process_all_current_state_logs(self):
        """Process current_state.log for all active strategies and aggregate account portfolios."""
        print("Processing current_state.log for all strategies:")
        exchanges = set(
            strat.get('exchange_trade', 'bitget')
            for strat in self.config_manager.get_active_strategies()
        )
        price_matrices = {}
        for exchange in exchanges:
            try:
                price_matrices[exchange] = self.load_price_matrix(exchange)
            except FileNotFoundError as e:
                print(e)
                continue

        accounts = set()
        for strat in self.config_manager.get_active_strategies():
            strat_name = strat['name']
            exchange = strat.get('exchange_trade', 'bitget')
            account = strat.get('account_trade')
            family = strat.get('type', 'basketspread')
            if strat_name not in self.strategies:
                self.add_strategy(strat_name, family)
            accounts.add((account, exchange))
            price_matrix = price_matrices.get(exchange)
            if price_matrix is None:
                print(f"Warning: Price matrix not found for exchange '{exchange}', skipping strategy {strat_name}")
                continue
            output_file = f"portfolio_{strat_name}.json"
            closed_file = f"closed_positions_{strat_name}.json"
            print(f"Processing strategy: {strat_name}")
            self.strategies[strat_name].process_current_state_log(price_matrix, output_file, closed_file)

        # Aggregate portfolios for each account
        for account, exchange in accounts:
            price_matrix = price_matrices.get(exchange)
            if price_matrix is None:
                print(f"Warning: Price matrix not found for exchange '{exchange}', skipping account {account}")
                continue
            self.aggregate_account_portfolio(account, exchange, price_matrix)

if __name__ == "__main__":
    manager = StrategyManager()
    for strat in manager.config_manager.get_active_strategies():
        strat_name = strat['name']
        family = strat.get('type', 'basketspread')
        manager.add_strategy(strat_name, family)
    manager.process_all_current_state_logs()