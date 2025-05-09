from typing import Dict, List
from dataclasses import dataclass
import csv
import json
import os

@dataclass
class PortfolioEntry:
    asset: str
    qty: float  # Positive for long, negative for short
    entry_price: float

class Strategy:
    def __init__(self, name: str, family: str, config: Dict):
        self.name = name
        self.family = family
        try:
            self.config = config['strategy'][name]
        except KeyError:
            print(f"Warning: Strategy '{name}' not found in config. Using default configuration.")
            self.config = {
                'type': family,
                'active': True,
                'max_total_expo': 8 if family in ['spread', 'copula'] else None,
                'allocation': 2500 if family == 'basketspread' else None,
                'leverage': 1 if family == 'basketspread' else None,
                'nb_short': 18 if family == 'basketspread' else None,
                'nb_long': 18 if family == 'basketspread' else None
            }
        self.portfolio: Dict[str, Dict[str, float]] = {}  # asset -> {qty, entry_price}
        self.initial_positions: Dict[str, Dict[str, float]] = {}  # For unopened positions
        self.first_timestamp: str = None
    
    def get_position_counts(self) -> tuple[int, int]:
        """Return the number of long and short positions."""
        long_count = sum(1 for info in self.portfolio.values() if info['qty'] > 0)
        short_count = sum(1 for info in self.portfolio.values() if info['qty'] < 0)
        return long_count, short_count
    
    def get_pair_count(self) -> int:
        """Return the number of active pairs (for spread/copula)."""
        long_count, short_count = self.get_position_counts()
        return min(long_count, short_count)  # A pair requires both legs
    
    def get_portfolio_at_timestamp(self) -> List[PortfolioEntry]:
        """Return the current portfolio as a list of PortfolioEntry objects."""
        return [
            PortfolioEntry(asset=asset, qty=info['qty'], entry_price=info['entry_price'])
            for asset, info in self.portfolio.items()
        ]
    
    def update_portfolio(self, signal: Dict):
        signal_type = signal['signal_type']
        direction = int(signal['direction'])
        timestamp = signal['timestamp']
        
        # Update first timestamp
        if self.first_timestamp is None or timestamp < self.first_timestamp:
            self.first_timestamp = timestamp
        
        # Handle exit signals
        if signal_type in ['exitsignal', 'LS_exitsignal', 'LS_exitforced_exit', 'LS_reshuffle signal']:
            if self.family in ['spread', 'copula']:
                if signal_type != 'exitsignal':
                    return  # Only exitsignal for spread/copula exits
                asset1, asset2 = signal['pair'].split('__')
                entry_exit = signal.get('entry_exit')
                if entry_exit != 'xx':
                    print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for exitsignal. Expected 'xx'. Ignoring.")
                    return
                max_total_expo = float(self.config.get('max_total_expo', 8))
                qty_per_leg = (1.0 / max_total_expo) / 2
                if asset1 not in self.portfolio or asset2 not in self.portfolio:
                    print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened pair {asset1}__{asset2}. Ignoring.")
                    return
                self.portfolio.pop(asset1, None)
                self.portfolio.pop(asset2, None)
            else:  # basketspread
                if signal_type not in ['LS_exitsignal', 'LS_exitforced_exit', 'LS_reshuffle signal']:
                    return  # Only LS_exitsignal, LS_exitforced_exit, LS_reshuffle signal for basketspread
                allocation = float(self.config.get('allocation', 2500))
                leverage = float(self.config.get('leverage', 1))
                aum = allocation * leverage
                if signal_type in ['LS_exitsignal', 'LS_exitforced_exit']:
                    asset = signal['asset']
                    if asset not in self.portfolio:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened position for asset {asset}. Ignoring.")
                        return
                    self.portfolio.pop(asset, None)
                elif signal_type == 'LS_reshuffle signal':
                    asset1, asset2 = signal['pair'].split('__')
                    entry_exit = signal.get('entry_exit')
                    if entry_exit not in ['xn', 'nx']:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for LS_reshuffle signal. Expected 'xn' or 'nx'. Ignoring.")
                        return
                    if entry_exit == 'xn' and asset1 not in self.portfolio:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened position for asset {asset1}. Ignoring.")
                        return
                    if entry_exit == 'nx' and asset2 not in self.portfolio:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Attempting to close unopened position for asset {asset2}. Ignoring.")
                        return
                    if entry_exit == 'xn':
                        self.portfolio.pop(asset1, None)
                    elif entry_exit == 'nx':
                        self.portfolio.pop(asset2, None)
        
        # Check position limits before adding new positions
        if self.family in ['spread', 'copula']:
            if signal_type != 'entry':
                return  # Only entry for new positions
            max_total_expo = float(self.config.get('max_total_expo', 8))
            max_pairs = max_total_expo
            current_pairs = self.get_pair_count()
            
            entry_exit = signal.get('entry_exit')
            if entry_exit == 'nn' and current_pairs >= max_pairs:
                print(f"Warning: {timestamp} - Strategy {self.name}: Number of pairs ({current_pairs}) exceeds maximum allowed ({max_pairs})")
                return
        
        elif self.family == 'basketspread':
            if signal_type not in ['LS_single_entry', 'LS_entry', 'LS_reshuffle signal']:
                return  # Only LS_single_entry, LS_entry, LS_reshuffle signal for new positions
            nb_long = int(self.config.get('nb_long', 18))
            nb_short = int(self.config.get('nb_short', 18))
            current_long, current_short = self.get_position_counts()
            
            if signal_type == 'LS_single_entry':
                qty = float(signal['quantity']) * float(signal['price']) / (float(self.config.get('allocation', 2500)) * float(self.config.get('leverage', 1)))
                qty = qty if direction == 1 else -qty
                if qty > 0 and current_long >= nb_long:
                    print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long})")
                    return
                if qty < 0 and current_short >= nb_short:
                    print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short})")
                    return
            elif signal_type in ['LS_entry', 'LS_reshuffle signal'] and signal.get('entry_exit') in ['nn', 'xn', 'nx']:
                price1, price2 = map(float, signal['prices'].split(','))
                quantity1, quantity2 = map(float, signal['quantities'].split(','))
                aum = float(self.config.get('allocation', 2500)) * float(self.config.get('leverage', 1))
                qty1 = (quantity1 * price1) / aum
                qty2 = (quantity2 * price2) / aum
                entry_exit = signal.get('entry_exit')
                if signal_type == 'LS_entry' and entry_exit != 'nn':
                    print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for LS_entry. Expected 'nn'. Ignoring.")
                    return
                if signal_type == 'LS_reshuffle signal' and entry_exit not in ['xn', 'nx']:
                    return  # Handled above
                asset1_qty = qty1 if direction == 1 else -qty1
                asset2_qty = -qty2 if direction == 1 else qty2
                if entry_exit == 'nn':
                    if asset1_qty > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset1")
                        return
                    if asset1_qty < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset1")
                        return
                    if asset2_qty > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset2")
                        return
                    if asset2_qty < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset2")
                        return
                elif entry_exit == 'xn':
                    if asset2_qty > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset2")
                        return
                    if asset2_qty < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset2")
                        return
                elif entry_exit == 'nx':
                    if asset1_qty > 0 and current_long >= nb_long:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of long positions ({current_long}) exceeds maximum allowed ({nb_long}) for asset1")
                        return
                    if asset1_qty < 0 and current_short >= nb_short:
                        print(f"Warning: {timestamp} - Strategy {self.name}: Number of short positions ({current_short}) exceeds maximum allowed ({nb_short}) for asset1")
                        return
        
        # Proceed with updating portfolio for non-exit signals
        if signal_type == 'LS_single_entry':
            if self.family != 'basketspread':
                return
            asset = signal['asset']
            quantity = float(signal['quantity'])
            price = float(signal['price'])
            allocation = float(self.config.get('allocation', 2500))
            leverage = float(self.config.get('leverage', 1))
            aum = allocation * leverage
            notional = quantity * price
            qty = notional / aum if direction == 1 else -notional / aum
            self.portfolio[asset] = {
                'qty': qty,
                'entry_price': price
            }
        elif signal_type in ['entry', 'LS_entry', 'LS_reshuffle signal']:
            asset1, asset2 = signal['pair'].split('__')
            price1, price2 = map(float, signal['prices'].split(','))
            entry_exit = signal.get('entry_exit')
            
            if self.family in ['spread', 'copula']:
                if signal_type != 'entry':
                    return
                if entry_exit != 'nn':
                    print(f"Warning: {timestamp} - Strategy {self.name}: Invalid entry_exit '{entry_exit}' for entry. Expected 'nn'. Ignoring.")
                    return
                max_total_expo = float(self.config.get('max_total_expo', 8))
                qty_per_leg = (1.0 / max_total_expo) / 2
                self.portfolio[asset1] = {
                    'qty': qty_per_leg if direction == 1 else -qty_per_leg,
                    'entry_price': price1
                }
                self.portfolio[asset2] = {
                    'qty': -qty_per_leg if direction == 1 else qty_per_leg,
                    'entry_price': price2
                }
            else:  # basketspread
                if signal_type not in ['LS_entry', 'LS_reshuffle signal']:
                    return
                allocation = float(self.config.get('allocation', 2500))
                leverage = float(self.config.get('leverage', 1))
                aum = allocation * leverage
                quantity1, quantity2 = map(float, signal['quantities'].split(','))
                notional1 = quantity1 * price1
                notional2 = quantity2 * price2
                qty1 = notional1 / aum
                qty2 = notional2 / aum
                
                if signal_type == 'LS_reshuffle signal':
                    if entry_exit == 'xn':
                        self.portfolio[asset2] = {
                            'qty': qty2 if direction == 1 else -qty2,
                            'entry_price': price2
                        }
                    elif entry_exit == 'nx':
                        self.portfolio[asset1] = {
                            'qty': qty1 if direction == 1 else -qty1,
                            'entry_price': price1
                        }
                elif signal_type == 'LS_entry':
                    if entry_exit == 'nn':
                        self.portfolio[asset1] = {
                            'qty': qty1 if direction == 1 else -qty1,
                            'entry_price': price1
                        }
                        self.portfolio[asset2] = {
                            'qty': -qty2 if direction == 1 else qty2,
                            'entry_price': price2
                        }
    
    def process_signals(self, signal_file: str, output_file: str) -> Dict[str, Dict]:
        # Check if portfolio file exists
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r') as f:
                    portfolios = json.load(f)
                return portfolios
            except json.JSONDecodeError:
                print(f"Warning: Portfolio file {output_file} is corrupted. Recomputing portfolio.")
        
        # Compute portfolio if file doesn't exist or is invalid
        portfolios: Dict[str, Dict] = {}
        try:
            with open(signal_file, 'r') as f:
                reader = csv.reader(f, delimiter=';')
                for row in reader:
                    if len(row) < 8 or row[1] != 'INFO':
                        continue
                    
                    signal = {}
                    try:
                        if self.family == 'basketspread' and len(row) >= 9 and row[8] in ['LS_single_entry', 'LS_exitsignal', 'LS_exitforced_exit']:
                            signal = {
                                'timestamp': row[0],
                                'strategy': row[3],
                                'direction': row[4],
                                'asset': row[5],
                                'quantity': row[6],
                                'price': row[7],
                                'signal_type': row[8]
                            }
                        elif len(row) >= 10 and row[9] in (
                            ['entry', 'exitsignal'] if self.family in ['spread', 'copula'] else
                            ['LS_reshuffle signal', 'LS_entry']
                        ):
                            signal = {
                                'timestamp': row[0],
                                'strategy': row[3],
                                'direction': row[4],
                                'pair': row[5],
                                'quantities': row[6],
                                'prices': row[7],
                                'entry_exit': row[8],
                                'signal_type': row[9]
                            }
                        else:
                            continue
                    except IndexError as e:
                        print(f"Error parsing row {row}: {e}")
                        continue
                    
                    if signal['strategy'] == self.name:
                        self.update_portfolio(signal)
                        portfolios[signal['timestamp']] = {
                            'strategy': self.name,
                            'portfolio': [
                                {'asset': entry.asset, 'qty': entry.qty, 'entry_price': entry.entry_price}
                                for entry in self.get_portfolio_at_timestamp()
                            ]
                        }
        except FileNotFoundError:
            print(f"Signal file {signal_file} not found")
            return {}
        return portfolios
    
    def save_portfolio(self, portfolios: Dict[str, Dict], output_file: str):
        # Add portfolio_unbalance to each timestamp
        for timestamp, data in portfolios.items():
            unbalance = sum(entry['qty'] for entry in data['portfolio'])
            data['portfolio_unbalance'] = unbalance
        
        with open(output_file, 'w') as f:
            json.dump(portfolios, f, indent=4)

class StrategyManager:
    def __init__(self, config_file: str = 'config_pair_session_bin.json'):
        self.strategies: Dict[str, Strategy] = {}
        try:
            with open(config_file, 'r') as f:
                self.config = json.load(f)
        except FileNotFoundError:
            print(f"Error: Config file {config_file} not found. Using empty config.")
            self.config = {'strategy': {}, 'account_allocation': {}}
    
    def add_strategy(self, name: str, family: str):
        self.strategies[name] = Strategy(name, family, self.config)
    
    def process_strategy_signals(self, strategy_name: str, signal_file: str, output_file: str):
        if strategy_name not in self.strategies:
            print(f"Error: Strategy '{strategy_name}' not found in StrategyManager.")
            return
        portfolios = self.strategies[strategy_name].process_signals(signal_file, output_file)
        self.strategies[strategy_name].save_portfolio(portfolios, output_file)
        
        account = self.config['strategy'][strategy_name].get('account_trade')
        if account:
            self.update_cumulative_portfolio(account, signal_file)
    
    def update_cumulative_portfolio(self, account: str, signal_file: str):
        output_file = f"{self.config.get('output_directory', 'output_bin/')}portfolio_{account}.json"
        
        # Check if account portfolio file exists
        if os.path.exists(output_file):
            try:
                with open(output_file, 'r') as f:
                    cumulative_portfolios = json.load(f)
                return
            except json.JSONDecodeError:
                print(f"Warning: Account portfolio file {output_file} is corrupted. Recomputing portfolio.")
        
        # Compute cumulative portfolio if file doesn't exist or is invalid
        family_counts = {'spread': 0, 'basketspread': 0}
        for strat_name, strat in self.strategies.items():
            if strat.config.get('account_trade') == account and strat.config.get('active', False):
                family = 'spread' if strat.family in ['spread', 'copula'] else 'basketspread'
                family_counts[family] += 1
        
        account_allocation = self.config.get('account_allocation', {}).get(account, {})
        spread_weight = account_allocation.get('spread', 1.0) / max(family_counts['spread'], 1)
        basketspread_weight = account_allocation.get('basketspread', 1.0) / max(family_counts['basketspread'], 1)
        
        all_portfolios: Dict[str, Dict[str, float]] = {}
        for strat_name, strat in self.strategies.items():
            if strat.config.get('account_trade') == account and strat.config.get('active', False):
                strategy_output_file = f"{self.config.get('output_directory', 'output_bin/')}portfolio_{strat_name}.json"
                portfolios = strat.process_signals(signal_file, strategy_output_file)
                for timestamp, portfolio_data in portfolios.items():
                    if timestamp not in all_portfolios:
                        all_portfolios[timestamp] = {}
                    weight = spread_weight if strat.family in ['spread', 'copula'] else basketspread_weight
                    for entry in portfolio_data['portfolio']:
                        asset = entry['asset']
                        qty = entry['qty'] * weight
                        if asset in all_portfolios[timestamp]:
                            all_portfolios[timestamp][asset] = all_portfolios[timestamp].get(asset, 0.0) + qty
                        else:
                            all_portfolios[timestamp][asset] = qty
        
        cumulative_portfolios = {}
        for timestamp, assets in all_portfolios.items():
            portfolio_entries = [
                {'asset': asset, 'qty': qty}
                for asset, qty in assets.items() if abs(qty) > 1e-6
            ]
            unbalance = sum(entry['qty'] for entry in portfolio_entries)
            cumulative_portfolios[timestamp] = {
                'account': account,
                'portfolio': portfolio_entries,
                'portfolio_unbalance': unbalance
            }
        
        with open(output_file, 'w') as f:
            json.dump(cumulative_portfolios, f, indent=4)