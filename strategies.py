# strategies.py
from typing import Dict, List
from dataclasses import dataclass
import json

@dataclass
class Position:
    symbol: str
    quantity: float

class Strategy:
    def __init__(self, name: str, family: str, config: Dict):
        self.name = name
        self.family = family
        self.config = config
    
    def get_portfolio_at_timestamp(self, signal_file: str, timestamp: str) -> List[Position]:
        # Load signals and return portfolio at specified timestamp
        # Assumes signal_file is JSON: {"timestamp": {"symbol": quantity, ...}, ...}
        try:
            with open(signal_file, 'r') as f:
                signals = json.load(f)
            
            positions = signals.get(timestamp, {})
            return [Position(symbol=symbol, quantity=quantity) 
                    for symbol, quantity in positions.items()]
        except Exception as e:
            print(f"Error loading signals for {self.name} at {timestamp}: {e}")
            return []
    
    def get_all_timestamps(self, signal_file: str) -> List[str]:
        # Return all timestamps in the signal file
        try:
            with open(signal_file, 'r') as f:
                signals = json.load(f)
            return sorted(signals.keys())
        except Exception as e:
            print(f"Error reading timestamps for {self.name}: {e}")
            return []

class StrategyManager:
    def __init__(self):
        self.strategies: Dict[str, Strategy] = {}
    
    def add_strategy(self, name: str, family: str, config: Dict):
        self.strategies[name] = Strategy(name, family, config)
    
    def get_strategy_portfolio(self, strategy_name: str, signal_file: str, timestamp: str) -> List[Position]:
        return self.strategies[strategy_name].get_portfolio_at_timestamp(signal_file, timestamp)
    
    def get_strategy_timestamps(self, strategy_name: str, signal_file: str) -> List[str]:
        return self.strategies[strategy_name].get_all_timestamps(signal_file)
