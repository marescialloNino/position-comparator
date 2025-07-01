# config.py
import json
from typing import Dict, List

class Config:
    def __init__(self, config_file: str):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
    
    def get_active_strategies(self) -> List[Dict]:
        strategies = self.config['strategy']
        return [
            {
                'name': name,
                'type': config['type'],
                'exchange': config['exchange_trade'],
                'account': config['account_trade']
            }
            for name, config in strategies.items()
            if config['active']
        ]

