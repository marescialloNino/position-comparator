# config.py
import json
from typing import Dict, List, Tuple

class Config:
    def __init__(self, config_file: str):
        with open(config_file, 'r') as f:
            self.config = json.load(f)
    
    def get_exchanges(self) -> List[str]:
        return list(self.config['allocations'].keys())
    
    def get_accounts(self, exchange: str) -> List[str]:
        return list(self.config['allocations'][exchange].keys())
    
    def get_account_allocation(self, exchange: str, account: str) -> Tuple[str, float]:
        alloc_type, value = self.config['allocations'][exchange][account]
        return alloc_type, value
    
    def get_total_aum(self) -> float:
        total_aum = 0
        for exchange in self.get_exchanges():
            for account in self.get_accounts(exchange):
                alloc_type, value = self.get_account_allocation(exchange, account)
                if alloc_type == 'fixed':
                    total_aum += value
        return total_aum
    
    def get_strategy_families(self) -> Dict[str, float]:
        return self.config['allocation_ratios']
    
    def get_strategies(self, exchange: str, account: str) -> Dict[str, List[Dict]]:
        strategies = self.config['strategy']
        result = {'pairspread': [], 'basketspread': [], 'trend': []}
        
        for strat_name, strat_config in strategies.items():
            if strat_config['active'] and strat_config['exchange_trade'] == exchange and str(strat_config['account_trade']) == str(account):
                family = strat_config['type']
                if family in result:
                    result[family].append({'name': strat_name, **strat_config})
        
        return result