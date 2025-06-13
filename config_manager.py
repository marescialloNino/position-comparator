#!/usr/bin/env python3
"""
config_manager.py
~~~~~~~~~~~~~~~~~
Singleton class to load and manage configuration from config_pair_session_bitget.json.
Provides access to active strategies, allocations, output directory, and AUM calculations.
"""

from pathlib import Path
from typing import Dict, List, Tuple, Optional
import json

class ConfigManager:
    """Singleton class for managing configuration data."""
    _instance = None

    def __new__(cls, config_file: str = None):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self, config_file: str = None):
        if self._initialized:
            return
        self._config_file = Path(config_file or "config_pair_session_bitget.json")
        self._config: Dict = {}
        self._active_strategies: List[Dict] = []
        self._allocations: Dict = {}
        self._output_directory: Path = Path("output_bitget")
        self._load_config()
        self._initialized = True

    def _load_config(self):
        """Load and parse the configuration file."""
        if not self._config_file.exists():
            raise FileNotFoundError(f"Config file not found at {self._config_file}")
        try:
            with open(self._config_file, 'r') as f:
                self._config = json.load(f)
        except Exception as e:
            raise ValueError(f"Error reading config file {self._config_file}: {e}")

        # Parse strategies
        for name, cfg in self._config.get('strategy', {}).items():
            if cfg.get('active', False):
                self._active_strategies.append({
                    "name": name,
                    "type": cfg.get('type', 'basketspread'),
                    "account_trade": cfg.get('account_trade'),
                    "exchange_trade": cfg.get('exchange_trade', 'bitget'),
                    "nb_long": cfg.get('nb_long', 8),
                    "nb_short": cfg.get('nb_short', 8),
                    "max_total_expo": cfg.get('max_total_expo', 16)
                })

        # Parse allocations
        self._allocations = self._config.get('allocations', {})

        # Set output directory
        output_dir = self._config.get('output_directory')
        if output_dir:
            self._output_directory = Path(output_dir)

    def get_active_strategies(self) -> List[Dict]:
        """Return list of active strategies."""
        return self._active_strategies

    def get_allocation(self, exchange: str, account: str) -> Optional[Dict]:
        """Return allocation details for an exchange and account."""
        return self._allocations.get(exchange, {}).get(account)

    def get_output_directory(self) -> Path:
        """Return output directory path."""
        return self._output_directory

    def get_strategy_config(self, strategy_name: str) -> Optional[Dict]:
        """Return configuration for a specific strategy."""
        for strat in self._active_strategies:
            if strat["name"] == strategy_name:
                return strat
        return None

    def get_aum_and_max_positions(
        self, strategy_name: str, account: str, exchange: str
    ) -> Tuple[float, int]:
        """Calculate AUM and maximum number of positions for a strategy."""
        strat_config = self.get_strategy_config(strategy_name)
        if not strat_config:
            print(f"Warning: Strategy {strategy_name} not found")
            return 0.0, 0

        allocation = self.get_allocation(exchange, account)
        if not allocation:
            print(f"Warning: No allocation for account {account} on {exchange}")
            return 0.0, 0

        amount = allocation.get('amount', 0.0)
        leverage = allocation.get('leverage', 1.0)
        allocation_ratio = allocation.get('allocation_ratios', {}).get(strat_config['type'], 0.0)
        base_aum = amount * leverage * allocation_ratio

        # Count active basketspread strategies
        active_basket_strats = sum(
            1 for strat in self._active_strategies
            if strat['type'] == 'basketspread' and
            strat['account_trade'] == account and
            strat['exchange_trade'] == exchange
        )

        if active_basket_strats > 0 and strat_config['type'] == 'basketspread':
            aum = base_aum / active_basket_strats
        else:
            aum = base_aum

        if strat_config['type'] == 'basketspread':
            max_positions = strat_config['nb_long'] + strat_config['nb_short']
        else:
            max_positions = strat_config['max_total_expo'] 

        return aum, max_positions