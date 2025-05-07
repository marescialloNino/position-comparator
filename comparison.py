# comparison.py
from typing import Dict
import json

class PositionComparator:
    @staticmethod
    def load_actual_positions(pos_file: str) -> Dict[str, Dict[str, float]]:
        with open(pos_file, 'r') as f:
            return json.load(f)
    
    @staticmethod
    def compare_positions(theoretical: Dict[str, Dict[str, float]], actual: Dict[str, Dict[str, float]]) -> Dict[str, Dict[str, Dict[str, float]]]:
        comparison = {}
        all_timestamps = set(theoretical.keys()) | set(actual.keys())
        
        for timestamp in sorted(all_timestamps):
            theo_positions = theoretical.get(timestamp, {})
            actual_positions = actual.get(timestamp, {})
            timestamp_comparison = {}
            all_symbols = set(theo_positions.keys()) | set(actual_positions.keys())
            
            for symbol in all_symbols:
                theo_qty = theo_positions.get(symbol, 0)
                actual_qty = actual_positions.get(symbol, 0)
                discrepancy = theo_qty - actual_qty
                timestamp_comparison[symbol] = {
                    'theoretical': theo_qty,
                    'actual': actual_qty,
                    'discrepancy': discrepancy
                }
            comparison[timestamp] = timestamp_comparison
        
        return comparison
    
    @staticmethod
    def save_comparison(comparison: Dict, output_file: str):
        with open(output_file, 'w') as f:
            json.dump(comparison, f, indent=4)