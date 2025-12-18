"""
Advanced CCXT Orderbook Processor.
Uses SymbolState to generate high-frequency features and bar aggregates.
"""
from typing import Any, Dict, List, Optional, Union
import logging

from etl.processors.base_processor import BaseProcessor
from etl.features.state import SymbolState, StateConfig

logger = logging.getLogger(__name__)

class CcxtAdvancedOrderbookProcessor(BaseProcessor):
    """
    Stateful processor for CCXT orderbook data.
    Generates two streams of data:
    1. High-frequency feature rows (downsampled)
    2. Bar-level aggregates (OHLCV + microstructure stats)
    """
    
    def __init__(self, **kwargs):
        super().__init__()
        
        # Handle parameter name mapping from config.yaml to StateConfig
        # Supports both old and new parameter names for backward compatibility
        param_mapping = {
            'hf_sample_interval': 'hf_emit_interval',  # Old name → new name
        }
        
        # Apply mapping
        normalized_kwargs = {}
        for key, value in kwargs.items():
            mapped_key = param_mapping.get(key, key)
            normalized_kwargs[mapped_key] = value
        
        # Extract StateConfig parameters
        state_config_params = {
            k: v for k, v in normalized_kwargs.items() 
            if k in StateConfig.__annotations__
        }
        self.config = StateConfig(**state_config_params)
        self.states: Dict[str, SymbolState] = {}
        
        logger.info(f"[CcxtAdvancedOrderbookProcessor] Initialized with config: {self.config}")
        
    def process(self, data: Any) -> Dict[str, List[Dict[str, Any]]]:
        """
        Process orderbook data.
        
        Args:
            data: Single record or list of records
            
        Returns:
            Dict with keys 'hf' and 'bars', each containing a list of records.
        """
        if isinstance(data, list):
            results = {'hf': [], 'bars': []}
            for d in data:
                res = self._process_one(d)
                if res:
                    if res['hf']:
                        results['hf'].append(res['hf'])
                    if res['bars']:
                        results['bars'].extend(res['bars'])
            return results
        else:
            res = self._process_one(data)
            if res:
                return {'hf': [res['hf']] if res['hf'] else [], 'bars': res['bars']}
            return {'hf': [], 'bars': []}

    def _process_one(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single record (orderbook or trade)."""
        if not record:
            return None
            
        try:
            exchange = record.get('exchange')
            symbol = record.get('symbol')
            channel = record.get('channel')
            
            if not exchange or not symbol:
                return None
                
            key = f"{exchange}:{symbol}"
            
            if key not in self.states:
                self.states[key] = SymbolState(symbol, exchange, self.config)
                
            state = self.states[key]
            
            if channel == 'trades':
                state.process_trade(record)
                return None
            elif channel == 'orderbook':
                hf_row, bar_rows = state.process_snapshot(record)
                
                self.stats["records_output"] += (1 if hf_row else 0) + len(bar_rows)
                
                return {
                    'hf': hf_row,
                    'bars': bar_rows
                }
            else:
                # Unknown channel
                return None
            
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error processing record: {e}")
            return None
