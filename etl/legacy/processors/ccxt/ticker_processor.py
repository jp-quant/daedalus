"""CCXT Ticker Processor."""
from typing import Any, Dict, List, Union, Optional
from ..base_processor import BaseProcessor
from ..time_utils import add_time_fields


class CcxtTickerProcessor(BaseProcessor):
    """
    Process CCXT ticker data.
    
    Adds derived fields:
    - spread: ask - bid
    - spread_pct: (ask - bid) / bid * 100
    - time features: hour, day_of_week, is_weekend, etc.
    """
    
    def process(self, data: Any) -> Any:
        """
        Process ticker data.
        
        Args:
            data: Single record or list of records
            
        Returns:
            Processed record(s)
        """
        if isinstance(data, list):
            return [self._process_one(d) for d in data if d]
        return self._process_one(data)

    def _process_one(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single ticker record."""
        if not record:
            return None
            
        try:
            # Add derived fields
            bid = record.get('bid')
            ask = record.get('ask')
            
            if bid is not None and ask is not None:
                spread = ask - bid
                record['spread'] = spread
                
                if bid > 0:
                    record['spread_pct'] = (spread / bid) * 100
                else:
                    record['spread_pct'] = None
            
            # Add robust time features
            # Ticker records usually have 'datetime' (ISO) and 'timestamp' (ms)
            add_time_fields(record, 'datetime', 'timestamp')
            
            self.stats["records_output"] += 1
            return record
            
        except Exception:
            self.stats["errors"] += 1
            return None
