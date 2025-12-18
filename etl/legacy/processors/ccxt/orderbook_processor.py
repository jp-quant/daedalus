"""CCXT Orderbook Processor."""
from typing import Any, Dict, List, Optional
from ..base_processor import BaseProcessor


class CcxtOrderbookProcessor(BaseProcessor):
    """
    Process CCXT orderbook data.
    
    Adds derived fields:
    - best_bid_price
    - best_bid_amount
    - best_ask_price
    - best_ask_amount
    - spread
    - mid_price
    """
    
    def process(self, data: Any) -> Any:
        """
        Process orderbook data.
        
        Args:
            data: Single record or list of records
            
        Returns:
            Processed record(s)
        """
        if isinstance(data, list):
            return [self._process_one(d) for d in data if d]
        return self._process_one(data)

    def _process_one(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Process a single orderbook record."""
        if not record:
            return None
            
        try:
            bids = record.get('bids', [])
            asks = record.get('asks', [])
            
            # Initialize derived fields
            record['best_bid_price'] = None
            record['best_bid_amount'] = None
            record['best_ask_price'] = None
            record['best_ask_amount'] = None
            record['spread'] = None
            record['mid_price'] = None
            
            # Extract best bid/ask
            # CCXT sorts bids descending (highest first) and asks ascending (lowest first)
            if bids and len(bids) > 0:
                best_bid = bids[0]
                if len(best_bid) >= 2:
                    record['best_bid_price'] = best_bid[0]
                    record['best_bid_amount'] = best_bid[1]
            
            if asks and len(asks) > 0:
                best_ask = asks[0]
                if len(best_ask) >= 2:
                    record['best_ask_price'] = best_ask[0]
                    record['best_ask_amount'] = best_ask[1]
            
            # Calculate spread and mid price
            if record['best_bid_price'] is not None and record['best_ask_price'] is not None:
                record['spread'] = record['best_ask_price'] - record['best_bid_price']
                record['mid_price'] = (record['best_ask_price'] + record['best_bid_price']) / 2
            
            self.stats["records_output"] += 1
            return record
            
        except Exception as e:
            self.stats["errors"] += 1
            # logger.error(f"Error processing orderbook record: {e}")
            return None
