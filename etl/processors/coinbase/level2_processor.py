"""Coinbase level2 orderbook processor - reconstructs and derives LOB features."""
import logging
from typing import Dict, Any, List
from collections import defaultdict

from ..base_processor import BaseProcessor
from ..time_utils import add_time_fields

logger = logging.getLogger(__name__)


class Level2Processor(BaseProcessor):
    """
    Process Coinbase level2 orderbook data.
    
    Coinbase-specific implementation for processing level2 messages.
    
    Capabilities:
    - Basic validation and normalization
    - LOB reconstruction (maintain orderbook state)
    - Microstructure features (spread, depth, imbalance, etc.)
    - Book pressure indicators
    
    TODO: Implement full LOB reconstruction with state management
    """
    
    def __init__(
        self,
        reconstruct_lob: bool = False,
        compute_features: bool = False,
        add_derived_fields: bool = True,
        max_levels: int = 10,
        **kwargs
    ):
        """
        Initialize level2 processor.
        
        Args:
            reconstruct_lob: Maintain orderbook state and emit snapshots
            compute_features: Compute microstructure features
            add_derived_fields: Add basic derived fields (mid-price, etc.)
            max_levels: Depth for feature extraction
        """
        super().__init__()
        self.reconstruct_lob = reconstruct_lob
        self.compute_features = compute_features
        self.add_derived_fields = add_derived_fields
        self.max_levels = max_levels
        
        # State for LOB reconstruction (per product)
        self.orderbooks = defaultdict(lambda: {"bids": {}, "asks": {}})
        
        logger.info(
            f"[CoinbaseLevel2Processor] Initialized: reconstruct_lob={reconstruct_lob}, "
            f"compute_features={compute_features}"
        )
    
    def process(self, data: Any) -> Any:
        """Process level2 data."""
        if isinstance(data, dict):
            return self.process_record(data)
        elif isinstance(data, list):
            return self.process_batch(data)
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
    
    def process_record(self, record: dict) -> dict:
        """
        Process a single level2 record.
        
        Args:
            record: Parsed level2 record with side, price_level, new_quantity
            
        Returns:
            Enhanced record with derived fields
        """
        try:
            # Add basic derived fields
            if self.add_derived_fields:
                record = self._add_derived_fields(record)
            
            # Reconstruct LOB state (if enabled)
            if self.reconstruct_lob:
                record = self._update_orderbook(record)
            
            # Compute microstructure features (if enabled)
            if self.compute_features:
                record = self._compute_features(record)
            
            self.stats["records_processed"] += 1
            self.stats["records_output"] += 1
            
            return record
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"Error processing level2 record: {e}", exc_info=True)
            return record  # Return original on error
    
    def _add_derived_fields(self, record: dict) -> dict:
        """Add basic derived fields."""
        # Extract timestamp components for partitioning (year, month, day, hour, minute, second, microsecond, date)
        record = add_time_fields(
            record,
            timestamp_field="event_time",
            fallback_field="server_timestamp"
        )
        
        # Flag for snapshot vs update
        record["is_snapshot"] = record.get("event_type") == "snapshot"
        
        # Convert side to standardized format
        if record.get("side") == "offer":
            record["side_normalized"] = "ask"
        else:
            record["side_normalized"] = record.get("side", "bid")
        
        return record
    
    def _update_orderbook(self, record: dict) -> dict:
        """
        Update orderbook state and add LOB fields.
        
        TODO: Full implementation with:
        - Maintain bid/ask dictionaries per product
        - Handle snapshots (reset state)
        - Handle updates (add/remove/update levels)
        - Compute best bid/ask, spread, depth
        """
        product_id = record.get("product_id")
        if not product_id:
            return record
        
        book = self.orderbooks[product_id]
        side = record.get("side_normalized", record.get("side"))
        price_level = record.get("price_level")
        new_quantity = record.get("new_quantity", 0)
        
        # Handle snapshot: reset state
        if record.get("event_type") == "snapshot":
            if side in ("bid", "bids"):
                if new_quantity > 0:
                    book["bids"][price_level] = new_quantity
            elif side in ("ask", "asks", "offer"):
                if new_quantity > 0:
                    book["asks"][price_level] = new_quantity
        
        # Handle update: modify state
        elif record.get("event_type") == "update":
            if side in ("bid", "bids"):
                if new_quantity > 0:
                    book["bids"][price_level] = new_quantity
                else:
                    book["bids"].pop(price_level, None)
            elif side in ("ask", "asks", "offer"):
                if new_quantity > 0:
                    book["asks"][price_level] = new_quantity
                else:
                    book["asks"].pop(price_level, None)
        
        # Add LOB state to record
        if book["bids"] and book["asks"]:
            best_bid = max(book["bids"].keys()) if book["bids"] else None
            best_ask = min(book["asks"].keys()) if book["asks"] else None
            
            if best_bid and best_ask:
                record["best_bid_price"] = best_bid
                record["best_bid_size"] = book["bids"][best_bid]
                record["best_ask_price"] = best_ask
                record["best_ask_size"] = book["asks"][best_ask]
                record["mid_price"] = (best_bid + best_ask) / 2
                record["spread"] = best_ask - best_bid
                record["spread_bps"] = (record["spread"] / record["mid_price"]) * 10000
        
        return record
    
    def _compute_features(self, record: dict) -> dict:
        """
        Compute microstructure features.
        
        TODO: Implement advanced features:
        - Order book imbalance (volume at various depths)
        - Weighted mid-price
        - Book pressure indicators
        - Flow toxicity metrics
        """
        # Placeholder for future implementation
        return record
    
    def process_batch(self, records: list[dict]) -> list[dict]:
        """
        Process batch of level2 records.
        
        Important: Records should be sorted by sequence_num for correct LOB state.
        """
        # Sort by sequence number if available
        if records and "sequence_num" in records[0]:
            records = sorted(records, key=lambda r: (r.get("sequence_num", 0), r.get("event_time", "")))
        
        return super().process_batch(records)
