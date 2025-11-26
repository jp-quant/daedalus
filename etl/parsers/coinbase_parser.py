"""Coinbase NDJSON parser - extracts and validates Coinbase market data."""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime

from ingestion.utils.time import parse_timestamp


logger = logging.getLogger(__name__)


class CoinbaseParser:
    """
    Parse Coinbase Advanced Trade messages from NDJSON logs.
    
    Handles multiple channels:
    - ticker: Real-time price updates
    - level2: Order book snapshots and updates
    - market_trades: Trade executions
    - candles: OHLCV candles
    """
    
    def __init__(self):
        self.stats = {
            "parsed": 0,
            "errors": 0,
            "by_channel": {},
        }
    
    def parse_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a single NDJSON record.
        
        Args:
            record: Deserialized NDJSON record
            
        Returns:
            Parsed and normalized record, or None if parsing fails
        """
        try:
            # Extract envelope fields
            capture_ts = record.get("capture_ts")
            data = record.get("data", {})
            metadata = record.get("metadata", {})
            
            channel = data.get("channel", "unknown")
            
            # Route to channel-specific parser
            # Note: Coinbase uses different channel names in responses vs subscription:
            # - level2 (subscribe) → l2_data (response)
            # - ticker (subscribe) → ticker (response)
            if channel == "ticker":
                return self._parse_ticker(data, capture_ts, metadata)
            elif channel in ("level2", "l2_data"):  # l2_data is actual response name
                return self._parse_level2(data, capture_ts, metadata)
            elif channel == "market_trades":
                return self._parse_market_trades(data, capture_ts, metadata)
            elif channel == "candles":
                return self._parse_candles(data, capture_ts, metadata)
            else:
                logger.warning(f"[CoinbaseParser] Unknown channel: {channel}")
                return None
        
        except Exception as e:
            self.stats["errors"] += 1
            logger.error(f"[CoinbaseParser] Error parsing record: {e}", exc_info=True)
            return None
    
    def _parse_ticker(
        self,
        data: Dict[str, Any],
        capture_ts: str,
        metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Parse ticker channel messages."""
        records = []
        
        # Extract message-level fields
        server_timestamp = data.get("timestamp")
        sequence_num = data.get("sequence_num")
        client_id = data.get("client_id", "")
        
        for event in data.get("events", []):
            for ticker in event.get("tickers", []):
                try:
                    record = {
                        "channel": "ticker",
                        "event_type": event.get("type", "unknown"),
                        "product_id": ticker.get("product_id"),
                        "price": float(ticker.get("price", 0)),
                        "volume_24h": float(ticker.get("volume_24_h", 0)),
                        "low_24h": float(ticker.get("low_24_h", 0)),
                        "high_24h": float(ticker.get("high_24_h", 0)),
                        "low_52w": float(ticker.get("low_52_w", 0)),
                        "high_52w": float(ticker.get("high_52_w", 0)),
                        "price_percent_chg_24h": float(ticker.get("price_percent_chg_24_h", 0)),
                        "best_bid": float(ticker.get("best_bid", 0)),
                        "best_bid_quantity": float(ticker.get("best_bid_quantity", 0)),
                        "best_ask": float(ticker.get("best_ask", 0)),
                        "best_ask_quantity": float(ticker.get("best_ask_quantity", 0)),
                        "sequence_num": sequence_num,
                        "client_id": client_id,
                        "server_timestamp": server_timestamp,
                        "capture_timestamp": capture_ts,
                    }
                    records.append(record)
                    self.stats["parsed"] += 1
                    self.stats["by_channel"]["ticker"] = self.stats["by_channel"].get("ticker", 0) + 1
                
                except Exception as e:
                    logger.error(f"[CoinbaseParser] Error parsing ticker: {e}")
                    self.stats["errors"] += 1
        
        return records
    
    def _parse_level2(
        self,
        data: Dict[str, Any],
        capture_ts: str,
        metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """
        Parse level2 (order book) channel messages.
        
        Level2 provides order book snapshots and updates with:
        - sequence_num: Message sequence for ordering
        - events: List of snapshot/update events
        - updates: Price level changes (bid/offer side, price, quantity)
        """
        records = []
        
        # Extract message-level fields
        server_timestamp = data.get("timestamp")
        sequence_num = data.get("sequence_num")
        client_id = data.get("client_id", "")
        
        for event in data.get("events", []):
            event_type = event.get("type", "unknown")
            product_id = event.get("product_id")
            
            # Both snapshot and update have same structure
            for update in event.get("updates", []):
                try:
                    record = {
                        "channel": "level2",  # Normalize to 'level2' (from 'l2_data')
                        "event_type": event_type,  # "snapshot" or "update"
                        "product_id": product_id,
                        "side": update.get("side"),  # "bid" or "offer"
                        "price_level": float(update.get("price_level", 0)),
                        "new_quantity": float(update.get("new_quantity", 0)),
                        "event_time": update.get("event_time"),  # ISO8601 timestamp
                        "sequence_num": sequence_num,  # For ordering messages
                        "client_id": client_id,
                        "server_timestamp": server_timestamp,
                        "capture_timestamp": capture_ts,
                    }
                    records.append(record)
                except Exception as e:
                    logger.error(f"[CoinbaseParser] Error parsing level2 {event_type}: {e}")
                    self.stats["errors"] += 1
        
        self.stats["parsed"] += len(records)
        self.stats["by_channel"]["level2"] = self.stats["by_channel"].get("level2", 0) + len(records)
        
        return records
    
    def _parse_market_trades(
        self,
        data: Dict[str, Any],
        capture_ts: str,
        metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Parse market_trades channel messages."""
        records = []
        
        # Extract message-level fields
        server_timestamp = data.get("timestamp")
        sequence_num = data.get("sequence_num")
        client_id = data.get("client_id", "")
        
        for event in data.get("events", []):
            for trade in event.get("trades", []):
                try:
                    record = {
                        "channel": "market_trades",
                        "event_type": event.get("type", "unknown"),
                        "trade_id": trade.get("trade_id"),
                        "product_id": trade.get("product_id"),
                        "price": float(trade.get("price", 0)),
                        "size": float(trade.get("size", 0)),
                        "side": trade.get("side"),  # "BUY" or "SELL"
                        "time": trade.get("time"),
                        "sequence_num": sequence_num,
                        "client_id": client_id,
                        "server_timestamp": server_timestamp,
                        "capture_timestamp": capture_ts,
                    }
                    records.append(record)
                    self.stats["parsed"] += 1
                    self.stats["by_channel"]["market_trades"] = self.stats["by_channel"].get("market_trades", 0) + 1
                
                except Exception as e:
                    logger.error(f"[CoinbaseParser] Error parsing market_trade: {e}")
                    self.stats["errors"] += 1
        
        return records
    
    def _parse_candles(
        self,
        data: Dict[str, Any],
        capture_ts: str,
        metadata: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Parse candles channel messages (OHLCV data in 5-minute buckets)."""
        records = []
        
        # Extract message-level fields
        server_timestamp = data.get("timestamp")
        sequence_num = data.get("sequence_num")
        client_id = data.get("client_id", "")
        
        for event in data.get("events", []):
            for candle in event.get("candles", []):
                try:
                    record = {
                        "channel": "candles",
                        "event_type": event.get("type", "unknown"),
                        "product_id": candle.get("product_id"),
                        "start": candle.get("start"),  # UNIX timestamp as string
                        "high": float(candle.get("high", 0)),
                        "low": float(candle.get("low", 0)),
                        "open": float(candle.get("open", 0)),
                        "close": float(candle.get("close", 0)),
                        "volume": float(candle.get("volume", 0)),
                        "sequence_num": sequence_num,
                        "client_id": client_id,
                        "server_timestamp": server_timestamp,
                        "capture_timestamp": capture_ts,
                    }
                    records.append(record)
                    self.stats["parsed"] += 1
                    self.stats["by_channel"]["candles"] = self.stats["by_channel"].get("candles", 0) + 1
                
                except Exception as e:
                    logger.error(f"[CoinbaseParser] Error parsing candle: {e}")
                    self.stats["errors"] += 1
        
        return records
    
    def get_stats(self) -> Dict[str, Any]:
        """Get parser statistics."""
        return self.stats.copy()
