"""CCXT NDJSON parser - extracts and validates CCXT market data."""
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class CcxtParser:
    """
    Parse CCXT messages from NDJSON logs.
    
    Handles multiple message types:
    - ticker: Ticker updates
    - orderbook: Order book snapshots (if implemented)
    - trade: Trade executions (if implemented)
    """
    
    def __init__(self):
        self.stats = {
            "parsed": 0,
            "errors": 0,
            "by_type": {},
        }

    def get_stats(self) -> Dict[str, Any]:
        """Get parser statistics."""
        return self.stats
    
    def parse_record(self, record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Parse a single NDJSON record.
        
        Args:
            record: Deserialized NDJSON record
            
        Returns:
            Parsed and normalized record, or None if parsing fails
        """
        try:
            msg_type = record.get("type")
            exchange = record.get("exchange")
            symbol = record.get("symbol")
            data = record.get("data")
            collected_at = record.get("collected_at")
            
            if not msg_type or not data:
                return None
            
            # Update stats
            self.stats["by_type"][msg_type] = self.stats["by_type"].get(msg_type, 0) + 1
            
            # Route to type-specific parser
            if msg_type == "ticker":
                parsed = self._parse_ticker(data, exchange, symbol, collected_at)
            elif msg_type == "trades":
                # watchTrades returns a list of trades
                parsed = self._parse_trades(data, exchange, symbol, collected_at)
            elif msg_type == "orderbook":
                parsed = self._parse_orderbook(data, exchange, symbol, collected_at)
            else:
                # Unknown type
                return None
                
            if parsed:
                # Handle list vs dict for stats
                count = len(parsed) if isinstance(parsed, list) else 1
                self.stats["parsed"] += count
                return parsed
            else:
                self.stats["errors"] += 1
                return None
                
        except Exception as e:
            self.stats["errors"] += 1
            # logger.debug(f"Error parsing record: {e}")
            return None

    def _parse_trades(self, data: Any, exchange: str, symbol: str, collected_at: int) -> List[Dict[str, Any]]:
        """Parse CCXT trades data (list of trades)."""
        if not isinstance(data, list):
            # Sometimes it might be a single dict?
            if isinstance(data, dict):
                data = [data]
            else:
                return []
        
        parsed_trades = []
        for trade in data:
            timestamp = trade.get("timestamp") or collected_at
            dt_str = trade.get("datetime")
            
            if not dt_str and timestamp:
                try:
                    dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
                    dt_str = dt.isoformat()
                except Exception:
                    pass

            date_str = dt_str[:10] if dt_str else None
            
            parsed_trades.append({
                "channel": "trades",
                "exchange": exchange,
                "symbol": symbol,
                "product_id": symbol,  # Alias for compatibility with Coinbase config
                "date": date_str,
                "trade_id": trade.get("id"),
                "timestamp": timestamp,
                "datetime": dt_str,
                "symbol_ccxt": trade.get("symbol"),
                "order_id": trade.get("order"),
                "type": trade.get("type"),
                "side": trade.get("side"),
                "price": trade.get("price"),
                "amount": trade.get("amount"),
                "cost": trade.get("cost"),
                "takerOrMaker": trade.get("takerOrMaker"),
                "collected_at": collected_at
            })
        return parsed_trades

    def _parse_ticker(self, data: Dict[str, Any], exchange: str, symbol: str, collected_at: int) -> Dict[str, Any]:
        """Parse CCXT ticker data."""
        # CCXT ticker structure is standard
        # We flatten it for easier Parquet storage
        timestamp = data.get("timestamp") or collected_at
        dt_str = data.get("datetime")
        
        if not dt_str and timestamp:
            try:
                dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
                dt_str = dt.isoformat()
            except Exception:
                pass

        date_str = dt_str[:10] if dt_str else None
        
        return {
            "channel": "ticker",
            "exchange": exchange,
            "symbol": symbol,
            "product_id": symbol,  # Alias for compatibility with Coinbase config
            "date": date_str,
            "timestamp": timestamp,  # Exchange timestamp (ms)
            "datetime": dt_str,    # ISO8601 string
            "high": data.get("high"),
            "low": data.get("low"),
            "bid": data.get("bid"),
            "bidVolume": data.get("bidVolume"),
            "ask": data.get("ask"),
            "askVolume": data.get("askVolume"),
            "vwap": data.get("vwap"),
            "open": data.get("open"),
            "close": data.get("close"),
            "last": data.get("last"),
            "previousClose": data.get("previousClose"),
            "change": data.get("change"),
            "percentage": data.get("percentage"),
            "average": data.get("average"),
            "baseVolume": data.get("baseVolume"),
            "quoteVolume": data.get("quoteVolume"),
            "collected_at": collected_at
        }

    def _parse_orderbook(self, data: Dict[str, Any], exchange: str, symbol: str, collected_at: int) -> Dict[str, Any]:
        """Parse CCXT orderbook data."""
        # CCXT orderbook structure:
        # {
        #     'bids': [[price, amount], ...],
        #     'asks': [[price, amount], ...],
        #     'timestamp': 1234567890,
        #     'datetime': '2023-01-01T00:00:00Z',
        #     'nonce': 123
        # }
        
        # We keep the bids/asks as lists of lists. 
        # Parquet/Arrow can handle this as List<List<Double>>.
        timestamp = data.get("timestamp") or collected_at
        dt_str = data.get("datetime")
        
        if not dt_str and timestamp:
            try:
                dt = datetime.fromtimestamp(timestamp / 1000.0, tz=timezone.utc)
                dt_str = dt.isoformat()
            except Exception:
                pass

        date_str = dt_str[:10] if dt_str else None
        
        return {
            "channel": "orderbook",
            "exchange": exchange,
            "symbol": symbol,
            "product_id": symbol,  # Alias for compatibility with Coinbase config
            "date": date_str,
            "timestamp": timestamp,
            "datetime": dt_str,
            "nonce": data.get("nonce"),
            "bids": data.get("bids", []),
            "asks": data.get("asks", []),
            "collected_at": collected_at
        }
